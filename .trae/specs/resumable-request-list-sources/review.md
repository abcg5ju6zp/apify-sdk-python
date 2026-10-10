# 独立技术评审：ApifyRequestList 远程来源可恢复装载

- 评审日期：2026-10-10
- 评审人：independent-agent（未参与实现，只读评审）
- 评审对象：`src/apify/request_loaders/_remote_sources.py`（新增）、
  `src/apify/request_loaders/_apify_request_list.py`（修改）、
  `tests/unit/actor/test_request_list_remote_sources.py`（新增 26 个用例）、
  `tests/unit/actor/test_request_list.py`（53 个既有用例，未修改）

## 验证命令与结果

| 命令 | 结果 |
| --- | --- |
| `uv run pytest tests/unit/actor/test_request_list_remote_sources.py tests/unit/actor/test_request_list.py -q --numprocesses 0 --timeout=30` | **79 passed** |
| `uv run poe lint` | **All checks passed!** |
| `uv run ty check src/apify/request_loaders tests/unit/actor/test_request_list_remote_sources.py` | **All checks passed!** |
| `uv run pytest tests/unit --ignore=tests/unit/scrapy -q --timeout=60` | **474 passed, 1 failed**；唯一失败 `test_actor_lifecycle.py::test_actor_handles_migrating_event_correctly`（缺 python-socks）。已用 `git stash -u` 在干净 checkout 上复现同一失败，确认为预存环境问题；恢复后工作区完好 |
| `uv run poe type-check`（全量） | 94 diagnostics，干净 checkout 上同样 94 个，全部不在 `request_loaders/`，预存 |

另用 /tmp 一次性脚本（未改仓库）实测：默认 `ImpitHttpClient` 默认发送
`Accept-Encoding: gzip, deflate, br, zstd`，透明解压 gzip 但**保留** `Content-Encoding: gzip`
与压缩态 `Content-Length` 响应头（见 Findings F4）。

## Checkpoints

### 规则型 AC

- [x] **AC-1 确定性合并与全局去重 — PASS**
  - 合并严格串行：[_consume_sources](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1094-L1119) 按 runner 索引顺序消费，下载并发（L1197-L1200）但合并顺序与完成顺序无关；去重裁决在 [_commit_segment](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1049-L1054) 与内联循环 [_apify_request_list.py:103-107](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_apify_request_list.py#L103-L107)，首次出现胜出。
  - 证据：`test_deterministic_order_regardless_of_completion`（3 轮、慢源 0.2s）与 `test_global_unique_key_dedup_first_wins` 全绿；断言完整有序序列，非同义反复。

- [x] **AC-2 稳定来源身份与来源集代际 — PASS**
  - 身份纯函数 [_canonical_source/_compute_source_id/_compute_source_set_hash](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L125-L146)；清单复用/失效在 [append_remote_requests](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1145-L1166)。
  - 证据：`test_source_identities_are_stable_and_order_sensitive`、`test_named_open_creates_manifest_and_reuses_generation`、`test_changed_source_set_discards_old_generation`（断言代际变化 + 旧键被删）。小瑕疵：取消恢复用例未显式断言复用同一 `generation_id`（见 F7）。

- [x] **AC-3 206 身份续传 — PASS（证据强度有瑕疵，见 F7）**
  - Range/If-Range 构造 [L797-L805](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L797-L805)，`_can_resume` 校验 Content-Range 起点与 identity [L857-L865](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L857-L865)；续传解析器从 `raw_offset` 起算 [L897-L904](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L897-L904)。
  - 证据：`test_range_resume_from_seeded_checkpoint`（oneshot 206 路径，输出==LINES）、`test_interrupted_download_resumes_via_range`（断言 2 次请求且第 2 次带 `bytes=`）。用例未直接断言“续传字节数 < 总大小”（F7），代码路径正确。

- [ ] **AC-4 内容变化废弃重装 — FAIL**
  - ETag 变化（200）与前缀哈希不符两条路径有效：`_response_changed` [L867-L876](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L867-L876)、replay 校验 [L554-L586](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L554-L586)；测试 `test_changed_etag_triggers_full_reload`、`test_tampered_prefix_triggers_reload` 通过。
  - **但 416 窗口漏掉“总长度缩短”信号**：[L814-L823](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L814-L823) 对 416 无条件走 `_drive_finalized`，不解析 `Content-Range: bytes */<新长度>`、不比对响应 ETag，直接把旧检查点标 completed。已用 /tmp 脚本复现：实体缩短后恢复，输出仍是旧 30 条 URL、检查点以旧 ETag 标记完成，新内容零请求。详见 **F1（major）**。

- [ ] **AC-5 200 重放前缀验证 + 身份跳过 — FAIL**
  - 主路径正确：replay 在发 plan 前完成整段前缀验证 [L983-L1007](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L983-L1007)，verify 段不进合并队列；`test_replay_on_range_ignoring_server`、`test_tampered_prefix_triggers_reload` 通过。
  - **但检查点携带 206 续传产生的 `wire_spans`（span.start>0）时，未篡改内容也必报前缀不符**：[_consume_span_bytes](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L554-L568) 的 hasher 从字节 0 累加，却与仅覆盖 `[start,end)` 的 span 摘要比较。/tmp 实测：内容未变、服务器恒回 200 时发生 2 次 GET（误判内容变化→弃用检查点→整包重装）。这恰是“206 续传后再次中断、遇不支持 Range 的服务器”这一特性核心场景。详见 **F2（major）**。最终序列因重装而仍正确，但 AC-5 要求的“未变前缀必须验证通过并跳过”被违反，且白付整包带宽。

- [x] **AC-6 压缩流可恢复边界 — PASS**
  - 魔数+响应头双重判定与增量解压 [L427-L475](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L427-L475)；206+压缩拒绝续传改重放 [L862-L863](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L862-L863)；按完整行切分（0x0A 不可能落在 UTF-8 多字节序列内），半行不入确认边界。
  - 证据：`test_encodings_produce_identical_urls`（identity/gzip/deflate 三方一致）、`test_byte_sized_chunks_and_multibyte_tail`（逐字节喂入 + gzip 逐字节）、`test_gzip_source_replays_after_interruption`。

- [x] **AC-7 JSONL 解析与回退 — PASS**
  - 实现 [_parse_jsonl_details/_requests_from_line](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L292-L335)：字符串行/对象行/坏 JSON/无 url/普通文本矩阵、行级整体覆盖、非法 method 抛 ValidationError、深拷贝隔离均有用例（`test_jsonl_line_matrix`、`test_invalid_jsonl_object_raises_validation_error`、`test_line_parsing_matches_full_text_regex`）。仅“JSONL 内容端到端中断恢复”无专门用例（解析/恢复两条路径分别已覆盖，F7）。

- [x] **AC-8 检查点保存失败保留旧记录 — PASS**
  - 回滚逻辑 [L1082-L1090](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1082-L1090)（弹出 span、撤回 completed）；本地 FS KVS 底层为 `atomic_write`（已核实 crawlee `_key_value_store_client.py:334`），无半写。
  - 证据：`test_checkpoint_save_failure_keeps_old_record` 注入持续失败，输出正确且旧记录可解析。

- [x] **AC-9 下载中断有界重试 — PASS（附带 minor 发现 F3）**
  - 4 次有界重试 + 退避 [L754-L791](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L754-L791)，重试以内存中已推进检查点续传，plan 仅发一次。证据：`test_interrupted_download_resumes_via_range`（首次流中断、第 2 次成功，断言 Range）。持续失败抛错路径无测试（F7）；意外 5xx 状态码抛的 `_RetryableDownloadError` 反而不在重试元组中（F3，minor）。

- [x] **AC-10 并入后游标未推进的身份复核 — PASS（直接证据偏弱，F7）**
  - “裁决→并入→检查点内存推进”为无 await 的同步块 [L1049-L1065](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1049-L1065)，取消只能落在 save 等待点；崩溃后以旧 KVS 边界 replay/resume，`seen` 由 `confirmed_requests` 重建，窗口内记录按 unique_key 重新裁决，幂等无重复（代码走查闭环）。
  - 证据：`test_cancellation_resumes_same_generation`（真实 `task.cancel()` 后重开，输出完整）。该用例无法证明取消点落在“并入后推进前”，也未断言同代际，属证据瑕疵而非功能缺陷。

- [x] **AC-11 PERSIST_STATE 刷新与注销 — PASS（证据偏弱，F7）**
  - 注册/注销 [L1192-L1203](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1192-L1203)，force-persist/drain 协议 [L721-L730](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L721-L730)、[L1009-L1016](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1009-L1016)，15s 有界超时；crawlee 包装器吞掉监听器异常，不会拖垮 migration 流程（已核实 `_event_manager.py:267-279`）。
  - 证据：`test_persist_state_flushes_mid_load_and_unregisters` 验证注销为空。但事件后未立即快照中间 `rawOffset`，完成后再读记录无法区分“中途刷新”与“最终刷新”（F7）。

- [x] **AC-12 无名纯内存且行为不回退 — PASS**
  - no-op 存储 [L204-L219](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L204-L219)，`store.enabled` 为假时不 `on()` [L1193-L1194](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1193-L1194)；`test_unnamed_open_does_not_touch_storage` 断言存储目录为空；53 个既有用例零修改全绿。唯一有意行为差异（全局 unique_key 首次胜出）已经需求确认。

- [x] **AC-13 HTTP 客户端注入与数据处理兼容 — PASS**
  - 全部下载走注入 client，仅追加 Range/If-Range（/tmp 探针确认 impit 透传自定义 Range）；来源条目不携带 headers 发起下载 [L797-L809](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L797-L809)；远端请求 userData 深拷贝 [L279-L289](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L279-L289)（与旧实现一致），内联路径维持旧语义；非 UTF-8 既有用例通过。证据：`test_custom_client_used_for_range_requests` + 既有 53 用例。

- [x] **AC-14 已完成来源跨重启零下载重建 — PASS**
  - completed plan 零 HTTP [L748-L752](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L748-L752)，合并器直接重建 [L1104-L1110](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1104-L1110)；Request JSON 往返有 `test_checkpoint_model_camel_case_roundtrip`（含 payload b'x'）。证据：`test_completed_source_reopened_without_downloads`（oneshot 零命中，URL/方法一致）。字段级对比未覆盖 payload/headers/userData（F7）。

### 评分型 AC（rubric）

- **AC-U1 恢复正确性与幂等设计严密性：3 / 5 —— 未达阈值（需 ≥4）**
  - 主状态机（段提交、同步裁决、save 失败回滚、200/206/压缩/取消窗口）设计严密，未见可构造的重复/丢失/脏检查点路径；本地原子写也已核实。
  - 但存在两个声明窗口内的具体反例：416 缩短检测缺失会**静默保留过期请求并错误标记完成**（F1）；wire_spans 重放验证在内容未变时必然误判（F2）。前者产出错误最终序列，按 anchor 描述（“存在可构造的重复/丢失/脏检查点路径”评 1；“主路径正确但边界窗口依赖时序巧合”评 3），综合给 3。

- **AC-U2 流式资源效率：3 / 5 —— 未达阈值（需 ≥4）**
  - 正面：真正流式 `read_stream()`、内存有界（队列 maxsize=8 + 段 100 条 + 单行残留 + 背压阻塞下载）、identity 206 精确到行边界、完成来源零下载、压缩与无 Range 服务器回退重放均符合 anchor 4~5 的主干要求。
  - 扣分：F2 使“206 续传后再中断 + 服务器不支持 Range”这一现实场景必然弃用检查点整包重下；默认 impit 透明解压 gzip 却保留 `Content-Encoding`/压缩态 `Content-Length`，导致经压缩提供的来源在无校验符时可能被 F4 路径误判重下（功能安全，带宽受损）；open() 失败路径无最终刷新（F6）。达不到 anchor 5“仅压缩/无 Range 服务器回退重放”的限定。

- **AC-U3 与仓库架构/约定一致性：5 / 5 —— 达到阈值**
  - Pydantic 模型 camelCase 别名符合 AGENTS.md；复用 Crawlee 默认 KVS 与事件管理器模式；无新增第三方依赖（zlib/hashlib/json 标准库，impit 随 crawlee 已存在）；私有模块组织清晰、docstring 与公开符号（`URL_NO_COMMAS_REGEX`、`url_input_adapter`）保留；关键路径均有 logger；lint、定向 ty、79 个相关用例全绿。

## Findings

### F1（major，违反 AC-4/FR-7）416 响应不做内容变化判定，实体缩短后静默保留旧请求并误标完成

- 位置：[_remote_sources.py:814-823](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L814-L823) 与 [_drive_finalized](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L937-L944)。
- 复现（/tmp 一次性脚本实测）：检查点 `raw_offset=B`（30 行边界，ETag `"v1"`）；服务器把文件截断为 1 行，对 `Range: bytes=B-` 合规返回 `416` + `Content-Range: bytes */<新长度>` + 新 ETag `"v2-shrunk"`。装载器直接 finalize：输出为旧 30 条（新内容 `http://www.totally-new-content.com` 零请求），KVS 记录 `completed=true, etag="v1"`，此后重开永远零下载旧数据。
- 根因：416 分支既不解析 `Content-Range: bytes */total`（现有正则要求数字起点），也不比对响应 ETag/Last-Modified，把“边界恰达实体尾”当作唯一可能。
- 建议：放宽 `_CONTENT_RANGE_REGEX` 允许起点为 `*`；416 时若声明 `total < raw_offset`（或响应校验符与检查点强校验符不一致）则抛 `_ContentChangedError` 走重装；仅当 total 未知/等于边界且校验符一致或缺省时才 finalize。补“截断 + 416”单测。

### F2（major，违反 AC-5/FR-8、削弱 AC-U2）wire_spans 重放校验从字节 0 起算，与跨度摘要口径不一致，未变内容必被误判为变化

- 位置：[_consume_span_bytes](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L554-L568)；span 写入点 [_commit_segment:1070-1079](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1070-L1079)；合成 span（start=0）路径 [L405-L411](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L405-L411)。
- 复现（/tmp 实测）：identity 大文件首次中断在 B1（前缀摘要覆盖 [0,B1)）；206 续传后再中断在 B2，检查点 `wire_spans=[{start:B1,end:B2,digest:sha256(BODY[B1:B2])}]`；第三次打开遇恒回 200 的服务器 → replay 时 hasher 对 [0,B2) 全部行哈希，与 (B1,B2] 摘要比较必不等 → 误判 content changed → 删除检查点并整包重装（实测 2 次 GET）。生产阈值 256KB，任何 >256KB 的 identity 来源“两次中断 + 服务器不支持/临时拒绝 Range”都命中，正是本特性的目标场景。
- 影响：最终输出因重装仍正确（无重复/丢失），但违反 AC-5“未篡改前缀必须验证通过”，且检查点被无谓废弃、整包重下；现有测试只覆盖无 span 的单次重放，未覆盖该路径。
- 建议：校验器记录当前 span 起点，`content_offset < target.start` 时不累加（或在到达 start 时重置 hasher），从 `[start,end)` 起算后再比对；保持 start=0 合成 span 行为。补“206 续传产生 span → 200 重放必须仅 1 次请求且检查点存活”的端到端用例。

### F3（minor）名为 `_RetryableDownloadError` 的异常未被纳入重试元组，意外 5xx 一次即致命

- 位置：[_RETRYABLE_EXCEPTIONS:77-86](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L77-L86) 与抛出点 [L812](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L812)、[L848-L850](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L848-L850)。
- 流中途异常走有界重试，但“意外状态码”（如瞬时 500/503/429）抛出的 `_RetryableDownloadError` 不在捕获元组中，立即 FATAL 使整个 `open()` 失败，与命名及 FR-9 的有界重试意图相悖。
- 建议：将 `_RetryableDownloadError` 纳入 `_RETRYABLE_EXCEPTIONS`（耗尽后仍作为终态异常抛出，L790-L791 已兼容），并对 zlib.error 这类确定性坏数据考虑不重试（非必须）。

### F4（minor，观测项）默认 impit 透明解压但保留 `Content-Encoding`/压缩态 `Content-Length`，抑制 206 并可能触发误判

- /tmp 探针实测：`ImpitHttpClient` 默认发 `Accept-Encoding: gzip, deflate, br, zstd`，对 gzip 响应交付解压后明文（魔数双重判定正确回退 identity，不会产出坏数据），但响应头仍为 `Content-Encoding: gzip`、`Content-Length: <压缩长度>`。
- 后果（均为安全降级，无错误输出）：① 这类来源 206 永远被 `_can_resume` 拒绝 → 回退整段重放，identity 续传能力在默认客户端 + gzip 服务器下事实失效（影响 AC-U2 评分）；② 无 ETag/Last-Modified 时重放路径 `_response_changed` 会用压缩长度与解压坐标 `raw_offset` 比较，可能误判“长度缩短”而弃用检查点重装。
- 建议：解析器实际判定为 identity 透传时，以该事实覆盖检查点/恢复判定中的 `encoding`（或在透明解压后忽略声明的 Content-Length 做长度比较）；至少在文档/注释中固化该客户端行为。

### F5（minor）416 finalize 以 content_offset 回写压缩检查点的 raw_offset

- 位置：[_make_resume_parser:927-935](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L927-L935) + `_flush.end_raw`。
- 若压缩来源（raw_offset 为压缩字节计数）走到 416，空 final 段会把 `raw_offset` 改写为 `content_offset`（解压坐标），破坏字段语义；completed 后不再被消费，故仅为记录不一致。修复 F1 时一并让 416 分支保留/校验原 raw_offset 即可。

### F6（minor，偏离 FR-10 字面）open() 失败路径无最终刷新；重试换解析器时 force-persist 标志丢失

- finally 仅注销监听器 [L1201-L1203](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1201-L1203)；他源致命导致整体失败时，本源已并入但未达 256KB 阈值的确认段不刷盘（下次重下，正确性无损）。
- PERSIST_STATE 触发时若 runner 正处于重试退避（旧 parser 已废、新 parser 未建），`force_next_persist()` 标志丢失（[L728-L729](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L728-L729)），drain 要等到下个 256KB/final 才 resolve（有 15s 超时兜底，异常被 crawlee 包装器吞掉）。
- 建议：把 force 标志保存在 runner 上并在 `_make_parser` 后传递；失败路径在 finally 中做一次 best-effort 刷新。

### F7（minor，测试证据强度）

- AC-3：未断言 206 实际下载字节数 < 总大小（oneshot 200 也能通过）。
- AC-9：缺“持续失败 → 抛出且旧检查点保留”用例。
- AC-10：取消点不确定落在“并入后、推进前”，且未断言复用同一 `generation_id`。
- AC-11：事件派发后未立即快照中间 `rawOffset`/confirmedKeys，完成后再读无法证明中途刷新。
- AC-14：重建字段级断言仅 url/method，未覆盖 payload/headers/userData（往返模型另有覆盖，但端到端缺口存在）。
- AC-7：JSONL 内容没有端到端中断恢复用例。
- 建议：用可控 handler/客户端补齐上述断言（多数仅需增加 spy 与即时 KVS 快照）。

## 结论

- 规则 AC：**AC-4、AC-5 未通过**（F1、F2，均为 major）；其余 12 项通过（其中 AC-9/AC-10/AC-11 的测试证据偏弱，见 F7）。
- Rubric：**AC-U1=3、AC-U2=3 均未达 ≥4 阈值**；AC-U3=5 达标。
- lint 与定向 ty 通过；79 个相关用例与 474 个全量单测通过；唯一单测失败已在干净 checkout 证实为预存环境问题。
- 存在 2 个 major 可执行发现（其中 F1 产出静默错误结果，F2 违反重放协议并浪费整包带宽）。

**总体结果：FAIL**

## Review History

- [R1|date=2026-10-10] <reviewer=independent-agent> <result=fail> <summary=主状态机/流式/去重/兼容性实现扎实且测试多为有效断言，但发现 2 个 major：416 窗口不检测实体缩短致旧请求静默保留（AC-4），wire_spans 重放校验从 0 起算致未变内容必被误判并整包重装（AC-5）；AC-U1=3、AC-U2=3 未达阈值；lint/定向 ty/79 相关用例全绿，唯一全量单测失败为预存环境问题。>

## Re-review R2 (2026-10-10)

- 评审人：independent-agent-r2（未参与实现与 R1，只读复审；本轮仅在 /tmp 写一次性探针，未改仓库文件）
- 评审对象：R1 整改后的 `_remote_sources.py` / `_apify_request_list.py` / 两个测试文件（相关用例现 32 个）

### 验证命令与结果

| 命令 | 结果 |
| --- | --- |
| `uv run pytest tests/unit/actor/test_request_list_remote_sources.py tests/unit/actor/test_request_list.py -q --numprocesses 0 --timeout=30` | **85 passed**（32+53） |
| `uv run poe lint` | **All checks passed!** |
| `uv run ty check src/apify/request_loaders tests/unit/actor/test_request_list_remote_sources.py` | **All checks passed!** |
| `uv run pytest tests/unit --ignore=tests/unit/scrapy -q --timeout=60` | **480 passed, 1 failed**；唯一失败仍为 `test_actor_lifecycle.py::test_actor_handles_migrating_event_correctly`（python-socks 缺失），与 R1 记录的预存环境问题一致 |

另在 `/tmp/r2probe`（独立 conftest 隔离 storage/service_locator）跑了 11 个一次性探针：416 判定矩阵、
fresh→206→206→200 多重启链状 span、非合规 416、原始 gzip 客户端 416 收尾、坏 gzip 有界终止、
真实压缩中断回退重放、取消/致命时 best-effort 刷盘、以及 F1/F2/F3 回归对“回退修复”的敏感性。

### R1 major 复核

- **F1 已真正修复。** 正则 [_CONTENT_RANGE_REGEX](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L75)
  现接受 `bytes */total` 与 `*`；416 分支 [L814-L825](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L814-L825)
  经 [_unsatisfied_range_means_changed](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L868-L883)
  裁决：校验符变化→`_ContentChangedError` 重装；total 已知且 `< raw_offset`→重装；无 total 无一致校验符→保守重装；
  仅 total==边界（identity 坐标下即实体尾）才 `_drive_finalized`，且 plan 尚未发出，重装不会污染已合并序列。
  identity finalize 的空 final 段 end_raw==content_offset==raw_offset，**不破坏偏移**。
  证据：既有 `test_shrunk_entity_returning_416_triggers_reload` 全绿；/tmp 判定矩阵探针 5/6 符合预期；
  回退探针把该函数改为恒 False 后，截断场景立刻重现“旧 5 条保留 + completed=True”（证明回归有效）。
  **残留 1 个非合规服务器边角，见 R2-N1（minor）。**
- **F2 已真正修复，且未引入新的 span 校验缺陷。** 重放目标由
  [_replay_spans](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L955-L966)
  合成为 `[0,content_offset)` 的**连续覆盖**（prefix_sha256 合成首段 + 各 wire_spans）；
  [_consume_span_bytes](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L549-L568)
  严格按 `[start,end)`、以完整行为单位哈希，每行至多关闭一个 span，边界不等或摘要不符即判变化。
  /tmp 端到端探针（阈值 128B）实测**跨 3 次进程级中断的链状续传**：open1 落 prefix[0,240)、
  open2 落 span[240,480)、open3 落 spans[240,480)+[480,720)（KVS 记录逐步断言）；
  第 4 次打开切到恒 200 服务器，**仅 1 次 GET** 即完成 prefix+两段 span 校验并输出全部 60 行、
  completed 落盘。压缩重放（种子 gzip 用例 + 真实流中断后 206 被拒→无 Range 重放）输出与 identity 一致。
  回退探针恢复 R1 语义后同场景必发 2 次 GET（误判重装），证明 `test_wire_spans_replay_verifies_with_single_full_get`
  能抓住回退。span 连续性依赖本模块自身的记账不变量（persist 失败不推进窗口、206 起点=上次持久化边界），
  走查未发现破坏该不变量的路径；非连续 span 只能来自 KVS 被篡改，不在威胁模型内。

### 规则型 AC

- [x] **AC-1 — PASS**：串行合并（[L1147-L1164](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1147-L1164)）+ 首次胜出（[L1095-L1100](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1095-L1100)）；两轮新探针输出均确定，既有 3 轮打乱用例全绿。
- [x] **AC-2 — PASS**：身份/代际纯函数与清单复用/废弃逻辑未变；取消恢复用例本轮已补同 generation 断言（见 AC-10）。
- [x] **AC-3 — PASS**：206 路径保持正确；新断言“实际投递字节 < 总大小”（`test_range_resume_from_seeded_checkpoint` 的 `0 < served_bytes < len(BODY)`）堵住 F7 的 oneshot 漏洞。
- [x] **AC-4 — PASS（R1 FAIL 项，已修复）**：校验符变化（200/416）、前缀/键/span 不符、长度缩短三路失效均导致弃用旧记录并以新内容重装；`test_shrunk_entity_returning_416_triggers_reload`（416+`bytes */短长度`+新 ETag→新实体全量、新 ETag 落盘）与回退敏感性探针为证。
- [x] **AC-5 — PASS（R1 FAIL 项，已修复）**：200 重放在发 plan **之前**完成整段前缀验证（[L1003-L1010](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1003-L1010) 与 [L1028-L1052](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1028-L1052)），链状 span 未变内容 1 次 GET 验证通过；篡改前缀仍转 AC-4 重装。
- [x] **AC-6 — PASS（有条件保留，见 R2-N2）**：魔数+头双判定、增量解压、行边界语义不变；gzip/deflate/identity 一致、逐字节、真实中断回退重放探针全绿。**但**注入“透传原始压缩字节”的客户端且末块送达后连接重置（无 EOF）、实体无尾换行时，416 收尾会丢失尾部记录——预设立足于 AC-6 标准客户端场景（仓库自带 impit/httpx 客户端均透明解压，该路径不可达），故维持 PASS 并记 minor。
- [x] **AC-7 — PASS**：JSONL 矩阵/非法对象/深拷贝隔离不变；新增 `test_jsonl_source_replays_from_confirmed_prefix`（JSONL 200 重放、POST 对象行顺序）补齐 F7 端到端缺口。
- [x] **AC-8 — PASS**：保存失败回滚 span/completed、旧记录可解析的用例不变；底层本地 KVS 原子写。
- [x] **AC-9 — PASS（F3 已修复，有实测）**：`_RetryableDownloadError` 已入重试元组（[L86-L96](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L86-L96)）；`test_transient_5xx_is_retried`（503×2→200，3 次成功）、`test_persistent_5xx_fails_and_keeps_checkpoint`（持续 500 抛出且旧记录可读）全绿；/tmp 探针确认**持续坏 gzip（zlib.error 确定性坏数据）恰好重试 4 次后抛出、不无限循环**。回退探针（元组移除该异常）下首次 503 即致命，证明回归有效。
- [x] **AC-10 — PASS**：裁决→并入→内存推进仍为无 await 同步块；取消用例现显式断言取消前后 `generationId` 一致；cancel 后 206 重开输出完整。
- [x] **AC-11 — PASS**：force-persist 标志跨重试保留（runner 级标志 [L713](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L713)，在每个新 parser `_drive` 开头补打 [L995-L997](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L995-L997)）；事件用例现轮询快照到中途 `rawOffset>0 且 completed=False` 再等结束，区分了中途/最终刷新；open 后监听器为空。
- [x] **AC-12 — PASS**：no-op 存储路径不变，53 个既有用例零修改全绿。
- [x] **AC-13 — PASS**：下载全走注入客户端、仅追加 Range/If-Range；自定义 client 用例断言第 2 次请求带 Range。
- [x] **AC-14 — PASS**：零下载重建用例现断言 payload/headers/userData 字段级一致（`test_completed_source_reopened_without_downloads`）。

### F4 / F6 专项复核

- **F4（长度缩短仅限 identity）— 已修复。** [_response_changed](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L895-L905)
  对非 identity 声明跳过 Content-Length 比较，单测覆盖压缩头+小长度不判变、identity+小长度判变两路。
  走查补充：416 路径仍会比较 total 与 raw_offset——对原始压缩客户端是同坐标比较（合理），对透明解压客户端
  可能“误判变化”，方向安全（触发重装而非保留脏数据）。
- **F6（force 标志 carry + 失败路径 best-effort 刷盘，不掩盖原异常）— 已修复且有实测。**
  finally [L1247-L1253](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1247-L1253)
  注销后 `suppress(BaseException)` 包住 [_flush_pending_checkpoints](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1256-L1268)
  （内部逐源 try/except + `gather(return_exceptions=True)`）。Python 语义上该 suppress 只吞刷盘自身异常，
  try 块的原异常在 finally 结束后照常传播——/tmp 两探针实测：阈值调到 10⁹ 后取消，KVS 落到 (480, False, 20 keys)
  且 `open` 仍抛 CancelledError；持续致命失败后 KVS 落到 (240, False, 10 keys) 且 open 仍抛
  ExceptionGroup。即“刷到了边界 + 不掩盖原异常”两点同时成立。

### 独立状态机复审（新 bug 扫描）

- 重装决策全部发生在 plan 发出之前（416/200/206/重放验证皆然），`_ContentChangedError` 不会在旧段已合并后
  才出现；`if self._plan_emitted: raise` 仅为兜底。206 流中断的进程序重试以内存行边界续传、无重叠；
  跨进程从较旧 KVS 边界续传不会在**新进程的**序列内重复（旧序列随进程丢弃，seen 由 KVS confirmed 重建）。
- 持久化失败回滚不回退 confirmed_keys/内存游标是有意设计：下一持久化窗口以未前移的
  `persist_start` 重新记账，span 仍连续；finish 落盘失败只使 completed 留 False，重开可恢复，无脏写。
- 队列背压（maxsize=8）+ 段 100 条 + 单行残留维持内存有界；PERSIST_STATE 等待有 15s 超时且监听器异常被
  crawlee 包装器吞掉，不会拖垮 migration。
- 未发现整改引入的重复/丢失/脏检查点新路径；残留仅有下面两条历史/边角问题。

### 新发现

#### R2-N1（minor，416 边角）非合规 416 声明 total > raw_offset 时提前 finalize，后缀请求丢失

- 位置：[_unsatisfied_range_means_changed](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L879-L883)
  使用 `current_total < checkpoint.raw_offset`。
- 复现（/tmp 探针实测）：种子检查点边界=前 20 行，服务器对 `Range:` 回 `416 + Content-Range: bytes */<len(BODY)+500>`
  （无 ETag）。裁决返回 False → 直接 finalize：输出仅 20 条、`completed=True`，后缀 40 条永久丢失且之后零下载。
- 前提：服务器违反 RFC 9110（`bytes=N-` 仅在 N≥complete-length 时才允许 416），合规服务器不会在 total>N 时回 416；
  也与 I-1 自述策略“仅 total==边界才 finalize”不符。
- 建议：收紧为 `current_total != checkpoint.raw_offset`（或对非相等情形一律重装/重试），一行改动。

#### R2-N2（minor，F5 类问题升级）原始压缩客户端 + 末块后重置 + 无尾换行时，416 收尾静默丢失尾部记录

- 位置：416 分支对压缩检查点同样走 [_make_resume_parser](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L968-L976)
  强制 identity + `_drive_finalized`；压缩检查点的 `raw_offset` 是**压缩字节**计数，可等于压缩总长，
  而此时 `finish()` 尚未运行，尾部无换行记录仍在 parser tail / pending 中。
- 复现（/tmp 探针实测，fake HttpClient 透传**原始 gzip**）：30 行无尾换行内容，前 25 行已随末块并入，
  末压缩块送达后连接 reset（无 EOF）；重试 `Range: bytes=123-`（==压缩总长）→ 合规 416 + 匹配 ETag
  → finalize 并写 `completed=True`，**第 26-30 条永久丢失**，记录 rawOffset 还被改写成解压坐标
  （并可能追加 start=压缩长/end=解压长的伪 span）。跨重启同样永久。
- 影响面（为何评 minor）：仓库自带两个客户端 impit 与 crawlee httpx（`aiter_bytes()`）都**透明解压**，
  魔数双判定使解析器回落 identity（偏移为行对齐解压坐标，该路径安全），故随仓库客户端不可达；
  仅当用户注入“透传原始压缩字节且保留 Content-Encoding”的自定义客户端、且末块后异常断连、且文件无尾换行
  三个条件同时成立才触发。R1 的 F5 把它当作“仅记录不一致”低估了。
- 建议：416 分支在 `checkpoint.encoding != 'identity'`（或解析器实际曾走解压路径）时不得 finalize，
  改走无 Range 全量重放（FR-8）由前缀/键复核自然收回尾部；finalize 只允许在 identity 坐标且
  total==raw_offset 时发生。

### 评分型 AC（rubric）

- **AC-U1：4 / 5（达标）。** R1 的两个可执行反例（416 截断、span 误判）已闭环并有“多重启链状 span”
  这种超出用例强度的独立探针证据；段提交、save 失败回滚、取消/致命刷盘、压缩回退窗口均有显式转换与实测。
  未给 5：仍存在 R2-N1/R2-N2 两个可构造（但前提狭窄）的静默丢失边角，未达到 anchor 5“所有声明窗口均闭环”。
- **AC-U2：4 / 5（达标）。** identity 206 精确到行边界、完成来源零下载、压缩/无 Range 服务器回退重放均实测；
  链状续传后 200 重放仅 1 次 GET。未给 5：默认 impit 保留压缩头抑制 206 的 F4 观测项仍在（带宽向安全降级），
  且超大检查点记录“降级重放”在 Constraints 中提及但代码无显式尺寸降级（保存失败按瞬时错误处理，不阻断装载，
  功能等价）。
- **AC-U3：5 / 5（达标）。** 模型别名、KVS/事件复用、标准库依赖、docstring、日志与 lint/ty/85 相关用例全绿。

### 结论

- R1 的 F1、F2 两个 major 均已真正修复且回归测试对回退敏感；F3/F4/F6 按声明收口并有独立实测；F7 各项测试
  强度补强均落实。规则型 AC-1..AC-14 全部通过（AC-6 的结论限于其标准客户端 Given 窗口，附带 R2-N2 保留意见）。
- 新发现仅 2 个 **minor** 边角（R2-N1 非合规 416、R2-N2 原始压缩客户端 416 收尾丢尾），均有明确一行级/
  小改建议，不阻塞本特性；建议在后续小补丁中收口。
- lint、定向 ty、85 个相关用例、480 个全量单测全绿；唯一单测失败为 R1 已记录的预存 python-socks 环境问题。

**总体结果：PASS**

- [R2|date=2026-10-10] <reviewer=independent-agent-r2> <result=pass> <summary=F1/F2 两个 major 经独立探针证实真正修复（416 缩短/校验符/信息不足三路裁决、fresh→206→206→200 多重启链状 span 单次 GET 重放、压缩回退），F3/F4/F6 收口且回归对回退敏感；AC-1..14 全过，AC-U1=4、AC-U2=4、AC-U3=5；lint/ty/85 相关用例/480 全量单测全绿（唯一失败为预存 python-socks）；新增 2 个 minor 边角（非合规 416 total>边界提前 finalize、原始压缩客户端末块重置后 416 收尾丢尾，仓库自带透明解压客户端不可达）。>

## Re-review R3（2026-10-10，R2-N1/R2-N2 整改 delta 专项）

- 评审人：independent-agent-r3（未参与此前实现与 R1/R2；本轮仅在 /tmp/r3probe 写一次性探针，未改仓库文件）
- 评审范围：仅限 R2 两条非阻塞 minor 的整改 delta——
  [_unsatisfied_range_means_changed](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L873-L889)
  的 `<`→`!=` 收紧、416 分支按编码分流（[L814-L830](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L814-L830)
  ＋ [_attempt_replay](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L851-L862)）、
  以及两个新增回归用例（[L518-L557](file:///Users/ding/Documents/swe/0910092/project-10/tests/unit/actor/test_request_list_remote_sources.py#L518-L557)、
  [L559-L595](file:///Users/ding/Documents/swe/0910092/project-10/tests/unit/actor/test_request_list_remote_sources.py#L559-L595)）；不重评整个特性。

### 验证命令与结果

| 命令 | 结果 |
| --- | --- |
| `uv run pytest tests/unit/actor/test_request_list_remote_sources.py -q --numprocesses 0 --timeout=30` | **34 passed**（R2 时 32 个 + 本次 2 个新增） |
| `uv run pytest tests/unit/actor/test_request_list.py -q --numprocesses 0 --timeout=30`（相邻既有文件，交互回归） | **53 passed** |
| `uv run poe lint` | **All checks passed!**（323 files already formatted） |
| `uv run ty check src/apify/request_loaders tests/unit/actor/test_request_list_remote_sources.py` | **All checks passed!** |

/tmp 探针（独立隔离 storage/service_locator，默认 ImpitHttpClient，对真实本地 HTTP 服务器）：
`probe.py` 在同一脚本内分别以**当前代码**与**回退的 R2 语义**（`<` 比较 + 压缩 416 也 finalize）
跑两个新用例的同构场景；`probe_edge.py` 补测测试套件未直接覆盖的“精确到尾 identity 416 finalize”边角。

### R2-N1 复核：`!=` 收紧 — PASS

- [_unsatisfied_range_means_changed](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L873-L889)
  裁决顺序正确：**先** [_validators_changed](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L864-L871)
  （两侧校验符都存在且不一致即判变；缺省不视为变化），再解析 `Content-Range`；total 已知时
  `return current_total != checkpoint.raw_offset`——只有**严格相等**（identity 坐标下即边界恰达实体尾）
  才允许 finalize，total>边界（非合规 416）与 total<边界（缩短）都判变；total 缺省保守判变。
- 矩阵走查：缩短+新 ETag/旧 ETag、total>边界+同 ETag、total==边界+同 ETag、无 total 四向均符合 R2 建议；
  判变路径只发 `_ContentChangedError` 且此时 plan 尚未发出（[L819-L820](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L819-L820)
  在任何 parser/plan 之前），重装不污染已合并序列。
- **既有交互未破坏**：① 缩短重装由 `test_shrunk_entity_returning_416_triggers_reload` 覆盖且通过；
  ② 精确到尾 identity finalize 无专门用例，/tmp 探针实测：种子检查点 60 行全确认、
  `raw_offset=content_offset=len(BODY)`，合规 416（`bytes */1440` + 匹配 ETag）→ **仅 1 次请求**、
  输出 60 条、`completed=True` 且 raw/content offset 均保持 1440 无漂移（空 final 段不破坏偏移）。

### R2-N2 复核：压缩 416 改走全量重放 — PASS

- 416 分支 [L821-L830](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L821-L830)
  现按 `checkpoint.encoding != 'identity'` 分流：压缩检查点不再构造 identity resume parser / 不 finalize，
  仅置 `replay_without_range=True`，退出 416 响应上下文后由
  [_attempt_replay](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L851-L862)
  **无 Range 重新 GET**：200 校验（压缩跳过 Content-Length 比较，[L901-L911](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L901-L911)）
  → 带 `verify_keys`/`content_offset`/前缀摘要的 replay parser 全量复核，尾部无换行记录由正常 finish 路径自然收回，
  R2-N2 的“原始 gzip 客户端 + 末块后重置 + 无尾换行丢尾”与 F5 的 raw/content 坐标改写、伪 span 问题一并消除
  （该路径根本不产生 final 段）。
- 状态机交互走查：416 分支未创建 parser、未发 plan；replay 的 plan 仍在
  [_verify_prefix](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py#L1034-L1058)
  成功之后才发出，重放复核失败时旧 confirmed 请求不会被合并器接纳，`_ContentChangedError` 照旧删点重装。
  压缩场景下若 total 未知或 `!= raw_offset`（且校验符一致），则按判变走保守整包重装——方向安全，仅多付带宽，
  与 R2 已接受的保守策略一致。

### 两个新增用例的有效性（对 R1/R2 时代代码必失败，断言正确）

- `test_416_with_total_beyond_boundary_is_not_finalized`：identity 检查点 20 行（真实前缀摘要 + ETag v1），
  服务器非合规回 416 + `bytes */len(BODY)+50` + 同 ETag。R1/R2 语义下 `<` 判定为“未变”→直接 finalize，
  探针实测仅 **20 条 URL、1 次请求**；新代码判变→删点→第 2 次无 Range GET 全量恢复 60 条。
  断言 `== LINES`（抓后缀丢失）与 `request_count == 2`（抓错误 finalize）均有效且非同义反复。
- `test_416_for_compressed_checkpoint_replays_full_entity`：gzip 检查点
  `raw_offset==压缩总长`、已确认 18 行，416 回 `bytes */压缩总长`（无 ETag，精确相等）。
  R1/R2 语义下无条件 finalize，探针实测仅 **18 条、1 次请求**；新代码改无 Range 全量重放，
  前缀/边界复核通过后收回剩余 42 条，共 60 条、2 次请求。两条断言同样有效。
- /tmp 回退探针输出：当前代码两场景均 `urls=60, requests=2`；回退 R2 语义后分别
  `urls=20/requests=1` 与 `urls=18/requests=1`，两个新用例在 R1/R2 代码上**必然失败**，在当前代码上通过。

### 结论

- R2-N1、R2-N2 均已按建议正确收口；delta 最小且自洽，未破坏精确到尾 identity finalize、缩短重装、
  206 续传/重放及 53 个既有 request_list 用例；lint、定向 ty、34+53 个相关用例全绿。
- 两个新回归用例被 /tmp 探针证实对回退敏感、断言有效；R2 记录的其余事项不在本轮范围。

**总体结果：PASS**

- [R3|date=2026-10-10] <reviewer=independent-agent-r3> <result=pass> <summary=R2-N1（416 裁决 `<`→`!=`，校验符优先、严格相等才 finalize）与 R2-N2（压缩检查点 416 不再 finalize，改无 Range 全量重放并经前缀复核收尾）均正确收口；34 个远程来源用例（含 2 新增）+53 个既有用例、lint、定向 ty 全绿；/tmp 回退探针实证两个新用例在 R2 语义下必失败（分别只产出 20/18 条、1 次请求），当前代码下精确到尾 identity finalize 仍为 1 次请求且偏移无漂移；无交互回归。>
