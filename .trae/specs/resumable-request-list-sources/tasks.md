# ApifyRequestList 远程来源可恢复装载 - 实施计划

说明：任务为依赖有序的纵向切片，实现与对应单元测试同一切片内完成。代码集中在
`src/apify/request_loaders/_apify_request_list.py`（必要时在 `request_loaders/` 内新增私有模块），
测试位于 `tests/unit/actor/test_request_list.py`（必要时新增同目录测试文件）。

## Task 1: 检查点模型、稳定身份与代际清单存储
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 新增 Pydantic 模型（camelCase 别名，遵循 AGENTS.md 模型约定）：
    - `SourceCheckpoint`：`source_id`、`generation_id`、`url`、`etag`、`last_modified`、`encoding`、
      `total_length`、`raw_offset`、`prefix_sha256`、`confirmed_keys`（有序）、`confirmed_requests`
      （Request 的 JSON 序列化形态，可完整重建）、`completed`。
    - `CheckpointManifest`：`generation_id`、`source_set_hash`、有序 `source_ids`、`version`。
  - 实现稳定身份派生：规范化（canonical JSON，键排序、ensure ASCII 确定性）条目后计算
    `sha256(index + canonical(entry))` 为 `source_id`；`source_set_hash` 对全部来源条目顺序敏感地哈希；
    `generation_id` 为 uuid4 十六进制。
  - KVS 键布局：清单键 `SDK_REQUEST_LIST_SOURCES_STATE-{name}`，每来源键
    `SDK_REQUEST_LIST_SOURCE_STATE-{name}-{index}-{short_id}`（不含原始 URL）。
  - 实现检查点存储组件：经 `crawlee.storages.KeyValueStore.open()` 默认 KVS 读取/写入清单与来源记录；
    写入为整条 `model_dump(by_alias=True)` JSON；提供“载入/丢弃全部/写入来源/写入清单/删除”操作；
    无 `name` 时给出空实现（no-op）存储，绝不访问 KVS。
  - `open()` 早期接入：计算当前输入身份；无清单→建清单与代际；哈希一致→复用；不一致→清理旧键、建新一代。
- **Acceptance Criteria Addressed**: AC-2, AC-12, AC-14
- **Test Requirements**:
  - `rule` TR-1.1: 相同输入两次计算 `source_id`/`source_set_hash` 相等；交换来源顺序或改任一字段后集合哈希不同（单测直接断言纯函数）。
  - `rule` TR-1.2: 首次 `open(name=...)` 后默认 KVS 存在清单键且含 uuid `generation_id`；再次相同输入 `open()` 复用该代际；改动输入后旧键被删除、代际更新（基于本地文件系统 KVS 的单测，检查记录与删除行为）。
  - `rule` TR-1.3: 无 `name` 的 `open()` 过程中 KVS 无任何新增/读取记录（spy 或空目录断言）。
  - `rule` TR-1.4: `SourceCheckpoint` 可 camelCase JSON 往返，`confirmed_requests` 中的 Request 往返后字段逐字段相等。
- **Notes**: Request 序列化采用 `model_dump(mode='json', by_alias=True)` / `Request.model_validate`，与 Crawlee `RequestListData` 一致。

## Task 2: 流式分块解析管线（解压、解码、行边界、JSONL/正则记录）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 实现来源内容流式解析器（输入：`http_client.stream()` 的响应；输出：按段产出
    `ParsedSegment(records, raw_offset, prefix_sha256, ended)`）：
    - 用 `hashlib.sha256` 对实际消费的实体字节增量计算前缀摘要；按原始字节累计 `raw_offset`。
    - 编码判定：响应头 `Content-Encoding` 为 gzip/deflate **且**首块含对应魔数/格式时用
      `zlib.decompressobj(wbits=16|MAX_WBITS / -15)` 增量解压；否则按透传字节处理（兼容客户端已透明解压）。
    - `codecs.getincrementaldecoder('utf-8')(errors='replace')` 跨块解码；按行切分，无换行结尾的尾部保留为残留缓冲；
      流结束时冲洗最后一行；空行不出记录。
    - 每行：尝试 JSON 解析→字符串行取其值为 URL；对象行取 `url` + 可选 `method/payload/headers/userData`
      （camelCase），经现有 `_RequestDetails` 风格校验，行级字段整体覆盖来源默认；非 JSON 或无 `url` 行→
      在该行文本上运行 `URL_NO_COMMAS_REGEX`；Request 构造沿用 `Request.from_url` 与 `userData` 深拷贝。
  - 段大小有界（按行数/字节数阈值切段），保证内存有界。
- **Acceptance Criteria Addressed**: AC-6, AC-7, AC-11, AC-13, NFR-1
- **Test Requirements**:
  - `rule` TR-2.1: identity 与 gzip、deflate 三种编码、相同多行 URL 内容，解析出的记录 URL 序列完全一致。
  - `rule` TR-2.2: 在任意字节边界切分块（逐字节迭代喂入）时，多字节字符不产生 U+FFFD 之外的差异、半行不产出记录；流结束冲洗最终行。
  - `rule` TR-2.3: JSONL 行类型矩阵（字符串、完整对象、headers/userData 嵌套值、坏 JSON、无 `url` 对象、混合普通文本）产出符合 FR-4；行级字段覆盖来源默认；不同请求的 `userData` 嵌套 dict 互不别名。
  - `rule` TR-2.4: 按行正则提取与现行全文 `re.finditer` 对多行样本的提取结果一致（URL 集合与顺序）。
  - `rubric` TR-2.5: 内存有界性；scale 1-5；anchors 1=整包缓冲/残留无限增长，3=有残留但无上限，5=残留≤1 行、段大小有上限且不随文件总大小增长；threshold >= 4；evidence 代码走查+大文件分片测试中峰值缓冲断言。

## Task 3: 来源恢复状态机（首下、206 续传、200 重放、内容失效、有界重试）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - 实现单来源装载协程：
    - 无检查点或已失效：GET（经 `stream()`）首下，采集 `ETag`/`Last-Modified`/`Accept-Ranges`/`Content-Length`/
      实际编码，流式解析并逐段提交给合并器（Task 4），完成后写 `completed=True` 检查点。
    - 有未完成检查点：发 `Range: bytes={raw_offset}-`；有校验符时附 `If-Range`。
      - `206` 且 `Content-Range` 起点==`raw_offset` 且 identity：从边界续传解析。
      - `206` 但压缩编码：关闭响应，改走重放。
      - `200` 且校验符表明变化（If-Range 失败 / ETag、Last-Modified 不一致）：内容变化→丢弃来源检查点、首下重装。
      - `200` 其余（服务器不支持 Range）：重放——从头流式解析，逐记录核对 `confirmed_keys[i]` 与
        `[0,raw_offset)` 前缀 SHA-256；前缀记录全部跳过；哈希不符/身份不符→内容变化→丢弃重装；越过边界后续传式提交。
    - 总长度可知且 `< raw_offset`：按内容变化处理。
  - 流中连接错误/超时：保留上次已提交检查点，有界重试（指数退避可被测试注入零等待），每次重试重走上述决策；
    耗尽后向上抛出。
  - 已完成来源：直接由检查点的 `confirmed_requests` 重建，不发起请求。
- **Acceptance Criteria Addressed**: AC-3, AC-4, AC-5, AC-9, AC-14, NFR-4
- **Test Requirements**:
  - `rule` TR-3.1: 支持 Range+ETag 的服务器上中断于行边界后重开：请求带 `Range`/`If-Range`、返回 206，仅下载后缀字节；输出序列与一次成功一致。
  - `rule` TR-3.2: ETag 变化（If-Range→200 新内容）与前缀篡改（重放哈希不符）两条路径均丢弃旧检查点并整体重装，序列以新内容为准。
  - `rule` TR-3.3: 对 Range 恒回 200 且内容未变的服务器：重放后前缀请求零重复并入，最终序列与无中断一致；篡改前缀时转为重装。
  - `rule` TR-3.4: 前 N 次流式读取抛连接错误、之后成功：`open()` 成功且仅重传必要字节；持续失败时抛出且 KVS 旧检查点仍可读。
  - `rule` TR-3.5: gzip 来源中断恢复走重放路径，输出与 identity 一致（与 Task 2 测试联动）。
  - `rule` TR-3.6: 已完成来源重开时 httpserver 零命中，重建 Request 字段与首次一致。

## Task 4: 确定性合并器、全局去重与段提交原子性
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - 重构 `open()` 装载编排：远程来源下载任务并发创建（保持 TaskGroup），但每个来源把段投递到各自的有序队列/异步迭代器；
    单一合并协程严格按“内联请求（输入顺序）→ 来源索引 0..N”消费：来源 i 未完成前不消费 i+1 的结果
    （其下载仍在后台继续），保证完成顺序不影响合并。
  - 全局去重集合 `set[str]`：内联请求先入集；合并段时对每条记录，`unique_key` 已在集则丢弃、否则入集并计入最终序列；
    全部裁决完成后才把该段交来源检查点提交（`raw_offset`/前缀摘要/`confirmed_keys`/`confirmed_requests`/游标推进）。
  - 段提交与持久化：游标推进与检查点写入之间任何异常/取消都可重放该段（重解析+身份复核），保证幂等；
    写入失败按 FR-9 保留旧值、标记 dirty 并在后续段/PERSIST_STATE 重试。
  - 恢复时从 KVS 重建全局集合：内联请求 + 各未失效来源 `confirmed_keys` 按确定顺序回放构建；
    已完成来源直接以 `confirmed_requests` 重建序列。
- **Acceptance Criteria Addressed**: AC-1, AC-8, AC-10
- **Test Requirements**:
  - `rule` TR-4.1: 多来源完成顺序随机化（注入延迟/屏障）多次运行，最终 URL 序列逐位相同；跨来源/来源内重复 `unique_key` 仅保留确定顺序首次。
  - `rule` TR-4.2: 段已并入但检查点写入前注入 `CancelledError`，同代际重开后无重复无丢失；注入 `set_value` 失败后旧检查点仍可完整读取，重开结果正确。
  - `rule` TR-4.3: 全局集合重建（模拟只保留 KVS、清空内存）后，续传段中新记录去重裁决与首次运行口径一致。
  - `rubric` TR-4.4: 提交协议幂等严密性；scale 1-5；anchors 1=存在重复/丢失窗口，3=靠写入时序侥幸，5=“解码→裁决→并入→提交”单向状态转换且每步可复核；threshold >= 4；evidence 走查+取消点注入测试。

## Task 5: 生命周期接线（PERSIST_STATE、最终刷新、取消传播、无 name 路径）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 4
- **Description**:
  - 具名装载期间经 `service_locator.get_event_manager()` 注册 `Event.PERSIST_STATE` 监听：把各来源当前已确认边界
    （含 dirty 标记的最新游标）刷新到 KVS；`open()` 结束（成功/异常）路径注销监听并做一次最终刷新；
    刷新失败不吞用户原始异常。
  - `CancelledError` 传播语义：下载/合并任务被取消时不产生半提交；取消后重新 `open()` 走同一 `generation_id` 恢复。
  - 无 `name` 路径：使用 no-op 检查点存储、不注册监听；远程下载仍走同一流式/合并管线（内存态游标）。
  - 保持 `open()` 签名与 docstring 更新（说明恢复/续传/JSONL/去重行为）；`_fetch_requests_from_url` 等旧结构
    被新管线取代或收敛为薄封装；保持 `URL_NO_COMMAS_REGEX` 等公开符号不删除。
- **Acceptance Criteria Addressed**: AC-11, AC-12, AC-13, FR-10, FR-11, FR-12
- **Test Requirements**:
  - `rule` TR-5.1: 装载进行中派发 `Event.PERSIST_STATE`（本地事件管理器）后，KVS 记录反映最新 `raw_offset`；`open()` 返回后监听器已注销（再派发不产生写入）。
  - `rule` TR-5.2: 无 `name` 时不访问 KVS、不注册监听器；带 name 与不带 name 的最终请求序列一致。
  - `rule` TR-5.3: 自定义 `HttpClient`（spy）收到全部下载请求（含续传头），代理/客户端构造参数未被改动；来源 `headers`/`payload` 正确映射到产出 Request 且不随下载请求发送。
  - `rule` TR-5.4: 非 UTF-8 正文、单个小 URL、仅内联输入、空输入等既有场景测试保持通过。

## Task 6: 全量质量门禁与回归固化
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 5
- **Description**:
  - 更新/补充 `tests/unit/actor/test_request_list.py`：固定“全局首次胜出”去重新语义
    （原按集合断言的测试补充顺序/重复用例），补齐 AC 对应回归用例中未覆盖项。
  - 运行 `uv run poe lint`、`uv run poe type-check`、`uv run poe unit-tests`（至少 request_list 相关全量），修复全部问题。
- **Acceptance Criteria Addressed**: AC-1~AC-14（回归面）、NFR-3
- **Test Requirements**:
  - `rule` TR-6.1: `uv run poe lint` 退出码 0。
  - `rule` TR-6.2: `uv run poe type-check` 退出码 0。
  - `rule` TR-6.3: `uv run poe unit-tests` 全量通过（或在环境受限下 `tests/unit/actor/test_request_list.py` 与 request_loaders 相关测试全部通过并记录）。
  - `rubric` TR-6.4: 测试覆盖对 AC 矩阵的映射完整性；scale 1-5；anchors 1=关键恢复路径无测试，3=仅快乐路径，5=续传/重放/失效/取消/写入失败/编码/JSONL 均有确定性单测；threshold >= 4；evidence 测试清单逐条映射 AC。

## Completion Evidence

- **Task 1**: `_CheckpointStore` / `_SourceCheckpoint` / `_CheckpointManifest` / 身份哈希纯函数落地于
  [_remote_sources.py](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_remote_sources.py)；
  TR-1.1~1.4 由 `test_source_identities_are_stable_and_order_sensitive`、
  `test_checkpoint_model_camel_case_roundtrip`、`test_named_open_creates_manifest_and_reuses_generation`、
  `test_changed_source_set_discards_old_generation`、`test_unnamed_open_does_not_touch_storage` 覆盖，全部通过。
- **Task 2**: `_LineStreamParser`（头部两字节缓冲、zlib 增量 gzip/deflate、按字节换行切分、逐行 UTF-8 容错解码、
  JSONL/正则双解析、段有界刷新、前缀/跨度 SHA-256）；TR-2.1~2.5 由 `TestLineStreamParser` 全部用例覆盖
  （含逐字节喂入、多字节字符、JSONL 矩阵、非法对象 ValidationError）。
- **Task 3**: `_SourceRunner` 状态机（首下/206 续传/200 重放/416 收尾/内容变化重装/有界重试）；
  TR-3.1~3.6 由 `TestResume`、`TestRetriesAndFailures::test_interrupted_download_resumes_via_range` 覆盖
  （gzip 重放 `test_gzip_source_replays_after_interruption`、ETag/前缀篡改两路失效、416 与 in-process 重试经
  临时端到端脚本验证）。
- **Task 4**: plan 单次发布 + 串行来源合并 + 全局 `unique_key` 首次胜出 + 段提交 digest/span 记账；
  TR-4.1~4.4 由 `test_deterministic_order_regardless_of_completion`（3 轮打乱）、
  `test_global_unique_key_dedup_first_wins`、`test_checkpoint_save_failure_keeps_old_record` 覆盖。
- **Task 5**: `PERSIST_STATE` 注册/直接派发刷新/结束注销（`test_persist_state_flushes_mid_load_and_unregisters`）、
  真实外部 `task.cancel()` 后同代际恢复（`test_cancellation_resumes_same_generation`）、自定义客户端 Range 头
  （`test_custom_client_used_for_range_requests`）、无 name 纯内存（`test_unnamed_open_does_not_touch_storage`）。
- **Task 6**: 新增 26 个单元测试全部通过；既有 `test_request_list.py` 53 个测试零修改全部通过；
  `uv run poe lint` 通过；`uv run ty check src/apify/request_loaders tests/unit/actor/test_request_list_remote_sources.py`
  通过；`uv run pytest tests/unit --ignore=tests/unit/scrapy` 474 通过（唯一失败
  `test_actor_handles_migrating_event_correctly` 与 9 个 scrapy 收集错误为缺 python-socks/scrapy 可选依赖的
  预存环境问题，clean checkout 上同样失败）。

## Issue I-1: 416 窗口不检测实体缩短/校验符变化（F1, major, AC-4）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Description**: 416 分支无条件 finalize；须解析 `Content-Range: bytes */total`：total < raw_offset 或 ETag/Last-Modified 不一致→内容变化重装；信息不足（无 total 无校验符）→保守重装；仅 total==边界/校验符一致时才 finalize，且不得改动 raw_offset。补截断 416 回归测试。
- **Acceptance Criteria Addressed**: AC-4
- **Test Requirements**:
  - `rule` TR-I1.1: 种子检查点 raw_offset>B1，服务器对 Range 回 416+`bytes */<短长度>`+新 ETag，输出为新实体内容（完整 200 重装），旧记录被新检查点取代。

## Issue I-2: wire_spans 重放校验从 0 起算导致误判（F2, major, AC-5）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Description**: 重放校验目标改为完整连续覆盖（prefix_sha256 合成 [0,first.start) + 各 wire_spans）；span hasher 仅在 [start,end) 区间累加并按行边界比对。补“206 续传 span → 200 重放仅 1 次 GET 且检查点存活”端到端用例。
- **Acceptance Criteria Addressed**: AC-5, AC-U1, AC-U2
- **Test Requirements**:
  - `rule` TR-I2.1: 种子 cp 含 prefix(0,B1)+span(B1,B2) 与 20 行 confirmed keys，恒 200 服务器：仅 1 次 GET、输出完整 60 行、检查点未被删除（重放后 completed=True 且仍为同记录）。

## Issue I-3: minor 收口（F3/F4/F6 + F7 测试强度）
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: I-1, I-2
- **Description**: F3 `_RetryableDownloadError` 纳入重试元组并补 5xx 重试测试；F4 长度缩短判定仅限 identity；F6 runner 跨重试保留 force-persist 标志、open finally best-effort 刷新所有已提交检查点；F7 补强 AC-3（续传字节数）、AC-9（持续失败）、AC-10（同 generation 断言）、AC-11（中途 rawOffset 快照）、AC-14（payload/headers/userData 字段级）、AC-7（JSONL 端到端恢复）断言。
- **Acceptance Criteria Addressed**: AC-3, AC-7, AC-9, AC-10, AC-11, AC-14, AC-U2
- **Test Requirements**:
  - `rule` TR-I3.1: 500 两次后 200 → open 成功；持续 500 → 抛出且旧检查点记录保留。
  - `rule` TR-I3.2: `_response_changed` 单测：压缩头+小 Content-Length 不判变；identity+小长度判变。
  - `rule` TR-I3.3: 取消后重开 generation_id 与取消前一致；事件中途刷新读到 completed=False 且 rawOffset>0；206 实际投递字节<总长度；completed 重建 payload/headers/userData 字段一致；JSONL 来源 200 重放完整。

## Remediation Round 1 (after R1 fail) - Completion Evidence

- **I-1 (F1)**: `_CONTENT_RANGE_REGEX` 现支持 `bytes */total`；416 分支经
  `_unsatisfied_range_means_changed`（total<raw_offset 或校验符变化→重装；信息不足→保守重装；仅边界恰好
  到尾且校验一致才 finalize）。回归：`test_shrunk_entity_returning_416_triggers_reload`（新实体全量重装、
  新 ETag 落盘）。
- **I-2 (F2)**: replay 校验目标改为 `_replay_spans` 合成的 [0,content_offset) 完整连续覆盖
  （prefix_sha256 合成首段 + 各 wire_spans）；`_consume_span_bytes` 严格按 [start,end) 区间逐行哈希，
  每行至多关闭一个 span（修复了关闭行被重复计入下一 span 的实现缺陷）。回归：
  `test_wire_spans_replay_verifies_with_single_full_get`（未变内容仅 1 次 GET、completed 落盘）。
- **I-3 (F3/F4/F6/F7)**: `_RetryableDownloadError` 纳入重试元组（`test_transient_5xx_is_retried`、
  `test_persistent_5xx_fails_and_keeps_checkpoint`）；长度缩短判定限 identity
  （`test_response_changed_ignores_compressed_content_length`）；force-persist 标志跨重试保留、
  finally best-effort 刷新未完成检查点；补强断言：206 实际投递字节<总大小、取消后同 generation、
  PERSIST_STATE 中途 rawOffset/completed=False 快照、completed 重建 payload/headers/userData 字段级、
  JSONL 端到端重放（`test_jsonl_source_replays_from_confirmed_prefix`）。
- 验证：目标文件 lint/ty 全绿；新测试套件 32 个全绿；`uv run pytest tests/unit --ignore=tests/unit/scrapy`
  **480 passed, 1 failed**（唯一失败为预存 python-socks 环境问题）。

## Remediation Round 2 (after R2 pass, advisory minors) - Completion Evidence

- **R2-N1**: 416 total 判定从 `< raw_offset` 收紧为 `!= raw_offset`（非合规超长 total 不再误 finalize）；
  回归 `test_416_with_total_beyond_boundary_is_not_finalized`。
- **R2-N2**: 压缩检查点遇到 416 不再 finalize，改为无 Range 全量重放并复核；
  回归 `test_416_for_compressed_checkpoint_replays_full_entity`。
- 补齐精确到尾 identity 416 finalize 永久用例 `test_416_at_exact_entity_end_finalizes_identity_source`
  （R3 仅有 /tmp 探针覆盖）。
- 最终验证：lint 通过、定向 ty 通过；相关测试 **88 passed**（53 既有 + 35 新增）；
  全量 `tests/unit --ignore=tests/unit/scrapy` **482 passed**（唯一失败为预存 python-socks 环境问题，
  clean checkout 同样失败）。R3 独立 delta 复审 **pass**。
