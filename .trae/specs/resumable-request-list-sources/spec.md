# ApifyRequestList 远程来源可恢复装载 - 产品需求文档

## Overview

- **Summary**: 为 `ApifyRequestList` 的远程 URL 来源（`requestsFromUrl`）引入分代、分来源的检查点机制：流式下载与解析、按完整记录边界推进检查点、支持 HTTP 范围续传与内容变化失效、确定性合并与全局去重，并在 migration / reboot / 事件循环取消后可幂等恢复。
- **Purpose**: 消除大响应在解析中断或 Actor 重启后从头重复下载、已加入请求与其他来源再次合并导致的带宽浪费以及顺序/去重漂移。
- **Target Users**: 使用标准 `requestListSources` Actor 输入（含一个或多个 `requestsFromUrl` 远程来源，且来源可能很大）构建 `ApifyRequestList` 的 Actor 开发者。

## Goals

- 每个远程来源具备稳定身份、响应验证信息与已确认解析边界，且检查点持久化到默认 KeyValueStore。
- 仅当一段记录完整解码、经全局去重裁决并进入最终请求序列后，才推进该来源的检查点。
- 支持范围请求的服务器从确认边界续传；内容变化时废弃旧检查点整体重新装载；不支持续传时从头验证并按稳定请求身份跳过已确认部分。
- 压缩流（gzip/deflate）与逐行 JSON（JSONL）的记录边界可重启恢复。
- 并发下载的完成顺序不改变最终确定顺序；跨来源全局按 `Request.unique_key` 去重，首次出现胜出。
- migration、reboot、事件循环取消后，重新 `open()` 继续同一来源代际；已完成的来源不再重新下载。
- 保持现有输入校验、HTTP 客户端注入（代理/认证）、用户数据处理与 `open()` 签名兼容。

## Non-Goals

- 不改变 Crawlee `RequestList` 自身的进度状态（`nextIndex` / `nextUniqueKey` / `inProgress`）语义与 RequestList 状态键。
- 不支持 brotli（`br`）内容编码、ZIP/多成员归档等非流式格式。
- 不支持多写者并发共享同一检查点（假设同一具名 RequestList 在同一 Actor Run 内单写者）。
- 不新增每来源的代理/认证配置入口；代理与认证继续通过调用方注入的 `http_client` 提供。
- 不改变远程请求的方法（始终 GET）与现有“不携带来源条目 headers 发起下载”的行为。

## Background & Context

- 现状实现：[_apify_request_list.py](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/request_loaders/_apify_request_list.py) 在 `open()` 中用 `asyncio.TaskGroup` 并发 `send_request(GET).read()` 下载全部远程来源、对整个解码后正文跑 `URL_NO_COMMAS_REGEX`，然后以 `简单 URL 输入 + 远程来源（按输入顺序）` 拼成内存 list 交给 Crawlee `RequestList`。
- 现状不去重：重复 URL（相同 `unique_key`）会在结果序列中出现多次（已实测确认）。Crawlee `RequestLoader._transform_requests` 已提供“按 `unique_key` 首次出现胜出”的辅助但当前未被使用。
- Crawlee `RecoverableState`（[recoverable_state.py](file:///Users/ding/Documents/swe/0910092/project-10/.venv/lib/python3.14/site-packages/crawlee/_utils/recoverable_state.py)）示范了默认 KVS 持久化与 `Event.PERSIST_STATE` 监听模式；具名 `RequestList` 的进度键为 `SDK_REQUEST_LIST_STATE-{name}`。
- `HttpClient.stream()` 返回支持 `read_stream()` 分块读取的响应（[_base.py](file:///Users/ding/Documents/swe/0910092/project-10/.venv/lib/python3.14/site-packages/crawlee/http_clients/_base.py)），`send_request`/`stream` 均接受自定义请求头，可发送 `Range` / `If-Range`。
- Actor reboot 与 migration 前会等待 `PERSIST_STATE` 与 `MIGRATING` 监听器完成（[_actor.py](file:///Users/ding/Documents/swe/0910092/project-10/src/apify/_actor.py#L1354-L1380)）；重启后用户代码重新执行，即重新调用 `ApifyRequestList.open()`，默认 KVS 内容跨重启保留。

## Functional Requirements

- **FR-1（稳定来源身份与代际）**：每个远程来源具有跨进程稳定的 `source_id`，由其在输入中的索引与规范化条目内容（`requestsFromUrl`、`method`、`payload`、`headers`、`userData`）哈希派生。所有来源共享一个 `generation_id`，并对整个来源集合（顺序敏感）计算 `source_set_hash`。重新 `open()` 时：集合哈希一致则复用同一代际与各来源检查点；不一致（增删/改序/改内容）则废弃全部旧检查点、以新一代际全新装载。
- **FR-2（响应验证信息）**：每个来源检查点至少记录：`source_id`、`generation_id`、来源 URL、首次响应采集的 `ETag` 与 `Last-Modified`（有则记）、实体总长度（可知时）、内容编码、已确认原始字节边界 `raw_offset`、`[0, raw_offset)` 前缀的 SHA-256、按解析顺序排列的已确认请求身份（`unique_key`）及其完整可重建 Request 数据、完成标志。
- **FR-3（流式解码与记录边界）**：远程下载使用 `http_client.stream()` 分块读取，禁止一次性缓冲整个响应：
  - 内容编码为 gzip/deflate 且字节确为压缩流时，使用标准库增量解压器（`zlib.decompressobj`）逐块解压；identity/客户端已透明解压时按透传字节处理。
  - 使用 UTF-8 增量解码器跨块解码；半个多字节字符跨块不产生乱码或请求（沿用 `errors='replace'` 容错）。
  - 以完整行作为记录确认边界：尾部无换行的半行保持缓冲，不产生请求、不推进边界；流正常结束时冲洗最后一行。
  - 每个完整行：优先按 JSONL 解析（见 FR-4），否则对该行执行现有 `URL_NO_COMMAS_REGEX` 提取；按行扫描与现行全文扫描对 URL 的提取结果保持一致（URL 不含裸换行）。
- **FR-4（JSONL 记录）**：逐行 JSON 中，一行可以是（a）JSON 字符串（其值作为 URL），或（b）JSON 对象，必填 `url`，可选 `method`、`payload`、`headers`、`userData`（命名与 `requestListSources` 条目一致，camelCase）。行级字段对该请求整体覆盖来源级默认值；请求构造仍经现有 Pydantic 校验与 `userData` 深拷贝语义。无法解析为 JSON 或对象无 `url` 的行回退到正则提取。
- **FR-5（段提交协议）**：来源以“段”（一批完整行解析出的请求）为单位向合并器交账。仅当该段全部记录完成解码与请求构造、经全局去重集合裁决、并入最终请求序列之后，才更新该来源内存游标并计划持久化检查点；持久化以完整 JSON 记录原子写入。解析/裁决完成与检查点落盘之间发生崩溃时，按 FR-8 的身份复核保证幂等。
- **FR-6（确定性合并与全局去重）**：下载可并发，但最终合并严格按“内联 `url` 条目（输入顺序）→ 远程来源（输入索引顺序）→ 来源内解析顺序”进行；远程任务完成先后不得改变结果。全局去重集合按该确定顺序构建，`unique_key` 首次出现胜出（身份计算沿用 `Request.from_url` 现行规则）。
- **FR-7（范围续传）**：存在未完成来源检查点时，恢复请求发送 `Range: bytes={raw_offset}-` 与 `If-Range: <ETag|Last-Modified>`（有校验符时）：
  - `206` 且 `Content-Range` 与边界一致、内容为 identity：从边界续传，增量解码/解析，边界后请求正常并入。
  - `206` 但内容为 gzip/deflate（无法从压缩字节中途解压）：放弃续传，转入 FR-8 的从头重放路径。
  - `200`、`If-Range` 校验失败、`ETag`/`Last-Modified` 变化、前缀 SHA-256 不符或声明总长度小于边界：判定内容变化，废弃该来源旧检查点（及其已确认请求贡献），从零重新装载并以新内容重新参与合并。
- **FR-8（无续传能力时的从头验证与跳过）**：服务器忽略 `Range`（返回 `200` 完整实体）但验证信息表明内容未变化时，从头流式下载：对 `[0, raw_offset)` 重算 SHA-256 与检查点比对；重放解析出的请求按已确认身份有序复核并跳过（不重复并入）；越过边界后按正常路径并入。前缀哈希不符按 FR-7 内容变化处理。
- **FR-9（失败与取消语义）**：
  - 下载响应中断（连接错误/超时/流中途异常）：保留最近一次成功落盘的检查点，对同一来源进行有界自动重试，重试时按 FR-7/FR-8 续传或重放；重试耗尽才向调用方抛出。
  - 检查点保存失败（KVS 写入异常）：旧检查点保持有效且绝不被半写破坏；内存中可继续处理，在下一段或下一次持久化时机重试，日志告警。
  - 请求已并入全局序列但来源游标尚未推进（崩溃/取消发生在两者之间）：恢复时重新解析该段，以稳定请求身份（`unique_key` + 来源内顺序）复核，已存在则不重复并入，未确认者补入。
  - `asyncio.CancelledError`：不允许出现半提交状态（游标推进先于或原子于记录写入），取消后以同一代际恢复，行为与崩溃一致。
- **FR-10（生命周期一致性）**：启用持久化时注册 `Event.PERSIST_STATE` 监听器，在 migration、reboot 前的持久化事件中把所有来源最新已确认边界刷新到 KVS；`open()` 结束（成功/失败）时做一次最终刷新并注销监听器。进程重启后的 `open()` 从 KVS 恢复同一 `generation_id`：已完成来源直接由检查点重建 Request，不发起任何 HTTP 请求；未完成来源按 FR-7/FR-8 续传或重放。
- **FR-11（兼容性）**：
  - `open()` 签名（`name`、`request_list_sources_input`、`http_client`）与返回类型不变。
  - 内联 `url` 条目仍在最前，Pydantic 输入校验（含任意 JSON `userData`）与 `payload`/`headers` 映射不变。
  - 调用方注入的 `http_client`（含其代理/认证/impersonation 配置）继续被唯一使用；续传仅额外叠加 `Range`/`If-Range`/`Accept-Encoding` 等请求头，不替换客户端、不改变跟随重定向等行为。
  - 非 UTF-8 响应体容错（`errors='replace'`）保持；远程来源 `userData` 深拷贝、请求间不别名保持。
  - 单个小 URL、内联 requests、无远程来源等场景行为与输出保持可用且结果等价。
- **FR-12（无名称即无持久化）**：未提供 `name` 时不打开 KVS、不读写检查点、不注册事件监听器；处理路径与具名场景共用同一套流式/合并/去重逻辑，输出结果一致。

## Non-Functional Requirements

- **NFR-1（内存有界）**：任意时刻只保留分块缓冲、至多一行残留与待合并段，不随响应总大小线性增长。
- **NFR-2（无新增重依赖）**：压缩（`zlib`）、哈希（`hashlib`）、JSON 等使用标准库；不引入新的必需第三方依赖。
- **NFR-3（质量门禁）**：`uv run poe lint`、`uv run poe type-check` 与相关单元测试通过；新逻辑具备确定性、可注入假 HTTP 服务器的单元测试覆盖。
- **NFR-4（可观测）**：续传、重放、检查点失效与写入失败等关键路径有 logger 记录。

## Constraints

- **Technical**: 必须基于 Crawlee `HttpClient` 抽象（`stream()`/`send_request()`、`HttpResponse.status_code`/`headers`/`read_stream()`）；持久化经默认 `KeyValueStore`（平台记录大小有限，约 9MB/记录）。检查点采用“清单记录 + 每来源独立记录”的键布局，单来源记录过大无法写入时，该来源降级为 FR-8 重放路径（不阻断整体装载）。Python 3.11–3.14，Ruff 行宽 120、单引号、docstring 双引号，模型遵循仓库 Pydantic 别名约定（见 AGENTS.md）。
- **Business**: 不破坏既有 `requestListSources` 用户；去重口径变更（跨来源重复 URL 仅保留首次）已经需求确认，作为有意的行为修复。
- **Dependencies**: `crawlee`（`RequestList`、`KeyValueStore`、事件系统、`Request`）、`apify-client` 模型；无新增依赖。

## Assumptions

- 恢复语义只针对“装载阶段”产出的确定请求序列； Crawlee `RequestList` 已有的 `next_unique_key` 一致性校验在内容未变化时继续成立，内容变化导致的旧进度失配属于既有行为（本特性通过不改变未变化内容的确定序列来避免该失配）。
- 来源条目 `headers` 现行不随下载请求发送，续传请求同样不发送，仅用于生成的爬取 Request；Range 等恢复头由装载器自行添加。
- JSONL 行级字段采用整体覆盖（非深合并）来源级默认值；空行忽略。
- 平台 KVS 单次 `set_value` 对调用方表现为原子生效：成功则新值可读，失败则旧值保留。
- 注入的 HTTP 客户端可能已透明解压 `Content-Encoding`；实现以“响应头声明 + 字节魔数”双重判断是否需要自行增量解压，偏移量以装载器实际消费的实体字节为准。

## Acceptance Criteria

### AC-1: 确定性合并与全局去重
- **Type**: `rule`
- **Given**: 输入含多个内联 `url` 与多个 `requestsFromUrl` 来源，且来源间/来源内存在相同 `unique_key` 的 URL，并人为让各远程来源以任意顺序、不同延迟完成。
- **When**: 调用 `ApifyRequestList.open(name=...)` 并耗尽 `fetch_next_request()`。
- **Then**: 输出序列为“内联条目输入顺序 → 远程来源输入索引顺序 → 来源内顺序”，同一 `unique_key` 仅保留确定顺序下的首次出现。
- **Pass Condition**: 多次运行（含打乱完成顺序）输出的 URL 序列完全相同；重复键仅保留首次。
- **Evidence**: 新增单元测试（httpserver + 可控延迟）断言完整有序序列。

### AC-2: 稳定来源身份与来源集代际
- **Type**: `rule`
- **Given**: 具名 `ApifyRequestList` 首次成功装载后 KVS 中存在清单与每来源检查点记录。
- **When**: 以完全相同的输入再次 `open()`。
- **Then**: 复用同一 `generation_id` 与全部来源检查点，已完成来源不发起任何 HTTP 请求，重建出的请求序列与首次完全一致；改变来源顺序、URL 或任一条目字段后再 `open()`，则生成新代际、旧检查点被废弃并全部重新下载。
- **Pass Condition**: httpserver 断言第二次打开对已完成来源零请求；改动输入后断言旧 KVS 记录被清理且发生全量下载。
- **Evidence**: 单元测试断言 HTTP 命中次数、KVS 键内容与输出序列。

### AC-3: 支持范围请求时从确认边界续传
- **Type**: `rule`
- **Given**: 远程来源服务器支持 `Range`/`Accept-Ranges` 并提供 `ETag`；首次装载在若干完整行边界之后中断（连接错误）。
- **When**: 重试或重新 `open()` 恢复该来源。
- **Then**: 恢复请求携带 `Range: bytes=N-`（N 为已确认完整行边界）与 `If-Range`，服务器返回 `206`/`Content-Range` 后仅下载并解析后缀；最终请求序列与一次性成功装载完全一致（无重复、无丢失）。
- **Pass Condition**: 断言请求头、`206` 路径、续传字节数 < 总大小，且输出序列与对照一致。
- **Evidence**: 单元测试（httpserver 支持 Range，分块/断连后重开）。

### AC-4: 内容变化时废弃旧检查点重新装载
- **Type**: `rule`
- **Given**: 来源存在已确认检查点，恢复时实体内容已变化（ETag 变化致 `If-Range` 失败返回 `200`，或前缀 SHA-256 不符，或总长度缩短）。
- **When**: 重新 `open()`。
- **Then**: 该来源旧检查点与已确认请求贡献被整体废弃，按新内容从零装载；最终序列只反映新内容，且不与其他来源的确定顺序相互污染。
- **Pass Condition**: 输出以新内容为准；KVS 中该来源检查点被新代际/新边界取代。
- **Evidence**: 单元测试分别覆盖校验符变化与前缀哈希不符两条触发路径。

### AC-5: 服务器不支持续传时从头验证并跳过已确认请求
- **Type**: `rule`
- **Given**: 来源已有检查点，但服务器对带 `Range` 的请求回 `200` 完整实体，内容未变化。
- **When**: 重新 `open()`。
- **Then**: 从头流式下载，重算前缀 SHA-256 与检查点一致；边界之前的请求按已确认身份有序跳过，边界之后正常并入。
- **Pass Condition**: 最终序列与无中断情形完全一致、无重复；前缀确实验证（篡改前缀即转为 AC-4 路径）。
- **Evidence**: 单元测试断言跳过逻辑与最终序列。

### AC-6: 压缩流的可恢复边界
- **Type**: `rule`
- **Given**: 来源分别以 gzip、deflate 编码返回多行 URL（含跨块的多字节字符），在分块解析中途中断。
- **When**: 恢复（续传不可行时自动走从头重放）。
- **Then**: 增量解压/解码不产出半行或半字符请求；恢复后请求集合、顺序与 identity 编码一次性装载一致。
- **Pass Condition**: gzip/deflate 两种编码下输出与 identity 对照一致；中途检查点 `raw_offset` 之后不携带半个多字节字符产生的请求。
- **Evidence**: 单元测试（压缩分块 + 中断恢复）。

### AC-7: JSONL 解析与回退兼容
- **Type**: `rule`
- **Given**: 远程内容为逐行 JSON，混合包含：URL 字符串行、含 `url`+`method`/`payload`/`headers`/`userData` 的对象行、坏 JSON 行、无 `url` 对象行、普通文本行。
- **When**: 装载并在任意行边界中断后恢复。
- **Then**: 字符串行与对象行按 FR-4 构造请求（行级字段覆盖来源默认，经校验与深拷贝）；坏/无 `url` 行与普通文本行回退正则提取；恢复后序列与一次性装载一致。
- **Pass Condition**: 各类行的产出符合预期；非法对象产生校验错误时行为与现行内联校验错误策略一致（Pydantic 报错或按既有容错）；中断恢复无重复/丢失。
- **Evidence**: 单元测试覆盖行类型矩阵与恢复。

### AC-8: 检查点保存失败保留旧检查点
- **Type**: `rule`
- **Given**: 检查点 KVS 写入在某次推进时抛异常。
- **When**: 装载继续后中断并重新 `open()`。
- **Then**: KVS 中保留最近一次成功写入的旧检查点（无半写/损坏记录），恢复从旧边界经身份复核后得到正确序列。
- **Pass Condition**: 注入 `set_value` 失败后，再次 `open()` 输出与一次成功一致；旧记录 JSON 始终可解析。
- **Evidence**: 单元测试 mock KVS 写入失败。

### AC-9: 下载中断有界重试
- **Type**: `rule`
- **Given**: 来源流式下载过程中出现连接错误/超时，随后恢复正常。
- **When**: `open()` 执行中。
- **Then**: 装载器在有界次数内自动重试同一来源（续传或重放）并成功完成；超出次数才向上抛出异常，且已成功提交的检查点不回滚。
- **Pass Condition**: 前几次流失败、最终成功时 `open()` 成功且结果正确；持续失败时抛出且 KVS 保留旧检查点。
- **Evidence**: 单元测试（前 N 次失败的服务器/客户端 mock）。

### AC-10: 并入后游标未推进时按身份复核
- **Type**: `rule`
- **Given**: 某段请求已经全局去重并入最终序列，但在来源检查点游标推进/持久化前收到 `CancelledError` 或进程崩溃。
- **When**: 同代际重新 `open()` 恢复。
- **Then**: 该段被重新解析，以 `unique_key` 与来源内顺序复核：已并入者不重复加入，缺失者补入；最终序列与无中断一致。
- **Pass Condition**: 在“并入后、推进前”窗口注入取消，恢复后无重复无丢失。
- **Evidence**: 单元测试注入取消点。

### AC-11: migration/reboot 持久化事件刷新检查点
- **Type**: `rule`
- **Given**: 具名 RequestList 正在装载（或已完成装载）。
- **When**: 触发 `Event.PERSIST_STATE`（含 `is_migrating=True`，对应 migration/reboot 前流程）。
- **Then**: 各来源最新已确认边界被刷新到 KVS；重新 `open()` 后从该边界继续同一 `generation_id`。
- **Pass Condition**: 事件后 KVS 记录包含最新 `raw_offset`/已确认身份；监听器在 `open()` 结束后注销。
- **Evidence**: 单元测试经本地事件管理器派发事件并检查 KVS。

### AC-12: 无名称时纯内存且现有行为不回退
- **Type**: `rule`
- **Given**: 不带 `name` 调用 `open()`，覆盖现有测试中的全部输入形态（内联各 HTTP 方法/可选字段、单个小 URL、非 UTF-8、多来源）。
- **When**: 调用 `open()`。
- **Then**: 不发生任何 KVS 访问、不注册事件监听器；除“全局去重首次胜出”这一有意变更外，输出与现行实现等价；现有测试套件在必要的最小断言更新后全部通过。
- **Pass Condition**: 无 KVS 交互证据（spy/mock 断言）；`tests/unit/actor/test_request_list.py` 全绿。
- **Evidence**: 既有测试 + 新增无持久化断言。

### AC-13: HTTP 客户端注入与数据处理兼容
- **Type**: `rule`
- **Given**: 调用方注入自定义 `HttpClient`（代理/认证/impersonation 配置在其上）与含 `headers`/`payload`/`userData` 的来源条目。
- **When**: 首次装载与恢复装载。
- **Then**: 所有下载（含续传）均经该客户端完成，装载器不替换客户端、不改其配置；生成 Request 的 method/payload/headers/userData 与现行映射一致，`userData` 深拷贝互不别名；非 UTF-8 正文容错保持。
- **Pass Condition**: 自定义客户端记录到全部请求（含 `Range` 头）；请求属性与拷贝隔离断言通过。
- **Evidence**: 单元测试（mock/spy 客户端 + 既有 userData 测试保持通过）。

### AC-14: 已完成来源跨重启零下载并完整重建
- **Type**: `rule`
- **Given**: 首次装载全部成功，检查点标记来源完成（含完整 Request 数据）；模拟进程重启（清空内存态，保留 KVS）。
- **When**: 再次 `open()` 相同输入。
- **Then**: 远程来源零 HTTP 请求，重建 Request 的 url/method/payload/headers/userData 与首次产出逐字段一致，最终序列与首次完全相同。
- **Pass Condition**: httpserver 零命中；字段级对比一致。
- **Evidence**: 单元测试。

### AC-U1: 恢复正确性与幂等设计质量
- **Type**: `rubric`
- **Dimension**: 各中断/失效/取消窗口下状态机的严密性（段提交、身份复核、前缀验证、代际失效的闭环程度）
- **Scale**: 1-5
- **Anchors**: 1 = 存在可构造的重复/丢失/脏检查点路径；3 = 主路径正确但边界窗口依赖时序巧合；5 = 所有声明窗口均有显式状态转换与幂等复核，且有测试证据
- **Pass Threshold**: >= 4
- **Evidence**: 代码走查 + AC-3~AC-10 测试矩阵。

### AC-U2: 流式资源效率
- **Type**: `rubric`
- **Dimension**: 内存占用与带宽节省是否达成（分块处理、有界缓冲；未变化时已完成/已确认字节不重复下载）
- **Scale**: 1-5
- **Anchors**: 1 = 仍整包缓冲或整包重下；3 = 流式处理但恢复路径恒从头下载；5 = identity 续传精确到行边界、完成来源零下载，仅压缩/无 Range 服务器回退重放
- **Pass Threshold**: >= 4
- **Evidence**: 缓冲上限与 HTTP 字节计数测试。

### AC-U3: 与仓库架构和约定的一致性
- **Type**: `rubric`
- **Dimension**: 模型/别名、KVS 与事件用法、日志、文档字符串、代码组织与 AGENTS.md 约定的契合度
- **Scale**: 1-5
- **Anchors**: 1 = 引入新依赖或绕开既有抽象；3 = 功能可用但风格游离；5 = 复用 Crawlee KVS/事件模式与 Pydantic 约定，lint/type-check 全绿
- **Pass Threshold**: >= 4
- **Evidence**: lint/type-check 输出与代码走查。

## Open Questions

- 无（全局去重口径、JSONL/gzip 范围、持久化启用方式已经需求确认；brotli 与每来源认证明确列为 Non-Goals）。
