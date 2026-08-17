# streamd 崩溃恢复协议

| 属性 | 内容 |
| --- | --- |
| 状态 | Draft / V1 实现基线 |
| 依赖 | [V1 存储格式](storage-format.md)、[Append 与提交协议](append-commit-protocol.md)、[主备复制协议](replication-protocol.md) |
| 原则 | 只接受唯一可证明的历史；索引可重建，Record 不猜测 |

## 1. 恢复目标

恢复必须在进程崩溃、掉电、文件系统短写和发布中断后得到唯一结果：

- 所有已确认写入仍然存在；
- 未确认写入可以存在或不存在，但不能损坏已确认前缀；
- Sequence、Byte Offset、Entry ID 和 Recorded At 连续性成立；
- 不把 Staging、Orphan 或未发布文件误认为事实；
- 无法证明完整性时失败关闭，不跳过坏 Record。

## 2. 恢复模式

```text
BOOTSTRAP_EMPTY       新数据目录
NORMAL_LOCAL          单节点正常启动
PRIMARY_RESTART       原 Primary 重启并与 Standby 对账
STANDBY_RESTART       Standby 本地恢复后追赶
SNAPSHOT_INSTALL      从可安装 Snapshot 恢复
OFFLINE_REPAIR        人工授权的只读检查/恢复工具
```

普通服务进程不能自动进入 OFFLINE_REPAIR。

## 3. 事实优先级

从高到低：

1. 当前合法 Term/Fencing 状态；
2. CURRENT 指向并校验通过的 Manifest；
3. Manifest 引用且校验通过的 Segment；
4. Manifest Checkpoint 之后的连续合法 WAL；
5. 可从上述事实重建的 Registry、Tail、Index、Locator 和 Cache；
6. 未被 CURRENT/Manifest/Snapshot 引用的文件仅是候选 Orphan。

Replication State Checkpoint 提供恢复起点和下界，不能覆盖 WAL/Manifest 的相反证据。

## 4. 启动前安全检查

1. 获取数据目录独占进程锁；
2. 读取 NODE，验证 Cluster/Group/Node ID；
3. 检查文件系统为可写、支持 `fsync` 和原子 rename；
4. 禁止跟随数据根之外的符号链接；
5. 读取磁盘水位，低于启动安全空间时只读启动；
6. 加载 Binary 支持的格式 Capability Matrix；
7. HA 节点在恢复完成前不获取写 Lease。

NODE 缺失但目录非空时不能自动初始化。

## 5. Manifest 恢复

### 5.1 CURRENT 有效

验证 CURRENT CRC、Manifest Name、Generation、File ID 和 SHA-256，然后完整解析 Manifest。

### 5.2 CURRENT 损坏或缺失

只允许按以下规则回退：

1. 扫描 Manifest 候选文件名；
2. 对每个候选完成 Header、Footer、SHA 和 Previous Manifest Chain 校验；
3. 找出唯一最高、链连续、所有必需 Segment 可用的 Generation；
4. 如果最高合法历史不唯一，失败关闭；
5. 人工或恢复工具确认后重建 CURRENT。

普通启动可以自动选择 CURRENT 明确指向的前一 Generation，但不能跨过一个已发布且可能包含唯一数据的损坏 Generation。

### 5.3 Segment 验证

启动只执行 Open Check 和 Directory Check，不 mmap/打开全部 Payload。每个 Manifest Reference 必须满足：

- 文件或对象位置至少一个可用；
- Segment ID、大小、Content SHA 元数据一致；
- Header/Footer 和 Section Range 合法；
- Directory 的 Stream 范围不自相矛盾。

本地 Segment 缺失但对象副本完整时，可以先进入 `RECOVERING_SEGMENT` 下载，不能返回该范围的读取结果。

## 6. WAL 发现与排序

1. 读取 WAL-CURRENT；
2. 扫描 `wal/` 候选，只相信 Header 中 File ID 和 First Entry ID；
3. 校验所有 Sealed WAL Footer/SHA；
4. 从 Manifest `last_entry_id + 1` 找到唯一连续文件链；
5. 校验跨文件 `previous_entry_crc32c`；
6. 只允许链尾的 WAL 为 Active/Unsealed。

同一 First Entry 出现两个内容不同的合法文件时视为日志分叉，失败关闭。

## 7. Active WAL 尾部处理

顺序扫描：

```text
position = WAL_HEADER_LENGTH
last_good = position

while position < file_size:
    validate minimum header
    validate entry_length bounds
    validate header CRC
    validate full frame and entry CRC
    validate Entry/Stream continuity
    last_good = position + entry_length
    position = last_good
```

处理规则：

- EOF 落在最后一条 Header/Frame/CRC 中间：截断到 `last_good`；
- 最后一条长度完整但 CRC 错误：只有能证明它是未完成尾写时才截断，否则失败关闭；
- 中间 Entry 损坏：失败关闭；
- Sealed WAL 任意损坏：失败关闭；
- 截断后执行 `fsync`，记录原大小、新大小和最后 Entry ID。

恢复扫描不得依据损坏 Length 分配内存。

## 8. 连续性校验

每条 WAL Entry 必须验证：

- Entry ID 等于前一 Entry ID 加一；
- Previous CRC 匹配；
- Envelope 与 Record Frame 重复字段相同；
- Stream Sequence 等于恢复中的 Tentative Tail；
- Byte Offset 等于上一 Frame 末尾；
- Recorded At 不小于该 Stream 前值；
- Batch Index/Count 连续、Request ID/Hash 相同；
- Registry Record 先于对应用户 Stream 第一条 Record。

任何不连续都不是“可跳过坏日志”，而是历史冲突。

## 9. Commit 边界恢复

### 9.1 已发布 Segment

Manifest Checkpoint 及以前全部视为 Committed/Applied。

### 9.2 SINGLE_SYNC

Checkpoint 之后 Active WAL 中完整、连续、CRC 正确的 Entry 视为 durable suffix。V1 保留并提交该 suffix，包括可能未向客户端响应的 Record。

### 9.3 REPLICATED_STRICT Primary Restart

Primary 不单独决定尾部：

1. 恢复本地 durable suffix；
2. 以 RECOVERING 身份连接 Standby；
3. 比较 Entry ID 和 CRC 前缀；
4. 保留双方一致的连续 durable 前缀；
5. 只有取得当前 Term/Lease 后才能重新开放写入。

若 Standby 不可用，节点可以只读启动，但不能把无法证明已复制的尾部响应为 Strict 成功。

### 9.4 Standby Promotion

按复制协议保留本地合法 durable suffix。由于客户端可能丢失响应，该 suffix 可以在新 Primary 上变为可见。

### 9.5 Batch 尾部

- 完整 Batch：按模式决定保留；
- WAL 只包含 Batch 前缀：截断整个 Batch，包括已完整编码的前几条；
- Batch 字段不一致：失败关闭；
- 已发布 Segment 中出现部分 Batch：格式/发布协议故障，失败关闭。

## 10. 投影重建

### 10.1 Stream Registry

加载 Registry Snapshot Header 与 Sparse Block Index，不遍历全部 Registry Entry；使用 Registry
Stream Tail 的 Record Count 校验 Snapshot Entry Count，并以 `entry_count + 1` 作为下一分配 ID。
Checkpoint 之后的 Registry Stream Record 重放到内存 Overlay。名称查询先查 Overlay，再按需读取
Snapshot Block；Block 长度、Entry CRC 或 Key 顺序损坏时，从当前 Manifest 的 Registry Stream
Segment 惰性重建完整事实映射。Snapshot Header/Index 无效则启动时直接使用相同事实重建路径。

### 10.2 Tail Catalog

加载 Sealed Tail Checkpoint；Slot CRC/Generation 无效的条目按缺失处理。使用 Segment Directory 和后续 WAL 修复到 `applied_entry_id`。

### 10.3 Locator

加载并校验 Locator Snapshot；Sealed Pack 不在启动时整体读取，而是在 Page Cache Miss 时按需校验
Pack Header/Footer 和目标 Page CRC。Snapshot 缺失或损坏时不安装 Locator；Pack/Page 在运行时
缺失或损坏时回退当前 Manifest 的 Segment Descriptor。当前 V1 Runtime 不在启动阶段立即重写
Locator，下一次 Checkpoint/Merge 会从 Segment Directory 发布新的 Pack、Snapshot、Root 和 Tail
Pointer。

### 10.4 Unified Index

Segment Index 必须与 Frame 校验一致。Active/Immutable MemTable Index 从 WAL 重放生成。

### 10.5 Cache

Hot Stream、Page、Handle 和 Payload Cache 全部从空状态启动，不能从未经校验的进程 dump 恢复。

## 11. WAL Replay 与发布

```text
1. 从 Manifest checkpoint + 1 开始。
2. 顺序解码完整 Commit Unit。
3. Apply 到 Recovery MemTable/Index。
4. 更新 Registry、Tail 和 Locator 投影。
5. 每完成一个 Batch 推进 applied_entry_id。
6. 全部完成后生成新的 State Checkpoint。
7. 原子切换为正常 Active MemTable。
8. 才开放 Read；取得 Lease 后再开放 Append。
```

恢复期间不能让 Reader 看到部分重放状态。

## 12. Snapshot 安装恢复

1. Snapshot 下载到唯一 Staging 目录；
2. 校验 Snapshot Manifest 和每个 Artifact；
3. `fsync` 所有文件和目录；
4. 写入安装意图标记，包含旧/新 CURRENT；
5. 原子切换 CURRENT；
6. `fsync` 数据根；
7. 写入完成标记；
8. 从 Checkpoint + 1 追赶 WAL；
9. 安装成功后延迟清理旧文件。

重启发现安装意图：

- CURRENT 仍指向旧 Manifest：删除未引用 Staging，继续旧历史；
- CURRENT 指向新 Manifest且完整：完成状态和 WAL 追赶；
- 两边都不完整：失败关闭。

不能把旧 Tail/Locator 与新 Segment 集合混合。

## 13. Orphan 与临时文件

文件分类：

```text
LIVE       当前 Manifest/Snapshot/Reader 引用
PINNED     历史 Snapshot 或传输 Lease 引用
STAGING    尚未发布
ORPHAN     完整但从未被任何已发布引用采用
CORRUPT    格式或 checksum 失败
TRASH      已解除引用并等待删除
```

普通启动只报告，不立即删除 ORPHAN/CORRUPT。清理必须由 Segment Lifecycle 的引用图和宽限期决定。

## 14. Crash Point 结果

| Crash Point | 恢复结果 |
| --- | --- |
| WAL Header 写一半 | 非当前合法 WAL，忽略或隔离 |
| WAL Entry 写一半 | Active 尾部截断 |
| WAL 完整但未 fsync | 可以存在或不存在；存在且连续时按模式决议 |
| WAL fsync 后未 Apply | 重放并 Apply |
| Batch 写入一部分 | 整个 Batch 截断 |
| Segment 临时文件写一半 | STAGING，不可见 |
| Segment fsync 后未 rename | STAGING/ORPHAN，不可见 |
| Segment rename 后 Manifest 未发布 | ORPHAN，不可见 |
| Manifest 写一半 | 不成为候选 Generation |
| Manifest 完整但 CURRENT 未切换 | 旧 Generation 有效，新文件为 ORPHAN/PIN 候选 |
| CURRENT rename 后目录未 fsync | 允许旧或新 Generation，但必须各自完整且唯一 |
| 新 Manifest 发布、旧 WAL 未删 | 新 Manifest有效，WAL 可重复存在 |
| Merge 发布、旧 Segment 未删 | 旧文件延迟回收 |
| Tail Slot 撕裂 | Slot 无效，从事实重建 |
| Snapshot Staging 中断 | 不影响旧 CURRENT |
| 响应发送前崩溃 | Record 可能恢复为已提交，客户端原样重试 |

## 15. 失败关闭条件

- 两条已提交历史在同一 Entry ID 内容不同；
- 当前唯一 Segment 的 checksum 失败且无可验证副本；
- Manifest Chain 分叉且无法证明唯一已发布分支；
- Registry 映射冲突；
- 已发布 Stream Sequence 或 Byte Offset 不连续；
- 不支持的必需格式版本；
- Snapshot 缺少唯一数据 Artifact；
- NODE 身份与部署配置不一致。

失败关闭时保持证据文件不变，输出结构化诊断，不自动格式化或重新初始化数据目录。

## 16. 恢复完成条件

```text
manifest_valid
AND all_required_segments_available
AND wal_prefix_unique_and_contiguous
AND registry_rebuilt
AND tails_rebuilt
AND applied_entry_id == recovered_commit_entry_id
AND no_unresolved_data_corruption
```

满足后才能标记 `READY_READ`。HA Primary 还必须拥有有效 Lease 才能标记 `READY_WRITE`。

## 17. 测试要求

- 对每个文件写入、`fsync`、rename、目录 `fsync` 边界注入崩溃；
- 对每个 Length/CRC/SHA/Offset 注入 bit flip 和截断；
- 在同一初始状态重复恢复 100 次，结果必须相同；
- 恢复后逐 Stream 校验 Sequence/Offset/Time；
- 比较恢复前所有成功响应与恢复后 Record 集合；
- 大 WAL、百万 Stream 和冷 Segment 下验证恢复内存有界；
- Primary/Standby 在不同 Crash Point 组合下验证无双主和无已确认数据丢失。
