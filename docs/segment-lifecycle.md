# streamd Segment 生命周期

| 属性 | 内容 |
| --- | --- |
| 状态 | Draft / V1 实现基线 |
| 依赖 | [V1 存储格式](storage-format.md)、[崩溃恢复协议](recovery-protocol.md) |
| 范围 | MemTable Freeze、Flush、Merge、Snapshot Pin、对象存储与物理回收 |

## 1. 目标

Segment 生命周期只改变物理布局，不改变任何逻辑 Record。必须同时满足：

- Record Frame 字节不变；
- Sequence、Byte Offset、Entry ID、Recorded At 不变；
- Reader 在 Manifest 切换期间只看到完整旧视图或完整新视图；
- WAL、Segment 和 Snapshot 不会出现“最后一份副本被删除”；
- 文件数量、Merge 放大和本地空间有界。

## 2. 对象状态机

```text
Active MemTable
      |
      | freeze at Entry N
      v
Immutable MemTable
      |
      | flush
      v
Staging Segment
      |
      | fsync + verify + Manifest publish
      v
Live Segment
      |
      +---- merge input ----> Replacement Segment
      |
      +---- snapshot pin ---> Pinned Segment
      |
      +---- archive -------> Remote-backed Segment
      |
      | no references
      v
Retired -> Trash Grace Period -> Physical Delete
```

状态由引用关系决定，不靠文件名猜测。

## 3. MemTable Freeze

### 3.1 触发条件

- Active MemTable 达到字节上限；
- Record/Stream 数达到上限；
- WAL 未 Flush 字节接近水位；
- Snapshot 要求 Checkpoint；
- 优雅 Shutdown；
- 管理员显式 Flush。

### 3.2 Freeze Barrier

Shard Writer 在短临界区内：

1. 选择不晚于当前 `applied_entry_id` 的边界 `N`；
2. 关闭 Active MemTable 的追加；
3. 记录它覆盖的 Entry/Stream 范围；
4. 安装新的空 Active MemTable；
5. 新 Append 立即进入新 MemTable；
6. 将旧表加入 Immutable Queue。

Freeze 不等待磁盘写入。Batch 不能跨 Freeze Barrier。

### 3.3 Immutable Queue

- 按 Entry 范围顺序 Flush；
- V1 同一 Shard 最多允许有限个 Immutable MemTable；
- 达到上限对 Append 背压；
- 不允许跳过更早 Immutable 而发布更晚 Checkpoint；
- Flush 失败保留内存和 WAL，不丢弃事实。

## 4. Flush

### 4.1 输入

Flush 输入是一个或多个相邻 Immutable MemTable，必须覆盖连续的已应用 Entry 前缀。不能把仍未 Commit 的 Frame 写进可发布 Segment。

### 4.2 构建

1. 按 StreamID 排序目录；
2. 每个 Stream 按 Sequence 连接原始 Frame；
3. 生成 Dense Unified Index；
4. 验证 Sequence/Offset/Time 连续；
5. 写临时 Segment Header、Directory、Index、Data、Footer；
6. 计算每条 CRC 和 Content SHA-256；
7. `fsync` 文件；
8. 完成全量 Scrub Check；
9. rename 为正式 Segment；
10. `fsync segments/`。

Flush 不重新编码 Frame，也不按 Header/Payload 内容排序。

### 4.3 Manifest 发布

新 Manifest：

- 保留所有仍然 Live 的旧 Segment；
- 加入新 Segment Reference；
- 推进连续 `last_entry_id/last_entry_crc32c`；
- 更新 Locator/Tail Checkpoint Reference；
- 使用新 Generation 原子替换 CURRENT。

只有 CURRENT 和数据根目录完成 `fsync` 后，新 Segment 才是 Live。

## 5. Flush 与 WAL

Segment 发布不立即等于 WAL 可删除。WAL 回收还要求：

1. 对应 Entry 已进入已发布 Segment；
2. 存在覆盖它的完整、可安装 Snapshot；
3. Snapshot Artifact 已校验并 Pin；
4. 不存在恢复/复制会话仍持有该 WAL Range Pin。

Standby 已经 durable 不能替代 Snapshot 条件。

## 6. Merge

### 6.1 目的

- 减少一个 Stream 的 Extent/Segment 数；
- 降低冷查询打开文件数量；
- 回收被替代物理副本；
- 为未来冷压缩或对象归档生成新布局。

Merge 不是逻辑 Compaction，不按 Key 去重，也不删除 Record。

### 6.2 候选选择

综合评分：

```text
score = segment_count_pressure
      + overlapping_stream_pressure
      + small_segment_pressure
      + cold_read_amplification
      - snapshot_pin_penalty
      - hot_read_penalty
      - current_io_pressure
```

V1 优先合并相邻 Entry/时间范围的小 Segment。被频繁读取、刚发布或正在 Snapshot 传输的 Segment 降低优先级。

当前运行时在每次 Checkpoint 后最多尝试一次自动 Merge。默认在 32 个 Live Segment 时触发，
一次最多选择 8 个 Entry ID 相邻的 Segment，输入文件总量不超过 64 MiB。若不存在至少两个满足
预算的相邻 Segment，本轮跳过，不突破内存预算强制合并。参数由 `compaction` 配置段覆盖。

Merge 发布与退休分离：Replacement Segment 和新 Manifest 先持久化，Engine 再切换到新
Generation；旧读视图退出、Handle 关闭后，输入 Segment 才移动到 `trash/`。在线 Snapshot 的
Manifest Pin 会继续延迟退休。

### 6.3 正确性

- 输入 Segment 全部不可变且校验通过；
- 对同一 Stream，输入 Extent 必须按 Sequence/Offset 连续；
- 输出逐字节复制 Frame；
- 输出 Index 与 Frame 重新校验，但 Frame CRC 不改变；
- 输出逻辑 Record 数和输入去重后的集合完全相同；
- 不允许两个输入同时声称拥有同一 Stream/Sequence 的不同 Frame；
- Merge 期间新 Append 不参与当前输出。

### 6.4 发布

```text
build replacement
-> fsync
-> scrub
-> rename
-> fsync directory
-> publish Manifest replacing input refs with output refs
-> wait readers/pins
-> retire inputs
```

崩溃前后 Reader 只看到旧集合或新集合。

## 7. Locator 更新

Flush/Merge 为每个受影响 Stream 生成新 Extent：

- 新 Locator Page 在 Active Pack 尾部完成写入和 Page CRC；
- Page 通过 Previous Pointer 连接历史；
- Tail Active Slot 更新为新 Pack/Ordinal；
- Manifest/Snapshot 需要 Checkpoint 时先 Seal Pack，再生成 Locator Snapshot；
- 旧 Page/Pack 只有在没有 Root、Snapshot 或 Reader 引用后才能回收。

Locator 是投影。发布失败时可以从 Segment Directory 重建，不能反向决定 Segment 是否删除。

当前 V1 Runtime 每次 Checkpoint/Merge 从该 Generation 的完整 Segment Descriptor 集构建一个
新的 Sealed Pack 和 Locator Snapshot，不复用 Active Pack。Page 内 Extent 连续，超过单页容量时
通过 Previous Pointer 串联；格式支持 Skip Pointer，但当前 Builder 尚不生成。新 Manifest、Tail
Catalog 和 Locator Snapshot 同代发布，切换 Reader View 后才退休旧 Snapshot/Pack；在线 Snapshot
复制期间 Pack 受 Pin 保护。Active Pack 增量追加与跨 Pack Skip 属于后续优化。

同一次发布也从 Manifest 内部 Registry Stream 重建并校验连续 StreamID，生成新的排序 Registry
Snapshot。Compaction 构建期间可以继续 Append；视图切换前在 Engine Mutex 下把
`created_entry_id > manifest.last_entry_id` 的 Registry Overlay 合并到新 Snapshot 基线之上，避免
活跃 Stream 映射丢失。旧 Registry Snapshot 与 Tail/Locator 一起在新视图可见后退休。

## 8. Snapshot Pin

### 8.1 创建

Checkpoint `N`：

1. 确保所有 `<= N` 的 Committed Entry 已 Flush；
2. Seal 当前 Locator Pack；
3. 生成 Tail、Locator、Registry Checkpoint；
4. 发布覆盖 `N` 的 Manifest；
5. 构建 Snapshot Manifest；
6. 对所有 Artifact 增加 Snapshot Pin；
7. 校验可安装性后标记 AVAILABLE。

### 8.2 Pin 类型

```text
CURRENT_MANIFEST
HISTORICAL_MANIFEST
SNAPSHOT_AVAILABLE
SNAPSHOT_TRANSFER_LEASE
READER_HANDLE
MERGE_INPUT
REMOTE_UPLOAD
RECOVERY_SESSION
```

每个 Pin 有 Owner、创建时间和可选 Expiry。CURRENT、Snapshot Available 等持久 Pin 不能只存在内存。

### 8.3 Pin 释放

- Snapshot 被更晚 Snapshot 安全替代并超过保留策略；
- Standby 安装成功或 Transfer Lease 到期；
- Reader/Recovery Session 关闭；
- Merge/Upload 结束。

进程崩溃后，临时 Pin 按 Lease/Owner Epoch 清理，持久 Pin 从 Manifest/Snapshot 重建。

## 9. 对象存储

### 9.1 上传

1. 只上传 Sealed Segment/Artifact；
2. Object Key 包含 Cluster、Group、Artifact Type 和 Content Digest；
3. 上传完成后执行远端 size/checksum 验证；
4. 发布新 Manifest，增加稳定 Object Location；
5. 完成一次实际 Range Read 验证；
6. 才允许把本地副本变为可回收候选。

临时上传 ID 和短期凭据不能进入 Manifest。

### 9.2 远端读取

- 先查本地 Segment；
- 缺失时通过有界 Range Read 获取 Header/Directory/Index/Data；
- 下载内容仍验证 Frame CRC 和 Artifact Digest；
- 可选本地 Cache 不是唯一副本；
- 远端超时返回可重试错误，不能伪造空结果。

### 9.3 对象删除

“永不逻辑删除”下，V1 默认不删除唯一远端 Record Artifact。删除被 Merge 替代的远端物理副本前，必须证明新 Artifact 逻辑等价并被所有所需 Manifest/Snapshot 引用。

## 10. 引用图与回收

```text
CURRENT
  -> Manifest
      -> Segments
      -> Tail/Locator/Registry Artifacts

Snapshot Manifest
  -> Manifest
  -> all required Artifacts

Runtime Pins
  -> Segment/WAL/Artifact
```

文件可进入 Trash 当且仅当入度为零，且不属于 Active WAL、Active MemTable、Staging 安装或未完成发布事务。

### 10.1 Trash

- rename 到 `trash/`；
- `fsync` 原目录和 Trash 目录；
- 记录 Artifact ID、Digest、原因和时间；
- 等待 `trash_grace_period`；
- 再删除并 `fsync trash/`。

Trash 提供误判恢复窗口，但不是备份。

### 10.2 永不执行

- 按磁盘水位删除最老逻辑 Record；
- 删除仍被 Snapshot 引用的 Segment；
- 为缓解空间压力跳过 checksum；
- 在 Manifest 发布前删除输入文件；
- 使用进程内 RefCount 作为唯一删除依据。

## 11. 空间压力

水位建议：

```text
LOW       正常
HIGH      加速 Flush/Merge/Upload，限制低优先级读取
CRITICAL  停止新 Append，保持 Read/Recovery
FULL      只读失败保护，不通过删除历史自救
```

必须预留：Active WAL、一个最大 Flush Segment、一次 Merge 输出、Snapshot/Manifest 元数据和文件系统运行空间。

## 12. 调度与资源隔离

- Flush 优先级高于 Merge；
- WAL/Replication `fsync` 优先于后台 IO；
- Snapshot Upload 和 Scrub 使用独立带宽令牌桶；
- Merge 有最大 Read/Write MB/s 和并发数；
- Foreground Read latency 超阈值时暂停新 Merge；
- 不能让慢对象存储阻塞 WAL Writer。

## 13. 失败处理

| 故障 | 行为 |
| --- | --- |
| Flush 构建失败 | 删除/隔离 Staging，保留 MemTable/WAL |
| Segment Scrub 失败 | 不发布，报警 |
| Manifest 发布失败 | 旧 CURRENT 有效，新 Segment 为 Orphan |
| Merge 输入损坏 | 停止 Merge，隔离 Shard，不生成输出 |
| Object Upload 失败 | 保留本地副本，退避重试 |
| Snapshot Pin 泄漏 | 告警，按 Owner/Lease 审计，不强删 |
| Trash 删除失败 | 不影响逻辑数据，重试并监控 |
| 磁盘 Critical | 停止 Append，继续安全 Flush/Read |

## 14. 指标

- Active/Immutable MemTable bytes/count；
- Flush queue、latency、bytes/s、失败数；
- Segment count/size distribution；
- Extents per Stream 和 Read Amplification；
- Merge input/output bytes、write amplification、暂停时间；
- Manifest Generation 和发布耗时；
- Pin 数量、年龄和 Owner；
- WAL/Snapshot/Segment 可回收字节；
- Local/Object/Trash 字节；
- Object upload/range-read latency 和 checksum failure；
- 预计磁盘耗尽时间。

## 15. 测试矩阵

- Freeze 与并发 Append；
- 每个 Flush/Manifest `fsync`/rename Crash Point；
- Merge 与 Reader/Subscribe/Snapshot 并发；
- 输入 Segment 重叠、空洞、重复和损坏；
- Snapshot Pin 期间 WAL GC 和 Segment Retirement；
- Object Upload 完成前后本地文件丢失；
- 进程重启后的 Pin/Orphan/Trash 重建；
- 磁盘 High/Critical/Full；
- 百万小 Stream 的 Directory/Extent 放大；
- 长时间 Soak 下文件数量和空间是否有界。
