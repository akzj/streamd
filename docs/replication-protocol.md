# streamd 主备复制协议

| 属性 | 内容 |
| --- | --- |
| 状态 | Draft / 待评审 |
| 版本 | V1 |
| 范围 | 单 Shard、一个 Primary、一个 Standby、一个外部协调器 |
| 默认保证 | Strict：Append 成功表示 WAL 已在两个数据节点持久化 |

## 1. 目的

本文定义 streamd 的存储级主备复制协议，解决以下问题：

- Primary 如何把已排序的 WAL 事实复制到 Standby；
- Append 在什么时刻可以对客户端确认；
- Standby 短暂掉线后如何增量追赶；
- Primary 已回收旧 WAL 时，Standby 如何通过 Snapshot 恢复；
- 如何在网络分区、节点故障和进程重启后避免双主；
- 新 Primary 如何接续 Entry ID、Stream Sequence 和 Byte Offset；
- 旧 Primary 如何安全地作为 Standby 重新加入。

本协议不复制公共 API 请求，也不要求主备生成完全相同的物理 Segment。它复制的是 Primary 已经完成排序和字段分配的 WAL Entry。

## 2. 范围与非目标

### 2.1 拓扑

```text
Clients
   |
   v
Primary  -- WAL replication -->  Standby
   |                                |
   +---------- local WAL -----------+
                 |
       external coordinator
        term / lease / fencing
```

- 一个复制组在任意时刻最多有一个合法 Primary；
- Primary 和 Standby 是保存完整数据的两个数据节点；
- 外部协调器只保存 Term、Leader、Lease 和 Fencing 状态，不保存 Record；
- 协调器必须自身具备可靠的一致性实现，可以是 etcd、Consul 或等价服务；
- 两个数据节点本身不组成多数派，不尝试用相互探测决定谁是 Primary。

### 2.2 非目标

- 不定义跨 Shard 共识或事务；
- 不定义自动分片和副本重平衡；
- 不复制 Hot Stream Cache、Page Cache 或打开的 Segment Handle；
- 不要求主备以相同时间 Flush、Merge 或压缩 Segment；
- 不使用外部 PostgreSQL、Redis 或搜索索引参与提交；
- 不保证 Degraded 模式下已经确认的写入在 Primary 永久丢失后仍然存在。

## 3. 术语

### 3.1 Term

`term` 是协调器分配的单调递增领导任期。每次合法选主或人工切换都必须获得更高 Term。节点拒绝来自低于其已知 Term 的复制、提交和 Snapshot 消息。

### 3.2 Entry ID

`entry_id` 是复制组内从 `0` 开始连续递增的 WAL 顺序号。它定义跨 Stream 的唯一复制顺序。空日志使用 `NONE` 表示尚无 Entry，不使用无符号数回绕模拟 `-1`。

### 3.3 Durable、Committed 与 Applied

- **appended**：完整 Entry 已写入本地 WAL 缓冲或文件，但不保证掉电后存在；
- **durable**：Entry 已由 `fsync` 完成的 WAL，或由已原子安装的 Snapshot Checkpoint 覆盖，掉电恢复后必须存在；
- **committed**：Primary 已确认该 Entry 满足当前 durability 模式，可以向读取路径发布；
- **applied**：Committed Entry 已应用到 MemTable、Tail Catalog 等可见投影。

必须保持：

```text
applied_entry_id <= commit_entry_id <= local_durable_entry_id <= last_appended_entry_id
```

Primary 还维护：

```text
commit_entry_id <= replicated_entry_id <= last_appended_entry_id
```

其中 `replicated_entry_id` 表示 Standby 已确认持久化的最大连续 Entry。

### 3.4 Snapshot Checkpoint

Snapshot 是某个已提交 Entry `N` 之后的完整可安装状态。安装成功后，Standby 不需要 `N` 及以前的 WAL，只需从 `N + 1` 继续复制。

## 4. 核心不变量

以下不变量优先于具体传输和实现：

1. 同一 Term 最多只有一个持有有效 Lease 的 Primary 可以接受 Append。
2. 节点只接受当前或更高 Term；观察到更高 Term 后立即持久化并拒绝旧 Term。
3. WAL Entry 以 Entry ID 连续复制，不能跳过、重排或重新分配字段。
4. Primary 分配 `entry_id`、`sequence`、`byte_offset` 和 `recorded_at`；Standby 原样校验和重放。
5. Strict 模式下，Append 成功意味着对应 Entry 已在 Primary 和 Standby 的 WAL 上持久化。
6. Read 只能看到 Committed Entry；未提交物理尾部不直接对公共读接口可见。
7. 已确认的 Strict Append 在任意单节点永久故障后仍可恢复。
8. WAL 只有在存在覆盖它的、已校验且可安装的 Snapshot 时才能越过 Standby 的进度回收。
9. Cache 永远不是复制事实；丢失后必须能从 WAL、Segment 和 Manifest 重建。
10. Segment 布局可以不同，但相同 Entry ID 对应的逻辑 Record 必须完全相同。
11. Promotion 必须由协调器授予新 Term，或由人工完成等价的旧主隔离；仅凭两节点互相不可达不能自动切换。
12. 未提交尾部可以物理截断；已提交 Record 不允许被逻辑删除、改写或复用 Sequence。

## 5. 持久状态

每个节点必须持久化复制元数据，不能只保存在进程内：

```text
ReplicationState {
  group_id
  node_id

  term
  role                    // PRIMARY | STANDBY | RECOVERING
  leader_id
  lease_expires_at

  last_appended_entry_id
  local_durable_entry_id
  replicated_entry_id     // Primary meaningful
  commit_entry_id
  applied_entry_id

  earliest_wal_entry_id
  installed_snapshot_id
  snapshot_entry_id
}
```

水位只能沿连续日志前进。复制状态文件使用临时文件、`fsync`、原子 rename 和目录 `fsync` 更新，或写入具有同等崩溃保证的元数据 WAL。

### 5.1 WAL Entry

WAL 格式预留复制字段：

Record Frame、WAL File 和 WAL Entry 的 V1 精确字节布局见 [V1 存储格式](storage-format.md)。本节定义复制协议需要的逻辑字段。

```text
WALEntry {
  format_version
  term
  entry_id

  stream_id
  sequence
  byte_offset
  recorded_at
  request_id
  request_hash
  record_frame

  previous_entry_hash?
  checksum
}
```

- `entry_id` 必须等于前一 Entry ID 加 `1`；
- Stream 内 `sequence` 必须等于该 Stream 的当前 `next_sequence`；
- `byte_offset` 必须等于该 Stream 的当前 `next_byte_offset`；
- `previous_entry_hash` 可用于快速确认日志前缀，V1 是否启用由格式评审决定；
- 即使不启用 Hash Chain，每条 Entry 也必须有长度边界和 checksum。

复制 WAL Entry，而不是 `Append(namespace, stream, payload)`，可以保证 Standby 不重新执行时间生成、名称分配、Sequence 分配或业务校验。

## 6. Term、Lease 与 Fencing

### 6.1 获得领导权

候选节点成为 Primary 前必须：

1. 从协调器取得高于历史值的新 Term；
2. 获得带过期时间的 Leader Lease；
3. 确认旧 Primary 已失去 Lease，或由人工/基础设施完成电源、网络、磁盘级隔离；
4. 将新 Term 和角色持久化到本地；
5. 完成日志恢复和尾部决议后才开放 Append。

### 6.2 Lease 行为

- Primary 只能在本地确认 Lease 尚有效时分配新 Entry；
- Lease 续约必须预留安全裕量，不能等到过期瞬间；
- 无法联系协调器时，Primary 只能工作到已取得 Lease 的截止时间；
- Lease 过期后立即停止 Append 和强一致读，可以继续提供明确标记为 Stale 的诊断读；
- Standby 不因复制连接断开自行提升；
- 时钟漂移上限和 Lease 裕量必须作为部署前提监控。

Term 是协议级 Fencing Token。Remote storage、Snapshot 发布和其他会改变共享状态的操作也必须携带 Term，拒绝旧 Term 写入。

### 6.3 为什么需要外部协调器

当 Primary 与 Standby 互相不可达时，两者都无法区分“对端故障”和“网络分区”。两个数据节点没有多数派，不能同时保证自动可用和无双主。因此 V1 必须选择以下之一：

- 外部一致性协调器授予 Term 与 Lease；
- 人工确认并隔离旧 Primary 后再提升 Standby。

## 7. 连接建立与追赶决策

Standby 建立连接后发送：

```text
ReplicaHello {
  group_id
  node_id
  known_term
  installed_snapshot_id
  snapshot_entry_id
  last_appended_entry_id
  local_durable_entry_id
  commit_entry_id
  applied_entry_id
  last_entry_checksum
}
```

Primary 验证身份、复制组、Term 和日志前缀，然后返回：

```text
ReplicationPlan {
  term
  leader_id
  mode                   // INCREMENTAL | SNAPSHOT | REJECT
  start_entry_id?
  snapshot_id?
  checkpoint_entry_id?
  earliest_wal_entry_id
  commit_entry_id
}
```

基本决策：

```text
standby_next = standby.local_durable_entry_id + 1

if prefix matches and standby_next >= primary.earliest_wal_entry_id:
    INCREMENTAL from standby_next
else if installable snapshot exists:
    SNAPSHOT, then INCREMENTAL from checkpoint_entry_id + 1
else:
    REJECT with NO_RECOVERY_SOURCE
```

若 Standby 声明的 Entry ID 与 Primary 相同位置 checksum 不一致，则不能继续追加。正常重连返回 `LOG_DIVERGED`，进入第 12 节的重入流程。

## 8. 稳态 WAL 复制

### 8.1 数据消息

```text
AppendEntries {
  term
  leader_id
  first_entry_id
  previous_entry_id
  previous_entry_checksum
  entries[]
}
```

Standby 按顺序执行：

1. 验证 Term、Leader 和复制组；
2. 验证 `previous_entry_id + checksum` 与本地日志前缀一致；
3. 验证 Entries 的 ID 连续、Frame 长度和 checksum；
4. 顺序追加到 Standby WAL；
5. 更新 `last_appended_entry_id`，但不提前报告 durable；
6. 等待后续 Barrier 或本地 Group Commit 触发 `fsync`。

重复收到已经存在且 checksum 相同的 Entry 是幂等成功；相同 Entry ID 内容不同必须返回 `LOG_DIVERGED`。

### 8.2 Durability Barrier

```text
DurabilityBarrier {
  term
  through_entry_id
}

DurableAck {
  term
  durable_entry_id
  last_entry_checksum
}
```

收到 Barrier `N` 后，Standby 必须确认本地拥有从当前前缀到 `N` 的全部连续 Entry，执行 WAL `fsync`，持久化 `local_durable_entry_id >= N`，然后才发送 `DurableAck(N)`。

Barrier 和 ACK 都是累计水位，丢失后可以安全重发。ACK 不代表 Entry 已对公共读取可见。

### 8.3 Strict 提交路径

```text
Primary                          Standby
   |                                |
   |-- AppendEntries [..N] -------->|
   |                                | append WAL
   | local WAL fsync through N      |
   |-- DurabilityBarrier(N) ------->|
   |                                | fsync WAL through N
   |<-- DurableAck(N) --------------|
   | advance commit_entry_id=N      |
   | apply/publish through N         |
   |-- CommitAdvance(N) ----------->|
   |                                | apply/publish through N
   | respond success to clients      |
```

Primary 可以并行执行本地 WAL 写入和网络发送，但必须同时满足：

```text
primary.local_durable_entry_id >= N
standby DurableAck >= N
Primary Lease remains valid
```

才能把 `commit_entry_id` 推进到 `N`。提交必须保持前缀性质，不能越过未持久化 Entry。

```text
CommitAdvance {
  term
  commit_entry_id
}
```

Standby 只 Apply 不超过 `min(commit_entry_id, local_durable_entry_id)` 的 Entry。CommitAdvance 可累计、重复和乱序到达，但水位不能倒退。

### 8.4 Group Commit

一次 Barrier 可以覆盖多个 Stream 和多个客户端请求。Primary 保存每个请求对应的最高 Entry ID，只有 `commit_entry_id` 覆盖完整请求后才响应。AppendBatch 的全部 Entries 必须一起提交或不对外可见。

### 8.5 响应或 ACK 丢失

- DurableAck 丢失：Primary 重发 Barrier，Standby 从持久水位幂等 ACK；
- CommitAdvance 丢失：Standby 重连后从 Primary 获得累计 Commit 水位；
- 客户端响应丢失：客户端使用相同 `expected_sequence + request_id + request_hash` 重试；
- Primary 在 Commit 后、响应前崩溃：记录可以在新 Primary 上可见，重试返回原结果。

## 9. 未确认尾部的决议

节点崩溃时可能存在已持久化但尚未得到客户端成功响应的连续 WAL 尾部。例如 Standby 已发送 DurableAck，但 Primary 尚未持久化或传播 Commit 水位。

V1 采用以下规则：

> Promotion 时保留并提交候选节点上 checksum 正确、Entry ID 连续、字段连续且不与已提交前缀冲突的 durable suffix。

理由：有效 Fencing 保证旧 Primary 不能再生成另一条合法历史；保留尾部可以覆盖“双方已经持久化、但提交消息或客户端响应丢失”的情况。

因此协议允许：客户端未收到成功的 Append，稍后却发现该 Record 已经存在。这不是重复写；客户端必须使用 Sequence-bound Idempotency 协议解析不确定结果。

以下尾部不得直接提交：

- checksum 或 Frame 不完整；
- Entry ID 有空洞；
- Stream Sequence、Byte Offset 或 Recorded At 连续性失败；
- 与新 Primary 的已提交前缀不一致；
- 来自已经被更高 Term 隔离后的旧主写入。

这些尾部只能截断到最后一个合法边界。截断的是从未成为公共事实的物理 WAL，不违反 Record 永不逻辑删除。

## 10. Snapshot 生成与安装

### 10.1 Snapshot 内容

```text
Snapshot {
  format_version
  group_id
  snapshot_id
  term
  checkpoint_entry_id
  checkpoint_entry_checksum
  manifest_generation

  segments[] {
    segment_id
    size
    checksum
    object_location?
  }

  tail_catalog
  extent_locator_roots
  stream_registry
  replication_metadata
  checksum
}
```

Snapshot 必须足以恢复 `checkpoint_entry_id` 及以前的全部逻辑 Record、Stream Tail 和索引根。Cache 内容不进入 Snapshot。

### 10.2 在线生成

Primary 在已提交 Entry `N` 上生成 Snapshot：

1. 记录 `checkpoint_entry_id = N` 和对应 checksum；
2. 冻结覆盖 `N` 及以前状态的 MemTable/Manifest 视图；
3. 新 Append 继续进入新的 WAL 和 MemTable，不阻塞整个 Snapshot 上传周期；
4. Flush 所有不晚于 `N` 的状态，生成完整不可变 Segment 集合；
5. 生成 Tail Catalog、Extent Locator Roots 和 Stream Registry Checkpoint；
6. 校验所有文件，原子发布 Snapshot Manifest；
7. Pin Snapshot 引用的 Segment，直到安装完成或传输 Lease 过期。

Snapshot 不得包含只能依赖 `N` 之后 WAL 才能解释的半完成状态。

### 10.3 协议消息

```text
SnapshotOffer {
  term
  snapshot_id
  checkpoint_entry_id
  total_bytes
  expires_at
  manifest_checksum
}

SnapshotManifest {
  snapshot
}

SnapshotInstalled {
  term
  snapshot_id
  checkpoint_entry_id
  manifest_checksum
}
```

文件传输可以使用复制连接、HTTP 或对象存储签名地址，但身份、完整性和过期语义必须一致。

### 10.4 Standby 安装

Standby 执行：

1. 下载到独立 Staging 目录；
2. 对每个文件检查 size、checksum 和格式；
3. 对可复用的相同 Segment 通过 checksum 确认后复用；
4. `fsync` 新文件和 Staging 目录；
5. 写入并 `fsync` 新 Manifest/CURRENT；
6. 原子切换 CURRENT，再 `fsync` 父目录；
7. 将 `applied_entry_id`、`commit_entry_id` 和 durable 基线设置为 checkpoint `N`；
8. 删除或隔离与 Snapshot 不一致的旧未提交尾部；
9. 发送 SnapshotInstalled；
10. 从 `N + 1` 开始增量复制。

Snapshot 安装后，`N` 是新的日志基线。校验 `N + 1` 的 `previous_entry_id/checksum` 时使用 Snapshot 保存的 Checkpoint ID 和 checksum，不要求 Standby 仍保留 `N` 的 WAL 文件。

安装过程中崩溃时，重启必须选择旧 CURRENT 或完整的新 CURRENT，不能组合两个 Snapshot。Staging 目录永远不参与正常读取。

### 10.5 物理布局

Snapshot 安装可以直接复制 Primary 当时的 Segment 文件。安装完成后，Standby 可以独立 Flush、Merge 或压缩，因此主备长期不要求拥有相同的 Segment 边界或文件名。复制一致性以 WAL Entry 和逻辑 Record 为准，不以 Segment 一致为准。

## 11. WAL 保留与回收

### 11.1 水位

实现至少跟踪：

```text
local_segmented_entry_id
replica_durable_entry_id
replica_applied_entry_id
snapshot_available_entry_id
archive_durable_entry_id
earliest_wal_entry_id
```

### 11.2 回收条件

某段 WAL 可以回收，当且仅当：

1. 其中的 Committed Entry 已进入本地已发布 Segment；
2. 存在覆盖该范围的完整 Snapshot；
3. Snapshot 的 Manifest 与所有 Segment 均可读、checksum 已验证，并被本地或对象存储 Pin。

Standby 当前已经持久化该范围，可以作为延后或加快回收的策略输入，但不能替代 Snapshot 条件。否则 Standby 在 Primary 删除 WAL 后丢盘，就没有可用于重新加入的完整恢复源。

始终禁止：

```text
WAL 已删除
AND
不存在覆盖该历史的可安装 Snapshot
```

WAL 保留必须有容量上限。Standby 长期离线不能永久阻止 WAL 回收：当其进度落后于 `earliest_wal_entry_id`，状态变为 `NEEDS_SNAPSHOT`，重新连接后安装 Snapshot。

### 11.3 Snapshot 生命周期

- 至少保留一个覆盖 `earliest_wal_entry_id - 1` 的已验证 Snapshot；
- 新 Snapshot 未完整发布前不能释放旧 Snapshot；
- 正在安装的 Snapshot 通过传输 Lease Pin；
- Segment 在所有 Snapshot、Manifest 和活跃 Reader 引用释放前不能物理删除；
- Snapshot Segment 在传输期间消失属于存储完整性故障，当前安装失败并重新协商，不得静默跳过。

## 12. Failover、Promotion 与 Rejoin

### 12.1 Promotion

Standby 提升为 Primary 的顺序：

1. 停止接受旧 Primary 的复制连接；
2. 从协调器获取更高 Term 和有效 Lease，或确认人工 Fencing 已完成；
3. 持久化新 Term；
4. 恢复本地 WAL 到最后一个完整 durable Entry；
5. 按第 9 节验证并决议 durable suffix；
6. 推进 Commit/Applied 水位；
7. 从最后合法 Entry ID 加 `1` 继续分配；
8. 从每个 Stream 的最后合法 Tail 继续分配 Sequence、Byte Offset 和 Recorded At；
9. 对外发布新 Term/Leader，开放 Append。

Entry ID 和 Stream Sequence 都不能因换主回到 `0` 或复用旧值。

### 12.2 Strict 模式故障行为

- Standby 不可用：Primary 停止确认新 Append，Read 继续；
- 协调器不可用：Primary 可工作到 Lease 到期，到期后停止写；
- Primary 故障：完成 Fencing 后提升 Standby；
- Primary 与 Standby 分区：持有有效 Lease的一方才可能保持 Primary，另一方不能自升主；
- 两个数据节点都不可用：等待至少一个有效副本恢复，不从不完整外部索引重建事实。

### 12.3 旧 Primary 重入

旧 Primary 恢复后始终以 `RECOVERING` 身份连接，不能沿用旧 Lease。新 Primary 比较双方日志：

```text
if old node is an exact prefix and WAL is retained:
    incremental catch-up
else if common prefix exists and differing suffix was never committed:
    truncate differing physical suffix, then incremental catch-up
else:
    install Snapshot, then incremental catch-up
```

任何被新 Term 历史覆盖的旧 Term 尾部都不得重新发布。若发现两个已提交前缀发生冲突，说明 Fencing 或存储完整性已经失效，复制组必须失败关闭并要求人工处理，不能自动选择一边。

### 12.4 Degraded 模式

Degraded 是显式、可审计的运维模式，不得由超时自动触发：

```text
durability = DEGRADED_LOCAL_ONLY
```

在该模式下，Primary 可以在本地 WAL 持久化后确认 Append。其代价必须明确展示：

- Standby 落后期间，Primary 节点或磁盘永久丢失会导致 RPO 大于 `0`；
- 已向客户端确认的 Record 可能在 Failover 后丢失；
- 丢失尾部对应的 Sequence 可以由新 Primary 从最后保留点继续使用，调用者必须按灾难恢复语义处理；
- 恢复 Strict 前，必须等待 Standby 追到 Primary 当前 durable 水位；
- 模式进入、退出、操作者、Term 和影响范围必须写入审计日志与指标。

如果产品不能接受以上语义，就不应提供 Degraded 模式。

## 13. 读取与订阅

### 13.1 Primary 读取

强一致 Read、Inspect 和 Subscribe 默认由持有有效 Lease 的 Primary 提供，只返回 `applied_entry_id` 以内的数据。Append 成功后 Primary 必须满足 Read-your-write。

### 13.2 Standby 读取

Standby 读取是可选能力，请求必须携带或由网关推导最低水位：

```text
minimum_entry_id
```

只有 `applied_entry_id >= minimum_entry_id` 时才能响应，否则等待、重定向 Primary 或返回 `REPLICA_LAGGING`。没有最低水位的 Standby Read 必须明确标记为可能陈旧。

### 13.3 Subscribe 重连

Subscribe 的公共 Cursor 仍是每个 Stream 的 `Sequence`，不暴露 WAL Entry ID 作为消费 Cursor。换主或断线后，客户端使用最后处理完成的 `next_sequence` 重连；重复交付允许，永久跳过不允许。

## 14. 消息与错误码

协议与传输无关，可以映射为 gRPC 双向流、QUIC 或其他可靠字节流。所有消息都必须携带 `protocol_version`、`group_id`、节点身份和 Term，以下为语义消息：

| 消息 | 方向 | 作用 |
| --- | --- | --- |
| `ReplicaHello` | Standby → Primary | 声明本地日志、Snapshot 和水位 |
| `ReplicationPlan` | Primary → Standby | 选择增量、Snapshot 或拒绝 |
| `AppendEntries` | Primary → Standby | 发送连续 WAL Entry |
| `DurabilityBarrier` | Primary → Standby | 要求持久化至累计水位 |
| `DurableAck` | Standby → Primary | 确认累计 durable 水位 |
| `CommitAdvance` | Primary → Standby | 发布累计 commit 水位 |
| `Heartbeat` | 双向 | Term、Lease、进度和健康状态 |
| `SnapshotOffer` | Primary → Standby | 提供可安装 Checkpoint |
| `SnapshotManifest` | Primary → Standby | 描述 Snapshot 文件集合 |
| `SnapshotInstalled` | Standby → Primary | 确认原子安装完成 |

错误码至少包括：

| 错误码 | 含义 | 处理 |
| --- | --- | --- |
| `TERM_STALE` | 消息 Term 低于本地 | 发送方降级并重新发现 Leader |
| `NOT_LEADER` | 接收方不是当前 Primary | 重新发现 Leader |
| `LEASE_EXPIRED` | Primary 已无写 Lease | 停止写入并重新选主 |
| `LOG_GAP` | Entry ID 不连续 | 从缺失点重发或 Snapshot |
| `LOG_DIVERGED` | 相同位置 checksum 不同 | 进入 Rejoin/人工检查 |
| `NEEDS_SNAPSHOT` | 所需 WAL 已回收 | 安装 Snapshot |
| `CHECKSUM_MISMATCH` | Entry 或文件损坏 | 重传；重复失败则隔离节点 |
| `SNAPSHOT_EXPIRED` | Snapshot Pin 已释放 | 重新协商 Snapshot |
| `NO_RECOVERY_SOURCE` | 无 WAL 且无可安装 Snapshot | 失败关闭并报警 |
| `REPLICA_LAGGING` | Standby 未达到读取水位 | 等待或转 Primary |
| `RESOURCE_EXHAUSTED` | WAL/磁盘/队列达到安全上限 | 背压，不能降低 durability |

未知消息类型可在版本允许时跳过；未知的强制字段或不兼容格式必须拒绝，不能猜测解释。

## 15. 流控、安全与观测

### 15.1 流控

- AppendEntries 具有有界 in-flight bytes 和 Entry 数；
- Standby 定期报告接收、durable、commit、applied 水位；
- Primary 在复制队列达到上限时对 Append 背压；
- Strict 模式不因复制延迟自动转成本地确认；
- Snapshot 传输与实时 WAL 复制分离限速，避免大文件阻塞 Tail Catch-up。

### 15.2 安全

- 数据节点和协调器使用 mTLS 双向认证；
- 证书身份绑定 `cluster_id/group_id/node_id`；
- Standby 不接受任意节点发送的 Snapshot 或 WAL；
- Snapshot 对象地址使用短期凭据，Manifest checksum 通过已认证控制通道传递；
- Payload 默认不进入复制日志的诊断输出。

### 15.3 指标

至少暴露：

- 当前 Term、Role、Leader 和 Lease 剩余时间；
- appended/durable/replicated/commit/applied 水位；
- replication lag entries/bytes/time；
- Barrier latency、remote fsync latency 和 Group Commit size；
- earliest retained WAL、WAL bytes 和预计耗尽时间；
- Snapshot checkpoint、生成耗时、传输进度、安装耗时和失败原因；
- Promotion、Fencing、Rejoin 和 Degraded 模式变更次数；
- checksum failure、log gap、log divergence 和 stale term 次数。

## 16. 故障测试矩阵

实现前必须把以下场景变成可重复故障注入测试：

| 场景 | 必须验证的结果 |
| --- | --- |
| Primary 在本地 WAL 写一半时崩溃 | 截断半条 Entry，不提交损坏数据 |
| Primary 在本地 fsync 前/后崩溃 | 未确认写可存在或不存在；合法 durable suffix 按规则决议 |
| Standby 在收到 Entry 后、fsync 前崩溃 | 不得 ACK；重启后从连续 durable 水位重放 |
| Standby 在 fsync 后、ACK 前崩溃 | Barrier 重发后幂等 ACK |
| DurableAck 丢失 | Primary 不提前响应，重发后完成提交 |
| CommitAdvance 丢失 | 重连时按累计 commit 水位恢复 |
| 客户端响应丢失 | 相同 Expected Sequence 重试返回原 Record |
| 主备网络分区 | 不出现两个持有合法 Term/Lease 的写主 |
| Primary Lease 到期 | 停止 Append，不继续分配 Entry/Sequence |
| Snapshot 生成中 Primary 崩溃 | 未发布 Snapshot 不可见，旧 Snapshot 仍可用 |
| Snapshot 下载/安装中 Standby 崩溃 | CURRENT 指向旧或完整新版本，不出现混合状态 |
| Snapshot Segment 传输中消失 | 安装失败，重新协商，不跳过文件 |
| Standby 追赶时 WAL 轮转/GC | WAL 被 Pin，或明确切换为 NEEDS_SNAPSHOT |
| 旧 Primary 携带旧 Term 尾部重入 | 丢弃未提交冲突尾部，不能覆盖新历史 |
| Standby 空盘 | Snapshot 安装后从 checkpoint + 1 追赶 |
| Standby WAL/Segment 损坏 | checksum 检出，重传或重装 Snapshot |
| Standby 掉线且 Strict 开启 | 新 Append 不成功，Read 保持可用 |
| Standby 掉线且显式 Degraded | 本地确认并持续暴露非零 RPO 告警 |
| 协调器不可达 | 现主只工作到 Lease 截止，Standby 不自升主 |

测试必须区分“客户端收到成功”“Entry 已 durable”“Entry 已 committed”“Entry 已 applied”，不能只检查进程是否重新启动。

## 17. 实施顺序

建议在单节点文件格式和崩溃恢复稳定后分阶段实现：

1. 为 WAL Entry 固定 Term、Entry ID 和 checksum 格式；
2. 实现 ReplicaHello、连续日志校验和 AppendEntries；
3. 实现 Barrier、双副本 Group Commit 和 CommitAdvance；
4. 接入外部 Term/Lease/Fencing；
5. 实现 Snapshot Checkpoint、Pin、安装和增量追赶；
6. 实现 WAL 有界保留与 NEEDS_SNAPSHOT；
7. 实现 Promotion、旧主 Rejoin 和故障注入；
8. 最后评估是否需要显式 Degraded 模式。

## 18. 待评审问题

以下问题不影响协议主干，但需要在实现前固定：

1. V1 采用哪一种协调器，以及 Lease 的时钟漂移和续约参数；
2. WAL 是否启用 `previous_entry_hash`，还是仅用 Entry checksum + Snapshot checkpoint checksum；
3. Snapshot 文件走复制连接还是对象存储，是否两者都支持；
4. Primary 的 Commit 水位使用独立元数据 WAL，还是可以完全由 durable suffix promotion 规则恢复；
5. 是否在 V1 暴露 Standby Read；
6. 是否完全不提供 Degraded 模式，以保持“成功即双副本持久”的单一契约；
7. 一个 WAL Entry 是否只包含一条 Record，AppendBatch 如何编码为不可分割提交单元。
