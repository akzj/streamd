# streamd Append 与提交协议

| 属性 | 内容 |
| --- | --- |
| 状态 | Draft / V1 实现基线 |
| 依赖 | [V1 存储格式](storage-format.md)、[主备复制协议](replication-protocol.md) |
| 范围 | Append、AppendBatch、Group Commit、幂等重试、提交与可见性 |

## 1. 目标

本文固定从客户端提交 Record 到 Record 可读之间的状态机。核心保证是：

- 单 Stream Sequence 从 `0` 开始连续、无重复、无空洞；
- 可靠写入必须使用 Compare-And-Append；
- AppendBatch 对外全部可见或全部不可见；
- 返回成功前满足当前 Durability Mode；
- 响应丢失后可以定点判断原请求是否已经提交；
- 不依赖外部数据库、全局 Request ID 索引或跨系统事务。

## 2. V1 决策

1. 所有 Append 和 AppendBatch 都必须携带 `expected_sequence`。
2. V1 不提供 Blind Append。
3. 一条 Record 对应一条 WAL Entry；Batch 对应连续的一组 WAL Entry。
4. 同一 Batch 的 WAL Entry 不允许被其他请求穿插，也不跨 WAL 文件。
5. Batch 只支持单 Stream，并严格原子可见。
6. Request Hash 固定为 SHA-256。
7. 单节点 Sync 模式在本地 WAL durable 后提交。
8. 主备 Strict 模式在主备 WAL 都 durable 后提交。
9. 未收到客户端成功响应的合法 durable 尾部允许在恢复时成为可见事实。
10. Entry ID 可用于审计诊断，但不是公共分页或订阅 Cursor。

## 3. 状态与水位

每个 Shard Writer 维护：

```text
WriterState {
  next_entry_id
  last_entry_crc32c
  current_term

  local_durable_entry_id
  replicated_entry_id
  commit_entry_id
  applied_entry_id
}
```

每个 Stream Tail 维护：

```text
StreamTail {
  next_sequence
  next_byte_offset
  last_recorded_at
  last_entry_id
}
```

水位必须保持：

```text
applied <= committed <= local_durable <= appended
```

Strict 主节点还要求：

```text
committed <= replicated
```

公共读取只能返回不晚于 `applied_entry_id` 的 Record。

## 4. 请求模型

### 4.1 Append

```text
AppendRequest {
  namespace
  stream
  expected_sequence
  request_id
  headers
  payload
}
```

### 4.2 AppendBatch

```text
AppendBatchRequest {
  namespace
  stream
  expected_sequence
  request_id
  records[] {
    headers
    payload
  }
}
```

要求：

- `request_id` 不能为空，最大 256 bytes；
- Batch 至少一条，不能超过配置和格式上限；
- Namespace、Stream、Header 和大小先完成静态校验；
- 调用者重试时必须逐字节复用原请求。

## 5. Request Hash

Request Hash 用于确认“同一个 Sequence 上是否是同一次请求”，不是全局唯一键。

V1 规范化输入：

```text
hash_input =
  domain_separator("streamd.append.v1")
  || length_prefixed(namespace_utf8)
  || length_prefixed(stream_utf8)
  || u64_le(expected_sequence)
  || length_prefixed(request_id)
  || u32_le(record_count)
  || for each record in request order:
       u32_le(header_count)
       || headers sorted by raw UTF-8 key:
            length_prefixed(key)
            || length_prefixed(value)
       || length_prefixed(payload)

request_hash = SHA256(hash_input)
```

`length_prefixed(bytes)` 使用 `u64_le(length) || bytes`。认证 Producer、服务端时间、Entry ID、Sequence 和 Byte Offset 不进入 Hash，因为它们由服务端分配。

相同 Request ID 但内容不同得到不同 Hash；streamd 不在历史中搜索 Request ID。

## 6. Compare-And-Append

在持有该 Stream 串行写权限后检查：

### 6.1 Expected 等于 Tail

```text
expected_sequence == current.next_sequence
```

请求可以进入新写入流程。

### 6.2 Expected 小于 Tail

直接读取 `expected_sequence` 的第一条 Record：

- Request ID、Request Hash、Batch Count 相同：读取整个 Batch，验证所有 Frame 后返回原结果，`deduplicated=true`；
- 任一字段不同：返回 `SEQUENCE_CONFLICT`；
- 该 Sequence 位于某个 Batch 中间：返回 `SEQUENCE_CONFLICT`，调用者必须使用 Batch 的起始 Sequence 重试。

该路径是已知 Sequence 的直接索引读取，不需要 Request ID LSM。

### 6.3 Expected 大于 Tail

返回 `SEQUENCE_AHEAD`，携带当前 `next_sequence`，不写 WAL。

### 6.4 并发

同一 Stream 使用 FIFO Append Gate。V1 同一时刻只允许一个 Commit Unit 完成 Expected 检查、字段分配和提交；后续请求排队，必须在前驱得到确定 Commit/Failure 后重新读取 Committed Tail。不同 Stream 可以并发准备，并由 Shard WAL Writer 组成同一次 Group Commit。

Gate 不等于持有 Mutex 等待网络或 `fsync`：实现应记录当前 Owner，释放内部锁，再异步等待提交。该规则避免 Tentative Tail 回滚和 Pending Range Dedup 的复杂状态。单热点 Stream 需要吞吐时使用 AppendBatch，而不是依赖多个未提交请求流水线。

## 7. 字段分配

对包含 `K` 条 Record 的请求，一次性预留：

```text
entry_ids = [next_entry_id, next_entry_id + K)
sequences = [stream.next_sequence, stream.next_sequence + K)
```

逐条计算：

```text
recorded_at = max(wall_clock_now, previous_recorded_at)
byte_offset = previous_next_byte_offset
next_byte_offset = byte_offset + encoded_frame_length
```

Batch 中所有 Frame 保存相同 Request ID、Request Hash、Batch Count，并使用连续 Batch Index。

任何整数溢出、Frame 超限或编码失败都发生在 WAL 排队前，整个请求失败且不消耗 Sequence 或 Entry ID。

一旦完整 WAL Entry 进入 Writer 队列，其字段不能重新分配。发生不确定故障时通过恢复协议决定保留或截断整个合法尾部。

## 8. Stream Registry

新 `(namespace, stream)` 首次出现时：

1. 在内存 Registry 锁中确认名称不存在；
2. 分配下一个用户 StreamID；
3. 向内部 Registry Stream 写入 Registry Record；
4. Registry Record 提交后映射永久保留；
5. 再处理用户 Stream 的 Expected Sequence 和第一条 Record。

Registry Reservation 可以与第一条 Record 进入同一次 WAL Group Commit，但它是独立 Record。若 Registry 已提交而用户 Record 未提交，保留一个空 Reservation；StreamID 不能复用。公共 API 默认把没有用户 Record 的 Reservation 视为不存在。

## 9. WAL 排队与 Group Commit

### 9.1 Pending Request

```text
PendingRequest {
  first_entry_id
  last_entry_id
  stream_id
  first_sequence
  record_count
  encoded_entries[]
  durability_mode
  completion
}
```

### 9.2 Group 形成

WAL Writer 根据以下任一条件关闭当前 Group：

- 累计字节达到 `group_commit_max_bytes`；
- 请求数达到 `group_commit_max_requests`；
- 最早请求等待达到 `group_commit_max_delay`；
- WAL 需要轮转；
- Shutdown/Flush Barrier；
- 管理员要求立即同步。

Batch 不能被拆到两个 Group 或两个 WAL 文件。超大但合法 Batch 独占一个 Group。

### 9.3 写入顺序

```text
1. writev 完整 WAL Entry 字节。
2. 检查短写并继续，直到 Group 全部写完或失败。
3. 执行 fdatasync/fsync。
4. 推进 local_durable_entry_id。
5. 根据 Durability Mode 等待复制条件。
6. 推进 commit_entry_id。
7. Apply 并原子发布 Stream Tail，释放对应 Append Gate。
8. 唤醒 Read/Subscribe。
9. 返回客户端。
```

V1 不允许在第 3 步以前响应成功。

## 10. Durability Mode

### 10.1 SINGLE_SYNC

```text
commit = local WAL durable
```

适用于单节点部署。节点或磁盘永久丢失时依赖 Snapshot/备份，RPO 由备份决定。

### 10.2 REPLICATED_STRICT

```text
commit = local WAL durable
         AND Standby DurableAck
         AND Primary Lease valid
```

默认生产 HA 模式。Standby 不可用时新 Append 保持 Pending 到超时后返回 `REPLICA_UNAVAILABLE`；服务不能自动降级为本地确认。

一旦复制链路不能推进 durable 前缀，Shard 停止接纳新的 Append。已经进入 WAL 的请求即使客户端 Deadline 到期也保留内部 Pending 状态和 Append Gate，直到 Standby 恢复后提交，或由 Failover/Recovery 对尾部作出确定决议。不能释放该位置再让后续请求复用 Sequence。

### 10.3 DEGRADED_LOCAL_ONLY

不是 V1 常规 API 能力。只有显式运维开关、审计记录和持续告警时才能启用，其非零 RPO 语义见复制协议。客户端响应必须标记当前 Durability，不能伪装成 Strict 成功。

## 11. Commit 与 Apply

### 11.1 Commit 是前缀

Commit 水位只能覆盖完整连续 WAL 前缀。即使后面的某个请求已先完成复制，也必须等待前面的 Entry。

### 11.2 Batch 原子可见

Batch 只有在 `commit_entry_id >= batch.last_entry_id` 后才 Apply。Reader 永远不能看到 Batch 的一部分。

如果进程在 Apply 中途崩溃，Tail Catalog 的 `applied_entry_id` 使恢复过程重新应用整个未完成 Batch。内存发布使用新 Tail/Index Root 一次替换；不得逐条唤醒公共 Reader。

### 11.3 Apply 内容

- 把 Frame 字节加入 Active MemTable；
- 追加 Unified Record Index；
- 更新 Tentative Tail 为 Committed Tail；
- 更新 Registry 投影或普通 Stream Tail；
- 推进 `applied_entry_id`；
- 发送合并式 Tail Notification。

通知不是事实；先发布可读状态，再发送通知。

### 11.4 Commit 水位持久化

V1 不为每次 Group Commit 再执行一次独立 Metadata WAL `fsync`。Replication State 周期生成 Checkpoint；崩溃后按以下确定性规则恢复：

- SINGLE_SYNC：Active WAL 中完整、CRC 正确的连续尾部可以提交；
- REPLICATED_STRICT：与 Standby 对账，保留双方一致的 durable 前缀；Promotion 按复制协议保留合法 durable suffix；
- 已持久化的旧 Commit Checkpoint 是下界，不是唯一真值。

该规则避免数据 WAL `fsync` 后再进行第二次元数据 `fsync`，同时允许未收到客户端响应的写入在恢复后存在。

## 12. 响应语义

```text
AppendResult {
  first_sequence
  next_sequence
  record_count
  recorded_at_first
  recorded_at_last
  storage_entry_id_first   // diagnostic
  storage_entry_id_last    // diagnostic
  deduplicated
  durability
}
```

- `next_sequence` 是客户端下一次 Append/Read 的 Cursor；
- Entry ID 在当前复制组内稳定，用于审计和支持诊断；
- 客户端不能使用 Entry ID 跨 Stream 分页或 Subscribe；
- Deduplicated 响应必须返回与原成功响应相同的 Sequence 范围。

## 13. 错误与重试

| 错误 | 是否可重试 | 规则 |
| --- | --- | --- |
| `INVALID_ARGUMENT` | 否 | 修正请求，不能复用同一 Request ID 冒充原请求 |
| `SEQUENCE_AHEAD` | 读取 Tail 后决定 | 原请求尚未占用该位置 |
| `SEQUENCE_CONFLICT` | 否 | 该 Sequence 已被其他内容占用 |
| `RESOURCE_EXHAUSTED` | 是 | 保持原请求和 Expected Sequence，退避 |
| `REPLICA_UNAVAILABLE` | 是 | 结果可能不确定，必须原样重试 |
| `DEADLINE_EXCEEDED` | 是 | 结果不确定，必须原样重试 |
| `NOT_LEADER` | 是 | 发现新 Primary 后原样重试 |
| `DATA_LOSS` | 否 | 失败关闭，等待运维恢复 |

客户端在任何“请求可能已经到达服务端”的错误后，都不能把 Expected Sequence 改成新 Tail 再发送相同业务事件。

## 14. 背压与取消

- 请求进入 WAL 队列前可以因客户端取消而移除，不消耗位置；
- Entry 已进入 WAL 队列后，客户端取消只停止等待，不撤销写入；
- Pending WAL Bytes、Pending Requests、单 Stream 队列和 Namespace 配额都有硬上限；
- 达到上限返回 `RESOURCE_EXHAUSTED`，不能丢弃旧请求腾空间；
- Subscribe、Flush、Merge 不能持有 Shard Writer 临界区。

## 15. 必测状态转换

- 并发客户端使用相同 Expected Sequence，只有一个内容获胜；
- 同 Request/Hash 重试返回原范围；
- 相同 Request ID、不同内容冲突；
- Batch 编码失败不消耗任何位置；
- Batch WAL 写一半、完整未 fsync、fsync 后未 Apply；
- Group 中多个 Stream 的先后可见性符合 Commit 前缀；
- 客户端取消发生在排队前、排队后、fsync 后；
- Registry Reservation 已提交、第一条用户 Record 未提交；
- Standby ACK、客户端响应、Primary Lease 在所有边界点丢失；
- Recovery 后 Stream Append Gate 为空，Tail 完全来自 Committed/Applied 状态。

## 16. 验收标准

1. 任意故障注入后，每个 Stream 的已提交 Sequence 连续。
2. Batch 永不部分可读。
3. 成功响应满足声明的 Durability。
4. 不确定结果可以通过原 Expected Sequence 定点解析。
5. 写入热路径只有数据 WAL 所需的同步点，不引入外部事务。
6. 同 Stream 排队不阻塞其他 Stream 的编码和读取。
