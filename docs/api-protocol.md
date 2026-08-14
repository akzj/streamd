# streamd V1 API 协议

| 属性 | 内容 |
| --- | --- |
| 状态 | Draft / V1 实现基线 |
| 传输 | gRPC 为规范协议；V1 核心不要求 HTTP Gateway |
| 公共 Cursor | 每个 Stream 从 `0` 连续递增的 Sequence |

## 1. API 原则

- API 只暴露通用 Record，不理解业务事件；
- 所有可靠写入使用 Expected Sequence；
- Read/Subscribe 使用 Sequence，不使用 Byte Offset 或 Entry ID；
- 服务端时间、Producer 和存储字段不能由客户端伪造；
- 错误必须区分确定失败与结果不确定；
- V1 不提供 Update/Delete/Truncate/Consumer Group。

## 2. 服务

```text
service StreamService {
  rpc Append(AppendRequest) returns (AppendResponse)
  rpc AppendBatch(AppendBatchRequest) returns (AppendBatchResponse)
  rpc Read(ReadRequest) returns (ReadResponse)
  rpc Subscribe(SubscribeRequest) returns (stream SubscribeResponse)
  rpc ResolveTime(ResolveTimeRequest) returns (ResolveTimeResponse)
  rpc InspectStream(InspectStreamRequest) returns (InspectStreamResponse)
  rpc Health(HealthRequest) returns (HealthResponse)
}
```

protobuf Field Number 和枚举值已经在 [`api/streamd/v1/streamd.proto`](../api/streamd/v1/streamd.proto) 冻结，后续版本只能新增，不能修改或复用。

## 3. 公共类型

```text
StreamRef {
  namespace string
  stream    string
}

InputRecord {
  headers map<string, bytes>
  payload bytes
}

StoredRecord {
  sequence uint64
  recorded_at timestamp
  request_id bytes
  producer string
  headers map<string, bytes>
  payload bytes

  storage_entry_id uint64  // 诊断字段，不是 Cursor
}
```

公共响应不暴露 StreamID 和 Byte Offset。管理员诊断接口可以显示，但不能形成客户端兼容契约。

## 4. Append

```text
AppendRequest {
  stream StreamRef
  expected_sequence uint64
  request_id bytes
  record InputRecord
}

AppendResponse {
  sequence uint64
  next_sequence uint64
  recorded_at timestamp
  storage_entry_id uint64
  deduplicated bool
  durability Durability
}
```

行为由 Append Commit Protocol 固定。成功响应表示：

- Record 已 Committed/Applied；
- 立即从 Primary Read 可见；
- 当前 Durability 保证已经满足；
- 相同请求重试返回相同 Sequence。

## 5. AppendBatch

```text
AppendBatchRequest {
  stream StreamRef
  expected_sequence uint64
  request_id bytes
  records[] InputRecord
}

AppendBatchResponse {
  first_sequence uint64
  next_sequence uint64
  record_count uint32
  first_recorded_at timestamp
  last_recorded_at timestamp
  first_storage_entry_id uint64
  last_storage_entry_id uint64
  deduplicated bool
  durability Durability
}
```

- 只支持一个 Stream；
- Record 顺序与请求数组相同；
- 全部成功或全部失败；
- Read/Subscribe 永远不观察到部分 Batch；
- 重试必须携带 Batch 起始 Expected Sequence。

## 6. Read

```text
ReadRequest {
  stream StreamRef
  from_sequence uint64
  max_records uint32
  max_bytes uint64
}

ReadResponse {
  records[] StoredRecord
  next_sequence uint64
  current_next_sequence uint64
}
```

规则：

- `from_sequence == current_next_sequence` 返回空成功；
- `from_sequence > current_next_sequence` 返回 `SEQUENCE_AHEAD`；
- 同时受 Record 数和编码后响应字节上限约束；
- 如果第一条 Record 单独超过 `max_bytes`，返回 `RECORD_TOO_LARGE` 并给出所需字节，不返回部分 Record；
- `next_sequence` 是本响应之后继续读取的位置；
- 一个 Batch 可以跨 Read 响应边界，因为写入原子性不等于读取必须整 Batch返回；每条 Record 都已完整提交。

V1 Read 由 Single 节点或当前 Primary 提供强一致读取。Standby Read 默认不对公共 API 开放。

## 7. Subscribe

```text
SubscribeRequest {
  stream StreamRef
  from_sequence uint64
  max_batch_records uint32
  max_batch_bytes uint64
  heartbeat_interval duration
}

SubscribeResponse {
  records[] StoredRecord
  next_sequence uint64
  current_next_sequence uint64
  heartbeat bool
}
```

语义：

1. 从 `from_sequence` 补齐历史；
2. 到 Tail 后等待合并式通知；
3. 新 Record 提交后继续 Read；
4. at-least-once；
5. 客户端仅在处理完成后持久化 `next_sequence`；
6. 连接断开后从最后持久化位置重连。

服务端不保存 Consumer Offset，不为慢消费者永久缓存。发送缓冲超限返回 `SLOW_CONSUMER` 并断开。

Heartbeat 不推进 Cursor，也不证明后续 Record 不存在。

## 8. ResolveTime

```text
ResolveTimeRequest {
  stream StreamRef
  recorded_at timestamp
  mode AT_OR_AFTER | AT_OR_BEFORE
}

ResolveTimeResponse {
  found bool
  sequence uint64
  actual_recorded_at timestamp
}
```

- 查询的是 streamd 服务端 Recorded At；
- 空 Stream 返回 `found=false`；
- 超出边界返回最近合法结果或 `found=false`，具体按 Mode；
- 业务发生时间不参与此 API。

## 9. InspectStream

```text
InspectStreamRequest {
  stream StreamRef
}

InspectStreamResponse {
  exists bool
  head_sequence uint64
  next_sequence uint64
  record_count uint64
  first_recorded_at timestamp?
  last_recorded_at timestamp?
}
```

V1 `head_sequence=0`、`record_count=next_sequence`。内部 Registry Reservation 但没有用户 Record 时，公共 API 返回 `exists=false`。

## 10. Health

```text
HealthResponse {
  status STARTING | READY_READ | READY_WRITE | DEGRADED | FAILED
  role SINGLE | PRIMARY | STANDBY | RECOVERING
  term uint64?
  durability Durability
  local_durable_entry_id uint64?
  commit_entry_id uint64?
  applied_entry_id uint64?
  replication_lag_entries uint64?
  reasons[] string
}
```

Health 不包含 Payload、Token 或完整 Stream Name。

## 11. Durability

```text
SINGLE_SYNC
REPLICATED_STRICT
DEGRADED_LOCAL_ONLY
```

客户端可以要求最低 Durability：

```text
required_durability = REPLICATED_STRICT
```

服务当前模式低于要求时，请求在写 WAL 前返回 `DURABILITY_UNAVAILABLE`。V1 SDK 生产默认要求 Strict，单节点部署必须显式配置接受 Single Sync。

## 12. 错误模型

错误详情统一包含：

```text
StreamdError {
  code
  message
  retryable
  result_uncertain
  current_next_sequence?
  leader_hint?
  required_bytes?
  request_id?
}
```

| Code | gRPC Status | 结果 |
| --- | --- | --- |
| `INVALID_ARGUMENT` | INVALID_ARGUMENT | 确定未写 |
| `UNAUTHENTICATED` | UNAUTHENTICATED | 确定未写 |
| `PERMISSION_DENIED` | PERMISSION_DENIED | 确定未写 |
| `STREAM_NOT_FOUND` | NOT_FOUND | 确定未写 |
| `SEQUENCE_AHEAD` | OUT_OF_RANGE | 确定未写 |
| `SEQUENCE_CONFLICT` | ABORTED | 确定未写 |
| `RECORD_TOO_LARGE` | RESOURCE_EXHAUSTED | 确定未写 |
| `RESOURCE_EXHAUSTED` | RESOURCE_EXHAUSTED | 若入队前则确定，否则详情标记 |
| `DURABILITY_UNAVAILABLE` | FAILED_PRECONDITION | 确定未写 |
| `NOT_LEADER` | FAILED_PRECONDITION | 通常未写，按详情处理 |
| `REPLICA_UNAVAILABLE` | UNAVAILABLE | 可能不确定 |
| `DEADLINE_EXCEEDED` | DEADLINE_EXCEEDED | 不确定 |
| `SLOW_CONSUMER` | RESOURCE_EXHAUSTED | 订阅重连 |
| `DATA_LOSS` | DATA_LOSS | 失败关闭，不自动重试写 |

SDK 的首要规则是读取 `result_uncertain`，不能只按 gRPC Status 推断。

## 13. Deadline 与取消

- 客户端必须设置 Deadline；
- 服务端排队预计不能在 Deadline 前完成时尽早拒绝；
- 请求进入 WAL Queue 后取消不能撤销写入；
- 因取消/超时返回时标记 `result_uncertain=true`；
- Read/Subscribe 取消不改变任何服务端 Cursor。

## 14. 身份与授权

连接使用 mTLS。认证层产生：

```text
Principal {
  tenant
  service
  instance
}
```

`producer` 从 Principal 生成并持久化，客户端 Header 不能覆盖。

授权粒度：

```text
namespace + stream_prefix + operation
```

Operation：`append`、`read`、`subscribe`、`inspect`。Health 分为匿名存活检查和受保护详细检查。

## 15. 限制

V1 格式硬上限之外，服务配置默认使用更小限制：

- Namespace/Stream Name 长度；
- 单 Record/Batch 字节；
- Header 数与总字节；
- Read/Subscribe 响应字节；
- 单连接并发 RPC；
- 单 Principal/Namespace QPS、Bytes/s 和 Subscribe 数。

服务在启动时验证配置不能超过存储格式硬上限。

## 16. HTTP Gateway

V1 核心只冻结 gRPC 语义。HTTP Gateway 可以独立提供，但必须：

- 使用 Base64 表示二进制 Request ID/Header Value/Payload；
- 保留 64-bit 整数精度，JSON 中使用十进制字符串；
- 映射相同 Error Code 和 `result_uncertain`；
- 不引入 Blind Append 或不同的幂等规则。

Gateway 不阻塞存储引擎 V1 实现。

## 17. SDK 责任

- 自动生成并持久化 Request ID；
- 维护每 Stream Expected Sequence；
- 不确定错误后原样重试；
- 冲突时把决定权交给业务，不自动跳到 Tail；
- Read/Subscribe 只在处理完成后保存 Cursor；
- 记录响应 Durability；
- 遵守服务端 Backoff/Retry Hint。

## 18. 兼容性

- protobuf Field Number 永不复用；
- 新字段必须有安全默认值；
- 新 Error Code 对旧 SDK 默认按 `retryable/result_uncertain` 处理；
- 修改 Sequence、Batch、Durability 或错误不确定性语义需要 API Major Version；
- 服务端至少支持一个滚动升级窗口内的前一 API Minor Version。

## 19. 契约测试

- 每个 Expected Sequence 分支；
- Request Hash/Dedup 跨 SDK 语言一致；
- Batch 原子可见；
- 64-bit Sequence JSON/gRPC 精度；
- Deadline 在 WAL 入队前后；
- 所有错误的 `result_uncertain`；
- Subscribe 断线、重复、慢消费者和心跳；
- 权限 Prefix 边界和 Producer 防伪造；
- 新旧客户端字段兼容。
