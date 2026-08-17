# streamd Readiness 与诊断契约

| 属性 | 内容 |
| --- | --- |
| 状态 | V1 Draft |
| 范围 | `/readyz`、`/diagnostics` 与 gRPC `Health` 的统一状态模型 |
| 安全边界 | 只读、无副作用、仅通过 loopback 管理监听提供 |

## 1. 单一状态源

节点运行时必须只有一个 Diagnostics Provider。以下消费者读取同一份状态快照，不能分别推导：

- `/readyz`：供编排器决定节点是否可以接收其角色对应的流量；
- `/diagnostics`：供人员和自动化读取完整的结构化状态；
- `streamd_write_ready` 等 Prometheus 指标；
- Primary/Single 的 gRPC `Health`。

Provider 只读取内存中的 Engine、Leadership 或 Receiver 状态，不执行网络探测、不触发恢复、续租、
Checkpoint、Snapshot 或复制。一次 HTTP 请求内的所有字段来自同一逻辑快照。

## 2. HTTP 语义

管理接口只允许 `GET` 和 `HEAD`，其他方法返回 `405 Method Not Allowed`。

- `/readyz`：`ready=true` 返回 HTTP 200，否则返回 HTTP 503；响应体始终是完整诊断 JSON；
- `/diagnostics`：只要诊断处理器可运行就返回 HTTP 200，节点故障由 JSON 的 `status` 和 `ready` 表达；
- 两者返回 `Content-Type: application/json` 和 `Cache-Control: no-store`；
- `/livez` 仍只证明进程事件循环可响应，不检查数据路径；
- 响应不包含文件路径、证书、Principal、Namespace、Stream、Request ID 或原始错误文本。

## 3. V1 JSON

```json
{
  "schema_version": "v1",
  "status": "ready_write",
  "ready": true,
  "write_ready": true,
  "role": "primary",
  "durability": "replicated_strict",
  "term": 7,
  "lease_expires_at": "2026-08-17T12:00:15Z",
  "watermarks": {
    "appended": 100,
    "local_durable": 100,
    "replicated": 100,
    "committed": 100,
    "applied": 100
  },
  "replication_lag_entries": 0,
  "apply_lag_entries": 0,
  "reasons": []
}
```

缺失水位编码为 `null`，因此 Entry 0 与“尚无 Entry”不会混淆。Single 的 `term` 为 0 且没有
`lease_expires_at`；Standby 没有 `replicated` 水位，`write_ready` 永远为 false。

固定枚举：

- `status`：`starting`、`ready_read`、`ready_write`、`degraded`、`failed`；
- `role`：`single`、`primary`、`standby`、`recovering`；
- `durability`：`single_sync`、`replicated_strict`；
- `schema_version`：V1 恒为 `v1`。

## 4. Ready 定义

`ready` 是当前角色的部署 Readiness，不等同于 `write_ready`：

- Single：Engine 无 Fatal、未 Drain、写 Guard 可用时两者均为 true；
- Primary：还必须持有当前 Term 的安全 Lease，Strict 提交路径可用；
- Standby：Receiver 状态可读且未 Fatal 时 `ready=true`、`write_ready=false`；
- Recovering、Drain 中的节点和 Failed 节点：`ready=false`；
- `ready=false` 不触发自动降级 Durability。

## 5. Reason 契约

每个原因包含稳定 `code` 和面向人员的固定 `message`。V1 Code：

| Code | 含义 |
| --- | --- |
| `commit_core_failed` | Engine/Commit Core 进入 Fatal 状态 |
| `server_draining` | 节点正在受控停止，不再接收新请求 |
| `write_guard_unavailable` | 当前写 Guard 不允许分配或提交 WAL |
| `lease_unsafe` | Primary Lease 缺失、过期或进入 Safety Margin |
| `replication_state_unavailable` | Standby Receiver 状态不可安全读取 |
| `state_inconsistent` | 角色、Term 或水位不满足内部不变量 |

不得把 `error.Error()` 直接放入 `code` 或 `message`。详细底层错误只写结构化日志和 Trace。
Reason 顺序按上表优先级固定，测试和自动化不得依赖自然语言文本。

## 6. gRPC Health 映射

Primary/Single 的 `HealthResponse` 从同一快照映射：

- `status`、`role`、`term`、`durability` 和水位保持一致；
- `reasons` 返回稳定 Reason Code，而非底层错误字符串；
- HTTP JSON 是运维诊断契约，protobuf Field Number 不因本接口改变；
- Standby 不注册公共 `StreamService`，因此只通过管理接口暴露诊断状态。

## 7. 兼容性与测试

- 可以在 V1 JSON 末尾新增可选字段，不能改变现有字段含义或枚举值；
- 删除/重命名字段、改变 HTTP 状态规则或 Reason Code 需要新 `schema_version`；
- 单元测试覆盖 Entry 0、空水位、Drain、Lease 失效、Fatal 和 Standby；
- Compose HA 测试必须同时验证 Primary 与 Standby 的角色、同一 Term、Readiness 和水位；
- 诊断接口测试不得依赖 Docker Socket，也不得放宽生产管理监听的 loopback 限制。
