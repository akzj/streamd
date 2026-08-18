# streamd 可观测性契约

| 属性 | 内容 |
| --- | --- |
| 状态 | V1 节点、RPC 与 Primary/Single Group Commit 指标已实现 |
| 范围 | Prometheus 指标、标签基数、Readiness 与告警输入 |
| 目标 | 仅依赖节点自身状态即可判断角色、写安全性、复制进度和存储压力 |

## 1. 原则

- `/metrics` 是只读管理接口，不改变节点状态；
- 指标名称和标签值是兼容性契约，删除或改名需要版本化；
- 不使用 `namespace`、`stream`、`request_id`、用户身份、文件名、错误文本作为标签；
- `role`、`durability`、`stage`、`kind`、`method`、gRPC `code` 只能取本文定义的有限集合；
- Entry ID 使用 Gauge 表示当前水位，不使用 Counter 推导恢复后的状态；
- 缺失水位由独立的 `present` 指标表达，不能把 Entry 0 与“尚无 Entry”混为一谈；
- 采集失败必须失败关闭为 `streamd_observer_collection_success 0`，不能返回看似正常的零值；
- 所有容量单位为 byte，所有时间单位为 second。

## 2. 节点与写安全

| 指标 | 类型 | 标签 | 语义 |
| --- | --- | --- | --- |
| `streamd_node_info` | Gauge | `role`, `durability` | 当前节点模式，恒为 1 |
| `streamd_leadership_term` | Gauge | 无 | 当前持久化/运行中的 HA Term；单节点为 0 |
| `streamd_lease_expires_unixtime_seconds` | Gauge | 无 | Primary Lease 到期时间；无 Lease 为 0 |
| `streamd_lease_remaining_seconds` | Gauge | 无 | 距 Lease 到期的秒数，过期后保持 0 |
| `streamd_write_ready` | Gauge | 无 | 公共 Append 是否可安全接受，布尔值 |
| `streamd_observer_collection_success` | Gauge | 无 | 最近一次存储状态采集是否成功 |

`role` 只允许 `single`、`primary`、`standby`、`recovering`；`durability` 只允许
`single_sync`、`replicated_strict`。角色和 Term 变化不使用 node/group ID 标签；身份由部署目标标签补充。

## 3. 数据推进水位

| 指标 | 类型 | 标签 | 语义 |
| --- | --- | --- | --- |
| `streamd_watermark_entry_id` | Gauge | `stage` | 对应阶段的最大 Entry ID |
| `streamd_watermark_present` | Gauge | `stage` | 对应水位是否存在 |
| `streamd_replication_lag_entries` | Gauge | 无 | `local_durable - replicated`，Standby 使用 0 |
| `streamd_apply_lag_entries` | Gauge | 无 | `committed - applied` |

`stage` 固定为 `appended`、`local_durable`、`replicated`、`committed`、`applied`。
差值在任一输入不存在时为 0；监控方必须同时检查 `present`。实现发现水位倒退或顺序违背时，
`streamd_observer_collection_success` 必须为 0。

## 4. 本地存储

| 指标 | 类型 | 标签 | 语义 |
| --- | --- | --- | --- |
| `streamd_storage_files` | Gauge | `kind` | 数据根内当前文件数 |
| `streamd_storage_bytes` | Gauge | `kind` | 数据根内当前文件字节数 |
| `streamd_disk_capacity_bytes` | Gauge | 无 | 数据根所在文件系统容量 |
| `streamd_disk_available_bytes` | Gauge | 无 | 非特权进程可用容量 |

`kind` 固定为 `wal`、`segment`、`snapshot`、`manifest`、`staging`、`trash`、`other`。
采集器只遍历固定的 streamd 数据目录，不跟随符号链接。文件在并发 rename/GC 中消失属于瞬时现象，
本轮跳过；权限、I/O 或格式之外的遍历错误使本轮采集失败。

## 5. 请求指标

| 指标 | 类型 | 标签 | 语义 |
| --- | --- | --- | --- |
| `streamd_rpc_requests_total` | Counter | `method`, `code` | 已完成 gRPC 请求数 |
| `streamd_rpc_duration_seconds` | Histogram | `method` | Append、Read、Subscribe 和复制 RPC 端到端延迟 |
| `streamd_rpc_active` | Gauge | `method` | 当前执行中的 RPC 数 |

`method` 只能是静态注册的 gRPC Full Method；`code` 是标准 gRPC Code。V1 不记录 Stream、Principal
或客户端地址标签。

### 5.1 Group Commit 指标

Engine 在真实 Committer 边界累计以下数据，Checkpoint 更换 WAL/Committer 后继续累计且不重复；
`streamd-bench` 和 Primary/Single 的生产 Prometheus Collector 消费同一快照：

| 指标 | 类型 | 标签 | 语义 |
| --- | --- | --- | --- |
| `streamd_commit_groups_total` | Counter | 无 | 已进入 Committer 处理的 Group |
| `streamd_commit_requests_total` | Counter | 无 | Group 内请求数；包含内部 Registry 请求 |
| `streamd_commit_entries_total` | Counter | 无 | Group 内 WAL Entry 数 |
| `streamd_commit_bytes_total` | Counter | 无 | Group 内编码 WAL 字节 |
| `streamd_commit_local_sync_total` | Counter | 无 | local sync 调用次数，含失败调用 |
| `streamd_commit_replicate_total` | Counter | 无 | Strict replicate 调用次数，含失败调用 |
| `streamd_commit_queue_wait_seconds_total` | Counter | 无 | 每请求从尝试进入有界队列到 Group 开始处理的累计时间 |
| `streamd_commit_stage_seconds_total` | Counter | `stage` | Group 各阶段累计时间 |
| `streamd_commit_queue_depth` | Gauge | 无 | scrape 时有界 channel 中等待的请求数 |
| `streamd_commit_queue_capacity` | Gauge | 无 | channel 固定容量 |

`stage` 只允许 `collect`、`append`、`local_sync`、`replicate`、`apply`、`process`。Group size、每请求等待和
各阶段占比由 Counter 的 `rate()` 计算，不发布进程内计算的 ratio 指标。当前累计最大 Group Size 只用于
benchmark 报告；生产监控需要分布时应增加有界 Histogram，而不是使用不可合并的进程 lifetime max。
Standby Receiver 拥有独立的 Append/Barrier/fsync 路径，不注册上述 Committer Collector；它的阶段指标
必须在 Receiver 边界单独实现，不能把 Primary 的 replicate 时间冒充成 Standby local fsync 时间。
Segment Flush、Snapshot 和 Cache 仍需在各模块拥有明确事件边界后新增，不得通过定时扫描伪造累计次数或延迟。

## 6. Readiness 对应关系

- Single：Engine 无 Fatal、未 Drain 且写路径可用时 `write_ready=1`；
- Primary：除 Engine 条件外，必须持有当前 Term 的安全 Lease 且 Strict Standby 可完成提交；
- Standby：复制状态可读取时进程 Ready，但 `write_ready` 永远为 0；
- Recovering：`write_ready=0`；
- `/readyz` 与运行时 Ready 判定共用同一个 Provider，禁止各自推导。

Readiness 原因在下一阶段通过结构化只读诊断接口暴露。错误文本只进入日志和 Trace，不进入指标标签。

## 7. 采集与测试要求

- 自定义 Collector 在每次 scrape 读取一致的状态快照；
- Collector 不持有 Engine/Receiver 锁执行文件系统遍历；
- 状态采集和磁盘采集相互独立，单项失败不阻止 Prometheus 输出其他指标；
- 单元测试覆盖空水位、Entry 0、落后、过期 Lease、目录并发变化和标签集合；
- Compose HA 黑盒测试必须断言 Primary/Standby 角色、Term、写就绪和复制/提交/应用水位；
- Dashboard 与告警只消费本文指标，不解析日志文本。
