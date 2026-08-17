# streamd 生产运维设计

| 属性 | 内容 |
| --- | --- |
| 状态 | Draft / 生产化基线 |
| 范围 | 部署、配置、容量、监控、备份恢复、切换、升级与安全运行 |
| 默认生产模式 | Primary + Standby + 外部协调器，REPLICATED_STRICT |

## 1. 运维原则

- 数据正确性优先于写可用性；
- 不因超时、磁盘压力或副本故障自动降低 Durability；
- Readiness 表示真实可服务状态，不只表示进程存活；
- 所有切换、Degraded、恢复和删除物理副本操作可审计；
- 自动化必须调用与人工相同的受控管理接口；
- 生产故障中不修改二进制文件格式或手工编辑 Manifest。

## 2. 部署模式

### 2.1 开发/测试单节点

```text
Client -> streamd SINGLE_SYNC -> Local SSD
```

适合开发和性能基线。机器/磁盘永久丢失时不保证 RPO=0。

### 2.2 生产 Strict 主备

```text
Clients -> Primary -> Standby
              |
         Coordinator
              |
        Object Storage
```

- 两个数据节点位于独立故障域；
- Coordinator 使用奇数成员的一致性集群；
- Primary/Standby 使用本地 SSD，不共享可写数据目录；
- Object Storage 保存可安装 Snapshot 和归档 Segment；
- 服务发现只把写流量发送到持有有效 Lease 的 Primary。

### 2.3 禁止拓扑

- 两节点互相投票自动选主；
- 主备共享同一个可写 NFS 数据目录；
- 多进程同时打开同一数据目录；
- Coordinator 与两个数据节点全部位于同一单机；
- 把异步对象存储上传当成 Strict Standby。

## 3. 主机与文件系统

### 3.1 要求

- 64-bit Linux；
- 本地 SSD/NVMe；
- 文件系统支持可靠 `fsync`、目录 `fsync` 和原子 rename；
- 关闭会把写错误静默吞掉的缓存策略；
- 数据盘与系统日志盘最好隔离；
- 时间通过 NTP/PTP 同步并监控漂移。

### 3.2 目录权限

- streamd 使用独立系统用户；
- 数据根不允许其他服务写；
- 文件默认 `0640`、目录 `0750` 或更严格；
- TLS Key、对象存储凭据不放入数据目录；
- 禁止自动跟随数据根外的符号链接。

### 3.3 容量预留

磁盘不能按“当前数据大小”配满。至少考虑：

```text
live segments
+ active/sealed WAL
+ one max flush output
+ one max merge output
+ snapshot staging
+ object upload staging
+ trash grace period
+ filesystem reserve
```

最终比例由 Benchmark 和工作负载固定。

## 4. 配置分类

### 4.1 Immutable Identity

```text
cluster_id
group_id
node_id
data_directory
```

初始化后不能在线修改。

### 4.2 Restart Required

```text
listen addresses
TLS identity
coordinator endpoints
object store backend
format writer version
shard count
```

### 4.3 Dynamic Safe

```text
rate limits
cache budgets
merge/scrub/upload bandwidth
snapshot schedule
log level
alert thresholds
```

动态配置必须版本化、校验、审计并支持回滚。不能动态改变格式语义、Durability 或 Sequence 规则。

## 5. 启动与 Readiness

V1 单节点进程使用 JSON 配置启动：

```bash
streamd -config /etc/streamd/streamd.json
```

配置结构见 [`configs/streamd.example.json`](../configs/streamd.example.json)。gRPC 监听强制 TLS 1.3 和已验证客户端证书；管理监听必须绑定 loopback，仅提供 `/livez`、`/readyz`、`/diagnostics` 和 `/metrics`。`/readyz` 与 `/diagnostics` 的字段和状态码遵循[诊断契约](diagnostics.md)。配置文件中的 URI SAN 到 Principal 映射使用精确匹配，授权规则按 Namespace、Stream Prefix 和 Operation 判断。若配置 `otlp_trace_endpoint`，进程通过 TLS OTLP/gRPC 导出 Trace。

本地 Segment Compaction 使用以下重启生效配置：

| 字段 | 默认值 | 约束 |
| --- | ---: | --- |
| `compaction.min_segments` | 32 | 至少为 2；达到该数量后尝试合并 |
| `compaction.max_input_segments` | 8 | 2 到 `min_segments`；限制单次输入数量 |
| `compaction.max_input_bytes` | 67108864 | 至少 1 MiB；限制单次 Merge 内存与 IO 放大 |

每个 Checkpoint 周期最多执行一次 Merge。超过阈值但找不到满足字节预算的相邻 Segment 时会跳过，
不能通过增大内存占用强制满足 Segment 数目标。打开的 Segment Reader 由独立 Handle Cache 限制，
当前默认最多保留 64 个空闲/复用 Handle。

Strict 节点使用同一个二进制。Primary 的关键配置如下，Standby 将 `role` 改为 `standby`，且不配置
`peer_*`：

```json
{
  "replication": {
    "role": "primary",
    "peer_address": "dns:///streamd-standby.internal:7443",
    "peer_server_name": "streamd-standby.internal",
    "peer_node_id": "44444444-4444-4444-4444-444444444444",
    "lease_ttl": "15s",
    "lease_safety_margin": "3s",
    "renew_interval": "3s",
    "max_entries": 1024,
    "max_bytes": 16777216,
    "etcd": {
      "endpoints": ["https://etcd-1.internal:2379", "https://etcd-2.internal:2379", "https://etcd-3.internal:2379"],
      "prefix": "/streamd/v1",
      "dial_timeout": "5s",
      "server_name": "etcd.internal",
      "certificate_file": "/etc/streamd/etcd/client.crt",
      "private_key_file": "/etc/streamd/etcd/client.key",
      "ca_file": "/etc/streamd/etcd/ca.crt"
    }
  }
}
```

节点证书必须同时满足 TLS DNS 校验，并包含复制协议规定的 Node URI SAN。Primary 只有在 etcd
线性事务取得新 Term/Lease、旧 Leader Key 已消失、Standby 前缀追平后才打开公共 Stream API。
Standby 只注册内部 ReplicationService，不接受公共 Append。所需 WAL 已 GC 时 Primary 保持不就绪，
运维先按 Snapshot Install Runbook 恢复 Standby，再重启 Primary 完成增量追赶。

状态：

```text
STARTING
RECOVERING
READY_READ
READY_WRITE
DEGRADED
FAILED
```

### 5.1 Liveness

只证明进程事件循环仍工作，不检查对象存储或 Standby，不应触发频繁重启恢复中的节点。

### 5.2 Readiness Read

要求 Manifest/Segment/WAL 恢复完成，`applied == recovered_commit`，没有未解决数据损坏。

### 5.3 Readiness Write

额外要求：

- 磁盘高于安全水位；
- WAL Writer 正常；
- 当前 Durability 可满足；
- Primary Lease 有效且剩余时间超过安全裕量；
- Strict Standby 可复制或系统明确阻止写。

## 6. 优雅关闭

1. 从服务发现撤下 Write Endpoint；
2. 停止接受新 Append；
3. 等待已入队请求 Commit/明确失败；
4. 停止新 Subscribe 并通知客户端重连；
5. Flush 可选，不强制等待大型 Merge；
6. 持久化 State Checkpoint；
7. 释放 Lease；
8. 关闭文件和进程锁。

超时强制退出仍依赖 WAL Recovery，不得为追求“干净退出”无限等待。

## 7. 监控

### 7.1 可用性

- Role、Term、Leader、Lease Remaining；
- Ready Read/Write；
- RPC success/error/result_uncertain；
- Append/Read/Subscribe P50/P95/P99/P999；
- Active Connections 和 Slow Consumers。

### 7.2 持久性

- appended/durable/replicated/commit/applied watermarks；
- replication lag entries/bytes/time；
- local/remote fsync latency；
- WAL bytes、oldest retained Entry；
- latest installable Snapshot age/checkpoint；
- checksum/data loss errors。

### 7.3 存储

- disk used/free/inode/forecast；
- MemTable/Flush/Merge queue；
- Segment/Extent count；
- Snapshot/Pin/Trash bytes；
- Object upload/read latency/error；
- Scrub last success and corrupt artifacts。

### 7.4 资源

- CPU、RSS、GC、FD、mmap、goroutine/thread；
- cache bytes/hit/eviction；
- disk latency/queue/utilization；
- network throughput/retransmit；
- coordinator request latency/error。

## 8. 告警分级

### P0

- 两个合法 Write Leader；
- 已确认 Record 丢失或已提交历史分叉；
- 当前唯一数据副本 checksum 失败；
- Strict 模式返回成功但未双副本 durable。

### P1

- Primary 不可写且无法自动恢复；
- Lease 即将到期/持续续约失败；
- Standby Lag 超过 WAL 保留且无可安装 Snapshot；
- 磁盘进入 Critical；
- Snapshot 长期不可安装；
- 恢复失败关闭。

### P2

- P99/P999 超 SLO；
- Merge/Flush backlog；
- Snapshot/Scrub/Upload 失败；
- Cache/FD/内存接近上限；
- Trash/Pin 异常增长。

每个告警必须链接 Runbook，不只给指标名。

## 9. 备份与 Snapshot

Single V1 提供离线工具：

```bash
streamd-tool scrub -data /var/lib/streamd
streamd-tool snapshot -data /var/lib/streamd -out /backup/streamd-snapshot-001
streamd-tool verify-snapshot -path /backup/streamd-snapshot-001
```

三个命令都会失败关闭。`scrub` 需要取得数据目录独占锁并逐 Frame、Segment SHA-256、Manifest 引用、Stream Extent 和 WAL 链校验；`snapshot` 同样要求节点离线，先完成 Checkpoint，再原子发布包含 CURRENT、Manifest、全部 Segment 和 Snapshot Manifest 的新目录。目标目录不得位于数据根内部且必须尚不存在。

离线 `snapshot` 只接受 Single 数据目录。它遇到 PRIMARY、STANDBY 或 RECOVERING Replication
State 时必须拒绝，不能按 Single 恢复规则把物理 WAL 尾部推断为 committed。Strict HA Snapshot
只能由运行中的 Strict Primary Engine 创建：创建前后都要求节点无 fatal、Lease Guard 可写，并验证
Snapshot Checkpoint 不晚于 committed watermark。当前 `CreateOnline` 是内部集成入口，尚未提供管理
RPC 或周期调度；在该入口接入以前，Strict HA 不得使用离线命令替代。

### 9.1 策略

- 周期生成完整可安装 Snapshot；
- Snapshot 发布前验证全部 Artifact；
- 至少保留当前和前一个已验证 Snapshot；
- 跨故障域保存对象副本；
- 定期在隔离环境执行真实 Restore Drill；
- 记录 Checkpoint Entry、创建时间、大小和校验状态。

### 9.2 备份不是复制

Strict Standby 提供单节点故障 RPO=0；Snapshot 提供磁盘全损、误操作和长期恢复来源。二者不能互相替代。

### 9.3 Restore Drill

1. 新建空数据目录和新 Node ID；
2. 下载 Snapshot；
3. 校验并安装；
4. 以隔离只读模式启动；
5. 随机/全量校验 Stream；
6. 记录 RTO 和缺陷；
7. 销毁临时环境。

没有 Restore Drill 的 Snapshot 不视为已验证备份体系。

### 9.4 WAL 回收

WAL 只能在节点离线时通过已验证且固定在数据根 `snapshots/` 下的 Snapshot 回收：

```bash
streamd-tool collect-wal \
  -data /var/lib/streamd \
  -snapshot /var/lib/streamd/snapshots/checkpoint
```

命令同时校验 Manifest 覆盖、Snapshot Checkpoint、Strict 副本 durable 水位和连续 WAL
前缀，并原子推进 `earliest_wal_entry_id`。禁止直接删除 `wal/` 文件。可选的
`-max-retained-bytes` 只报告保留压力，不会越过安全水位强制删除。

## 10. Failover Runbook

### 10.1 自动切换前提

- Coordinator 多数可用；
- Standby Health/日志前缀可验证；
- 旧 Primary Lease 已失效；
- 基础设施 Fencing 能阻止旧主继续写。

### 10.2 步骤

1. 确认旧 Primary 不再持有合法 Lease；
2. Coordinator 分配新 Term；
3. Standby 恢复 durable suffix；
4. 校验 Stream Tail 和 Commit；
5. 获得新 Lease；
6. 标记 READY_WRITE；
7. 服务发现切流；
8. 监控错误、Lag 和不确定重试；
9. 旧主按 Rejoin 流程恢复。

### 10.3 禁止

- 只因 Ping 不通同时启动两边写；
- 在旧主 Lease 未失效时人工强升；
- 为缩短 RTO 跳过 WAL/CRC 检查；
- 把旧主数据目录直接挂载给新主。

## 11. Rejoin Runbook

1. 确认节点没有旧 Lease；
2. 以 RECOVERING/STANDBY 启动；
3. 比较 Term、Snapshot 和日志前缀；
4. 截断仅未提交冲突尾部，或安装 Snapshot；
5. 追赶到 Primary Commit；
6. 完成 Scrub/Health；
7. 才进入可用 Standby。

当 Primary 报告 `NEEDS_SNAPSHOT` 时，停止 Standby，在协调器当前 Term 下执行：

```bash
streamd-tool verify-snapshot -path /srv/streamd-snapshots/latest
streamd-tool install-snapshot \
  -data /var/lib/streamd \
  -path /srv/streamd-snapshots/latest \
  -term <current-term> \
  -leader-id <current-primary-node-id>
```

安装崩溃后可显式执行 `streamd-tool resume-install -data /var/lib/streamd`；正常 `streamd`
启动也会在存储恢复前自动续做安装事务。

发现两个已提交前缀冲突立即 P0，禁止自动选择。

## 12. Degraded Mode

默认禁用。若产品决定启用：

- 需要双人或等价高权限审批；
- 明确记录开始时间、Term、操作者、原因和潜在丢失起点；
- API 响应返回 `DEGRADED_LOCAL_ONLY`；
- 持续 P1 告警；
- 禁止自动 Failover，除非接受已确认尾部丢失；
- Standby 追平 durable 水位后才能退出；
- 生成事件审计和事后复盘。

如果业务不能接受成功写入丢失，就不部署该能力。

## 13. 磁盘 Runbook

### HIGH

- 加速安全 Flush/Upload；
- 暂停低优先级 Merge/Scrub；
- 检查 Pin/Trash/WAL GC；
- 预测耗尽时间并扩容。

### CRITICAL

- 停止新 Append；
- 保持 Read 和恢复能力；
- 不删除逻辑历史；
- 扩容或完成已经验证的远端迁移。

### FULL

- 节点保持失败保护；
- 禁止手工 `rm` Segment/WAL；
- 先增加空间，再按引用图恢复后台任务。

## 14. 数据损坏 Runbook

1. 停止相关 Shard 写入；
2. 保存日志、Manifest、Artifact ID 和 checksum 证据；
3. 判断是否有 Standby/Object/Snapshot 等价副本；
4. 从验证副本恢复到 Staging；
5. 校验完整 Artifact；
6. 通过 Manifest Generation 发布替换；
7. 全量 Scrub 受影响范围；
8. 调查硬件、文件系统和软件路径。

禁止跳过坏 Frame 或用零填充继续服务。

## 15. 滚动升级

### 15.1 前置

- 新旧版本 API/格式 Capability 兼容；
- 当前 Snapshot 可安装；
- Standby Lag 为零；
- 无进行中的 Failover/Snapshot Install；
- 有回滚 Binary，但确认新 Writer 尚未生成旧 Binary 不支持的格式。

### 15.2 顺序

1. 升级 Standby；
2. 追平并观察；
3. 计划切换使新版本成为 Primary；
4. 升级旧 Primary 并 Rejoin；
5. 完成新 Snapshot/Restore Smoke Test；
6. 才允许启用新 Writer Format。

格式升级与 Binary 升级分离。先部署 Reader Capability，再在整个复制组支持后切换 Writer Version。

## 16. TLS 与密钥

- 数据节点、客户端、Coordinator 全部 mTLS；
- 证书绑定 Cluster/Node/Service Identity；
- 私钥不进入 Stream Payload、日志、Snapshot Manifest；
- 支持双证书重叠窗口轮换；
- 对象存储使用短期凭据和最小权限；
- 数据盘/对象存储启用静态加密；
- 密钥轮换不能原地改写不可变 Record 格式。

## 17. 日志与审计

结构化日志至少包含：

```text
timestamp, cluster_id, group_id, node_id
role, term, operation, result, duration
entry_id/sequence range when safe
artifact_id, manifest_generation
principal and request_id digest
error_code and result_uncertain
```

默认不记录 Payload、Header Value、完整 Request ID、Token 或证书私钥。Failover、Degraded、Snapshot、Restore、GC、Upgrade 和配置变更进入不可变审计流。

## 18. SLO/RPO/RTO

设计保证：

- REPLICATED_STRICT 单数据节点永久故障 RPO=0；
- SINGLE_SYNC 的机器/磁盘故障 RPO 取决于 Snapshot；
- Degraded RPO 非零且显式；
- RTO 取决于 WAL 大小、Snapshot 大小、网络和故障检测。

部署必须根据 Benchmark 填写具体 SLO/RTO，不能从设计文档臆测。

## 19. 生产上线清单

- 格式 Golden/Fuzz/Crash Test 全通过；
- 72 小时 Soak 通过；
- 容量和磁盘耗尽预测验证；
- 主备跨故障域；
- Coordinator/Fencing 验证；
- Snapshot Restore Drill 通过；
- Failover/Rejoin Drill 通过；
- Dashboard/Alert/Runbook 完整；
- TLS/授权/限流完成；
- Upgrade/Rollback 演练；
- On-call 明确；
- Degraded 默认关闭。

## 20. 管理接口边界

管理操作至少分为：

```text
read diagnostics
trigger flush/snapshot/scrub
change safe dynamic limits
planned failover
enter degraded mode
offline repair
physical garbage collection
```

后四类属于高风险操作，需要独立权限、审计和幂等 Operation ID。管理接口不能提供 Delete Record、Reset Sequence 或跳过 checksum。
