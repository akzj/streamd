# streamd 当前架构审计

| 属性 | 内容 |
| --- | --- |
| 审计性质 | 代码现实版整体架构审计，不是目标架构复述 |
| 审计基线 | `ddbbdd4f6202cddea07bd7e3f978415c5fa1d560` |
| 审计日期 | 2026-08-17 |
| 审计后实施 | Snapshot/WAL GC Safety；Role/Install Recovery；显式 `snapshot-primary` 运维恢复路径 |
| 覆盖范围 | API、存储、索引、Checkpoint、Compaction、恢复、Snapshot、WAL GC、Strict HA、并发与运维入口 |
| 不在本次范围 | 新功能实现、性能压测、生产部署验收、协议或格式变更 |

## 1. 审计结论

当前代码已经形成一条可以运行、测试和继续演进的单节点主路径，以及一条能够完成双 WAL
Strict Append 的主备路径。WAL、MemTable、Segment、Manifest、投影索引、mTLS/RBAC、复制水位、
Lease Fencing 和诊断接口之间已经存在真实调用关系，不是只有接口或设计占位。

但当前实现还不能按“生产级完整 HA Stream Store”验收。主要原因不是某个局部模块缺失，而是以下
跨模块闭环尚未完成：

1. 复制规划能判断 `PlanSnapshot`，Snapshot 也能创建、校验和原子安装，但运行时没有 Snapshot
   传输/安装协议；Primary 遇到该计划会直接启动失败。
2. `ResolveRejoin` 已实现并测试，但没有接入节点启动、旧主回归或 WAL 后缀处理流程。
3. 审计时 HA 数据目录的离线 Snapshot 会通过 Single Engine 恢复全部物理 WAL；审计后的 Safety
   Boundary 已拒绝该路径，但在线 HA Snapshot 尚未接入节点管理入口。
4. 审计时 WAL GC 没有取得数据目录独占锁；审计后的 Safety Boundary 已让 Retention Manager 在
   全部读取、删除和 State 发布期间持有数据根独占锁。
5. 启动仍加载全部 Segment Descriptor 和 Stream Directory；Locator/Registry 查询已经有界，
   但百万 Stream 的启动内存目标尚未实现。

因此，建议状态是：**可以继续开发，但应先完成架构决策和 P0/P1 正确性闭环，不应继续按局部模块
自然生长，也不应宣称已可生产部署。**

## 2. 代码现实中的系统边界

### 2.1 进程与入口

```text
streamd
  cmd/streamd/main.go
    -> node.LoadConfig
    -> node.Run
       ├── Single: internal/node/node.go
       └── Strict HA: internal/node/ha.go

streamd-tool（离线运维）
  cmd/streamd-tool/main.go
    ├── scrub
    ├── snapshot
    ├── verify-snapshot
    ├── install-snapshot
    ├── resume-install
    └── collect-wal

streamd-bench
  cmd/streamd-bench/main.go
```

`streamd` 对外暴露：

- mTLS gRPC `StreamService`；
- HA 节点上的 mTLS gRPC `ReplicationService`；
- 独立管理监听地址上的 `/livez`、`/readyz`、`/diagnostics` 和 `/metrics`。

当前没有业务 HTTP Gateway。管理 HTTP 不是 Record Stream API。

### 2.2 真实组件图

```text
Client
  |
  | mTLS + static RBAC
  v
StreamService
  |  limits / rate control / drain
  v
Engine Store
  ├── Stream Registry
  ├── per-stream Append Gate
  ├── Group Committer
  │    ├── WAL append + fsync
  │    ├── Strict replication
  │    └── MemTable apply
  ├── Read Store
  │    ├── MemTable
  │    ├── Locator page cache
  │    ├── Segment descriptor fallback
  │    └── Segment handle cache
  └── Maintenance
       ├── Checkpoint / Flush
       ├── Projection publication
       └── Segment Compaction

Strict Primary                         Strict Standby
  Leadership Controller                 Replication Receiver
       |                                      |
       +---- etcd Lease / Term                 +---- local WAL + fsync
       |                                      +---- committed MemTable apply
       +---- Replication RPC ----------------->+---- replication state checkpoint
```

## 3. 两种运行模式

### 3.1 Single

真实启动顺序：

```text
validate config
-> load mTLS
-> ResumeInstall
-> engine.OpenWithIdentity
-> recovery.Open
-> StreamService
-> gRPC + admin HTTP
-> periodic Checkpoint
-> bounded Compaction
```

Single 的成功 Append 在本地 WAL `fsync` 后可见。它不承诺节点或磁盘永久损坏时 RPO=0。

### 3.2 Strict Primary

真实启动顺序：

```text
ensure RECOVERING replication state
-> etcd Acquire
-> obtain globally increasing revision as Term
-> Promote local durable suffix
-> start Lease renewal
-> connect fixed peer with mTLS node identity
-> query Standby Status
-> incremental WAL catch-up
-> engine.OpenReplicated
-> expose StreamService + replication negotiation service
-> periodic storage checkpoint
-> periodic replication-state checkpoint
-> bounded Compaction
```

Primary 在 Standby 可连接并追到本地 durable watermark 以前不会开放业务服务。Strict 不会自动降级为
本地确认。

### 3.3 Strict Standby

真实启动顺序：

```text
query current leader from etcd
-> OpenStandby
-> recover only through durable committed watermark
-> open WAL History
-> expose ReplicationService only
-> periodic replication-state checkpoint
```

Standby 不注册 `StreamService`，因此当前不能承担读流量。它在内存中维护 replicated committed 数据，
但这些数据只用于恢复和 Promotion。

## 4. 数据事实、发布边界与投影

| 对象 | 当前角色 | 是否可重建 | 当前加载方式 |
| --- | --- | --- | --- |
| Record Frame | 逻辑 Record 的规范编码 | 否 | WAL/MemTable/Segment 中读取 |
| WAL | 尚未进入 Manifest Checkpoint 的顺序事实和复制日志 | 被 Segment+Snapshot 覆盖后可回收 | Active WAL 打开；History 扫描文件元数据 |
| Segment | 已 Checkpoint 的不可变 Record 事实 | 无其他副本时不可重建 | 启动读取全部 Descriptor/Directory，Payload 按需打开 |
| Registry Stream | 名称到 StreamID 分配事实 | 否 | Segment/WAL；Snapshot 只加速查询 |
| Manifest + CURRENT | 当前不可变 Segment/Artifact 集合的原子发布边界 | 不能从 Cache 推断 | 当前 Generation 常驻内存 |
| Tail Catalog | Stream Tail 投影 | 是 | Header/Footer 启动校验，Slot 按需读取 |
| Locator Snapshot/Pack | 冷 Extent 投影 | 是 | Root 常驻，Page 使用容量 256 的 LRU |
| Registry Snapshot | Registry Stream 的排序投影 | 是 | Sparse Block Index 常驻，Block 使用容量 64 的 LRU |
| Read stream cache | StreamID 到 Descriptor Extent 的回退缓存 | 是 | 容量 1024 LRU |
| Segment handle cache | 打开的 Segment Reader | 是 | 容量 64、引用计数、LRU |
| Replication State | Term、Role 和恢复水位的持久边界 | 不能用内存状态替代 | 双 Generation 指针式更新 |
| Snapshot | 某个 committed checkpoint 的可安装 Artifact 集合 | 否；它本身是恢复副本 | 当前由离线工具生成/安装 |

必须保留一个重要区分：Tail、Locator、Registry Snapshot 损坏可以回退事实数据；Manifest、唯一
Segment、WAL 未覆盖后缀或唯一 Snapshot 损坏不能按“投影损坏”处理。

## 5. 关键数据路径

### 5.1 Append 与提交

```text
1. mTLS Principal -> RBAC(namespace, operation)
2. 请求大小、批量和速率限制
3. 获取 namespace/name 对应的 per-stream gate
4. 在 Engine mutex 下解析或分配 StreamID
5. 检查 Expected Sequence / Request Hash 幂等
6. 分配连续 EntryID、Sequence、ByteOffset、RecordedAt
7. Enqueue 到 Group Committer
8. WAL append
9. local WAL fsync
10. Strict: Standby Append + Barrier + durable ack
11. 再次检查 Lease Guard
12. 推进 Primary committed watermark
13. Apply 到 MemTable
14. 异步发送 Standby cumulative CommitAdvance
15. 返回成功并唤醒 Subscribe
```

Group Commit 可以合并不同 Stream 的请求，不会把同一 Stream 的多个请求放入同一组。每个 Stream 的
Append Gate 保证 Sequence 检查、分配和完成顺序。

客户端 Context 超时不取消已 Enqueue 的提交。此时 API 返回 `ResultUncertain`，后台仍等待 Future，
完成后释放 Stream Gate 并通知 Subscribe。客户端必须使用同一 Expected Sequence 和 Request ID 重试。

Committer 在以下持久化中途错误后进入 fatal：WAL append/sync、Lease 在本地 durable 后失效、复制失败、
replica ack 不一致、MemTable Apply 失败。后续写入停止，避免在无法确定日志尾时继续分配。

### 5.2 Read、ResolveTime 与 Subscribe

Read 先通过 Registry 解析 StreamID，再组合当前 MemTable 和同一 Manifest Generation 的 Segment：

```text
active sequence -> MemTable
cold sequence   -> Locator Root/Page -> Segment Handle
locator failure -> Descriptor/Directory fallback -> Segment Handle
```

ResolveTime 当前对 Sequence 范围做二分，每个探测点再走上述 Record 读取路径。它不是独立的全局时间
索引。Subscribe 是“有界 Read + 等待 Stream 通知”的循环，不拥有独立消息队列或消费位点。

### 5.3 Checkpoint

```text
maintenanceMu
-> Engine mu
-> Committer Barrier
-> snapshot active MemTable
-> close current Committer
-> seal/rotate WAL
-> write new Segment
-> build Tail + Locator + Registry projections
-> publish Manifest
-> publish CURRENT
-> open and validate next projections
-> viewMu: atomically replace MemTable/Reader/Registry/Locator
-> create new Committer on new WAL
-> retire replaced projections
```

Checkpoint 在构造和发布期间持有 Engine `mu`，因此 Append 暂停；Read 继续使用旧视图，直到短暂的
`viewMu` 写锁切换。

### 5.4 Compaction

Compaction 由 `maintenanceMu` 与 Checkpoint 串行化。构建合并 Segment 时 Append 可以继续；发布新
Manifest 后，Compaction 在 Engine `mu` 下把 Registry Snapshot checkpoint 之后的 Overlay 合并到
新 Registry View，再以 `viewMu` 原子切换 Reader，最后退休输入 Segment 和旧投影。

当前 Merge 会读取选中 Segment 的全部 Frame 并在内存按 Stream 聚合。输入数量和字节受配置限制，
但峰值内存仍与一次 Compaction 的输入记录量相关。

### 5.5 Recovery

```text
CURRENT -> current Manifest
-> verify every referenced Segment
-> load every Segment Descriptor and Stream Directory
-> optionally open Tail/Locator/Registry projections
-> seed every Stream Tail into MemTable
-> recover Registry from Snapshot or Registry Stream
-> replay sealed WAL chain
-> replay and truncate Active WAL to last valid complete Batch
```

HA 恢复通过 `ApplyThrough=committed` 限制可见数据。Primary 若在 durable WAL 中仍有超过 durable
committed state 的后缀，必须先经过 `Promote` 处理，不能直接 `engine.OpenReplicated`。

### 5.6 Snapshot 与 WAL GC

Snapshot 模块已经实现：精确 Manifest Generation Checkpoint、内存 Pin、Artifact 逐个校验和复制、
Snapshot Manifest、安装 Journal、崩溃续装以及三组 CURRENT 指针切换。

但运行形态分裂为两类：

- `CreateOnline(store, ...)` 使用运行中的 Engine，在 Checkpoint 前后验证 Source Role、Strict
  Durability、fatal、Lease Guard 和 committed watermark，但当前节点没有调度或 RPC 接入；
- `streamd-tool snapshot` 只允许 Single 数据目录；检测到 PRIMARY、STANDBY 或 RECOVERING
  Replication State 时失败关闭；
- `streamd-tool snapshot-primary` 只允许停止的 durable Strict Primary，或由 hash-linked 紧邻前态证明
  是同 Term Primary 的 cleanly released RECOVERING；按 committed state 打开 replicated Engine，并拒绝
  任何超过 committed 的物理 WAL suffix。

WAL GC 要求当前 Manifest、已验证且位于 `data/snapshots/` 的 Snapshot，以及 replication state 的
committed 证据。它只删除被 Segment 和 Snapshot 同时覆盖的连续 sealed WAL 前缀；Catch-up Pin 会
阻止删除正在传输的文件。

## 6. Strict HA 协议现实

### 6.1 复制水位

```text
Standby:        last_appended >= local_durable >= committed >= applied
Primary Strict: last_appended >= local_durable >= replicated >= committed >= applied
```

Standby `Append` 只推进 `last_appended`；`Barrier` 执行 WAL `fsync` 并推进 `local_durable`；
`CommitAdvance` 是累积通知，只能把不晚于 durable 的完整 Batch 变为 committed/applied。

Primary 在收到 Standby 对准确 EntryID+CRC 的 durable ack 后才推进本地 committed。对 Standby 的
`CommitAdvance` 失败被忽略，因为双副本 durable 事实已经成立；后续 Append 会累积重试。若没有后续
Append 就发生 Failover，Promotion 会验证并提交物理 durable 后缀。

### 6.2 Term 与 Lease

etcd leader key 的创建事务 Header Revision 作为 Term。KeepAlive 不改 key，Renew 会同时校验：

- key 的 `ModRevision == Term`；
- value 是当前 NodeID；
- LeaseID 仍是本进程持有的 Session。

etcd Revision 是集群全局单调值，不要求在单个 replication group 内连续，因此可作为 fencing token。
Controller 使用本地计算的 `ExpiresAt` 和 Safety Margin 阻止临近过期的写。

续约失败不会立即杀进程；当旧的安全期限耗尽，`CanCommit` 会阻止新写。若失败发生在本地 WAL 已
durable 之后，Committer 进入 fatal，结果标记 uncertain。

### 6.3 Catch-up、Promotion 与 Rejoin

已接入的路径：

- Primary 启动时查询 Standby Status；
- 规划匹配前缀；
- Pin 所需 WAL 文件；
- 分批复制到 Primary 当前 local durable；
- 推进 Standby committed；
- 获得安全 Lease 后，Promotion 校验并提交本地完整 durable Batch 后缀。

未接入的路径：

- `PlanSnapshot` 会进入只读 Admin 诊断状态并生成结构化 Recovery Task，但不会自动执行该任务；
- 没有 SnapshotOffer、文件传输、安装确认和自动继续 WAL catch-up；
- `ResolveRejoin` 没有生产调用者；
- 没有自动隔离/截断旧主的 divergent uncommitted suffix；
- 空盘 Standby 不能仅靠当前运行时自动加入。

## 7. 并发、锁和所有权

### 7.1 Engine 锁层次

| 锁 | 保护内容 | 典型持有者 |
| --- | --- | --- |
| `maintenanceMu` | Checkpoint、Compaction、Close 的生命周期串行化 | 后台维护、关闭 |
| `Store.mu` | WAL 分配尾、Registry Overlay、Committer 切换、shutdown | Append、Checkpoint、Compaction、Replication State Checkpoint |
| `gateMu` + per-stream channel | Stream Gate 表和同 Stream Append 串行 | Append |
| `viewMu` | Reader、Segment set、投影和 MemTable 视图切换 | Read RLock；Checkpoint/Compaction WLock |
| `fatalMu` | Engine 首个 fatal error | 写与维护路径 |
| `notifyMu` | Subscribe waiter channel | Subscribe、Append 完成、Close |

当前主要获取顺序是：

```text
maintenanceMu -> Store.mu -> lifecycle.mu -> viewMu
Store.mu -> Committer.submitMu/stateMu
viewMu -> ReadStore.mu -> HandleCache.mu / Locator.mu
notifyMu -> viewMu（WaitForAppend 的即时检查）
```

代码中尚未发现反向获取 `viewMu -> Store.mu` 的持续路径。Compaction 在读取旧视图后先释放
`viewMu.RLock`，再获取 `Store.mu`。不过锁顺序目前只存在于实现惯例，没有形成代码注释或并发契约；
后续修改很容易引入反向依赖。

### 7.2 Committer 所有权

`submitMu` 保证 Enqueue、Barrier 和 Close 向单一 Queue 的顺序；只有 Committer goroutine 改变 WAL
提交过程。`stateMu` 保护 fatal/closed/watermarks。Enqueue 后 Committer 接管 encoded byte slice，
调用方不能再修改。

### 7.3 文件生命周期

Manifest 在新文件完整 `fsync` 后发布 CURRENT。Reader 视图切换完成后才将旧 Segment/Artifact
rename 到 `trash/`。Segment Reader Handle 有引用计数，在线 Snapshot 使用 Lifecycle Pin。WAL
Catch-up 使用独立的 WAL file Pin。

这些 Pin 都是进程内短期 Pin。持久 Snapshot 通过复制出独立 Artifact 集合获得生命周期独立性；当前
没有通用的持久 Pin Registry 或传输 Lease。

## 8. 设计声明与代码现实

| 能力 | 状态 | 代码现实 |
| --- | --- | --- |
| gRPC Record Stream API | 已实现 | Append、AppendBatch、Read、ResolveTime、Inspect、Subscribe |
| mTLS + namespace RBAC | 已实现 | 客户端与复制 Peer 均有身份校验 |
| WAL/Batch/Expected Sequence/幂等 | 已实现 | Request ID + Request Hash + 已知 Sequence 直接读取 |
| Group Commit | 已实现 | 多 Stream 合并，WAL fsync 后 Apply |
| Segment/Manifest/Checkpoint | 已实现 | 不可变文件 + 原子 CURRENT |
| 有界 Segment Handle | 已实现 | 默认 64、引用计数 LRU |
| Locator Page Cache | 已实现 | 默认 256 LRU，损坏回退 Descriptor |
| Registry Block Cache | 已实现 | Sparse Index + 默认 64 LRU，损坏回退 Registry Stream |
| 自动有界 Compaction | 已实现 | 相邻 Segment、受输入数和字节限制 |
| Strict 双 WAL Append | 已实现 | 两端 durable 后成功，不自动降级 |
| etcd Term/Lease/Fencing | 已实现 | 单 leader key，Revision Term，Safety Margin |
| 增量 WAL Catch-up | 已实现 | 启动前同步并 Pin WAL |
| Promotion | 已实现 | 更高 Term 下验证并提交 durable suffix |
| Snapshot 格式/校验/原子安装 | 已实现但未接入 HA 网络闭环 | Single 可离线创建；HA 只允许安全在线 Primary Engine 创建，尚无管理触发入口 |
| 自动 Snapshot Catch-up | 未实现 | `PlanSnapshot` 后 Primary 只提供 recovery-blocked Admin 诊断，等待运维安装 |
| 旧主 Rejoin | 仅决策函数 | 无运行时调用者，无自动 suffix 处理 |
| 自动 WAL GC | 未实现 | 只有离线工具 |
| 在线 Snapshot 调度 | 未实现 | `CreateOnline` 只有测试/库调用 |
| Standby Read | 未实现 | Standby 只注册 ReplicationService |
| HTTP Gateway | 未实现 | 只有 admin HTTP |
| 对象存储 | 未实现 | 仅格式字段与设计边界 |
| mmap 读取 | 未实现 | 当前 Segment/投影主要使用 ReadAt/pread |
| 全部 Extent 有界启动加载 | 未实现 | 所有 Descriptor/Directory 仍常驻 |
| 百万 Stream 启动验收 | 未完成 | 文档也标明为过渡实现 |
| 生产 Dashboard | 延后 | 当前只有 Prometheus 指标与诊断 JSON |

## 9. 风险清单

### P0：必须在生产或破坏性测试前解决

#### P0-1 HA 离线 Snapshot 可能包含未提交 WAL 后缀（Safety Boundary 已实施）

审计基线中的 `streamd-tool snapshot` 调用 `snapshot.Create`，后者读取 replication state 的 Term，
却用 `engine.Open` 按 Single 语义恢复数据。Single 恢复不使用 HA committed watermark，会 Apply
全部物理完整 WAL Batch。

若节点曾在“本地 WAL durable、Strict commit 未完成”后失败，离线 Snapshot 可能把未提交 Record
固化进 Manifest。`snapshot.Install` 校验 Artifact 完整性和目标水位，但无法证明来源 Snapshot 的
checkpoint 曾 committed。

已实施门禁：普通离线创建检测到 replicated role 后直接拒绝；`CreateOnline` 只接受无 fatal、Lease
Guard 安全的 Strict Primary，并验证 Checkpoint 被 committed watermark 覆盖；显式离线
`snapshot-primary` 要求 durable Primary provenance，并由 Engine 拒绝 unresolved suffix。正常关闭产生的
RECOVERING 只有在紧邻前态为同 Term Primary 时才成立。在线自动调度仍未实现，但 HA 运维恢复不再依赖
不安全的 Single 推断。

#### P0-2 WAL GC 没有代码级独占运行保证（Safety Boundary 已实施）

审计基线把 `collect-wal` 定义为离线命令，但 `retention.Open` 没有通过 `fsutil.OpenRoot` 或
`LockExistingRoot` 获取数据目录锁。若误与在线进程并发执行，它可能删除在线 WAL History 中的 sealed
文件；“靠 Runbook 不并发”不足以保护存储事实。

已实施门禁：`retention.Open` 先取得 `LockExistingRoot`，再在锁内读取 NODE、WAL、Manifest 和
Replication State；Manager 持锁完成 Snapshot 校验、WAL 删除和 State 发布，显式 `Close` 后才释放。
CLI 在成功和失败路径都关闭 Manager；在线 Store 与 Retention Manager 互相排斥，并由测试验证。

#### P0-3 Snapshot Catch-up/Rejoin 不构成自动 HA 恢复闭环（显式运维闭环已验证）

一旦 Standby 落后超过 earliest WAL、空盘或拥有 divergent suffix，自动路径仍无法恢复服务。当前
采用明确的运维状态机：停止节点、`snapshot-primary`、校验、WAL GC、重置/安装 Standby、启动后增量
追赶。Compose Strict HA 套件执行该完整路径。运行时尚未实现设计文档中的在线
SnapshotOffer/SnapshotInstalled/Rejoin 消息。

运行时现在把 `NO_RECOVERY_SOURCE`、`LOG_DIVERGED`、`NEEDS_SNAPSHOT` 和 `PlanSnapshot` 转换为
确定性 Recovery Task。Primary 保持 Lease renewal，但关闭公共 gRPC，只在 Admin 端口暴露
`snapshot_required`、恢复动作、Term、source/target、Snapshot/WAL/durable 边界和稳定 `task_id`。
Compose 套件先观察并核对任务，再用任务 Term 安装 Snapshot，最后验证增量追赶。

当前门禁：可以声称显式运维闭环拥有结构化、可审计的恢复任务并经过空盘 Standby 自动化验证；不能
声称自动 Snapshot 传输/安装、无停机恢复或通用 divergent suffix truncate 已实现。

### P1：继续扩展功能前解决或冻结明确约束

#### P1-1 角色启动与 Snapshot install resume 不统一（已实施）

审计基线中 Single 启动自动调用 `ResumeInstall`，HA 启动路径不会；HA 依赖显式
`streamd-tool resume-install`。同一个数据目录若使用错误的 Single 配置启动，`OpenWithIdentity`
也不校验已有 replication role。

已实施边界：Single、Primary、Standby 都在 Engine/Coordinator/Listener 以前调用同一 Resume helper；
Engine 的 Single open 会加载 NODE 与 Replication State，并拒绝 PRIMARY、STANDBY、RECOVERING。
Snapshot 安装后的 Standby 数据也必须用 replicated Engine 恢复。后续仍需冻结完整角色迁移状态机，
但已不能通过错误的 Single 配置绕过现有角色。

#### P1-2 全量 Descriptor/Directory 常驻

Locator 解决了正常冷读不扫描全部 Extent，但 Recovery 和 fallback 仍构造全部
`[]segment.Descriptor` 及其 Directories，Read Store 也保留副本。Segment 数和 Stream Extent 数继续
线性放大启动时间与 RSS。

#### P1-3 Single 与 HA 节点编排重复

两个入口分别实现 gRPC/admin 生命周期、Checkpoint ticker、Compaction 和 shutdown。功能增加时可能
出现 Single/Primary 行为漂移，例如 Snapshot resume 已经不同。

建议抽取通用 Node Runtime 生命周期，但不要把存储角色安全判断抽象成隐式分支。

#### P1-4 后台 maintenance 失败只记录日志

周期 Checkpoint、Replication State Checkpoint 和 Compaction 失败后继续运行。Engine fatal 会使写停止，
但非 fatal 的持续失败目前没有重试退避、failure budget 或 readiness 降级的统一策略。磁盘压力和长期
Checkpoint 失败可能直到容量耗尽才变成硬故障。

#### P1-5 Replication State 只周期落盘

这是有意避免每次 Group Commit 增加 metadata fsync，恢复逻辑也会扫描物理 WAL 补足。但是
Promotion、Snapshot、WAL GC 和运维诊断必须始终区分“持久 state 下界”和“物理 WAL 事实”，不能把
周期 state 当实时精确水位。该约束应进入模块接口和测试命名。

### P2：规模与运维成熟度

- Compaction 构建按 Stream 聚合完整输入 Frame，峰值内存尚未用生产上限验证；
- ResolveTime 是多次随机 Record 读取，不是独立时间索引；
- Locator 损坏回退会扫描全部 Descriptor，故障时延不受正常 Cache SLO 约束；
- Registry Snapshot `LookupID` 需要顺序访问 Block，虽然当前热路径不使用；
- 没有 Snapshot/WAL GC 自动调度、传输带宽隔离、持久传输 Lease 和对象存储实现；
- 单节点 etcd Compose 验证不了 quorum loss、member replacement 和长期 Lease 抖动；
- 指标已经覆盖节点、RPC 和核心水位，但 Snapshot、GC、Compaction、Cache、Pin 的细分 SLO 尚不完整；
- 所有设计文档仍标记 Draft，当前代码与目标设计的状态没有统一发布版本。

## 10. 需要架构 Review 决定的事项

### D1 Snapshot 的生产所有者

已采用双入口：在线 Snapshot 由正确角色 Engine 生成；停机维护只能用 `snapshot-primary`，由 durable
Replication State 及其 hash-linked 前态提供 Primary provenance 和 committed boundary，并拒绝 unresolved
suffix。普通 `snapshot` 永远不推断 HA commit。

### D2 HA 恢复是自动协议还是强约束运维流程

第一版已经选择强约束运维流程，同时保留未来自动协议方向：

- 自动：Replication RPC 提供 Snapshot 协商、分块传输/对象地址、Pin Lease、安装确认和后续 catch-up；
- 运维驱动：当前由结构化 Recovery Task、`snapshot-primary`、verify、install/resume 和重启 catch-up
  组成，并由 Compose HA 执行。任务身份由持久恢复事实确定性生成；节点不自动 truncate 或安装。

### D3 Rejoin 是否允许截断未提交后缀

推荐：不提供通用 WAL truncate。只在 committed prefix 已证明一致、Coordinator Term 已提升且 suffix
明确未提交时执行受审计的 suffix discard；否则安装 Snapshot。第一版可以全部走 Snapshot，牺牲 RTO
换取较小正确性表面。

### D4 Standby 是否需要读

当前系统明确是 Primary-only Read。若目标只是审计流存储，第一版保持该约束更简单；如果要读扩展，
必须定义 committed/applied lag、stale-read 契约、RBAC 和客户端路由，不能只注册 `StreamService`。

### D5 百万 Stream 是否是进入 HA 闭环前的门槛

推荐优先级：先修 P0 正确性和恢复闭环，再实现有界 Descriptor/Directory 启动。两者不要并行修改同一
Recovery 主链，以免无法区分数据安全回归和内存模型变化。

### D6 文档状态管理

推荐把文档拆成：

- `Implemented`：由代码和自动化测试持续证明；
- `Accepted Design`：已经 Review、允许实现；
- `Proposal`：仍需决定。

当前所有文档统一写 Draft，容易让“格式已冻结”和“运行闭环已实现”混在一起。

## 11. 后续开发门禁与建议顺序

在用户 Review 本审计和 D1-D6 前，不继续自动开发新模块。

Review 通过后的建议顺序：

1. 将已完成 committed boundary 校验的 `CreateOnline` 接入受控 Primary 管理入口，并继续完善数据目录角色/模式校验；
2. WAL GC 代码级独占所有权已完成；后续只在端到端运维测试中验证命令退出和锁恢复；
3. Snapshot Catch-up 已冻结为结构化运维任务；后续若增加自动传输，必须保持同一任务和安全边界；
4. 空盘 Standby/WAL 已回收恢复测试已完成；仍需补旧主 divergent suffix 的独立端到端演练；
5. Role/Install 恢复前置边界已统一；其余 gRPC/admin/maintenance 进程生命周期骨架仍待抽取；
6. 再实施 Segment Descriptor/Directory 的有界启动加载；
7. 执行规模、长稳、磁盘压力、etcd quorum 和恢复演练；
8. 根据证据把对应设计章节从 Draft 提升为 Implemented/Accepted。

每个阶段完成条件不只是单元测试，而应包括：

- 代码调用链已接入，不是只有库函数；
- crash point 前后保持同一事实边界；
- 运维误用被代码拒绝，而不是仅写在 Runbook；
- diagnostics/metrics 能解释当前阻塞原因；
- 文档的“已实现”矩阵同步更新；
- 独立 commit 后再进入下一个架构阶段。

## 12. 本次审计的证据入口

| 主题 | 主要代码 |
| --- | --- |
| Single/HA 编排 | `internal/node/node.go`, `internal/node/ha.go` |
| API 与限流/RBAC | `internal/service/server.go`, `internal/access/` |
| Engine 与锁 | `internal/storage/engine/store.go` |
| Group Commit | `internal/storage/commit/commit.go` |
| Recovery | `internal/storage/recovery/recovery.go` |
| Manifest/Segment 生命周期 | `internal/storage/manifest/store.go`, `internal/storage/lifecycle/manager.go` |
| Read/Cache | `internal/storage/read/store.go`, `internal/storage/locator/`, `internal/storage/registry/` |
| WAL History/GC Pin | `internal/storage/wal/history.go` |
| Snapshot | `internal/storage/snapshot/` |
| WAL Retention | `internal/storage/retention/manager.go` |
| Strict Replication | `internal/replication/primary.go`, `receiver.go`, `standby.go`, `catchup.go` |
| Plan/Promotion/Rejoin | `internal/replication/planner.go`, `promotion.go`, `rejoin.go` |
| Term/Lease | `internal/coordinator/etcd/coordinator.go`, `internal/leadership/controller.go` |
| 运维入口 | `cmd/streamd-tool/main.go` |

本文件描述的是上述基线代码的现实。如果代码改变了启动顺序、事实边界、锁顺序、复制水位或运维
所有权，必须先更新本审计矩阵，再继续声称架构结论仍然成立。
