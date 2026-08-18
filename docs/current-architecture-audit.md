# streamd 当前架构审计

| 属性 | 内容 |
| --- | --- |
| 审计性质 | 代码现实版整体架构审计，不是目标架构复述 |
| 审计基线 | `da6f134`（`main`，2026-08-18） |
| 审计日期 | 2026-08-18 |
| 相对上次审计新增 | Locator/Tail/Registry 投影外部排序与流式构建；Replicated Checkpoint durable state 顺序保持 |
| 覆盖范围 | API、存储、索引、Checkpoint、Compaction、恢复、Snapshot、WAL GC、Strict HA、并发与运维入口 |
| 验证边界 | 代码、单元/race/vet、Compose HA；不等同于性能、长稳、磁盘故障或生产部署验收 |

## 1. 审计结论

当前代码已经形成两条真实主路径：Single 本地同步存储，以及固定双节点的 Strict Primary/Standby。
后者已经在三成员 etcd 下验证双 WAL durable ack、单成员故障、quorum 丢失写入 Fencing、进程级
Failover/Failback、Snapshot 安装、WAL GC、空盘 Standby 恢复和恢复阻塞期间 Lease 失效。WAL、
Segment、Manifest、投影索引、mTLS/RBAC、复制水位和诊断不是设计占位。

历史审计列出的数据安全风险已作为回归门禁关闭：HA Snapshot 不再走 Single 恢复语义；WAL GC 持有
数据根独占锁；所有角色都会在恢复和监听前续做 Snapshot Install；Installed Snapshot 元数据也不会被
误当作可传输 Package。`PlanSnapshot` 不再只返回进程错误，而是进入只开放 loopback Admin 的
recovery-blocked 状态，输出确定性 Recovery Task。

当前仍不能按“生产级完整 HA Stream Store”验收，原因已经从局部数据正确性转向运行闭环和规模边界：

1. Snapshot 恢复是结构化人工任务，不包含传输、执行、确认或持久任务状态机；正常数据面在任务完成前
   保持关闭。
2. `ResolveRejoin` 没有生产调用者；V1 明确不自动截断 divergent suffix。独立 Compose 场景已经验证
   `LOG_DIVERGED` 进入恢复阻塞、准确报告冲突水位，并通过 Snapshot 覆盖旧后缀后重新加入；该路径仍是
   显式运维恢复，不是自动 Rejoin。
3. WAL 已回收且本地 Installed Snapshot Package 不存在时，独立 Compose 场景已经验证节点进入
   `NO_RECOVERY_SOURCE`、不开放公共 gRPC，并在人工创建 replacement Snapshot 后恢复；该路径同样不是
   自动恢复状态机。
4. 历史 Stream Directory、Extent、Tail 与 Locator Root 已不再在启动时全量加载或常驻；启动仍全量
   保留 Manifest Segment Reference 与 Registry Sparse Block Index；Locator/Tail/Registry 构建中的
   全量 Stream 投影常驻峰值已关闭，但百万 Stream 的 RSS/时延目标尚未验收。
5. 没有生产级 Snapshot/GC 调度、对象存储、容量故障策略、长稳和规模验收。

结论：**可以继续开发，当前代码适合作为正确性优先的 V1 工程基线；尚不能宣称生产就绪。后续应按
跨模块风险推进，而不是继续增加彼此孤立的存储功能。**

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

当前没有业务 HTTP Gateway。管理 HTTP 不是 Record Stream API，也没有独立认证层；配置校验强制它绑定
loopback，测试中的 Admin Proxy 只存在于隔离 Compose 网络。

当前部署单元是“一个进程、一个数据目录、一个 replication group”。Strict 数据面固定为一个 Primary
和一个 Standby，Peer NodeID/地址静态配置；etcd 只协调 Term/Lease，不保存 Record，也不把两副本扩展成
多 Standby 或数据 quorum。

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

Recovery-blocked Primary
  ├── 保持/监视当前 Lease，不开放公共 gRPC
  ├── loopback Admin: livez / readyz(503) / diagnostics / metrics
  └── deterministic Recovery Task -> 外部运维执行 Snapshot create/install

Replicated node during startup
  ├── one loopback Admin lifecycle: livez / readyz(503) / diagnostics / metrics
  ├── leadership_pending: acquire/discover coordinator leadership
  ├── replica_catchup_pending: Primary waits for configured Standby
  └── public gRPC remains closed until role-specific readiness
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
ResumeInstall
-> open coordinator client
-> start loopback Admin: starting/leadership_pending
-> ensure RECOVERING replication state
-> retry etcd Acquire for transient unavailability/current Leader
-> obtain globally increasing revision as Term
-> Promote local durable suffix
-> start Lease renewal
-> Admin: starting/replica_catchup_pending
-> connect fixed peer with mTLS node identity
-> query Standby Status
-> incremental WAL catch-up
   └── Snapshot required: Admin-only recovery-blocked state
-> engine.OpenReplicated
-> expose StreamService + replication negotiation service
-> periodic storage checkpoint
-> periodic replication-state checkpoint
-> bounded Compaction
```

Primary 在 Standby 可连接并追到本地 durable watermark 以前不会开放业务服务。等待期间同一 Admin
Listener 保持存活，`readyz=503`；公共 gRPC 仍未绑定。Strict 不会自动降级为本地确认。

### 3.3 Strict Standby

真实启动顺序：

```text
ResumeInstall
-> open coordinator client
-> start loopback Admin: starting/leadership_pending
-> retry current leader discovery for no-Leader/transient coordinator failure
-> OpenStandby
-> recover only through durable committed watermark
-> open WAL History
-> expose ReplicationService only
-> periodic replication-state checkpoint
```

Standby 不注册 `StreamService`，因此当前不能承担读流量。它在内存中维护 replicated committed 数据，
但这些数据只用于恢复和 Promotion。没有当前 Leader 时进程不再退出；它保持 not-ready Admin，直到发现
Leader 或收到 shutdown。非法 coordinator state 仍立即失败，不会被无限重试隐藏。

## 4. 数据事实、发布边界与投影

| 对象 | 当前角色 | 是否可重建 | 当前加载方式 |
| --- | --- | --- | --- |
| Record Frame | 逻辑 Record 的规范编码 | 否 | WAL/MemTable/Segment 中读取 |
| WAL | 尚未进入 Manifest Checkpoint 的顺序事实和复制日志 | 被 Segment+Snapshot 覆盖后可回收 | Active WAL 打开；History 扫描文件元数据 |
| Segment | 已 Checkpoint 的不可变 Record 事实 | 无其他副本时不可重建 | 启动验证轻量 Header/Footer；仅最新 Directory 恢复时间水位，历史 Directory 按需打开 |
| Registry Stream | 名称到 StreamID 分配事实 | 否 | Segment/WAL；Snapshot 只加速查询 |
| Manifest + CURRENT | 当前不可变 Segment/Artifact 集合的原子发布边界 | 不能从 Cache 推断 | 当前 Generation 常驻内存 |
| Tail Catalog | Stream Tail 投影 | 是 | Header/Footer 启动校验，Slot 按需读取 |
| Locator Snapshot/Pack | 冷 Extent 投影 | 是 | Header/Pack 常驻；Root `ReadAt` 二分并使用容量 1024 LRU；Page 使用容量 256 LRU |
| Registry Snapshot | Registry Stream 的排序投影 | 是 | Sparse Block Index 常驻，Block 使用容量 64 的 LRU |
| Tail Resolver cache | StreamID 到历史 Tail 的回退缓存 | 是 | 容量 1024 LRU；MemTable/WAL Overlay 优先 |
| Segment handle cache | 打开的 Segment Reader | 是 | 容量 64、引用计数、LRU |
| Replication State | Term、Role 和恢复水位的持久边界 | 不能用内存状态替代 | 双 Generation 指针式更新 |
| Snapshot | 某个 committed checkpoint 的可安装 Artifact 集合 | 否；它本身是恢复副本 | 离线工具生成/安装；在线创建仅有库入口 |

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
cold sequence   -> Locator Root(ReadAt binary search)/Page -> Segment Handle
locator failure -> Descriptor/Directory fallback -> Segment Handle
```

ResolveTime 当前对 Sequence 范围做二分，每个探测点再走上述 Record 读取路径。它不是独立的全局时间
索引。Subscribe 是“有界 Read + 等待 Stream 通知”的循环，不拥有独立消息队列或消费位点。

### 5.3 Checkpoint

```text
maintenanceMu
-> Engine mu
-> Committer Barrier
-> Strict HA: persist Replication State committed/applied floor
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

Locator/Tail 投影不会再一次性物化全部 Segment Directory。Builder 每次只访问一个 Directory，写入
定长临时 Run，以 fan-in 32 多轮外部归并；归并结果按 Stream/Sequence 排序后，每次只编码一个 Locator
Page，Root 直接顺序落入 Snapshot 输入文件，并在完成每个 Stream 时直接输出 Tail Slot。成功或失败均
清理 `.build-*`，Locator Pack、Snapshot 与 Tail Catalog 仍使用同一 Generation/Covered Entry ID。
Registry Builder 按 Sequence 校验内部 Registry Stream，以约 4 MiB Entry 分块生成排序 Run，再以
fan-in 32 多轮归并；最终 Run 分别顺序扫描以计算布局、写 Sparse Block Index 和写 Entry。输出与旧 V1
字节格式一致，不再构造完整 `[]Mapping`、`[]RegistryEntry` 或编码缓冲；`RebuildMappings` 仅保留为
Snapshot 损坏时的事实回退。

### 5.4 Compaction

Compaction 由 `maintenanceMu` 与 Checkpoint 串行化。构建合并 Segment 时 Append 可以继续；发布新
Manifest 后，Compaction 在 Engine `mu` 下把 Registry Snapshot checkpoint 之后的 Overlay 合并到
新 Registry View，再以 `viewMu` 原子切换 Reader，最后退休输入 Segment 和旧投影。

当前 Merge 会读取选中 Segment 的全部 Frame 并在内存按 Stream 聚合。输入数量和字节受配置限制，
但峰值内存仍与一次 Compaction 的输入记录量相关。

### 5.5 Recovery

```text
CURRENT -> current Manifest
-> verify every referenced Segment Header/Footer/Manifest Reference
-> read only the newest Segment Directory for global RecordedAt watermark
-> optionally open Tail/Locator/Registry projections; Locator Root remains on disk
-> keep checkpoint-only Tail in Tail Catalog + bounded Resolver cache
-> recover Registry from Snapshot or Registry Stream
-> replay sealed WAL chain; seed only WAL-active Streams on demand
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
阻止删除正在传输的文件。Retention Manager 从读取证据到删除和 State 发布全程持有数据根独占锁，
在线进程存在时离线命令立即失败。

Replication State 中的 Installed Snapshot 只证明本地数据从哪个 checkpoint 恢复，不证明对应 Package
仍可供其他副本安装。复制规划先按 WAL 事实尝试增量；只有遇到 `NO_RECOVERY_SOURCE` 或
`LOG_DIVERGED` 时才扫描 `data/snapshots/`，并且只有 Snapshot ID、Group、checkpoint 和 checksum
全部匹配且完整 `Verify` 通过的 Package 才进入 `PlanSnapshot`。正常增量协商不会为此读取大型 Artifact。

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
- Installed Snapshot metadata 与可传输 Package 分离；只有恢复确实需要时才发现并完整验证 Package；
- 获得安全 Lease 后，Promotion 校验并提交本地完整 durable Batch 后缀。
- `NO_RECOVERY_SOURCE`、`LOG_DIVERGED`、`NEEDS_SNAPSHOT` 和 `PlanSnapshot` 转为 Recovery Task；
- Recovery Task 由 action/reason、Term、Group、source/target、Snapshot、earliest WAL 和目标 durable
  position/checksum 确定性生成，同一组事实得到同一 `task_id`；
- recovery-blocked Primary 不监听公共 gRPC，只提供 Admin 诊断；Lease 失效后转为
  `failed/recovering`，任务身份保持不变。

未接入的路径：

- `PlanSnapshot` 会进入只读 Admin 诊断状态并生成结构化 Recovery Task，但不会自动执行该任务；
- 没有 SnapshotOffer、文件传输、安装确认和自动继续 WAL catch-up；
- `ResolveRejoin` 没有生产调用者；
- 没有自动隔离/截断旧主的 divergent uncommitted suffix；
- 空盘 Standby 不能仅靠当前运行时自动加入。

### 6.4 当前 HA 验收覆盖

Compose 使用真实进程、mTLS、三成员 etcd、Toxiproxy 和一次性 Volume，串行验证：

- Standby 无 Leader 时保持进程和 Admin 存活，报告 `leadership_pending`，补齐 Leader 后原进程 ready；
- Primary 无 Standby 时报告 `replica_catchup_pending` 且公共 gRPC 关闭，补齐 Standby 后原进程 ready；
- Strict append/read/idempotency 与 Primary/Standby 诊断水位；
- 一个 etcd member 失效仍可写、失去 quorum 后 Lease Safety Margin 内停止写、恢复后重新服务；
- Primary `SIGKILL` 后提升 Standby、旧主以 Snapshot 重装后 Failback；
- 离线安全 `snapshot-primary`、Snapshot verify、WAL GC、空盘 Standby 安装与增量追赶；
- Snapshot 恢复任务、公共 gRPC 关闭、`readyz=503`，以及恢复阻塞期间再次失去 quorum；
- 合法但未提交的 Standby WAL 冲突后缀触发 `LOG_DIVERGED`，Recovery Task 精确绑定目标 Entry/CRC，
  Snapshot 安装替换全部旧 WAL 后恢复 Strict Append；
- GC 后删除唯一可传输 Snapshot Package、清空 Standby，验证 `NO_RECOVERY_SOURCE` 失败关闭；再创建并
  安装 replacement Snapshot，验证历史数据和新 Strict Append；
- Standby 链路分区时 Strict Append 不能成功确认。

这些测试不覆盖磁盘满/只读、I/O 延迟或损坏、etcd member replacement、跨版本滚动升级、长时间网络
抖动、对象存储故障和生产数据规模。CI 每次 push/PR 运行一次完整 HA，nightly 重复十次；这仍是验收
门禁，不是长期稳定性或容量证明。

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
没有通用的持久 Pin Registry 或传输 Lease。当前 WAL GC 是离线命令，数据根独占锁排除了它与在线
Catch-up 的跨进程竞争；同一 `wal.History` 内部的 Pin/Collect 由 mutex 和文件 pin 计数保护，并有并发
回归测试证明持有 Pin 时 Collect 可完成但不会删除被 Pin 文件。

## 8. 当前能力矩阵

状态只使用三类：`Implemented` 表示生产调用链已接入且有自动化证据；`Bounded` 表示已实现但存在明确
运行边界；`Not implemented` 表示只有设计、库函数或没有入口。

| 能力 | 状态 | 代码现实与边界 |
| --- | --- | --- |
| gRPC Record Stream API | Implemented | Append、AppendBatch、Read、ResolveTime、Inspect、Subscribe |
| mTLS + namespace RBAC | Implemented | 业务 Client 和复制 Peer 均验证身份；Admin 仅允许 loopback |
| WAL/Batch/Expected Sequence/幂等 | Implemented | Request ID + Request Hash；不确定结果可用原请求重试 |
| Group Commit | Implemented | 多 Stream 合并；本地 fsync 后才进入后续提交阶段 |
| Segment/Manifest/Checkpoint | Implemented | 不可变文件、校验 Footer、原子 CURRENT、Crash Test |
| Tail/Locator/Registry 投影 | Implemented | 损坏可回退事实数据；正常查询使用有界 Cache |
| Segment Handle/Locator Root/Locator Page/Registry Block Cache | Implemented | 默认容量 64/1024/256/64；引用计数或 LRU |
| 自动有界 Compaction | Bounded | 输入 Segment 数/字节有界；内部仍按 Stream 聚合完整输入 Frame |
| Strict 双 WAL Append | Implemented | 两端 durable 后成功；不自动降级 |
| 数据副本拓扑 | Bounded | 固定 1 Primary + 1 Standby；静态 Peer；不支持多 Standby/数据 quorum |
| etcd Term/Lease/Fencing | Implemented | Revision Term、Lease value/ModRevision/LeaseID 校验、Safety Margin |
| Replicated Startup Admin | Implemented | 单一 Admin 跨 starting/recovery/ready；暂态 Coordinator/Peer 等待不开放公共 gRPC |
| 增量 WAL Catch-up/Pin | Implemented | Primary 开放业务监听前追赶；传输期文件 Pin；离线 GC 由数据根锁互斥 |
| Promotion | Implemented | 新 Term 下验证并提交本地完整 durable Batch suffix |
| Snapshot 格式/校验/原子安装 | Implemented | 安装 Journal、Crash Resume、角色与 committed boundary 校验；新 WAL 发布后清除被替换历史 |
| Recovery Task | Bounded | 结构化且确定性；只存在于运行时诊断，不执行、不确认、不授权 |
| 显式 Snapshot 恢复 | Implemented | 离线 create/verify/install/resume + Compose 空盘恢复证据 |
| 自动 Snapshot 传输/安装 | Not implemented | 没有分块传输、对象地址、持久任务或安装确认协议 |
| 旧主 Rejoin | Bounded | `ResolveRejoin` 仅决策函数；生产路径优先 Snapshot 重装 |
| WAL GC | Bounded | 安全离线命令；无在线/周期调度 |
| 在线 Snapshot | Bounded | `CreateOnline` 正确性边界已实现，但没有受控管理入口或调度 |
| Standby Read | Not implemented | Standby 只注册 ReplicationService |
| HTTP Gateway | Not implemented | 只有 loopback Admin HTTP |
| 对象存储/归档 | Not implemented | 没有实现和故障语义 |
| 有界启动元数据 | Bounded | 历史 Directory/Extent/Tail/Locator Root 已延迟加载；Segment Reference、Registry Sparse Index 仍随规模增长 |
| 百万 Stream/长稳/磁盘压力验收 | Not implemented | 现有 benchmark 不是生产容量证明 |
| Dashboard | Deferred | 当前只有 Prometheus 指标和诊断 JSON |

## 9. 风险清单

### 9.1 已关闭的历史 Safety 风险

以下问题保留为回归门禁，不再列为当前 P0：

1. **HA Snapshot 混用 Single 语义**：Single open 会拒绝 replicated role；`snapshot-primary` 通过 durable
   role provenance、`ExpectedStateID` 和 `RejectUncommittedSuffix` 绑定 committed 边界。
2. **WAL GC 与在线进程并发**：Retention Manager 全程持有数据根独占锁，读取、删除和 Replication
   State 发布属于同一个离线所有权区间。
3. **Snapshot Install 启动恢复不一致**：Single/Primary/Standby 都在 Coordinator、Engine 和 Listener
   以前执行 `ResumeInstall`。
4. **Snapshot required 只有非结构化错误**：协议错误已映射为确定性 Recovery Task；数据面关闭、
   `readyz=503`，Lease 丢失后任务仍可审计。
5. **Snapshot 覆盖非空副本后遗留旧 active WAL**：安装 Journal 下先发布新的 `WAL-CURRENT`，再删除并
   fsync 全部被替换 WAL；续装可重复执行同一替换。单元测试直接重开完整 WAL History，Compose 使用
   含冲突后缀的 Standby 验证进程重启。
6. **Installed Snapshot 被误当作恢复源**：复制规划不再从 Replication State 直接发布 Snapshot；仅在
   恢复需要时发现并验证真实 Package。Compose 删除唯一 Package 后验证 `NO_RECOVERY_SOURCE`，并通过
   replacement Snapshot 完成恢复。
7. **Catch-up Pin 与 WAL GC 竞争**：离线 GC 与在线节点由数据根锁互斥；同一 History 内 Pin/Collect
   并发测试验证被 Pin 文件不会删除且操作不死锁。
8. **启动阶段管理面不连续**：Replicated 节点在 Coordinator/Peer 等待前启动同一 loopback Admin；
   transient/no-Leader 由进程内重试，结构化 reason 和 Compose 场景证明公共 gRPC 保持关闭并可原进程
   转入 ready。
9. **Manifest checkpoint 超前于 durable committed state**：Strict Primary 的周期 Checkpoint 在同一 Engine
   临界区先 Barrier 并持久化 Replication State，再发布可能覆盖相同 Entry 的 Manifest；Crash-point
   测试保证进程在 Manifest 发布后立即失败时，durable committed/applied 水位仍不落后于 Manifest。

这些边界不得为了自动化或缩短 RTO 而放宽。

### 9.2 P0：生产声明前必须完成

当前没有发现新的、已由现有测试证明可直接破坏 committed 数据的开放 P0 缺陷；但以下验收缺口在
生产声明前必须关闭：

- 磁盘满、只读文件系统、短写、fsync 延迟/失败和文件损坏的节点级故障矩阵；
- Snapshot/Restore Drill 的真实数据规模、恢复耗时和容量预算；
- 长稳、进程反复崩溃、网络抖动以及 etcd member replacement；
- 升级/回滚和磁盘格式兼容门禁；
- 明确的备份副本、密钥、审计与灾难恢复责任人。

在这些证据完成前，`make test-ha` 只能证明当前受控拓扑的协议闭环，不能作为生产就绪证明。

### 9.3 P1：下一阶段架构风险

#### P1-1 Recovery Task 不是恢复状态机

任务没有持久记录、claim/ack、重试状态或自动执行器。相同事实通过 hash 得到稳定 ID，但进程重启获得
新 Term 后会形成新任务。当前 Runbook 必须重新读取诊断并核对事实，不能把旧 `task_id` 当授权令牌。

#### P1-2 剩余稀疏元数据仍未完全有界

Recovery 全量 Directory/Extent/Tail/Locator Root 常驻已经关闭：运行视图只保留轻量 Descriptor，Tail
使用固定 Slot 按需读取和容量 1024 的 LRU，Locator Root 使用固定 Entry 的 `ReadAt` 二分与容量 1024
的 LRU，WAL/Append 活跃 Stream 才 Seed 到 MemTable。Locator/Tail Builder 已使用外部 Run 和流式输出，
不再同时保留全部 Directory/Extent/Root/Tail；Registry Builder 也已使用有界分块、外部 Run 和流式
Snapshot Writer。剩余规模阻塞是 Registry Sparse Block Index 与 Manifest Segment Reference 常驻，
它们仍会随 Registry Block 或 Segment 数量线性放大启动 RSS。Compaction Frame 合并受输入字节上限
约束，但内部仍会物化一次选定输入。

#### P1-3 Node Runtime 生命周期仍未完全统一

Primary、Standby 和 recovery-blocked 已共享 Replicated Admin Runtime 与诊断 Provider 切换，但 Single
仍独立管理 Admin，三种角色也分别管理 gRPC、后台 ticker 和 storage shutdown。后续抽取必须基于实际
重复继续收敛，不能为了形式统一移动角色、Durability 和恢复判断的强类型边界。

#### P1-4 Replication State 是持久下界而非实时真值

State 周期落盘是避免每次 Group Commit 增加 metadata fsync 的有意取舍；但 Strict storage checkpoint
必须在同一 Engine 临界区先发布覆盖该 Manifest 的 committed/applied State 下界。Promotion、Snapshot、
WAL GC 和诊断仍必须结合物理 WAL；任何新模块都不能把 State watermark 当作每次提交后的精确实时值。

### 9.4 P2：性能与运维成熟度

- Compaction 峰值内存仍与一次输入的记录量相关；
- ResolveTime 是 Sequence 二分加随机 Record 读取，不是专用时间索引；
- Locator 损坏会逐 Segment 按需扫描 Directory，故障时延没有 SLO，但不会常驻全部 Extent；
- Registry 的反向 `LookupID` 可能跨 Block 顺序读取，当前不在业务热路径；
- 没有 Snapshot/GC 自动调度、传输限速、持久 Pin Lease 或对象存储；
- Maintenance failure policy 属于可维护性战略预警：持续非 fatal Checkpoint/Compaction/State 失败尚无
  统一退避、failure budget、readiness 降级或容量预测，但它不是当前功能完整性的首要阻塞；
- 已有磁盘容量/可用空间和按类型文件字节指标，但 Snapshot、GC、Compaction、Cache、Pin 的事件指标、
  告警阈值和容量耗尽预测尚不足以形成完整 SLO；
- 文档多数仍是 `Draft / V1 实现基线`，格式冻结、实现状态和提案没有统一版本发布机制。

## 10. 已冻结决策与仍需 Review 的事项

### 10.1 已冻结的 V1 决策

- **Snapshot 所有者**：Single 使用普通离线 Snapshot；HA 只允许受 committed boundary 约束的在线
  Strict Primary 或安全 `snapshot-primary`。
- **HA 恢复方式**：V1 是结构化、强约束的运维任务，不是 Agent/节点自行执行危险修复。
- **Divergent suffix**：V1 不提供通用 truncate；无法证明安全时统一安装 Snapshot。
- **读拓扑**：V1 Primary-only Read，Standby 不注册业务服务。
- **开发顺序**：先完成数据安全与恢复证据，再修改大规模启动元数据模型。

### 10.2 仍需 Review

1. Recovery Task 是否只作为诊断契约，还是要发展为持久、可 claim/ack 的控制面资源；
2. 在线 Snapshot 的触发者是独立运维控制面、受认证 Admin RPC，还是仅保留停机工具；
3. 当前已选择投影损坏时允许逐 Segment 慢速扫描事实数据；是否增加独立修复工具和故障时延 SLO；
4. 文档如何发布 `Implemented / Accepted Design / Proposal` 状态以及格式兼容版本。

## 11. 后续开发门禁与建议顺序

建议按以下顺序推进，每项单独提交并重新做整体审计：

恢复证据与 Replicated 启动可观测性阶段已经完成。下一轮从以下顺序继续：

1. **执行规模与故障验收**：百万 Stream 启动并量化 Registry Sparse Index、Manifest Segment Reference
   和投影外部 Run 的 RSS/时延/临时磁盘；补磁盘满、短写、fsync 故障、etcd member replacement、
   升级/回滚和 Restore Drill。
2. **按测量结果收敛剩余元数据**：只有确认 Sparse Index 或 Segment Reference 超出预算，才引入分页、
   分层 Manifest 或更强的磁盘索引，避免在缺少规模证据时继续增加格式复杂度。
3. **接入受控 Snapshot/GC 自动化**：只有在所有权、认证、幂等任务和 Pin 生命周期冻结后，才接入
   online Snapshot、归档和 GC 调度。
4. **补 Maintenance failure policy**：作为可维护性和容量战略预警，把连续失败映射到稳定诊断、告警
   和退避；它不早于上述功能完整性与规模验收。

每个阶段完成条件：

- 调用链已接入，不是只有库函数；
- crash point 前后保持同一事实边界；
- 运维误用被代码拒绝；
- diagnostics/metrics 能解释阻塞原因；
- 单元、race、静态检查和相应故障场景通过；
- 能力矩阵、Runbook 和设计状态同步更新。

## 12. 本次审计的证据入口

| 主题 | 主要代码 |
| --- | --- |
| Single/HA 编排 | `internal/node/node.go`, `internal/node/ha.go`, `internal/node/admin_runtime.go` |
| API 与限流/RBAC | `internal/service/server.go`, `internal/access/` |
| Engine 与锁 | `internal/storage/engine/store.go` |
| Group Commit | `internal/storage/commit/commit.go` |
| Recovery/Tail | `internal/storage/recovery/recovery.go`, `internal/storage/tail/resolver.go`, `internal/storage/segment/descriptor.go` |
| Manifest/Segment 生命周期 | `internal/storage/manifest/store.go`, `internal/storage/lifecycle/manager.go` |
| Read/Cache/投影构建 | `internal/storage/read/store.go`, `internal/storage/tail/`, `internal/storage/locator/`, `internal/storage/registry/` |
| WAL History/GC Pin | `internal/storage/wal/history.go` |
| Snapshot | `internal/storage/snapshot/` |
| WAL Retention | `internal/storage/retention/manager.go` |
| Strict Replication | `internal/replication/primary.go`, `receiver.go`, `standby.go`, `catchup.go` |
| Plan/Promotion/Rejoin | `internal/replication/planner.go`, `promotion.go`, `rejoin.go` |
| Term/Lease | `internal/coordinator/etcd/coordinator.go`, `internal/leadership/controller.go` |
| Diagnostics/Metrics | `internal/diagnostics/`, `internal/observe/` |
| 运维入口 | `cmd/streamd-tool/main.go` |
| 真实 HA 验收 | `test/ha/compose.sh`, `test/ha/ha_test.go`, `test/ha/cmd/inject-divergence/`, `.github/workflows/ci.yaml` |

本文件描述的是上述基线代码的现实。如果代码改变了启动顺序、事实边界、锁顺序、复制水位或运维
所有权，必须先更新本审计矩阵，再继续声称架构结论仍然成立。
