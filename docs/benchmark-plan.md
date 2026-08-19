# streamd 基准与可靠性验证计划

| 属性 | 内容 |
| --- | --- |
| 状态 | V1 基线工具已实现，完整生产验证待执行 |
| 对照 | yatsdb Stream Store、streamd 单节点、streamd Strict 主备 |
| 原则 | 固定环境、公布分布、报告尾延迟和资源成本，不只报告峰值吞吐 |

## 1. 目标

基准用于回答设计问题，不用于制造单一漂亮数字：

- 新 Record/Index 格式相对 yatsdb 增加多少 CPU、空间和延迟；
- Group Commit 如何平衡吞吐与 P99/P999；
- Dense Index 和 Tail/Extent Cache 是否保持稳定读延迟；
- Segment Flush/Merge/Snapshot 是否造成前台抖动；
- Strict 复制的网络和双 `fsync` 成本；
- 恢复时间和最大 WAL/Snapshot 大小是否可运维；
- pread、mmap、Page Size 和 API 默认 Record Limit 如何选择。

## 2. 基准纪律

- 每次结果记录 Git Commit、配置 Hash、Binary Hash；
- 独占机器，关闭不相关后台任务；
- 固定 CPU Governor、NUMA、文件系统和 Mount Option；
- 报告 SSD 型号、Firmware、容量、使用率和写入历史；
- 每组至少一次 Warmup、五次正式运行；
- 报告 Median、Min/Max 和置信区间；
- 不丢弃失败、超时或校验错误样本；
- 原始 HDR Histogram/Prometheus/系统指标归档；
- yatsdb 对照使用同硬件、同 Payload、同 durability。

## 3. 环境矩阵

### 3.1 最小环境

```text
CPU model / cores / SMT
RAM / NUMA
Local NVMe model / filesystem
Kernel / Go version
Network NIC / MTU / RTT
Coordinator version
Object storage implementation
```

### 3.2 部署

- 单进程单 Shard；
- 单节点多 Shard；
- Primary/Standby 同机架低 RTT；
- Primary/Standby 人工注入 1/5/20/50 ms RTT；
- 对象存储本地模拟和真实远端。

生产结论不能只来自容器 OverlayFS 或共享 CI Runner。

## 4. 数据集

### 4.1 Record 大小

```text
64 B, 256 B, 1 KiB, 4 KiB, 16 KiB, 1 MiB, 16 MiB
```

分别测试固定大小和真实长尾分布。默认 Payload 使用不可压缩随机字节，另设可压缩数据但 V1 不启用压缩。

### 4.2 Stream 数

```text
1
1,000
100,000
1,000,000
10,000,000 metadata-only feasibility
```

### 4.3 热度

- 单热点 Stream；
- Uniform；
- Zipf 0.8/1.0/1.2；
- 1% Stream 承担 99% 流量；
- 长时间不访问后随机冷读。

### 4.4 历史规模

- Active MemTable only；
- 10/100/1,000/100,000 Segment；
- 每 Stream 1/10/1,000/100,000 Extent；
- WAL Replay 1 GiB、10 GiB、100 GiB；
- Snapshot 100 GiB、1 TiB（可使用生成器）。

## 5. 写入基准

### 5.1 场景

- 单 Stream 顺序 Append；
- 多 Stream 并发 Append；
- AppendBatch 1/10/100/1,000 Records；
- Expected Sequence 冲突；
- 响应丢失 Dedup Retry；
- 新 Stream 高频创建；
- SINGLE_SYNC 与 REPLICATED_STRICT；
- Group Commit Delay/Bytes/Requests 参数扫描。

### 5.2 指标

- Records/s、MiB/s；
- enqueue、WAL write、local fsync、remote fsync、commit、apply 分段延迟；
- P50/P95/P99/P999/Max；
- Group size 和 deadline miss；
- CPU cycles/record、allocations/record、GC pause；
- WAL bytes/record、write amplification；
- Sequence conflict/dedup throughput。

### 5.3 正确性伴随检查

每轮后扫描所有测试 Stream：Sequence 连续、Request Hash 一致、Record Count 正确。吞吐测试发现一条错误即判整轮失败。

## 6. 读取基准

- Active Tail Read；
- Hot Segment Sequence Random Read；
- Cold Segment Random Read；
- 连续 1/10/1,000 Record Range Read；
- ResolveTime 命中/边界/大量相同时间；
- Locator 1/10/100 Page Hop；
- pread vs index-only mmap vs full mmap；
- 本地 Segment vs 对象 Range Read。

记录 API latency、实际读取字节、Page Fault、FD/mmap 数、Cache Hit 和 Extent Hop。

## 7. Subscribe 基准

- 1/1,000/100,000 空闲 Subscriber；
- 多 Subscriber 订阅同一热点 Stream；
- 历史 Catch-up 后进入 Tail；
- 快、慢、暂停消费者混合；
- 服务端断开与 Cursor 重连；
- Primary Failover 后继续订阅。

验证慢消费者不影响 WAL Commit latency，内存始终受限。

## 8. 后台任务干扰

分别在稳定前台负载下启动：

- MemTable Flush；
- 1/2/4 并发 Merge；
- Full Scrub；
- Snapshot Generation；
- Object Upload；
- Standby Snapshot Catch-up。

比较任务前、中、后的 P99/P999 和吞吐，报告最大抖动窗口，而不是只报告平均值。

## 9. Cache 与规模

- Hot Cache 容量扫描；
- Zipf 热集合命中稳定性；
- 一次全历史扫描后的 Cache 污染；
- Locator Page 64 KiB 与候选 16/32/128 KiB 对照；
- Segment Handle 上限和 FD 压力；
- 百万 Stream Tail Catalog RSS/Page Fault；
- Cache 全清空后的冷启动曲线。

Extent Page Size 只有在查询 Hop、IO 放大、内存和构建成本综合结果明确后才能冻结。

2026-08-19 的默认一百万 Stream 验收已经给出第一组真实边界：预创建耗时 4146.99 秒，Checkpoint
生成 1,000,000 个 64 KiB Locator Page，单个 Locator Pack 为 65,536,069,720 bytes，最终数据目录约
63 GiB；重开耗时 0.273 秒，重开后 Heap Alloc 为 39,538,424 bytes，全量 Scrub 成功。该结果证明启动
元数据已按需读取，但也证明“一 Stream 至少一固定 Page”的 V1 格式对大量低记录数 Stream 存在约
64 KiB/Stream 的确定性磁盘放大，后续必须比较可变长 Page 或多 Stream Page，不能只调整 Cache 容量。

原验收二进制的 Scrub 因 `VerifyPack` 整体 `ReadFile` 出现 65,357,528 KiB 峰值 RSS；改为流式 SHA-256
后，在同一 65.5 GB Pack 上独立全量 Scrub 用时 41.45 秒、峰值 RSS 626,280 KiB、退出码 0。两组数字
必须分别解读：前者是已关闭的校验实现缺陷，后者不消除 Locator 格式本身的磁盘放大。

## 10. 空间效率

对每种 Record 大小报告：

```text
logical payload bytes
record frame bytes
WAL bytes
dense index bytes
segment padding bytes
manifest/locator bytes
total local bytes
remote snapshot bytes
```

计算 Metadata Amplification、Write Amplification 和双副本总成本。与 yatsdb 的差异必须拆到具体字段/索引，不能只报目录大小。

## 11. 恢复基准

- Clean Restart；
- 1/10/100 GiB WAL Replay；
- Tail/Locator Checkpoint 有效和全部重建；
- 百万/千万 Stream Registry 加载；
- Segment 1千/10万文件 Open Check；
- Snapshot 下载、校验、安装和后续追赶；
- Standby 短/长时间离线；
- 单个损坏 Artifact 的检测时间。

指标：Ready Read、Ready Write、Peak RSS、读取/写入字节、CPU 和恢复阶段耗时。

## 12. 故障性能

- Standby 网络丢包/延迟/断连；
- Primary/Standby fsync latency spike；
- Coordinator 短时不可达和 Lease 到期；
- 磁盘 80/90/95/99% 使用率；
- Object Storage throttling；
- Manifest/Snapshot 目录大量历史文件。

必须同时报告正确故障行为：停止写、背压、切换或恢复，而不仅是 latency。

## 13. Soak Test

当前 `make test-soak-72h` 已提供可审计执行入口：默认 100 requests/s 控制本地 72 小时磁盘预算，周期
Checkpoint 后执行有界 Compaction，每小时创建 verified linked Snapshot 并回收 Primary covered WAL；
运行目录持续保存 RSS/VSZ/FD/Primary bytes/Standby bytes。该 harness 的 Standby 是独立 durable WAL，
不是完整 Standby 进程，因此 HA 进程切换与网络故障仍由 Compose 门禁覆盖，不能由此替代。

正式 72 小时运行前的 3 分钟 Strict smoke 必须至少跨两次 Checkpoint，并确认 `trash/` 不随投影替换增长。
2026-08-19 的回归跨 3 次 Checkpoint、17,999 requests，错误为 0、最终 Scrub 和 Standby WAL 验证成功，
Primary 始终只有 1 个 live Locator Pack 且 `trash_files=0`。

同日首次正式 Run `soak-72h-20260819-r2` 在约 2.5 小时主动终止并判无效：第一次 Snapshot/WAL GC 后，
后续 replicated Snapshot 因 Replication State 缺少 Installed Snapshot 恢复锚点而持续失败。修复将节点与
benchmark 收敛到同一个在线 retention 事务，并让 benchmark 把周期 Checkpoint/Compaction/Snapshot 错误
实时写入 stderr。修复后的 20 秒 Strict 诊断跨 3 次 Snapshot，`errors=0`、Scrub/Standby 验证成功、
只保留 1 个 Snapshot、Primary WAL 回落到 2 个。该诊断只证明缺陷闭环，不能替代重新计时的 72 小时 Run。

至少包含：

- 72 小时持续混合读写；
- 周期 Flush/Merge/Snapshot/Scrub；
- 随机进程 Kill；
- 随机网络分区；
- Subscribe 重连；
- 每小时抽样全链路 Record 校验；
- 结束后全量 Sequence/CRC/SHA 验证。

观察 RSS 漂移、FD 泄漏、Pin 泄漏、Segment 数、WAL/Snapshot GC、P999 漂移和磁盘增长。

## 14. yatsdb 对照

只比较两者共同能力：

- `Append(streamID, bytes)` 数据路径；
- WAL sync/group commit；
- MemTable 聚合；
- Segment 顺序读取；
- 多 Stream 分布。

streamd 的 Record Header、CRC、Dense Index、Registry 和主备复制开销单独列出。不能用 yatsdb 不提供的可靠性能力反向宣称吞吐优势，也不能忽略 streamd 的额外语义成本。

## 15. Profile

每个关键场景采集：

- CPU Profile；
- Allocation/Heap Profile；
- Mutex/Block Profile；
- `perf` cycles/cache-miss/context-switch；
- `iostat` latency/queue/utilization；
- Network throughput/retransmit；
- Filesystem/Block trace（仅诊断轮次）。

Profile 采集轮次与正常基准轮次分开，避免工具扰动结果。

## 16. 发布门槛

自动化入口包括 `make test-faults`、`make test-compat`、`make test-ha`、`make test-scale` 和
`make test-soak-72h`。前四项的退出状态可以即时判定；72 小时门禁只能在自然结束和资源曲线复核后判定。

V1 不在设计文档预设绝对吞吐数字。实现进入生产候选前必须满足：

1. 所有正确性和 Crash Injection 通过；
2. 72 小时 Soak 无数据错误、资源泄漏和无界增长；
3. 目标硬件上的 P99/P999 满足业务 SLO；
4. Strict 复制下单节点故障 RPO=0；
5. 恢复时间满足部署设定的 RTO；
6. 相比 yatsdb 的开销可以由新增可靠性字段解释；
7. 后台 Merge/Snapshot 不使前台超过约定 Error Budget；
8. 容量模型可以预测磁盘耗尽时间。

## 17. 结果格式

每份报告包含：

```text
commit/config/environment
workload generator version
dataset and distributions
warmup/duration/repetitions
throughput and HDR histograms
CPU/memory/disk/network
correctness result
profiles and raw data locations
known anomalies
decision supported or rejected
```

没有原始数据、配置和正确性结果的吞吐数字不进入设计决策。

## 18. 当前可执行基线

Checkpoint 的三个持久化边界都具备子进程崩溃恢复测试：

```bash
go test ./internal/storage/engine -run TestCheckpointCrashRecovery -count=1
```

单请求、单 Record、每次请求同步落盘的 Go Benchmark：

```bash
go test ./internal/storage/engine \
  -run '^$' \
  -bench BenchmarkAppendSingleSync \
  -benchmem
```

同进程、两套独立文件系统 WAL 的 Strict 双 `fsync` 基准：

```bash
go test ./internal/storage/engine \
  -run '^$' \
  -bench BenchmarkAppendReplicatedStrict \
  -benchmem
```

短时吞吐和周期 Checkpoint 正确性检查：

```bash
go run ./cmd/streamd-bench \
  -duration 30s \
  -workers 8 \
  -streams 1000 \
  -batch 10 \
  -payload-bytes 1024 \
  -checkpoint-interval 10s
```

单节点 72 小时基线：

```bash
go run ./cmd/streamd-bench \
  -duration 72h \
  -workers 8 \
  -streams 100000 \
  -batch 10 \
  -payload-bytes 1024 \
  -checkpoint-interval 1m \
  -data /mnt/streamd-soak
```

Strict 双 WAL 72 小时基线（两个目录必须位于计划测试的独立磁盘）：

```bash
go run ./cmd/streamd-bench \
  -mode strict \
  -duration 72h \
  -workers 8 \
  -streams 100000 \
  -batch 10 \
  -payload-bytes 1024 \
  -checkpoint-interval 1m \
  -data /mnt/primary/streamd-soak \
  -standby-data /mnt/standby/streamd-soak
```

可重复 HA 语义演练：

```bash
go test ./internal/replication ./internal/storage/engine ./internal/storage/snapshot \
  -run 'HADrill|InstallAndCrashResume|Promote' \
  -count=100
```

`streamd-bench` 只接受一个新目录或空目录，避免覆盖既有数据。默认在计时结束后执行最终 Checkpoint 和完整 Scrub，并以 JSON 输出吞吐、错误数和校验结果。`-precreate-streams` 会在计时前创建全部 Stream，单独报告 `setup_seconds`，用于把 Registry 创建成本与已有 Stream 的稳态 Append 分开。计时结束后工具通过 Commit Barrier 排空此前已经入队的请求，报告 `drain_seconds`、deadline exit 和 uncertain result，不把 Context 超时后仍在提交的请求静默丢失。

`single` 模式是 `SINGLE_SYNC` 基线；`strict` 模式通过两套独立 WAL 执行真实双 `fsync`、Group Commit 和
Strict 客户端完成条件，并在结束时校验 Primary 数据与 Standby WAL 连续性。CLI Strict 基线不包含真实网络 RTT、
mTLS 或 Standby Apply 成本；这些必须使用双进程部署基准补充。当前工具也不注入随机 Kill、网络分区、
HDR Histogram 或读写混合负载，不能从单次结果外推 72 小时门槛已经满足。

## 19. Group Commit 可执行测量

Committer 是单 goroutine WAL writer，有界 channel 默认容量 1024；一组最多合并 64 个不同 Stream、
4 MiB 编码数据或等待 250 µs，然后执行一次 WAL Append 和一次 local `fsync`。Strict 在同一组 local
durable 后再等待 Standby durable ack。同一 Stream 的连续请求不会进入同一组，保持现有 Tail 校验与 Apply 顺序。

`streamd-bench` 可扫描：

```bash
go run ./cmd/streamd-bench \
  -duration 30s \
  -workers 32 \
  -streams 1000 \
  -precreate-streams \
  -group-delay 250us \
  -group-requests 64 \
  -group-bytes 4194304 \
  -queue-capacity 1024
```

JSON 把 queue admission/wait、主动 collect、WAL append、local sync、replicate、apply 和完整 process
分别累计。`local_sync_process_ratio` 用于判断 Committer 内部是否由 fsync 主导；
`local_sync_wall_ratio` 使用“计时窗口 + 最终 drain”作分母。Group Commit 请求数可能大于成功业务请求数：
新 Stream 还包含 Registry Record，deadline 返回不确定的请求也会在后台完成。

2026-08-18 开发机的一秒到两秒探索轮次只用于验证工具和选择后续实验，不是发布基线：

| 场景 | 结果摘要 |
| --- | --- |
| Single，32 workers，250 µs，max group 1 | 约 0.47k req/s，queue wait 约 64.6 ms |
| Single，32 workers，250 µs，max group 64 | 约 8.5k req/s，平均 group 约 27.8，queue wait 约 0.83 ms |
| Strict 双本地 WAL，32 workers，250 µs | 约 4.46k req/s；local sync/process 约 44.6%，replicate/process 约 51.1% |
| 新 Stream 持续创建，8 workers | 约 0.26k req/s；Registry Stream 0 串行创建路径主导 |
| 预创建 1,000 Stream 后稳态，8 workers | 约 2.01k req/s；平均 group 约 8 |

当前机器上 Single 的 local `fsync` 占 Committer process 约 91%–99%，所以合并有明确收益。同时低压力下
250 µs Go timer 的实际 collect 常接近 1.1 ms；固定 delay 不是通用最优值。正式决策必须在目标 NVMe、
文件系统和 Strict 网络拓扑上按本计划重复五轮并记录尾延迟。下一步先补 HDR latency、CPU/iostat 和更大
Stream setup/restart 样本，再决定默认 delay 是否可配置或需要基于压力自适应。
