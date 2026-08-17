# streamd 设计文档索引

## 推荐阅读顺序

1. [总体设计](../DESIGN.md)：目标、边界和整体数据路径。
2. [V1 存储格式](storage-format.md)：所有持久化字节和版本边界。
3. [Append 与提交协议](append-commit-protocol.md)：Sequence、幂等、Group Commit 和可见性。
4. [崩溃恢复协议](recovery-protocol.md)：Manifest/WAL/Snapshot 恢复与失败关闭。
5. [Segment 生命周期](segment-lifecycle.md)：Flush、Merge、Pin、对象存储和 GC。
6. [索引与缓存设计](index-cache-design.md)：Tail 热路径和冷 Extent 定位。
7. [V1 API 协议](api-protocol.md)：对外 RPC、错误与 SDK 契约。
8. [主备复制协议](replication-protocol.md)：Strict 双副本、Snapshot Catch-up 和 Failover。
9. [基准与可靠性验证计划](benchmark-plan.md)：性能、正确性和规模验证。
10. [生产运维设计](operations.md)：部署、监控、备份恢复、切换与升级。
11. [可观测性契约](observability.md)：低基数指标、写安全、复制水位与磁盘压力。

## 契约依赖

```text
storage-format
   ├── append-commit
   │      ├── api-protocol
   │      └── recovery-protocol
   ├── segment-lifecycle
   │      └── index-cache-design
   └── replication-protocol

benchmark-plan validates all above
operations turns them into production procedures
```

## 实现门槛

开始实现前至少冻结：

- Record Frame、WAL Entry、Segment 和 Manifest V1；
- Expected Sequence、Request Hash 和 Batch 原子性；
- Commit/Apply/Recovery 边界；
- Snapshot/WAL GC 不变量；
- gRPC protobuf Field Number；
- 目标硬件和第一版 Benchmark 配置。

进入生产前必须满足 Benchmark Plan 和 Operations 的上线清单。

## 文档状态

当前文档均为 V1 Draft。Draft 表示协议已经足够进入评审和原型实现，不表示字段编号、性能参数或部署 SLO 已经冻结。任何改变持久化语义的决定必须先更新相关文档，再修改实现。
