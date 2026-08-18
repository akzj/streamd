# streamd 索引与缓存设计

| 属性 | 内容 |
| --- | --- |
| 状态 | Draft / V1 实现基线 |
| 目标负载 | 大量 Stream、Append/Tail 为主、少量历史随机读 |
| 原则 | 索引有界加载，Cache 可丢；不加载全部 Extent，不永久打开全部 Segment |

## 实现状态

当前运行时已经实现：

- Segment Descriptor 与打开的 Reader 分离；恢复逐个验证 Segment 后关闭文件；
- 默认容量 64 的 Segment Handle Cache，Cache Handle 使用引用计数，空闲 Handle 按 LRU 淘汰；
- 读视图绑定 Manifest Generation，Checkpoint/Merge 使用新视图替换旧视图，旧 Handle Cache 在无活跃 Reader 后关闭；
- 有界自动 Merge；先发布新 Generation，再切换 Reader，最后退休旧 Segment；
- 在线 Snapshot Pin 精确的 Manifest Generation，复制完成前输入 Segment 不退休。
- Tail Catalog 与 Manifest Generation 一起发布和恢复；启动只校验 Header/Footer，Slot 按 StreamID
  `ReadAt` 并延迟校验 Generation/CRC；
- 每个 Generation 发布 Sealed Locator Pack/Snapshot，Tail Slot 保存最新 Page Pointer；
- 冷 Sequence Read 通过 Locator Root、Previous Page 链和页内二分定位 Segment，正常路径不再扫描
  全部 Descriptor；
- 默认容量 256 的 Locator Page LRU Cache；Entry 只保存已校验的 Page Metadata，不保存 Payload；
- Snapshot、Scrub、Pin 和退休引用图均显式包含 Locator Pack；Locator 损坏时回退 Segment
  Descriptor，不能影响事实数据读取。
- Registry Snapshot 与 Manifest 同代发布；启动只加载 Sparse Block Index，名称 Entry 按 Block
  `ReadAt`，默认使用容量 64 的 LRU Block Cache；
- Checkpoint 内映射保留在磁盘 Snapshot，新提交映射只进入 WAL Overlay；Compaction 切换时合并
  Checkpoint 之后的 Overlay，Registry Block 损坏时从内部 Registry Stream 惰性重建。
- Recovery 常驻轻量 Segment Descriptor，只验证每个 Segment 的 Header/Footer/Manifest Reference；除最新
  Segment 为恢复全局 `RecordedAt` 水位外，不在启动时读取历史 Stream Directory；
- 历史 Stream Tail 不再 Seed 到 MemTable。Tail Resolver 按 `Active MemTable -> 容量 1024 的 Tail LRU
  -> Tail Catalog Slot -> Segment Fact Scan` 查询；只有 WAL Replay 或 Append 实际写入的 Stream 才进入
  Active MemTable；
- Locator 损坏回退不再缓存某个 Stream 的全部历史 Extent；它逐 Segment 打开 Directory 查找目标 Extent，
  故障路径可能较慢，但常驻内存不再与全部历史 Extent 数量线性增长；
- Checkpoint/Compaction 构建投影时显式 Materialize Directory，发布后立即切回轻量 Descriptor。

当前仍是过渡实现：历史 Directory、Extent 和 Tail 的启动常驻问题已经关闭，但 Manifest Segment
Reference、Locator Root 和 Registry Sparse Block Index 仍分别随 Segment、Stream 和 Registry Block
数量增长；Checkpoint/Compaction 的投影 Builder 也会临时聚合当前 Generation 的全部 Descriptor 与
Extent。TinyLFU Admission、Segmented LRU、Cache Shard 和 Skip Pointer 生成尚未接入运行时；当前
Builder 只生成 Previous Pointer。因此已经满足“历史 Extent 不常驻”的结构性要求，但尚未完成百万
Stream 启动 RSS/时延及投影构建峰值内存验收。

## 1. 查询问题

streamd 需要高效回答：

1. `(namespace, stream_name) -> StreamID`；
2. `StreamID -> next_sequence/next_byte_offset`；
3. `(StreamID, Sequence) -> Segment/MemTable -> Frame`；
4. `(StreamID, RecordedAt) -> Sequence`；
5. `(StreamID, ByteOffset) -> Frame`，仅内部诊断/读取路径；
6. Subscribe 是否有新 Committed Record。

典型访问高度偏向 Tail，但系统必须支持任意历史 Sequence，且内存不能与全部历史 Extent 数量线性增长。

## 2. 索引层次

```text
Name Registry
  (namespace, name) -> StreamID

Hot Stream Cache
  StreamID -> active tail + recent extents

Tail Catalog
  StreamID -> durable latest tail + latest extent pointer

Cold Extent Locator
  StreamID + range -> Segment

Segment Directory + Unified Record Index
  Sequence/Time/Offset -> Frame physical position
```

MemTable 另有同结构的内存 Index，覆盖尚未进入 Segment 的 Committed Record。

## 3. 一致性边界

- WAL/Segment 是事实；
- Registry、Tail、Locator 和 Index 是事实的确定性投影；
- Cache 不拥有 `fsync` 或独立事务；
- `applied_entry_id` 表示投影已包含的最大连续 Entry；
- Reader 只组合属于同一个 Manifest Generation 和不晚于 Applied Watermark 的对象；
- Cache Entry 必须携带 Generation/Epoch，旧 Generation 命中视为 Miss。

## 4. Name Registry

### 4.1 持久来源

内部 Registry Stream 是事实，Registry Snapshot 是排序 Checkpoint。

### 4.2 两级查找

```text
Registry Hot Cache
  hash(namespace, name) -> StreamID

Registry Snapshot
  Sparse Block Index -> sorted Registry Entries
```

Snapshot 每个 Block 保存固定数量 Entry，Block Index 常驻小型稀疏表：

```text
RegistryBlockIndex {
  first_key_prefix
  file_offset
  entry_count
}
```

查找先二分 Block，再在 Block 内二分。V1 不要求通用 LSM；新映射先存在 WAL/Active Registry Map，Snapshot 时合并为新排序文件。

当前 Runtime 常驻 Header 和完整 Sparse Block Index，不常驻 Checkpoint 内全部 Registry Entry。Block
按 Index Offset 读取，逐 Entry 校验长度/CRC/Key 顺序后进入容量 64 的 LRU；磁盘读取在 Cache Lock
之外执行。`LookupID` 当前需要顺序访问 Block，热路径只使用名称查找。Snapshot 只是投影：Block
损坏会触发一次 Registry Stream Segment 扫描并切换为内存事实映射，后续 Checkpoint 再发布新
Snapshot。

### 4.3 Cache

- Key 使用完整 Namespace/Name 字节并防 Hash Collision；
- 正命中可缓存；
- 负命中只短 TTL，避免刚创建 Stream 被旧负缓存遮蔽；
- 每次 Registry Applied Epoch 变化使相关负缓存失效；
- Principal 权限结果不能与名称映射混为同一 Cache。

## 5. Active MemTable Index

每个 Stream 在 MemTable 内维护：

```text
MemStreamIndex {
  first_sequence
  first_byte_offset
  first_recorded_at
  entries[] {
    chunk_id
    chunk_offset
    frame_length
    recorded_at_delta
    entry_id
  }
}
```

- Sequence 通过数组下标 O(1)；
- Time/Offset 在同一单调数组二分；
- Frame 数据按 Stream Chunk 追加；
- Freeze 后 Index 只读；
- Batch Apply 完成前不发布 Index Length。

## 6. Hot Stream Cache

```text
HotStreamState {
  stream_id
  manifest_generation
  applied_entry_id

  next_sequence
  next_byte_offset
  last_recorded_at
  last_entry_id

  active_memtable_ref
  immutable_refs[]
  latest_segment_id
  latest_extent_pointer
  recent_extents[]

  writer_pin
  subscriber_count
  last_access
  estimated_bytes
}
```

### 6.1 命中路径

- Append：Writer Pin 保证状态驻留；
- Read Tail：先查 Active/Immutable，再查 Recent Extent；
- Subscribe：Subscriber Pin 保留 Tail State，不保留完整历史 Payload；
- Cache Miss：从 Tail Catalog 恢复，不扫描全部 Segment。

### 6.2 淘汰

使用 Segmented LRU：

```text
Probation -> Protected
```

- 首次历史扫描进入 Probation；
- 重复命中或活跃写/订阅进入 Protected；
- Writer 正在分配、Reader 正持有引用的 Entry 不淘汰；
- Subscriber Pin 只保护 Tail Metadata，不无限保护 Recent Extent；
- 淘汰仅删除内存对象，不触发磁盘写。

## 7. Tail Catalog

Tail Catalog 固定 Slot，位置由 StreamID 计算。读取流程：

1. 读取 `generation_begin`；
2. 复制 Slot；
3. 读取 `generation_end`；
4. 两者相等、为偶数且 CRC 正确才接受；
5. `applied_entry_id` 落后时重放 WAL 或回到 Segment/Manifest；
6. 写入 Hot Stream Cache。

Tail Catalog 的 OS Page Cache 按需加载，不能在启动时 Touch 全文件。

## 8. Cold Extent Locator

### 8.1 Pointer

```text
ExtentPointer {
  pack_id
  page_ordinal
}
```

Tail Slot 指向最新 Page。Page 保存一组连续 Extent、Previous Pointer 和 Skip Pointer。

### 8.2 Sequence Lookup

```text
if target in active/immutable:
    use MemTable
else:
    page = latest_extent_pointer
    while target < page.first_sequence:
        choose farthest skip whose range remains >= target
        else page = previous
    binary search extent range
```

不得把沿途所有 Page 固定到 Hot Stream State；只进入有界 Page Cache。

### 8.3 Time Lookup

相同 Page 链使用 `first_recorded_at/last_recorded_at` 定位。Recorded At 可以相等：

- `AT_OR_AFTER` 选择最早满足位置；
- `AT_OR_BEFORE` 选择最晚满足位置；
- Segment 内用 Unified Index 二分并处理相等时间边界。

### 8.4 Offset Lookup

仅内部使用，按 Extent `first/next_byte_offset` 和 Index Relative Offset 二分。

## 9. Locator Page Cache

```text
PageCacheKey = (pack_id, page_ordinal)
```

- Entry 在插入前完成 Page CRC 和边界校验；
- 使用 TinyLFU Admission + Segmented LRU Eviction，避免一次全历史扫描污染；
- Cache Value 是已解析只读 Metadata，不缓存 Payload；
- Reader RefCount 期间不能淘汰底层 mmap/Buffer；
- checksum 失败不进入 Cache，并隔离对应 Artifact。

当前 V1 Runtime 的 Locator Page 与 Registry Block 分别使用单容量上限 LRU，并在锁外执行文件
读取；TinyLFU、分段与分片是后续优化，不属于当前已实现能力。

## 10. Segment Handle Cache

```text
SegmentHandle {
  segment_id
  manifest_generation
  local_or_object_location
  fd_or_reader
  parsed_header
  directory_view
  ref_count
  last_access
  estimated_bytes
}
```

### 10.1 生命周期

- Open 前验证 Manifest Reference；
- 首次 Open 完成 Header/Footer/Directory Check；
- 每次 Frame Read 验证 Frame CRC；
- RefCount 为零才可 Evict/Close；
- Segment Retirement 先从新查询集合移除，再等待 Handle RefCount；
- 文件描述符和 mmap 数均有独立硬上限。

### 10.2 mmap 与 pread

V1 默认实现优先 `pread`：生命周期清晰、避免大量 Segment 地址空间和 Unmap 问题。可以对热的 Index/Directory 使用小范围 mmap。完整 Payload mmap 是否启用由 Benchmark 决定，不改变格式或 API。

## 11. Segment 内查询

### 11.1 Sequence

```text
i = target_sequence - directory.first_sequence
index_pos = record_index_offset + i * 24
read DenseIndexEntry
frame_pos = stream_data_offset + relative_byte_offset
pread frame_length bytes
validate frame CRC and metadata
```

### 11.2 Range Read

读取第一条 Index 后，连续 Frame 尽量合并为一次有界 `pread`。响应受 `max_records/max_bytes` 限制，不能因连续区间而无界读取。

### 11.3 ResolveTime

对 `recorded_at_delta` 二分。相等时间使用 lower_bound/upper_bound，保证 Mode 语义确定。

## 12. Payload Cache

V1 不默认实现应用层 Payload Cache，优先依赖 OS Page Cache：

- 避免与内核重复缓存；
- 避免大 Payload 挤压索引；
- Tail 数据已在 MemTable；
- 冷对象存储可以使用独立、字节上限明确的 Disk Cache。

若增加 Payload Cache，Key 必须包含 Segment ID、Frame Offset 和 Length，且不能绕过 CRC。

## 13. 内存预算

```text
total_memory =
  active_memtables
  + immutable_memtables
  + registry_cache
  + hot_stream_cache
  + locator_page_cache
  + segment_handle_metadata
  + subscription_buffers
  + replication_buffers
  + read_scratch
  + safety_margin
```

每部分拥有独立上限，不能只依赖进程总 RSS。建议 Safety Margin 不低于进程预算的 15%，最终比例由 Benchmark 固定。

Cache Entry 必须实现 `estimated_bytes`，包括 Slice Capacity、Map Overhead 和引用对象，不能只计算 Payload Length。

## 14. Cache 压力顺序

内存压力时按顺序：

1. 拒绝新的低优先级历史 Prefetch；
2. 淘汰 Probation Page；
3. 关闭空闲 Segment Handle；
4. 缩减 Protected Cache；
5. 断开超过缓冲上限的慢 Subscriber；
6. 对 Append 背压，防止 Immutable MemTable 继续增长；
7. 绝不丢弃 Committed 但尚未 Flush 的 MemTable。

## 15. 并发

- Published Index/Extent/Page 不可变，可无锁读取；
- Hot Stream Tail 用 Versioned Snapshot 或短 RW Lock；
- Batch Apply 构建新视图后原子替换；
- Cache Shard 按 Hash 分区，避免单全局 LRU Lock；
- 文件关闭与 Reader 使用通过 RefCount/RCU Epoch 协调；
- Manifest Generation 切换后旧 Reader 保留旧 View，直到退出。

## 16. 预取

只允许有界预取：

- 顺序 Read 预取下一小段 Index/Data；
- Subscribe 接近 Tail 时不预取冷历史；
- Locator Page 沿 Skip 跳转时最多预取一个候选 Page；
- 对象存储 Range Read 根据观察到的顺序模式扩大窗口；
- 一次性随机历史查询不触发深度预取。

## 17. 指标

- Registry/Hot Stream/Page/Handle Cache hit/miss/admission/eviction；
- 各 Cache bytes、entries、pinned entries；
- Extent Page hops 和 Skip 命中；
- Segment opens、FD 数、mmap bytes；
- Sequence/Time/Offset lookup 分阶段 latency；
- pread bytes/request 和 read amplification；
- Cache Generation stale miss；
- Tail Slot CRC/rebuild；
- 对象 Range Read 和 Disk Cache 命中。

## 18. 验收

- Stream/Segment/Extent 总量增长时，常驻内存保持配置上限；
- Append/Tail 命中不访问历史 Locator；
- 任意 Sequence 最终可定位且结果一致；
- 全历史扫描不会把稳定热集合完全逐出；
- Segment Merge/Manifest 切换期间 Reader 不读错文件；
- Cache 全清空后功能正确，只影响性能；
- 百万 Stream 启动不遍历全部 Tail Slot 或打开全部 Segment。

当前已通过任意 Sequence 定位、Page CRC、Previous Page 多页回溯、Registry 多 Block 查找、容量
1 淘汰、Cache Clear、Generation 切换、Compaction Overlay 保留、Snapshot Pin 和投影损坏回退
测试。新增测试证明 checkpoint-only Stream 不进入 MemTable、历史 Segment Directory 不在启动时读取、
首次 Append 能按需恢复准确 Tail，以及 Tail Cache 保持配置容量。百万 Stream 启动仍未验收，因为
Locator Root 与 Registry Sparse Block Index 尚未分页，Manifest Segment Reference 也仍全量常驻。
