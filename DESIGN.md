# streamd 设计方案

| 属性 | 内容 |
| --- | --- |
| 状态 | Draft / 待评审 |
| 版本 | V1 |
| 项目定位 | 独立、通用、高性能、永久追加的 Record Stream 服务 |
| 设计来源 | 借鉴 yatsdb 已验证的 WAL、Stream Offset、MemTable、Segment 和 mmap 读取模型 |

完整专题文档和推荐阅读顺序见 [设计文档索引](docs/README.md)。

## 1. 摘要

`streamd` 是一个独立于具体业务的追加式流存储服务。它为大量逻辑 Stream 提供：

- 顺序追加；
- 按连续 Sequence 任意读取；
- 按服务端记录时间定位；
- 从任意 Sequence 持续订阅；
- 崩溃恢复；
- 永不逻辑删除；
- 可校验、可审计的永久历史。

`streamd` 不理解 Agent、LLM、Tool、Metrics 或消息队列等业务概念。调用者只向一个 Stream 追加带 Header 的不透明 Record，并使用从 `0` 开始连续递增的 Sequence 继续读取。Byte Offset 只服务于底层顺序存储，不作为公共 Cursor。

系统分为两层：

```text
Record Stream Service
├── Record framing
├── Stream name registry
├── Expected Sequence
├── Request ID retry recognition
├── Unified record index
├── Read / Tail / Subscribe
└── Authentication / namespace isolation

Append-Only Stream Storage Engine
├── WAL
├── Stream byte offset (internal)
├── MemTable
├── Immutable Segment
├── mmap / pread reader
├── Unified record index
├── Tail cache / extent locator
├── Segment merge
└── Crash recovery
```

Record 层提供稳定的服务语义，底层继续使用经过 yatsdb 性能验证的 Byte Stream 模型。

## 2. 背景与动机

yatsdb 已经验证了一条有效的数据路径：

```text
Append(streamID, bytes)
        ↓
WAL 顺序写入和批量同步
        ↓
按 Stream 聚合到 MemTable
        ↓
Flush 为不可变 Segment
        ↓
利用 Stream Offset 直接定位和连续读取
```

该模型在海量 Metrics 和消息中间件场景中表现出稳定的写入性能和很强的读取能力。其主要代价是未压缩数据占用较多磁盘空间。

Agent Runtime 又天然产生大量追加式数据：用户输入、模型输入输出、Agent 间消息、工具请求、工具结果和执行日志。这些数据需要永久审计、崩溃恢复和实时 Tail，访问模式与 yatsdb 的 Stream Store 高度一致。

`streamd` 因此不从通用关系数据库或传统消息队列出发，而是将 yatsdb 的高性能存储思想提炼为独立基础设施。

## 3. 目标与非目标

### 3.1 目标

- 支持大量相互独立的 Stream。
- 对单个 Stream 提供严格的追加顺序。
- 每个 Stream 的 Record Sequence 从 `0` 开始严格连续，成功一次加 `1`。
- Append 成功后返回稳定、永久有效的 Sequence。
- 支持从任意有效 Sequence 读取完整 Record。
- 支持从任意 Sequence 实时追踪后续 Record。
- 支持按服务端记录时间查找对应 Sequence。
- Append 在确认持久化后才向调用方返回成功。
- 进程或机器异常退出后可以恢复所有已确认写入。
- 永不对外提供修改、覆盖或逻辑删除 Record 的能力。
- 文件格式、协议和索引均可版本化演进。
- 保持服务通用，不依赖上层业务 Schema。
- 保留 yatsdb 数据路径的低 CPU、低放大和高吞吐特征。

### 3.2 非目标

- V1 不提供 SQL、全文搜索或任意 Header 查询。
- V1 不提供跨 Stream 事务。
- V1 不提供 Consumer Group、消息确认或业务重试队列。
- V1 不提供 Kafka 协议兼容。
- V1 不提供 Record 更新和删除。
- V1 不把业务事件反序列化为内部对象。
- V1 不实现跨节点分布式共识或自动分片。
- V1 不承诺业务处理的 exactly-once；只提供幂等 Append 基础。
- V1 不在热写入路径启用压缩。

## 4. 核心设计原则

### 4.1 Stream 是事实，索引是投影

不可变 Record 是唯一事实。Record Index、Segment Locator、Tail Catalog、统计信息和缓存都可以从 WAL 与 Segment 重建。

### 4.2 Sequence 永久连续

一旦 Append 返回 Sequence，该 Sequence 在 Segment Flush、Merge、冷热迁移和进程重启后保持不变。失败或未提交的 Append 不消耗 Sequence，恢复后从最后一个已提交 Sequence 的下一个值继续。

### 4.3 追加成功意味着已经持久化

默认配置下，服务必须在对应 WAL 数据完成 `fsync` 后才能确认 Append。仅进入内存队列不算成功。

### 4.4 热路径保持简单

V1 热路径只执行校验、Record 编码、顺序 WAL 写入、批量同步和 MemTable 追加。不在写入路径进行复杂索引、压缩或业务解析。

### 4.5 永不逻辑删除

任何已经成功写入的 Record 在逻辑 Stream 中永久存在。Segment Merge 或冷热迁移可以回收完全等价的旧物理副本，但不得改变逻辑数据、Sequence、内部 Byte Offset 或校验结果。

### 4.6 故障时失败关闭

Active Segment 的尾部半条 Record 可以依据明确规则截断。已封存 Segment 的校验失败不得静默跳过，服务应拒绝启动相关分片并报警。

### 4.7 主备复制保持逻辑一致，而非物理一致

高可用模式复制 Primary 已经完成排序和字段分配的 WAL Entry，不复制公共 API 操作，也不要求主备生成相同的物理 Segment。Strict 模式下，Append 只有在主备两端 WAL 均持久化后才能成功。Term、Lease、WAL 追赶、Snapshot 安装和故障切换的完整语义见 [主备复制协议](docs/replication-protocol.md)。

## 5. 数据模型

### 5.1 Namespace

Namespace 是隔离和授权边界：

```text
Namespace {
  name
}
```

外部 Stream 由 `(namespace, stream_name)` 唯一标识。内部可以将其映射为紧凑的数字 `StreamID`，但数字 ID 不成为公共协议的一部分。

### 5.2 Stream

Stream 是一个永久追加的 Record 序列，同时在底层表现为连续逻辑字节流：

```text
Stream {
  namespace
  name
  stream_id          // internal；0 保留给 Registry Stream，用户 Stream 从 1 开始
  head_sequence      // V1 永远为 0
  next_sequence      // 下一次成功 Append 使用的 Sequence
  next_byte_offset   // internal
  created_at
}
```

Stream 不需要显式创建。第一次成功 Append 可以原子创建 Stream。

### 5.3 Record

公共 Record 保持通用：

```text
Record {
  request_id
  headers: map<string, bytes>
  payload: bytes
}
```

服务持久化后补充：

```text
StoredRecord {
  namespace
  stream
  sequence
  request_id
  request_hash
  entry_id
  recorded_at
  producer
  headers
  payload
  checksum

  byte_offset       // internal
  next_byte_offset  // internal
}
```

- `sequence` 是 Stream 内从 `0` 开始连续递增的公共 Cursor。
- `request_id` 标识一次 Compare-And-Append 请求，只与请求的 `expected_sequence` 共同用于识别响应丢失后的重试，不承诺在全部历史中全局唯一。
- `request_hash` 用于确认重试内容与原请求完全相同。
- `entry_id` 是复制组内稳定的全局单调 ID，用于 WAL、Segment、恢复和审计诊断，但不作为跨 Stream 分页或订阅 Cursor。
- `recorded_at` 由服务端生成，不能由调用者伪造。
- `producer` 来自认证身份，而不是调用者 Header。
- `payload` 对 streamd 完全不透明。
- `byte_offset` 和 `next_byte_offset` 只用于底层定位、顺序读写与校验，不暴露为常规公共 Cursor。

### 5.4 Sequence 与 Offset 语义

空 Stream 的 `next_sequence` 和 `next_byte_offset` 都为 `0`。一次提交成功后：

```text
record.sequence       = previous.next_sequence
stream.next_sequence  = record.sequence + 1

record.byte_offset      = previous.next_byte_offset
stream.next_byte_offset = record.byte_offset + encoded_record_size
```

客户端只保存 `next_sequence`，恢复时从该 Sequence 继续读取。内部 Byte Offset 在 Segment Flush、Merge 和冷热迁移后保持逻辑不变，用于将 Sequence 快速映射到物理位置。

Sequence、Byte Offset 和 `recorded_at` 都天然单调：Sequence 每次加 `1`，Byte Offset 增加 Frame 长度，服务端时间单调不减。因此三者可以共用一套按追加顺序形成的 Record Index，不需要分别排序。

### 5.5 时间语义

系统区分两种时间：

- `recorded_at`：streamd 接受并排序 Record 的服务端时间，用于时间索引。
- 业务发生时间：由调用者放入 Header 或 Payload，streamd 不解释。

为了避免系统时钟回拨破坏索引，`recorded_at` 在单个写入域内必须单调不减。V1 可以使用 `max(wall_clock_now, last_recorded_at)`；未来可演进为 Hybrid Logical Clock。

## 6. Record Frame

Record Frame、WAL、Segment、Manifest、Locator 和 Snapshot 的 V1 字节布局与兼容规则见 [V1 存储格式](docs/storage-format.md)。本节只说明逻辑结构。

Byte Stream 中每条 Record 使用自描述 Frame：

```text
+--------------------+
| Magic + Version    |
+--------------------+
| Frame Length       |
+--------------------+
| Header Length      |
+--------------------+
| Entry ID           |
+--------------------+
| Sequence           |
+--------------------+
| Recorded At        |
+--------------------+
| Request ID         |
+--------------------+
| Headers            |
+--------------------+
| Payload            |
+--------------------+
| CRC32C             |
+--------------------+
```

要求：

- Frame 可以从 Record Index 给出的合法 Byte Offset 独立解码。
- Length 必须有严格上限，防止损坏长度导致巨量分配。
- CRC32C 覆盖 Header 和 Payload。
- Version 决定 Frame 解码方式。
- 未识别的未来 Header 可以跳过。
- Record 不跨 WAL Entry，也不跨 MemTable Chunk；超过常规 Chunk 大小的合法 Record 使用独占 Chunk。

`next_byte_offset` 不必持久化，可由 `byte_offset + frame_length` 计算。

## 7. 公共服务契约

传输协议可以使用 gRPC，另外提供轻量 HTTP Gateway。以下先定义语义，不固定 protobuf 字段编号。

V1 以 gRPC 为规范传输；完整请求、响应、错误和 SDK 语义见 [V1 API 协议](docs/api-protocol.md)。HTTP Gateway 不阻塞核心 V1。

### 7.1 Append

```text
Append {
  namespace
  stream
  expected_sequence
  request_id
  headers
  payload
}

AppendResult {
  sequence
  next_sequence
  entry_id
  recorded_at
  deduplicated
}
```

`expected_sequence` 提供乐观并发控制和定点重试识别：

- 等于当前 `next_sequence`：在该 Sequence 提交新 Record。
- 小于当前 `next_sequence`：直接读取该 Sequence；若 `request_id + request_hash` 相同，则返回原结果并标记 `deduplicated=true`，否则返回 `SEQUENCE_CONFLICT`。
- 大于当前 `next_sequence`：返回 `SEQUENCE_AHEAD`。

V1 要求所有可靠 Append 都携带 `expected_sequence`。调用者在网络结果不确定时必须使用完全相同的 Sequence、Request ID 和内容重试，不能擅自改用新 Tail。

### 7.2 AppendBatch

```text
AppendBatch {
  namespace
  stream
  expected_sequence
  request_id
  records[]
}
```

V1 只支持同一 Stream 的 Batch。Batch 在逻辑上全部成功或全部失败，并只进行一次 Expected Sequence 检查。一次 Batch 占用一段连续 Sequence，跨 Stream Batch 不提供原子性。

### 7.3 Read

```text
Read {
  namespace
  stream
  from_sequence
  max_records
  max_bytes
}

ReadResult {
  records[]
  next_sequence
  current_next_sequence
}
```

读取规则：

- `from_sequence == current_next_sequence` 返回空结果，不是错误。
- `from_sequence > current_next_sequence` 返回 `SEQUENCE_AHEAD`。
- 返回受到 Record 数量和字节数双重限制。
- 单条 Record 超过 `max_bytes` 时返回 `RECORD_TOO_LARGE` 和所需字节，不返回部分 Record。

### 7.4 Subscribe

```text
Subscribe {
  namespace
  stream
  from_sequence
}
```

订阅先补齐历史 Record，再持续发送新 Record。服务端不为慢消费者永久缓存：

- 消费者跟不上时连接可以断开；
- 客户端使用最后确认的 `next_sequence` 重连；
- 订阅语义为 at-least-once；
- streamd 不维护业务消费进度。

### 7.5 ResolveTime

```text
ResolveTime {
  namespace
  stream
  recorded_at
  mode: AT_OR_AFTER | AT_OR_BEFORE
}

ResolveTimeResult {
  sequence
  actual_recorded_at
}
```

时间索引返回对应 Record 的 Sequence。

### 7.6 Inspect

```text
InspectStream {
  namespace
  stream
}

StreamInfo {
  head_sequence
  next_sequence
  record_count
  first_recorded_at?
  last_recorded_at?
}
```

V1 的 `head_sequence` 固定为 `0`，因此 `record_count == next_sequence`，无需额外统计索引。

### 7.7 永远不提供的接口

```text
DeleteRecord
UpdateRecord
TruncateStream
ResetSequence
CompactByKey
```

## 8. 一致性与幂等语义

字段分配、Request Hash、Group Commit、Batch 原子可见和错误恢复语义见 [Append 与提交协议](docs/append-commit-protocol.md)。

### 8.1 单 Stream 顺序

同一写入域内，Stream 的 Append 必须串行分配 Sequence。已提交 Sequence 从 `0` 开始连续且无空洞；并发客户端通过 Compare-And-Append 决定最终顺序。

### 8.2 Read-your-write

Append 返回成功后，任意后续 Read 必须能够读取该 Record，即使 Record 仍位于 MemTable 或 WAL 中。

### 8.3 Sequence-bound Idempotency

streamd 不在全部历史中全量搜索 Request ID。重试必须携带原 `expected_sequence`：

- 如果该 Sequence 尚未被提交，则正常 Append。
- 如果该 Sequence 的 `request_id + request_hash` 与当前请求相同，返回原结果，`deduplicated=true`。
- 如果该 Sequence 已被其他请求占用，返回 `SEQUENCE_CONFLICT`。

该模型将永久幂等检查从任意 Key 点查收敛为已知 Sequence 的直接读取，因此不需要全局 Event ID LSM。上层需要的 Event ID、Causation ID 和 Correlation ID 可以作为 Header 保存并由外部索引查询，但不参与 streamd 的唯一性约束。

### 8.4 Core Index 与单 WAL 事务

决定 Stream 正确性的索引必须内置，包括 Stream Registry、Stream Tail、Unified Record Index 和 Stream Segment Locator。Header 搜索、全文检索、跨 Stream 分析等二级索引可以异步投影到 PostgreSQL、Redis 或搜索引擎，但不能参与 Append 正确性。

一次 Append 的 WAL Entry 包含重建 Record 与全部 Core Index 所需的信息：

```text
WALEntry {
  entry_id
  stream_id
  sequence
  byte_offset
  recorded_at
  request_id
  request_hash
  record_frame
}
```

同一 Stream 由 Shard Writer 串行执行：

```text
1. 检查 expected_sequence。
2. 必要时按已知 Sequence 比较 Request ID 和 Request Hash。
3. 分配 Entry ID、Sequence、Byte Offset 和 Recorded At。
4. 将完整 WALEntry 追加并 fsync。
5. 更新 Active MemTable、Unified Record Index、Tail 投影和 Cache。
6. 发布可见状态并返回成功。
```

第 4 步是持久提交点。第 4 步之后、第 5 步之前崩溃时，启动重放 WAL 恢复全部索引。Core Index 不拥有第二份独立 WAL，也不需要与外部数据库执行 2PC。

外部索引以 `entry_id` 为消费 Cursor，在外部数据库的同一事务中写入索引行和 `indexed_through_entry_id`。它允许落后、失败和重建，只影响查询能力，不影响 Append、Read 或 Sequence 连续性。

### 8.5 不承诺业务 exactly-once

streamd 可以识别遵守 Compare-And-Append 协议的请求重试，但不能保证消费者只处理一次。消费者必须使用自身幂等机制或持久化消费 Sequence。

## 9. 存储引擎架构

MemTable Freeze、Flush、Merge、Snapshot Pin、对象存储和物理回收见 [Segment 生命周期](docs/segment-lifecycle.md)。

```text
Append Request
     |
     v
Stream Registry / Expected Sequence Check
     |
     v
Shard Writer
     |
     +--> assign Entry ID, Sequence, Byte Offset, Recorded At
     |
     v
WAL Encode -> Group Commit -> fsync -> acknowledge durability
     |
     v
MemTable grouped by StreamID
     |
     v
Immutable Segment
     |
     +--> mmap / pread Read Path
     +--> Unified Record Index
     +--> Stream Segment Locator
```

### 9.1 WAL

WAL 是已确认写入的第一持久事实：

WAL File Header、WAL Entry、Seal Footer 和 checksum 覆盖范围见 [V1 存储格式](docs/storage-format.md)。

- 只顺序追加；
- 每条 Entry 有长度、类型和 CRC；
- Entry ID 单调递增；
- 支持 Group Commit；
- WAL 文件达到阈值后轮转；
- 只有对应数据已经安全进入已发布 Segment 后才能删除旧 WAL；
- 启动时只允许截断最后一个 WAL 的尾部半条 Entry；
- 中间 WAL 损坏必须失败关闭。

单节点默认 `SINGLE_SYNC`；生产主备默认 `REPLICATED_STRICT`。可丢失场景只能使用显式、可审计且默认关闭的 `DEGRADED_LOCAL_ONLY`，不能静默降低保证。

### 9.2 MemTable

MemTable 延续 yatsdb 的优势：按 Stream 聚合连续字节块。

```text
MemTable
├── stream 1 -> data chunks + record index [sequence 0, 100)
├── stream 2 -> data chunks + record index [sequence 0, 50)
└── stream 3 -> data chunks + record index [sequence 200, 240)
```

要求：

- 多 Stream 数据可以共享一个 MemTable；
- 单 Stream 内字节严格连续；
- 单 Stream 内 Sequence 严格连续；
- 每次 Append 同时向 Data Chunk 和统一 Record Index 尾部追加；
- 达到内存阈值后冻结为 Immutable MemTable；
- 新写入立即切换到新 MemTable；
- Reader 可以跨 Segment、Immutable MemTable 和 Active MemTable 连续读取；
- 内存分配使用可控 Block Pool，避免超大固定预分配和 GC 压力。

### 9.3 Segment

Segment 是不可变文件，内部按 Stream 排列数据：

Segment V1 的精确 Section、Directory、Dense Index 和 Footer 布局见 [V1 存储格式](docs/storage-format.md)。

```text
Segment Header
Stream Directory sorted by StreamID
  - stream_id
  - first_sequence
  - record_count
  - logical_from_offset
  - logical_to_offset
  - first_recorded_at
  - last_recorded_at
  - data_physical_offset
  - record_index_physical_offset
Unified Record Indexes
Stream Data Blocks
Footer
Segment Checksum
```

这种布局保留 yatsdb 的核心读取优势：已知 StreamID 和 Sequence 后，可以先通过 Stream Segment Locator 定位文件，再由 Segment Directory 和 Record Index 直接定位连续数据范围，不需要扫描其他 Stream。

### 9.4 Segment 发布协议

Flush 必须遵守以下顺序：

```text
1. 创建临时 Segment。
2. 写入完整 Header、Directory、Data、Index 和 Footer。
3. fsync 临时 Segment。
4. 原子 rename 为正式 Segment。
5. fsync Segment 目录。
6. 原子发布新 Manifest 或 Segment 集合。
7. Reader 可以看到新 Segment。
8. 确认 Manifest 持久化。
9. 才允许删除已被覆盖的 WAL。
```

任何顺序倒置都可能在掉电后造成已确认数据丢失。

### 9.5 Segment Merge

Merge 只改变物理布局，不改变逻辑 Stream：

- 输入 Segment 全部不可变；
- 输出 Segment 使用临时文件；
- 输出完成校验并持久化后原子发布；
- Reader 引用旧 Segment 时不能提前回收；
- 新旧数据逐字节或逐 Record 校验一致；
- Sequence、内部 Byte Offset、Entry ID、Recorded At 和 Request ID 保持不变；
- 旧物理 Segment 在新 Segment 和远端副本确认后才可回收。

### 9.6 Manifest

不应仅通过扫描目录猜测当前有效 Segment 集合。V1 应设计版本化 Manifest：

Manifest Generation、Artifact Reference 和 CURRENT 原子指针格式见 [V1 存储格式](docs/storage-format.md)。

```text
Manifest {
  generation
  format_version
  segments[]
  last_entry_id
  checksum
}
```

Manifest 使用写临时文件、`fsync`、rename、目录 `fsync` 的方式原子更新。启动时选择最后一个完整且校验通过的 generation。

## 10. 索引与缓存

各级查询算法、Cache 淘汰、内存预算和并发边界见 [索引与缓存设计](docs/index-cache-design.md)。

索引分为两级：

```text
Level 1: Stream Segment Locator
         StreamID + Sequence/Time/Offset -> Segment

Level 2: Segment Unified Record Index
         Sequence/Time/Offset -> Record Frame
```

streamd 的典型负载是 Append、Tail 和 Subscribe。预计绝大多数读写都发生在最新位置，因此不得将全部历史 Extent 常驻内存。最新状态通过有界 Cache 访问，历史 Locator 按需加载。

### 10.1 Unified Record Index

Sequence、服务端时间和内部 Byte Offset 都按照 Record 追加顺序单调增长，可以共用一份数组：

```text
StreamRecordIndex {
  first_sequence
  first_byte_offset
  first_recorded_at
  record_count

  entries[] {
    relative_byte_offset
    recorded_at_delta
  }
}
```

- Sequence 是 `first_sequence + array_index`，查询为 O(1)。
- Time 在同一数组中单调不减，可以二分查找。
- Byte Offset 单调增加，可以二分查找。
- 物理位置由 `stream_data_base + relative_byte_offset` 计算。
- 不需要为三个维度分别排序，也不需要通用 LSM。

V1 只采用 Dense Index，即每条 Record 一个固定大小索引项，以换取 Sequence 的直接定位。未来 Sparse Index 必须使用新的明确格式版本，不能改变 V1 Index 解释。

### 10.2 Stream Extent

每个 Segment 为其中每个 Stream 生成一条范围描述：

```text
StreamExtent {
  stream_id
  segment_id

  first_sequence
  next_sequence
  first_byte_offset
  next_byte_offset
  first_recorded_at
  last_recorded_at

  record_index_position
  stream_data_position
}
```

同一 Stream 的 Extent 按 Sequence、Byte Offset 和 Time 天然递增，不需要排序。Sequence、Time 或 Byte Offset 查询都可以使用同一条 Extent 链定位 Segment。

### 10.3 Hot Stream Cache

热路径只缓存最新状态：

```text
HotStreamState {
  stream_id
  next_sequence
  next_byte_offset
  last_recorded_at
  last_entry_id

  active_memtable
  latest_segment_id
  recent_extents[]

  pin_count
  last_access
}
```

Append、Read Latest 和 Subscribe 命中该 Cache 后，不访问任何历史 Extent。存在 Active Writer 或 Subscriber 的 Stream 被 Pin；只进行一次历史扫描的 Stream 不得污染长期热集合。Cache 至少区分 Probation 和 Protected 两个 LRU 区域。

Cache Entry 不是事务事实。WAL 提供持久真值，Tail Catalog 提供可重建的最新状态投影。Cache 只允许淘汰能够从二者恢复的状态。

### 10.4 Tail Catalog

Cache Miss 时首先读取 Stream 最新位置，而不是加载完整 Extent 历史：

```text
TailRecord {
  stream_id
  next_sequence
  next_byte_offset
  last_recorded_at
  latest_segment_id
  latest_extent_pointer
  last_entry_id
  applied_entry_id
}
```

内部 StreamID 连续自增时，Tail Catalog 可以使用固定槽位数组：

```text
slot_position = stream_id * fixed_record_size
```

Tail Catalog 可使用 mmap，由操作系统按需加载 Page。它是 WAL/Segment 的投影，不单独拥有事实；崩溃后根据 `applied_entry_id` 重放缺失 WAL Entry 修复。

### 10.5 Cold Extent Locator

TailRecord 只指向最新 Extent Page。历史 Extent 使用不可变 Page 按需读取：

```text
ExtentPage {
  stream_id
  first_sequence
  next_sequence
  first_recorded_at
  last_recorded_at
  extents[]
  previous_page
  skip_pages[]
}
```

每个 Page 保存一组连续 Extent。最新查询只读取当前 Page；旧 Sequence/Time 查询使用 Page Range 和 Skip Pointer 向历史跳转，避免扫描所有 Segment。

Segment Merge 同时合并连续 Extent，使一个 Stream 指向更少的 Segment。新 Locator Page 与 Manifest 发布完成后，旧 Page 才能在无 Reader 引用时回收。

### 10.6 Locator Snapshot

不能在每次启动时扫描所有 Segment 文件。周期性生成：

```text
SegmentLocatorSnapshot {
  manifest_generation
  tail_catalog_checkpoint
  extent_page_roots
  checksum
}
```

启动时加载最近 Snapshot，再重放后续 Manifest 的 Segment Add/Replace 事件。Snapshot 可以落后，因为 Segment Directory 与 Manifest 仍是可重建事实。

### 10.7 Segment Handle Cache

不能永久打开或 mmap 全部 Segment：

```text
SegmentHandleCache {
  segment_id
  file_or_object_location
  ref_count
  last_access
  file_descriptor_or_mmap
}
```

热 Segment 保持打开，冷 Segment 按需打开并通过 LRU 回收。Reader 持有引用时不得关闭或删除 Segment。Payload 数据优先依赖操作系统 Page Cache，避免在应用层重复缓存。

### 10.8 Stream Name Registry

外部字符串 Stream Name 映射为内部数字 StreamID：

```text
(namespace, stream_name) -> stream_id
```

Registry 自身必须使用同等级持久性保证。可以作为特殊系统 Stream 实现，从而避免第二套事实存储。

V1 按 [V1 存储格式](docs/storage-format.md) 固定 `StreamID=0` 为内部 Registry Stream。名称分配记录先进入同一 WAL；Registry Snapshot 只是可重建 Checkpoint，不拥有第二份事实。

## 11. 读取路径

```text
Read(stream, sequence)
      |
      v
Resolve external name -> StreamID
      |
      v
Hot Stream Cache
      |
      v
Active MemTable / Recent Extent / Cold Extent Locator
      |
      v
Segment Unified Record Index
      |
      v
Byte Offset -> Decode Record Frames sequentially
      |
      v
Return records + next_sequence + current_next_sequence
```

读取优先采用 mmap 还是 `pread` 应通过目标部署环境验证：

- mmap 有利于零拷贝式切片和操作系统 Page Cache；
- pread 更容易控制生命周期、并发和大文件地址空间；
- 公共存储格式不能绑定某一种读取实现。

## 12. 订阅与背压

Subscribe 不是另一套消息存储，只是 Read + Wakeup：

```text
1. 注册 Stream Tail 通知。
2. 从客户端 Cursor 读取全部已有 Record。
3. 到达当前 Tail 后等待通知。
4. 有新 Append 时继续 Read。
```

通知只表示“可能有新数据”，不携带唯一事实。即使通知合并或丢失，客户端仍可通过 Cursor 读取完整数据。

慢消费者处理：

- 服务端限制单连接发送缓冲；
- 超过限制后断开连接；
- 客户端从最后 `next_sequence` 重连；
- 不允许慢消费者阻塞 WAL Writer 或 MemTable Flush。

## 13. 崩溃恢复

启动状态机、Manifest/WAL 选择、Commit 边界、投影重建和失败关闭条件见 [崩溃恢复协议](docs/recovery-protocol.md)。

启动恢复顺序：

```text
1. 加载最后一个有效 Manifest。
2. 校验所有已发布 Segment 的元数据和引用，不将全部 Segment 打开或 mmap。
3. 加载 Locator Snapshot、Tail Catalog 和 Extent Page Roots。
4. 找到 Segment 已覆盖的 last_entry_id。
5. 顺序重放更晚的 WAL Entry，修复 Unified Record Index 与 Tail 投影。
6. 最后一个 WAL 尾部不完整时截断到最后合法边界。
7. 校验每个 Stream 的 Sequence、Byte Offset 和 Recorded At 单调连续性。
8. 打开新的 Active WAL，开始服务。
```

### 13.1 必须覆盖的 Crash Point

- WAL 写入一半；
- WAL 写完但尚未 `fsync`；
- WAL 已 `fsync`、MemTable 尚未更新；
- MemTable 冻结过程中；
- Segment 写入一半；
- Segment 已 `fsync`、尚未 rename；
- Segment 已 rename、目录尚未 `fsync`；
- Manifest 更新一半；
- 新 Manifest 已发布、旧 WAL 尚未删除；
- Merge 输出已完成、旧 Segment 尚未回收；
- Append 已持久化但响应丢失，客户端使用原 Expected Sequence、Request ID 和内容重试。

每个 Crash Point 都必须有自动故障注入测试和明确恢复结果。

## 14. 数据完整性

完整性分为三层：

```text
Record CRC32C
    ↓
Segment checksum
    ↓
Manifest checksum / generation
```

可选增加 Stream 级 Hash Chain：

```text
record_hash = H(previous_hash, immutable_record_fields)
```

Hash Chain 对审计有价值，但会让同一 Stream 的 Append 更难并行，也会增加 Frame 成本。V1 不启用 Stream Hash Chain；未来只有在明确威胁模型和基准结果支持时才通过新格式版本增加。

后台 Scrubber 应周期性读取冷 Segment 并验证校验和，以发现静默磁盘损坏。发现损坏后从备份恢复，而不是跳过数据继续服务。

## 15. 压缩与冷热分层

V1 保持热数据不压缩：

```text
Active WAL / MemTable       不压缩
Hot Sealed Segment          不压缩
Cold Segment                未来可按 Block 压缩
Archive Segment             对象存储
```

未来压缩必须满足：

- 只作用于不可变 Segment；
- 按 Block 独立压缩，不能要求解压整个 Segment；
- Block Index 可以从 Sequence 定位对应 Stream Byte Offset；
- 支持 codec/version 字段；
- Merge 和冷热迁移不改变 Sequence 与逻辑 Byte Offset；
- 热读取性能下降必须可量化。

推荐先接受磁盘占用换取性能，通过真实工作负载确定是否需要 Zstd/Snappy 等 Block Codec。

## 16. 永久存储与物理容量

“永不删除”定义为逻辑 Record 永久存在，而不是同一份物理文件永远留在本地 SSD。

长期容量模型：

```text
Local SSD
  - WAL
  - Active Segment
  - Hot Segment

Object Storage
  - Sealed Segment replica
  - Cold / Archive Segment
  - Manifest history
```

本地回收某个 Segment 前，必须确认：

- 对象存储副本上传完成；
- 文件大小和 checksum 一致；
- Manifest 已引用远端位置；
- 远端读取路径经过验证；
- 不存在尚未迁移的唯一副本。

永久保存会与隐私删除和秘密泄露处理产生冲突。streamd 不应接收密码、Token、私钥等秘密；上游必须在 Append 前脱敏。V1 不提供 Namespace 级删除或密钥销毁式删除，需要此类合规能力的部署不能在未完成独立威胁与法规设计前写入相关数据。

## 17. 安全与多租户

streamd 只负责自身 Stream 的访问控制，不理解上层业务 RBAC。

### 17.1 身份

- 服务间优先使用 mTLS；
- 本地开发可以使用短期 Token；
- `producer` 从认证连接获取；
- 调用者不能通过 Header 伪造 producer。

### 17.2 授权

授权粒度：

```text
namespace + stream prefix + operation
```

操作至少包括：

```text
append
read
subscribe
inspect
```

Aegis 的用户 RBAC 仍由 Aegis API Server 负责。Aegis 作为生产者访问 streamd 时，streamd 只验证 Aegis 服务身份及其 Namespace 权限。

### 17.3 加密

- 网络使用 TLS；
- 磁盘可以使用文件系统或块设备加密；
- 对象存储使用独立 Namespace 密钥；
- 加密不能破坏 Segment 校验和和恢复能力。

## 18. 资源限制与故障模式

服务必须显式限制：

- 单条 Record 大小；
- 单 Batch Record 数和总字节数；
- 单连接并发 Append；
- 订阅连接数和发送缓冲；
- MemTable 总内存；
- Immutable MemTable 数量；
- WAL 未 Flush 字节数；
- Hot Stream Cache、Extent Page Cache 和 Segment Handle Cache 内存；
- 单 Namespace 写入速率。

### 18.1 磁盘写满

磁盘达到安全水位后：

- 停止接受新 Append；
- 保持 Read 和 Subscribe 已有数据可用；
- 不删除历史数据自救；
- 暴露明确的 `RESOURCE_EXHAUSTED`；
- 等待扩容或完成已配置的冷存储迁移。

### 18.2 Flush 跟不上写入

- 对 Append 施加背压；
- 不允许无限增加 Immutable MemTable；
- 不通过丢数据或自动降低 durability 缓解压力。

### 18.3 索引损坏

索引可以从 WAL 和 Segment 重建。重建期间服务可以处于只读或不可用状态，但不得返回未经验证的数据。

## 19. 可观测性

生产 Dashboard、告警分级和 Runbook 见 [生产运维设计](docs/operations.md)。

核心指标：

- Append records/bytes per second；
- Append latency：排队、编码、WAL write、fsync、ack；
- Group Commit batch size；
- WAL bytes 和文件数量；
- Active/Immutable MemTable 数量与字节数；
- Flush latency 和 throughput；
- Segment 数量、大小和 Merge 放大；
- Read records/bytes per second；
- Sequence lookup、Extent lookup 和 ResolveTime latency；
- mmap page fault 或 pread latency；
- Subscribe 数量、积压和断开；
- Request retry dedup 和 Sequence conflict；
- Recovery duration 和 replay entries；
- checksum failure；
- 磁盘空间和预计耗尽时间。

日志必须包含 Namespace、Stream 的安全标识、Entry ID、Sequence 和错误阶段，但不得默认输出 Payload。

## 20. Agent Event 使用示例

以下只是调用方示例，不属于 streamd 核心 Schema。

Stream 命名：

```text
namespace: aegis/{tenant}

conversation/{conversation_id}
execution/{execution_id}
model/{invocation_id}
```

Header：

```text
event_type: user.input | llm.input | llm.output | tool.input | tool.output
actor_type: user | agent | system | remote_exec
actor_id: ...
causation_id: ...
correlation_id: ...
content_type: application/json
```

Payload 是 Aegis 定义的业务内容。streamd 只保证它被永久、有序、完整地保存。

## 21. 与 yatsdb 的关系

### 21.1 建议继承的设计

- `Append(streamID, bytes) -> offset` 的底层抽象；
- WAL 顺序追加和 Group Commit；
- 全局 Entry ID；
- 按 Stream 聚合的 MemTable；
- Stream Offset Map；
- 不可变 Segment；
- Segment 内按 Stream 排列；
- mmap 读取路径；
- Segment Merge；
- WAL Reload。

### 21.2 必须重新设计的部分

- 从裸字节扩展为有边界的 Record Frame；
- 字符串 Namespace/Stream Name 与内部 StreamID Registry；
- Compare-And-Append；
- Expected Sequence 与 Request ID 定点重试；
- Unified Record Index；
- Hot Tail Cache 与 Cold Extent Locator；
- 服务端时间进入 Unified Record Index；
- Subscribe 协议；
- CRC、Segment checksum 和 Manifest；
- 完整的 `fsync`/rename/目录同步协议；
- 永久保存，移除基于时间或容量的逻辑 Retention；
- 文件格式版本化；
- 冷热分层；
- 安全、限流、观测和运维接口；
- 系统化故障注入。

### 21.3 代码复用原则

不应让 streamd 直接依赖整个 yatsdb module。可以在验证语义后有选择地迁移：

- WAL Encoder/Decoder 的思想或经过重写的实现；
- MemTable Chunk 组织；
- Segment Stream Directory；
- Stream Reader 跨 Segment 查找算法；
- Merge 算法和性能测试模型。

迁移代码前必须先固定新文件格式和可靠性契约，避免把 TSDB 的历史假设带入独立服务。

## 22. 部署边界

基础 V1 先完成单节点存储语义：

```text
Clients
   |
   v
streamd API
   |
   v
Single storage engine
   |
   +-- Local SSD
   +-- Optional object storage backup
```

单节点不等于不可靠：它仍应具备正确持久化、进程恢复、备份和校验。但节点或磁盘完全丢失时，需要依赖备份恢复。

高可用扩展选择每个 Shard 使用主备 WAL 复制。对象存储或复制块存储可以保存 Snapshot 和 Segment 副本，但不替代 streamd 的 WAL 提交、Term 与 Fencing 语义。

### 22.1 主备高可用扩展

主备复制采用两个完整数据节点和一个外部一致性协调器：

```text
Primary -- WAL replication --> Standby
             |
      external coordinator
       term / lease / fencing
```

核心约束：

- 两节点不能在网络分区下自行完成安全自动选主，Promotion 必须取得协调器的新 Term/Lease，或先人工 Fencing 旧主；
- Primary 分配 Entry ID、Sequence、Byte Offset 和 Recorded At，Standby 原样重放，不重新计算；
- Strict 模式确认成功意味着 WAL 已在主备两端持久化；Standby 不可用时停止写确认但保持读；
- 短时掉线通过增量 WAL 追赶；所需 WAL 已回收时安装 Snapshot，再从 Checkpoint 后继续追赶；
- WAL 可以有界回收，但任何被回收的历史都必须由一个已校验、可安装并被 Pin 的 Snapshot 覆盖；
- Cache 不复制；主备可以从相同逻辑 WAL 构建不同的 Segment 布局；
- 旧 Primary 只能以 Standby/Recovering 身份重入，不能沿用旧 Term 或发布旧尾部。

详细状态机、消息、故障语义和测试矩阵见 [主备复制协议](docs/replication-protocol.md)。

## 23. V1 实施阶段

### 阶段 0：格式与故障模型

- 固定 Record Frame V1；
- 固定 WAL Entry V1；
- 固定 Segment V1；
- 评审并冻结 [V1 存储格式](docs/storage-format.md)；
- 编写文件格式 golden tests；
- 明确所有 Crash Point 的预期行为；
- 建立与 yatsdb 的基准对照。

### 阶段 1：单节点存储引擎

- WAL 和 Group Commit；
- 连续 Sequence 与内部 Byte Offset；
- MemTable；
- Segment Flush；
- Manifest；
- Read；
- Recovery；
- Fault Injection。

### 阶段 2：服务协议

- Namespace 和 Stream Registry；
- Append / AppendBatch；
- Read / Inspect；
- Expected Sequence；
- Request ID retry recognition；
- Unified Record Index；
- Tail Catalog 与 Extent Locator；
- Subscribe；
- Recorded At in Unified Record Index；
- mTLS 和基础授权。

### 阶段 3：生产运维

- Metrics 和 tracing；
- 备份和恢复工具；
- Scrubber；
- 磁盘水位和背压；
- 在线升级；
- Benchmark 与 soak test。

### 阶段 4：容量演进

- 对象存储归档；
- 冷 Segment Block Compression；
- 分片；
- 复制与故障切换。

复制与故障切换按 [主备复制协议](docs/replication-protocol.md) 分阶段实现，默认只接受 Strict 双副本持久化语义。

## 24. 验收标准

### 24.1 正确性

- 并发 Append 后单 Stream Sequence 从 `0` 开始连续、无重复、无空洞。
- Append 确认后立即可读。
- 使用相同 Expected Sequence、Request ID 和内容重试不会产生重复 Record。
- Expected Sequence 可以可靠拒绝并发冲突。
- 读取任意合法 Sequence 返回相同数据。
- Flush、Merge、重启后 Sequence 与逻辑 Byte Offset 不变。
- ResolveTime 返回正确 Record 边界。
- Subscribe 重连后无永久丢失。

### 24.2 崩溃安全

- 对所有列出的 Crash Point 执行进程级和文件级故障注入。
- 已确认 Append 在恢复后全部存在。
- 未确认 Append 可以存在或不存在，但不能产生损坏 Record。
- Active 尾部修复不会影响之前的合法 Record。
- 已封存 Segment 损坏必须被检测。

### 24.3 性能

完整环境矩阵、工作负载、yatsdb 对照、故障与 Soak 方法见 [基准与可靠性验证计划](docs/benchmark-plan.md)。

需要使用统一硬件分别测试：

- 单 Stream 顺序写；
- 多 Stream 高并发写；
- 小 Record 和大 Record；
- sync 和 Group Commit latency；
- Active Tail；
- 热 Segment 随机 Sequence 读取；
- 大量 Stream 的 Tail Cache 命中与冷 Extent Lookup；
- 重启和 WAL Replay；
- Segment Merge 期间的读写抖动；
- 与 yatsdb 原始 Stream Store 的对照。

设计阶段不预设吞吐数字，基准环境和数据分布固定后再定义 SLO。

## 25. V1 已决策事项与验证输入

### 25.1 已决策

- 项目和仓库名为 `streamd`；
- Sequence 是唯一公共 Cursor，Byte Offset 只用于内部诊断；
- Entry ID 是复制组内稳定审计字段，不作为跨 Stream Cursor；
- 所有可靠 Append 强制 Expected Sequence，不提供 Blind Append；
- 单 Stream AppendBatch 严格全部可见或全部不可见；
- gRPC 是 V1 规范协议，HTTP Gateway 可独立增加；
- V1 只实现 Dense Unified Index，不实现 Sparse Index；
- V1 不启用 Stream Hash Chain；WAL 使用 CRC32C 连续链；
- Record 永不逻辑删除，等价替换并解除全部引用后可以回收旧物理副本；
- 对象存储是 Snapshot/归档能力，不参与前台 Strict Commit；
- 热读取默认以 pread 为实现基线，mmap 只作为 Benchmark 候选；
- `StreamID=0` 是 Registry Stream，Tail Slot 固定 128 bytes；
- 主备生产模式默认 REPLICATED_STRICT，Standby Read 和自动 Degraded 默认关闭。

### 25.2 必须由环境提供

- 典型/最大 Record 分布和单 Namespace/Stream 数量级；
- 目标硬件、文件系统、网络 RTT 和对象存储；
- Append/Read/Subscribe SLO 和部署 RTO；
- Coordinator 实现、Lease 时长、时钟漂移预算和 Fencing 能力；
- protobuf Field Number 和 API Package Name；
- yatsdb/旧消息实现中可用于对照的真实数据集与基准结果。

## 26. 核心结论

`streamd` 的第一性能力不是复杂消息语义，而是：

```text
大量 Stream
+ 顺序追加
+ 连续 Sequence
+ 任意位置读取
+ 实时 Tail
+ 崩溃后不丢已确认数据
```

服务层保持 Record 语义稳定，底层保留 yatsdb 已验证的 Byte Stream 高性能路径。V1 先证明单节点正确性、持久性和性能；压缩、对象存储、分片和复制均在真实数据证明需要后演进。
