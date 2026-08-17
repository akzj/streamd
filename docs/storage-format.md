# streamd V1 存储格式

| 属性 | 内容 |
| --- | --- |
| 状态 | Draft / 待评审 |
| 版本 | V1 |
| 范围 | Record Frame、WAL、Segment、Manifest、Locator、Registry 与 Snapshot 的持久化格式 |
| 设计目标 | 可直接实现、可校验恢复、可版本演进、不依赖 Go 内存布局 |

## 1. 目的

本文把 `DESIGN.md` 中的逻辑对象固定为可持久化的 V1 格式。它回答：

- 哪些字节构成一条永久 Record；
- WAL、复制流和 Segment 是否保存同一份 Record 字节；
- Sequence、Entry ID、Byte Offset 和 Recorded At 如何编码；
- Segment 如何定位 Stream、Record Index 和数据；
- Manifest、CURRENT 和 Snapshot 如何形成原子可恢复的文件集合；
- checksum 覆盖哪些范围，损坏后允许和禁止做什么；
- 新版本如何与旧文件共存，而不原地改写历史数据。

本文只定义存储格式和发布边界，不定义 API protobuf、完整 Append 状态机或启动恢复算法。相关行为分别由后续协议文档定义。

## 2. 格式不变量

V1 必须满足：

1. Record Frame 是逻辑 Record 的稳定编码。
2. 同一条 Record 在 WAL、主备复制和 Segment 中使用完全相同的 Frame 字节。
3. `next_byte_offset = byte_offset + frame_length`，Frame 在 Flush、Merge、Snapshot 和冷热迁移后不得重新编码。
4. WAL Entry 额外保存复制顺序和快速校验字段，但不得与 Frame 中的字段矛盾。
5. Segment、Manifest、Locator Page 和 Snapshot 一经发布即不可变。
6. 文件名只用于发现候选文件，文件内容中的 Magic、Version、ID、Length 和 checksum 才是事实。
7. 任何 Length、Count、Offset 在使用前都必须完成上限、下限、加法溢出和文件边界校验。
8. 不能把 Go struct、内存指针、机器字长、map 迭代顺序或 native endian 直接写入磁盘。
9. 未识别的必需 Flag、Codec 或结构版本必须失败关闭，不能猜测解析。
10. Cache 不是持久事实；Tail Catalog 和 Locator Snapshot 也是可从 Segment、Manifest 和 WAL 重建的投影。

## 3. 通用编码约定

### 3.1 标量类型

| 类型 | 编码 |
| --- | --- |
| `u8` | 1 字节无符号整数 |
| `u16` | 2 字节 Little Endian |
| `u32` | 4 字节 Little Endian |
| `u64` | 8 字节 Little Endian |
| `i64` | 8 字节 Little Endian 二进制补码 |
| `bool` | `u8`，只能为 `0` 或 `1` |
| `UUID` | 16 个原始字节，按 RFC 4122 网络显示顺序打印 |
| `Timestamp` | `i64` Unix Nanoseconds，UTC |
| `CRC32C` | Castagnoli 多项式，保存为 `u32` Little Endian |
| `SHA256` | 32 个原始 digest 字节 |

所有磁盘 Offset 都是从当前文件第一个字节开始的绝对 `u64` Offset，除非字段明确命名为 `relative_*`。

### 3.2 ID 与范围

| ID | V1 类型 | 分配规则 |
| --- | --- | --- |
| `StreamID` | `u64` | `0` 保留给内部 Registry Stream；用户 Stream 从 `1` 单调分配 |
| `EntryID` | `u64` | 复制组内连续，从 `0` 开始 |
| `Sequence` | `u64` | Stream 内连续，从 `0` 开始 |
| `ByteOffset` | `u64` | Stream 内连续，从 `0` 开始 |
| `Term` | `u64` | 外部协调器单调分配 |
| `SegmentID` | `UUID` | 随机生成，不能复用 |
| `SnapshotID` | `UUID` | 随机生成，不能复用 |
| `FileID` | `UUID` | 每个物理文件随机生成 |

V1 不允许任何计数器回绕。分配下一值会溢出时，该写入域进入只读故障状态，必须通过未来格式版本迁移，不能重新从 `0` 开始。

可选水位不通过 `u64(-1)` 表示。固定结构使用独立 `has_value` Flag；集合为空时，`first/last` 字段必须为零并由 Count 等于零判定为无效。

### 3.3 字符串与字节串

- 所有字符串使用 UTF-8，不保存结尾 `NUL`；
- 长度是字节数，不是 Unicode 字符数；
- Namespace、Stream Name 和 Header Key 必须是合法 UTF-8；
- Payload、Header Value 和 Request ID 是不透明字节；
- 同一逻辑键不做 Unicode Normalization，调用者必须使用完全相同的 UTF-8 字节；
- 字符串比较和排序使用未签名字节的字典序。

### 3.4 对齐与 Padding

- 固定 Header 的大小必须是 8 的倍数；
- Segment 的 Directory、Index、Data 和 Footer Section 起点按 4096 字节对齐；
- Section 内的 Record Frame 和 Index Entry 紧密排列，不逐条对齐；
- Padding 字节必须写为零，Reader 必须忽略已声明的 Padding，但校验摘要覆盖这些字节；
- V1 不要求 Direct I/O，不能依赖设备扇区原子写。

### 3.5 校验算法

V1 使用两类校验：

- CRC32C：保护单条 Record、WAL Entry、Page 和小型元数据，目标是快速检测意外损坏；
- SHA-256：标识不可变文件的规范 Content Region 和 Snapshot Artifact，目标是跨机器、对象存储和长期保存时确认内容完全一致。

包含自身 Digest 的 Footer 无法参与自身摘要。每种文件格式必须明确 `content_sha256` 的起止范围；Manifest 和 Snapshot 中的 Artifact Digest 必须使用被引用格式定义的同一范围，并同时校验文件大小和 Footer CRC。

CRC32C 不是认证机制。网络传输仍必须使用 mTLS，对象存储仍必须进行身份授权。

### 3.6 Flags 兼容规则

每个结构的 `flags` 为 `u32` 或 `u16`：

- 低半区保留给“可忽略提示 Flag”；未知时可以忽略；
- 高半区保留给“解析必需 Flag”；出现未知位必须拒绝读取；
- V1 Writer 必须把所有未使用位写为零；
- 每个结构章节必须列出 V1 允许的非零位，否则 V1 合法值只有零。

### 3.7 通用不可变 Artifact Footer

Tail Catalog Checkpoint、Locator Pack、Locator Snapshot 和 Registry Snapshot 使用同一个 88 字节 Footer：

```text
ArtifactFooter {
  magic             [8]byte = "ARTENDV1"
  format_version    u16 = 1
  footer_length     u16 = 88
  artifact_type     u16
  flags             u16 = 0
  artifact_id       UUID
  file_length       u64
  content_length    u64
  content_sha256    SHA256
  footer_crc32c     u32
  reserved          u32
}
```

`content_sha256` 覆盖文件 Offset `[0, content_length)`，`file_length = content_length + 88`。Footer CRC 覆盖此前 Footer 字段，不覆盖自身和 Reserved。拥有该 Footer 且全部校验通过的文件才是可被 Manifest/Snapshot 引用的 Sealed Artifact；Active 投影文件不能被引用。

V1 Artifact Type 编号固定为：`1=TAIL_CATALOG`、`2=LOCATOR_SNAPSHOT`、`3=REGISTRY_SNAPSHOT`、`4=LOCATOR_PACK`、`5=SNAPSHOT_MANIFEST`、`6=MANIFEST`、`7=SEGMENT`、`8=REPLICATION_STATE`。类型 1 到 5 以及类型 8 使用通用 Artifact Footer；Manifest 和 Segment 使用各自 Footer。

## 4. Record Frame V1

### 4.1 稳定性边界

Record Frame 是 Stream 逻辑字节流中的基本单元：

```text
Stream logical bytes = Frame[sequence=0] || Frame[1] || ...
```

因此：

```text
frame.sequence       = previous.sequence + 1
frame.byte_offset    = previous.byte_offset + previous.frame_length
next_byte_offset     = frame.byte_offset + frame.frame_length
```

Frame 字节生成并写入 WAL 后永久冻结。Segment Writer 必须复制 Frame 原始字节，不得重新排序 Header、改变 Padding、替换 checksum 或使用新的 Codec。

### 4.2 布局

```text
+------------------------------+
| Fixed Prefix                 | 32 bytes
+------------------------------+
| Fixed Metadata               | 80 bytes
+------------------------------+
| Request ID                   | request_id_length
+------------------------------+
| Producer                     | producer_length
+------------------------------+
| Encoded Headers              | headers_length
+------------------------------+
| Payload                      | payload_length
+------------------------------+
| Frame CRC32C                 | 4 bytes
+------------------------------+
```

`fixed_header_length` V1 固定为 `112`，表示从 Frame 起点到 Request ID 起点的字节数。

### 4.3 Fixed Prefix

| Offset | 字段 | 类型 | V1 值/含义 |
| ---: | --- | --- | --- |
| 0 | `magic` | `[4]byte` | ASCII `SRF1` |
| 4 | `format_version` | `u16` | `1` |
| 6 | `flags` | `u16` | V1 为 `0` |
| 8 | `frame_length` | `u32` | 包含最终 CRC 的完整长度 |
| 12 | `fixed_header_length` | `u16` | `112` |
| 14 | `header_count` | `u16` | Header Entry 数量 |
| 16 | `headers_length` | `u32` | Encoded Headers 总字节数 |
| 20 | `payload_length` | `u32` | Payload 字节数 |
| 24 | `producer_length` | `u16` | Producer UTF-8 字节数 |
| 26 | `request_id_length` | `u16` | Request ID 字节数 |
| 28 | `reserved` | `u32` | 必须为 `0` |

必须验证：

```text
frame_length == fixed_header_length
              + request_id_length
              + producer_length
              + headers_length
              + payload_length
              + 4
```

每次加法都必须先做 overflow 检查。

### 4.4 Fixed Metadata

| Offset | 字段 | 类型 | 含义 |
| ---: | --- | --- | --- |
| 32 | `entry_id` | `u64` | 全局 WAL 顺序 |
| 40 | `stream_id` | `u64` | 内部 StreamID |
| 48 | `sequence` | `u64` | Stream 内公共 Cursor |
| 56 | `byte_offset` | `u64` | Stream 内逻辑 Frame 起点 |
| 64 | `recorded_at` | `i64` | 服务端单调不减时间 |
| 72 | `batch_index` | `u32` | 当前 Record 在 AppendBatch 中的位置 |
| 76 | `batch_count` | `u32` | AppendBatch Record 总数，单条 Append 为 `1` |
| 80 | `request_hash` | `[32]byte` | 整个 Append/AppendBatch 规范化请求的 SHA-256 |

要求：

- `batch_count >= 1`；
- `batch_index < batch_count`；
- 同一个 Batch 的所有 Frame 使用相同 Request ID、Request Hash 和 Batch Count；
- 同一个 Batch 的 `batch_index` 从 `0` 连续到 `batch_count - 1`；
- 同一个 Batch 只属于一个 Stream，Sequence 和 Entry ID 均连续；
- Request Hash 的规范化输入由 Append 协议定义，Reader 不重新计算它。

Batch 字段进入永久 Frame，使 WAL Recovery 和 Segment-only Recovery 都能识别不可拆分的可见性单元。

### 4.5 Header Encoding

Header Entry 格式：

```text
HeaderEntry {
  key_length    u16
  flags         u16   // V1 = 0
  value_length  u32
  key           [key_length]byte
  value         [value_length]byte
}
```

编码要求：

- Header 按 Key 原始 UTF-8 字节严格升序排列；
- 禁止重复 Key；
- Key 不能为空；
- `header_count` 必须与成功解析的 Entry 数相同；
- 所有 Entry 消耗的字节数必须恰好等于 `headers_length`，不能存在未声明尾部。

规范化排序只发生在首次构造 Frame 时，之后不得再次编码。

### 4.6 Frame CRC

最终 `frame_crc32c` 覆盖从 `magic` 开始到 Payload 最后一个字节结束的全部内容，不覆盖 CRC 字段自身。

Reader 必须先验证最小长度和各 Length 边界，再在有界 Slice 上计算 CRC。CRC 错误的 Frame 不得返回部分 Header 或 Payload。

### 4.7 V1 硬限制

| 项目 | 格式硬限制 |
| --- | ---: |
| Frame 总长度 | 256 MiB |
| Request ID | 256 bytes |
| Producer | 1024 bytes |
| Header 数 | 1024 |
| 单个 Header Key | 1024 bytes |
| Headers 总长度 | 4 MiB |
| Batch Record 数 | 65535 |

服务配置可以设置更小限制，不能设置超过格式硬限制的值。V1 Writer 不能生成超限 Frame；Reader 遇到超限声明应返回格式损坏或不支持错误，不能按声明分配内存。

## 5. WAL V1

### 5.1 目录和命名

```text
wal/
  WAL-<20-digit-first-entry-id>-<file-id>.log
  WAL-CURRENT
```

十进制 Entry ID 固定补零只用于运维排序。Reader 必须从 WAL File Header 获得真实 `first_entry_id` 和 `file_id`。

### 5.2 WAL 文件布局

```text
+------------------------------+
| WAL File Header              | 64 bytes
+------------------------------+
| WAL Entry                    | variable
+------------------------------+
| ...                          |
+------------------------------+
| WAL Seal Footer              | sealed file only
+------------------------------+
```

Active WAL 没有 Seal Footer。只有当前 Active WAL 可以没有 Footer；更早的 WAL 缺少 Footer 视为异常，必须由恢复协议判断是未完成轮转还是损坏。

`WAL-CURRENT` 使用原子替换的小型指针：

```text
WALCurrentPointer {
  magic              [8]byte = "WALCURV1"
  format_version     u16 = 1
  length             u16
  flags              u32 = 0
  file_id            UUID
  first_entry_id     u64
  file_name_length   u16
  reserved           u16
  file_name          bytes
  crc32c             u32
}
```

File Name 只能是 `wal/` 下的单一相对文件名。Active WAL 内容持续增长，因此指针不保存文件 SHA-256。轮转时必须先 Seal 旧文件、持久化新 WAL Header，再原子替换 `WAL-CURRENT` 并 `fsync` 数据根目录。

### 5.3 WAL File Header

| Offset | 字段 | 类型 | 含义 |
| ---: | --- | --- | --- |
| 0 | `magic` | `[8]byte` | ASCII `STRMWAL1` |
| 8 | `format_version` | `u16` | `1` |
| 10 | `header_length` | `u16` | `64` |
| 12 | `flags` | `u32` | V1 为 `0` |
| 16 | `file_id` | `UUID` | 物理 WAL 文件 ID |
| 32 | `first_entry_id` | `u64` | 文件预计保存的第一个 Entry |
| 40 | `created_term` | `u64` | 创建文件时 Term |
| 48 | `created_at` | `i64` | 创建时间 |
| 56 | `header_crc32c` | `u32` | 覆盖 Offset `[0,56)` |
| 60 | `reserved` | `u32` | 必须为 `0` |

空 WAL 允许 Header 后没有 Entry。其 `first_entry_id` 表示下一条 Entry 的预期 ID。

### 5.4 WAL Entry

```text
+------------------------------+
| WAL Entry Header             | 96 bytes
+------------------------------+
| Record Frame                 | record_frame_length
+------------------------------+
| WAL Entry CRC32C             | 4 bytes
+------------------------------+
```

WAL Entry Header：

| Offset | 字段 | 类型 | 含义 |
| ---: | --- | --- | --- |
| 0 | `magic` | `[4]byte` | ASCII `SWE1` |
| 4 | `format_version` | `u16` | `1` |
| 6 | `flags` | `u16` | V1 为 `0` |
| 8 | `entry_length` | `u32` | Header + Frame + 最终 CRC |
| 12 | `header_length` | `u16` | `96` |
| 14 | `reserved_0` | `u16` | 必须为 `0` |
| 16 | `term` | `u64` | Entry 被分配时的 Term |
| 24 | `entry_id` | `u64` | 连续全局顺序 |
| 32 | `stream_id` | `u64` | 目标 Stream |
| 40 | `sequence` | `u64` | Stream Sequence |
| 48 | `byte_offset` | `u64` | Stream 逻辑 Byte Offset |
| 56 | `recorded_at` | `i64` | 服务端时间 |
| 64 | `batch_index` | `u32` | 与 Frame 相同 |
| 68 | `batch_count` | `u32` | 与 Frame 相同 |
| 72 | `record_frame_length` | `u32` | 嵌入 Frame 长度 |
| 76 | `previous_entry_crc32c` | `u32` | 前一 Entry 的完整 Entry CRC；全局 `EntryID=0` 时为 `0` |
| 80 | `reserved_1` | `[12]byte` | 必须全部为 `0` |
| 92 | `header_crc32c` | `u32` | 覆盖 Header Offset `[0,92)` |

最终 `wal_entry_crc32c` 覆盖完整 96 字节 Header 和 Record Frame，不覆盖自身。

WAL Entry Header 中与 Record Frame 重复的字段必须逐项相等。重复是有意设计：WAL 扫描器可以在解析可变 Record 之前验证复制顺序，而完整 Reader 可以检测 envelope/frame 不一致。

`previous_entry_crc32c` 用于快速检测意外分叉和错接文件，不是密码学 Hash Chain。主备节点不是互相恶意的安全模型；是否在未来版本增加 Entry SHA-256 Chain，必须经过吞吐基准后决定。

### 5.5 WAL 连续性

合法 WAL 集合必须满足：

- WAL 文件按 Header 的 `first_entry_id` 形成连续范围；
- 文件内第一条 Entry ID 等于 Header 的 `first_entry_id`；
- 后续 Entry ID 每次加 `1`；
- 每条 Entry 的 `previous_entry_crc32c` 等于前一条完整 Entry 的最终 CRC；
- 跨 WAL 文件时，第一条 Entry 继续引用前一文件最后 Entry 的 CRC；
- 同一 Stream 的 Sequence、Byte Offset 和 Recorded At 单调规则成立；
- Batch 不能跨 WAL 文件，Writer 必须在 Batch 开始前轮转。

禁止通过跳过损坏 Entry 恢复后续 Entry，因为这会破坏全局 Entry 顺序和 Stream 连续性。

### 5.6 WAL Seal Footer

Sealed WAL Footer 固定 96 字节：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `magic` | `[8]byte` | ASCII `WALSEAL1` |
| `format_version` | `u16` | `1` |
| `footer_length` | `u16` | `96` |
| `flags` | `u32` | V1 为 `0` |
| `file_id` | `UUID` | 必须与 Header 一致 |
| `entry_count` | `u64` | Entry 数量 |
| `last_entry_id` | `u64` | Count 为零时无效并写零 |
| `last_entry_crc32c` | `u32` | Count 为零时为零 |
| `reserved` | `u32` | 必须为零 |
| `content_sha256` | `[32]byte` | Header + 全部 WAL Entry，不含 Footer |
| `footer_crc32c` | `u32` | 覆盖 Footer 中此前所有字段 |
| `reserved_tail` | `u32` | 必须为零 |

Seal Footer 写入并 `fsync` 后文件不可再追加。WAL 文件内容相同但 File ID 不同，不视为同一个物理 Artifact。

### 5.7 WAL 尾部损坏

- Active WAL 最后一条不完整 Entry 可以截断到上一条完整、CRC 正确的边界；
- Active WAL 中间 Entry 损坏不得跳过；
- Sealed WAL 的 Header、任意 Entry、Footer 或 SHA-256 不匹配都视为完整性故障；
- 截断前必须确认该 WAL 是当前合法 Active WAL，而不是误把损坏的 Sealed WAL 当作 Active；
- 截断只处理未形成完整 Entry 的物理尾部，不决定该 Entry 是否 Committed。

## 6. Segment V1

### 6.1 文件命名与布局

```text
segments/
  SEG-<segment-id>.seg
```

```text
+------------------------------+
| Segment Header               | fixed, 4 KiB section
+------------------------------+
| Stream Directory             | sorted by StreamID
+------------------------------+
| Unified Record Indexes       | grouped by StreamID
+------------------------------+
| Stream Data                  | grouped by StreamID
+------------------------------+
| Segment Footer               | fixed, final 4 KiB section
+------------------------------+
```

各 Section 起点 4096 字节对齐，Section 长度记录实际有效字节，不包括到下一 Section 的零 Padding。

### 6.2 Segment Header

Segment Header 的有效 Header 固定为 160 字节，其余 Header Section 字节为零：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `magic` | `[8]byte` | ASCII `STRMSEG1` |
| `format_version` | `u16` | `1` |
| `header_length` | `u16` | `160` |
| `flags` | `u32` | V1 为 `0` |
| `segment_id` | `UUID` | 不可复用 ID |
| `created_at` | `i64` | 创建时间 |
| `first_entry_id` | `u64` | Segment 中最小 Entry ID |
| `last_entry_id` | `u64` | Segment 中最大 Entry ID |
| `stream_count` | `u64` | Directory Entry 数 |
| `record_count` | `u64` | 总 Record 数 |
| `directory_offset` | `u64` | Stream Directory 起点 |
| `directory_length` | `u64` | 有效字节数 |
| `index_offset` | `u64` | Unified Index Section 起点 |
| `index_length` | `u64` | 有效字节数 |
| `data_offset` | `u64` | Stream Data Section 起点 |
| `data_length` | `u64` | 有效字节数 |
| `footer_offset` | `u64` | Footer Section 起点 |
| `index_entry_size` | `u32` | V1 为 `24` |
| `record_codec` | `u16` | V1 为 `0 = NONE` |
| `reserved_0` | `u16` | 必须为零 |
| `reserved_1` | `[20]byte` | 必须为零 |
| `header_crc32c` | `u32` | 覆盖此前 156 字节，使 Header 达到 160 字节 |

空 Segment 不允许发布。`first_entry_id <= last_entry_id`，但 Segment 内 Entry ID 不要求物理连续，因为数据按 Stream 分组，且一次 Flush/Merge 可以覆盖多个来源范围。

### 6.3 Stream Directory Entry

Directory 按 `stream_id` 严格升序，V1 Entry 固定 112 字节：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `stream_id` | `u64` | StreamID |
| `first_sequence` | `u64` | 本 Extent 第一个 Sequence |
| `record_count` | `u64` | Record 数量 |
| `first_byte_offset` | `u64` | 第一个 Frame 的逻辑 Offset |
| `next_byte_offset` | `u64` | 最后 Frame 之后的逻辑 Offset |
| `first_recorded_at` | `i64` | 第一个时间 |
| `last_recorded_at` | `i64` | 最后时间 |
| `first_entry_id` | `u64` | 本 Extent 最小 Entry ID |
| `last_entry_id` | `u64` | 本 Extent 最大 Entry ID |
| `record_index_offset` | `u64` | 绝对文件 Offset |
| `record_index_length` | `u64` | 必须等于 `record_count * 24` |
| `stream_data_offset` | `u64` | 第一条 Frame 的物理 Offset |
| `stream_data_length` | `u64` | Frame 字节总数 |
| `entry_crc32c` | `u32` | 覆盖此前字段 |
| `reserved` | `u32` | 必须为零 |

必须满足：

```text
next_sequence = first_sequence + record_count
next_byte_offset = first_byte_offset + stream_data_length
```

V1 Segment 内一个 Stream 只能出现一个 Directory Entry。Merge 必须把输入中的连续 Extent 合并成一个输出 Extent；发现 Sequence 或 Byte Offset 不连续时不能生成该 Segment。

### 6.4 Unified Record Index Entry

每条 Record 对应一个 24 字节 Dense Index Entry：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `relative_byte_offset` | `u64` | 相对该 Stream `first_byte_offset` |
| `recorded_at_delta` | `u64` | 相对 `first_recorded_at` 的非负纳秒数 |
| `frame_length` | `u32` | Record Frame 完整长度 |
| `frame_crc32c` | `u32` | Frame 自身最终 CRC 值 |

第 `i` 个 Index Entry 隐式表示：

```text
sequence      = first_sequence + i
byte_offset   = first_byte_offset + relative_byte_offset
recorded_at   = first_recorded_at + recorded_at_delta
physical_pos  = stream_data_offset + relative_byte_offset
```

要求：

- 第一条 `relative_byte_offset == 0`；
- 相邻 Relative Offset 之差等于前一条 `frame_length`；
- 最后一条 Offset 加 Frame Length 等于 `stream_data_length`；
- `recorded_at_delta` 单调不减；
- Index 推导字段必须与实际 Frame 字段相等；
- `frame_crc32c` 必须与 Frame 尾部保存值相等。

Dense Index 用空间换取 Sequence O(1) 定位。未来 Sparse Index 必须使用新 Segment 格式或明确 Codec，V1 Reader 不能把未知 Index Entry Size 当作 V1 解释。

### 6.5 Stream Data Section

- Stream 按 Directory 顺序排列；
- 每个 Stream 内是原始 Record Frame 的紧密连接；
- Stream 之间不要求 4 KiB 对齐，避免大量小 Stream 产生空间放大；
- V1 `record_codec = NONE`，不压缩、不加密、不重新分块；
- 文件系统/块设备加密位于 Segment 格式之外。

未来 Block Compression 不能改变解压后的 Frame 字节和逻辑 Byte Offset，需要新的 Segment Version 或必需 Flag。

### 6.6 Segment Footer

Footer 有效内容固定 104 字节，位于最终 4 KiB Section：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `magic` | `[8]byte` | ASCII `SEGENDV1` |
| `format_version` | `u16` | `1` |
| `footer_length` | `u16` | `104` |
| `flags` | `u32` | V1 为 `0` |
| `segment_id` | `UUID` | 与 Header 相同 |
| `file_length` | `u64` | 包含完整 Footer Section 的文件长度 |
| `content_length` | `u64` | 从文件起点到 Footer Section 起点 |
| `stream_count` | `u64` | 与 Header 相同 |
| `record_count` | `u64` | 与 Header 相同 |
| `content_sha256` | `[32]byte` | 文件 Offset `[0, footer_offset)` |
| `footer_crc32c` | `u32` | 覆盖此前 Footer 字段 |
| `reserved` | `u32` | 必须为零 |

Footer Section 剩余字节必须为零，并包含在 `file_length` 中，但不包含在 `content_sha256` 中。Footer CRC 不覆盖 Section Padding。

### 6.7 Segment 验证级别

Reader 可以分层验证：

1. **Open Check**：Header/Footer Magic、Version、CRC、ID、Length 和 Section 边界；
2. **Directory Check**：Directory 排序、Entry CRC、Index/Data Range；
3. **Read Check**：读取目标 Frame 时校验 Index 与 Frame CRC；
4. **Scrub Check**：读取全文件，验证所有 Frame 和 `content_sha256`。

Manifest 发布前必须完成 Scrub Check。运行时打开冷 Segment 不要求每次重新计算全文件 SHA-256，但后台 Scrubber 必须周期验证。

## 7. Manifest 与 CURRENT V1

### 7.1 文件集合

```text
manifests/
  MANIFEST-<20-digit-generation>-<file-id>.bin
CURRENT
```

Manifest 每个 Generation 是新不可变文件，不原地更新。

### 7.2 Manifest 布局

```text
+------------------------------+
| Manifest Header              |
+------------------------------+
| Segment Reference[]          |
+------------------------------+
| Artifact Reference[]         |
+------------------------------+
| Manifest Footer              |
+------------------------------+
```

Manifest Header 固定部分：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `magic` | `[8]byte` | ASCII `STRMMAN1` |
| `format_version` | `u16` | `1` |
| `header_length` | `u16` | V1 为 `136` |
| `flags` | `u32` | V1 为 `0` |
| `file_id` | `UUID` | Manifest 物理文件 ID |
| `generation` | `u64` | 从 `0` 单调递增 |
| `previous_generation` | `u64` | Generation 0 写 `0` |
| `previous_manifest_sha256` | `[32]byte` | Generation 0 全零 |
| `created_at` | `i64` | 创建时间 |
| `last_entry_id` | `u64` | `record_count=0` 时无效写零 |
| `last_entry_crc32c` | `u32` | Checkpoint 最后一条 WAL Entry CRC；空集合写零 |
| `reserved_0` | `u32` | 必须为零 |
| `record_count` | `u64` | Manifest 覆盖的逻辑 Record 总数 |
| `segment_count` | `u64` | Segment Reference 数 |
| `artifact_count` | `u32` | 其他 Artifact Reference 数 |
| `reserved_1` | `u32` | 必须为零 |
| `reserved_2` | `u32` | 必须为零，使 V1 Header 为 136 字节 |
| `header_crc32c` | `u32` | 覆盖此前 Header 字段 |

`header_length` 允许未来在 Header 尾部追加可选字段。V1 Writer 不写未定义扩展。

Manifest 是一个连续 Segment Checkpoint：当 `record_count > 0` 时，它必须完整覆盖所有不晚于 `last_entry_id` 的已提交 Data/Registry Record，不能只保存该范围的稀疏子集。`last_entry_crc32c` 使 WAL 已被 Snapshot/GC 回收后，后续 WAL 仍能从 Checkpoint 继续校验 Entry CRC 链。

### 7.3 Segment Reference

每条 Reference 使用 `entry_length u32` 开头，因此未来可以增加尾部字段：

```text
SegmentReference {
  entry_length              u32
  flags                     u32
  segment_id                UUID
  file_size                 u64
  first_entry_id            u64
  last_entry_id             u64
  stream_count              u64
  record_count              u64
  local_path_length         u16
  object_location_length    u16
  reserved                  u32
  content_sha256            SHA256
  local_path                bytes
  object_location           bytes
  entry_crc32c              u32
}
```

V1 Flags：

- `HAS_LOCAL = 0x00000001`；
- `HAS_OBJECT = 0x00000002`；
- `REQUIRED_FLAG_MASK = 0xffff0000`，V1 必须为零。

至少设置一个 Location Flag。路径是 UTF-8、相对于 streamd 数据根的规范路径，禁止绝对路径和 `..`。Object Location 是不含临时凭据的稳定对象标识；访问凭据不进入 Manifest。

`file_size` 必须是完整 V1 Segment 的 4096 字节对齐长度，最小为 20480 字节。

Reference 按 `segment_id` 字节升序编码，Manifest 的语义不依赖操作系统目录顺序。

### 7.4 Artifact Reference

Manifest 可以引用构成同一 Checkpoint 的其他文件：

```text
ArtifactReference {
  entry_length       u32
  artifact_type      u16   // 使用 3.7 的 Artifact Type 编号
  format_version     u16
  flags              u32
  artifact_id        UUID
  file_size          u64
  covered_entry_id   u64
  path_length        u16
  reserved           u16
  content_sha256     SHA256
  path               bytes
  entry_crc32c       u32
}
```

Artifact 是投影。缺失或损坏时允许从 Segment/WAL 重建，但声称“可安装 Snapshot”的 Manifest 必须包含并验证其声明的所有必需 Artifact。

Artifact Reference 按 `(artifact_type, artifact_id)` 严格升序编码，同一键不允许重复。

### 7.5 Manifest Footer

```text
ManifestFooter {
  magic             [8]byte = "MANENDV1"
  format_version    u16 = 1
  footer_length     u16 = 88
  flags             u32
  file_id           UUID
  generation        u64
  file_length       u64
  content_sha256    SHA256  // footer 之前全部字节
  footer_crc32c     u32
  reserved          u32
}
```

Manifest 解析必须恰好消费 Header 声明数量的 Reference，并到达 Footer 起点；多余或缺少字节都视为损坏。

### 7.6 CURRENT

CURRENT 是指向唯一已发布 Manifest 的小型文件：

```text
CurrentPointer {
  magic                    [8]byte = "STRMCUR1"
  format_version           u16 = 1
  length                   u16   // V1 为 80 + manifest_name_length
  flags                    u32
  generation               u64
  manifest_file_id         UUID
  manifest_name_length     u16
  reserved                 u16
  manifest_sha256          SHA256
  manifest_name            bytes
  crc32c                   u32
}
```

Manifest Name 只能是 `manifests/` 下的相对文件名，不包含路径分隔符。

发布顺序：

```text
1. 写完整新 Segment/Artifact 临时文件。
2. fsync 文件，rename 为正式文件，fsync 对应目录。
3. 写 Manifest 临时文件并完成全部校验。
4. fsync Manifest，rename 为正式文件，fsync manifests/。
5. 写 CURRENT.tmp，fsync。
6. rename CURRENT.tmp -> CURRENT。
7. fsync 数据根目录。
8. 新 Generation 才算发布。
```

旧 Manifest 和 Segment 的回收属于单独生命周期协议，不能在第 8 步前发生。

## 8. Tail Catalog V1

Tail Catalog 是固定槽位的可重建投影：

```text
catalog/
  TAIL-ACTIVE.cat
  TAIL-<artifact-id>.cat
```

`TAIL-ACTIVE.cat` 可以 mmap 原地更新，不能被 Manifest 或 Snapshot 引用。创建 Checkpoint 时冻结一致视图、写入新的 Artifact ID 和 Covered Entry ID、追加通用 Artifact Footer，随后 rename 为 `TAIL-<artifact-id>.cat`；Sealed Checkpoint 永不修改。

### 8.1 Header

```text
TailCatalogHeader {
  magic                 [8]byte = "STRMTAIL"
  format_version        u16 = 1
  header_length         u16       // V1 = 72
  flags                 u32
  artifact_id           UUID
  slot_size             u32       // V1 = 128
  reserved_0            u32
  slot_count            u64
  covered_entry_id      u64
  manifest_generation   u64
  header_crc32c         u32
  reserved_1            u32
}
```

Slot `stream_id` 的物理位置：

```text
slot_position = align4096(header_length) + stream_id * slot_size
```

乘法和加法必须检查溢出。

### 8.2 Tail Slot

V1 Slot 固定 128 字节：

```text
TailSlot {
  generation_begin       u64
  flags                  u32       // PRESENT = 1
  reserved_0             u32
  stream_id              u64
  next_sequence          u64
  next_byte_offset       u64
  last_recorded_at       i64
  last_entry_id          u64
  applied_entry_id       u64
  latest_segment_id      UUID
  latest_extent_pack_id   UUID
  latest_page_ordinal     u32
  reserved_1              [12]byte
  slot_crc32c            u32
  reserved_2             u32
  generation_end         u64
}
```

Writer 更新 Slot：先把 Begin 写为奇数，再写内容和 CRC，随后写偶数 End，最后把 Begin 写为相同偶数。Reader 只有在 Begin 等于 End、值为偶数且 CRC 正确时接受 Slot。该协议只减少并发 mmap 撕裂；崩溃一致性仍依赖 WAL 重放和 `applied_entry_id`。

`slot_crc32c` 覆盖从 `flags` 开始到 `reserved_1` 结束的字节，不覆盖两个 Generation、CRC 自身和 `reserved_2`。发布时先写偶数 `generation_end`，最后写相同的偶数 `generation_begin`；任一步崩溃都会留下奇数或不相等的 Generation。`latest_extent_pack_id + latest_page_ordinal` 是可直接定位的 Extent Page Pointer。

Tail Catalog 不得单独证明 Record 存在。Slot 损坏时只失去快速 Tail 定位能力。

## 9. Cold Extent Locator V1

### 9.1 文件布局

```text
locator/
  EXTENTS-ACTIVE.loc
  EXTENTS-<artifact-id>.loc
  LOCATOR-<artifact-id>.snapshot
```

Locator Pack 由不可变 Page 组成。V1 Page 固定 64 KiB，文件 Header 占 4096 字节：

```text
LocatorPackHeader {
  magic                 [8]byte = "STRMLOC1"
  format_version        u16 = 1
  header_length         u16 = 72
  flags                 u32 = 0
  artifact_id           UUID
  page_size             u32 = 65536
  reserved_0            u32
  page_count            u64
  created_at            i64
  covered_entry_id      u64
  header_crc32c         u32
  reserved_1            u32
}
```

Page Ordinal 从 `0` 开始：

```text
page_position = 4096 + page_ordinal * 65536
```

`EXTENTS-ACTIVE.loc` 只能在尾部追加完整 Page，不被 Manifest/Snapshot 引用。Seal 时固定 Page Count 和 Covered Entry ID、`fsync`、追加通用 Artifact Footer，再 rename 为带 Artifact ID 的正式文件。新的 Page 写入新的 Active Pack。

### 9.2 Extent Page Header

```text
ExtentPageHeader {
  magic                   [8]byte = "EXTPAGE1"
  format_version          u16 = 1
  header_length           u16 = 112
  flags                   u32
  page_id                 UUID
  stream_id               u64
  first_sequence          u64
  next_sequence           u64
  first_recorded_at       i64
  last_recorded_at        i64
  extent_count            u32
  skip_pointer_count      u16
  reserved_0              u16
  previous_pack_id        UUID
  previous_page_ordinal   u32
  body_length             u32
  reserved_1              u32
  header_crc32c           u32
}
```

Page Body 依次保存 `ExtentEntry[]`、`SkipPointer[]`、零 Padding，Page 最后 4 字节保存覆盖前 65532 字节的 Page CRC32C。

V1 Page Flag `HAS_PREVIOUS = 0x00000001`。未设置时 Previous 字段必须为零；设置时 Previous Pointer 可以指向当前或更早的 Sealed Pack，但不能指向尚未持久化的 Page。

### 9.3 Extent Entry

```text
ExtentEntry {
  segment_id              UUID
  first_sequence          u64
  next_sequence           u64
  first_byte_offset       u64
  next_byte_offset        u64
  first_recorded_at       i64
  last_recorded_at        i64
  record_index_offset     u64
  stream_data_offset      u64
  entry_crc32c            u32
  reserved                u32
}
```

Extent 按 Sequence、Byte Offset 和 Time 自然升序，范围不能重叠或出现空洞。

### 9.4 Skip Pointer

```text
SkipPointer {
  target_pack_id          UUID
  target_page_ordinal     u32
  distance_pages          u32
  target_first_sequence   u64
  target_first_time       i64
}
```

Skip Pointer 只用于加速，错误或缺失时可以沿 Previous Pointer 回退。任何 Pointer 指向的 Page 都必须属于相同 Stream，且范围严格更早。

### 9.5 Locator Snapshot

Locator Snapshot Header：

```text
LocatorSnapshotHeader {
  magic                    [8]byte = "STRMLOCS"
  format_version           u16 = 1
  header_length            u16 = 88
  flags                    u32 = 0
  artifact_id              UUID
  manifest_generation      u64
  covered_entry_id         u64
  tail_catalog_artifact_id UUID
  pack_count               u32
  root_count               u32
  created_at               i64
  header_crc32c            u32
  reserved                 u32
}
```

Header 后先保存按 Artifact ID 排序的 `LocatorPackReference[]`，再保存按 StreamID 排序的 Root Entry，最后追加通用 Artifact Footer。

```text
LocatorPackReference {
  entry_length       u32
  flags              u32
  pack_id            UUID
  file_size          u64
  page_count         u64
  content_sha256     SHA256
  path_length        u16
  reserved           u16
  path               bytes
  entry_crc32c       u32
}

LocatorRootEntry {             // fixed 40 bytes
  stream_id          u64
  pack_id            UUID
  page_ordinal       u32
  reserved_0         u32
  entry_crc32c       u32
  reserved_1         u32
}
```

Locator Snapshot 只引用 Sealed Pack。它是投影，格式损坏时可以扫描当前 Manifest 的 Segment Directory 重建，不能据此删除 Segment。

实现说明：当前 V1 Builder 为每个 Manifest Generation 生成一个完整 Sealed Pack；多页 Stream 生成
Previous Pointer 链，暂不生成 Skip Pointer。Runtime 按 Root 延迟读取 Page，并使用有界 LRU 缓存
已校验 Metadata；Snapshot/Pin/Scrub/Retirement 必须沿 Locator Snapshot 显式遍历 Pack，因为 Pack
不是 Manifest 的直接 Artifact Reference。

## 10. Stream Registry Snapshot V1

Stream Registry 的事实变更必须进入 WAL；Registry Snapshot 只是加速恢复的 Checkpoint。

### 10.1 Registry Bootstrap 与 WAL 事实

V1 固定 `StreamID=0` 为内部 Registry Stream，用户 Namespace 不能访问或创建它。每次首次分配用户 StreamID 时，先向该 Stream 追加一条普通 Record Frame；它与其他 Record 使用相同 WAL、复制、提交和 Segment 格式。

Registry Record 的 Payload：

```text
RegistryRecordV1 {
  magic               [8]byte = "STRMREG1"
  format_version      u16 = 1
  flags               u16 = 0
  record_length       u32
  assigned_stream_id  u64
  namespace_length    u16
  stream_name_length  u16
  reserved            u32
  namespace           bytes
  stream_name         bytes
  crc32c              u32
}
```

- Registry Stream 自身不需要 Registry Record，它由格式固定；
- `assigned_stream_id` 从 `1` 开始且永不复用；
- Registry Record 必须先于该 Stream 的第一条用户 Record 出现在 Entry ID 顺序中；
- 映射已提交但第一条用户 Record 尚未提交时，可以保留这个空 StreamID Reservation；
- Reservation 不允许回收或分配给另一个名称；
- API 是否展示尚无用户 Record 的 Reservation，由 API 协议固定，不影响存储正确性。

这样 Stream Registry 仍然只依赖一份 WAL，不需要与独立数据库或第二份 Registry WAL 执行事务。

### 10.2 Registry Snapshot

```text
registry/
  REGISTRY-<artifact-id>.reg
```

```text
RegistrySnapshotHeader {
  magic              [8]byte = "STRMREGR"
  format_version     u16 = 1
  header_length      u16 = 88
  flags              u32 = 0
  artifact_id        UUID
  covered_entry_id   u64
  entry_count        u64
  block_count        u32
  reserved_0         u32
  block_index_offset u64
  entries_offset     u64
  created_at         i64
  header_crc32c      u32
  reserved_1         u32
}
```

Header 后保存 Sparse Block Index，再保存按 `(namespace bytes, stream_name bytes)` 字典序排列的 Registry Entry：

```text
RegistryBlockIndexEntry {
  entry_length        u32
  entry_count         u32
  entries_offset      u64
  namespace_length    u16
  stream_name_length  u16
  reserved            u32
  first_namespace     bytes
  first_stream_name   bytes
  entry_crc32c        u32
}
```

Block Index 保存每个 Block 的第一组完整 Key，按相同字节序排序。`entries_offset` 是该 Block 第一条 Registry Entry 的绝对文件 Offset；Block 内 Entry 数不得超过格式配置的 `registry_block_entries`。

Registry Entry：

```text
RegistryEntry {
  entry_length       u32
  flags              u32
  stream_id          u64
  created_entry_id   u64
  namespace_length   u16
  stream_name_length u16
  reserved           u32
  namespace          bytes
  stream_name        bytes
  entry_crc32c       u32
}
```

同一个名称只能映射一个 StreamID，同一个 StreamID 也只能映射一个名称。发现双向映射冲突时必须失败关闭并从 WAL/更早 Snapshot 重建。

Registry Entry 之后追加通用 Artifact Footer。Registry Snapshot 只包含 `created_entry_id <= covered_entry_id` 的映射，且必须包含这个 Checkpoint 以前的全部映射。

## 11. Snapshot Manifest V1

Snapshot 不是把整个数据目录打包成不透明压缩文件，而是一个不可变 Artifact 集合及其一致性 Checkpoint。

```text
snapshots/
  SNAPSHOT-<snapshot-id>.manifest
```

### 11.1 Header

```text
SnapshotHeader {
  magic                       [8]byte = "STRMSNP1"
  format_version              u16 = 1
  header_length               u16 = 136
  flags                       u32
  snapshot_id                 UUID
  group_id                    UUID
  term                        u64
  checkpoint_entry_id         u64
  checkpoint_entry_crc32c     u32
  reserved_0                  u32
  manifest_generation         u64
  manifest_sha256             SHA256
  created_at                  i64
  artifact_count              u64
  header_crc32c               u32
  reserved_1                  u32
}
```

空存储 Snapshot 使用独立 `EMPTY` Flag，此时 Checkpoint Entry 字段写零且不参与日志前缀比较。

### 11.2 Artifact Entry

Snapshot 对以下文件逐一引用：

- 当前 Manifest；
- Manifest 引用的所有必需 Segment；
- Tail Catalog Checkpoint；
- Locator Snapshot 和 Extent Page 文件；
- Registry Snapshot；
- 必需的格式/状态元数据。

NODE Identity 永远不进入可安装 Snapshot，也不能从来源节点复制。目标节点保留自己的 NODE；Replication State 根据本地 Node ID、Snapshot Checkpoint 和当前 Coordinator Term 重新生成，不能照抄来源节点的 Role/Lease。

```text
SnapshotArtifact {
  entry_length             u32
  artifact_type            u16
  format_version           u16
  flags                    u32
  artifact_id              UUID
  file_size                u64
  local_name_length        u16
  object_location_length   u16
  reserved                 u32
  content_sha256           SHA256
  local_name               bytes
  object_location          bytes
  entry_crc32c             u32
}
```

Artifact 按 `(artifact_type, artifact_id)` 排序。Snapshot Manifest 使用通用 Artifact Footer，`artifact_type = SNAPSHOT_MANIFEST`、`artifact_id = snapshot_id`；Footer 的 SHA-256 覆盖 Snapshot Header 和全部 Artifact Entry。

### 11.3 可安装条件

Snapshot 只有同时满足以下条件才可以用于 WAL GC 或 Standby 恢复：

1. Snapshot Manifest Header、Entry、Footer 全部校验通过；
2. Manifest Generation 与 Manifest SHA-256 精确匹配；
3. 每个必需 Artifact 都存在、大小相同且 SHA-256 一致；
4. Segment 集合覆盖 Checkpoint 以前全部已提交 Record；
5. Tail、Locator 和 Registry 的 Covered Entry ID 不晚于 Checkpoint，缺少差额可以由 Snapshot 同包 WAL 重放，或在发布前补齐；
6. Artifact 被 Pin，安装 Lease 内不能被 GC；
7. 安装到 Staging 后完成文件和目录 `fsync`，再原子切换 CURRENT。

V1 要求 Snapshot 中的投影全部覆盖到 Checkpoint，不引入 Snapshot 内部 WAL。未来若允许落后投影，必须使用新 Snapshot Format 并明确包含重放 WAL 范围。

### 11.4 Snapshot 安装事务

Snapshot 安装跨越 Manifest `CURRENT`、`WAL-CURRENT` 和 `REPLICATION-CURRENT` 三个指针，
因此使用一个可恢复的安装意图文件串联，而不假设三个 rename 能组成文件系统事务：

```text
SNAPSHOT-INSTALL.json
staging/snapshot-install-<snapshot_id>/
```

`SNAPSHOT-INSTALL.json` 是 UTF-8 JSON，V1 只允许以下字段且未知字段必须拒绝：

```text
{
  "version": 1,
  "snapshot_id": "<32 lowercase hex>",
  "group_id": "<32 lowercase hex>",
  "term": <u64>,
  "leader_id": "<32 lowercase hex>",
  "checkpoint_entry_id": <u64>,
  "checkpoint_entry_crc32c": <u32>,
  "stage_dir": "snapshot-install-<snapshot_id>"
}
```

安装顺序固定为：完整校验并 fsync Staging；原子发布安装意图；把不可变 Artifact 发布到最终目录；
发布从 `checkpoint + 1` 开始且以前一 CRC 为基线的新 WAL；发布 Manifest `CURRENT`；发布本地
Replication State；最后删除安装意图和 Staging。任一步崩溃后，节点必须在打开存储引擎和提供服务前
读取安装意图并幂等完成剩余步骤。存在安装意图时不得按中间的 `CURRENT`/`WAL-CURRENT` 组合提供服务。

## 12. Node 与 Replication State

节点身份和复制水位不进入 Segment，也不能只存在内存：

```text
NODE
meta/
  REPLICATION-STATE-<generation>
REPLICATION-CURRENT
```

### 12.1 NODE

NODE 在数据目录初始化时创建，之后不可变：

```text
NodeIdentity {
  magic             [8]byte = "STRMNODE"
  format_version    u16 = 1
  length            u16 = 80
  flags             u32
  cluster_id        UUID
  group_id          UUID
  node_id           UUID
  created_at        i64
  reserved          u32
  crc32c            u32
}
```

把另一个节点的数据盘挂载到本节点时，Node ID 不匹配必须阻止自动启动，不能静默改写 NODE。

### 12.2 Replication State Checkpoint

State Checkpoint 保存主备协议定义的：

- Term、Role、Leader ID、Lease Deadline；
- Last Appended、Local Durable、Replicated、Commit、Applied 水位，以及最后连续 Entry CRC；
- Earliest WAL、Installed Snapshot 和 Snapshot Entry；
- 每个可选水位的 `has_value` Flag；
- Generation、前一 State SHA-256、自身 CRC/SHA-256。

它使用与 Manifest 相同的“新文件 + CURRENT 指针”发布方式，禁止原地覆盖。V1 不为每个 Group Commit 增加独立 Metadata WAL 同步，而是周期生成 State Checkpoint，并按 Append Commit Protocol 的 durable suffix 规则恢复；State Checkpoint 是下界，不能要求修改已发布 Segment。

V1 文件名为：

```text
meta/REPLICATION-STATE-<generation:020d>-<state_id_hex>.bin
REPLICATION-CURRENT
```

State 文件由固定 320 字节 Header 和 88 字节通用 Artifact Footer 组成：

```text
ReplicationStateHeaderV1 {
  magic                         [8]byte = "STRMRST1"
  format_version                u16 = 1
  header_length                 u16 = 320
  flags                         u32

  state_id                      UUID
  generation                    u64
  previous_generation           u64
  previous_state_sha256         SHA256

  group_id                      UUID
  node_id                       UUID
  term                          u64
  role                          u16
  durability                    u16
  reserved_0                    u32 = 0

  leader_id                     UUID
  lease_expires_at              i64

  last_appended_entry_id        u64
  last_appended_entry_crc32c    u32
  reserved_1                    u32 = 0

  local_durable_entry_id        u64
  local_durable_entry_crc32c    u32
  reserved_2                    u32 = 0

  replicated_entry_id           u64
  replicated_entry_crc32c       u32
  reserved_3                    u32 = 0

  commit_entry_id               u64
  commit_entry_crc32c           u32
  reserved_4                    u32 = 0

  applied_entry_id              u64
  applied_entry_crc32c          u32
  reserved_5                    u32 = 0

  earliest_wal_entry_id         u64
  installed_snapshot_id         UUID
  snapshot_entry_id             u64
  snapshot_entry_crc32c         u32
  reserved_6                    u32 = 0

  created_at                    i64
  reserved_7                    [36]byte = 0
  header_crc32c                 u32
}
```

`header_crc32c` 使用 CRC32C Castagnoli，覆盖 Header 的前 316 字节。通用 Artifact Footer 使用 `artifact_type = REPLICATION_STATE`、`artifact_id = state_id`；Footer 的 SHA-256 覆盖完整 320 字节 Header。因此 Header 局部损坏由 CRC 检出，文件替换、截断和 Generation 链错误由 SHA-256 与 `REPLICATION-CURRENT` 检出。

Flags 固定为：

```text
bit 0  HAS_LAST_APPENDED
bit 1  HAS_LOCAL_DURABLE
bit 2  HAS_REPLICATED
bit 3  HAS_COMMITTED
bit 4  HAS_APPLIED
bit 5  HAS_LEADER
bit 6  HAS_LEASE
bit 7  HAS_INSTALLED_SNAPSHOT
```

Flag 未设置时对应 ID、CRC、时间或 UUID 字段必须全部为零。V1 Reader 遇到未知 Flag 必须拒绝；不能把未知状态解释成安全水位。

Role 编码固定为：

```text
1  SINGLE
2  PRIMARY
3  STANDBY
4  RECOVERING
```

Durability 编码固定为：

```text
1  SINGLE_SYNC
2  REPLICATED_STRICT
3  DEGRADED_LOCAL_ONLY
```

State 必须满足：

```text
applied <= committed <= local_durable <= last_appended
replicated <= last_appended
```

`REPLICATED_STRICT` Primary 还必须满足 `committed <= replicated`。`STANDBY` 不设置 `HAS_REPLICATED` 和 `HAS_LEASE`；`PRIMARY` 的 `leader_id` 必须等于本地 `node_id` 并拥有 Lease；`SINGLE` 不设置 Leader、Lease 或 Replicated，但可以记录用于本地恢复和 WAL GC 的已验证 Snapshot。`RECOVERING` 不拥有 Lease。

同一 Entry ID 出现在多个水位时 CRC32C 必须相同。Installed Snapshot Checkpoint 必须不晚于 Commit 水位；若 Snapshot 与 Commit 指向同一 Entry，CRC 必须相同。只要 `earliest_wal_entry_id > 0`，就必须存在覆盖到至少 `earliest_wal_entry_id - 1` 的已安装、已验证 Snapshot。空日志的所有可选水位均无值，`earliest_wal_entry_id = 0`。

Generation 规则与 Manifest 相同：Generation 0 的 `previous_generation` 和 `previous_state_sha256` 全零；后续 Generation 必须恰好引用前一代的 Generation 和 Footer Content SHA-256。

`REPLICATION-CURRENT` 使用以下可变长格式：

```text
ReplicationCurrentV1 {
  magic                   [8]byte = "STRMRSC1"
  format_version          u16 = 1
  total_length            u16
  flags                   u32 = 0
  generation              u64
  state_id                UUID
  state_file_name_length  u16
  reserved                u16 = 0
  state_sha256            SHA256
  state_file_name         bytes
  crc32c                  u32
}
```

最小长度为 80 字节，`crc32c` 覆盖它之前的全部内容。文件名必须是单个规范文件名，不能包含路径分隔符。启动只接受 Pointer、State Header、Artifact Footer 的 Generation、State ID 和 SHA-256 全部一致的状态；`meta/` 中更高但未被 Pointer 引用的文件只作为 Orphan 处理，不能自动提升为当前状态。

## 13. 数据目录

建议 V1 目录结构：

```text
data/
  NODE
  CURRENT
  WAL-CURRENT
  REPLICATION-CURRENT

  wal/
  segments/
  manifests/
  catalog/
  locator/
  registry/
  snapshots/
  meta/
  staging/
  trash/
```

- `staging/` 中的文件永远不参与正常读取；
- `trash/` 只保存已经从 Manifest/Snapshot 引用图中解除、等待延迟删除的文件；
- 启动不能仅通过扫描 `segments/` 猜测有效集合；
- 未被当前或历史 Pin Manifest 引用的完整文件是 Orphan，由恢复/GC 工具处理；
- 文件删除必须发生在 Manifest 发布、Reader 引用释放和 Snapshot Pin 释放之后。

## 14. 格式版本与升级

### 14.1 版本单位

Record Frame、WAL、Segment、Manifest、Locator 和 Snapshot 分别版本化。它们不要求同时升级，但组合必须在 Capability Matrix 中被当前 Binary 明确支持。

### 14.2 Reader/Writer 规则

- Reader 可以同时支持多个已知旧版本；
- Writer 只生成配置指定的一个版本；
- 升级不能原地修改已发布不可变文件；
- 新 Segment Version 通过 Merge/Rewrite 逐步生成，并由新 Manifest 引用；
- 新 Record Frame Version 一旦开始写入，同一 Stream 可以包含多版本 Frame，Reader 必须按每条 Frame 的 Version 解码；
- 降级前必须确认旧 Binary 能读取当前 Manifest 引用的所有格式；
- 未通过检查时拒绝降级启动，不能自动重写历史。

### 14.3 Header 扩展

固定 Header 依靠 `header_length` 向尾部扩展。旧 Reader：

- 可以跳过已知结构中声明为可选的尾部；
- 必须拒绝未知必需 Flag；
- 必须验证跳过范围仍在父结构 Length 内；
- 不能因为 Header 更长就假定它兼容。

### 14.4 格式演进触发条件

以下变化必须提升对应格式 Version：

- 改变已有字段含义或校验覆盖范围；
- 改变 Record Frame 的逻辑长度或 Byte Offset 计算；
- 引入压缩/加密导致 Data Section 不再是原始 Frame 连接；
- 改变 Dense Index Entry 布局；
- 改变 ID 宽度、字节序或 Timestamp 单位。

## 15. 损坏处理原则

### 15.1 可以自动修复

- Active WAL 最后的半条 Entry：截断到最后合法边界；
- Tail Slot CRC 错误：从 WAL/Segment 重建该 Slot；
- Locator Snapshot/Page 缺失：从当前 Manifest 的 Segment Directory 重建；
- Registry Snapshot 缺失：从 Registry WAL 事实重建；
- Staging 中未发布文件：确认不被任何 CURRENT 引用后清理。

### 15.2 必须失败关闭

- Sealed WAL 中间损坏或 SHA-256 不一致；
- 当前 Manifest 或 CURRENT 校验失败且无法唯一选择前一完整 Generation；
- 已发布 Segment 的 Header/Footer/Frame/SHA-256 损坏；
- 同 Entry ID 内容不同，或同 Stream Sequence/Byte Offset 出现分叉；
- Segment Directory/Index 与 Frame 元数据不一致；
- Snapshot 声称可安装但缺少唯一数据 Artifact；
- 未知必需 Flag、Codec 或不支持的 Format Version。

失败关闭意味着停止相关 Shard 的 Append 和不可信读取，报警并从另一副本、Snapshot 或对象存储恢复。不得跳过坏 Record 后继续返回后续数据。

## 16. Golden Files 与格式测试

每种格式必须提交跨版本 Golden File，至少验证：

1. 全零/最小合法对象；
2. 最大合法 Length 边界附近对象；
3. 多 Header、空 Payload、二进制 Payload 和 UTF-8 名称；
4. 单条 Append 与多 Record Batch；
5. WAL 轮转和跨文件 Entry CRC 链；
6. 多 Stream Segment 与 Unified Index 定位；
7. Manifest 多 Generation 和 CURRENT 切换；
8. Snapshot Artifact 完整性；
9. 每个 Length、Offset、Count 的截断、越界和整数溢出；
10. 每个 CRC/SHA 字段的单 bit 损坏；
11. 未知可选 Flag 与未知必需 Flag；
12. Little Endian 固定字节结果，不依赖 CPU 架构。

Fuzz Target 至少包括：

- Record Frame Decoder；
- WAL Scanner；
- Segment Header/Directory/Index Reader；
- Manifest/Snapshot Parser；
- Locator Page Reader。

Parser 必须在任意输入下满足：不 panic、不越界、不按攻击者声明进行无界分配、不返回未经 CRC/边界验证的数据。

## 17. V1 已固定与验证门槛

### 17.1 本文建议固定

- Little Endian；
- Record Frame 字节在 WAL、复制和 Segment 中保持不变；
- CRC32C 保护细粒度结构，SHA-256 保护不可变 Artifact；
- Entry ID、Sequence、Byte Offset、Term 使用 `u64`；
- Recorded At 使用 Unix Nanoseconds `i64`；
- Segment V1 使用不压缩原始 Frame；
- Dense Index Entry 固定 24 字节；
- Segment Section 4 KiB 对齐，Extent Page 固定 64 KiB；
- Manifest/Snapshot 使用不可变 Generation + 原子 CURRENT；
- AppendBatch 信息进入每条永久 Frame，Batch 不跨 WAL 文件。

### 17.2 已收敛

- Frame 格式硬上限保持 256 MiB，API/部署默认限制必须更小；
- Request Hash 固定 SHA-256；
- V1 使用 `previous_entry_crc32c`，不引入密码学 WAL Hash Chain；
- Tail 使用可变 Active mmap 文件，Snapshot 只引用已 Seal Checkpoint；
- Segment Directory 保存 Entry ID Min/Max；
- Commit State 周期 Checkpoint，不为每个 Group 增加 Metadata WAL 同步；
- V1 Snapshot 的全部投影覆盖 Checkpoint，不携带隐式短 WAL。

Extent Page V1 采用 64 KiB 作为实现初值；在格式冻结前必须按 [基准与可靠性验证计划](benchmark-plan.md) 对照 16/32/128 KiB。若结果要求改变，只修改尚未发布的 V1 常量；一旦产生生产文件只能通过新 Format Version 演进。
