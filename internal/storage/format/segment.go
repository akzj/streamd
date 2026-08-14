package format

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"
)

var (
	segmentMagic       = [8]byte{'S', 'T', 'R', 'M', 'S', 'E', 'G', '1'}
	segmentFooterMagic = [8]byte{'S', 'E', 'G', 'E', 'N', 'D', 'V', '1'}
)

const recordCodecNone uint16 = 0

// SegmentHeader describes the canonical five-section V1 Segment layout.
type SegmentHeader struct {
	SegmentID       UUID
	CreatedAt       int64
	FirstEntryID    uint64
	LastEntryID     uint64
	StreamCount     uint64
	RecordCount     uint64
	DirectoryOffset uint64
	DirectoryLength uint64
	IndexOffset     uint64
	IndexLength     uint64
	DataOffset      uint64
	DataLength      uint64
	FooterOffset    uint64
}

// NewSegmentHeader calculates canonical section offsets for a non-empty Segment.
func NewSegmentHeader(segmentID UUID, createdAt int64, firstEntryID, lastEntryID, streamCount, recordCount, dataLength uint64) (SegmentHeader, error) {
	directoryLength, err := checkedMul(streamCount, StreamDirectoryEntryLength, "directory length")
	if err != nil {
		return SegmentHeader{}, err
	}
	indexLength, err := checkedMul(recordCount, DenseIndexEntryLength, "index length")
	if err != nil {
		return SegmentHeader{}, err
	}
	directoryEnd, err := checkedAdd(SegmentSectionAlignment, directoryLength)
	if err != nil {
		return SegmentHeader{}, err
	}
	indexOffset, err := alignUp(directoryEnd, SegmentSectionAlignment)
	if err != nil {
		return SegmentHeader{}, err
	}
	indexEnd, err := checkedAdd(indexOffset, indexLength)
	if err != nil {
		return SegmentHeader{}, err
	}
	dataOffset, err := alignUp(indexEnd, SegmentSectionAlignment)
	if err != nil {
		return SegmentHeader{}, err
	}
	dataEnd, err := checkedAdd(dataOffset, dataLength)
	if err != nil {
		return SegmentHeader{}, err
	}
	footerOffset, err := alignUp(dataEnd, SegmentSectionAlignment)
	if err != nil {
		return SegmentHeader{}, err
	}
	header := SegmentHeader{
		SegmentID:       segmentID,
		CreatedAt:       createdAt,
		FirstEntryID:    firstEntryID,
		LastEntryID:     lastEntryID,
		StreamCount:     streamCount,
		RecordCount:     recordCount,
		DirectoryOffset: SegmentSectionAlignment,
		DirectoryLength: directoryLength,
		IndexOffset:     indexOffset,
		IndexLength:     indexLength,
		DataOffset:      dataOffset,
		DataLength:      dataLength,
		FooterOffset:    footerOffset,
	}
	if err := validateSegmentHeader(header); err != nil {
		return SegmentHeader{}, err
	}
	return header, nil
}

// MarshalSegmentHeader returns the exact 160-byte V1 header. The caller pads
// it to SegmentSectionAlignment when constructing a Segment file.
func MarshalSegmentHeader(header SegmentHeader) ([]byte, error) {
	if err := validateSegmentHeader(header); err != nil {
		return nil, err
	}
	encoded := make([]byte, SegmentHeaderLength)
	copy(encoded[0:8], segmentMagic[:])
	putU16(encoded[8:10], VersionV1)
	putU16(encoded[10:12], SegmentHeaderLength)
	putU32(encoded[12:16], 0)
	copy(encoded[16:32], header.SegmentID[:])
	putI64(encoded[32:40], header.CreatedAt)
	putU64(encoded[40:48], header.FirstEntryID)
	putU64(encoded[48:56], header.LastEntryID)
	putU64(encoded[56:64], header.StreamCount)
	putU64(encoded[64:72], header.RecordCount)
	putU64(encoded[72:80], header.DirectoryOffset)
	putU64(encoded[80:88], header.DirectoryLength)
	putU64(encoded[88:96], header.IndexOffset)
	putU64(encoded[96:104], header.IndexLength)
	putU64(encoded[104:112], header.DataOffset)
	putU64(encoded[112:120], header.DataLength)
	putU64(encoded[120:128], header.FooterOffset)
	putU32(encoded[128:132], DenseIndexEntryLength)
	putU16(encoded[132:134], recordCodecNone)
	putU16(encoded[134:136], 0)
	putU32(encoded[156:160], checksum(encoded[:156]))
	return encoded, nil
}

// UnmarshalSegmentHeader validates one exact V1 header.
func UnmarshalSegmentHeader(encoded []byte) (SegmentHeader, error) {
	var header SegmentHeader
	if len(encoded) < SegmentHeaderLength {
		return header, truncatedf("Segment header needs %d bytes, got %d", SegmentHeaderLength, len(encoded))
	}
	if len(encoded) != SegmentHeaderLength {
		return header, invalidf("Segment header has trailing bytes: %d", len(encoded)-SegmentHeaderLength)
	}
	if !bytes.Equal(encoded[0:8], segmentMagic[:]) {
		return header, invalidf("Segment magic is %q", encoded[0:8])
	}
	if version := getU16(encoded[8:10]); version != VersionV1 {
		return header, unsupportedVersion("Segment", version)
	}
	if length := getU16(encoded[10:12]); length != SegmentHeaderLength {
		return header, invalidf("Segment header_length is %d", length)
	}
	if flags := getU32(encoded[12:16]); flags != 0 {
		return header, invalidf("Segment flags contain unsupported bits: 0x%08x", flags)
	}
	if indexEntrySize := getU32(encoded[128:132]); indexEntrySize != DenseIndexEntryLength {
		return header, invalidf("Segment index_entry_size is %d", indexEntrySize)
	}
	if codec := getU16(encoded[132:134]); codec != recordCodecNone {
		return header, invalidf("Segment record_codec is %d", codec)
	}
	if err := expectZero(encoded[134:156], "Segment reserved fields"); err != nil {
		return header, err
	}
	storedCRC := getU32(encoded[156:160])
	actualCRC := checksum(encoded[:156])
	if storedCRC != actualCRC {
		return header, checksumf("Segment header CRC32C is %08x, want %08x", storedCRC, actualCRC)
	}
	copy(header.SegmentID[:], encoded[16:32])
	header.CreatedAt = getI64(encoded[32:40])
	header.FirstEntryID = getU64(encoded[40:48])
	header.LastEntryID = getU64(encoded[48:56])
	header.StreamCount = getU64(encoded[56:64])
	header.RecordCount = getU64(encoded[64:72])
	header.DirectoryOffset = getU64(encoded[72:80])
	header.DirectoryLength = getU64(encoded[80:88])
	header.IndexOffset = getU64(encoded[88:96])
	header.IndexLength = getU64(encoded[96:104])
	header.DataOffset = getU64(encoded[104:112])
	header.DataLength = getU64(encoded[112:120])
	header.FooterOffset = getU64(encoded[120:128])
	if err := validateSegmentHeader(header); err != nil {
		return SegmentHeader{}, err
	}
	return header, nil
}

// UnmarshalSegmentHeaderSection validates the complete fixed 4 KiB Header Section.
func UnmarshalSegmentHeaderSection(section []byte) (SegmentHeader, error) {
	if len(section) != SegmentSectionAlignment {
		return SegmentHeader{}, invalidf("Segment header section is %d bytes, want %d", len(section), SegmentSectionAlignment)
	}
	header, err := UnmarshalSegmentHeader(section[:SegmentHeaderLength])
	if err != nil {
		return SegmentHeader{}, err
	}
	if err := expectZero(section[SegmentHeaderLength:], "Segment header section padding"); err != nil {
		return SegmentHeader{}, err
	}
	return header, nil
}

func validateSegmentHeader(header SegmentHeader) error {
	if isZeroUUID(header.SegmentID) {
		return invalidf("Segment ID is zero")
	}
	if header.StreamCount == 0 || header.RecordCount == 0 {
		return invalidf("empty Segment is not allowed")
	}
	if header.DataLength == 0 {
		return invalidf("non-empty Segment has zero data_length")
	}
	minimumDataLength, err := checkedMul(header.RecordCount, RecordFixedHeaderLength+RecordCRCSize, "minimum Segment data length")
	if err != nil || header.DataLength < minimumDataLength {
		return invalidf("Segment data_length is %d, minimum is %d", header.DataLength, minimumDataLength)
	}
	if header.FirstEntryID > header.LastEntryID {
		return invalidf("Segment first_entry_id %d is after last_entry_id %d", header.FirstEntryID, header.LastEntryID)
	}
	expectedDirectoryLength, err := checkedMul(header.StreamCount, StreamDirectoryEntryLength, "directory length")
	if err != nil || header.DirectoryLength != expectedDirectoryLength {
		return invalidf("Segment directory_length is %d, want %d", header.DirectoryLength, expectedDirectoryLength)
	}
	expectedIndexLength, err := checkedMul(header.RecordCount, DenseIndexEntryLength, "index length")
	if err != nil || header.IndexLength != expectedIndexLength {
		return invalidf("Segment index_length is %d, want %d", header.IndexLength, expectedIndexLength)
	}
	if header.DirectoryOffset != SegmentSectionAlignment {
		return invalidf("Segment directory_offset is %d, want %d", header.DirectoryOffset, SegmentSectionAlignment)
	}
	directoryEnd, err := checkedAdd(header.DirectoryOffset, header.DirectoryLength)
	if err != nil {
		return invalidf("Segment Directory range overflows")
	}
	expectedIndexOffset, err := alignUp(directoryEnd, SegmentSectionAlignment)
	if err != nil || header.IndexOffset != expectedIndexOffset {
		return invalidf("Segment index_offset is %d, want %d", header.IndexOffset, expectedIndexOffset)
	}
	indexEnd, err := checkedAdd(header.IndexOffset, header.IndexLength)
	if err != nil {
		return invalidf("Segment Index range overflows")
	}
	expectedDataOffset, err := alignUp(indexEnd, SegmentSectionAlignment)
	if err != nil || header.DataOffset != expectedDataOffset {
		return invalidf("Segment data_offset is %d, want %d", header.DataOffset, expectedDataOffset)
	}
	dataEnd, err := checkedAdd(header.DataOffset, header.DataLength)
	if err != nil {
		return invalidf("Segment Data range overflows")
	}
	expectedFooterOffset, err := alignUp(dataEnd, SegmentSectionAlignment)
	if err != nil || header.FooterOffset != expectedFooterOffset {
		return invalidf("Segment footer_offset is %d, want %d", header.FooterOffset, expectedFooterOffset)
	}
	return nil
}

// StreamDirectoryEntry describes one Stream's single V1 Extent in a Segment.
type StreamDirectoryEntry struct {
	StreamID          uint64
	FirstSequence     uint64
	RecordCount       uint64
	FirstByteOffset   uint64
	NextByteOffset    uint64
	FirstRecordedAt   int64
	LastRecordedAt    int64
	FirstEntryID      uint64
	LastEntryID       uint64
	RecordIndexOffset uint64
	RecordIndexLength uint64
	StreamDataOffset  uint64
	StreamDataLength  uint64
}

func MarshalStreamDirectoryEntry(entry StreamDirectoryEntry) ([]byte, error) {
	if err := validateStreamDirectoryEntry(entry); err != nil {
		return nil, err
	}
	encoded := make([]byte, StreamDirectoryEntryLength)
	putU64(encoded[0:8], entry.StreamID)
	putU64(encoded[8:16], entry.FirstSequence)
	putU64(encoded[16:24], entry.RecordCount)
	putU64(encoded[24:32], entry.FirstByteOffset)
	putU64(encoded[32:40], entry.NextByteOffset)
	putI64(encoded[40:48], entry.FirstRecordedAt)
	putI64(encoded[48:56], entry.LastRecordedAt)
	putU64(encoded[56:64], entry.FirstEntryID)
	putU64(encoded[64:72], entry.LastEntryID)
	putU64(encoded[72:80], entry.RecordIndexOffset)
	putU64(encoded[80:88], entry.RecordIndexLength)
	putU64(encoded[88:96], entry.StreamDataOffset)
	putU64(encoded[96:104], entry.StreamDataLength)
	putU32(encoded[104:108], checksum(encoded[:104]))
	return encoded, nil
}

func UnmarshalStreamDirectoryEntry(encoded []byte) (StreamDirectoryEntry, error) {
	var entry StreamDirectoryEntry
	if len(encoded) < StreamDirectoryEntryLength {
		return entry, truncatedf("Stream Directory Entry needs %d bytes, got %d", StreamDirectoryEntryLength, len(encoded))
	}
	if len(encoded) != StreamDirectoryEntryLength {
		return entry, invalidf("Stream Directory Entry has trailing bytes: %d", len(encoded)-StreamDirectoryEntryLength)
	}
	if err := expectZero(encoded[108:112], "Stream Directory reserved"); err != nil {
		return entry, err
	}
	storedCRC := getU32(encoded[104:108])
	actualCRC := checksum(encoded[:104])
	if storedCRC != actualCRC {
		return entry, checksumf("Stream Directory CRC32C is %08x, want %08x", storedCRC, actualCRC)
	}
	entry.StreamID = getU64(encoded[0:8])
	entry.FirstSequence = getU64(encoded[8:16])
	entry.RecordCount = getU64(encoded[16:24])
	entry.FirstByteOffset = getU64(encoded[24:32])
	entry.NextByteOffset = getU64(encoded[32:40])
	entry.FirstRecordedAt = getI64(encoded[40:48])
	entry.LastRecordedAt = getI64(encoded[48:56])
	entry.FirstEntryID = getU64(encoded[56:64])
	entry.LastEntryID = getU64(encoded[64:72])
	entry.RecordIndexOffset = getU64(encoded[72:80])
	entry.RecordIndexLength = getU64(encoded[80:88])
	entry.StreamDataOffset = getU64(encoded[88:96])
	entry.StreamDataLength = getU64(encoded[96:104])
	if err := validateStreamDirectoryEntry(entry); err != nil {
		return StreamDirectoryEntry{}, err
	}
	return entry, nil
}

func validateStreamDirectoryEntry(entry StreamDirectoryEntry) error {
	if entry.RecordCount == 0 {
		return invalidf("Stream Directory record_count is zero")
	}
	if entry.StreamDataLength == 0 {
		return invalidf("Stream Directory stream_data_length is zero")
	}
	minimumDataLength, err := checkedMul(entry.RecordCount, RecordFixedHeaderLength+RecordCRCSize, "minimum Stream data length")
	if err != nil || entry.StreamDataLength < minimumDataLength {
		return invalidf("Stream Directory stream_data_length is %d, minimum is %d", entry.StreamDataLength, minimumDataLength)
	}
	if entry.FirstEntryID > entry.LastEntryID {
		return invalidf("Stream Directory first_entry_id is after last_entry_id")
	}
	if entry.FirstRecordedAt > entry.LastRecordedAt {
		return invalidf("Stream Directory first_recorded_at is after last_recorded_at")
	}
	expectedIndexLength, err := checkedMul(entry.RecordCount, DenseIndexEntryLength, "Stream Directory index length")
	if err != nil || entry.RecordIndexLength != expectedIndexLength {
		return invalidf("Stream Directory record_index_length is %d, want %d", entry.RecordIndexLength, expectedIndexLength)
	}
	nextSequence, err := checkedAdd(entry.FirstSequence, entry.RecordCount)
	if err != nil {
		return invalidf("Stream Directory Sequence range overflows")
	}
	_ = nextSequence
	expectedNextByteOffset, err := checkedAdd(entry.FirstByteOffset, entry.StreamDataLength)
	if err != nil || entry.NextByteOffset != expectedNextByteOffset {
		return invalidf("Stream Directory next_byte_offset is %d, want %d", entry.NextByteOffset, expectedNextByteOffset)
	}
	return nil
}

// DenseIndexEntry is the fixed-size V1 per-Record index entry.
type DenseIndexEntry struct {
	RelativeByteOffset uint64
	RecordedAtDelta    uint64
	FrameLength        uint32
	FrameCRC32C        uint32
}

func MarshalDenseIndexEntry(entry DenseIndexEntry) ([]byte, error) {
	if err := validateDenseIndexEntry(entry); err != nil {
		return nil, err
	}
	encoded := make([]byte, DenseIndexEntryLength)
	putU64(encoded[0:8], entry.RelativeByteOffset)
	putU64(encoded[8:16], entry.RecordedAtDelta)
	putU32(encoded[16:20], entry.FrameLength)
	putU32(encoded[20:24], entry.FrameCRC32C)
	return encoded, nil
}

func UnmarshalDenseIndexEntry(encoded []byte) (DenseIndexEntry, error) {
	var entry DenseIndexEntry
	if len(encoded) < DenseIndexEntryLength {
		return entry, truncatedf("Dense Index Entry needs %d bytes, got %d", DenseIndexEntryLength, len(encoded))
	}
	if len(encoded) != DenseIndexEntryLength {
		return entry, invalidf("Dense Index Entry has trailing bytes: %d", len(encoded)-DenseIndexEntryLength)
	}
	entry.RelativeByteOffset = getU64(encoded[0:8])
	entry.RecordedAtDelta = getU64(encoded[8:16])
	entry.FrameLength = getU32(encoded[16:20])
	entry.FrameCRC32C = getU32(encoded[20:24])
	if err := validateDenseIndexEntry(entry); err != nil {
		return DenseIndexEntry{}, err
	}
	return entry, nil
}

func validateDenseIndexEntry(entry DenseIndexEntry) error {
	if entry.FrameLength < RecordFixedHeaderLength+RecordCRCSize || entry.FrameLength > MaxFrameLength {
		return invalidf("Dense Index frame_length out of range: %d", entry.FrameLength)
	}
	return nil
}

// ValidateDenseIndex checks one Stream's complete Dense Index against its Directory Entry.
func ValidateDenseIndex(directory StreamDirectoryEntry, entries []DenseIndexEntry) error {
	if err := validateStreamDirectoryEntry(directory); err != nil {
		return err
	}
	if uint64(len(entries)) != directory.RecordCount {
		return invalidf("Dense Index has %d entries, want %d", len(entries), directory.RecordCount)
	}
	var expectedOffset uint64
	var previousTime uint64
	for i, entry := range entries {
		if err := validateDenseIndexEntry(entry); err != nil {
			return fmt.Errorf("Dense Index entry %d: %w", i, err)
		}
		if entry.RelativeByteOffset != expectedOffset {
			return invalidf("Dense Index entry %d relative offset is %d, want %d", i, entry.RelativeByteOffset, expectedOffset)
		}
		if i > 0 && entry.RecordedAtDelta < previousTime {
			return invalidf("Dense Index recorded_at_delta decreases at entry %d", i)
		}
		if entry.RecordedAtDelta > math.MaxInt64 {
			return invalidf("Dense Index recorded_at_delta overflows timestamp at entry %d", i)
		}
		delta := int64(entry.RecordedAtDelta)
		if directory.FirstRecordedAt > math.MaxInt64-delta {
			return invalidf("Dense Index timestamp overflows at entry %d", i)
		}
		var err error
		expectedOffset, err = checkedAdd(expectedOffset, uint64(entry.FrameLength))
		if err != nil {
			return invalidf("Dense Index byte offsets overflow at entry %d", i)
		}
		previousTime = entry.RecordedAtDelta
	}
	if len(entries) > 0 && entries[0].RecordedAtDelta != 0 {
		return invalidf("Dense Index first recorded_at_delta is not zero")
	}
	if expectedOffset != directory.StreamDataLength {
		return invalidf("Dense Index covers %d data bytes, want %d", expectedOffset, directory.StreamDataLength)
	}
	lastRecordedAt := directory.FirstRecordedAt + int64(previousTime)
	if lastRecordedAt != directory.LastRecordedAt {
		return invalidf("Dense Index last timestamp is %d, want %d", lastRecordedAt, directory.LastRecordedAt)
	}
	return nil
}

// ValidateSegmentLayout checks Directory ordering and canonical Index/Data placement.
func ValidateSegmentLayout(header SegmentHeader, directories []StreamDirectoryEntry) error {
	if err := validateSegmentHeader(header); err != nil {
		return err
	}
	if uint64(len(directories)) != header.StreamCount {
		return invalidf("Segment has %d Directory entries, want %d", len(directories), header.StreamCount)
	}
	expectedIndexOffset := header.IndexOffset
	expectedDataOffset := header.DataOffset
	var totalRecords uint64
	var previousStreamID uint64
	var firstEntryID uint64
	var lastEntryID uint64
	for i, directory := range directories {
		if err := validateStreamDirectoryEntry(directory); err != nil {
			return fmt.Errorf("Directory entry %d: %w", i, err)
		}
		if i > 0 && directory.StreamID <= previousStreamID {
			return invalidf("Directory StreamIDs are not strictly sorted at entry %d", i)
		}
		if directory.FirstEntryID < header.FirstEntryID || directory.LastEntryID > header.LastEntryID {
			return invalidf("Directory entry %d Entry ID range is outside Segment", i)
		}
		if i == 0 || directory.FirstEntryID < firstEntryID {
			firstEntryID = directory.FirstEntryID
		}
		if i == 0 || directory.LastEntryID > lastEntryID {
			lastEntryID = directory.LastEntryID
		}
		if directory.RecordIndexOffset != expectedIndexOffset {
			return invalidf("Directory entry %d index offset is %d, want %d", i, directory.RecordIndexOffset, expectedIndexOffset)
		}
		if directory.StreamDataOffset != expectedDataOffset {
			return invalidf("Directory entry %d data offset is %d, want %d", i, directory.StreamDataOffset, expectedDataOffset)
		}
		var err error
		expectedIndexOffset, err = checkedAdd(expectedIndexOffset, directory.RecordIndexLength)
		if err != nil {
			return err
		}
		expectedDataOffset, err = checkedAdd(expectedDataOffset, directory.StreamDataLength)
		if err != nil {
			return err
		}
		totalRecords, err = checkedAdd(totalRecords, directory.RecordCount)
		if err != nil {
			return err
		}
		previousStreamID = directory.StreamID
	}
	headerIndexEnd, err := checkedAdd(header.IndexOffset, header.IndexLength)
	if err != nil {
		return err
	}
	headerDataEnd, err := checkedAdd(header.DataOffset, header.DataLength)
	if err != nil {
		return err
	}
	if totalRecords != header.RecordCount || expectedIndexOffset != headerIndexEnd || expectedDataOffset != headerDataEnd {
		return invalidf("Directory totals do not match Segment Header")
	}
	if firstEntryID != header.FirstEntryID || lastEntryID != header.LastEntryID {
		return invalidf("Directory Entry ID bounds do not match Segment Header")
	}
	return nil
}

// SegmentFooter identifies and checksums one immutable Segment file.
type SegmentFooter struct {
	SegmentID     UUID
	FileLength    uint64
	ContentLength uint64
	StreamCount   uint64
	RecordCount   uint64
	ContentSHA256 [sha256.Size]byte
}

func NewSegmentFooter(header SegmentHeader, content []byte) (SegmentFooter, error) {
	if err := validateSegmentHeader(header); err != nil {
		return SegmentFooter{}, err
	}
	if uint64(len(content)) != header.FooterOffset {
		return SegmentFooter{}, invalidf("Segment content length is %d, header footer_offset is %d", len(content), header.FooterOffset)
	}
	fileLength, err := checkedAdd(header.FooterOffset, SegmentFooterSectionLength)
	if err != nil {
		return SegmentFooter{}, err
	}
	return SegmentFooter{
		SegmentID:     header.SegmentID,
		FileLength:    fileLength,
		ContentLength: header.FooterOffset,
		StreamCount:   header.StreamCount,
		RecordCount:   header.RecordCount,
		ContentSHA256: sha256.Sum256(content),
	}, nil
}

func MarshalSegmentFooter(footer SegmentFooter) ([]byte, error) {
	if err := validateSegmentFooter(footer); err != nil {
		return nil, err
	}
	encoded := make([]byte, SegmentFooterLength)
	copy(encoded[0:8], segmentFooterMagic[:])
	putU16(encoded[8:10], VersionV1)
	putU16(encoded[10:12], SegmentFooterLength)
	putU32(encoded[12:16], 0)
	copy(encoded[16:32], footer.SegmentID[:])
	putU64(encoded[32:40], footer.FileLength)
	putU64(encoded[40:48], footer.ContentLength)
	putU64(encoded[48:56], footer.StreamCount)
	putU64(encoded[56:64], footer.RecordCount)
	copy(encoded[64:96], footer.ContentSHA256[:])
	putU32(encoded[96:100], checksum(encoded[:96]))
	return encoded, nil
}

func UnmarshalSegmentFooter(encoded []byte) (SegmentFooter, error) {
	var footer SegmentFooter
	if len(encoded) < SegmentFooterLength {
		return footer, truncatedf("Segment footer needs %d bytes, got %d", SegmentFooterLength, len(encoded))
	}
	if len(encoded) != SegmentFooterLength {
		return footer, invalidf("Segment footer has trailing bytes: %d", len(encoded)-SegmentFooterLength)
	}
	if !bytes.Equal(encoded[0:8], segmentFooterMagic[:]) {
		return footer, invalidf("Segment footer magic is %q", encoded[0:8])
	}
	if version := getU16(encoded[8:10]); version != VersionV1 {
		return footer, unsupportedVersion("Segment footer", version)
	}
	if length := getU16(encoded[10:12]); length != SegmentFooterLength {
		return footer, invalidf("Segment footer_length is %d", length)
	}
	if flags := getU32(encoded[12:16]); flags != 0 {
		return footer, invalidf("Segment footer flags contain unsupported bits: 0x%08x", flags)
	}
	if err := expectZero(encoded[100:104], "Segment footer reserved"); err != nil {
		return footer, err
	}
	storedCRC := getU32(encoded[96:100])
	actualCRC := checksum(encoded[:96])
	if storedCRC != actualCRC {
		return footer, checksumf("Segment footer CRC32C is %08x, want %08x", storedCRC, actualCRC)
	}
	copy(footer.SegmentID[:], encoded[16:32])
	footer.FileLength = getU64(encoded[32:40])
	footer.ContentLength = getU64(encoded[40:48])
	footer.StreamCount = getU64(encoded[48:56])
	footer.RecordCount = getU64(encoded[56:64])
	copy(footer.ContentSHA256[:], encoded[64:96])
	if err := validateSegmentFooter(footer); err != nil {
		return SegmentFooter{}, err
	}
	return footer, nil
}

func validateSegmentFooter(footer SegmentFooter) error {
	if isZeroUUID(footer.SegmentID) {
		return invalidf("Segment footer ID is zero")
	}
	if footer.StreamCount == 0 || footer.RecordCount == 0 {
		return invalidf("Segment footer describes an empty Segment")
	}
	expectedLength, err := checkedAdd(footer.ContentLength, SegmentFooterSectionLength)
	if err != nil || footer.FileLength != expectedLength {
		return invalidf("Segment file_length is %d, want %d", footer.FileLength, expectedLength)
	}
	if footer.ContentLength%SegmentSectionAlignment != 0 {
		return invalidf("Segment content_length is not section aligned")
	}
	if isZeroDigest(footer.ContentSHA256) {
		return invalidf("Segment content SHA-256 is zero")
	}
	return nil
}

// VerifySegmentFooter validates a complete Footer Section against content and header.
func VerifySegmentFooter(header SegmentHeader, content, footerSection []byte) (SegmentFooter, error) {
	if err := validateSegmentHeader(header); err != nil {
		return SegmentFooter{}, err
	}
	if len(footerSection) != SegmentFooterSectionLength {
		return SegmentFooter{}, invalidf("Segment footer section is %d bytes, want %d", len(footerSection), SegmentFooterSectionLength)
	}
	footer, err := UnmarshalSegmentFooter(footerSection[:SegmentFooterLength])
	if err != nil {
		return SegmentFooter{}, err
	}
	if err := expectZero(footerSection[SegmentFooterLength:], "Segment footer section padding"); err != nil {
		return SegmentFooter{}, err
	}
	if footer.SegmentID != header.SegmentID || footer.StreamCount != header.StreamCount || footer.RecordCount != header.RecordCount || footer.ContentLength != header.FooterOffset {
		return SegmentFooter{}, invalidf("Segment footer does not match header")
	}
	if uint64(len(content)) != footer.ContentLength || uint64(len(content)+len(footerSection)) != footer.FileLength {
		return SegmentFooter{}, invalidf("Segment footer lengths do not match file sections")
	}
	if digest := sha256.Sum256(content); digest != footer.ContentSHA256 {
		return SegmentFooter{}, checksumf("Segment content SHA-256 mismatch")
	}
	return footer, nil
}
