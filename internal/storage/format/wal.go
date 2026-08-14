package format

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"
	"unicode/utf8"
)

var (
	walFileMagic    = [8]byte{'S', 'T', 'R', 'M', 'W', 'A', 'L', '1'}
	walEntryMagic   = [4]byte{'S', 'W', 'E', '1'}
	walSealMagic    = [8]byte{'W', 'A', 'L', 'S', 'E', 'A', 'L', '1'}
	walCurrentMagic = [8]byte{'W', 'A', 'L', 'C', 'U', 'R', 'V', '1'}
)

type WALCurrentPointer struct {
	FileID       UUID
	FirstEntryID uint64
	FileName     string
}

func MarshalWALCurrentPointer(p WALCurrentPointer) ([]byte, error) {
	if isZeroUUID(p.FileID) || p.FileName == "" || !utf8.ValidString(p.FileName) || strings.ContainsRune(p.FileName, 0) || strings.ContainsAny(p.FileName, "/\\") || p.FileName == "." || p.FileName == ".." {
		return nil, invalidf("WAL-CURRENT identity or name is invalid")
	}
	n := 48 + len(p.FileName)
	if n > int(^uint16(0)) || len(p.FileName) > int(^uint16(0)) {
		return nil, fmtTooLarge("WAL-CURRENT", n, ^uint16(0))
	}
	b := make([]byte, n)
	copy(b[:8], walCurrentMagic[:])
	putU16(b[8:10], VersionV1)
	putU16(b[10:12], uint16(n))
	copy(b[16:32], p.FileID[:])
	putU64(b[32:40], p.FirstEntryID)
	putU16(b[40:42], uint16(len(p.FileName)))
	copy(b[44:], p.FileName)
	putU32(b[n-4:], checksum(b[:n-4]))
	return b, nil
}
func UnmarshalWALCurrentPointer(b []byte) (WALCurrentPointer, error) {
	var p WALCurrentPointer
	if len(b) < 48 {
		return p, truncatedf("WAL-CURRENT is truncated")
	}
	if !bytes.Equal(b[:8], walCurrentMagic[:]) {
		return p, invalidf("WAL-CURRENT magic is invalid")
	}
	if v := getU16(b[8:10]); v != VersionV1 {
		return p, unsupportedVersion("WAL-CURRENT", v)
	}
	nameLen := int(getU16(b[40:42]))
	if int(getU16(b[10:12])) != len(b) || len(b) != 48+nameLen || getU32(b[12:16]) != 0 {
		return p, invalidf("WAL-CURRENT length or flags are invalid")
	}
	if err := expectZero(b[42:44], "WAL-CURRENT reserved"); err != nil {
		return p, err
	}
	if stored, actual := getU32(b[len(b)-4:]), checksum(b[:len(b)-4]); stored != actual {
		return p, checksumf("WAL-CURRENT CRC mismatch")
	}
	copy(p.FileID[:], b[16:32])
	p.FirstEntryID = getU64(b[32:40])
	p.FileName = string(bytes.Clone(b[44 : len(b)-4]))
	if _, err := MarshalWALCurrentPointer(p); err != nil {
		return WALCurrentPointer{}, err
	}
	return p, nil
}

const (
	walEntryCRCSize       = 4
	maxWALEntryLength     = WALEntryHeaderLength + MaxFrameLength + walEntryCRCSize
	minimumWALEntryLength = WALEntryHeaderLength + RecordFixedHeaderLength + RecordCRCSize + walEntryCRCSize
)

// WALFileHeader is the fixed header of one active or sealed WAL file.
type WALFileHeader struct {
	FileID       UUID
	FirstEntryID uint64
	CreatedTerm  uint64
	CreatedAt    int64
}

// MarshalWALFileHeader returns the exact 64-byte V1 encoding.
func MarshalWALFileHeader(header WALFileHeader) []byte {
	encoded := make([]byte, WALFileHeaderLength)
	copy(encoded[0:8], walFileMagic[:])
	putU16(encoded[8:10], VersionV1)
	putU16(encoded[10:12], WALFileHeaderLength)
	putU32(encoded[12:16], 0)
	copy(encoded[16:32], header.FileID[:])
	putU64(encoded[32:40], header.FirstEntryID)
	putU64(encoded[40:48], header.CreatedTerm)
	putI64(encoded[48:56], header.CreatedAt)
	putU32(encoded[56:60], checksum(encoded[:56]))
	return encoded
}

// UnmarshalWALFileHeader validates one exact V1 WAL file header.
func UnmarshalWALFileHeader(encoded []byte) (WALFileHeader, error) {
	var header WALFileHeader
	if len(encoded) < WALFileHeaderLength {
		return header, truncatedf("WAL file header needs %d bytes, got %d", WALFileHeaderLength, len(encoded))
	}
	if len(encoded) != WALFileHeaderLength {
		return header, invalidf("WAL file header has trailing bytes: %d", len(encoded)-WALFileHeaderLength)
	}
	if !bytes.Equal(encoded[0:8], walFileMagic[:]) {
		return header, invalidf("WAL file magic is %q", encoded[0:8])
	}
	if version := getU16(encoded[8:10]); version != VersionV1 {
		return header, unsupportedVersion("WAL file", version)
	}
	if length := getU16(encoded[10:12]); length != WALFileHeaderLength {
		return header, invalidf("WAL file header_length is %d", length)
	}
	if flags := getU32(encoded[12:16]); flags != 0 {
		return header, invalidf("WAL file flags contain unsupported bits: 0x%08x", flags)
	}
	if err := expectZero(encoded[60:64], "WAL file reserved"); err != nil {
		return header, err
	}
	storedCRC := getU32(encoded[56:60])
	actualCRC := checksum(encoded[:56])
	if storedCRC != actualCRC {
		return header, checksumf("WAL file header CRC32C is %08x, want %08x", storedCRC, actualCRC)
	}
	copy(header.FileID[:], encoded[16:32])
	header.FirstEntryID = getU64(encoded[32:40])
	header.CreatedTerm = getU64(encoded[40:48])
	header.CreatedAt = getI64(encoded[48:56])
	if isZeroUUID(header.FileID) {
		return WALFileHeader{}, invalidf("WAL file ID is zero")
	}
	return header, nil
}

// WALEntry is a validated WAL envelope and its immutable Record Frame.
type WALEntry struct {
	Term                uint64
	EntryID             uint64
	StreamID            uint64
	Sequence            uint64
	ByteOffset          uint64
	RecordedAt          int64
	BatchIndex          uint32
	BatchCount          uint32
	PreviousEntryCRC32C uint32
	Frame               []byte
	Record              RecordFrame
	CRC32C              uint32
}

// MarshalWALEntry wraps one already encoded Record Frame in a V1 WAL Entry.
func MarshalWALEntry(term uint64, previousEntryCRC32C uint32, frame []byte) ([]byte, error) {
	record, err := decodeRecordFrame(frame, false)
	if err != nil {
		return nil, fmt.Errorf("WAL record frame: %w", err)
	}
	if record.EntryID == 0 && previousEntryCRC32C != 0 {
		return nil, invalidf("EntryID 0 must have previous_entry_crc32c 0")
	}
	entryLength := WALEntryHeaderLength + len(frame) + walEntryCRCSize
	if entryLength > maxWALEntryLength {
		return nil, fmtTooLarge("WAL entry", entryLength, maxWALEntryLength)
	}
	encoded := make([]byte, entryLength)
	copy(encoded[0:4], walEntryMagic[:])
	putU16(encoded[4:6], VersionV1)
	putU16(encoded[6:8], 0)
	putU32(encoded[8:12], uint32(entryLength))
	putU16(encoded[12:14], WALEntryHeaderLength)
	putU16(encoded[14:16], 0)
	putU64(encoded[16:24], term)
	putU64(encoded[24:32], record.EntryID)
	putU64(encoded[32:40], record.StreamID)
	putU64(encoded[40:48], record.Sequence)
	putU64(encoded[48:56], record.ByteOffset)
	putI64(encoded[56:64], record.RecordedAt)
	putU32(encoded[64:68], record.BatchIndex)
	putU32(encoded[68:72], record.BatchCount)
	putU32(encoded[72:76], uint32(len(frame)))
	putU32(encoded[76:80], previousEntryCRC32C)
	putU32(encoded[92:96], checksum(encoded[:92]))
	copy(encoded[WALEntryHeaderLength:], frame)
	putU32(encoded[entryLength-walEntryCRCSize:], checksum(encoded[:entryLength-walEntryCRCSize]))
	return encoded, nil
}

// DecodeWALEntry decodes the first V1 WAL Entry in encoded and returns bytes consumed.
func DecodeWALEntry(encoded []byte) (WALEntry, int, error) {
	var entry WALEntry
	declaredLength, err := inspectWALEntryHeader(encoded)
	if err != nil {
		return entry, 0, err
	}
	if len(encoded) < declaredLength {
		return entry, 0, truncatedf("WAL entry declares %d bytes, got %d", declaredLength, len(encoded))
	}
	entry, err = unmarshalWALEntryExact(encoded[:declaredLength])
	if err != nil {
		return WALEntry{}, 0, err
	}
	return entry, declaredLength, nil
}

// UnmarshalWALEntry validates exactly one V1 WAL Entry.
func UnmarshalWALEntry(encoded []byte) (WALEntry, error) {
	entry, consumed, err := DecodeWALEntry(encoded)
	if err != nil {
		return WALEntry{}, err
	}
	if consumed != len(encoded) {
		return WALEntry{}, invalidf("WAL entry has %d trailing bytes", len(encoded)-consumed)
	}
	return entry, nil
}

func inspectWALEntryHeader(encoded []byte) (int, error) {
	if len(encoded) < WALEntryHeaderLength {
		return 0, truncatedf("WAL entry header needs %d bytes, got %d", WALEntryHeaderLength, len(encoded))
	}
	if !bytes.Equal(encoded[0:4], walEntryMagic[:]) {
		return 0, invalidf("WAL entry magic is %q", encoded[0:4])
	}
	if version := getU16(encoded[4:6]); version != VersionV1 {
		return 0, unsupportedVersion("WAL entry", version)
	}
	if flags := getU16(encoded[6:8]); flags != 0 {
		return 0, invalidf("WAL entry flags contain unsupported bits: 0x%04x", flags)
	}
	declaredLength := uint64(getU32(encoded[8:12]))
	if declaredLength < minimumWALEntryLength {
		return 0, invalidf("WAL entry length is too small: %d", declaredLength)
	}
	if declaredLength > maxWALEntryLength {
		return 0, fmtTooLarge("WAL entry", declaredLength, maxWALEntryLength)
	}
	if headerLength := getU16(encoded[12:14]); headerLength != WALEntryHeaderLength {
		return 0, invalidf("WAL entry header_length is %d", headerLength)
	}
	if err := expectZero(encoded[14:16], "WAL entry reserved_0"); err != nil {
		return 0, err
	}
	if err := expectZero(encoded[80:92], "WAL entry reserved_1"); err != nil {
		return 0, err
	}
	storedHeaderCRC := getU32(encoded[92:96])
	actualHeaderCRC := checksum(encoded[:92])
	if storedHeaderCRC != actualHeaderCRC {
		return 0, checksumf("WAL entry header CRC32C is %08x, want %08x", storedHeaderCRC, actualHeaderCRC)
	}
	length, err := checkedInt(declaredLength, "WAL entry length")
	return length, err
}

func unmarshalWALEntryExact(encoded []byte) (WALEntry, error) {
	var entry WALEntry
	frameLength := uint64(getU32(encoded[72:76]))
	expectedLength, err := checkedAdd(WALEntryHeaderLength, frameLength, walEntryCRCSize)
	if err != nil {
		return entry, err
	}
	if expectedLength != uint64(len(encoded)) {
		return entry, invalidf("WAL entry components sum to %d, entry_length is %d", expectedLength, len(encoded))
	}
	storedCRC := getU32(encoded[len(encoded)-walEntryCRCSize:])
	actualCRC := checksum(encoded[:len(encoded)-walEntryCRCSize])
	if storedCRC != actualCRC {
		return entry, checksumf("WAL entry CRC32C is %08x, want %08x", storedCRC, actualCRC)
	}
	frameStart := WALEntryHeaderLength
	frameEnd := len(encoded) - walEntryCRCSize
	frame := bytes.Clone(encoded[frameStart:frameEnd])
	record, err := decodeRecordFrame(frame, false)
	if err != nil {
		return entry, fmt.Errorf("WAL record frame: %w", err)
	}

	entry.Term = getU64(encoded[16:24])
	entry.EntryID = getU64(encoded[24:32])
	entry.StreamID = getU64(encoded[32:40])
	entry.Sequence = getU64(encoded[40:48])
	entry.ByteOffset = getU64(encoded[48:56])
	entry.RecordedAt = getI64(encoded[56:64])
	entry.BatchIndex = getU32(encoded[64:68])
	entry.BatchCount = getU32(encoded[68:72])
	entry.PreviousEntryCRC32C = getU32(encoded[76:80])
	entry.Frame = frame
	entry.Record = record
	entry.CRC32C = storedCRC
	if entry.EntryID == 0 && entry.PreviousEntryCRC32C != 0 {
		return WALEntry{}, invalidf("EntryID 0 must have previous_entry_crc32c 0")
	}

	if entry.EntryID != record.EntryID ||
		entry.StreamID != record.StreamID ||
		entry.Sequence != record.Sequence ||
		entry.ByteOffset != record.ByteOffset ||
		entry.RecordedAt != record.RecordedAt ||
		entry.BatchIndex != record.BatchIndex ||
		entry.BatchCount != record.BatchCount {
		return WALEntry{}, invalidf("WAL envelope fields do not match Record Frame")
	}
	return entry, nil
}

// WALSealFooter closes one immutable WAL file.
type WALSealFooter struct {
	FileID          UUID
	EntryCount      uint64
	LastEntryID     uint64
	LastEntryCRC32C uint32
	ContentSHA256   [sha256.Size]byte
}

// NewWALSealFooter computes the canonical content digest for a sealed WAL.
func NewWALSealFooter(fileID UUID, content []byte, entryCount, lastEntryID uint64, lastEntryCRC32C uint32) (WALSealFooter, error) {
	if entryCount == 0 && (lastEntryID != 0 || lastEntryCRC32C != 0) {
		return WALSealFooter{}, invalidf("empty WAL seal has non-zero last entry metadata")
	}
	return WALSealFooter{
		FileID:          fileID,
		EntryCount:      entryCount,
		LastEntryID:     lastEntryID,
		LastEntryCRC32C: lastEntryCRC32C,
		ContentSHA256:   sha256.Sum256(content),
	}, nil
}

// MarshalWALSealFooter returns the exact 96-byte V1 footer encoding.
func MarshalWALSealFooter(footer WALSealFooter) ([]byte, error) {
	if footer.EntryCount == 0 && (footer.LastEntryID != 0 || footer.LastEntryCRC32C != 0) {
		return nil, invalidf("empty WAL seal has non-zero last entry metadata")
	}
	encoded := make([]byte, WALSealFooterLength)
	copy(encoded[0:8], walSealMagic[:])
	putU16(encoded[8:10], VersionV1)
	putU16(encoded[10:12], WALSealFooterLength)
	putU32(encoded[12:16], 0)
	copy(encoded[16:32], footer.FileID[:])
	putU64(encoded[32:40], footer.EntryCount)
	putU64(encoded[40:48], footer.LastEntryID)
	putU32(encoded[48:52], footer.LastEntryCRC32C)
	copy(encoded[56:88], footer.ContentSHA256[:])
	putU32(encoded[88:92], checksum(encoded[:88]))
	return encoded, nil
}

// UnmarshalWALSealFooter validates one exact V1 footer.
func UnmarshalWALSealFooter(encoded []byte) (WALSealFooter, error) {
	var footer WALSealFooter
	if len(encoded) < WALSealFooterLength {
		return footer, truncatedf("WAL seal footer needs %d bytes, got %d", WALSealFooterLength, len(encoded))
	}
	if len(encoded) != WALSealFooterLength {
		return footer, invalidf("WAL seal footer has trailing bytes: %d", len(encoded)-WALSealFooterLength)
	}
	if !bytes.Equal(encoded[0:8], walSealMagic[:]) {
		return footer, invalidf("WAL seal magic is %q", encoded[0:8])
	}
	if version := getU16(encoded[8:10]); version != VersionV1 {
		return footer, unsupportedVersion("WAL seal", version)
	}
	if length := getU16(encoded[10:12]); length != WALSealFooterLength {
		return footer, invalidf("WAL seal footer_length is %d", length)
	}
	if flags := getU32(encoded[12:16]); flags != 0 {
		return footer, invalidf("WAL seal flags contain unsupported bits: 0x%08x", flags)
	}
	if err := expectZero(encoded[52:56], "WAL seal reserved"); err != nil {
		return footer, err
	}
	if err := expectZero(encoded[92:96], "WAL seal reserved_tail"); err != nil {
		return footer, err
	}
	storedCRC := getU32(encoded[88:92])
	actualCRC := checksum(encoded[:88])
	if storedCRC != actualCRC {
		return footer, checksumf("WAL seal CRC32C is %08x, want %08x", storedCRC, actualCRC)
	}
	copy(footer.FileID[:], encoded[16:32])
	footer.EntryCount = getU64(encoded[32:40])
	footer.LastEntryID = getU64(encoded[40:48])
	footer.LastEntryCRC32C = getU32(encoded[48:52])
	copy(footer.ContentSHA256[:], encoded[56:88])
	if footer.EntryCount == 0 && (footer.LastEntryID != 0 || footer.LastEntryCRC32C != 0) {
		return WALSealFooter{}, invalidf("empty WAL seal has non-zero last entry metadata")
	}
	return footer, nil
}

// VerifyWALSealFooter checks that footer identifies content and fileID.
func VerifyWALSealFooter(content, encodedFooter []byte, fileID UUID) (WALSealFooter, error) {
	footer, err := UnmarshalWALSealFooter(encodedFooter)
	if err != nil {
		return WALSealFooter{}, err
	}
	if footer.FileID != fileID {
		return WALSealFooter{}, invalidf("WAL seal file_id does not match WAL header")
	}
	actualDigest := sha256.Sum256(content)
	if actualDigest != footer.ContentSHA256 {
		return WALSealFooter{}, checksumf("WAL seal content SHA-256 mismatch")
	}
	if len(content) < WALFileHeaderLength {
		return WALSealFooter{}, truncatedf("sealed WAL content needs a file header")
	}
	header, err := UnmarshalWALFileHeader(content[:WALFileHeaderLength])
	if err != nil {
		return WALSealFooter{}, fmt.Errorf("sealed WAL header: %w", err)
	}
	if header.FileID != fileID {
		return WALSealFooter{}, invalidf("WAL header file_id does not match expected file_id")
	}

	position := WALFileHeaderLength
	var count uint64
	var lastID uint64
	var lastCRC uint32
	for position < len(content) {
		entry, consumed, err := DecodeWALEntry(content[position:])
		if err != nil {
			return WALSealFooter{}, fmt.Errorf("sealed WAL entry %d: %w", count, err)
		}
		if count == 0 {
			if entry.EntryID != header.FirstEntryID {
				return WALSealFooter{}, invalidf("first WAL entry ID is %d, header declares %d", entry.EntryID, header.FirstEntryID)
			}
		} else {
			if lastID == ^uint64(0) || entry.EntryID != lastID+1 {
				return WALSealFooter{}, invalidf("WAL entry ID %d does not follow %d", entry.EntryID, lastID)
			}
			if entry.PreviousEntryCRC32C != lastCRC {
				return WALSealFooter{}, invalidf("WAL entry %d previous CRC does not match entry %d", entry.EntryID, lastID)
			}
		}
		count++
		lastID = entry.EntryID
		lastCRC = entry.CRC32C
		position += consumed
	}
	if count != footer.EntryCount {
		return WALSealFooter{}, invalidf("WAL seal entry_count is %d, scanned %d", footer.EntryCount, count)
	}
	if count == 0 {
		if footer.LastEntryID != 0 || footer.LastEntryCRC32C != 0 {
			return WALSealFooter{}, invalidf("empty WAL seal has non-zero last entry metadata")
		}
	} else if footer.LastEntryID != lastID || footer.LastEntryCRC32C != lastCRC {
		return WALSealFooter{}, invalidf("WAL seal last entry metadata does not match content")
	}
	return footer, nil
}
