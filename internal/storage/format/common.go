package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	VersionV1 uint16 = 1

	RecordFixedHeaderLength = 112
	RecordCRCSize           = 4
	MaxFrameLength          = 256 << 20
	MaxRequestIDLength      = 256
	MaxProducerLength       = 1024
	MaxHeaderCount          = 1024
	MaxHeaderKeyLength      = 1024
	MaxHeadersLength        = 4 << 20
	MaxBatchRecordCount     = 65535

	WALFileHeaderLength  = 64
	WALEntryHeaderLength = 96
	WALSealFooterLength  = 96

	SegmentSectionAlignment    = 4096
	SegmentHeaderLength        = 160
	StreamDirectoryEntryLength = 112
	DenseIndexEntryLength      = 24
	SegmentFooterLength        = 104
	SegmentFooterSectionLength = 4096
	ManifestHeaderLength       = 136
	ManifestFooterLength       = 88
)

var (
	ErrInvalid            = errors.New("invalid storage format")
	ErrChecksum           = errors.New("checksum mismatch")
	ErrTruncated          = errors.New("truncated storage object")
	ErrTooLarge           = errors.New("storage object exceeds format limit")
	ErrUnsupportedVersion = errors.New("unsupported storage format version")
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// UUID is the 16-byte storage representation used for physical artifact IDs.
type UUID [16]byte

func checksum(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliTable)
}

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

func truncatedf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrTruncated, fmt.Sprintf(format, args...))
}

func checksumf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrChecksum, fmt.Sprintf(format, args...))
}

func unsupportedVersion(kind string, got uint16) error {
	return fmt.Errorf("%w: %s version %d", ErrUnsupportedVersion, kind, got)
}

func fmtTooLarge(field string, got, limit any) error {
	return fmt.Errorf("%w: %s is %v, limit is %v", ErrTooLarge, field, got, limit)
}

func putU16(dst []byte, value uint16) { binary.LittleEndian.PutUint16(dst, value) }
func putU32(dst []byte, value uint32) { binary.LittleEndian.PutUint32(dst, value) }
func putU64(dst []byte, value uint64) { binary.LittleEndian.PutUint64(dst, value) }
func putI64(dst []byte, value int64)  { binary.LittleEndian.PutUint64(dst, uint64(value)) }

func getU16(src []byte) uint16 { return binary.LittleEndian.Uint16(src) }
func getU32(src []byte) uint32 { return binary.LittleEndian.Uint32(src) }
func getU64(src []byte) uint64 { return binary.LittleEndian.Uint64(src) }
func getI64(src []byte) int64  { return int64(binary.LittleEndian.Uint64(src)) }

func checkedAdd(parts ...uint64) (uint64, error) {
	var sum uint64
	for _, part := range parts {
		if ^uint64(0)-sum < part {
			return 0, invalidf("integer overflow while summing lengths")
		}
		sum += part
	}
	return sum, nil
}

func checkedMul(left, right uint64, field string) (uint64, error) {
	if left != 0 && right > ^uint64(0)/left {
		return 0, invalidf("integer overflow while multiplying %s", field)
	}
	return left * right, nil
}

func alignUp(value, alignment uint64) (uint64, error) {
	if alignment == 0 || alignment&(alignment-1) != 0 {
		return 0, invalidf("alignment must be a non-zero power of two")
	}
	added, err := checkedAdd(value, alignment-1)
	if err != nil {
		return 0, err
	}
	return added &^ (alignment - 1), nil
}

func checkedInt(value uint64, field string) (int, error) {
	converted := int(value)
	if converted < 0 || uint64(converted) != value {
		return 0, invalidf("%s does not fit int: %d", field, value)
	}
	return converted, nil
}

func expectZero(data []byte, field string) error {
	for _, value := range data {
		if value != 0 {
			return invalidf("%s must be zero", field)
		}
	}
	return nil
}

func isZeroUUID(value UUID) bool {
	return value == UUID{}
}

func isZeroDigest(value [32]byte) bool {
	return value == [32]byte{}
}
