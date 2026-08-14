package format

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
)

func sampleUUID() UUID {
	return UUID{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
}

func TestWALFileHeaderGoldenAndRoundTrip(t *testing.T) {
	header := WALFileHeader{
		FileID:       sampleUUID(),
		FirstEntryID: 7,
		CreatedTerm:  4,
		CreatedAt:    1_700_000_000_000_000_000,
	}
	encoded := MarshalWALFileHeader(header)
	assertGolden(t, "wal_file_header_v1.hex", encoded)
	decoded, err := UnmarshalWALFileHeader(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != header {
		t.Fatalf("decoded header differs: got %+v want %+v", decoded, header)
	}
}

func TestWALEntryGoldenAndRoundTrip(t *testing.T) {
	frame, err := MarshalRecordFrame(sampleRecord())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalWALEntry(4, 0x12345678, frame)
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "wal_entry_v1.hex", encoded)

	decoded, err := UnmarshalWALEntry(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Term != 4 || decoded.EntryID != 7 || decoded.PreviousEntryCRC32C != 0x12345678 {
		t.Fatalf("unexpected WAL entry: %+v", decoded)
	}
	if !bytes.Equal(decoded.Frame, frame) {
		t.Fatal("decoded WAL frame differs")
	}
}

func TestDecodeWALEntryConsumesOneEntry(t *testing.T) {
	frame := sampleRecord()
	frame.EntryID = 1
	encodedFrame, err := MarshalRecordFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	first, err := MarshalWALEntry(2, 99, encodedFrame)
	if err != nil {
		t.Fatal(err)
	}
	frame.EntryID = 2
	frame.Sequence++
	frame.ByteOffset += uint64(len(encodedFrame))
	secondFrame, err := MarshalRecordFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	second, err := MarshalWALEntry(2, getU32(first[len(first)-walEntryCRCSize:]), secondFrame)
	if err != nil {
		t.Fatal(err)
	}
	joined := append(bytes.Clone(first), second...)
	decoded, consumed, err := DecodeWALEntry(joined)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(first) || decoded.EntryID != 1 {
		t.Fatalf("consumed=%d entry=%d", consumed, decoded.EntryID)
	}
}

func TestWALEntryDetectsCorruption(t *testing.T) {
	frame, err := MarshalRecordFrame(sampleRecord())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalWALEntry(4, 0x12345678, frame)
	if err != nil {
		t.Fatal(err)
	}

	badHeader := bytes.Clone(encoded)
	badHeader[24] ^= 1
	if _, err := UnmarshalWALEntry(badHeader); !errors.Is(err, ErrChecksum) {
		t.Fatalf("header corruption: got %v, want checksum error", err)
	}

	badBody := bytes.Clone(encoded)
	badBody[WALEntryHeaderLength+RecordFixedHeaderLength] ^= 1
	if _, err := UnmarshalWALEntry(badBody); !errors.Is(err, ErrChecksum) {
		t.Fatalf("body corruption: got %v, want checksum error", err)
	}

	mismatch := bytes.Clone(encoded)
	putU64(mismatch[40:48], 999)
	putU32(mismatch[92:96], checksum(mismatch[:92]))
	putU32(mismatch[len(mismatch)-walEntryCRCSize:], checksum(mismatch[:len(mismatch)-walEntryCRCSize]))
	if _, err := UnmarshalWALEntry(mismatch); !errors.Is(err, ErrInvalid) {
		t.Fatalf("envelope mismatch: got %v, want invalid error", err)
	}

	if _, err := UnmarshalWALEntry(encoded[:len(encoded)-1]); !errors.Is(err, ErrTruncated) {
		t.Fatalf("truncated entry: got %v, want truncated error", err)
	}
}

func TestWALEntryZeroRequiresZeroPreviousCRC(t *testing.T) {
	record := sampleRecord()
	record.EntryID = 0
	frame, err := MarshalRecordFrame(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := MarshalWALEntry(1, 1, frame); !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v, want invalid previous CRC", err)
	}
}

func TestWALSealFooterGoldenAndVerification(t *testing.T) {
	header := MarshalWALFileHeader(WALFileHeader{
		FileID:       sampleUUID(),
		FirstEntryID: 7,
		CreatedTerm:  4,
		CreatedAt:    1_700_000_000_000_000_000,
	})
	frame, err := MarshalRecordFrame(sampleRecord())
	if err != nil {
		t.Fatal(err)
	}
	entry, err := MarshalWALEntry(4, 0x12345678, frame)
	if err != nil {
		t.Fatal(err)
	}
	content := append(bytes.Clone(header), entry...)
	entryCRC := getU32(entry[len(entry)-walEntryCRCSize:])
	footer, err := NewWALSealFooter(sampleUUID(), content, 1, 7, entryCRC)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalWALSealFooter(footer)
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "wal_seal_footer_v1.hex", encoded)
	verified, err := VerifyWALSealFooter(content, encoded, sampleUUID())
	if err != nil {
		t.Fatal(err)
	}
	if verified.ContentSHA256 != sha256.Sum256(content) {
		t.Fatal("verified seal has wrong digest")
	}

	corruptContent := bytes.Clone(content)
	corruptContent[len(corruptContent)-1] ^= 1
	if _, err := VerifyWALSealFooter(corruptContent, encoded, sampleUUID()); !errors.Is(err, ErrChecksum) {
		t.Fatalf("corrupt content: got %v, want checksum error", err)
	}

	wrongCount := bytes.Clone(encoded)
	putU64(wrongCount[32:40], 2)
	putU32(wrongCount[88:92], checksum(wrongCount[:88]))
	if _, err := VerifyWALSealFooter(content, wrongCount, sampleUUID()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong entry count: got %v, want invalid error", err)
	}
}

func FuzzUnmarshalWALEntry(f *testing.F) {
	frame, err := MarshalRecordFrame(sampleRecord())
	if err != nil {
		f.Fatal(err)
	}
	seed, err := MarshalWALEntry(4, 0x12345678, frame)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("not a WAL entry"))
	f.Fuzz(func(t *testing.T, data []byte) {
		entry, err := UnmarshalWALEntry(data)
		if err != nil {
			return
		}
		reencoded, err := MarshalWALEntry(entry.Term, entry.PreviousEntryCRC32C, entry.Frame)
		if err != nil {
			t.Fatalf("valid decoded WAL entry cannot be encoded: %v", err)
		}
		if !bytes.Equal(data, reencoded) {
			t.Fatal("successful WAL decode did not round trip exactly")
		}
	})
}
