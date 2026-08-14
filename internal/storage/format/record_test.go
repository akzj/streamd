package format

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sampleRecord() RecordFrame {
	var requestHash [32]byte
	for i := range requestHash {
		requestHash[i] = byte(i)
	}
	return RecordFrame{
		EntryID:     7,
		StreamID:    3,
		Sequence:    2,
		ByteOffset:  100,
		RecordedAt:  1_700_000_000_123_456_789,
		BatchIndex:  0,
		BatchCount:  1,
		RequestHash: requestHash,
		RequestID:   []byte("req-1"),
		Producer:    "svc/test",
		Headers: []Header{
			{Key: "x-bin", Value: []byte{0, 1, 2}},
			{Key: "content-type", Value: []byte("application/json")},
		},
		Payload: []byte(`{"ok":true}`),
	}
}

func TestRecordFrameGoldenAndRoundTrip(t *testing.T) {
	encoded, err := MarshalRecordFrame(sampleRecord())
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "record_v1.hex", encoded)

	decoded, err := UnmarshalRecordFrame(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.EntryID != 7 || decoded.StreamID != 3 || decoded.Sequence != 2 || decoded.ByteOffset != 100 {
		t.Fatalf("unexpected identity fields: %+v", decoded)
	}
	if got := []string{decoded.Headers[0].Key, decoded.Headers[1].Key}; got[0] != "content-type" || got[1] != "x-bin" {
		t.Fatalf("headers are not canonical: %v", got)
	}
	reencoded, err := MarshalRecordFrame(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatal("record encoding is not stable after round trip")
	}
}

func TestRecordFrameRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		edit func(*RecordFrame)
		want error
	}{
		{
			name: "duplicate header",
			edit: func(frame *RecordFrame) {
				frame.Headers = append(frame.Headers, Header{Key: "x-bin", Value: []byte("other")})
			},
			want: ErrInvalid,
		},
		{
			name: "request id too large",
			edit: func(frame *RecordFrame) {
				frame.RequestID = make([]byte, MaxRequestIDLength+1)
			},
			want: ErrTooLarge,
		},
		{
			name: "invalid producer UTF-8",
			edit: func(frame *RecordFrame) {
				frame.Producer = string([]byte{0xff})
			},
			want: ErrInvalid,
		},
		{
			name: "batch index outside count",
			edit: func(frame *RecordFrame) {
				frame.BatchIndex = 1
			},
			want: ErrInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frame := sampleRecord()
			test.edit(&frame)
			_, err := MarshalRecordFrame(frame)
			if !errors.Is(err, test.want) {
				t.Fatalf("got %v, want error %v", err, test.want)
			}
		})
	}
}

func TestRecordFrameDetectsCorruption(t *testing.T) {
	encoded, err := MarshalRecordFrame(sampleRecord())
	if err != nil {
		t.Fatal(err)
	}

	corruptPayload := bytes.Clone(encoded)
	corruptPayload[len(corruptPayload)-RecordCRCSize-1] ^= 0x80
	if _, err := UnmarshalRecordFrame(corruptPayload); !errors.Is(err, ErrChecksum) {
		t.Fatalf("payload corruption: got %v, want checksum error", err)
	}

	truncated := encoded[:len(encoded)-1]
	if _, err := UnmarshalRecordFrame(truncated); !errors.Is(err, ErrTruncated) {
		t.Fatalf("truncation: got %v, want truncated error", err)
	}

	badBatch := bytes.Clone(encoded)
	putU32(badBatch[76:80], 0)
	rewriteRecordCRC(badBatch)
	if _, err := UnmarshalRecordFrame(badBatch); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad batch: got %v, want invalid error", err)
	}

	badOrder := bytes.Clone(encoded)
	headersStart := RecordFixedHeaderLength + len(sampleRecord().RequestID) + len(sampleRecord().Producer)
	firstKeyStart := headersStart + 8
	badOrder[firstKeyStart] = 'z'
	rewriteRecordCRC(badOrder)
	if _, err := UnmarshalRecordFrame(badOrder); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad header order: got %v, want invalid error", err)
	}
}

func TestUnmarshalRecordFrameOwnsVariableData(t *testing.T) {
	encoded, err := MarshalRecordFrame(sampleRecord())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalRecordFrame(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for i := range encoded {
		encoded[i] = 0
	}
	if string(decoded.RequestID) != "req-1" || string(decoded.Payload) != `{"ok":true}` {
		t.Fatal("decoded variable fields alias caller input")
	}
	if !bytes.Equal(decoded.Headers[1].Value, []byte{0, 1, 2}) {
		t.Fatal("decoded header value aliases caller input")
	}
}

func FuzzUnmarshalRecordFrame(f *testing.F) {
	seed, err := MarshalRecordFrame(sampleRecord())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("not a frame"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := UnmarshalRecordFrame(data)
		if err != nil {
			return
		}
		reencoded, err := MarshalRecordFrame(decoded)
		if err != nil {
			t.Fatalf("valid decoded frame cannot be encoded: %v", err)
		}
		if !bytes.Equal(data, reencoded) {
			t.Fatal("successful decode did not round trip exactly")
		}
	})
}

func rewriteRecordCRC(encoded []byte) {
	putU32(encoded[len(encoded)-RecordCRCSize:], checksum(encoded[:len(encoded)-RecordCRCSize]))
}

func assertGolden(t *testing.T, name string, actual []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	expected, err := hex.DecodeString(strings.TrimSpace(string(contents)))
	if err != nil {
		t.Fatalf("decode golden %s: %v", path, err)
	}
	if !bytes.Equal(expected, actual) {
		t.Fatalf("golden %s mismatch\nactual: %x", path, actual)
	}
}
