package wal

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
)

func TestCreateAppendRecoverTruncatedTailAndSeal(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	log, err := Create(root.Path(), 0, 0, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	requestHash := sha256.Sum256([]byte("request"))
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: 0, StreamID: 1, Sequence: 0, ByteOffset: 0, RecordedAt: 10, BatchCount: 1, RequestHash: requestHash, RequestID: []byte("r"), Producer: "test", Payload: []byte("data")})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := format.MarshalWALEntry(0, 0, frame)
	if err != nil {
		t.Fatal(err)
	}
	if err = log.Append(entry); err != nil {
		t.Fatal(err)
	}
	if err = log.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = log.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(root.Path(), "wal", "*.log"))
	if err != nil || len(files) != 1 {
		t.Fatalf("files %v %v", files, err)
	}
	f, err := os.OpenFile(files[0], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte("torn-tail")); err != nil {
		t.Fatal(err)
	}
	f.Close()
	log, err = Open(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	if log.Scan().TruncatedBytes != 9 || log.Scan().EntryCount != 1 {
		t.Fatalf("scan %+v", log.Scan())
	}
	if err = log.Seal(); err != nil {
		t.Fatal(err)
	}
	if err = log.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	header, err := format.UnmarshalWALFileHeader(data[:format.WALFileHeaderLength])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = format.VerifyWALSealFooter(data[:len(data)-format.WALSealFooterLength], data[len(data)-format.WALSealFooterLength:], header.FileID); err != nil {
		t.Fatal(err)
	}
}

func TestAppendRejectsDiscontinuity(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	log, err := Create(root.Path(), 0, 0, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	hash := sha256.Sum256([]byte("x"))
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: 1, StreamID: 1, Sequence: 0, RecordedAt: 1, BatchCount: 1, RequestHash: hash, Producer: "p"})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := format.MarshalWALEntry(0, 0, frame)
	if err != nil {
		t.Fatal(err)
	}
	if err = log.Append(entry); err == nil {
		t.Fatal("discontinuous Entry accepted")
	}
}

func TestRotatePreservesEntryCRCChain(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	log, err := Create(root.Path(), 0, 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("batch"))
	firstFrame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: 0, StreamID: 1, Sequence: 0, RecordedAt: 1, BatchCount: 1, RequestHash: hash, Producer: "p"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := format.MarshalWALEntry(1, 0, firstFrame)
	if err != nil {
		t.Fatal(err)
	}
	if err = log.Append(first); err != nil {
		t.Fatal(err)
	}
	firstEntry, err := format.UnmarshalWALEntry(first)
	if err != nil {
		t.Fatal(err)
	}
	if err = log.Rotate(2, time.Now()); err != nil {
		t.Fatal(err)
	}
	secondFrame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: 1, StreamID: 1, Sequence: 1, ByteOffset: uint64(len(firstFrame)), RecordedAt: 2, BatchCount: 1, RequestHash: hash, Producer: "p"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := format.MarshalWALEntry(2, firstEntry.CRC32C, secondFrame)
	if err != nil {
		t.Fatal(err)
	}
	if err = log.Append(second); err != nil {
		t.Fatal(err)
	}
	if err = log.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = log.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(root.Path(), "wal", "*.log"))
	if err != nil || len(files) != 2 {
		t.Fatalf("files %v %v", files, err)
	}
	reopened, err := Open(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Scan().LastEntryID != 1 {
		t.Fatalf("scan %+v", reopened.Scan())
	}
}
