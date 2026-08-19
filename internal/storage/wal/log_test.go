package wal

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
)

func BenchmarkSealWithIncrementalDigest(b *testing.B) {
	for _, payloadBytes := range []int{1 << 20, 32 << 20} {
		b.Run(fmt.Sprintf("payload_%dMiB", payloadBytes>>20), func(b *testing.B) {
			requestHash := sha256.Sum256([]byte("seal-benchmark"))
			frame, err := format.MarshalRecordFrame(format.RecordFrame{
				EntryID: 0, StreamID: 1, Sequence: 0, RecordedAt: 1, BatchCount: 1,
				RequestHash: requestHash, RequestID: []byte("seal"), Producer: "benchmark",
				Payload: make([]byte, payloadBytes),
			})
			if err != nil {
				b.Fatal(err)
			}
			entry, err := format.MarshalWALEntry(1, 0, frame)
			if err != nil {
				b.Fatal(err)
			}
			base := b.TempDir()
			b.ReportMetric(float64(len(entry)+format.WALFileHeaderLength), "wal_bytes")
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				root, openErr := fsutil.OpenRoot(filepath.Join(base, fmt.Sprintf("run-%d", i)))
				if openErr != nil {
					b.Fatal(openErr)
				}
				log, createErr := Create(root.Path(), 0, 1, time.Unix(1, 0))
				if createErr == nil {
					createErr = log.Append(entry)
				}
				if createErr == nil {
					createErr = log.Sync()
				}
				if createErr != nil {
					root.Close()
					b.Fatal(createErr)
				}
				b.StartTimer()
				sealErr := log.Seal()
				b.StopTimer()
				closeErr := log.Close()
				rootErr := root.Close()
				if sealErr != nil || closeErr != nil || rootErr != nil {
					b.Fatalf("Seal=%v Log.Close=%v Root.Close=%v", sealErr, closeErr, rootErr)
				}
			}
		})
	}
}

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
	first, err := format.UnmarshalWALEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	secondFrame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: 1, StreamID: 1, Sequence: 1, ByteOffset: uint64(len(frame)), RecordedAt: 11, BatchCount: 1, RequestHash: requestHash, RequestID: []byte("r2"), Producer: "test", Payload: []byte("more")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := format.MarshalWALEntry(0, first.CRC32C, secondFrame)
	if err != nil {
		t.Fatal(err)
	}
	if err = log.Append(second); err != nil {
		t.Fatal(err)
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
	sealed, err := ScanSealed(files[0], nil)
	if err != nil || sealed.EntryCount != 2 || sealed.LastEntryID != 1 {
		t.Fatalf("sealed scan %+v, error %v", sealed, err)
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
	reopened, err := OpenWithPrevious(root.Path(), firstEntry.CRC32C)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Scan().LastEntryID != 1 {
		t.Fatalf("scan %+v", reopened.Scan())
	}
}

func TestScanSealedAcceptsMultipleContinuousEntries(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	log, err := Create(root.Path(), 0, 0, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("batch"))
	previous := uint32(0)
	entries := make([][]byte, 0, 2)
	for i := 0; i < 2; i++ {
		frame, frameErr := format.MarshalRecordFrame(format.RecordFrame{EntryID: uint64(i), StreamID: 1, Sequence: uint64(i), RecordedAt: int64(i), BatchIndex: uint32(i), BatchCount: 2, RequestHash: hash, Producer: "p"})
		if frameErr != nil {
			t.Fatal(frameErr)
		}
		entry, entryErr := format.MarshalWALEntry(0, previous, frame)
		if entryErr != nil {
			t.Fatal(entryErr)
		}
		decoded, decodeErr := format.UnmarshalWALEntry(entry)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		previous = decoded.CRC32C
		entries = append(entries, entry)
	}
	if err = log.Append(entries...); err != nil {
		t.Fatal(err)
	}
	if err = log.Seal(); err != nil {
		t.Fatal(err)
	}
	if err = log.Close(); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(root.Path(), "wal", "*.log"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("paths = %v, error = %v", paths, err)
	}
	scan, err := ScanSealed(paths[0], nil)
	if err != nil || scan.EntryCount != 2 || scan.LastEntryID != 1 {
		t.Fatalf("scan = %+v, error = %v", scan, err)
	}
}

func TestRotateEmptyWALPreservesSnapshotCRCBase(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	const previous = uint32(12345)
	log, err := CreateAfter(root.Path(), 5, 1, previous, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if err = log.Rotate(2, time.Now()); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("after-snapshot"))
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: 5, StreamID: 1, Sequence: 0, RecordedAt: 1, BatchCount: 1, RequestHash: hash, Producer: "p"})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := format.MarshalWALEntry(2, previous, frame)
	if err != nil {
		t.Fatal(err)
	}
	if err = log.Append(entry); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(root.Path(), "wal", "*.log"))
	if err != nil || len(files) != 1 {
		t.Fatalf("empty rotated WAL was retained: %v, %v", files, err)
	}
}
