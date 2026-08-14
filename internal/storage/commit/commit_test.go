package commit

import (
	"context"
	"crypto/sha256"
	"errors"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/memtable"
	"testing"
)

type fakeLog struct {
	appended int
	syncErr  error
}

func (f *fakeLog) Append(b ...[]byte) error { f.appended += len(b); return nil }
func (f *fakeLog) Sync() error              { return f.syncErr }
func encodedBatch(t *testing.T) [][]byte {
	t.Helper()
	hash := sha256.Sum256([]byte("r"))
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: 0, StreamID: 1, BatchCount: 1, RequestHash: hash, Producer: "p"})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := format.MarshalWALEntry(0, 0, frame)
	if err != nil {
		t.Fatal(err)
	}
	return [][]byte{entry}
}
func TestCommitDurableThenVisible(t *testing.T) {
	log := &fakeLog{}
	table := memtable.New(0)
	c := New(log, table)
	result, err := c.Commit(context.Background(), encodedBatch(t))
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultUncertain || log.appended != 1 {
		t.Fatalf("result %+v", result)
	}
	water := c.Watermarks()
	if !water.HasValue || water.Applied != 0 {
		t.Fatalf("watermarks %+v", water)
	}
	if tail, ok := table.Tail(1); !ok || tail.NextSequence != 1 {
		t.Fatalf("tail %+v %v", tail, ok)
	}
}
func TestSyncFailurePoisonsCommitter(t *testing.T) {
	stop := errors.New("sync failed")
	c := New(&fakeLog{syncErr: stop}, memtable.New(0))
	result, err := c.Commit(context.Background(), encodedBatch(t))
	if !errors.Is(err, stop) || !result.ResultUncertain {
		t.Fatalf("result %+v error %v", result, err)
	}
	if _, err = c.Commit(context.Background(), encodedBatch(t)); err == nil {
		t.Fatal("poisoned Committer accepted request")
	}
}
