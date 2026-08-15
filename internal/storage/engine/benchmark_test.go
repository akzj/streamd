package engine

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/wal"
)

type benchmarkDiskReplica struct{ log *wal.Log }

func (r benchmarkDiskReplica) Replicate(_ context.Context, encoded [][]byte) (uint64, error) {
	if err := r.log.Append(encoded...); err != nil {
		return 0, err
	}
	if err := r.log.Sync(); err != nil {
		return 0, err
	}
	entry, err := format.UnmarshalWALEntry(encoded[len(encoded)-1])
	return entry.EntryID, err
}

func (benchmarkDiskReplica) AdvanceCommit(context.Context, uint64) error { return nil }

func BenchmarkAppendSingleSync(b *testing.B) {
	store, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	payload := make([]byte, 1024)
	requestID := make([]byte, 8)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(requestID, uint64(i))
		_, err = store.Append(context.Background(), AppendRequest{Namespace: "bench", Stream: "events", ExpectedSequence: uint64(i), RequestID: requestID, Producer: "benchmark", Records: []InputRecord{{Payload: payload}}})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendReplicatedStrict(b *testing.B) {
	standbyRoot, err := fsutil.OpenRoot(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer standbyRoot.Close()
	standbyLog, err := wal.Create(standbyRoot.Path(), 0, 1, time.Now())
	if err != nil {
		b.Fatal(err)
	}
	defer standbyLog.Close()
	identity := format.NodeIdentity{ClusterID: engineID(1), GroupID: engineID(2), NodeID: engineID(3), CreatedAt: 1}
	store, err := OpenReplicated(b.TempDir(), identity, ReplicationOptions{Term: 1, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: benchmarkDiskReplica{log: standbyLog}, Guard: writableGuard{}})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	payload := make([]byte, 1024)
	requestID := make([]byte, 8)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(requestID, uint64(i))
		_, err = store.Append(context.Background(), AppendRequest{Namespace: "bench", Stream: "strict", ExpectedSequence: uint64(i), RequestID: requestID, Producer: "benchmark", Records: []InputRecord{{Payload: payload}}})
		if err != nil {
			b.Fatal(err)
		}
	}
}
