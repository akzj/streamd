package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
)

type partitionReplica struct {
	partitioned bool
}

func (r *partitionReplica) Replicate(_ context.Context, encoded [][]byte) (uint64, error) {
	if r.partitioned {
		return 0, errors.New("injected Standby partition")
	}
	entry, err := format.UnmarshalWALEntry(encoded[len(encoded)-1])
	return entry.EntryID, err
}

func (*partitionReplica) AdvanceCommit(context.Context, uint64) error { return nil }

type writableGuard struct{}

func (writableGuard) CanCommit() error { return nil }

func TestHADrillPartitionStopsStrictCommitButKeepsReads(t *testing.T) {
	identity := format.NodeIdentity{ClusterID: engineID(1), GroupID: engineID(2), NodeID: engineID(3), CreatedAt: 1}
	replica := &partitionReplica{}
	store, err := OpenReplicated(t.TempDir(), identity, ReplicationOptions{Term: 1, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: replica, Guard: writableGuard{}})
	if err != nil {
		t.Fatal(err)
	}
	request := AppendRequest{Namespace: "ha", Stream: "events", RequestID: []byte("one"), Producer: "test", Records: []InputRecord{{Payload: []byte("committed")}}}
	if _, err = store.Append(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	replica.partitioned = true
	request.ExpectedSequence = 1
	request.RequestID = []byte("two")
	request.Records[0].Payload = []byte("uncommitted")
	if _, err = store.Append(context.Background(), request); err == nil {
		t.Fatal("partitioned Strict Append succeeded")
	}
	read, readErr := store.Read("ha", "events", 0, 10, 0)
	if readErr != nil || len(read.Records) != 1 || string(read.Records[0].Payload) != "committed" {
		t.Fatalf("Read during partition = %+v, %v", read, readErr)
	}
	health := store.Health()
	if health.Fatal == nil || health.Watermarks.Committed >= health.Watermarks.LocalDurable {
		t.Fatalf("partition Health = %+v", health)
	}
	if closeErr := store.Close(); closeErr == nil {
		t.Fatal("failed Strict commit was absent from Close error")
	}
}
