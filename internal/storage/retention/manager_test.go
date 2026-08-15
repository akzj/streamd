package retention

import (
	"context"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/snapshot"
)

func TestManagerCollectsOnlyFromPinnedVerifiedSnapshot(t *testing.T) {
	root := t.TempDir()
	identity := format.NodeIdentity{ClusterID: retentionID(1), GroupID: retentionID(2), NodeID: retentionID(3), CreatedAt: 1}
	store, err := engine.OpenWithIdentity(root, identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("record")}}}); err != nil {
		t.Fatal(err)
	}
	snapshotPath := root + "/snapshots/checkpoint"
	verified, err := snapshot.CreateOnline(store, snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	stateStore, err := replicationstate.Open(root, identity)
	if err != nil {
		t.Fatal(err)
	}
	position := format.ReplicationPosition{Present: true, EntryID: verified.CheckpointEntryID, CRC32C: verified.CheckpointCRC32C}
	_, err = stateStore.Update(time.Now(), func(header *format.ReplicationStateHeader) error {
		header.Term = 1
		header.Role = format.ReplicationRolePrimary
		header.Durability = format.ReplicationDurabilityStrict
		header.HasLeader = true
		header.LeaderID = identity.NodeID
		header.HasLease = true
		header.LeaseExpiresAt = time.Now().Add(time.Minute).UnixNano()
		header.LastAppended = position
		header.LocalDurable = position
		header.Replicated = position
		header.Committed = position
		header.Applied = position
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := Open(root, identity)
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.Collect(snapshotPath, 0)
	if err != nil || len(result.DeletedFiles) == 0 || result.EarliestWAL != verified.CheckpointEntryID+1 {
		t.Fatalf("GC = %+v, %v", result, err)
	}
	current, ok := stateStore.Current()
	if !ok {
		stateStore, err = replicationstate.Open(root, identity)
		if err != nil {
			t.Fatal(err)
		}
		current, ok = stateStore.Current()
	}
	// Reopen because Manager owns another Store instance and published the next generation.
	stateStore, err = replicationstate.Open(root, identity)
	if err != nil {
		t.Fatal(err)
	}
	current, ok = stateStore.Current()
	if !ok || current.Header.EarliestWALEntryID != result.EarliestWAL || !current.Header.HasInstalledSnapshot || current.Header.InstalledSnapshotID != verified.SnapshotID {
		t.Fatalf("Replication State = %+v, ok = %v", current.Header, ok)
	}
}

func retentionID(value byte) format.UUID {
	var id format.UUID
	id[15] = value
	return id
}
