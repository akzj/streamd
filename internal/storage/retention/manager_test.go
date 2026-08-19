package retention

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/snapshot"
)

type retentionGuard struct{}

func (retentionGuard) CanCommit() error { return nil }

type retentionReplica struct{}

func (retentionReplica) Replicate(_ context.Context, encoded [][]byte) (uint64, error) {
	entry, err := format.UnmarshalWALEntry(encoded[len(encoded)-1])
	return entry.EntryID, err
}

func (retentionReplica) AdvanceCommit(context.Context, uint64) error { return nil }

func TestOnlineReplicatedRetentionCanAdvanceSnapshotAfterWALCollection(t *testing.T) {
	root := t.TempDir()
	identity := format.NodeIdentity{ClusterID: retentionID(1), GroupID: retentionID(2), NodeID: retentionID(3), CreatedAt: 1}
	store, err := engine.OpenReplicated(root, identity, engine.ReplicationOptions{Term: 7, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: retentionReplica{}, Guard: retentionGuard{}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	states, err := replicationstate.Open(root, identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = states.Update(time.Unix(100, 0), func(header *format.ReplicationStateHeader) error {
		header.Term = 7
		header.Role = format.ReplicationRolePrimary
		header.Durability = format.ReplicationDurabilityStrict
		header.HasLeader = true
		header.LeaderID = identity.NodeID
		header.HasLease = true
		header.LeaseExpiresAt = time.Unix(100, 0).Add(time.Hour).UnixNano()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r1"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("one")}}}); err != nil {
		t.Fatal(err)
	}
	first, err := CreateSnapshotAndCollect(store, states, filepath.Join(root, "snapshots", "first"), 1<<30, time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", ExpectedSequence: 1, RequestID: []byte("r2"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("two")}}}); err != nil {
		t.Fatal(err)
	}
	second, err := CreateSnapshotAndCollect(store, states, filepath.Join(root, "snapshots", "second"), 1<<30, time.Unix(300, 0))
	if err != nil {
		t.Fatal(err)
	}
	if second.Snapshot.CheckpointEntryID <= first.Snapshot.CheckpointEntryID || second.Snapshot.SnapshotID == first.Snapshot.SnapshotID {
		t.Fatalf("Snapshots did not advance: first=%+v second=%+v", first.Snapshot, second.Snapshot)
	}
	current, ok := states.Current()
	if !ok || !current.Header.HasInstalledSnapshot || current.Header.InstalledSnapshotID != second.Snapshot.SnapshotID || current.Header.InstalledSnapshotEntry.EntryID != second.Snapshot.CheckpointEntryID || current.Header.EarliestWALEntryID != second.Collection.EarliestWAL {
		t.Fatalf("Replication State did not retain the second Snapshot recovery floor: %+v, ok=%v", current.Header, ok)
	}
}

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
	manager, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
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

func TestManagerRequiresExclusiveDataRootAndCloseReleasesIt(t *testing.T) {
	root := t.TempDir()
	node := format.NodeIdentity{ClusterID: retentionID(1), GroupID: retentionID(2), NodeID: retentionID(3), CreatedAt: 1}
	store, err := engine.OpenWithIdentity(root, node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Open(root); !errors.Is(err, fsutil.ErrLocked) {
		store.Close()
		t.Fatalf("Retention Open with live Store error = %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}

	manager, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.OpenWithIdentity(root, node); !errors.Is(err, fsutil.ErrLocked) {
		manager.Close()
		t.Fatalf("Store Open with live Retention Manager error = %v", err)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = manager.Collect(root+"/snapshots/missing", 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("Collect after Close error = %v", err)
	}

	reopened, err := engine.OpenWithIdentity(root, node)
	if err != nil {
		t.Fatal(err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func retentionID(value byte) format.UUID {
	var id format.UUID
	id[15] = value
	return id
}
