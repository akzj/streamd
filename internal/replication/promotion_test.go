package replication

import (
	"context"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/wal"
)

type promotionGuard struct{}

func (promotionGuard) CanCommit() error { return nil }

type promotionReplica struct{}

func (promotionReplica) Replicate(_ context.Context, encoded [][]byte) (uint64, error) {
	entry, err := format.UnmarshalWALEntry(encoded[len(encoded)-1])
	return entry.EntryID, err
}

func (promotionReplica) AdvanceCommit(context.Context, uint64) error { return nil }

func TestPromoteValidatesAndCommitsDurableSuffix(t *testing.T) {
	root := t.TempDir()
	node := format.NodeIdentity{ClusterID: uuid(9), GroupID: uuid(1), NodeID: uuid(3), CreatedAt: 1}
	store, err := engine.OpenWithIdentity(root, node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("value")}}}); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	history, err := wal.OpenHistory(root)
	if err != nil {
		t.Fatal(err)
	}
	_, committedEntry, err := history.EntryAt(0)
	if err != nil {
		t.Fatal(err)
	}
	_, tailEntry, err := history.EntryAt(1)
	if err != nil {
		t.Fatal(err)
	}
	stateStore, err := replicationstate.Open(root, node)
	if err != nil {
		t.Fatal(err)
	}
	committed := format.ReplicationPosition{Present: true, EntryID: 0, CRC32C: committedEntry.CRC32C}
	tail := format.ReplicationPosition{Present: true, EntryID: 1, CRC32C: tailEntry.CRC32C}
	_, err = stateStore.Update(time.Unix(1, 0), func(header *format.ReplicationStateHeader) error {
		header.Term = 1
		header.Role = format.ReplicationRoleStandby
		header.Durability = format.ReplicationDurabilityStrict
		header.HasLeader = true
		header.LeaderID = uuid(2)
		header.LastAppended = tail
		header.LocalDurable = tail
		header.Committed = committed
		header.Applied = committed
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(10, 0)
	result, err := Promote(root, node, PromotionGrant{Term: 2, LeaderID: node.NodeID, ExpiresAt: now.Add(time.Minute), Fenced: true, SafetyMargin: 5 * time.Second, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if result.SuffixEntries != 1 || !result.Committed.Valid || result.Committed.EntryID != 1 {
		t.Fatalf("Promotion = %+v", result)
	}
	stateStore, err = replicationstate.Open(root, node)
	if err != nil {
		t.Fatal(err)
	}
	current, ok := stateStore.Current()
	if !ok || current.Header.Role != format.ReplicationRolePrimary || current.Header.Term != 2 || current.Header.Committed != tail || current.Header.Replicated != tail {
		t.Fatalf("Promoted State = %+v, ok = %v", current.Header, ok)
	}
	reopened, err := engine.OpenReplicated(root, node, engine.ReplicationOptions{Term: 2, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: promotionReplica{}, Guard: promotionGuard{}})
	if err != nil {
		t.Fatal(err)
	}
	health := reopened.Health()
	if !health.Watermarks.HasCommitted || health.Watermarks.Committed != 1 || !health.Watermarks.HasReplicated {
		t.Fatalf("recovered Health = %+v", health)
	}
	if _, err = reopened.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", ExpectedSequence: 1, RequestID: []byte("after-promotion"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("next")}}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = reopened.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if afterCheckpoint := reopened.Health().Watermarks; !afterCheckpoint.HasCommitted || afterCheckpoint.Committed != 2 || !afterCheckpoint.HasReplicated {
		t.Fatalf("watermarks regressed across Checkpoint: %+v", afterCheckpoint)
	}
	checkpoint, err := reopened.CheckpointReplicationState(stateStore)
	if err != nil || checkpoint.Header.Committed.EntryID != 2 || checkpoint.Header.Replicated.EntryID != 2 {
		t.Fatalf("Replication checkpoint = %+v, %v", checkpoint.Header, err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPromoteRejectsUnsafeLease(t *testing.T) {
	now := time.Unix(10, 0)
	_, err := Promote(t.TempDir(), format.NodeIdentity{}, PromotionGrant{Term: 1, ExpiresAt: now.Add(time.Minute), SafetyMargin: time.Second, Now: func() time.Time { return now }})
	if !IsCode(err, ErrInvalidState) {
		t.Fatalf("unsafe Promotion error = %v", err)
	}
}

func TestResolveRejoinUsesIncrementalSnapshotAndFailsOnCommittedConflict(t *testing.T) {
	checksums := map[uint64]uint32{0: 100, 1: 101, 2: 102}
	lookup := func(id uint64) (uint32, bool) { value, ok := checksums[id]; return value, ok }
	leader := RejoinView{GroupID: uuid(1), NodeID: uuid(2), Term: 3, EarliestWAL: 0, LastDurable: position(2, 102), Committed: position(2, 102), ChecksumAt: lookup}
	local := RejoinView{GroupID: uuid(1), NodeID: uuid(3), Term: 2, LastDurable: position(1, 101), Committed: position(0, 100), ChecksumAt: lookup}
	decision, err := ResolveRejoin(local, leader)
	if err != nil || decision.Mode != RejoinIncremental || decision.StartEntryID != 2 {
		t.Fatalf("incremental = %+v, %v", decision, err)
	}
	local.LastDurable = position(1, 999)
	decision, err = ResolveRejoin(local, leader)
	if err != nil || decision.Mode != RejoinSnapshot || !decision.DiscardLocalSuffix {
		t.Fatalf("Snapshot = %+v, %v", decision, err)
	}
	local.Committed = position(0, 999)
	if _, err = ResolveRejoin(local, leader); !IsCode(err, ErrLogDiverged) {
		t.Fatalf("committed conflict error = %v", err)
	}
}
