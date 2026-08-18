package replication

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/wal"
)

func TestStandbyStorePersistsStrictApplyAndReopens(t *testing.T) {
	standbyNode := format.NodeIdentity{ClusterID: uuid(9), GroupID: uuid(1), NodeID: uuid(3), CreatedAt: 1}
	primaryNode := format.NodeIdentity{ClusterID: uuid(9), GroupID: uuid(1), NodeID: uuid(2), CreatedAt: 1}
	standbyPath := t.TempDir()
	standby, err := OpenStandby(standbyPath, standbyNode, 1, primaryNode.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	primaryProtocol, err := NewPrimary(primaryNode.GroupID, primaryNode.NodeID, 1, ReceiverPeer{Receiver: standby.Receiver()})
	if err != nil {
		t.Fatal(err)
	}
	primary, err := engine.OpenReplicated(t.TempDir(), primaryNode, engine.ReplicationOptions{Term: 1, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: primaryProtocol, Guard: promotionGuard{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = primary.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("value")}}}); err != nil {
		t.Fatal(err)
	}
	if err = standby.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if records, bytes := standby.state.MemTable.Stats(); records != 0 || bytes != 0 {
		t.Fatalf("active MemTable retained checkpointed data: records=%d bytes=%d", records, bytes)
	}
	manifest, ok := standby.state.Manifest.Current()
	if !ok || manifest.Header.LastEntryID != 1 || len(manifest.SegmentReferences) != 1 {
		t.Fatalf("Standby Manifest = %+v, present = %v", manifest.Header, ok)
	}
	hello, err := standby.Hello()
	if err != nil || !hello.Committed.Valid || hello.Committed.EntryID != 1 || hello.Applied.EntryID != 1 {
		t.Fatalf("Hello = %+v, error = %v", hello, err)
	}
	if _, err = primary.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", ExpectedSequence: 1, RequestID: []byte("r2"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("value-2")}}}); err != nil {
		t.Fatal(err)
	}
	if tail, ok := standby.state.MemTable.Tail(1); !ok || tail.NextSequence != 2 {
		t.Fatalf("post-checkpoint active Tail = %+v, ok = %v", tail, ok)
	}
	if err = primary.Close(); err != nil {
		t.Fatal(err)
	}
	if err = standby.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStandby(standbyPath, standbyNode, 1, primaryNode.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	hello, err = reopened.Hello()
	if err != nil || hello.Committed.EntryID != 2 || hello.LocalDurable.EntryID != 2 {
		t.Fatalf("reopened Hello = %+v, error = %v", hello, err)
	}
	tail, ok, err := reopened.state.TailResolver.Lookup(1)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || tail.NextSequence != 2 {
		t.Fatalf("reopened data Tail = %+v, ok = %v", tail, ok)
	}
}

func TestStandbyApplyResolvesCheckpointTailOnDemand(t *testing.T) {
	standbyNode := format.NodeIdentity{ClusterID: uuid(9), GroupID: uuid(1), NodeID: uuid(3), CreatedAt: 1}
	primaryNode := format.NodeIdentity{ClusterID: uuid(9), GroupID: uuid(1), NodeID: uuid(2), CreatedAt: 1}
	root := t.TempDir()
	seed, err := engine.OpenWithIdentity(root, standbyNode)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = seed.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("one"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("one")}}}); err != nil {
		t.Fatal(err)
	}
	readResult, err := seed.Read("n", "s", 0, 1, 0)
	if err != nil || len(readResult.Records) != 1 {
		t.Fatalf("seed Read = %+v, %v", readResult, err)
	}
	firstFrame, err := format.MarshalRecordFrame(readResult.Records[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, created, checkpointErr := seed.Checkpoint(); checkpointErr != nil || !created {
		t.Fatalf("Checkpoint created=%v error=%v", created, checkpointErr)
	}
	if err = seed.Close(); err != nil {
		t.Fatal(err)
	}
	history, err := wal.OpenHistory(root)
	if err != nil {
		t.Fatal(err)
	}
	_, checkpointEntry, err := history.EntryAt(1)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := format.ReplicationPosition{Present: true, EntryID: checkpointEntry.EntryID, CRC32C: checkpointEntry.CRC32C}
	states, err := replicationstate.Open(root, standbyNode)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = states.Update(time.Unix(1, 0), func(header *format.ReplicationStateHeader) error {
		header.Term = 1
		header.Role = format.ReplicationRoleStandby
		header.Durability = format.ReplicationDurabilityStrict
		header.HasLeader = true
		header.LeaderID = primaryNode.NodeID
		header.LastAppended = checkpoint
		header.LocalDurable = checkpoint
		header.Committed = checkpoint
		header.Applied = checkpoint
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	standby, err := OpenStandby(root, standbyNode, 1, primaryNode.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	defer standby.Close()
	hash := sha256.Sum256([]byte("two"))
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: 2, StreamID: 1, Sequence: 1, ByteOffset: uint64(len(firstFrame)), RecordedAt: readResult.Records[0].RecordedAt + 1, BatchCount: 1, RequestHash: hash, RequestID: []byte("two"), Producer: "test", Payload: []byte("two")})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := format.MarshalWALEntry(1, checkpointEntry.CRC32C, frame)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := format.UnmarshalWALEntry(encoded)
	if err != nil {
		t.Fatal(err)
	}
	previous := Position{Valid: true, EntryID: checkpointEntry.EntryID, CRC32C: checkpointEntry.CRC32C}
	if err = standby.Receiver().Append(AppendEntries{GroupID: standbyNode.GroupID, Term: 1, LeaderID: primaryNode.NodeID, Previous: previous, Entries: [][]byte{encoded}}); err != nil {
		t.Fatal(err)
	}
	if _, err = standby.Receiver().Barrier(DurabilityBarrier{GroupID: standbyNode.GroupID, Term: 1, LeaderID: primaryNode.NodeID, ThroughEntryID: entry.EntryID}); err != nil {
		t.Fatal(err)
	}
	if err = standby.Receiver().AdvanceCommit(CommitAdvance{GroupID: standbyNode.GroupID, Term: 1, LeaderID: primaryNode.NodeID, CommitEntryID: entry.EntryID}); err != nil {
		t.Fatal(err)
	}
	tail, ok := standby.state.MemTable.Tail(1)
	if !ok || tail.NextSequence != 2 {
		t.Fatalf("Standby Tail = %+v, ok = %v", tail, ok)
	}
}

func TestStandbyCheckpointPreservesDurableUncommittedSuffix(t *testing.T) {
	standbyNode := format.NodeIdentity{ClusterID: uuid(9), GroupID: uuid(1), NodeID: uuid(3), CreatedAt: 1}
	primaryNode := format.NodeIdentity{ClusterID: uuid(9), GroupID: uuid(1), NodeID: uuid(2), CreatedAt: 1}
	standbyPath := t.TempDir()
	standby, err := OpenStandby(standbyPath, standbyNode, 1, primaryNode.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	primaryProtocol, err := NewPrimary(primaryNode.GroupID, primaryNode.NodeID, 1, ReceiverPeer{Receiver: standby.Receiver()})
	if err != nil {
		t.Fatal(err)
	}
	primary, err := engine.OpenReplicated(t.TempDir(), primaryNode, engine.ReplicationOptions{Term: 1, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: primaryProtocol, Guard: promotionGuard{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = primary.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r1"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("one")}}}); err != nil {
		t.Fatal(err)
	}
	hello, err := standby.Hello()
	if err != nil {
		t.Fatal(err)
	}
	tail, ok := standby.state.MemTable.Tail(1)
	if !ok {
		t.Fatal("committed data Stream has no Tail")
	}
	hash := sha256.Sum256([]byte("uncommitted"))
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: 2, StreamID: 1, Sequence: tail.NextSequence, ByteOffset: tail.NextByteOffset, RecordedAt: tail.LastRecordedAt + 1, BatchCount: 1, RequestHash: hash, RequestID: []byte("uncommitted"), Producer: "test", Payload: []byte("two")})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := format.MarshalWALEntry(1, hello.LocalDurable.CRC32C, frame)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := format.UnmarshalWALEntry(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err = standby.Receiver().Append(AppendEntries{GroupID: standbyNode.GroupID, Term: 1, LeaderID: primaryNode.NodeID, Previous: hello.LocalDurable, Entries: [][]byte{encoded}}); err != nil {
		t.Fatal(err)
	}
	if _, err = standby.Receiver().Barrier(DurabilityBarrier{GroupID: standbyNode.GroupID, Term: 1, LeaderID: primaryNode.NodeID, ThroughEntryID: entry.EntryID}); err != nil {
		t.Fatal(err)
	}
	if err = standby.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	manifest, ok := standby.state.Manifest.Current()
	if !ok || manifest.Header.LastEntryID != 1 {
		t.Fatalf("Manifest checkpoint = %+v, present = %v", manifest.Header, ok)
	}
	if err = primary.Close(); err != nil {
		t.Fatal(err)
	}
	if err = standby.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStandby(standbyPath, standbyNode, 1, primaryNode.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	hello, err = reopened.Hello()
	if err != nil || !hello.LocalDurable.Valid || hello.LocalDurable.EntryID != 2 || !hello.Committed.Valid || hello.Committed.EntryID != 1 || hello.Applied.EntryID != 1 {
		t.Fatalf("reopened Hello = %+v, error = %v", hello, err)
	}
}
