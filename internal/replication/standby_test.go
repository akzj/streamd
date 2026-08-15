package replication

import (
	"context"
	"testing"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
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
	if err = primary.Close(); err != nil {
		t.Fatal(err)
	}
	if err = standby.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	hello, err := standby.Hello()
	if err != nil || !hello.Committed.Valid || hello.Committed.EntryID != 1 || hello.Applied.EntryID != 1 {
		t.Fatalf("Hello = %+v, error = %v", hello, err)
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
	if err != nil || hello.Committed.EntryID != 1 || hello.LocalDurable.EntryID != 1 {
		t.Fatalf("reopened Hello = %+v, error = %v", hello, err)
	}
	tail, ok := reopened.state.MemTable.Tail(1)
	if !ok || tail.NextSequence != 1 {
		t.Fatalf("reopened data Tail = %+v, ok = %v", tail, ok)
	}
}
