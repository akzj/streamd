package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
)

func TestFindAvailableRequiresVerifiedTransferablePackage(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data")
	node := format.NodeIdentity{ClusterID: snapshotID(1), GroupID: snapshotID(2), NodeID: snapshotID(3), CreatedAt: 1}
	store, err := engine.OpenWithIdentity(data, node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("available"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("record")}}}); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(data, "snapshots", "available")
	created, err := Create(data, path)
	if err != nil {
		t.Fatal(err)
	}
	found, ok, err := FindAvailable(data, created.SnapshotID, created.GroupID, created.CheckpointEntryID, created.CheckpointCRC32C)
	if err != nil || !ok || found.Path != path {
		t.Fatalf("available Snapshot = %+v, %v, %v", found, ok, err)
	}
	if err = os.Remove(filepath.Join(path, "CURRENT")); err != nil {
		t.Fatal(err)
	}
	if found, ok, err = FindAvailable(data, created.SnapshotID, created.GroupID, created.CheckpointEntryID, created.CheckpointCRC32C); err != nil || ok {
		t.Fatalf("incomplete Snapshot = %+v, %v, %v", found, ok, err)
	}
}
