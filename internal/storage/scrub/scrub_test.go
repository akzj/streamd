package scrub_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/retention"
	"github.com/akzj/streamd/internal/storage/scrub"
)

func TestDataRootAcceptsManifestCheckpointCoveredByCollectedWALPrefix(t *testing.T) {
	root := t.TempDir()
	identity := format.NodeIdentity{ClusterID: scrubID(1), GroupID: scrubID(2), NodeID: scrubID(3), CreatedAt: 1}
	store, err := engine.OpenWithIdentity(root, identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("record")}}}); err != nil {
		store.Close()
		t.Fatal(err)
	}
	result, err := retention.CreateSnapshotAndCollect(store, nil, filepath.Join(root, "snapshots", "checkpoint"), 1<<30, time.Unix(200, 0))
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	if len(result.Collection.DeletedFiles) == 0 {
		store.Close()
		t.Fatal("online retention did not collect the checkpoint WAL")
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = scrub.DataRoot(root); err != nil {
		t.Fatal(err)
	}
}

func scrubID(value byte) (id format.UUID) {
	for i := range id {
		id[i] = value
	}
	return id
}
