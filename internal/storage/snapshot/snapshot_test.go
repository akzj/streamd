package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/scrub"
)

func TestCreateVerifyAndScrubSnapshot(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data")
	node := format.NodeIdentity{ClusterID: snapshotID(1), GroupID: snapshotID(2), NodeID: snapshotID(3), CreatedAt: 1}
	store, err := engine.OpenWithIdentity(data, node)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("record")}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "snapshot")
	created, err := Create(data, destination)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := Verify(destination)
	if err != nil {
		t.Fatal(err)
	}
	if verified.SnapshotID != created.SnapshotID || verified.Artifacts != 2 {
		t.Fatalf("created = %+v, verified = %+v", created, verified)
	}
	report, err := scrub.DataRoot(data)
	if err != nil {
		t.Fatal(err)
	}
	if report.Segments != 1 || report.Records != 2 {
		t.Fatalf("scrub report = %+v", report)
	}
	segments, err := filepath.Glob(filepath.Join(destination, "segments", "*.seg"))
	if err != nil || len(segments) != 1 {
		t.Fatalf("segments = %v, %v", segments, err)
	}
	file, err := os.OpenFile(segments[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, 1); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if _, err = Verify(destination); err == nil {
		t.Fatal("corrupt Snapshot passed verification")
	}
}

func snapshotID(value byte) format.UUID {
	var id format.UUID
	id[15] = value
	return id
}
