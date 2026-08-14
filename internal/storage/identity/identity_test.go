package identity

import (
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
)

func TestEnsureCreatesAndRejectsIdentityChange(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	desired := format.NodeIdentity{ClusterID: testID(1), GroupID: testID(2), NodeID: testID(3), CreatedAt: 10}
	created, err := Ensure(root.Path(), desired)
	if err != nil || created != desired {
		t.Fatalf("created = %+v, error = %v", created, err)
	}
	desired.CreatedAt = 20
	loaded, err := Ensure(root.Path(), desired)
	if err != nil || loaded.CreatedAt != 10 {
		t.Fatalf("loaded = %+v, error = %v", loaded, err)
	}
	desired.NodeID = testID(4)
	if _, err = Ensure(root.Path(), desired); err == nil {
		t.Fatal("identity change was accepted")
	}
}

func testID(value byte) format.UUID {
	var id format.UUID
	id[15] = value
	return id
}
