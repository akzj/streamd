package manifest

import (
	"crypto/sha256"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"path/filepath"
	"testing"
)

func id(value byte) format.UUID { var id format.UUID; id[15] = value; return id }
func TestPublishAndReopenGenerationChain(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	store, err := Open(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Publish(format.Manifest{Header: format.ManifestHeader{FileID: id(1)}})
	if err != nil {
		t.Fatal(err)
	}
	previous := first.Footer.ContentSHA256
	second, err := store.Publish(format.Manifest{Header: format.ManifestHeader{FileID: id(2), Generation: 1, PreviousGeneration: 0, PreviousManifestSHA256: previous}})
	if err != nil {
		t.Fatal(err)
	}
	if second.Header.Generation != 1 {
		t.Fatalf("second %+v", second.Header)
	}
	reopened, err := Open(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	current, ok := reopened.Current()
	if !ok || current.Header.FileID != id(2) {
		t.Fatalf("current %+v %v", current, ok)
	}
	matches, err := filepath.Glob(filepath.Join(root.Path(), "manifests", "MANIFEST-*.bin"))
	if err != nil || len(matches) != 2 {
		t.Fatalf("files %v %v", matches, err)
	}
}
func TestRejectsBrokenGenerationChain(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	store, err := Open(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Publish(format.Manifest{Header: format.ManifestHeader{FileID: id(1)}}); err != nil {
		t.Fatal(err)
	}
	bad := sha256.Sum256([]byte("bad"))
	if _, err = store.Publish(format.Manifest{Header: format.ManifestHeader{FileID: id(2), Generation: 1, PreviousManifestSHA256: bad}}); err == nil {
		t.Fatal("broken chain accepted")
	}
}
