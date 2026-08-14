package fsutil

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRootLockAndAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	root, err := OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := OpenRoot(dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("second lock: %v", err)
	}
	if err := AtomicWrite(root.Path(), "CURRENT", []byte("value"), 0640, nil); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil || string(got) != "value" {
		t.Fatalf("read: %q %v", got, err)
	}
}
func TestAtomicWriteCrashBeforeRename(t *testing.T) {
	dir := t.TempDir()
	stop := errors.New("crash")
	err := AtomicWrite(dir, "CURRENT", []byte("new"), 0640, func(point string) error {
		if point == "after_file_sync" {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("got %v", err)
	}
	if _, err = os.Stat(filepath.Join(dir, "CURRENT")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file became visible: %v", err)
	}
}
func TestAtomicWriteRejectsUnsafeTarget(t *testing.T) {
	if err := AtomicWrite(t.TempDir(), "../NODE", nil, 0640, nil); err == nil {
		t.Fatal("unsafe name accepted")
	}
	if err := AtomicWrite(t.TempDir(), "NODE", nil, 0644, nil); err == nil {
		t.Fatal("unsafe permissions accepted")
	}
}
