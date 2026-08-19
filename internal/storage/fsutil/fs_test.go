package fsutil

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type shortWriterAt struct {
	writes int
	data   []byte
}

func (w *shortWriterAt) WriteAt(data []byte, offset int64) (int, error) {
	w.writes++
	if len(data) == 0 {
		return 0, nil
	}
	n := len(data) / 2
	if n == 0 {
		n = 1
	}
	end := int(offset) + n
	if end > len(w.data) {
		w.data = append(w.data, make([]byte, end-len(w.data))...)
	}
	copy(w.data[int(offset):end], data[:n])
	return n, nil
}

type stalledWriterAt struct{}

func (stalledWriterAt) WriteAt([]byte, int64) (int, error) { return 0, nil }

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

func TestWriteFullAtCompletesShortWrites(t *testing.T) {
	writer := &shortWriterAt{}
	want := []byte("short writes must not publish torn artifacts")
	if err := WriteFullAt(writer, want, 0); err != nil {
		t.Fatal(err)
	}
	if string(writer.data) != string(want) || writer.writes < 2 {
		t.Fatalf("writes = %d, data = %q", writer.writes, writer.data)
	}
	if err := WriteFullAt(stalledWriterAt{}, want, 0); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("stalled writer error = %v", err)
	}
}
