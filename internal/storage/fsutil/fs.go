// Package fsutil provides the durability primitives used by storage publishers.
package fsutil

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

var ErrLocked = errors.New("streamd data directory is locked")

type CrashHook func(point string) error

type Root struct {
	path string
	lock *os.File
}

func OpenRoot(path string) (*Root, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(abs, 0750); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(abs, "LOCK"), os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("%w: %s", ErrLocked, abs)
	}
	for _, name := range []string{"wal", "segments", "manifests", "catalog", "locator", "registry", "snapshots", "meta", "staging", "trash"} {
		if err = os.MkdirAll(filepath.Join(abs, name), 0750); err != nil {
			lock.Close()
			return nil, err
		}
	}
	if err = SyncDir(abs); err != nil {
		lock.Close()
		return nil, err
	}
	return &Root{path: abs, lock: lock}, nil
}

// LockExistingRoot acquires the data-root lock without creating directories or
// synchronizing metadata. It is intended for read-only offline verification.
func LockExistingRoot(path string) (*Root, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("data root is not a directory: %s", abs)
	}
	lock, err := os.OpenFile(filepath.Join(abs, "LOCK"), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("%w: %s", ErrLocked, abs)
	}
	return &Root{path: abs, lock: lock}, nil
}
func (r *Root) Path() string { return r.path }
func (r *Root) Close() error {
	if r == nil || r.lock == nil {
		return nil
	}
	err1 := syscall.Flock(int(r.lock.Fd()), syscall.LOCK_UN)
	err2 := r.lock.Close()
	r.lock = nil
	return errors.Join(err1, err2)
}

func AtomicWrite(dir, name string, data []byte, mode fs.FileMode, hook CrashHook) error {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name || strings.ContainsAny(name, "/\\") {
		return fmt.Errorf("invalid atomic file name %q", name)
	}
	if mode.Perm()&0007 != 0 {
		return fmt.Errorf("atomic file mode is accessible to other users: %04o", mode.Perm())
	}
	tmp, err := os.CreateTemp(dir, "."+name+".tmp-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	published := false
	defer func() {
		tmp.Close()
		if !published {
			_ = os.Remove(tmpName)
		}
	}()
	if err = tmp.Chmod(mode); err != nil {
		return err
	}
	if err = WriteFull(tmp, data); err != nil {
		return err
	}
	if hook != nil {
		if err = hook("after_write"); err != nil {
			return err
		}
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if hook != nil {
		if err = hook("after_file_sync"); err != nil {
			return err
		}
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpName, filepath.Join(dir, name)); err != nil {
		return err
	}
	published = true
	if hook != nil {
		if err = hook("after_rename"); err != nil {
			return err
		}
	}
	return SyncDir(dir)
}

// WriteFull writes the complete buffer or returns an error.
func WriteFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if n < 0 || n > len(data) {
			return fmt.Errorf("invalid Write count %d for %d bytes", n, len(data))
		}
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

// WriteFullAt writes the complete buffer or returns an error. Regular files
// normally report an error with a short write, but this explicit loop keeps
// artifact publishers correct under injected faults and unusual filesystems.
func WriteFullAt(writer interface {
	WriteAt([]byte, int64) (int, error)
}, data []byte, offset int64) error {
	for len(data) > 0 {
		n, err := writer.WriteAt(data, offset)
		if n < 0 || n > len(data) {
			return fmt.Errorf("invalid WriteAt count %d for %d bytes", n, len(data))
		}
		if n > 0 {
			data = data[n:]
			offset += int64(n)
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}
func SyncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
