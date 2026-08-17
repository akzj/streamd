// Package retention coordinates WAL collection with immutable Segment,
// Snapshot, replication-state, and replica-progress evidence.
package retention

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/identity"
	manifeststore "github.com/akzj/streamd/internal/storage/manifest"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/snapshot"
	"github.com/akzj/streamd/internal/storage/wal"
)

var ErrClosed = errors.New("retention manager is closed")

type Manager struct {
	mu       sync.Mutex
	rootLock *fsutil.Root
	root     string
	identity format.NodeIdentity
	history  *wal.History
	manifest *manifeststore.Store
	state    *replicationstate.Store
	now      func() time.Time
	closed   bool
}

func Open(root string) (*Manager, error) {
	rootLock, err := fsutil.LockExistingRoot(root)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Manager, error) {
		return nil, errors.Join(err, rootLock.Close())
	}
	abs := rootLock.Path()
	node, err := identity.Load(abs)
	if err != nil {
		return fail(fmt.Errorf("NODE: %w", err))
	}
	history, err := wal.OpenHistory(abs)
	if err != nil {
		return fail(err)
	}
	manifest, err := manifeststore.Open(abs)
	if err != nil {
		return fail(err)
	}
	state, err := replicationstate.Open(abs, node)
	if err != nil {
		return fail(err)
	}
	return &Manager{rootLock: rootLock, root: abs, identity: node, history: history, manifest: manifest, state: state, now: time.Now}, nil
}

func (m *Manager) Collect(snapshotPath string, maxRetainedBytes uint64) (wal.GCResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return wal.GCResult{}, ErrClosed
	}
	absSnapshot, err := filepath.Abs(snapshotPath)
	if err != nil {
		return wal.GCResult{}, err
	}
	if filepath.Dir(absSnapshot) != filepath.Join(m.root, "snapshots") {
		return wal.GCResult{}, fmt.Errorf("WAL GC Snapshot must be pinned inside the data root snapshots directory")
	}
	verified, err := snapshot.Verify(absSnapshot)
	if err != nil {
		return wal.GCResult{}, err
	}
	if verified.GroupID != m.identity.GroupID {
		return wal.GCResult{}, fmt.Errorf("WAL GC Snapshot belongs to another group")
	}
	manifest, ok := m.manifest.Current()
	if !ok || manifest.Header.RecordCount == 0 {
		return wal.GCResult{}, fmt.Errorf("WAL GC requires a published non-empty Manifest")
	}
	state, ok := m.state.Current()
	if !ok || !state.Header.Committed.Present || verified.CheckpointEntryID > state.Header.Committed.EntryID {
		return wal.GCResult{}, fmt.Errorf("WAL GC Snapshot is not covered by durable replication state")
	}
	replica := wal.HistoryPosition{Present: state.Header.Replicated.Present, EntryID: state.Header.Replicated.EntryID, CRC32C: state.Header.Replicated.CRC32C}
	result, collectErr := m.history.Collect(wal.GCOptions{SegmentedThrough: manifest.Header.LastEntryID, SnapshotThrough: verified.CheckpointEntryID, SnapshotVerified: true, ReplicaDurable: replica, MaxRetainedBytes: maxRetainedBytes})
	if collectErr != nil && !errors.Is(collectErr, wal.ErrRetentionPressure) {
		return result, collectErr
	}
	_, stateErr := m.state.Update(m.now(), func(header *format.ReplicationStateHeader) error {
		header.EarliestWALEntryID = result.EarliestWAL
		checkpoint := format.ReplicationPosition{Present: true, EntryID: verified.CheckpointEntryID, CRC32C: verified.CheckpointCRC32C}
		if !header.HasInstalledSnapshot || verified.CheckpointEntryID >= header.InstalledSnapshotEntry.EntryID {
			header.HasInstalledSnapshot = true
			header.InstalledSnapshotID = verified.SnapshotID
			header.InstalledSnapshotEntry = checkpoint
		}
		return nil
	})
	return result, errors.Join(collectErr, stateErr)
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	return m.rootLock.Close()
}
