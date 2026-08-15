// Package replicationstate publishes immutable replication-state checkpoints.
package replicationstate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
)

type Store struct {
	mu       sync.RWMutex
	root     string
	groupID  format.UUID
	nodeID   format.UUID
	current  *format.ReplicationState
	fileName string
	hook     fsutil.CrashHook
}

func Open(root string, identity format.NodeIdentity) (*Store, error) {
	if _, err := format.MarshalNodeIdentity(identity); err != nil {
		return nil, fmt.Errorf("invalid NODE identity: %w", err)
	}
	store := &Store{root: root, groupID: identity.GroupID, nodeID: identity.NodeID}
	pointerBytes, err := os.ReadFile(filepath.Join(root, "REPLICATION-CURRENT"))
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	pointer, err := format.UnmarshalReplicationCurrent(pointerBytes)
	if err != nil {
		return nil, err
	}
	stateBytes, err := os.ReadFile(filepath.Join(root, "meta", pointer.StateFileName))
	if err != nil {
		return nil, err
	}
	state, err := format.UnmarshalReplicationState(stateBytes)
	if err != nil {
		return nil, err
	}
	if state.Header.Generation != pointer.Generation || state.Header.StateID != pointer.StateID || state.Footer.ContentSHA256 != pointer.StateSHA256 {
		return nil, fmt.Errorf("REPLICATION-CURRENT does not match Replication State")
	}
	if state.Header.GroupID != identity.GroupID || state.Header.NodeID != identity.NodeID {
		return nil, fmt.Errorf("Replication State does not belong to NODE")
	}
	store.current = &state
	store.fileName = pointer.StateFileName
	return store, nil
}

func (s *Store) Current() (format.ReplicationState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.current == nil {
		return format.ReplicationState{}, false
	}
	return *s.current, true
}

func (s *Store) Publish(next format.ReplicationState) (format.ReplicationState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if next.Header.GroupID != s.groupID || next.Header.NodeID != s.nodeID {
		return format.ReplicationState{}, fmt.Errorf("Replication State identity does not match NODE")
	}
	if err := s.validateTransition(next); err != nil {
		return format.ReplicationState{}, err
	}
	encoded, err := format.MarshalReplicationState(next)
	if err != nil {
		return format.ReplicationState{}, err
	}
	verified, err := format.UnmarshalReplicationState(encoded)
	if err != nil {
		return format.ReplicationState{}, err
	}
	name := format.ReplicationStateFileName(verified.Header.Generation, verified.Header.StateID)
	stateHook := prefixHook(s.hook, "state_")
	if err = fsutil.AtomicWrite(filepath.Join(s.root, "meta"), name, encoded, 0640, stateHook); err != nil {
		return format.ReplicationState{}, err
	}
	if s.hook != nil {
		if err = s.hook("after_state_publish"); err != nil {
			return format.ReplicationState{}, err
		}
	}
	pointer := format.ReplicationCurrent{Generation: verified.Header.Generation, StateID: verified.Header.StateID, StateSHA256: verified.Footer.ContentSHA256, StateFileName: name}
	pointerBytes, err := format.MarshalReplicationCurrent(pointer)
	if err != nil {
		return format.ReplicationState{}, err
	}
	if err = fsutil.AtomicWrite(s.root, "REPLICATION-CURRENT", pointerBytes, 0640, prefixHook(s.hook, "current_")); err != nil {
		return format.ReplicationState{}, err
	}
	s.current = &verified
	s.fileName = name
	return verified, nil
}

func (s *Store) validateTransition(next format.ReplicationState) error {
	if s.current == nil {
		if next.Header.Generation != 0 || next.Header.PreviousGeneration != 0 || next.Header.PreviousStateSHA256 != [32]byte{} {
			return fmt.Errorf("initial Replication State must be Generation 0")
		}
		return nil
	}
	current := s.current.Header
	want := current.Generation + 1
	if next.Header.Generation != want || next.Header.PreviousGeneration != current.Generation || next.Header.PreviousStateSHA256 != s.current.Footer.ContentSHA256 {
		return fmt.Errorf("Replication State does not continue current Generation")
	}
	if next.Header.Term < current.Term {
		return fmt.Errorf("Replication State Term regressed from %d to %d", current.Term, next.Header.Term)
	}
	if err := monotonicPosition("committed", current.Committed, next.Header.Committed); err != nil {
		return err
	}
	if err := monotonicPosition("applied", current.Applied, next.Header.Applied); err != nil {
		return err
	}
	if err := monotonicPosition("installed Snapshot", current.InstalledSnapshotEntry, next.Header.InstalledSnapshotEntry); current.HasInstalledSnapshot && err != nil {
		return err
	}
	if next.Header.EarliestWALEntryID < current.EarliestWALEntryID {
		return fmt.Errorf("Replication State earliest WAL Entry regressed")
	}
	return nil
}

func monotonicPosition(name string, current, next format.ReplicationPosition) error {
	if !current.Present {
		return nil
	}
	if !next.Present || next.EntryID < current.EntryID {
		return fmt.Errorf("Replication State %s position regressed", name)
	}
	if next.EntryID == current.EntryID && next.CRC32C != current.CRC32C {
		return fmt.Errorf("Replication State %s checksum changed", name)
	}
	return nil
}

func prefixHook(hook fsutil.CrashHook, prefix string) fsutil.CrashHook {
	if hook == nil {
		return nil
	}
	return func(point string) error { return hook(prefix + point) }
}
