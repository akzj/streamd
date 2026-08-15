package replicationstate

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
)

var errCrash = errors.New("crash")

func TestPublishReopenAndContinueGeneration(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	identity := testIdentity()
	store, err := Open(root.Path(), identity)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Publish(testState(identity, 0, [32]byte{}))
	if err != nil {
		t.Fatal(err)
	}
	secondInput := testState(identity, 1, first.Footer.ContentSHA256)
	secondInput.Header.Term = 2
	second, err := store.Publish(secondInput)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(root.Path(), identity)
	if err != nil {
		t.Fatal(err)
	}
	current, ok := reopened.Current()
	if !ok || current.Header.StateID != second.Header.StateID || current.Header.Generation != 1 {
		t.Fatalf("current = %+v, ok = %v", current, ok)
	}
	files, err := filepath.Glob(filepath.Join(root.Path(), "meta", "REPLICATION-STATE-*.bin"))
	if err != nil || len(files) != 2 {
		t.Fatalf("State files = %v, error = %v", files, err)
	}
}

func TestPublishRejectsBrokenTransition(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	identity := testIdentity()
	store, err := Open(root.Path(), identity)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Publish(testState(identity, 0, [32]byte{}))
	if err != nil {
		t.Fatal(err)
	}
	bad := testState(identity, 2, first.Footer.ContentSHA256)
	bad.Header.PreviousGeneration = 0
	if _, err = store.Publish(bad); err == nil {
		t.Fatal("generation gap accepted")
	}
	bad = testState(identity, 1, first.Footer.ContentSHA256)
	bad.Header.Term = 0
	if _, err = store.Publish(bad); err == nil {
		t.Fatal("Term regression accepted")
	}
	bad = testState(identity, 1, first.Footer.ContentSHA256)
	bad.Header.Committed.EntryID = 0
	bad.Header.Committed.CRC32C = 100
	bad.Header.Applied = bad.Header.Committed
	bad.Header.LocalDurable = bad.Header.Committed
	bad.Header.LastAppended = bad.Header.Committed
	if _, err = store.Publish(bad); err == nil {
		t.Fatal("Commit regression accepted")
	}
}

func TestStatePublishBeforePointerLeavesOrphan(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	identity := testIdentity()
	store, err := Open(root.Path(), identity)
	if err != nil {
		t.Fatal(err)
	}
	store.hook = func(point string) error {
		if point == "after_state_publish" {
			return errCrash
		}
		return nil
	}
	if _, err = store.Publish(testState(identity, 0, [32]byte{})); !errors.Is(err, errCrash) {
		t.Fatalf("Publish error = %v", err)
	}
	if _, err = os.Stat(filepath.Join(root.Path(), "REPLICATION-CURRENT")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Pointer exists after crash: %v", err)
	}
	reopened, err := Open(root.Path(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reopened.Current(); ok {
		t.Fatal("orphan State became current")
	}
}

func TestOpenRejectsPointerIdentityMismatch(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	identity := testIdentity()
	store, err := Open(root.Path(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Publish(testState(identity, 0, [32]byte{})); err != nil {
		t.Fatal(err)
	}
	other := identity
	other.NodeID = id(9)
	if _, err = Open(root.Path(), other); err == nil {
		t.Fatal("foreign NODE accepted Replication State")
	}
}

func testIdentity() format.NodeIdentity {
	return format.NodeIdentity{ClusterID: id(1), GroupID: id(2), NodeID: id(3), CreatedAt: 1}
}

func testState(identity format.NodeIdentity, generation uint64, previous [sha256.Size]byte) format.ReplicationState {
	position := format.ReplicationPosition{Present: true, EntryID: 1, CRC32C: 101}
	return format.ReplicationState{Header: format.ReplicationStateHeader{
		StateID:             id(byte(4 + generation)),
		Generation:          generation,
		PreviousGeneration:  max(generation, 1) - 1,
		PreviousStateSHA256: previous,
		GroupID:             identity.GroupID,
		NodeID:              identity.NodeID,
		Term:                1,
		Role:                format.ReplicationRolePrimary,
		Durability:          format.ReplicationDurabilityStrict,
		HasLeader:           true,
		LeaderID:            identity.NodeID,
		HasLease:            true,
		LeaseExpiresAt:      100,
		LastAppended:        position,
		LocalDurable:        position,
		Replicated:          position,
		Committed:           position,
		Applied:             position,
		CreatedAt:           int64(generation + 1),
	}}
}

func id(value byte) format.UUID {
	var result format.UUID
	result[15] = value
	return result
}
