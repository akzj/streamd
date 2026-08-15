package leadership

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/replicationstate"
)

type fakeCoordinator struct {
	acquire  LeaseGrant
	renew    LeaseGrant
	renewErr error
	released bool
}

func (f *fakeCoordinator) Acquire(context.Context, format.UUID, format.UUID) (LeaseGrant, error) {
	return f.acquire, nil
}

func (f *fakeCoordinator) Renew(context.Context, format.UUID, format.UUID, uint64) (LeaseGrant, error) {
	return f.renew, f.renewErr
}

func (f *fakeCoordinator) Release(context.Context, format.UUID, format.UUID, uint64) error {
	f.released = true
	return nil
}

func TestControllerRequiresDurableFencedLease(t *testing.T) {
	now := time.Unix(100, 0)
	coordinator := &fakeCoordinator{acquire: LeaseGrant{Term: 4, LeaderID: testID(2), ExpiresAt: now.Add(time.Minute), Fenced: true}}
	var persisted []State
	controller, err := New(coordinator, Options{GroupID: testID(1), NodeID: testID(2), KnownTerm: 3, SafetyMargin: 5 * time.Second, Now: func() time.Time { return now }, Persist: func(state State) error {
		persisted = append(persisted, state)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(controller.CanWrite(), ErrNotWritable) {
		t.Fatal("recovering node accepted writes")
	}
	if err = controller.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = controller.CanWrite(); err != nil {
		t.Fatal(err)
	}
	if len(persisted) != 1 || persisted[0].Term != 4 || !persisted[0].Fenced {
		t.Fatalf("persisted = %+v", persisted)
	}
	now = now.Add(56 * time.Second)
	if !errors.Is(controller.CanWrite(), ErrNotWritable) {
		t.Fatal("lease inside safety margin accepted writes")
	}
}

func TestControllerRenewalFailureUsesOnlyExistingLease(t *testing.T) {
	now := time.Unix(100, 0)
	stop := errors.New("coordinator unavailable")
	coordinator := &fakeCoordinator{acquire: LeaseGrant{Term: 1, LeaderID: testID(2), ExpiresAt: now.Add(time.Minute), Fenced: true}, renewErr: stop}
	controller := newTestController(t, coordinator, &now)
	if err := controller.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := controller.Renew(context.Background()); !errors.Is(err, stop) {
		t.Fatalf("Renew error = %v", err)
	}
	if err := controller.CanWrite(); err != nil {
		t.Fatalf("existing safe Lease was discarded: %v", err)
	}
	now = now.Add(time.Minute)
	if !errors.Is(controller.CanWrite(), ErrNotWritable) {
		t.Fatal("expired existing Lease remained writable")
	}
}

func TestControllerPersistsHigherTermBeforeStandby(t *testing.T) {
	now := time.Unix(100, 0)
	coordinator := &fakeCoordinator{acquire: LeaseGrant{Term: 1, LeaderID: testID(2), ExpiresAt: now.Add(time.Minute), Fenced: true}}
	controller := newTestController(t, coordinator, &now)
	if err := controller.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := controller.ObserveHigherTerm(2, testID(3)); err != nil {
		t.Fatal(err)
	}
	state := controller.Snapshot()
	if state.Role != RoleStandby || state.Term != 2 || state.LeaderID != testID(3) {
		t.Fatalf("state = %+v", state)
	}
	if !errors.Is(controller.CanWrite(), ErrNotWritable) {
		t.Fatal("Standby accepted writes")
	}
}

func TestControllerRejectsUnfencedOrStaleGrant(t *testing.T) {
	now := time.Unix(100, 0)
	for _, grant := range []LeaseGrant{
		{Term: 3, LeaderID: testID(2), ExpiresAt: now.Add(time.Minute), Fenced: true},
		{Term: 4, LeaderID: testID(2), ExpiresAt: now.Add(time.Minute), Fenced: false},
		{Term: 4, LeaderID: testID(3), ExpiresAt: now.Add(time.Minute), Fenced: true},
		{Term: 4, LeaderID: testID(2), ExpiresAt: now.Add(time.Second), Fenced: true},
	} {
		controller, err := New(&fakeCoordinator{acquire: grant}, Options{GroupID: testID(1), NodeID: testID(2), KnownTerm: 3, SafetyMargin: 5 * time.Second, Now: func() time.Time { return now }, Persist: func(State) error { return nil }})
		if err != nil {
			t.Fatal(err)
		}
		if err = controller.Acquire(context.Background()); err == nil {
			t.Fatalf("invalid grant accepted: %+v", grant)
		}
	}
}

func TestControllerReleaseStopsWrites(t *testing.T) {
	now := time.Unix(100, 0)
	coordinator := &fakeCoordinator{acquire: LeaseGrant{Term: 1, LeaderID: testID(2), ExpiresAt: now.Add(time.Minute), Fenced: true}}
	controller := newTestController(t, coordinator, &now)
	if err := controller.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := controller.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !coordinator.released || !errors.Is(controller.CanWrite(), ErrNotWritable) {
		t.Fatal("released Controller remained writable")
	}
}

func TestControllerRestoresValidatedPrimaryState(t *testing.T) {
	now := time.Unix(100, 0)
	initial := State{Role: RolePrimary, Term: 7, LeaderID: testID(2), ExpiresAt: now.Add(time.Minute), Fenced: true}
	controller, err := New(&fakeCoordinator{}, Options{GroupID: testID(1), NodeID: testID(2), KnownTerm: 7, SafetyMargin: 5 * time.Second, Now: func() time.Time { return now }, Persist: func(State) error { return nil }, Initial: &initial})
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.CanWrite(); err != nil {
		t.Fatalf("restored safe Primary is not writable: %v", err)
	}
	invalid := initial
	invalid.LeaderID = testID(3)
	if _, err = New(&fakeCoordinator{}, Options{GroupID: testID(1), NodeID: testID(2), KnownTerm: 7, SafetyMargin: 5 * time.Second, Now: func() time.Time { return now }, Persist: func(State) error { return nil }, Initial: &invalid}); err == nil {
		t.Fatal("foreign initial Primary state was accepted")
	}
}

func TestControllerPersistsReplicationStateTransitions(t *testing.T) {
	now := time.Unix(100, 0)
	identity := format.NodeIdentity{ClusterID: testID(9), GroupID: testID(1), NodeID: testID(2), CreatedAt: 1}
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	stateStore, err := replicationstate.Open(root.Path(), identity)
	if err != nil {
		t.Fatal(err)
	}
	persist, err := ReplicationStatePersistence(stateStore, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	coordinator := &fakeCoordinator{acquire: LeaseGrant{Term: 1, LeaderID: identity.NodeID, ExpiresAt: now.Add(time.Minute), Fenced: true}}
	controller, err := New(coordinator, Options{GroupID: identity.GroupID, NodeID: identity.NodeID, SafetyMargin: 5 * time.Second, Now: func() time.Time { return now }, Persist: persist})
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, ok := stateStore.Current()
	if !ok || current.Header.Role != format.ReplicationRolePrimary || current.Header.Term != 1 || !current.Header.HasLease {
		t.Fatalf("Primary State = %+v, ok = %v", current.Header, ok)
	}
	if err = controller.ObserveHigherTerm(2, testID(3)); err != nil {
		t.Fatal(err)
	}
	current, ok = stateStore.Current()
	if !ok || current.Header.Generation != 1 || current.Header.Role != format.ReplicationRoleStandby || current.Header.Term != 2 || current.Header.HasLease {
		t.Fatalf("Standby State = %+v, ok = %v", current.Header, ok)
	}
}

func newTestController(t *testing.T, coordinator Coordinator, now *time.Time) *Controller {
	t.Helper()
	controller, err := New(coordinator, Options{GroupID: testID(1), NodeID: testID(2), SafetyMargin: 5 * time.Second, Now: func() time.Time { return *now }, Persist: func(State) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func testID(value byte) format.UUID {
	var id format.UUID
	id[15] = value
	return id
}
