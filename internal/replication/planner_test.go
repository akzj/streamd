package replication

import (
	"math"
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
)

func TestPlanIncrementalFromEmptyAndExistingPrefix(t *testing.T) {
	view := primaryView()
	for _, test := range []struct {
		name  string
		hello ReplicaHello
		start uint64
	}{
		{"empty", hello(), 0},
		{"existing", withDurable(hello(), position(4, 104)), 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, err := Plan(view, test.hello)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Mode != PlanIncremental || plan.StartEntryID != test.start || plan.Term != view.Term || plan.LeaderID != view.LeaderID || plan.Committed != view.Committed {
				t.Fatalf("plan = %+v", plan)
			}
		})
	}
}

func TestPlanSnapshotWhenWALPrefixWasCollected(t *testing.T) {
	view := primaryView()
	view.EarliestWAL = 5
	view.Snapshot = &InstallableSnapshot{SnapshotID: uuid(9), Checkpoint: position(7, 107)}
	view.ChecksumAt = func(id uint64) (uint32, bool) {
		if id < view.EarliestWAL {
			return 0, false
		}
		return uint32(100 + id), id <= 9
	}
	plan, err := Plan(view, withDurable(hello(), position(2, 102)))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != PlanSnapshot || plan.SnapshotID != uuid(9) || plan.Checkpoint != position(7, 107) || plan.StartEntryID != 8 {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestPlanContinuesFromMatchingInstalledSnapshot(t *testing.T) {
	view := primaryView()
	view.EarliestWAL = 5
	view.Snapshot = &InstallableSnapshot{SnapshotID: uuid(9), Checkpoint: position(4, 104)}
	view.ChecksumAt = func(id uint64) (uint32, bool) {
		if id < view.EarliestWAL {
			return 0, false
		}
		return uint32(100 + id), id <= 9
	}
	h := withDurable(hello(), position(4, 104))
	h.InstalledSnapshotID = uuid(9)
	h.Snapshot = position(4, 104)
	plan, err := Plan(view, h)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != PlanIncremental || plan.StartEntryID != 5 {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestPlanRejectsDivergedOrUnrecoverablePrefix(t *testing.T) {
	for _, test := range []struct {
		name string
		view PrimaryView
		h    ReplicaHello
		code ErrorCode
	}{
		{"checksum", primaryView(), withDurable(hello(), position(4, 999)), ErrLogDiverged},
		{"ahead", primaryView(), withDurable(hello(), position(12, 112)), ErrLogDiverged},
		{"collected", func() PrimaryView { v := primaryView(); v.EarliestWAL = 5; return v }(), hello(), ErrNoRecoverySource},
		{"newer-term", primaryView(), func() ReplicaHello { h := hello(); h.KnownTerm = 8; return h }(), ErrTermStale},
		{"wrong-group", primaryView(), func() ReplicaHello { h := hello(); h.GroupID = uuid(8); return h }(), ErrWrongGroup},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Plan(test.view, test.h)
			if !IsCode(err, test.code) {
				t.Fatalf("error = %v, want %s", err, test.code)
			}
		})
	}
}

func TestPlanValidatesWatermarkOrderingAndIdentity(t *testing.T) {
	bad := hello()
	bad.LocalDurable = position(3, 103)
	bad.Committed = position(4, 104)
	if _, err := Plan(primaryView(), bad); !IsCode(err, ErrInvalidState) {
		t.Fatalf("watermark error = %v", err)
	}
	bad = hello()
	bad.NodeID = uuid(2)
	if _, err := Plan(primaryView(), bad); !IsCode(err, ErrInvalidState) {
		t.Fatalf("identity error = %v", err)
	}
	bad = hello()
	bad.LastAppended = position(3, 103)
	bad.LocalDurable = position(3, 999)
	if _, err := Plan(primaryView(), bad); !IsCode(err, ErrInvalidState) {
		t.Fatalf("checksum ordering error = %v", err)
	}
}

func TestPlanDoesNotWrapEntryID(t *testing.T) {
	view := primaryView()
	view.LastAppended = position(math.MaxUint64, 1)
	view.LocalDurable = view.LastAppended
	view.Committed = view.LastAppended
	view.ChecksumAt = func(id uint64) (uint32, bool) { return 1, id == math.MaxUint64 }
	_, err := Plan(view, withDurable(hello(), view.LastAppended))
	if !IsCode(err, ErrNoRecoverySource) {
		t.Fatalf("overflow error = %v", err)
	}
}

func primaryView() PrimaryView {
	return PrimaryView{
		GroupID:      uuid(1),
		LeaderID:     uuid(2),
		Term:         7,
		EarliestWAL:  0,
		LastAppended: position(9, 109),
		LocalDurable: position(9, 109),
		Committed:    position(8, 108),
		ChecksumAt: func(id uint64) (uint32, bool) {
			if id > 9 {
				return 0, false
			}
			return uint32(100 + id), true
		},
	}
}

func hello() ReplicaHello {
	return ReplicaHello{GroupID: uuid(1), NodeID: uuid(3), KnownTerm: 7}
}

func withDurable(h ReplicaHello, durable Position) ReplicaHello {
	h.LastAppended = durable
	h.LocalDurable = durable
	return h
}

func position(id uint64, checksum uint32) Position {
	return Position{Valid: true, EntryID: id, CRC32C: checksum}
}

func uuid(last byte) format.UUID {
	var id format.UUID
	id[len(id)-1] = last
	return id
}
