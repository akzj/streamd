package diagnostics

import (
	"errors"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/replication"
	"github.com/akzj/streamd/internal/storage/commit"
	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
)

func TestEngineSnapshotPreservesEntryZeroAndStableReasons(t *testing.T) {
	health := engine.Health{
		Role: format.ReplicationRoleSingle, Durability: format.ReplicationDurabilitySingleSync,
		Watermarks: commit.Watermarks{
			HasValue: true, Appended: 0, HasLocalDurable: true, LocalDurable: 0,
			HasCommitted: true, Committed: 0, HasApplied: true, Applied: 0,
		},
	}
	snapshot := EngineSnapshot(health, false, nil)
	if !snapshot.Ready || !snapshot.WriteReady || snapshot.Status != StatusReadyWrite || snapshot.Watermarks.Appended == nil || *snapshot.Watermarks.Appended != 0 || snapshot.Watermarks.Replicated != nil {
		t.Fatalf("healthy snapshot = %+v", snapshot)
	}
	snapshot = EngineSnapshot(health, true, nil)
	if snapshot.Ready || snapshot.Status != StatusReadyRead || len(snapshot.Reasons) != 1 || snapshot.Reasons[0].Code != ReasonServerDraining {
		t.Fatalf("draining snapshot = %+v", snapshot)
	}
	health.Fatal = errors.New("secret disk path")
	snapshot = EngineSnapshot(health, false, nil)
	if snapshot.Status != StatusFailed || len(snapshot.Reasons) != 1 || snapshot.Reasons[0].Code != ReasonCommitCoreFailed || snapshot.Reasons[0].Message == health.Fatal.Error() {
		t.Fatalf("fatal snapshot = %+v", snapshot)
	}
}

func TestPrimarySnapshotUsesLeaseReasonAndComputesLags(t *testing.T) {
	expires := time.Unix(100, 0)
	health := engine.Health{
		Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Term: 8,
		WriteUnavailable: errors.New("opaque guard error"),
		Watermarks: commit.Watermarks{
			HasValue: true, Appended: 12, HasLocalDurable: true, LocalDurable: 12,
			HasReplicated: true, Replicated: 10, HasCommitted: true, Committed: 10,
			HasApplied: true, Applied: 8,
		},
	}
	snapshot := EngineSnapshot(health, false, func() LeaseState { return LeaseState{Term: 8, ExpiresAt: expires, Unsafe: true} })
	if snapshot.Status != StatusReadyRead || snapshot.Ready || snapshot.LeaseExpiresAt == nil || !snapshot.LeaseExpiresAt.Equal(expires) || snapshot.ReplicationLagEntries != 2 || snapshot.ApplyLagEntries != 2 || len(snapshot.Reasons) != 1 || snapshot.Reasons[0].Code != ReasonLeaseUnsafe {
		t.Fatalf("unsafe Primary snapshot = %+v", snapshot)
	}
	health.WriteUnavailable = nil
	snapshot = EngineSnapshot(health, false, func() LeaseState { return LeaseState{Term: 7, ExpiresAt: expires} })
	if snapshot.Status != StatusFailed || len(snapshot.Reasons) != 1 || snapshot.Reasons[0].Code != ReasonStateInconsistent {
		t.Fatalf("Term mismatch snapshot = %+v", snapshot)
	}
}

func TestStandbySnapshotIsReadyRead(t *testing.T) {
	receiver, err := replication.NewReceiver(noopStandbyLog{}, replication.ReceiverConfig{
		GroupID: diagnosticsUUID(1), NodeID: diagnosticsUUID(2),
		State:        replication.ReceiverState{Term: 3, LeaderID: diagnosticsUUID(3)},
		ChecksumAt:   func(uint64) (uint32, bool) { return 0, false },
		EntryAt:      func(uint64) (format.WALEntry, bool) { return format.WALEntry{}, false },
		ObserveTerm:  func(uint64, format.UUID) error { return nil },
		ApplyEntries: func([]format.WALEntry) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewStandbyProvider(receiver)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := provider.Snapshot()
	if !snapshot.Ready || snapshot.WriteReady || snapshot.Status != StatusReadyRead || snapshot.Role != "standby" || snapshot.Term != 3 || len(snapshot.Reasons) != 0 {
		t.Fatalf("Standby snapshot = %+v", snapshot)
	}
}

type noopStandbyLog struct{}

func (noopStandbyLog) Append(...[]byte) error { return nil }
func (noopStandbyLog) Sync() error            { return nil }

func diagnosticsUUID(value byte) format.UUID {
	var id format.UUID
	id[len(id)-1] = value
	return id
}
