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

func TestRecoveryBlockedSnapshotIsStructuredAndNotReady(t *testing.T) {
	position := format.ReplicationPosition{Present: true, EntryID: 12, CRC32C: 101}
	header := format.ReplicationStateHeader{Term: 8, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, LastAppended: position, LocalDurable: position, Replicated: position, Committed: position, Applied: position}
	task := RecoveryTask{
		TaskID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Action: RecoveryCreateAndInstallSnapshot, Reason: RecoveryNoRecoverySource,
		Term: 8, GroupID: "33333333333333333333333333333333", SourceNodeID: "11111111111111111111111111111111", TargetNodeID: "22222222222222222222222222222222", EarliestWALEntryID: 13,
	}
	expires := time.Unix(100, 0)
	snapshot := RecoveryBlockedSnapshot(header, task, LeaseState{Term: 8, ExpiresAt: expires})
	if snapshot.Status != StatusDegraded || snapshot.Ready || snapshot.WriteReady || snapshot.Role != "primary" || snapshot.Recovery == nil || snapshot.Recovery.TaskID != task.TaskID || len(snapshot.Reasons) != 1 || snapshot.Reasons[0].Code != ReasonSnapshotRequired {
		t.Fatalf("recovery snapshot = %+v", snapshot)
	}
	if err := Validate(snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot = RecoveryBlockedSnapshot(header, task, LeaseState{Term: 8, Unsafe: true})
	if snapshot.Status != StatusFailed || snapshot.Role != "recovering" || snapshot.LeaseExpiresAt != nil || len(snapshot.Reasons) != 2 || snapshot.Reasons[1].Code != ReasonLeaseUnsafe {
		t.Fatalf("unsafe recovery snapshot = %+v", snapshot)
	}
}

func TestStartingSnapshotAndProviderTransition(t *testing.T) {
	starting := StartingSnapshot("standby", 0, time.Time{}, ReasonLeadershipPending)
	if starting.Status != StatusStarting || starting.Ready || starting.WriteReady || starting.Role != "standby" || starting.Term != 0 || len(starting.Reasons) != 1 || starting.Reasons[0].Code != ReasonLeadershipPending {
		t.Fatalf("starting Standby = %+v", starting)
	}
	provider, err := NewSwitchableProvider(ProviderFunc(func() Snapshot { return starting }))
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Unix(200, 0)
	waiting := StartingSnapshot("primary", 9, expires, ReasonReplicaCatchUpPending)
	if err = provider.Set(ProviderFunc(func() Snapshot { return waiting })); err != nil {
		t.Fatal(err)
	}
	current := provider.Snapshot()
	if current.Role != "primary" || current.Term != 9 || current.LeaseExpiresAt == nil || !current.LeaseExpiresAt.Equal(expires) || current.Reasons[0].Code != ReasonReplicaCatchUpPending {
		t.Fatalf("waiting Primary = %+v", current)
	}
	invalid := current
	invalid.Status = StatusReadyWrite
	if err = provider.Set(ProviderFunc(func() Snapshot { return invalid })); err == nil {
		t.Fatal("invalid diagnostics transition was accepted")
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
