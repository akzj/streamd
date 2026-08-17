package node

import (
	"errors"
	"testing"

	"github.com/akzj/streamd/internal/diagnostics"
	"github.com/akzj/streamd/internal/replication"
)

func TestStandbyRecoveryTaskIsDeterministicAndBoundToFacts(t *testing.T) {
	view := replication.PrimaryView{
		GroupID: nodeTestID(3), LeaderID: nodeTestID(1), Term: 7, EarliestWAL: 12,
		Snapshot: &replication.InstallableSnapshot{SnapshotID: nodeTestID(9), Checkpoint: replication.Position{Valid: true, EntryID: 11, CRC32C: 101}},
	}
	hello := replication.ReplicaHello{NodeID: nodeTestID(2), LocalDurable: replication.Position{Valid: true, EntryID: 4, CRC32C: 44}}
	cause := errors.New("opaque protocol detail")
	first := newStandbyRecoveryRequired(diagnostics.RecoveryLogDiverged, view, hello, replication.ReplicationPlan{}, cause)
	second := newStandbyRecoveryRequired(diagnostics.RecoveryLogDiverged, view, hello, replication.ReplicationPlan{}, cause)
	var required *standbyRecoveryRequired
	if !errors.As(first, &required) || !errors.Is(first, cause) {
		t.Fatalf("recovery error = %v", first)
	}
	if required.task.TaskID == "" || required.task.TaskID != second.(*standbyRecoveryRequired).task.TaskID || required.task.Action != diagnostics.RecoveryInstallSnapshot || required.task.SnapshotID != "00000000000000000000000000000009" || required.task.SnapshotCheckpoint == nil || *required.task.SnapshotCheckpoint != 11 || required.task.TargetDurableEntryID == nil || *required.task.TargetDurableEntryID != 4 {
		t.Fatalf("recovery task = %+v", required.task)
	}
	changedHello := hello
	changedHello.LocalDurable.CRC32C++
	changed := newStandbyRecoveryRequired(diagnostics.RecoveryLogDiverged, view, changedHello, replication.ReplicationPlan{}, cause).(*standbyRecoveryRequired)
	if changed.task.TaskID == required.task.TaskID {
		t.Fatalf("recovery task ID did not change with target durable checksum: before=%+v after=%+v", required.task, changed.task)
	}
	withoutSnapshot := view
	withoutSnapshot.Snapshot = nil
	created := newStandbyRecoveryRequired(diagnostics.RecoveryNoRecoverySource, withoutSnapshot, hello, replication.ReplicationPlan{}, cause).(*standbyRecoveryRequired)
	if created.task.Action != diagnostics.RecoveryCreateAndInstallSnapshot || created.task.SnapshotID != "" || created.task.TaskID == required.task.TaskID {
		t.Fatalf("create recovery task = %+v", created.task)
	}
}
