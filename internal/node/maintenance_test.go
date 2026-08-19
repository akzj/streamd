package node

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
)

func TestMaintenanceControllerTriggersThresholdsAndCapacityHysteresis(t *testing.T) {
	now := time.Unix(100, 0)
	controller := &maintenanceController{
		limits: maintenanceLimits{
			checkInterval: time.Second, memTableBytes: 100, activeWALBytes: 200,
			diskHighPercent: 80, diskCriticalPercent: 90, minimumAvailableBytes: 100,
		},
		maximumInterval: time.Minute,
		lastCheckpoint:  now,
	}
	decision := controller.evaluate(now.Add(time.Second), engine.MaintenanceStats{MemTableBytes: 100}, diskCapacity{capacity: 1000, available: 500})
	if !decision.checkpoint || decision.high || decision.critical {
		t.Fatalf("MemTable decision = %+v", decision)
	}
	controller.checkpointCompleted(now.Add(time.Second))
	decision = controller.evaluate(now.Add(2*time.Second), engine.MaintenanceStats{}, diskCapacity{capacity: 1000, available: 150})
	if !decision.checkpoint || !decision.high || decision.critical {
		t.Fatalf("high decision = %+v", decision)
	}
	decision = controller.evaluate(now.Add(3*time.Second), engine.MaintenanceStats{}, diskCapacity{capacity: 1000, available: 90})
	if !decision.critical {
		t.Fatalf("critical decision = %+v", decision)
	}
	decision = controller.evaluate(now.Add(4*time.Second), engine.MaintenanceStats{}, diskCapacity{capacity: 1000, available: 150})
	if !decision.critical {
		t.Fatalf("critical state cleared without hysteresis: %+v", decision)
	}
	decision = controller.evaluate(now.Add(5*time.Second), engine.MaintenanceStats{}, diskCapacity{capacity: 1000, available: 250})
	if decision.high || decision.critical {
		t.Fatalf("capacity state did not recover: %+v", decision)
	}
}

func TestCreateSnapshotAndCollectWALClosesOnlineRetentionLoop(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data")
	identity := format.NodeIdentity{ClusterID: nodeTestID(1), GroupID: nodeTestID(2), NodeID: nodeTestID(3), CreatedAt: 1}
	store, err := engine.OpenWithIdentity(data, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("record")}}}); err != nil {
		t.Fatal(err)
	}
	if err = createSnapshotAndCollectWAL(store, nil, 1<<30, time.Unix(200, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", ExpectedSequence: 1, RequestID: []byte("r2"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("record-2")}}}); err != nil {
		t.Fatal(err)
	}
	if err = createSnapshotAndCollectWAL(store, nil, 1<<30, time.Unix(300, 0)); err != nil {
		t.Fatal(err)
	}
	logs, err := filepath.Glob(filepath.Join(data, "wal", "*.log"))
	if err != nil || len(logs) != 1 {
		t.Fatalf("WAL files = %v, error = %v", logs, err)
	}
	snapshots, err := filepath.Glob(filepath.Join(data, "snapshots", "auto-*"))
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("automatic Snapshots = %v, error = %v", snapshots, err)
	}
}

func TestMaintenanceControllerTriggersMaximumIntervalAndWALBytes(t *testing.T) {
	now := time.Unix(100, 0)
	controller := &maintenanceController{
		limits:          maintenanceLimits{memTableBytes: 100, activeWALBytes: 200, diskHighPercent: 80, diskCriticalPercent: 90, minimumAvailableBytes: 100},
		maximumInterval: time.Minute, lastCheckpoint: now,
	}
	disk := diskCapacity{capacity: 1000, available: 500}
	if decision := controller.evaluate(now.Add(time.Second), engine.MaintenanceStats{ActiveWALBytes: 200}, disk); !decision.checkpoint {
		t.Fatalf("WAL threshold decision = %+v", decision)
	}
	controller.checkpointCompleted(now.Add(time.Second))
	if decision := controller.evaluate(now.Add(61*time.Second), engine.MaintenanceStats{}, disk); !decision.checkpoint {
		t.Fatalf("maximum interval decision = %+v", decision)
	}
}
