package node

import (
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/engine"
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
