package node

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/akzj/streamd/internal/replication"
	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"golang.org/x/sys/unix"
)

type diskCapacity struct {
	capacity  uint64
	available uint64
}

func runStandbyMaintenance(ctx context.Context, config Config, store *replication.StandbyStore, logger *slog.Logger) error {
	controller, err := newMaintenanceController(config, time.Now())
	if err != nil {
		return err
	}
	ticker := time.NewTicker(controller.limits.checkInterval)
	defer ticker.Stop()
	for {
		now := time.Now()
		disk, sampleErr := sampleDiskCapacity(config.DataDirectory)
		if sampleErr != nil {
			store.SetCapacityCritical(true)
			logger.Error("Standby capacity sample failed; replication writes paused", "error", sampleErr)
		} else {
			stats := store.MaintenanceStats()
			decision := controller.evaluate(now, engine.MaintenanceStats{MemTableRecords: stats.MemTableRecords, MemTableBytes: stats.MemTableBytes, ActiveWALBytes: stats.ActiveWALBytes}, disk)
			store.SetCapacityCritical(decision.critical)
			if decision.checkpoint {
				if checkpointErr := store.Checkpoint(); checkpointErr != nil {
					logger.Error("Standby replication checkpoint failed", "error", checkpointErr)
				} else {
					controller.checkpointCompleted(now)
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

type maintenanceDecision struct {
	checkpoint bool
	high       bool
	critical   bool
}

type maintenanceController struct {
	limits          maintenanceLimits
	maximumInterval time.Duration
	lastCheckpoint  time.Time
	high            bool
	critical        bool
}

func newMaintenanceController(config Config, now time.Time) (*maintenanceController, error) {
	limits, err := config.maintenanceLimits()
	if err != nil {
		return nil, err
	}
	maximum, err := config.checkpointDuration()
	if err != nil {
		return nil, err
	}
	return &maintenanceController{limits: limits, maximumInterval: maximum, lastCheckpoint: now}, nil
}

func (c *maintenanceController) evaluate(now time.Time, stats engine.MaintenanceStats, disk diskCapacity) maintenanceDecision {
	usedPercent := uint32(0)
	if disk.capacity > 0 && disk.available < disk.capacity {
		usedPercent = uint32((disk.capacity - disk.available) * 100 / disk.capacity)
	}
	minimumRecovery := c.limits.minimumAvailableBytes
	if minimumRecovery <= ^uint64(0)/2 {
		minimumRecovery *= 2
	}
	criticalNow := usedPercent >= c.limits.diskCriticalPercent || disk.available <= c.limits.minimumAvailableBytes
	if c.critical {
		c.critical = !(usedPercent < c.limits.diskHighPercent && disk.available > minimumRecovery)
	} else {
		c.critical = criticalNow
	}
	highNow := c.critical || usedPercent >= c.limits.diskHighPercent || disk.available <= minimumRecovery
	enteredHigh := highNow && !c.high
	c.high = highNow
	checkpoint := now.Sub(c.lastCheckpoint) >= c.maximumInterval ||
		stats.MemTableBytes >= c.limits.memTableBytes ||
		stats.ActiveWALBytes >= c.limits.activeWALBytes || enteredHigh
	return maintenanceDecision{checkpoint: checkpoint, high: c.high, critical: c.critical}
}

func (c *maintenanceController) checkpointCompleted(now time.Time) { c.lastCheckpoint = now }

func sampleDiskCapacity(root string) (diskCapacity, error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(root, &stats); err != nil {
		return diskCapacity{}, err
	}
	return diskCapacity{
		capacity:  saturatingMultiply(stats.Blocks, uint64(stats.Bsize)),
		available: saturatingMultiply(stats.Bavail, uint64(stats.Bsize)),
	}, nil
}

func saturatingMultiply(a, b uint64) uint64 {
	if a != 0 && b > ^uint64(0)/a {
		return ^uint64(0)
	}
	return a * b
}

type engineMaintenanceStore interface {
	MaintenanceStats() engine.MaintenanceStats
	SetCapacityCritical(bool)
	Compact(engine.CompactionOptions) (engine.CompactionResult, error)
}

func runEngineMaintenance(ctx context.Context, config Config, store engineMaintenanceStore, checkpoint func() (format.Manifest, bool, error), logger *slog.Logger) error {
	controller, err := newMaintenanceController(config, time.Now())
	if err != nil {
		return err
	}
	if checkpoint == nil {
		return fmt.Errorf("checkpoint operation is required")
	}
	ticker := time.NewTicker(controller.limits.checkInterval)
	defer ticker.Stop()
	for {
		now := time.Now()
		disk, sampleErr := sampleDiskCapacity(config.DataDirectory)
		if sampleErr != nil {
			store.SetCapacityCritical(true)
			logger.Error("capacity sample failed; writes paused", "error", sampleErr)
		} else {
			decision := controller.evaluate(now, store.MaintenanceStats(), disk)
			store.SetCapacityCritical(decision.critical)
			if decision.checkpoint {
				manifest, created, checkpointErr := checkpoint()
				if checkpointErr != nil {
					logger.Error("storage checkpoint failed", "error", checkpointErr)
				} else {
					controller.checkpointCompleted(now)
					if created {
						logger.Info("storage checkpoint published", "generation", manifest.Header.Generation, "entry_id", manifest.Header.LastEntryID)
					}
					compactMaintenanceStore(store, config, logger)
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func compactMaintenanceStore(store engineMaintenanceStore, config Config, logger *slog.Logger) {
	minSegments, maxInputSegments, maxInputBytes, _ := config.compactionLimits()
	result, err := store.Compact(engine.CompactionOptions{MinSegments: minSegments, MaxInputSegments: maxInputSegments, MaxInputBytes: maxInputBytes})
	if err != nil {
		logger.Error("Segment Compaction failed", "error", err)
		return
	}
	if result.Created {
		logger.Info("Segment Compaction published", "generation", result.Manifest.Header.Generation, "input_segments", result.InputSegments, "input_bytes", result.InputBytes, "live_segments", len(result.Manifest.SegmentReferences))
	}
}
