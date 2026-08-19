package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/akzj/streamd/internal/replication"
	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/retention"
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
					minSegments, maxInputSegments, maxInputBytes, _ := config.compactionLimits()
					compacted, compactErr := store.Compact(replication.StandbyCompactionOptions{MinSegments: minSegments, MaxInputSegments: maxInputSegments, MaxInputBytes: maxInputBytes})
					if compactErr != nil {
						logger.Error("Standby Segment Compaction failed", "error", compactErr)
					} else if compacted.Created {
						logger.Info("Standby Segment Compaction published", "generation", compacted.Generation, "input_segments", compacted.InputSegments, "input_bytes", compacted.InputBytes, "live_segments", compacted.LiveSegments)
					}
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
	lastSnapshot    time.Time
	nextRetention   time.Time
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
	return &maintenanceController{limits: limits, maximumInterval: maximum, lastCheckpoint: now, lastSnapshot: now}, nil
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

func (c *maintenanceController) retentionDue(now time.Time, walBytes uint64, high bool) bool {
	if now.Before(c.nextRetention) {
		return false
	}
	return high || now.Sub(c.lastSnapshot) >= c.limits.snapshotInterval || walBytes > c.limits.maxRetainedWALBytes
}

func (c *maintenanceController) retentionAttempted(now time.Time) {
	retry := 30 * time.Second
	if candidate := c.limits.checkInterval * 10; candidate > retry {
		retry = candidate
	}
	c.nextRetention = now.Add(retry)
}

func (c *maintenanceController) snapshotCompleted(now time.Time) { c.lastSnapshot = now }

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

func runEngineMaintenance(ctx context.Context, config Config, store *engine.Store, states *replicationstate.Store, checkpoint func() (format.Manifest, bool, error), logger *slog.Logger) error {
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
			checkpointReady := true
			if decision.checkpoint {
				manifest, created, checkpointErr := checkpoint()
				if checkpointErr != nil {
					checkpointReady = false
					logger.Error("storage checkpoint failed", "error", checkpointErr)
				} else {
					controller.checkpointCompleted(now)
					if created {
						logger.Info("storage checkpoint published", "generation", manifest.Header.Generation, "entry_id", manifest.Header.LastEntryID)
					}
					compactMaintenanceStore(store, config, logger)
				}
			}
			if checkpointReady {
				walBytes, sizeErr := walDirectoryBytes(config.DataDirectory)
				if sizeErr != nil {
					logger.Error("WAL retention measurement failed", "error", sizeErr)
				} else if controller.retentionDue(now, walBytes, decision.high) {
					controller.retentionAttempted(now)
					retentionErr := createSnapshotAndCollectWAL(store, states, controller.limits.maxRetainedWALBytes, now)
					if retentionErr != nil {
						logger.Error("online Snapshot/WAL retention failed", "error", retentionErr)
					} else {
						controller.snapshotCompleted(now)
					}
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

func walDirectoryBytes(root string) (uint64, error) {
	entries, err := os.ReadDir(filepath.Join(root, "wal"))
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return 0, infoErr
		}
		if info.Size() > 0 {
			total += uint64(info.Size())
		}
	}
	return total, nil
}

func createSnapshotAndCollectWAL(store *engine.Store, states *replicationstate.Store, maxRetained uint64, now time.Time) error {
	destination := filepath.Join(store.DataRoot(), "snapshots", fmt.Sprintf("auto-%020d", now.UnixNano()))
	result, err := retention.CreateSnapshotAndCollect(store, states, destination, maxRetained, now)
	if err != nil {
		return err
	}
	if err = pruneAutomaticSnapshots(store.DataRoot(), result.Snapshot.Path); err != nil {
		return err
	}
	return nil
}

func pruneAutomaticSnapshots(root, keep string) error {
	directory := filepath.Join(root, "snapshots")
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	keep = filepath.Clean(keep)
	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) < len("auto-") || entry.Name()[:len("auto-")] != "auto-" {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		if filepath.Clean(path) == keep {
			continue
		}
		if err = os.RemoveAll(path); err != nil {
			return err
		}
	}
	handle, err := os.Open(directory)
	if err != nil {
		return err
	}
	err = handle.Sync()
	return errors.Join(err, handle.Close())
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
