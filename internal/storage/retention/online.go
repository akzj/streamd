package retention

import (
	"errors"
	"time"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/snapshot"
	"github.com/akzj/streamd/internal/storage/wal"
)

type OnlineResult struct {
	Snapshot   snapshot.Result
	Collection wal.GCResult
}

// CreateSnapshotAndCollect closes the online retention transaction around a
// live Store. Replicated stores publish the verified Snapshot as their durable
// recovery anchor before deleting WAL, then advance the durable earliest-WAL
// boundary after collection. That ordering leaves a conservative, recoverable
// state across a crash at every publication boundary.
func CreateSnapshotAndCollect(store *engine.Store, states *replicationstate.Store, destination string, maxRetainedBytes uint64, now time.Time) (OnlineResult, error) {
	var result OnlineResult
	var err error
	if states != nil {
		result.Snapshot, err = snapshot.CreateOnlineReplicatedLinked(store, states, destination)
	} else {
		result.Snapshot, err = snapshot.CreateOnlineLinked(store, destination)
	}
	if err != nil {
		return result, err
	}
	result.Snapshot, err = snapshot.Verify(result.Snapshot.Path)
	if err != nil {
		return result, err
	}
	if states != nil {
		if _, err = states.Update(now, func(header *format.ReplicationStateHeader) error {
			header.HasInstalledSnapshot = true
			header.InstalledSnapshotID = result.Snapshot.SnapshotID
			header.InstalledSnapshotEntry = format.ReplicationPosition{Present: true, EntryID: result.Snapshot.CheckpointEntryID, CRC32C: result.Snapshot.CheckpointCRC32C}
			return nil
		}); err != nil {
			return result, err
		}
	}
	result.Collection, err = store.CollectWAL(engine.WALCollectionEvidence{SnapshotEntryID: result.Snapshot.CheckpointEntryID, SnapshotVerified: true, MaxRetainedBytes: maxRetainedBytes})
	if err != nil && !errors.Is(err, wal.ErrRetentionPressure) {
		return result, err
	}
	if states != nil {
		_, stateErr := states.Update(now, func(header *format.ReplicationStateHeader) error {
			header.EarliestWALEntryID = result.Collection.EarliestWAL
			return nil
		})
		err = errors.Join(err, stateErr)
	}
	return result, err
}
