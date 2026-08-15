package replication

import (
	"context"
	"errors"
	"fmt"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/wal"
)

type CatchUpHistory interface {
	ReadRange(uint64, int, uint64) (wal.HistoryRange, error)
	PinRange(uint64, uint64) (func(), error)
}

// CatchUp copies only a known locally durable prefix. The WAL files remain
// pinned for the full transfer, then the cumulative committed watermark is
// sent after all required Entries are durable on the Standby.
func (p *Primary) CatchUp(ctx context.Context, history CatchUpHistory, start, durableThrough uint64, committed Position, maxEntries int, maxBytes uint64) error {
	if history == nil || maxEntries <= 0 || maxBytes == 0 || (committed.Valid && committed.EntryID > durableThrough) {
		return protocolError(ErrInvalidState, "catch-up bounds or history are invalid")
	}
	if start <= durableThrough {
		release, err := history.PinRange(start, durableThrough)
		if err != nil {
			if errors.Is(err, wal.ErrNotRetained) {
				return protocolError(ErrNeedsSnapshot, "catch-up WAL is no longer retained")
			}
			return fmt.Errorf("pin catch-up WAL: %w", err)
		}
		defer release()
		next := start
		for next <= durableThrough {
			batch, err := history.ReadRange(next, maxEntries, maxBytes)
			if err != nil {
				if errors.Is(err, wal.ErrNotRetained) {
					return protocolError(ErrNeedsSnapshot, "catch-up WAL was collected")
				}
				return fmt.Errorf("read catch-up WAL: %w", err)
			}
			if len(batch.Entries) == 0 {
				return protocolError(ErrLogGap, "catch-up WAL ended before durable watermark")
			}
			encoded := batch.Entries
			for i, value := range encoded {
				entry, decodeErr := format.UnmarshalWALEntry(value)
				if decodeErr != nil {
					return decodeErr
				}
				if entry.EntryID > durableThrough {
					encoded = encoded[:i]
					break
				}
			}
			if len(encoded) == 0 {
				return protocolError(ErrLogGap, "catch-up range skipped durable watermark")
			}
			last, err := p.Replicate(ctx, encoded)
			if err != nil {
				return err
			}
			if last == ^uint64(0) {
				return protocolError(ErrInvalidState, "catch-up Entry ID space is exhausted")
			}
			next = last + 1
		}
	}
	if committed.Valid {
		return p.AdvanceCommit(ctx, committed.EntryID)
	}
	return nil
}
