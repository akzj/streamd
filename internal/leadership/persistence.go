package leadership

import (
	"fmt"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/replicationstate"
)

// ReplicationStatePersistence adapts leadership transitions to the durable
// generation-chained Replication State store.
func ReplicationStatePersistence(store *replicationstate.Store, now func() time.Time) (Persist, error) {
	if store == nil {
		return nil, fmt.Errorf("Replication State store is required")
	}
	if now == nil {
		now = time.Now
	}
	return func(state State) error {
		_, err := store.Update(now(), func(header *format.ReplicationStateHeader) error {
			header.Term = state.Term
			header.Durability = format.ReplicationDurabilityStrict
			header.HasLease = false
			header.LeaseExpiresAt = 0
			header.HasLeader = false
			header.LeaderID = format.UUID{}
			switch state.Role {
			case RolePrimary:
				header.Role = format.ReplicationRolePrimary
				header.HasLeader = true
				header.LeaderID = header.NodeID
				header.HasLease = true
				header.LeaseExpiresAt = state.ExpiresAt.UnixNano()
				if !header.Replicated.Present && header.Committed.Present {
					header.Replicated = header.Committed
				}
			case RoleStandby:
				header.Role = format.ReplicationRoleStandby
				header.HasLeader = true
				header.LeaderID = state.LeaderID
				header.Replicated = format.ReplicationPosition{}
			case RoleRecovering:
				header.Role = format.ReplicationRoleRecovering
				header.Replicated = format.ReplicationPosition{}
			default:
				return fmt.Errorf("unknown leadership role %d", state.Role)
			}
			return nil
		})
		return err
	}, nil
}
