// Package leadership enforces coordinator-issued Term, Lease, and fencing
// before a replicated node is allowed to accept writes.
package leadership

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
)

var ErrNotWritable = errors.New("node does not hold a safe primary lease")

type LeaseGrant struct {
	Term      uint64
	LeaderID  format.UUID
	ExpiresAt time.Time
	Fenced    bool
}

// Coordinator must provide linearizable grants. Acquire may return only after
// the previous leader is fenced; Renew must never change the Term.
type Coordinator interface {
	Acquire(context.Context, format.UUID, format.UUID) (LeaseGrant, error)
	Renew(context.Context, format.UUID, format.UUID, uint64) (LeaseGrant, error)
	Release(context.Context, format.UUID, format.UUID, uint64) error
}

type Role uint8

const (
	RoleRecovering Role = iota + 1
	RolePrimary
	RoleStandby
)

type State struct {
	Role       Role
	Term       uint64
	LeaderID   format.UUID
	ExpiresAt  time.Time
	Fenced     bool
	LastReason string
}

type Persist func(State) error

type Options struct {
	GroupID      format.UUID
	NodeID       format.UUID
	KnownTerm    uint64
	SafetyMargin time.Duration
	Now          func() time.Time
	Persist      Persist
	Initial      *State
}

type Controller struct {
	opMu        sync.Mutex
	mu          sync.RWMutex
	coordinator Coordinator
	groupID     format.UUID
	nodeID      format.UUID
	safety      time.Duration
	now         func() time.Time
	persist     Persist
	state       State
}

func New(coordinator Coordinator, options Options) (*Controller, error) {
	if coordinator == nil || zeroUUID(options.GroupID) || zeroUUID(options.NodeID) || options.Persist == nil {
		return nil, fmt.Errorf("coordinator, node identity, and persistence are required")
	}
	if options.SafetyMargin <= 0 {
		return nil, fmt.Errorf("lease safety margin must be positive")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	initial := State{Role: RoleRecovering, Term: options.KnownTerm, LastReason: "leadership not acquired"}
	if options.Initial != nil {
		initial = *options.Initial
		if initial.Term < options.KnownTerm || (initial.Role == RolePrimary && (initial.LeaderID != options.NodeID || !initial.Fenced)) {
			return nil, fmt.Errorf("initial leadership state is invalid")
		}
	}
	return &Controller{coordinator: coordinator, groupID: options.GroupID, nodeID: options.NodeID, safety: options.SafetyMargin, now: options.Now, persist: options.Persist, state: initial}, nil
}

func (c *Controller) Acquire(ctx context.Context) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	grant, err := c.coordinator.Acquire(ctx, c.groupID, c.nodeID)
	if err != nil {
		return err
	}
	c.mu.RLock()
	knownTerm := c.state.Term
	c.mu.RUnlock()
	if err = c.validateGrant(grant, true, knownTerm); err != nil {
		return err
	}
	next := State{Role: RolePrimary, Term: grant.Term, LeaderID: grant.LeaderID, ExpiresAt: grant.ExpiresAt, Fenced: true}
	if err = c.persist(next); err != nil {
		return fmt.Errorf("persist acquired leadership: %w", err)
	}
	c.mu.Lock()
	c.state = next
	c.mu.Unlock()
	return nil
}

func (c *Controller) Renew(ctx context.Context) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.RLock()
	current := c.state
	c.mu.RUnlock()
	if current.Role != RolePrimary || !current.Fenced {
		return ErrNotWritable
	}
	grant, err := c.coordinator.Renew(ctx, c.groupID, c.nodeID, current.Term)
	if err != nil {
		return err
	}
	if err = c.validateGrant(grant, false, current.Term); err != nil {
		return err
	}
	if grant.ExpiresAt.Before(current.ExpiresAt) {
		return fmt.Errorf("renewed lease deadline regressed")
	}
	next := State{Role: RolePrimary, Term: grant.Term, LeaderID: grant.LeaderID, ExpiresAt: grant.ExpiresAt, Fenced: true}
	if err = c.persist(next); err != nil {
		return fmt.Errorf("persist renewed leadership: %w", err)
	}
	c.mu.Lock()
	c.state = next
	c.mu.Unlock()
	return nil
}

// ObserveHigherTerm is used by the replication receiver before accepting a
// message from a newer leader. The new Term is durable before it becomes live.
func (c *Controller) ObserveHigherTerm(term uint64, leaderID format.UUID) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.RLock()
	current := c.state
	c.mu.RUnlock()
	if term <= current.Term || zeroUUID(leaderID) || leaderID == c.nodeID {
		return fmt.Errorf("observed leadership is not a valid higher external Term")
	}
	next := State{Role: RoleStandby, Term: term, LeaderID: leaderID, LastReason: "higher Term observed"}
	if err := c.persist(next); err != nil {
		return fmt.Errorf("persist higher Term: %w", err)
	}
	c.mu.Lock()
	c.state = next
	c.mu.Unlock()
	return nil
}

func (c *Controller) Release(ctx context.Context) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.RLock()
	current := c.state
	c.mu.RUnlock()
	if current.Role != RolePrimary {
		return nil
	}
	if err := c.coordinator.Release(ctx, c.groupID, c.nodeID, current.Term); err != nil {
		return err
	}
	next := State{Role: RoleRecovering, Term: current.Term, LastReason: "leadership released"}
	c.mu.Lock()
	c.state = next
	c.mu.Unlock()
	if err := c.persist(next); err != nil {
		return fmt.Errorf("persist released leadership: %w", err)
	}
	return nil
}

func (c *Controller) CanWrite() error {
	c.mu.RLock()
	state := c.state
	c.mu.RUnlock()
	if state.Role != RolePrimary || !state.Fenced || !c.now().Add(c.safety).Before(state.ExpiresAt) {
		return ErrNotWritable
	}
	return nil
}

func (c *Controller) CanCommit() error { return c.CanWrite() }

func (c *Controller) Snapshot() State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	state := c.state
	if state.Role == RolePrimary && state.LastReason == "" && !c.now().Add(c.safety).Before(state.ExpiresAt) {
		state.LastReason = "lease is inside its safety margin"
	}
	return state
}

func (c *Controller) validateGrant(grant LeaseGrant, acquire bool, term uint64) error {
	if zeroUUID(grant.LeaderID) || grant.LeaderID != c.nodeID || !grant.Fenced {
		return fmt.Errorf("coordinator grant is not fenced for this node")
	}
	if (acquire && grant.Term <= term) || (!acquire && grant.Term != term) {
		return fmt.Errorf("coordinator grant Term %d is invalid after Term %d", grant.Term, term)
	}
	if !c.now().Add(c.safety).Before(grant.ExpiresAt) {
		return fmt.Errorf("coordinator lease is already inside its safety margin")
	}
	return nil
}

func zeroUUID(id format.UUID) bool { return id == format.UUID{} }
