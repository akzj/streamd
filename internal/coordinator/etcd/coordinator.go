// Package etcd implements linearizable Term, Lease, and fencing grants using
// an etcd key attached to an etcd Lease.
package etcd

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/akzj/streamd/internal/leadership"
	"github.com/akzj/streamd/internal/storage/format"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrLeaderExists = errors.New("replication group already has a leased leader")
	ErrNotLeader    = errors.New("node does not own the coordinator leader key")
)

type session struct {
	term    uint64
	leaseID clientv3.LeaseID
}

type Coordinator struct {
	client   *clientv3.Client
	prefix   string
	ttl      int64
	now      func() time.Time
	mu       sync.Mutex
	sessions map[string]session
}

func New(client *clientv3.Client, prefix string, ttl time.Duration) (*Coordinator, error) {
	if client == nil || ttl < 2*time.Second {
		return nil, fmt.Errorf("etcd client and Lease TTL of at least two seconds are required")
	}
	seconds := int64(ttl / time.Second)
	if ttl%time.Second != 0 {
		seconds++
	}
	prefix = path.Clean("/" + strings.TrimSpace(prefix))
	if prefix == "/" {
		prefix = "/streamd/v1"
	}
	return &Coordinator{client: client, prefix: prefix, ttl: seconds, now: time.Now, sessions: make(map[string]session)}, nil
}

func (c *Coordinator) Acquire(ctx context.Context, groupID, nodeID format.UUID) (leadership.LeaseGrant, error) {
	if zeroUUID(groupID) || zeroUUID(nodeID) {
		return leadership.LeaseGrant{}, fmt.Errorf("coordinator identity is zero")
	}
	key := c.key(groupID)
	lease, err := c.client.Grant(ctx, c.ttl)
	if err != nil {
		return leadership.LeaseGrant{}, err
	}
	value := hex.EncodeToString(nodeID[:])
	response, err := c.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, value, clientv3.WithLease(lease.ID))).
		Commit()
	if err != nil || !response.Succeeded {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = c.client.Revoke(cleanupCtx, lease.ID)
		cancel()
		if err != nil {
			return leadership.LeaseGrant{}, err
		}
		return leadership.LeaseGrant{}, ErrLeaderExists
	}
	term := uint64(response.Header.Revision)
	c.mu.Lock()
	c.sessions[key] = session{term: term, leaseID: lease.ID}
	c.mu.Unlock()
	return leadership.LeaseGrant{Term: term, LeaderID: nodeID, ExpiresAt: c.now().Add(time.Duration(lease.TTL) * time.Second), Fenced: true}, nil
}

func (c *Coordinator) Renew(ctx context.Context, groupID, nodeID format.UUID, term uint64) (leadership.LeaseGrant, error) {
	key := c.key(groupID)
	c.mu.Lock()
	owned, ok := c.sessions[key]
	c.mu.Unlock()
	if !ok || owned.term != term {
		return leadership.LeaseGrant{}, ErrNotLeader
	}
	keepalive, err := c.client.KeepAliveOnce(ctx, owned.leaseID)
	if err != nil {
		return leadership.LeaseGrant{}, err
	}
	response, err := c.client.Get(ctx, key)
	if err != nil {
		return leadership.LeaseGrant{}, err
	}
	if len(response.Kvs) != 1 || uint64(response.Kvs[0].ModRevision) != term || string(response.Kvs[0].Value) != hex.EncodeToString(nodeID[:]) || response.Kvs[0].Lease != int64(owned.leaseID) {
		return leadership.LeaseGrant{}, ErrNotLeader
	}
	return leadership.LeaseGrant{Term: term, LeaderID: nodeID, ExpiresAt: c.now().Add(time.Duration(keepalive.TTL) * time.Second), Fenced: true}, nil
}

func (c *Coordinator) Release(ctx context.Context, groupID, nodeID format.UUID, term uint64) error {
	key := c.key(groupID)
	c.mu.Lock()
	owned, ok := c.sessions[key]
	c.mu.Unlock()
	if !ok || owned.term != term {
		return ErrNotLeader
	}
	value := hex.EncodeToString(nodeID[:])
	response, err := c.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", int64(term)), clientv3.Compare(clientv3.Value(key), "=", value)).
		Then(clientv3.OpDelete(key)).
		Commit()
	if err != nil {
		return err
	}
	if !response.Succeeded {
		return ErrNotLeader
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, revokeErr := c.client.Revoke(cleanupCtx, owned.leaseID)
	cancel()
	c.mu.Lock()
	delete(c.sessions, key)
	c.mu.Unlock()
	return revokeErr
}

// Current returns the linearizable current leader grant for Standby discovery.
func (c *Coordinator) Current(ctx context.Context, groupID format.UUID) (leadership.LeaseGrant, error) {
	response, err := c.client.Get(ctx, c.key(groupID))
	if err != nil {
		return leadership.LeaseGrant{}, err
	}
	if len(response.Kvs) != 1 {
		return leadership.LeaseGrant{}, ErrNotLeader
	}
	kv := response.Kvs[0]
	nodeID, err := parseNode(string(kv.Value))
	if err != nil {
		return leadership.LeaseGrant{}, err
	}
	ttl, err := c.client.TimeToLive(ctx, clientv3.LeaseID(kv.Lease))
	if err != nil || ttl.TTL <= 0 {
		if err != nil {
			return leadership.LeaseGrant{}, err
		}
		return leadership.LeaseGrant{}, ErrNotLeader
	}
	return leadership.LeaseGrant{Term: uint64(kv.ModRevision), LeaderID: nodeID, ExpiresAt: c.now().Add(time.Duration(ttl.TTL) * time.Second), Fenced: true}, nil
}

func (c *Coordinator) key(groupID format.UUID) string {
	return c.prefix + "/groups/" + hex.EncodeToString(groupID[:]) + "/leader"
}

func parseNode(value string) (format.UUID, error) {
	var id format.UUID
	if len(value) != 32 || strings.ToLower(value) != value {
		return id, fmt.Errorf("coordinator node identity is invalid")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return id, err
	}
	copy(id[:], decoded)
	if zeroUUID(id) {
		return id, fmt.Errorf("coordinator node identity is zero")
	}
	return id, nil
}

func zeroUUID(id format.UUID) bool { return id == format.UUID{} }
