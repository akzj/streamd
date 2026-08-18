package node

import (
	"context"
	"errors"
	"testing"
	"time"

	etcdcoordinator "github.com/akzj/streamd/internal/coordinator/etcd"
	"github.com/akzj/streamd/internal/leadership"
)

func TestAwaitCoordinatorGrantRetriesOnlyTransientFailures(t *testing.T) {
	attempts := 0
	want := leadership.LeaseGrant{Term: 7}
	grant, err := awaitCoordinatorGrant(context.Background(), time.Millisecond, func(context.Context) (leadership.LeaseGrant, error) {
		attempts++
		if attempts < 3 {
			return leadership.LeaseGrant{}, etcdcoordinator.ErrNotLeader
		}
		return want, nil
	}, func(err error) bool { return errors.Is(err, etcdcoordinator.ErrNotLeader) })
	if err != nil || grant.Term != want.Term || attempts != 3 {
		t.Fatalf("grant = %+v, attempts = %d, error = %v", grant, attempts, err)
	}

	terminal := errors.New("invalid coordinator state")
	attempts = 0
	_, err = awaitCoordinatorGrant(context.Background(), time.Millisecond, func(context.Context) (leadership.LeaseGrant, error) {
		attempts++
		return leadership.LeaseGrant{}, terminal
	}, func(error) bool { return false })
	if !errors.Is(err, terminal) || attempts != 1 {
		t.Fatalf("terminal attempts = %d, error = %v", attempts, err)
	}
}

func TestAwaitCoordinatorGrantStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempted := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		_, err := awaitCoordinatorGrant(ctx, time.Second, func(context.Context) (leadership.LeaseGrant, error) {
			attempted <- struct{}{}
			return leadership.LeaseGrant{}, etcdcoordinator.ErrNotLeader
		}, func(error) bool { return true })
		done <- err
	}()
	<-attempted
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("coordinator wait did not stop")
	}
}
