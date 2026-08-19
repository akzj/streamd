package node

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	streamdv1 "github.com/akzj/streamd/api/streamd/v1"
	"github.com/akzj/streamd/internal/access"
	etcdcoordinator "github.com/akzj/streamd/internal/coordinator/etcd"
	"github.com/akzj/streamd/internal/diagnostics"
	"github.com/akzj/streamd/internal/leadership"
	"github.com/akzj/streamd/internal/observe"
	"github.com/akzj/streamd/internal/replication"
	"github.com/akzj/streamd/internal/service"
	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/identity"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/snapshot"
	"github.com/akzj/streamd/internal/storage/wal"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type currentCoordinator interface {
	leadership.Coordinator
	Current(context.Context, format.UUID) (leadership.LeaseGrant, error)
}

type standbyRecoveryRequired struct {
	task  diagnostics.RecoveryTask
	cause error
}

func (e *standbyRecoveryRequired) Error() string { return e.cause.Error() }
func (e *standbyRecoveryRequired) Unwrap() error { return e.cause }

func runReplicated(ctx context.Context, config Config, logger *slog.Logger) error {
	serverCredentials, err := config.serverCredentials()
	if err != nil {
		return fmt.Errorf("load mTLS credentials: %w", err)
	}
	if err = resumePendingSnapshotInstall(config.DataDirectory, logger); err != nil {
		return err
	}
	etcdClient, coordinator, err := openCoordinator(config)
	if err != nil {
		return err
	}
	defer etcdClient.Close()
	identityValue, _ := config.nodeIdentity()
	role := config.Replication.Role
	admin, err := startAdminRuntime(config, diagnostics.ProviderFunc(func() diagnostics.Snapshot {
		return diagnostics.StartingSnapshot(role, 0, time.Time{}, diagnostics.ReasonLeadershipPending)
	}))
	if err != nil {
		return fmt.Errorf("start Admin runtime: %w", err)
	}
	logger.Info("streamd startup diagnostics started", "role", role, "admin_address", admin.listener.Addr().String())
	roleCtx, cancelRole := context.WithCancel(ctx)
	adminResult := admin.monitor(roleCtx, cancelRole)
	var roleErr error
	if config.Replication.Role == "standby" {
		roleErr = runStandby(roleCtx, config, identityValue, serverCredentials, coordinator, admin, logger)
	} else {
		roleErr = runPrimary(roleCtx, config, identityValue, serverCredentials, coordinator, admin, logger)
	}
	cancelRole()
	adminErr := <-adminResult
	shutdownErr := admin.shutdown(config)
	return errors.Join(roleErr, adminErr, shutdownErr)
}

func runPrimary(ctx context.Context, config Config, nodeIdentity format.NodeIdentity, serverCredentials credentials.TransportCredentials, coordinator currentCoordinator, admin *adminRuntime, logger *slog.Logger) error {
	if err := ensureRecoveringState(config.DataDirectory, nodeIdentity); err != nil {
		return err
	}
	_, _, renewInterval, _ := config.Replication.durations()
	grant, err := awaitPrimaryGrant(ctx, renewInterval, coordinator, nodeIdentity.GroupID, nodeIdentity.NodeID)
	if err != nil {
		return fmt.Errorf("acquire Primary Lease: %w", err)
	}
	_, safety, _, _ := config.Replication.durations()
	releaseNeeded := true
	defer func() {
		if releaseNeeded {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = coordinator.Release(releaseCtx, nodeIdentity.GroupID, nodeIdentity.NodeID, grant.Term)
		}
	}()
	if _, err = replication.Promote(config.DataDirectory, nodeIdentity, replication.PromotionGrant{Term: grant.Term, LeaderID: nodeIdentity.NodeID, ExpiresAt: grant.ExpiresAt, Fenced: grant.Fenced, SafetyMargin: safety}); err != nil {
		return fmt.Errorf("promote Primary: %w", err)
	}
	states, err := replicationstate.Open(config.DataDirectory, nodeIdentity)
	if err != nil {
		return err
	}
	persist, err := leadership.ReplicationStatePersistence(states, time.Now)
	if err != nil {
		return err
	}
	initial := leadership.State{Role: leadership.RolePrimary, Term: grant.Term, LeaderID: nodeIdentity.NodeID, ExpiresAt: grant.ExpiresAt, Fenced: true}
	controller, err := leadership.New(coordinator, leadership.Options{GroupID: nodeIdentity.GroupID, NodeID: nodeIdentity.NodeID, KnownTerm: grant.Term, SafetyMargin: safety, Persist: persist, Initial: &initial})
	if err != nil {
		return err
	}
	stopRenewal, renewalDone := startLeaseRenewal(controller, renewInterval, logger)
	renewalStopped := false
	defer func() {
		if !renewalStopped {
			stopRenewal()
			<-renewalDone
		}
	}()
	if err = admin.setProvider(diagnostics.ProviderFunc(func() diagnostics.Snapshot {
		state := controller.Snapshot()
		return diagnostics.StartingSnapshot("primary", state.Term, state.ExpiresAt, diagnostics.ReasonReplicaCatchUpPending)
	})); err != nil {
		return err
	}
	peerCredentials, err := replicationClientCredentials(config)
	if err != nil {
		return err
	}
	connection, err := grpc.NewClient(config.Replication.PeerAddress, grpc.WithTransportCredentials(peerCredentials), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(replicationMessageLimit(config.Replication))), grpc.MaxCallSendMsgSize(int(replicationMessageLimit(config.Replication)))))
	if err != nil {
		return err
	}
	defer connection.Close()
	limits := replication.TransportLimits{MaxEntries: config.Replication.MaxEntries, MaxBytes: config.Replication.MaxBytes}
	rpcPeer, err := replication.NewRPCPeer(streamdv1.NewReplicationServiceClient(connection), limits)
	if err != nil {
		return err
	}
	primaryProtocol, err := replication.NewPrimary(nodeIdentity.GroupID, nodeIdentity.NodeID, grant.Term, rpcPeer)
	if err != nil {
		return err
	}
	if err = controller.CanWrite(); err != nil {
		return fmt.Errorf("Primary Lease unsafe before catch-up: %w", err)
	}
	peerNodeID, _ := parseUUID(config.Replication.PeerNodeID)
	if err = awaitStandbyCatchUp(ctx, renewInterval, controller, config.DataDirectory, nodeIdentity, peerNodeID, grant.Term, rpcPeer, primaryProtocol, states, limits); err != nil {
		var recovery *standbyRecoveryRequired
		if errors.As(err, &recovery) {
			logger.Warn("Primary blocked for Snapshot recovery", "task_id", recovery.task.TaskID, "action", recovery.task.Action, "reason", recovery.task.Reason, "target_node_id", recovery.task.TargetNodeID)
			serveErr := serveRecoveryBlocked(ctx, admin, controller, states, recovery.task, logger)
			stopRenewal()
			<-renewalDone
			renewalStopped = true
			releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 5*time.Second)
			releaseErr := controller.Release(releaseCtx)
			cancelRelease()
			if releaseErr == nil {
				releaseNeeded = false
			}
			return errors.Join(serveErr, releaseErr)
		}
		return fmt.Errorf("Standby catch-up: %w", err)
	}
	if err = controller.CanWrite(); err != nil {
		return fmt.Errorf("Primary Lease unsafe after catch-up: %w", err)
	}
	store, err := engine.OpenReplicated(config.DataDirectory, nodeIdentity, engine.ReplicationOptions{Term: grant.Term, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: primaryProtocol, Guard: controller})
	if err != nil {
		return err
	}
	commitMetrics, err := observe.NewCommitMetrics(store)
	if err != nil {
		return err
	}
	admin.registry.MustRegister(commitMetrics)
	closed := false
	defer func() {
		if !closed {
			_ = store.Close()
		}
	}()
	authorizer := newAuthorizer(config)
	sendTimeout, _ := config.subscribeSendDuration()
	leaseProvider := func() diagnostics.LeaseState {
		state := controller.Snapshot()
		return diagnostics.LeaseState{Term: state.Term, ExpiresAt: state.ExpiresAt, Unsafe: state.Role != leadership.RolePrimary || !state.Fenced || state.LastReason != ""}
	}
	streamService, err := service.NewWithOptions(store, authorizer, service.Options{SubscribeSendTimeout: sendTimeout, Limits: config.Limits, Lease: leaseProvider})
	if err != nil {
		return err
	}
	auth := replication.MTLSPeerAuthenticator{ClusterID: nodeIdentity.ClusterID, GroupID: nodeIdentity.GroupID, ExpectedNodeID: peerNodeID}
	protocolServer, err := replication.NewRPCServer(nil, func(hello replication.ReplicaHello) (replication.ReplicationPlan, error) {
		_, plan, viewErr := planReplication(config.DataDirectory, nodeIdentity, grant.Term, states, hello)
		if viewErr != nil {
			return replication.ReplicationPlan{}, viewErr
		}
		return plan, nil
	}, auth, limits)
	if err != nil {
		return err
	}
	rpcMetrics := observe.NewRPCMetrics(admin.registry)
	grpcServer := grpc.NewServer(grpc.Creds(serverCredentials), grpc.MaxRecvMsgSize(int(replicationMessageLimit(config.Replication))), grpc.MaxSendMsgSize(int(replicationMessageLimit(config.Replication))), grpc.StatsHandler(otelgrpc.NewServerHandler()), grpc.ChainUnaryInterceptor(rpcMetrics.UnaryInterceptor()), grpc.ChainStreamInterceptor(rpcMetrics.StreamInterceptor()))
	streamdv1.RegisterStreamServiceServer(grpcServer, streamService)
	streamdv1.RegisterReplicationServiceServer(grpcServer, protocolServer)
	grpcListener, serveErrors, err := serveHA(config, grpcServer)
	if err != nil {
		return err
	}
	defer grpcListener.Close()
	if err = admin.setProvider(streamService); err != nil {
		return err
	}
	backgroundCtx, stopBackground := context.WithCancel(context.Background())
	backgroundDone := make(chan struct{})
	go func() {
		defer close(backgroundDone)
		checkpoint := func() (format.Manifest, bool, error) { return store.CheckpointReplicated(states) }
		if maintenanceErr := runEngineMaintenance(backgroundCtx, config, store, states, checkpoint, logger); maintenanceErr != nil {
			logger.Error("maintenance loop stopped", "error", maintenanceErr)
		}
	}()
	logger.Info("streamd Strict Primary started", "term", grant.Term, "grpc_address", grpcListener.Addr().String(), "peer", config.Replication.PeerAddress)
	serveErr := waitServe(ctx, serveErrors)
	stopBackground()
	<-backgroundDone
	stopRenewal()
	<-renewalDone
	renewalStopped = true
	streamService.BeginDrain()
	shutdownGRPC(config, grpcServer)
	_, checkpointErr := store.CheckpointReplicationState(states)
	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 5*time.Second)
	releaseErr := controller.Release(releaseCtx)
	cancelRelease()
	if releaseErr == nil {
		releaseNeeded = false
	}
	closeErr := store.Close()
	closed = true
	return errors.Join(serveErr, checkpointErr, releaseErr, closeErr)
}

func startLeaseRenewal(controller *leadership.Controller, interval time.Duration, logger *slog.Logger) (context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(ctx, interval)
				if err := controller.Renew(renewCtx); err != nil {
					logger.Error("Primary Lease renewal failed", "error", err)
				}
				renewCancel()
			}
		}
	}()
	return cancel, done
}

func awaitPrimaryGrant(ctx context.Context, attemptTimeout time.Duration, coordinator currentCoordinator, groupID, nodeID format.UUID) (leadership.LeaseGrant, error) {
	return awaitCoordinatorGrant(ctx, attemptTimeout, func(attemptCtx context.Context) (leadership.LeaseGrant, error) {
		return coordinator.Acquire(attemptCtx, groupID, nodeID)
	}, func(err error) bool {
		return errors.Is(err, etcdcoordinator.ErrLeaderExists) || transientCoordinatorError(err)
	})
}

func awaitCurrentPrimary(ctx context.Context, attemptTimeout time.Duration, coordinator currentCoordinator, groupID format.UUID) (leadership.LeaseGrant, error) {
	return awaitCoordinatorGrant(ctx, attemptTimeout, func(attemptCtx context.Context) (leadership.LeaseGrant, error) {
		return coordinator.Current(attemptCtx, groupID)
	}, func(err error) bool {
		return errors.Is(err, etcdcoordinator.ErrNotLeader) || transientCoordinatorError(err)
	})
}

func awaitCoordinatorGrant(ctx context.Context, attemptTimeout time.Duration, operation func(context.Context) (leadership.LeaseGrant, error), retryable func(error) bool) (leadership.LeaseGrant, error) {
	if attemptTimeout <= 0 {
		attemptTimeout = 3 * time.Second
	}
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		grant, err := operation(attemptCtx)
		cancel()
		if err == nil {
			return grant, nil
		}
		if ctx.Err() != nil {
			return leadership.LeaseGrant{}, ctx.Err()
		}
		if !retryable(err) {
			return leadership.LeaseGrant{}, err
		}
		retry := time.NewTimer(min(attemptTimeout, time.Second))
		select {
		case <-ctx.Done():
			if !retry.Stop() {
				<-retry.C
			}
			return leadership.LeaseGrant{}, ctx.Err()
		case <-retry.C:
		}
	}
}

func transientCoordinatorError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded
}

func runStandby(ctx context.Context, config Config, nodeIdentity format.NodeIdentity, serverCredentials credentials.TransportCredentials, coordinator currentCoordinator, admin *adminRuntime, logger *slog.Logger) error {
	_, _, retryInterval, _ := config.Replication.durations()
	grant, err := awaitCurrentPrimary(ctx, retryInterval, coordinator, nodeIdentity.GroupID)
	if err != nil {
		return fmt.Errorf("discover Primary: %w", err)
	}
	store, err := replication.OpenStandby(config.DataDirectory, nodeIdentity, grant.Term, grant.LeaderID)
	if err != nil {
		return err
	}
	defer store.Close()
	limits := replication.TransportLimits{MaxEntries: config.Replication.MaxEntries, MaxBytes: config.Replication.MaxBytes}
	auth := replication.MTLSPeerAuthenticator{ClusterID: nodeIdentity.ClusterID, GroupID: nodeIdentity.GroupID, ExpectedNodeID: grant.LeaderID}
	protocolServer, err := replication.NewRPCServer(store.Receiver(), nil, auth, limits)
	if err != nil {
		return err
	}
	protocolServer.SetStatusProvider(store.Hello)
	diagnosticProvider, err := diagnostics.NewStandbyProvider(store.Receiver())
	if err != nil {
		return err
	}
	rpcMetrics := observe.NewRPCMetrics(admin.registry)
	grpcServer := grpc.NewServer(grpc.Creds(serverCredentials), grpc.MaxRecvMsgSize(int(replicationMessageLimit(config.Replication))), grpc.MaxSendMsgSize(int(replicationMessageLimit(config.Replication))), grpc.StatsHandler(otelgrpc.NewServerHandler()), grpc.ChainUnaryInterceptor(rpcMetrics.UnaryInterceptor()), grpc.ChainStreamInterceptor(rpcMetrics.StreamInterceptor()))
	streamdv1.RegisterReplicationServiceServer(grpcServer, protocolServer)
	grpcListener, serveErrors, err := serveHA(config, grpcServer)
	if err != nil {
		return err
	}
	defer grpcListener.Close()
	if err = admin.setProvider(diagnosticProvider); err != nil {
		return err
	}
	checkpointCtx, stopCheckpoint := context.WithCancel(context.Background())
	checkpointDone := make(chan struct{})
	go func() {
		defer close(checkpointDone)
		if maintenanceErr := runStandbyMaintenance(checkpointCtx, config, store, logger); maintenanceErr != nil {
			logger.Error("Standby maintenance loop stopped", "error", maintenanceErr)
		}
	}()
	logger.Info("streamd Strict Standby started", "term", grant.Term, "leader", fmt.Sprintf("%x", grant.LeaderID), "grpc_address", grpcListener.Addr().String())
	serveErr := waitServe(ctx, serveErrors)
	stopCheckpoint()
	<-checkpointDone
	shutdownGRPC(config, grpcServer)
	return errors.Join(serveErr, store.Close())
}

func awaitStandbyCatchUp(ctx context.Context, attemptTimeout time.Duration, controller *leadership.Controller, root string, node format.NodeIdentity, peerNodeID format.UUID, term uint64, peer *replication.RPCPeer, primary *replication.Primary, states *replicationstate.Store, limits replication.TransportLimits) error {
	if attemptTimeout <= 0 {
		attemptTimeout = 3 * time.Second
	}
	for {
		if err := controller.CanWrite(); err != nil {
			return err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		err := catchUpStandby(attemptCtx, root, node, peerNodeID, term, peer, primary, states, limits)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch status.Code(err) {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
			// Standby may start only after discovering this node's Lease. Retry
			// transport availability failures, while protocol and data errors
			// remain fail-closed for operator action.
		default:
			return err
		}
		retry := time.NewTimer(min(attemptTimeout, time.Second))
		select {
		case <-ctx.Done():
			if !retry.Stop() {
				<-retry.C
			}
			return ctx.Err()
		case <-retry.C:
		}
	}
}

func catchUpStandby(ctx context.Context, root string, node format.NodeIdentity, peerNodeID format.UUID, term uint64, peer *replication.RPCPeer, primary *replication.Primary, states *replicationstate.Store, limits replication.TransportLimits) error {
	hello, err := peer.Status(ctx, node.GroupID, node.NodeID, term)
	if err != nil {
		return err
	}
	if hello.NodeID != peerNodeID {
		return fmt.Errorf("Standby status node_id does not match configured peer_node_id")
	}
	view, plan, err := planReplication(root, node, term, states, hello)
	if err != nil {
		if replication.IsCode(err, replication.ErrNoRecoverySource) {
			return newStandbyRecoveryRequired(diagnostics.RecoveryNoRecoverySource, view, hello, replication.ReplicationPlan{}, err)
		}
		if replication.IsCode(err, replication.ErrLogDiverged) {
			return newStandbyRecoveryRequired(diagnostics.RecoveryLogDiverged, view, hello, replication.ReplicationPlan{}, err)
		}
		return err
	}
	if plan.Mode == replication.PlanSnapshot {
		return newStandbyRecoveryRequired(diagnostics.RecoverySnapshotOffered, view, hello, plan, fmt.Errorf("Standby requires Snapshot %x before it can join", plan.SnapshotID))
	}
	if !view.LocalDurable.Valid || plan.StartEntryID > view.LocalDurable.EntryID {
		if view.Committed.Valid {
			return primary.AdvanceCommit(ctx, view.Committed.EntryID)
		}
		return nil
	}
	history, err := wal.OpenHistory(root)
	if err != nil {
		return err
	}
	maxEntries := limits.MaxEntries
	if maxEntries <= 0 {
		maxEntries = 1024
	}
	maxBytes := limits.MaxBytes
	if maxBytes == 0 {
		maxBytes = 16 << 20
	}
	err = primary.CatchUp(ctx, history, plan.StartEntryID, view.LocalDurable.EntryID, view.Committed, maxEntries, maxBytes)
	if replication.IsCode(err, replication.ErrNeedsSnapshot) {
		return newStandbyRecoveryRequired(diagnostics.RecoveryWALNotRetained, view, hello, plan, err)
	}
	return err
}

func newStandbyRecoveryRequired(reason diagnostics.RecoveryReason, view replication.PrimaryView, hello replication.ReplicaHello, plan replication.ReplicationPlan, cause error) error {
	task := diagnostics.RecoveryTask{
		Action: diagnostics.RecoveryCreateAndInstallSnapshot, Reason: reason, Term: view.Term,
		GroupID: fmt.Sprintf("%x", view.GroupID), SourceNodeID: fmt.Sprintf("%x", view.LeaderID), TargetNodeID: fmt.Sprintf("%x", hello.NodeID), EarliestWALEntryID: view.EarliestWAL,
	}
	if hello.LocalDurable.Valid {
		task.TargetDurableEntryID = uint64Address(hello.LocalDurable.EntryID)
		task.TargetDurableCRC32C = uint32Address(hello.LocalDurable.CRC32C)
	}
	checkpoint := plan.Checkpoint
	snapshotID := plan.SnapshotID
	if snapshotID == (format.UUID{}) && view.Snapshot != nil {
		snapshotID, checkpoint = view.Snapshot.SnapshotID, view.Snapshot.Checkpoint
	}
	if snapshotID != (format.UUID{}) && checkpoint.Valid {
		task.Action = diagnostics.RecoveryInstallSnapshot
		task.SnapshotID = fmt.Sprintf("%x", snapshotID)
		task.SnapshotCheckpoint = uint64Address(checkpoint.EntryID)
	}
	snapshotCheckpoint, targetDurable, targetCRC := "-", "-", "-"
	if task.SnapshotCheckpoint != nil {
		snapshotCheckpoint = fmt.Sprint(*task.SnapshotCheckpoint)
	}
	if task.TargetDurableEntryID != nil {
		targetDurable = fmt.Sprint(*task.TargetDurableEntryID)
	}
	if task.TargetDurableCRC32C != nil {
		targetCRC = fmt.Sprint(*task.TargetDurableCRC32C)
	}
	identity := fmt.Sprintf("v1|%s|%s|%s|%s|%d|%s|%s|%s|%d|%s|%s", task.Action, task.Reason, task.GroupID, task.SourceNodeID, task.Term, task.TargetNodeID, task.SnapshotID, snapshotCheckpoint, task.EarliestWALEntryID, targetDurable, targetCRC)
	digest := sha256.Sum256([]byte(identity))
	task.TaskID = fmt.Sprintf("%x", digest)
	return &standbyRecoveryRequired{task: task, cause: cause}
}

func uint64Address(value uint64) *uint64 { return &value }
func uint32Address(value uint32) *uint32 { return &value }

func primaryView(root string, node format.NodeIdentity, term uint64, states *replicationstate.Store) (replication.PrimaryView, error) {
	state, ok := states.Current()
	if !ok {
		return replication.PrimaryView{}, fmt.Errorf("Primary Replication State is missing")
	}
	history, err := wal.OpenHistory(root)
	if err != nil {
		return replication.PrimaryView{}, err
	}
	earliest, _, present := history.Bounds()
	if !present {
		earliest = state.Header.EarliestWALEntryID
	}
	checksumAt := func(entryID uint64) (uint32, bool) {
		checksum, found, lookupErr := history.ChecksumAt(entryID)
		if lookupErr == nil && found {
			return checksum, true
		}
		if state.Header.HasInstalledSnapshot && state.Header.InstalledSnapshotEntry.EntryID == entryID {
			return state.Header.InstalledSnapshotEntry.CRC32C, true
		}
		return 0, false
	}
	view := replication.PrimaryView{GroupID: node.GroupID, LeaderID: node.NodeID, Term: term, EarliestWAL: earliest, LastAppended: replicationPosition(state.Header.LastAppended), LocalDurable: replicationPosition(state.Header.LocalDurable), Committed: replicationPosition(state.Header.Committed), ChecksumAt: checksumAt}
	return view, nil
}

func planReplication(root string, node format.NodeIdentity, term uint64, states *replicationstate.Store, hello replication.ReplicaHello) (replication.PrimaryView, replication.ReplicationPlan, error) {
	view, err := primaryView(root, node, term, states)
	if err != nil {
		return replication.PrimaryView{}, replication.ReplicationPlan{}, err
	}
	plan, err := replication.Plan(view, hello)
	needsSource := replication.IsCode(err, replication.ErrNoRecoverySource)
	if !needsSource && !replication.IsCode(err, replication.ErrLogDiverged) {
		return view, plan, err
	}
	state, ok := states.Current()
	if !ok || !state.Header.HasInstalledSnapshot {
		return view, plan, err
	}
	available, found, findErr := snapshot.FindAvailable(root, state.Header.InstalledSnapshotID, node.GroupID, state.Header.InstalledSnapshotEntry.EntryID, state.Header.InstalledSnapshotEntry.CRC32C)
	if findErr != nil {
		return view, replication.ReplicationPlan{}, fmt.Errorf("discover installable Snapshot: %w", findErr)
	}
	if !found {
		return view, plan, err
	}
	view.Snapshot = &replication.InstallableSnapshot{SnapshotID: available.SnapshotID, Checkpoint: replicationPosition(state.Header.InstalledSnapshotEntry)}
	if !needsSource {
		return view, plan, err
	}
	plan, err = replication.Plan(view, hello)
	return view, plan, err
}

func replicationPosition(value format.ReplicationPosition) replication.Position {
	return replication.Position{Valid: value.Present, EntryID: value.EntryID, CRC32C: value.CRC32C}
}

func ensureRecoveringState(rootPath string, node format.NodeIdentity) error {
	root, err := fsutil.OpenRoot(rootPath)
	if err != nil {
		return err
	}
	defer root.Close()
	if _, err = identity.Ensure(root.Path(), node); err != nil {
		return err
	}
	states, err := replicationstate.Open(root.Path(), node)
	if err != nil {
		return err
	}
	if _, ok := states.Current(); ok {
		return nil
	}
	_, err = states.Update(time.Now(), func(header *format.ReplicationStateHeader) error {
		header.Role = format.ReplicationRoleRecovering
		header.Durability = format.ReplicationDurabilityStrict
		return nil
	})
	return err
}

func openCoordinator(config Config) (*clientv3.Client, *etcdcoordinator.Coordinator, error) {
	tlsConfig, err := clientTLS(config.Replication.Etcd.CertificateFile, config.Replication.Etcd.PrivateKeyFile, config.Replication.Etcd.CAFile, config.Replication.Etcd.ServerName)
	if err != nil {
		return nil, nil, err
	}
	dialTimeout := 5 * time.Second
	if config.Replication.Etcd.DialTimeout != "" {
		dialTimeout, err = time.ParseDuration(config.Replication.Etcd.DialTimeout)
		if err != nil || dialTimeout <= 0 {
			return nil, nil, fmt.Errorf("replication.etcd.dial_timeout is invalid")
		}
	}
	etcdCredentials := fixedServerNameCredentials{TransportCredentials: credentials.NewTLS(tlsConfig), serverName: config.Replication.Etcd.ServerName}
	client, err := clientv3.New(clientv3.Config{Endpoints: config.Replication.Etcd.Endpoints, DialTimeout: dialTimeout, TLS: tlsConfig, DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(etcdCredentials)}})
	if err != nil {
		return nil, nil, err
	}
	ttl, _, _, _ := config.Replication.durations()
	coordinator, err := etcdcoordinator.New(client, config.Replication.Etcd.Prefix, ttl)
	if err != nil {
		client.Close()
		return nil, nil, err
	}
	return client, coordinator, nil
}

func replicationClientCredentials(config Config) (credentials.TransportCredentials, error) {
	tlsConfig, err := clientTLS(config.TLS.CertificateFile, config.TLS.PrivateKeyFile, config.TLS.ClientCAFile, config.Replication.PeerServerName)
	if err != nil {
		return nil, err
	}
	identityValue, _ := config.nodeIdentity()
	peerNodeID, err := parseUUID(config.Replication.PeerNodeID)
	if err != nil {
		return nil, err
	}
	wantURI := replication.NodeURI(identityValue.ClusterID, identityValue.GroupID, peerNodeID)
	tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
			return fmt.Errorf("replication peer certificate is not verified")
		}
		for _, identityURI := range state.PeerCertificates[0].URIs {
			if identityURI.String() == wantURI {
				return nil
			}
		}
		return fmt.Errorf("replication peer certificate URI SAN does not match peer_node_id")
	}
	return fixedServerNameCredentials{TransportCredentials: credentials.NewTLS(tlsConfig), serverName: config.Replication.PeerServerName}, nil
}

// fixedServerNameCredentials keeps DNS authentication independent from the
// dial target. gRPC otherwise replaces tls.Config.ServerName with the
// resolver address, which is incorrect when a controlled proxy or load
// balancer is the transport endpoint.
type fixedServerNameCredentials struct {
	credentials.TransportCredentials
	serverName string
}

func (c fixedServerNameCredentials) ClientHandshake(ctx context.Context, _ string, connection net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return c.TransportCredentials.ClientHandshake(ctx, c.serverName, connection)
}

func (c fixedServerNameCredentials) Clone() credentials.TransportCredentials {
	return fixedServerNameCredentials{TransportCredentials: c.TransportCredentials.Clone(), serverName: c.serverName}
}

func (c fixedServerNameCredentials) OverrideServerName(serverName string) error {
	if serverName != c.serverName {
		return fmt.Errorf("TLS server name is fixed to %q", c.serverName)
	}
	return nil
}

func clientTLS(certificateFile, keyFile, caFile, serverName string) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(certificateFile, keyFile)
	if err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA file contains no certificates")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: roots, ServerName: serverName}, nil
}

func newAuthorizer(config Config) access.Controller {
	return access.Controller{Authenticator: access.MTLSAuthenticator{PrincipalsByURI: config.PrincipalsByURI}, Policy: access.StaticPolicy{Rules: config.Authorization}}
}

func serveHA(config Config, grpcServer *grpc.Server) (net.Listener, <-chan error, error) {
	grpcListener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return nil, nil, err
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- grpcServer.Serve(grpcListener) }()
	return grpcListener, serveErrors, nil
}

func serveRecoveryBlocked(ctx context.Context, admin *adminRuntime, controller *leadership.Controller, states *replicationstate.Store, task diagnostics.RecoveryTask, logger *slog.Logger) error {
	provider := diagnostics.ProviderFunc(func() diagnostics.Snapshot {
		current, ok := states.Current()
		if !ok {
			return diagnostics.Snapshot{SchemaVersion: "v1", Status: diagnostics.StatusFailed, Role: "recovering", Durability: "replicated_strict", Term: task.Term, Reasons: []diagnostics.Reason{{Code: diagnostics.ReasonStateInconsistent, Message: "runtime state is internally inconsistent"}}}
		}
		state := controller.Snapshot()
		lease := diagnostics.LeaseState{Term: state.Term, ExpiresAt: state.ExpiresAt, Unsafe: state.Role != leadership.RolePrimary || !state.Fenced || state.LastReason != ""}
		return diagnostics.RecoveryBlockedSnapshot(current.Header, task, lease)
	})
	if err := admin.setProvider(provider); err != nil {
		return err
	}
	logger.Warn("streamd recovery diagnostics started", "admin_address", admin.listener.Addr().String(), "task_id", task.TaskID)
	<-ctx.Done()
	return nil
}

func waitServe(ctx context.Context, serveErrors <-chan error) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-serveErrors:
		if errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func shutdownGRPC(config Config, grpcServer *grpc.Server) {
	timeout, _ := config.shutdownDuration()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		grpcServer.Stop()
		<-done
	}
}

func replicationMessageLimit(config ReplicationConfig) uint64 {
	value := config.MaxBytes
	if value == 0 {
		value = 16 << 20
	}
	return value + (1 << 20)
}
