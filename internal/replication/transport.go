package replication

import (
	"context"
	"fmt"

	streamdv1 "github.com/akzj/streamd/api/streamd/v1"
	"github.com/akzj/streamd/internal/storage/format"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const ProtocolVersion uint32 = 1

type TransportLimits struct {
	MaxEntries int
	MaxBytes   uint64
}

func (l TransportLimits) withDefaults() TransportLimits {
	if l.MaxEntries <= 0 {
		l.MaxEntries = 1024
	}
	if l.MaxBytes == 0 {
		l.MaxBytes = 16 << 20
	}
	return l
}

type PeerAuthenticator interface {
	Authenticate(context.Context, format.UUID, format.UUID) error
}

type PlanProvider func(ReplicaHello) (ReplicationPlan, error)

type RPCServer struct {
	streamdv1.UnimplementedReplicationServiceServer
	receiver *Receiver
	planner  PlanProvider
	auth     PeerAuthenticator
	limits   TransportLimits
}

func NewRPCServer(receiver *Receiver, planner PlanProvider, auth PeerAuthenticator, limits TransportLimits) (*RPCServer, error) {
	if auth == nil || (receiver == nil && planner == nil) {
		return nil, protocolError(ErrInvalidState, "replication receiver or planner and peer authentication are required")
	}
	return &RPCServer{receiver: receiver, planner: planner, auth: auth, limits: limits.withDefaults()}, nil
}

func (s *RPCServer) Negotiate(ctx context.Context, request *streamdv1.ReplicationServiceNegotiateRequest) (*streamdv1.ReplicationServiceNegotiateResponse, error) {
	if request == nil || request.ProtocolVersion != ProtocolVersion || s.planner == nil {
		return nil, status.Error(codes.FailedPrecondition, "replication negotiation is unavailable or incompatible")
	}
	groupID, err := wireUUID(request.GroupId)
	if err != nil {
		return nil, rpcError(err)
	}
	nodeID, err := wireUUID(request.NodeId)
	if err != nil {
		return nil, rpcError(err)
	}
	if err = s.auth.Authenticate(ctx, groupID, nodeID); err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	snapshotID, err := optionalWireUUID(request.InstalledSnapshotId)
	if err != nil {
		return nil, rpcError(err)
	}
	plan, err := s.planner(ReplicaHello{GroupID: groupID, NodeID: nodeID, KnownTerm: request.KnownTerm, InstalledSnapshotID: snapshotID, Snapshot: fromWirePosition(request.Snapshot), LastAppended: fromWirePosition(request.LastAppended), LocalDurable: fromWirePosition(request.LocalDurable), Committed: fromWirePosition(request.Committed), Applied: fromWirePosition(request.Applied)})
	if err != nil {
		return nil, rpcError(err)
	}
	mode := streamdv1.ReplicationPlanMode_REPLICATION_PLAN_MODE_INCREMENTAL
	if plan.Mode == PlanSnapshot {
		mode = streamdv1.ReplicationPlanMode_REPLICATION_PLAN_MODE_SNAPSHOT
	}
	return &streamdv1.ReplicationServiceNegotiateResponse{ProtocolVersion: ProtocolVersion, Term: plan.Term, LeaderId: plan.LeaderID[:], Mode: mode, StartEntryId: plan.StartEntryID, SnapshotId: optionalUUIDBytes(plan.SnapshotID), Checkpoint: toWirePosition(plan.Checkpoint), EarliestWalEntryId: plan.EarliestWAL, Committed: toWirePosition(plan.Committed), GroupId: groupID[:]}, nil
}

func (s *RPCServer) Append(ctx context.Context, request *streamdv1.ReplicationServiceAppendRequest) (*streamdv1.ReplicationServiceAppendResponse, error) {
	if request == nil || request.ProtocolVersion != ProtocolVersion || s.receiver == nil {
		return nil, status.Error(codes.FailedPrecondition, "replication receiver is unavailable or incompatible")
	}
	if err := s.checkBatch(request.Entries); err != nil {
		return nil, rpcError(err)
	}
	groupID, leaderID, err := s.sender(ctx, request.GroupId, request.LeaderId)
	if err != nil {
		return nil, err
	}
	if err = s.receiver.Append(AppendEntries{GroupID: groupID, Term: request.Term, LeaderID: leaderID, Previous: fromWirePosition(request.Previous), Entries: request.Entries}); err != nil {
		return nil, rpcError(err)
	}
	return &streamdv1.ReplicationServiceAppendResponse{ProtocolVersion: ProtocolVersion, GroupId: groupID[:], NodeId: s.receiver.nodeID[:], Term: request.Term}, nil
}

func (s *RPCServer) Barrier(ctx context.Context, request *streamdv1.ReplicationServiceBarrierRequest) (*streamdv1.ReplicationServiceBarrierResponse, error) {
	if request == nil || request.ProtocolVersion != ProtocolVersion || s.receiver == nil {
		return nil, status.Error(codes.FailedPrecondition, "replication receiver is unavailable or incompatible")
	}
	groupID, leaderID, err := s.sender(ctx, request.GroupId, request.LeaderId)
	if err != nil {
		return nil, err
	}
	ack, err := s.receiver.Barrier(DurabilityBarrier{GroupID: groupID, Term: request.Term, LeaderID: leaderID, ThroughEntryID: request.ThroughEntryId})
	if err != nil {
		return nil, rpcError(err)
	}
	return &streamdv1.ReplicationServiceBarrierResponse{Term: ack.Term, Durable: toWirePosition(ack.Durable), ProtocolVersion: ProtocolVersion, GroupId: groupID[:], NodeId: s.receiver.nodeID[:]}, nil
}

func (s *RPCServer) AdvanceCommit(ctx context.Context, request *streamdv1.ReplicationServiceAdvanceCommitRequest) (*streamdv1.ReplicationServiceAdvanceCommitResponse, error) {
	if request == nil || request.ProtocolVersion != ProtocolVersion || s.receiver == nil {
		return nil, status.Error(codes.FailedPrecondition, "replication receiver is unavailable or incompatible")
	}
	groupID, leaderID, err := s.sender(ctx, request.GroupId, request.LeaderId)
	if err != nil {
		return nil, err
	}
	if err = s.receiver.AdvanceCommit(CommitAdvance{GroupID: groupID, Term: request.Term, LeaderID: leaderID, CommitEntryID: request.CommitEntryId}); err != nil {
		return nil, rpcError(err)
	}
	return &streamdv1.ReplicationServiceAdvanceCommitResponse{ProtocolVersion: ProtocolVersion, GroupId: groupID[:], NodeId: s.receiver.nodeID[:], Term: request.Term}, nil
}

func (s *RPCServer) sender(ctx context.Context, group, leader []byte) (format.UUID, format.UUID, error) {
	groupID, err := wireUUID(group)
	if err != nil {
		return format.UUID{}, format.UUID{}, rpcError(err)
	}
	leaderID, err := wireUUID(leader)
	if err != nil {
		return format.UUID{}, format.UUID{}, rpcError(err)
	}
	if err = s.auth.Authenticate(ctx, groupID, leaderID); err != nil {
		return format.UUID{}, format.UUID{}, status.Error(codes.Unauthenticated, err.Error())
	}
	return groupID, leaderID, nil
}

func (s *RPCServer) checkBatch(entries [][]byte) error {
	if len(entries) == 0 || len(entries) > s.limits.MaxEntries {
		return protocolError(ErrInvalidState, "replication batch Entry count exceeds its bound")
	}
	var bytes uint64
	for _, entry := range entries {
		if uint64(len(entry)) > s.limits.MaxBytes-bytes {
			return protocolError(ErrInvalidState, "replication batch bytes exceed their bound")
		}
		bytes += uint64(len(entry))
	}
	return nil
}

type RPCPeer struct {
	client streamdv1.ReplicationServiceClient
	limits TransportLimits
}

func NewRPCPeer(client streamdv1.ReplicationServiceClient, limits TransportLimits) (*RPCPeer, error) {
	if client == nil {
		return nil, protocolError(ErrInvalidState, "replication RPC client is required")
	}
	return &RPCPeer{client: client, limits: limits.withDefaults()}, nil
}

func (p *RPCPeer) Append(ctx context.Context, message AppendEntries) error {
	if err := (&RPCServer{limits: p.limits}).checkBatch(message.Entries); err != nil {
		return err
	}
	response, err := p.client.Append(ctx, &streamdv1.ReplicationServiceAppendRequest{ProtocolVersion: ProtocolVersion, GroupId: message.GroupID[:], Term: message.Term, LeaderId: message.LeaderID[:], Previous: toWirePosition(message.Previous), Entries: message.Entries})
	if err != nil {
		return err
	}
	return validateWireResponse(response.ProtocolVersion, response.GroupId, response.NodeId, response.Term, message.GroupID, message.Term)
}

func (p *RPCPeer) Barrier(ctx context.Context, message DurabilityBarrier) (DurableAck, error) {
	response, err := p.client.Barrier(ctx, &streamdv1.ReplicationServiceBarrierRequest{ProtocolVersion: ProtocolVersion, GroupId: message.GroupID[:], Term: message.Term, LeaderId: message.LeaderID[:], ThroughEntryId: message.ThroughEntryID})
	if err != nil {
		return DurableAck{}, err
	}
	if err = validateWireResponse(response.ProtocolVersion, response.GroupId, response.NodeId, response.Term, message.GroupID, message.Term); err != nil {
		return DurableAck{}, err
	}
	return DurableAck{Term: response.Term, Durable: fromWirePosition(response.Durable)}, nil
}

func (p *RPCPeer) AdvanceCommit(ctx context.Context, message CommitAdvance) error {
	response, err := p.client.AdvanceCommit(ctx, &streamdv1.ReplicationServiceAdvanceCommitRequest{ProtocolVersion: ProtocolVersion, GroupId: message.GroupID[:], Term: message.Term, LeaderId: message.LeaderID[:], CommitEntryId: message.CommitEntryID})
	if err != nil {
		return err
	}
	return validateWireResponse(response.ProtocolVersion, response.GroupId, response.NodeId, response.Term, message.GroupID, message.Term)
}

func validateWireResponse(version uint32, group, node []byte, term uint64, wantGroup format.UUID, wantTerm uint64) error {
	groupID, err := wireUUID(group)
	if err != nil {
		return err
	}
	if _, err = wireUUID(node); err != nil {
		return err
	}
	if version != ProtocolVersion || groupID != wantGroup || term != wantTerm {
		return protocolError(ErrInvalidState, "replication response identity or protocol version does not match request")
	}
	return nil
}

func wireUUID(value []byte) (format.UUID, error) {
	var id format.UUID
	if len(value) != len(id) {
		return id, protocolError(ErrInvalidState, "wire UUID must contain 16 bytes")
	}
	copy(id[:], value)
	if zeroUUID(id) {
		return id, protocolError(ErrInvalidState, "wire UUID cannot be zero")
	}
	return id, nil
}

func optionalWireUUID(value []byte) (format.UUID, error) {
	if len(value) == 0 {
		return format.UUID{}, nil
	}
	return wireUUID(value)
}

func optionalUUIDBytes(id format.UUID) []byte {
	if zeroUUID(id) {
		return nil
	}
	return id[:]
}

func toWirePosition(position Position) *streamdv1.ReplicationPosition {
	return &streamdv1.ReplicationPosition{Valid: position.Valid, EntryId: position.EntryID, Crc32C: position.CRC32C}
}

func fromWirePosition(position *streamdv1.ReplicationPosition) Position {
	if position == nil {
		return Position{}
	}
	return Position{Valid: position.Valid, EntryID: position.EntryId, CRC32C: position.Crc32C}
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	code := codes.FailedPrecondition
	if !IsCode(err, ErrInvalidState) && !IsCode(err, ErrWrongGroup) && !IsCode(err, ErrTermStale) && !IsCode(err, ErrNotLeader) && !IsCode(err, ErrLogGap) && !IsCode(err, ErrLogDiverged) && !IsCode(err, ErrNoRecoverySource) {
		return status.Error(codes.Internal, "replication operation failed")
	}
	if IsCode(err, ErrInvalidState) || IsCode(err, ErrWrongGroup) {
		code = codes.InvalidArgument
	} else if IsCode(err, ErrLogGap) {
		code = codes.OutOfRange
	} else if IsCode(err, ErrLogDiverged) {
		code = codes.DataLoss
	}
	return status.Error(code, fmt.Sprintf("replication: %v", err))
}
