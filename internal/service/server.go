package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	streamdv1 "github.com/akzj/streamd/api/streamd/v1"
	"github.com/akzj/streamd/internal/access"
	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	readstore "github.com/akzj/streamd/internal/storage/read"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Backend interface {
	Append(context.Context, engine.AppendRequest) (engine.AppendResult, error)
	Read(namespace, stream string, from uint64, maxRecords int, maxBytes uint64) (readstore.Result, error)
	Inspect(namespace, stream string) (readstore.StreamInfo, error)
	ResolveTime(namespace, stream string, target int64, mode readstore.TimeMode) (uint64, int64, bool, error)
	Health() engine.Health
	WaitForAppend(context.Context, string, string, uint64) error
}

type Server struct {
	streamdv1.UnimplementedStreamServiceServer
	backend     Backend
	authorizer  access.Authorizer
	sendTimeout time.Duration
	limits      Limits
	subMu       sync.Mutex
	subs        map[subscriptionKey]uint32
	rateMu      sync.Mutex
	rateBuckets map[subscriptionKey]rateBucket
	drainCtx    context.Context
	drainCancel context.CancelFunc
	draining    atomic.Bool
}

type Options struct {
	SubscribeSendTimeout time.Duration
	Limits               Limits
}

type Limits struct {
	MaxRecordBytes                        uint64 `json:"max_record_bytes"`
	MaxBatchRecords                       uint32 `json:"max_batch_records"`
	MaxBatchBytes                         uint64 `json:"max_batch_bytes"`
	MaxReadRecords                        uint32 `json:"max_read_records"`
	MaxReadBytes                          uint64 `json:"max_read_bytes"`
	MaxSubscribeBatchRecords              uint32 `json:"max_subscribe_batch_records"`
	MaxSubscribeBatchBytes                uint64 `json:"max_subscribe_batch_bytes"`
	MaxSubscriptionsPerPrincipalNamespace uint32 `json:"max_subscriptions_per_principal_namespace"`
	WriteRequestsPerSecond                uint32 `json:"write_requests_per_second"`
	WriteRequestBurst                     uint32 `json:"write_request_burst"`
	WriteBytesPerSecond                   uint64 `json:"write_bytes_per_second"`
	WriteByteBurst                        uint64 `json:"write_byte_burst"`
}

type subscriptionKey struct {
	producer  string
	namespace string
}

type rateBucket struct {
	updated  time.Time
	requests float64
	bytes    float64
}

func New(backend Backend, authorizer access.Authorizer) (*Server, error) {
	return NewWithOptions(backend, authorizer, Options{})
}

func NewWithOptions(backend Backend, authorizer access.Authorizer, options Options) (*Server, error) {
	if backend == nil || authorizer == nil {
		return nil, fmt.Errorf("backend and Authorizer are required")
	}
	if options.SubscribeSendTimeout < 0 {
		return nil, fmt.Errorf("Subscribe Send timeout cannot be negative")
	}
	if options.SubscribeSendTimeout == 0 {
		options.SubscribeSendTimeout = 30 * time.Second
	}
	limits, err := normalizeLimits(options.Limits)
	if err != nil {
		return nil, err
	}
	drainCtx, drainCancel := context.WithCancel(context.Background())
	return &Server{backend: backend, authorizer: authorizer, sendTimeout: options.SubscribeSendTimeout, limits: limits, subs: make(map[subscriptionKey]uint32), rateBuckets: make(map[subscriptionKey]rateBucket), drainCtx: drainCtx, drainCancel: drainCancel}, nil
}

func (s *Server) BeginDrain() {
	if s.draining.CompareAndSwap(false, true) {
		s.drainCancel()
	}
}

func (s *Server) ReadyWrite() bool {
	health := s.backend.Health()
	return !s.draining.Load() && health.Fatal == nil && health.WriteUnavailable == nil
}

func (s *Server) Append(ctx context.Context, request *streamdv1.AppendRequest) (*streamdv1.AppendResponse, error) {
	if request == nil || request.Record == nil {
		return nil, invalidArgument("Append request and record are required", nil)
	}
	result, err := s.append(ctx, request.Stream, request.ExpectedSequence, request.RequestId, []*streamdv1.InputRecord{request.Record}, request.RequiredDurability)
	if err != nil {
		return nil, err
	}
	return &streamdv1.AppendResponse{
		Sequence:       result.FirstSequence,
		NextSequence:   result.NextSequence,
		RecordedAt:     timestamp(result.FirstRecordedAt),
		StorageEntryId: result.FirstEntryID,
		Deduplicated:   result.Deduplicated,
		Durability:     durabilityProto(s.backend.Health().Durability),
	}, nil
}

func (s *Server) AppendBatch(ctx context.Context, request *streamdv1.AppendBatchRequest) (*streamdv1.AppendBatchResponse, error) {
	if request == nil || len(request.Records) == 0 {
		return nil, invalidArgument("AppendBatch requires at least one record", nil)
	}
	result, err := s.append(ctx, request.Stream, request.ExpectedSequence, request.RequestId, request.Records, request.RequiredDurability)
	if err != nil {
		return nil, err
	}
	return &streamdv1.AppendBatchResponse{
		FirstSequence:       result.FirstSequence,
		NextSequence:        result.NextSequence,
		RecordCount:         result.RecordCount,
		FirstRecordedAt:     timestamp(result.FirstRecordedAt),
		LastRecordedAt:      timestamp(result.LastRecordedAt),
		FirstStorageEntryId: result.FirstEntryID,
		LastStorageEntryId:  result.LastEntryID,
		Deduplicated:        result.Deduplicated,
		Durability:          durabilityProto(s.backend.Health().Durability),
	}, nil
}

func (s *Server) append(ctx context.Context, ref *streamdv1.StreamRef, expected uint64, requestID []byte, records []*streamdv1.InputRecord, required streamdv1.Durability) (engine.AppendResult, error) {
	if s.draining.Load() {
		return engine.AppendResult{}, unavailable("server is draining", requestID)
	}
	if err := validateStream(ref); err != nil {
		return engine.AppendResult{}, invalidArgument(err.Error(), requestID)
	}
	if len(requestID) == 0 {
		return engine.AppendResult{}, invalidArgument("request_id is required", nil)
	}
	available := durabilityProto(s.backend.Health().Durability)
	if required != streamdv1.Durability_DURABILITY_UNSPECIFIED && required != streamdv1.Durability_DURABILITY_SINGLE_SYNC && required != available {
		if required == streamdv1.Durability_DURABILITY_REPLICATED_STRICT || required == streamdv1.Durability_DURABILITY_DEGRADED_LOCAL_ONLY {
			return engine.AppendResult{}, streamdStatus(codes.FailedPrecondition, streamdv1.ErrorCode_ERROR_CODE_DURABILITY_UNAVAILABLE, "required durability is unavailable", false, false, nil, nil, requestID)
		}
		return engine.AppendResult{}, invalidArgument("required_durability is invalid for Append", requestID)
	}
	principal, err := s.authorize(ctx, ref, access.Append, requestID)
	if err != nil {
		return engine.AppendResult{}, err
	}
	inputs := make([]engine.InputRecord, len(records))
	if uint64(len(records)) > uint64(s.limits.MaxBatchRecords) {
		return engine.AppendResult{}, resourceExhausted("Batch record count exceeds the configured limit", nil, requestID)
	}
	var batchBytes uint64
	for i, record := range records {
		if record == nil {
			return engine.AppendResult{}, invalidArgument(fmt.Sprintf("record %d is missing", i), requestID)
		}
		recordBytes, overflow := inputRecordSize(record)
		if overflow {
			return engine.AppendResult{}, resourceExhausted("record size overflows", nil, requestID)
		}
		if recordBytes > s.limits.MaxRecordBytes {
			return engine.AppendResult{}, recordLimit(expected+uint64(i), recordBytes, requestID)
		}
		if batchBytes > math.MaxUint64-recordBytes {
			return engine.AppendResult{}, resourceExhausted("Batch size overflows", nil, requestID)
		}
		batchBytes += recordBytes
		inputs[i] = engine.InputRecord{Headers: inputHeaders(record.Headers), Payload: slices.Clone(record.Payload)}
	}
	if batchBytes > s.limits.MaxBatchBytes {
		return engine.AppendResult{}, resourceExhausted("Batch bytes exceed the configured limit", &batchBytes, requestID)
	}
	if !s.allowWrite(principal, ref.Namespace, batchBytes) {
		return engine.AppendResult{}, resourceExhausted("write rate exceeds the configured limit", nil, requestID)
	}
	result, err := s.backend.Append(ctx, engine.AppendRequest{Namespace: ref.Namespace, Stream: ref.Stream, ExpectedSequence: expected, RequestID: slices.Clone(requestID), Producer: principal.Producer(), Records: inputs})
	if err != nil {
		return engine.AppendResult{}, mapError(err, requestID)
	}
	return result, nil
}

func (s *Server) Read(ctx context.Context, request *streamdv1.ReadRequest) (*streamdv1.ReadResponse, error) {
	if request == nil {
		return nil, invalidArgument("Read request is required", nil)
	}
	if err := validateStream(request.Stream); err != nil {
		return nil, invalidArgument(err.Error(), nil)
	}
	if request.MaxRecords == 0 || request.MaxBytes == 0 {
		return nil, invalidArgument("max_records and max_bytes must be greater than zero", nil)
	}
	if _, err := s.authorize(ctx, request.Stream, access.Read, nil); err != nil {
		return nil, err
	}
	if request.MaxRecords > s.limits.MaxReadRecords || request.MaxBytes > s.limits.MaxReadBytes {
		return nil, resourceExhausted("Read limits exceed the configured maximum", nil, nil)
	}
	return s.read(request.Stream, request.FromSequence, request.MaxRecords, request.MaxBytes)
}

func (s *Server) read(ref *streamdv1.StreamRef, from uint64, maxRecords uint32, maxBytes uint64) (*streamdv1.ReadResponse, error) {
	result, err := s.backend.Read(ref.Namespace, ref.Stream, from, int(maxRecords), 0)
	if err != nil {
		return nil, mapError(err, nil)
	}
	response := &streamdv1.ReadResponse{NextSequence: from, CurrentNextSequence: result.CurrentNextSequence}
	for _, record := range result.Records {
		stored := storedRecord(record)
		candidate := proto.Clone(response).(*streamdv1.ReadResponse)
		candidate.Records = append(candidate.Records, stored)
		candidate.NextSequence = record.Sequence + 1
		required := uint64(proto.Size(candidate))
		if required > maxBytes {
			if len(response.Records) == 0 {
				return nil, recordTooLarge(record.Sequence, required)
			}
			break
		}
		response = candidate
	}
	return response, nil
}

func (s *Server) ResolveTime(ctx context.Context, request *streamdv1.ResolveTimeRequest) (*streamdv1.ResolveTimeResponse, error) {
	if request == nil || request.RecordedAt == nil {
		return nil, invalidArgument("ResolveTime request and recorded_at are required", nil)
	}
	if err := validateStream(request.Stream); err != nil {
		return nil, invalidArgument(err.Error(), nil)
	}
	if err := request.RecordedAt.CheckValid(); err != nil {
		return nil, invalidArgument("recorded_at is invalid", nil)
	}
	if _, err := s.authorize(ctx, request.Stream, access.Read, nil); err != nil {
		return nil, err
	}
	var mode readstore.TimeMode
	switch request.Mode {
	case streamdv1.ResolveTimeMode_RESOLVE_TIME_MODE_AT_OR_AFTER:
		mode = readstore.AtOrAfter
	case streamdv1.ResolveTimeMode_RESOLVE_TIME_MODE_AT_OR_BEFORE:
		mode = readstore.AtOrBefore
	default:
		return nil, invalidArgument("ResolveTime mode is required", nil)
	}
	sequence, recordedAt, found, err := s.backend.ResolveTime(request.Stream.Namespace, request.Stream.Stream, request.RecordedAt.AsTime().UnixNano(), mode)
	if err != nil {
		return nil, mapError(err, nil)
	}
	response := &streamdv1.ResolveTimeResponse{Found: found}
	if found {
		response.Sequence = sequence
		response.ActualRecordedAt = timestamp(recordedAt)
	}
	return response, nil
}

func (s *Server) InspectStream(ctx context.Context, request *streamdv1.InspectStreamRequest) (*streamdv1.InspectStreamResponse, error) {
	if request == nil {
		return nil, invalidArgument("InspectStream request is required", nil)
	}
	if err := validateStream(request.Stream); err != nil {
		return nil, invalidArgument(err.Error(), nil)
	}
	if _, err := s.authorize(ctx, request.Stream, access.Inspect, nil); err != nil {
		return nil, err
	}
	info, err := s.backend.Inspect(request.Stream.Namespace, request.Stream.Stream)
	if err != nil {
		return nil, mapError(err, nil)
	}
	response := &streamdv1.InspectStreamResponse{Exists: info.Exists, NextSequence: info.NextSequence, RecordCount: info.RecordCount}
	if info.Exists && info.RecordCount > 0 {
		response.FirstRecordedAt = timestamp(info.FirstRecordedAt)
		response.LastRecordedAt = timestamp(info.LastRecordedAt)
	}
	return response, nil
}

func (s *Server) Health(context.Context, *streamdv1.HealthRequest) (*streamdv1.HealthResponse, error) {
	health := s.backend.Health()
	response := &streamdv1.HealthResponse{
		Status:     streamdv1.HealthStatus_HEALTH_STATUS_READY_WRITE,
		Role:       roleProto(health.Role),
		Durability: durabilityProto(health.Durability),
	}
	if health.Term > 0 {
		response.Term = uint64ptr(health.Term)
	}
	if health.Fatal != nil {
		response.Status = streamdv1.HealthStatus_HEALTH_STATUS_FAILED
		response.Reasons = []string{"commit core failed"}
	}
	if s.draining.Load() && health.Fatal == nil {
		response.Status = streamdv1.HealthStatus_HEALTH_STATUS_READY_READ
		response.Reasons = []string{"server is draining"}
	}
	if health.WriteUnavailable != nil && health.Fatal == nil && !s.draining.Load() {
		response.Status = streamdv1.HealthStatus_HEALTH_STATUS_READY_READ
		response.Reasons = []string{health.WriteUnavailable.Error()}
	}
	if health.Watermarks.HasLocalDurable {
		response.LocalDurableEntryId = uint64ptr(health.Watermarks.LocalDurable)
	}
	if health.Watermarks.HasCommitted {
		response.CommitEntryId = uint64ptr(health.Watermarks.Committed)
	}
	if health.Watermarks.HasApplied {
		response.AppliedEntryId = uint64ptr(health.Watermarks.Applied)
	}
	if health.Watermarks.HasLocalDurable && health.Watermarks.HasReplicated && health.Watermarks.LocalDurable >= health.Watermarks.Replicated {
		response.ReplicationLagEntries = uint64ptr(health.Watermarks.LocalDurable - health.Watermarks.Replicated)
	}
	return response, nil
}

func durabilityProto(value format.ReplicationDurability) streamdv1.Durability {
	switch value {
	case format.ReplicationDurabilityStrict:
		return streamdv1.Durability_DURABILITY_REPLICATED_STRICT
	case format.ReplicationDurabilityDegraded:
		return streamdv1.Durability_DURABILITY_DEGRADED_LOCAL_ONLY
	default:
		return streamdv1.Durability_DURABILITY_SINGLE_SYNC
	}
}

func roleProto(value format.ReplicationRole) streamdv1.NodeRole {
	switch value {
	case format.ReplicationRolePrimary:
		return streamdv1.NodeRole_NODE_ROLE_PRIMARY
	case format.ReplicationRoleStandby:
		return streamdv1.NodeRole_NODE_ROLE_STANDBY
	case format.ReplicationRoleRecovering:
		return streamdv1.NodeRole_NODE_ROLE_RECOVERING
	default:
		return streamdv1.NodeRole_NODE_ROLE_SINGLE
	}
}

func validateStream(ref *streamdv1.StreamRef) error {
	if ref == nil || ref.Namespace == "" || ref.Stream == "" {
		return fmt.Errorf("stream namespace and name are required")
	}
	return nil
}

func (s *Server) authorize(ctx context.Context, ref *streamdv1.StreamRef, operation access.Operation, requestID []byte) (access.Principal, error) {
	principal, err := s.authorizer.Authorize(ctx, ref.Namespace, ref.Stream, operation)
	if err == nil {
		if validateErr := principal.Validate(); validateErr == nil {
			return principal, nil
		}
		err = access.ErrUnauthenticated
	}
	if errors.Is(err, access.ErrPermissionDenied) {
		return access.Principal{}, streamdStatus(codes.PermissionDenied, streamdv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "operation is not authorized", false, false, nil, nil, requestID)
	}
	return access.Principal{}, streamdStatus(codes.Unauthenticated, streamdv1.ErrorCode_ERROR_CODE_UNAUTHENTICATED, "authenticated identity is required", false, false, nil, nil, requestID)
}

func inputHeaders(headers map[string][]byte) []format.Header {
	out := make([]format.Header, 0, len(headers))
	for key, value := range headers {
		out = append(out, format.Header{Key: key, Value: slices.Clone(value)})
	}
	slices.SortFunc(out, func(a, b format.Header) int { return compareStrings(a.Key, b.Key) })
	return out
}

func storedRecord(record format.RecordFrame) *streamdv1.StoredRecord {
	headers := make(map[string][]byte, len(record.Headers))
	for _, header := range record.Headers {
		headers[header.Key] = slices.Clone(header.Value)
	}
	return &streamdv1.StoredRecord{Sequence: record.Sequence, RecordedAt: timestamp(record.RecordedAt), RequestId: slices.Clone(record.RequestID), Producer: record.Producer, Headers: headers, Payload: slices.Clone(record.Payload), StorageEntryId: record.EntryID}
}

func timestamp(nanos int64) *timestamppb.Timestamp { return timestamppb.New(time.Unix(0, nanos)) }
func uint64ptr(value uint64) *uint64               { return &value }

func compareStrings(left, right string) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

var _ streamdv1.StreamServiceServer = (*Server)(nil)

func (s *Server) Subscribe(request *streamdv1.SubscribeRequest, stream streamdv1.StreamService_SubscribeServer) error {
	if s.draining.Load() {
		return unavailable("server is draining", nil)
	}
	if request == nil {
		return invalidArgument("Subscribe request is required", nil)
	}
	if err := validateStream(request.Stream); err != nil {
		return invalidArgument(err.Error(), nil)
	}
	if request.MaxBatchRecords == 0 || request.MaxBatchBytes == 0 || request.HeartbeatInterval == nil {
		return invalidArgument("Subscribe limits and heartbeat_interval are required", nil)
	}
	if err := request.HeartbeatInterval.CheckValid(); err != nil || request.HeartbeatInterval.AsDuration() <= 0 {
		return invalidArgument("heartbeat_interval must be positive", nil)
	}
	principal, err := s.authorize(stream.Context(), request.Stream, access.Subscribe, nil)
	if err != nil {
		return err
	}
	if request.MaxBatchRecords > s.limits.MaxSubscribeBatchRecords || request.MaxBatchBytes > s.limits.MaxSubscribeBatchBytes {
		return resourceExhausted("Subscribe batch limits exceed the configured maximum", nil, nil)
	}
	if err = s.acquireSubscription(principal, request.Stream.Namespace); err != nil {
		return err
	}
	defer s.releaseSubscription(principal, request.Stream.Namespace)
	cursor := request.FromSequence
	heartbeat := request.HeartbeatInterval.AsDuration()
	for {
		if s.draining.Load() {
			return unavailable("server is draining", nil)
		}
		response, err := s.read(request.Stream, cursor, request.MaxBatchRecords, request.MaxBatchBytes)
		if err != nil {
			return err
		}
		if len(response.Records) > 0 {
			if err = s.send(stream, &streamdv1.SubscribeResponse{Records: response.Records, NextSequence: response.NextSequence, CurrentNextSequence: response.CurrentNextSequence}); err != nil {
				return err
			}
			cursor = response.NextSequence
			continue
		}

		waitCtx, cancel := context.WithTimeout(stream.Context(), heartbeat)
		stopDrain := context.AfterFunc(s.drainCtx, cancel)
		err = s.backend.WaitForAppend(waitCtx, request.Stream.Namespace, request.Stream.Stream, cursor)
		stopDrain()
		cancel()
		if s.draining.Load() {
			return unavailable("server is draining", nil)
		}
		if errors.Is(err, context.DeadlineExceeded) && stream.Context().Err() == nil {
			if err = s.send(stream, &streamdv1.SubscribeResponse{NextSequence: cursor, CurrentNextSequence: response.CurrentNextSequence, Heartbeat: true}); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return mapError(err, nil)
		}
	}
}

func normalizeLimits(limits Limits) (Limits, error) {
	if limits.MaxRecordBytes == 0 {
		limits.MaxRecordBytes = 16 << 20
	}
	if limits.MaxBatchRecords == 0 {
		limits.MaxBatchRecords = 1024
	}
	if limits.MaxBatchBytes == 0 {
		limits.MaxBatchBytes = 64 << 20
	}
	if limits.MaxReadRecords == 0 {
		limits.MaxReadRecords = 10_000
	}
	if limits.MaxReadBytes == 0 {
		limits.MaxReadBytes = 64 << 20
	}
	if limits.MaxSubscribeBatchRecords == 0 {
		limits.MaxSubscribeBatchRecords = 1_000
	}
	if limits.MaxSubscribeBatchBytes == 0 {
		limits.MaxSubscribeBatchBytes = 4 << 20
	}
	if limits.MaxSubscriptionsPerPrincipalNamespace == 0 {
		limits.MaxSubscriptionsPerPrincipalNamespace = 64
	}
	if limits.WriteRequestsPerSecond == 0 {
		limits.WriteRequestsPerSecond = 10_000
	}
	if limits.WriteRequestBurst == 0 {
		limits.WriteRequestBurst = limits.WriteRequestsPerSecond
	}
	if limits.WriteBytesPerSecond == 0 {
		limits.WriteBytesPerSecond = 256 << 20
	}
	if limits.WriteByteBurst == 0 {
		limits.WriteByteBurst = limits.WriteBytesPerSecond
	}
	if limits.WriteRequestBurst < 1 || limits.WriteByteBurst < 1 {
		return Limits{}, fmt.Errorf("write rate bursts must be positive")
	}
	if limits.MaxRecordBytes > format.MaxFrameLength || limits.MaxBatchRecords > format.MaxBatchRecordCount {
		return Limits{}, fmt.Errorf("service limits exceed storage format limits")
	}
	return limits, nil
}

func (s *Server) allowWrite(principal access.Principal, namespace string, bytes uint64) bool {
	key := subscriptionKey{producer: principal.Producer(), namespace: namespace}
	now := time.Now()
	s.rateMu.Lock()
	defer s.rateMu.Unlock()
	bucket, exists := s.rateBuckets[key]
	if !exists {
		bucket = rateBucket{updated: now, requests: float64(s.limits.WriteRequestBurst), bytes: float64(s.limits.WriteByteBurst)}
	} else {
		elapsed := now.Sub(bucket.updated).Seconds()
		bucket.requests = min(float64(s.limits.WriteRequestBurst), bucket.requests+elapsed*float64(s.limits.WriteRequestsPerSecond))
		bucket.bytes = min(float64(s.limits.WriteByteBurst), bucket.bytes+elapsed*float64(s.limits.WriteBytesPerSecond))
		bucket.updated = now
	}
	charge := bytes
	if charge == 0 {
		charge = 1
	}
	if bucket.requests < 1 || bucket.bytes < float64(charge) {
		s.rateBuckets[key] = bucket
		return false
	}
	bucket.requests--
	bucket.bytes -= float64(charge)
	s.rateBuckets[key] = bucket
	return true
}

func inputRecordSize(record *streamdv1.InputRecord) (uint64, bool) {
	total := uint64(len(record.Payload))
	for key, value := range record.Headers {
		addition := uint64(len(key)) + uint64(len(value))
		if total > math.MaxUint64-addition {
			return 0, true
		}
		total += addition
	}
	return total, false
}

func (s *Server) acquireSubscription(principal access.Principal, namespace string) error {
	key := subscriptionKey{producer: principal.Producer(), namespace: namespace}
	s.subMu.Lock()
	defer s.subMu.Unlock()
	if s.subs[key] >= s.limits.MaxSubscriptionsPerPrincipalNamespace {
		return resourceExhausted("Subscribe count exceeds the configured limit", nil, nil)
	}
	s.subs[key]++
	return nil
}

func (s *Server) releaseSubscription(principal access.Principal, namespace string) {
	key := subscriptionKey{producer: principal.Producer(), namespace: namespace}
	s.subMu.Lock()
	if s.subs[key] <= 1 {
		delete(s.subs, key)
	} else {
		s.subs[key]--
	}
	s.subMu.Unlock()
}

func (s *Server) send(stream streamdv1.StreamService_SubscribeServer, response *streamdv1.SubscribeResponse) error {
	done := make(chan error, 1)
	go func() { done <- stream.Send(response) }()
	timer := time.NewTimer(s.sendTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-stream.Context().Done():
		return mapError(stream.Context().Err(), nil)
	case <-s.drainCtx.Done():
		return unavailable("server is draining", nil)
	case <-timer.C:
		return streamdStatus(codes.ResourceExhausted, streamdv1.ErrorCode_ERROR_CODE_SLOW_CONSUMER, "Subscribe consumer is too slow", true, false, nil, nil, nil)
	}
}
