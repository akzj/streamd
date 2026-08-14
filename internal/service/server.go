package service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	streamdv1 "github.com/akzj/streamd/api/streamd/v1"
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

type ProducerResolver func(context.Context) (string, error)

type Server struct {
	streamdv1.UnimplementedStreamServiceServer
	backend         Backend
	resolveProducer ProducerResolver
	sendTimeout     time.Duration
}

type Options struct {
	SubscribeSendTimeout time.Duration
}

func New(backend Backend, resolveProducer ProducerResolver) (*Server, error) {
	return NewWithOptions(backend, resolveProducer, Options{})
}

func NewWithOptions(backend Backend, resolveProducer ProducerResolver, options Options) (*Server, error) {
	if backend == nil || resolveProducer == nil {
		return nil, fmt.Errorf("backend and Producer resolver are required")
	}
	if options.SubscribeSendTimeout < 0 {
		return nil, fmt.Errorf("Subscribe Send timeout cannot be negative")
	}
	if options.SubscribeSendTimeout == 0 {
		options.SubscribeSendTimeout = 30 * time.Second
	}
	return &Server{backend: backend, resolveProducer: resolveProducer, sendTimeout: options.SubscribeSendTimeout}, nil
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
		Durability:     streamdv1.Durability_DURABILITY_SINGLE_SYNC,
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
		Durability:          streamdv1.Durability_DURABILITY_SINGLE_SYNC,
	}, nil
}

func (s *Server) append(ctx context.Context, ref *streamdv1.StreamRef, expected uint64, requestID []byte, records []*streamdv1.InputRecord, required streamdv1.Durability) (engine.AppendResult, error) {
	if err := validateStream(ref); err != nil {
		return engine.AppendResult{}, invalidArgument(err.Error(), requestID)
	}
	if len(requestID) == 0 {
		return engine.AppendResult{}, invalidArgument("request_id is required", nil)
	}
	if required != streamdv1.Durability_DURABILITY_UNSPECIFIED && required != streamdv1.Durability_DURABILITY_SINGLE_SYNC {
		if required == streamdv1.Durability_DURABILITY_REPLICATED_STRICT {
			return engine.AppendResult{}, streamdStatus(codes.FailedPrecondition, streamdv1.ErrorCode_ERROR_CODE_DURABILITY_UNAVAILABLE, "required durability is unavailable", false, false, nil, nil, requestID)
		}
		return engine.AppendResult{}, invalidArgument("required_durability is invalid for Append", requestID)
	}
	producer, err := s.resolveProducer(ctx)
	if err != nil || producer == "" {
		return engine.AppendResult{}, streamdStatus(codes.Unauthenticated, streamdv1.ErrorCode_ERROR_CODE_UNAUTHENTICATED, "authenticated producer identity is required", false, false, nil, nil, requestID)
	}
	inputs := make([]engine.InputRecord, len(records))
	for i, record := range records {
		if record == nil {
			return engine.AppendResult{}, invalidArgument(fmt.Sprintf("record %d is missing", i), requestID)
		}
		inputs[i] = engine.InputRecord{Headers: inputHeaders(record.Headers), Payload: slices.Clone(record.Payload)}
	}
	result, err := s.backend.Append(ctx, engine.AppendRequest{Namespace: ref.Namespace, Stream: ref.Stream, ExpectedSequence: expected, RequestID: slices.Clone(requestID), Producer: producer, Records: inputs})
	if err != nil {
		return engine.AppendResult{}, mapError(err, requestID)
	}
	return result, nil
}

func (s *Server) Read(_ context.Context, request *streamdv1.ReadRequest) (*streamdv1.ReadResponse, error) {
	if request == nil {
		return nil, invalidArgument("Read request is required", nil)
	}
	if err := validateStream(request.Stream); err != nil {
		return nil, invalidArgument(err.Error(), nil)
	}
	if request.MaxRecords == 0 || request.MaxBytes == 0 {
		return nil, invalidArgument("max_records and max_bytes must be greater than zero", nil)
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

func (s *Server) ResolveTime(_ context.Context, request *streamdv1.ResolveTimeRequest) (*streamdv1.ResolveTimeResponse, error) {
	if request == nil || request.RecordedAt == nil {
		return nil, invalidArgument("ResolveTime request and recorded_at are required", nil)
	}
	if err := validateStream(request.Stream); err != nil {
		return nil, invalidArgument(err.Error(), nil)
	}
	if err := request.RecordedAt.CheckValid(); err != nil {
		return nil, invalidArgument("recorded_at is invalid", nil)
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

func (s *Server) InspectStream(_ context.Context, request *streamdv1.InspectStreamRequest) (*streamdv1.InspectStreamResponse, error) {
	if request == nil {
		return nil, invalidArgument("InspectStream request is required", nil)
	}
	if err := validateStream(request.Stream); err != nil {
		return nil, invalidArgument(err.Error(), nil)
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
		Role:       streamdv1.NodeRole_NODE_ROLE_SINGLE,
		Durability: streamdv1.Durability_DURABILITY_SINGLE_SYNC,
	}
	if health.Fatal != nil {
		response.Status = streamdv1.HealthStatus_HEALTH_STATUS_FAILED
		response.Reasons = []string{"commit core failed"}
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
	return response, nil
}

func validateStream(ref *streamdv1.StreamRef) error {
	if ref == nil || ref.Namespace == "" || ref.Stream == "" {
		return fmt.Errorf("stream namespace and name are required")
	}
	return nil
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
	cursor := request.FromSequence
	heartbeat := request.HeartbeatInterval.AsDuration()
	for {
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
		err = s.backend.WaitForAppend(waitCtx, request.Stream.Namespace, request.Stream.Stream, cursor)
		cancel()
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
	case <-timer.C:
		return streamdStatus(codes.ResourceExhausted, streamdv1.ErrorCode_ERROR_CODE_SLOW_CONSUMER, "Subscribe consumer is too slow", true, false, nil, nil, nil)
	}
}
