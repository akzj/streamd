package service

import (
	"context"
	"errors"
	"testing"

	streamdv1 "github.com/akzj/streamd/api/streamd/v1"
	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/errdefs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnaryServiceRoundTrip(t *testing.T) {
	store, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := New(store, func(context.Context) (string, error) { return "tenant/service", nil })
	if err != nil {
		t.Fatal(err)
	}
	ref := &streamdv1.StreamRef{Namespace: "agent", Stream: "events"}
	request := &streamdv1.AppendBatchRequest{
		Stream:           ref,
		ExpectedSequence: 0,
		RequestId:        []byte("request-1"),
		Records: []*streamdv1.InputRecord{
			{Headers: map[string][]byte{"kind": []byte("input")}, Payload: []byte("one")},
			{Payload: []byte("two")},
		},
		RequiredDurability: streamdv1.Durability_DURABILITY_SINGLE_SYNC,
	}
	appended, err := server.AppendBatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if appended.FirstSequence != 0 || appended.NextSequence != 2 || appended.RecordCount != 2 || appended.Deduplicated {
		t.Fatalf("AppendBatch response = %+v", appended)
	}
	duplicate, err := server.AppendBatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Deduplicated || duplicate.FirstStorageEntryId != appended.FirstStorageEntryId {
		t.Fatalf("deduplicated response = %+v", duplicate)
	}

	read, err := server.Read(context.Background(), &streamdv1.ReadRequest{Stream: ref, MaxRecords: 10, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Records) != 2 || read.NextSequence != 2 || read.CurrentNextSequence != 2 || read.Records[0].Producer != "tenant/service" || string(read.Records[0].Headers["kind"]) != "input" {
		t.Fatalf("Read response = %+v", read)
	}
	inspect, err := server.InspectStream(context.Background(), &streamdv1.InspectStreamRequest{Stream: ref})
	if err != nil {
		t.Fatal(err)
	}
	if !inspect.Exists || inspect.RecordCount != 2 || inspect.FirstRecordedAt == nil || inspect.LastRecordedAt == nil {
		t.Fatalf("Inspect response = %+v", inspect)
	}
	resolved, err := server.ResolveTime(context.Background(), &streamdv1.ResolveTimeRequest{Stream: ref, RecordedAt: read.Records[0].RecordedAt, Mode: streamdv1.ResolveTimeMode_RESOLVE_TIME_MODE_AT_OR_AFTER})
	if err != nil {
		t.Fatal(err)
	}
	if !resolved.Found || resolved.Sequence != 0 {
		t.Fatalf("ResolveTime response = %+v", resolved)
	}
	health, err := server.Health(context.Background(), &streamdv1.HealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if health.Status != streamdv1.HealthStatus_HEALTH_STATUS_READY_WRITE || health.Role != streamdv1.NodeRole_NODE_ROLE_SINGLE || health.LocalDurableEntryId == nil || health.AppliedEntryId == nil {
		t.Fatalf("Health response = %+v", health)
	}
}

func TestServiceReturnsStructuredErrors(t *testing.T) {
	store, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := New(store, func(context.Context) (string, error) { return "test", nil })
	if err != nil {
		t.Fatal(err)
	}
	ref := &streamdv1.StreamRef{Namespace: "n", Stream: "s"}
	_, err = server.Append(context.Background(), &streamdv1.AppendRequest{Stream: ref, ExpectedSequence: 0, RequestId: []byte("strict"), Record: &streamdv1.InputRecord{}, RequiredDurability: streamdv1.Durability_DURABILITY_REPLICATED_STRICT})
	assertError(t, err, codes.FailedPrecondition, streamdv1.ErrorCode_ERROR_CODE_DURABILITY_UNAVAILABLE, false)

	_, err = server.Append(context.Background(), &streamdv1.AppendRequest{Stream: ref, ExpectedSequence: 2, RequestId: []byte("ahead"), Record: &streamdv1.InputRecord{}})
	detail := assertError(t, err, codes.OutOfRange, streamdv1.ErrorCode_ERROR_CODE_SEQUENCE_AHEAD, false)
	if detail.CurrentNextSequence == nil || *detail.CurrentNextSequence != 0 {
		t.Fatalf("Sequence ahead detail = %+v", detail)
	}

	_, err = server.Read(context.Background(), &streamdv1.ReadRequest{Stream: ref, MaxRecords: 1, MaxBytes: 1})
	assertError(t, err, codes.NotFound, streamdv1.ErrorCode_ERROR_CODE_STREAM_NOT_FOUND, false)

	_, err = server.Append(context.Background(), &streamdv1.AppendRequest{Stream: ref, RequestId: []byte("record"), Record: &streamdv1.InputRecord{Payload: []byte("payload")}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = server.Append(context.Background(), &streamdv1.AppendRequest{Stream: ref, RequestId: []byte("different"), Record: &streamdv1.InputRecord{Payload: []byte("changed")}})
	assertError(t, err, codes.Aborted, streamdv1.ErrorCode_ERROR_CODE_SEQUENCE_CONFLICT, false)
	_, err = server.Read(context.Background(), &streamdv1.ReadRequest{Stream: ref, MaxRecords: 1, MaxBytes: 1})
	detail = assertError(t, err, codes.ResourceExhausted, streamdv1.ErrorCode_ERROR_CODE_RECORD_TOO_LARGE, false)
	if detail.RequiredBytes == nil || *detail.RequiredBytes <= 1 {
		t.Fatalf("Record too large detail = %+v", detail)
	}
}

func TestDeadlineAfterWriteIsMarkedUncertain(t *testing.T) {
	err := mapError(&errdefs.WriteError{Err: context.DeadlineExceeded, ResultUncertain: true}, []byte("request"))
	detail := assertError(t, err, codes.DeadlineExceeded, streamdv1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED, true)
	if string(detail.RequestId) != "request" {
		t.Fatalf("request ID = %q", detail.RequestId)
	}
}

func TestProducerResolverFailureIsUnauthenticated(t *testing.T) {
	store, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := New(store, func(context.Context) (string, error) { return "", errors.New("no identity") })
	if err != nil {
		t.Fatal(err)
	}
	_, err = server.Append(context.Background(), &streamdv1.AppendRequest{Stream: &streamdv1.StreamRef{Namespace: "n", Stream: "s"}, RequestId: []byte("r"), Record: &streamdv1.InputRecord{}})
	assertError(t, err, codes.Unauthenticated, streamdv1.ErrorCode_ERROR_CODE_UNAUTHENTICATED, false)
}

func assertError(t *testing.T, err error, grpcCode codes.Code, code streamdv1.ErrorCode, uncertain bool) *streamdv1.StreamdError {
	t.Helper()
	if status.Code(err) != grpcCode {
		t.Fatalf("gRPC code = %s, want %s: %v", status.Code(err), grpcCode, err)
	}
	for _, value := range status.Convert(err).Details() {
		if detail, ok := value.(*streamdv1.StreamdError); ok {
			if detail.Code != code || detail.ResultUncertain != uncertain {
				t.Fatalf("error detail = %+v", detail)
			}
			return detail
		}
	}
	t.Fatalf("StreamdError detail is missing: %v", err)
	return nil
}
