package service

import (
	"context"
	"io"
	"testing"
	"time"

	streamdv1 "github.com/akzj/streamd/api/streamd/v1"
	"github.com/akzj/streamd/internal/access"
	"github.com/akzj/streamd/internal/storage/engine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSubscribeHistoryNotificationAndHeartbeat(t *testing.T) {
	store, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := New(store, allow(access.Principal{Tenant: "test", Service: "client"}))
	if err != nil {
		t.Fatal(err)
	}
	ref := &streamdv1.StreamRef{Namespace: "n", Stream: "s"}
	appendRecord := func(expected uint64, id, payload string) {
		t.Helper()
		_, appendErr := server.Append(context.Background(), &streamdv1.AppendRequest{Stream: ref, ExpectedSequence: expected, RequestId: []byte(id), Record: &streamdv1.InputRecord{Payload: []byte(payload)}})
		if appendErr != nil {
			t.Fatal(appendErr)
		}
	}
	appendRecord(0, "first", "one")
	ctx, cancel := context.WithCancel(context.Background())
	stream := newTestSubscribeStream(ctx)
	done := make(chan error, 1)
	go func() {
		done <- server.Subscribe(&streamdv1.SubscribeRequest{Stream: ref, MaxBatchRecords: 10, MaxBatchBytes: 1 << 20, HeartbeatInterval: durationpb.New(20 * time.Millisecond)}, stream)
	}()
	first := receiveSubscribe(t, stream.sent)
	if len(first.Records) != 1 || first.NextSequence != 1 || first.Heartbeat {
		t.Fatalf("history response = %+v", first)
	}
	appendRecord(1, "second", "two")
	second := receiveSubscribe(t, stream.sent)
	if len(second.Records) != 1 || second.Records[0].Sequence != 1 || second.NextSequence != 2 {
		t.Fatalf("notification response = %+v", second)
	}
	heartbeat := receiveSubscribe(t, stream.sent)
	if !heartbeat.Heartbeat || heartbeat.NextSequence != 2 || heartbeat.CurrentNextSequence != 2 || len(heartbeat.Records) != 0 {
		t.Fatalf("heartbeat response = %+v", heartbeat)
	}
	cancel()
	select {
	case err = <-done:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("Subscribe cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Subscribe did not stop after cancellation")
	}
}

func TestSubscribeRejectsSlowConsumer(t *testing.T) {
	store, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := NewWithOptions(store, allow(access.Principal{Tenant: "test", Service: "client"}), Options{SubscribeSendTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ref := &streamdv1.StreamRef{Namespace: "n", Stream: "s"}
	if _, err = server.Append(context.Background(), &streamdv1.AppendRequest{Stream: ref, RequestId: []byte("r"), Record: &streamdv1.InputRecord{}}); err != nil {
		t.Fatal(err)
	}
	stream := newTestSubscribeStream(context.Background())
	stream.block = make(chan struct{})
	err = server.Subscribe(&streamdv1.SubscribeRequest{Stream: ref, MaxBatchRecords: 1, MaxBatchBytes: 1024, HeartbeatInterval: durationpb.New(time.Second)}, stream)
	close(stream.block)
	detail := assertError(t, err, codes.ResourceExhausted, streamdv1.ErrorCode_ERROR_CODE_SLOW_CONSUMER, false)
	if !detail.Retryable {
		t.Fatalf("slow consumer detail = %+v", detail)
	}
}

func TestSubscribeCountIsBoundedPerPrincipalNamespace(t *testing.T) {
	store, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := NewWithOptions(store, allow(access.Principal{Tenant: "test", Service: "client"}), Options{Limits: Limits{MaxSubscriptionsPerPrincipalNamespace: 1}})
	if err != nil {
		t.Fatal(err)
	}
	ref := &streamdv1.StreamRef{Namespace: "n", Stream: "s"}
	if _, err = server.Append(context.Background(), &streamdv1.AppendRequest{Stream: ref, RequestId: []byte("r"), Record: &streamdv1.InputRecord{}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	first := newTestSubscribeStream(ctx)
	done := make(chan error, 1)
	request := &streamdv1.SubscribeRequest{Stream: ref, MaxBatchRecords: 1, MaxBatchBytes: 1024, HeartbeatInterval: durationpb.New(time.Hour)}
	go func() { done <- server.Subscribe(request, first) }()
	receiveSubscribe(t, first.sent)
	second := newTestSubscribeStream(context.Background())
	err = server.Subscribe(request, second)
	detail := assertError(t, err, codes.ResourceExhausted, streamdv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED, false)
	if !detail.Retryable {
		t.Fatalf("Subscribe limit detail = %+v", detail)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("first Subscribe did not stop")
	}
}

type testSubscribeStream struct {
	ctx   context.Context
	sent  chan *streamdv1.SubscribeResponse
	block chan struct{}
}

func newTestSubscribeStream(ctx context.Context) *testSubscribeStream {
	return &testSubscribeStream{ctx: ctx, sent: make(chan *streamdv1.SubscribeResponse, 8)}
}

func (s *testSubscribeStream) Send(response *streamdv1.SubscribeResponse) error {
	if s.block != nil {
		<-s.block
	}
	select {
	case s.sent <- response:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
func (s *testSubscribeStream) SetHeader(metadata.MD) error  { return nil }
func (s *testSubscribeStream) SendHeader(metadata.MD) error { return nil }
func (s *testSubscribeStream) SetTrailer(metadata.MD)       {}
func (s *testSubscribeStream) Context() context.Context     { return s.ctx }
func (s *testSubscribeStream) SendMsg(any) error            { return nil }
func (s *testSubscribeStream) RecvMsg(any) error            { return io.EOF }

func receiveSubscribe(t *testing.T, responses <-chan *streamdv1.SubscribeResponse) *streamdv1.SubscribeResponse {
	t.Helper()
	select {
	case response := <-responses:
		return response
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Subscribe response")
		return nil
	}
}

var _ grpc.ServerStreamingServer[streamdv1.SubscribeResponse] = (*testSubscribeStream)(nil)
