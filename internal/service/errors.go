package service

import (
	"context"
	"errors"
	"fmt"

	streamdv1 "github.com/akzj/streamd/api/streamd/v1"
	"github.com/akzj/streamd/internal/storage/errdefs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mapError(err error, requestID []byte) error {
	var write *errdefs.WriteError
	uncertain := errors.As(err, &write) && write.ResultUncertain
	if errors.Is(err, context.DeadlineExceeded) {
		return streamdStatus(codes.DeadlineExceeded, streamdv1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED, "deadline exceeded", true, uncertain, nil, nil, requestID)
	}
	if errors.Is(err, context.Canceled) {
		return streamdStatus(codes.Canceled, streamdv1.ErrorCode_ERROR_CODE_UNSPECIFIED, "request canceled", false, uncertain, nil, nil, requestID)
	}
	if errors.Is(err, errdefs.ErrInvalidArgument) {
		return invalidArgument(err.Error(), requestID)
	}
	if errors.Is(err, errdefs.ErrStreamNotFound) {
		return streamdStatus(codes.NotFound, streamdv1.ErrorCode_ERROR_CODE_STREAM_NOT_FOUND, "stream not found", false, false, nil, nil, requestID)
	}
	var ahead *errdefs.SequenceAheadError
	if errors.As(err, &ahead) {
		return streamdStatus(codes.OutOfRange, streamdv1.ErrorCode_ERROR_CODE_SEQUENCE_AHEAD, ahead.Error(), false, false, &ahead.CurrentNextSequence, nil, requestID)
	}
	if errors.Is(err, errdefs.ErrSequenceConflict) {
		return streamdStatus(codes.Aborted, streamdv1.ErrorCode_ERROR_CODE_SEQUENCE_CONFLICT, "sequence conflict", false, false, nil, nil, requestID)
	}
	var tooLarge *errdefs.RecordTooLargeError
	if errors.As(err, &tooLarge) {
		return recordTooLarge(tooLarge.Sequence, tooLarge.RequiredBytes)
	}
	if errors.Is(err, errdefs.ErrClosed) {
		return streamdStatus(codes.Unavailable, streamdv1.ErrorCode_ERROR_CODE_UNSPECIFIED, "store is shutting down", true, false, nil, nil, requestID)
	}
	if errors.Is(err, errdefs.ErrNotLeader) {
		return streamdStatus(codes.FailedPrecondition, streamdv1.ErrorCode_ERROR_CODE_NOT_LEADER, "node is not a writable leader", true, uncertain, nil, nil, requestID)
	}
	if errors.Is(err, errdefs.ErrCapacityCritical) {
		return resourceExhausted("storage capacity is critical", nil, requestID)
	}
	if write != nil {
		return streamdStatus(codes.DataLoss, streamdv1.ErrorCode_ERROR_CODE_DATA_LOSS, "durable write failed", false, write.ResultUncertain, nil, nil, requestID)
	}
	return streamdStatus(codes.Internal, streamdv1.ErrorCode_ERROR_CODE_UNSPECIFIED, "internal storage error", false, false, nil, nil, requestID)
}

func invalidArgument(message string, requestID []byte) error {
	return streamdStatus(codes.InvalidArgument, streamdv1.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, message, false, false, nil, nil, requestID)
}

func recordTooLarge(sequence, required uint64) error {
	message := fmt.Sprintf("record at Sequence %d requires %d response bytes", sequence, required)
	return streamdStatus(codes.ResourceExhausted, streamdv1.ErrorCode_ERROR_CODE_RECORD_TOO_LARGE, message, false, false, nil, &required, nil)
}

func recordLimit(sequence, required uint64, requestID []byte) error {
	message := fmt.Sprintf("record at Sequence %d has %d input bytes and exceeds the configured limit", sequence, required)
	return streamdStatus(codes.ResourceExhausted, streamdv1.ErrorCode_ERROR_CODE_RECORD_TOO_LARGE, message, false, false, nil, &required, requestID)
}

func resourceExhausted(message string, required *uint64, requestID []byte) error {
	return streamdStatus(codes.ResourceExhausted, streamdv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED, message, true, false, nil, required, requestID)
}

func unavailable(message string, requestID []byte) error {
	return streamdStatus(codes.Unavailable, streamdv1.ErrorCode_ERROR_CODE_UNSPECIFIED, message, true, false, nil, nil, requestID)
}

func streamdStatus(grpcCode codes.Code, code streamdv1.ErrorCode, message string, retryable, uncertain bool, current, required *uint64, requestID []byte) error {
	detail := &streamdv1.StreamdError{Code: code, Message: message, Retryable: retryable, ResultUncertain: uncertain, CurrentNextSequence: current, RequiredBytes: required}
	if len(requestID) > 0 {
		detail.RequestId = append([]byte(nil), requestID...)
	}
	withDetails, err := status.New(grpcCode, message).WithDetails(detail)
	if err != nil {
		return status.Error(grpcCode, message)
	}
	return withDetails.Err()
}
