// Package errdefs defines storage errors that cross the engine/service boundary.
package errdefs

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidArgument  = errors.New("invalid argument")
	ErrStreamNotFound   = errors.New("stream not found")
	ErrSequenceAhead    = errors.New("sequence ahead")
	ErrSequenceConflict = errors.New("sequence conflict")
	ErrRecordTooLarge   = errors.New("record too large")
	ErrClosed           = errors.New("store closed")
)

type SequenceAheadError struct {
	Requested           uint64
	CurrentNextSequence uint64
}

func (e *SequenceAheadError) Error() string {
	return fmt.Sprintf("Sequence %d is ahead of tail %d", e.Requested, e.CurrentNextSequence)
}

func (e *SequenceAheadError) Unwrap() error { return ErrSequenceAhead }

type RecordTooLargeError struct {
	Sequence      uint64
	RequiredBytes uint64
}

func (e *RecordTooLargeError) Error() string {
	return fmt.Sprintf("Record at Sequence %d requires %d bytes", e.Sequence, e.RequiredBytes)
}

func (e *RecordTooLargeError) Unwrap() error { return ErrRecordTooLarge }

type WriteError struct {
	Err             error
	ResultUncertain bool
}

func (e *WriteError) Error() string { return e.Err.Error() }
func (e *WriteError) Unwrap() error { return e.Err }
