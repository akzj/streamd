package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/akzj/streamd/internal/storage/commit"
	"github.com/akzj/streamd/internal/storage/errdefs"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/identity"
	"github.com/akzj/streamd/internal/storage/lifecycle"
	"github.com/akzj/streamd/internal/storage/memtable"
	readstore "github.com/akzj/streamd/internal/storage/read"
	"github.com/akzj/streamd/internal/storage/recovery"
	"github.com/akzj/streamd/internal/storage/registry"
	"github.com/akzj/streamd/internal/storage/segment"
)

type InputRecord struct {
	Headers []format.Header
	Payload []byte
}
type AppendRequest struct {
	Namespace        string
	Stream           string
	ExpectedSequence uint64
	RequestID        []byte
	Producer         string
	Records          []InputRecord
}
type AppendResult struct {
	FirstSequence   uint64
	NextSequence    uint64
	RecordCount     uint32
	FirstRecordedAt int64
	LastRecordedAt  int64
	FirstEntryID    uint64
	LastEntryID     uint64
	Deduplicated    bool
}
type Health struct {
	Watermarks commit.Watermarks
	Fatal      error
}
type streamKey struct {
	namespace string
	stream    string
}
type Store struct {
	mu             sync.Mutex
	viewMu         sync.RWMutex
	fatalMu        sync.RWMutex
	notifyMu       sync.Mutex
	root           *fsutil.Root
	state          *recovery.Result
	committer      *commit.Committer
	reader         *readstore.Store
	now            func() time.Time
	notifications  map[streamKey]chan struct{}
	closed         bool
	fatal          error
	checkpointHook fsutil.CrashHook
}

func (s *Store) Read(namespace, name string, from uint64, maxRecords int, maxBytes uint64) (readstore.Result, error) {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	mapping, ok := s.state.Registry.Lookup(namespace, name)
	if !ok {
		return readstore.Result{}, errdefs.ErrStreamNotFound
	}
	return s.reader.Read(mapping.StreamID, from, maxRecords, maxBytes)
}
func (s *Store) Inspect(namespace, name string) (readstore.StreamInfo, error) {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	mapping, ok := s.state.Registry.Lookup(namespace, name)
	if !ok {
		return readstore.StreamInfo{}, nil
	}
	return s.reader.Inspect(mapping.StreamID)
}
func (s *Store) ResolveTime(namespace, name string, target int64, mode readstore.TimeMode) (uint64, int64, bool, error) {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	mapping, ok := s.state.Registry.Lookup(namespace, name)
	if !ok {
		return 0, 0, false, nil
	}
	return s.reader.ResolveTime(mapping.StreamID, target, mode)
}
func (s *Store) Health() Health {
	return Health{Watermarks: s.committer.Watermarks(), Fatal: errors.Join(s.committer.FatalError(), s.fatalError())}
}

func Open(path string) (*Store, error) {
	return open(path, nil)
}

func OpenWithIdentity(path string, node format.NodeIdentity) (*Store, error) {
	return open(path, &node)
}

func open(path string, node *format.NodeIdentity) (*Store, error) {
	root, err := fsutil.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	if node != nil {
		if _, err = identity.Ensure(root.Path(), *node); err != nil {
			root.Close()
			return nil, err
		}
	}
	state, err := recovery.Open(root.Path())
	if err != nil {
		root.Close()
		return nil, err
	}
	return &Store{root: root, state: state, committer: commit.New(state.WAL, state.MemTable), reader: readstore.New(state.MemTable, state.Segments, 1024), now: time.Now, notifications: make(map[streamKey]chan struct{})}, nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeNotifications()
	commitErr := s.committer.Close()
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	return errors.Join(commitErr, s.state.Close(), s.root.Close())
}
func (s *Store) Append(ctx context.Context, request AppendRequest) (AppendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fatalError(); err != nil {
		return AppendResult{}, fmt.Errorf("engine failed: %w", err)
	}
	if request.Namespace == "" || request.Stream == "" || len(request.RequestID) == 0 || len(request.RequestID) > format.MaxRequestIDLength || request.Producer == "" || len(request.Records) == 0 || len(request.Records) > format.MaxBatchRecordCount {
		return AppendResult{}, errdefs.ErrInvalidArgument
	}
	hash, err := RequestHash(request)
	if err != nil {
		return AppendResult{}, fmt.Errorf("%w: %v", errdefs.ErrInvalidArgument, err)
	}
	if _, err = format.MarshalRegistryRecord(format.RegistryRecord{AssignedStreamID: 1, Namespace: request.Namespace, StreamName: request.Stream}); err != nil {
		return AppendResult{}, fmt.Errorf("%w: %v", errdefs.ErrInvalidArgument, err)
	}
	if err = validateInputRecords(request, hash); err != nil {
		return AppendResult{}, fmt.Errorf("%w: %v", errdefs.ErrInvalidArgument, err)
	}
	mapping, exists := s.state.Registry.Lookup(request.Namespace, request.Stream)
	if !exists {
		proposal, _, err := s.state.Registry.NextAssignment(request.Namespace, request.Stream)
		if err != nil {
			return AppendResult{}, err
		}
		payload, err := format.MarshalRegistryRecord(proposal)
		if err != nil {
			return AppendResult{}, err
		}
		if err = s.commitRegistry(ctx, proposal, payload); err != nil {
			return AppendResult{}, err
		}
		mapping, _ = s.state.Registry.Lookup(request.Namespace, request.Stream)
	}
	tail, ok := s.state.MemTable.Tail(mapping.StreamID)
	if !ok {
		tail = zeroTail()
	}
	if request.ExpectedSequence < tail.NextSequence {
		return s.deduplicate(mapping.StreamID, request, hash, tail.NextSequence)
	}
	if request.ExpectedSequence > tail.NextSequence {
		return AppendResult{}, &errdefs.SequenceAheadError{Requested: request.ExpectedSequence, CurrentNextSequence: tail.NextSequence}
	}
	entryID := s.state.WAL.NextEntryID()
	previous := s.state.WAL.PreviousEntryCRC32C()
	sequence, offset := tail.NextSequence, tail.NextByteOffset
	count := uint64(len(request.Records))
	if count-1 > math.MaxUint64-entryID || count-1 > math.MaxUint64-sequence {
		return AppendResult{}, fmt.Errorf("Append identifiers overflow")
	}
	recordedAt := s.now().UnixNano()
	if tail.RecordCount > 0 && recordedAt < tail.LastRecordedAt {
		recordedAt = tail.LastRecordedAt
	}
	encoded := make([][]byte, 0, len(request.Records))
	firstTime := recordedAt
	for i, input := range request.Records {
		frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: entryID + uint64(i), StreamID: mapping.StreamID, Sequence: sequence + uint64(i), ByteOffset: offset, RecordedAt: recordedAt, BatchIndex: uint32(i), BatchCount: uint32(len(request.Records)), RequestHash: hash, RequestID: request.RequestID, Producer: request.Producer, Headers: input.Headers, Payload: input.Payload})
		if err != nil {
			return AppendResult{}, err
		}
		walEntry, err := format.MarshalWALEntry(0, previous, frame)
		if err != nil {
			return AppendResult{}, err
		}
		decoded, err := format.UnmarshalWALEntry(walEntry)
		if err != nil {
			return AppendResult{}, err
		}
		encoded = append(encoded, walEntry)
		previous = decoded.CRC32C
		if uint64(len(frame)) > math.MaxUint64-offset {
			return AppendResult{}, fmt.Errorf("Stream Byte Offset overflows")
		}
		offset += uint64(len(frame))
	}
	commitResult, err := s.committer.Commit(ctx, encoded)
	if err != nil {
		watermarks := s.committer.Watermarks()
		if watermarks.HasApplied && watermarks.Applied >= commitResult.LastEntryID && commitResult.RecordCount > 0 {
			s.signal(request.Namespace, request.Stream)
		}
		return AppendResult{}, &errdefs.WriteError{Err: err, ResultUncertain: commitResult.ResultUncertain}
	}
	s.signal(request.Namespace, request.Stream)
	return AppendResult{FirstSequence: sequence, NextSequence: sequence + uint64(len(encoded)), RecordCount: uint32(len(encoded)), FirstRecordedAt: firstTime, LastRecordedAt: recordedAt, FirstEntryID: commitResult.FirstEntryID, LastEntryID: commitResult.LastEntryID}, nil
}

func (s *Store) Checkpoint() (format.Manifest, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fatalError(); err != nil {
		return format.Manifest{}, false, fmt.Errorf("engine failed: %w", err)
	}
	records, _ := s.state.MemTable.Stats()
	if records == 0 {
		current, _ := s.state.Manifest.Current()
		return current, false, nil
	}
	snapshots := s.state.MemTable.Snapshot()
	flush := make([]memtable.StreamSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if len(snapshot.Frames) > 0 {
			flush = append(flush, snapshot)
		}
	}
	lastEntryID := s.state.WAL.NextEntryID() - 1
	lastCRC := s.state.WAL.PreviousEntryCRC32C()
	if err := s.committer.Close(); err != nil {
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	if err := s.state.WAL.Rotate(0, s.now()); err != nil {
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	if s.checkpointHook != nil {
		if err := s.checkpointHook("after_wal_rotate"); err != nil {
			return format.Manifest{}, false, err
		}
	}
	manager := lifecycle.New(s.root.Path(), s.state.Manifest)
	published, err := manager.PublishFlush(flush, lastEntryID, lastCRC)
	if err != nil {
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	if s.checkpointHook != nil {
		if err = s.checkpointHook("after_manifest_publish"); err != nil {
			s.setFatal(err)
			return format.Manifest{}, false, err
		}
	}
	existing := make(map[format.UUID]bool, len(s.state.Segments))
	for _, reader := range s.state.Segments {
		existing[reader.Header.SegmentID] = true
	}
	var newReference *format.SegmentReference
	for i := range published.SegmentReferences {
		if !existing[published.SegmentReferences[i].SegmentID] {
			newReference = &published.SegmentReferences[i]
			break
		}
	}
	if newReference == nil {
		err = fmt.Errorf("published Manifest has no new Segment")
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	reader, err := segment.Open(filepath.Join(s.root.Path(), newReference.LocalPath))
	if err != nil {
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	newTable := memtable.New(0)
	for _, snapshot := range snapshots {
		if err = newTable.SeedTail(snapshot.StreamID, snapshot.Tail); err != nil {
			reader.Close()
			s.setFatal(err)
			return format.Manifest{}, false, err
		}
	}
	s.viewMu.Lock()
	oldTable := s.state.MemTable
	s.state.MemTable = newTable
	s.state.Segments = append(s.state.Segments, reader)
	s.committer = commit.New(s.state.WAL, newTable)
	s.reader = readstore.New(newTable, s.state.Segments, 1024)
	s.viewMu.Unlock()
	oldTable.Freeze()
	if s.checkpointHook != nil {
		if err = s.checkpointHook("after_view_install"); err != nil {
			return published, true, err
		}
	}
	return published, true, nil
}

func (s *Store) fatalError() error {
	s.fatalMu.RLock()
	defer s.fatalMu.RUnlock()
	return s.fatal
}

func (s *Store) setFatal(err error) {
	s.fatalMu.Lock()
	if s.fatal == nil {
		s.fatal = err
	}
	s.fatalMu.Unlock()
}

func (s *Store) WaitForAppend(ctx context.Context, namespace, name string, after uint64) error {
	key := streamKey{namespace: namespace, stream: name}
	s.notifyMu.Lock()
	if s.closed {
		s.notifyMu.Unlock()
		return errdefs.ErrClosed
	}
	ch := s.notifications[key]
	if ch == nil {
		ch = make(chan struct{})
		s.notifications[key] = ch
	}
	info, err := s.Inspect(namespace, name)
	if err != nil || (info.Exists && info.NextSequence > after) {
		s.notifyMu.Unlock()
		return err
	}
	s.notifyMu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		s.notifyMu.Lock()
		closed := s.closed
		s.notifyMu.Unlock()
		if closed {
			return errdefs.ErrClosed
		}
		return nil
	}
}

func (s *Store) signal(namespace, name string) {
	key := streamKey{namespace: namespace, stream: name}
	s.notifyMu.Lock()
	if !s.closed {
		if ch := s.notifications[key]; ch != nil {
			close(ch)
		}
		s.notifications[key] = make(chan struct{})
	}
	s.notifyMu.Unlock()
}

func (s *Store) closeNotifications() {
	s.notifyMu.Lock()
	if !s.closed {
		s.closed = true
		for key, ch := range s.notifications {
			close(ch)
			delete(s.notifications, key)
		}
	}
	s.notifyMu.Unlock()
}
func (s *Store) commitRegistry(ctx context.Context, proposal format.RegistryRecord, payload []byte) error {
	tail, ok := s.state.MemTable.Tail(registry.RegistryStreamID)
	if !ok {
		tail = zeroTail()
	}
	entryID := s.state.WAL.NextEntryID()
	recorded := s.now().UnixNano()
	hash := sha256.Sum256(payload)
	requestID := []byte(fmt.Sprintf("registry/%d", proposal.AssignedStreamID))
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: entryID, StreamID: registry.RegistryStreamID, Sequence: tail.NextSequence, ByteOffset: tail.NextByteOffset, RecordedAt: recorded, BatchCount: 1, RequestHash: hash, RequestID: requestID, Producer: "streamd/registry", Payload: payload})
	if err != nil {
		return err
	}
	encoded, err := format.MarshalWALEntry(0, s.state.WAL.PreviousEntryCRC32C(), frame)
	if err != nil {
		return err
	}
	_, commitErr := s.committer.Commit(ctx, [][]byte{encoded})
	watermarks := s.committer.Watermarks()
	if watermarks.HasApplied && watermarks.Applied >= entryID {
		if err = s.state.Registry.ApplyRecord(entryID, payload); err != nil {
			return err
		}
	}
	if commitErr != nil {
		return commitErr
	}
	return nil
}
func (s *Store) deduplicate(streamID uint64, request AppendRequest, hash [32]byte, currentNext uint64) (AppendResult, error) {
	result, err := s.reader.Read(streamID, request.ExpectedSequence, len(request.Records), 0)
	if err != nil {
		return AppendResult{}, err
	}
	if len(result.Records) != len(request.Records) {
		return AppendResult{}, errdefs.ErrSequenceConflict
	}
	first := result.Records[0]
	if first.BatchIndex != 0 || int(first.BatchCount) != len(request.Records) {
		return AppendResult{}, errdefs.ErrSequenceConflict
	}
	for _, record := range result.Records {
		if !bytes.Equal(record.RequestID, request.RequestID) || record.RequestHash != hash || record.BatchCount != first.BatchCount {
			return AppendResult{}, errdefs.ErrSequenceConflict
		}
	}
	last := result.Records[len(result.Records)-1]
	return AppendResult{FirstSequence: first.Sequence, NextSequence: last.Sequence + 1, RecordCount: first.BatchCount, FirstRecordedAt: first.RecordedAt, LastRecordedAt: last.RecordedAt, FirstEntryID: first.EntryID, LastEntryID: last.EntryID, Deduplicated: true}, nil
}
func RequestHash(request AppendRequest) ([32]byte, error) {
	var zero [32]byte
	if request.Namespace == "" || request.Stream == "" || len(request.RequestID) == 0 {
		return zero, fmt.Errorf("invalid Request Hash input")
	}
	h := sha256.New()
	h.Write([]byte("streamd.append.v1"))
	writeLength(h, []byte(request.Namespace))
	writeLength(h, []byte(request.Stream))
	var u64 [8]byte
	binary.LittleEndian.PutUint64(u64[:], request.ExpectedSequence)
	h.Write(u64[:])
	writeLength(h, request.RequestID)
	var u32 [4]byte
	binary.LittleEndian.PutUint32(u32[:], uint32(len(request.Records)))
	h.Write(u32[:])
	for _, record := range request.Records {
		headers := slices.Clone(record.Headers)
		slices.SortFunc(headers, func(a, b format.Header) int { return bytes.Compare([]byte(a.Key), []byte(b.Key)) })
		binary.LittleEndian.PutUint32(u32[:], uint32(len(headers)))
		h.Write(u32[:])
		for i, header := range headers {
			if i > 0 && header.Key == headers[i-1].Key {
				return zero, fmt.Errorf("duplicate Header %q", header.Key)
			}
			writeLength(h, []byte(header.Key))
			writeLength(h, header.Value)
		}
		writeLength(h, record.Payload)
	}
	copy(zero[:], h.Sum(nil))
	return zero, nil
}
func validateInputRecords(request AppendRequest, hash [32]byte) error {
	offset := uint64(0)
	for i, input := range request.Records {
		frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: uint64(i), StreamID: 1, Sequence: uint64(i), ByteOffset: offset, BatchIndex: uint32(i), BatchCount: uint32(len(request.Records)), RequestHash: hash, RequestID: request.RequestID, Producer: request.Producer, Headers: input.Headers, Payload: input.Payload})
		if err != nil {
			return err
		}
		offset += uint64(len(frame))
	}
	return nil
}

type byteWriter interface{ Write([]byte) (int, error) }

func writeLength(w byteWriter, value []byte) {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
	w.Write(length[:])
	w.Write(value)
}
func zeroTail() memtable.Tail { return memtable.Tail{} }
