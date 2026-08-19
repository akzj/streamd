package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/errdefs"
	"github.com/akzj/streamd/internal/storage/format"
	readstore "github.com/akzj/streamd/internal/storage/read"
	"github.com/akzj/streamd/internal/storage/registry"
	"github.com/akzj/streamd/internal/storage/replicationstate"
)

type engineGuard struct {
	err error
}

func (g *engineGuard) CanCommit() error { return g.err }

type engineReplica struct {
	terms []uint64
}

func (r *engineReplica) Replicate(_ context.Context, encoded [][]byte) (uint64, error) {
	var last uint64
	for _, value := range encoded {
		entry, err := format.UnmarshalWALEntry(value)
		if err != nil {
			return 0, err
		}
		r.terms = append(r.terms, entry.Term)
		last = entry.EntryID
	}
	return last, nil
}

func (*engineReplica) AdvanceCommit(context.Context, uint64) error { return nil }

func TestAppendBatchDeduplicateAndRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	request := AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: 0, RequestID: []byte("request-1"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}, {Payload: []byte("two")}}}
	result, err := store.Append(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.FirstSequence != 0 || result.NextSequence != 2 || result.RecordCount != 2 || result.Deduplicated {
		t.Fatalf("result %+v", result)
	}
	duplicate, err := store.Append(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Deduplicated || duplicate.FirstEntryID != result.FirstEntryID {
		t.Fatalf("duplicate %+v", duplicate)
	}
	conflict := request
	conflict.Records = []InputRecord{{Payload: []byte("changed")}, {Payload: []byte("two")}}
	if _, err = store.Append(context.Background(), conflict); err == nil {
		t.Fatal("conflicting retry accepted")
	}
	ahead := request
	ahead.ExpectedSequence = 3
	ahead.RequestID = []byte("ahead")
	if _, err = store.Append(context.Background(), ahead); err == nil {
		t.Fatal("ahead request accepted")
	}
	read, err := store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(read.Records) != 2 {
		t.Fatalf("read %+v %v", read, err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	next := AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: 2, RequestID: []byte("request-2"), Producer: "test", Records: []InputRecord{{Payload: []byte("three")}}}
	result, err = store.Append(context.Background(), next)
	if err != nil || result.FirstSequence != 2 {
		t.Fatalf("restart append %+v %v", result, err)
	}
}

func TestCommitStatsSurviveCheckpointWithoutDuplication(t *testing.T) {
	store, err := OpenWithGroupCommit(t.TempDir(), GroupCommitOptions{MaxDelay: time.Microsecond})
	if err != nil {
		t.Fatal(err)
	}
	request := AppendRequest{Namespace: "stats", Stream: "events", RequestID: []byte("one"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}}}
	if _, err = store.Append(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	before := store.CommitStats()
	if before.Groups == 0 || before.Groups != before.LocalSyncs {
		t.Fatalf("before checkpoint stats = %+v", before)
	}
	if _, created, checkpointErr := store.Checkpoint(); checkpointErr != nil || !created {
		t.Fatalf("checkpoint created=%v error=%v", created, checkpointErr)
	}
	afterCheckpoint := store.CommitStats()
	if afterCheckpoint.Groups != before.Groups || afterCheckpoint.LocalSyncs != before.LocalSyncs {
		t.Fatalf("checkpoint duplicated or lost stats: before=%+v after=%+v", before, afterCheckpoint)
	}
	request.ExpectedSequence = 1
	request.RequestID = []byte("two")
	if _, err = store.Append(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	afterAppend := store.CommitStats()
	if afterAppend.Groups != before.Groups+1 || afterAppend.LocalSyncs != before.LocalSyncs+1 {
		t.Fatalf("new Committer generation stats = %+v, before = %+v", afterAppend, before)
	}
	if repeated := store.CommitStats(); repeated.Groups != afterAppend.Groups || repeated.LocalSyncs != afterAppend.LocalSyncs {
		t.Fatalf("repeated snapshot duplicated stats: first=%+v repeated=%+v", afterAppend, repeated)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	closed := store.CommitStats()
	if closed.Groups != afterAppend.Groups || closed.LocalSyncs != afterAppend.LocalSyncs {
		t.Fatalf("Close duplicated stats: before=%+v closed=%+v", afterAppend, closed)
	}
}

func TestOpenRejectsNegativeGroupCommitOptions(t *testing.T) {
	tests := []GroupCommitOptions{
		{MaxDelay: -time.Nanosecond},
		{MaxRequests: -1},
		{QueueCapacity: -1},
	}
	for _, options := range tests {
		if store, err := OpenWithGroupCommit(t.TempDir(), options); err == nil {
			_ = store.Close()
			t.Fatalf("accepted negative Group Commit options: %+v", options)
		}
	}
}

func TestReplicatedEngineUsesTermGuardAndStrictWatermarks(t *testing.T) {
	identity := format.NodeIdentity{ClusterID: engineID(1), GroupID: engineID(2), NodeID: engineID(3), CreatedAt: 1}
	guard := &engineGuard{}
	replica := &engineReplica{}
	store, err := OpenReplicated(t.TempDir(), identity, ReplicationOptions{Term: 7, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: replica, Guard: guard})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, err = store.Append(context.Background(), AppendRequest{Namespace: "strict", Stream: "events", RequestID: []byte("one"), Producer: "test", Records: []InputRecord{{Payload: []byte("value")}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(replica.terms) != 2 || replica.terms[0] != 7 || replica.terms[1] != 7 {
		t.Fatalf("replicated Terms = %v", replica.terms)
	}
	health := store.Health()
	if health.Role != format.ReplicationRolePrimary || health.Durability != format.ReplicationDurabilityStrict || health.Term != 7 || !health.Watermarks.HasReplicated || health.Watermarks.Replicated != health.Watermarks.Committed {
		t.Fatalf("Health = %+v", health)
	}
	guard.err = errors.New("lease expired")
	_, err = store.Append(context.Background(), AppendRequest{Namespace: "strict", Stream: "events", ExpectedSequence: 1, RequestID: []byte("two"), Producer: "test", Records: []InputRecord{{}}})
	if !errors.Is(err, errdefs.ErrNotLeader) {
		t.Fatalf("expired Lease error = %v", err)
	}
}

func TestSingleEngineRejectsReplicatedDataRoot(t *testing.T) {
	root := t.TempDir()
	node := format.NodeIdentity{ClusterID: engineID(1), GroupID: engineID(2), NodeID: engineID(3), CreatedAt: 1}
	store, err := OpenWithIdentity(root, node)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	states, err := replicationstate.Open(root, node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = states.Update(time.Now(), func(header *format.ReplicationStateHeader) error {
		header.Term = 7
		header.Role = format.ReplicationRoleRecovering
		header.Durability = format.ReplicationDurabilityStrict
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenWithIdentity(root, node); err == nil || !strings.Contains(err.Error(), "cannot open as Single") {
		t.Fatalf("OpenWithIdentity replicated root error = %v", err)
	}
	if _, err = Open(root); err == nil || !strings.Contains(err.Error(), "cannot open as Single") {
		t.Fatalf("Open replicated root error = %v", err)
	}

	recovering, err := OpenReplicated(root, node, ReplicationOptions{Term: 7, Role: format.ReplicationRoleRecovering, Durability: format.ReplicationDurabilityStrict, Guard: &engineGuard{}})
	if err != nil {
		t.Fatal(err)
	}
	if err = recovering.Close(); err != nil {
		t.Fatal(err)
	}
	stale, ok := states.Current()
	if !ok {
		t.Fatal("missing Replication State")
	}
	if _, err = states.Update(time.Now(), func(*format.ReplicationStateHeader) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenReplicated(root, node, ReplicationOptions{Term: 7, Role: format.ReplicationRoleRecovering, Durability: format.ReplicationDurabilityStrict, Guard: &engineGuard{}, ExpectedStateID: stale.Header.StateID}); err == nil || !strings.Contains(err.Error(), "changed before engine lock") {
		t.Fatalf("stale Replication State open error = %v", err)
	}
}

func engineID(value byte) format.UUID {
	var id format.UUID
	id[15] = value
	return id
}

func TestConcurrentAppendsAcrossStreams(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const streams = 24
	for i := 0; i < streams; i++ {
		name := fmt.Sprintf("stream-%02d", i)
		if _, err = store.Append(context.Background(), AppendRequest{Namespace: "concurrent", Stream: name, RequestID: []byte("initial"), Producer: "test", Records: []InputRecord{{Payload: []byte("zero")}}}); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	errorsByStream := make(chan error, streams)
	var wait sync.WaitGroup
	for i := 0; i < streams; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			<-start
			name := fmt.Sprintf("stream-%02d", i)
			_, appendErr := store.Append(context.Background(), AppendRequest{Namespace: "concurrent", Stream: name, ExpectedSequence: 1, RequestID: []byte("second"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}}})
			errorsByStream <- appendErr
		}(i)
	}
	close(start)
	wait.Wait()
	close(errorsByStream)
	for appendErr := range errorsByStream {
		if appendErr != nil {
			t.Fatal(appendErr)
		}
	}
	for i := 0; i < streams; i++ {
		name := fmt.Sprintf("stream-%02d", i)
		result, readErr := store.Read("concurrent", name, 0, 10, 0)
		if readErr != nil || len(result.Records) != 2 || result.Records[1].Sequence != 1 {
			t.Fatalf("%s Read = %+v, error = %v", name, result, readErr)
		}
	}
}

func TestConcurrentSameSequenceHasSingleWinner(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.Append(context.Background(), AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("initial"), Producer: "test", Records: []InputRecord{{Payload: []byte("zero")}}}); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			<-start
			_, appendErr := store.Append(context.Background(), AppendRequest{Namespace: "n", Stream: "s", ExpectedSequence: 1, RequestID: []byte(fmt.Sprintf("request-%d", i)), Producer: "test", Records: []InputRecord{{Payload: []byte{byte(i)}}}})
			results <- appendErr
		}(i)
	}
	close(start)
	var successes, conflicts int
	for i := 0; i < 2; i++ {
		appendErr := <-results
		if appendErr == nil {
			successes++
		} else if errors.Is(appendErr, errdefs.ErrSequenceConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected Append error = %v", appendErr)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes = %d, conflicts = %d", successes, conflicts)
	}
}

func TestWaitForAppendDoesNotLoseNotifications(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	woke := make(chan error, 1)
	go func() { woke <- store.WaitForAppend(context.Background(), "n", "s", 0) }()
	request := AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Producer: "test", Records: []InputRecord{{Payload: []byte("record")}}}
	if _, err = store.Append(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-woke:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForAppend did not observe committed Append")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = store.WaitForAppend(ctx, "n", "s", 0); err != nil {
		t.Fatalf("already-visible Append was missed: %v", err)
	}
}

func TestCloseUnblocksAppendWaiters(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	woke := make(chan error, 1)
	go func() { woke <- store.WaitForAppend(context.Background(), "n", "s", 0) }()
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-woke:
		if !errors.Is(err, errdefs.ErrClosed) {
			t.Fatalf("wait error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock WaitForAppend")
	}
}
func TestRequestHashCanonicalHeaderOrder(t *testing.T) {
	a := AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Records: []InputRecord{{Headers: []format.Header{{Key: "z", Value: []byte("1")}, {Key: "a", Value: []byte("2")}}}}}
	b := a
	b.Records = []InputRecord{{Headers: []format.Header{{Key: "a", Value: []byte("2")}, {Key: "z", Value: []byte("1")}}}}
	hashA, err := RequestHash(a)
	if err != nil {
		t.Fatal(err)
	}
	hashB, err := RequestHash(b)
	if err != nil {
		t.Fatal(err)
	}
	if hashA != hashB {
		t.Fatal("stable request hashes differ")
	}
}

func TestCheckpointPublishesSegmentRotatesWALAndRestarts(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	request := AppendRequest{Namespace: "agent", Stream: "events", RequestID: []byte("first"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}, {Payload: []byte("two")}}}
	if _, err = store.Append(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	manifest, created, err := store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if !created || manifest.Header.RecordCount != 3 || len(manifest.SegmentReferences) != 1 {
		t.Fatalf("checkpoint Manifest = %+v, created = %v", manifest, created)
	}
	if len(manifest.ArtifactReferences) != 3 || manifest.ArtifactReferences[0].ArtifactType != format.ArtifactTailCatalog || manifest.ArtifactReferences[1].ArtifactType != format.ArtifactLocatorSnapshot || manifest.ArtifactReferences[2].ArtifactType != format.ArtifactRegistrySnapshot || store.state.TailCatalog == nil || store.state.Locator == nil || !store.state.Registry.HasSnapshot() || store.state.TailCatalog.Header().ManifestGeneration != manifest.Header.Generation {
		t.Fatalf("checkpoint Tail Catalog was not installed: %+v", manifest.ArtifactReferences)
	}
	if _, created, err = store.Checkpoint(); err != nil || created {
		t.Fatalf("empty checkpoint created=%v error=%v", created, err)
	}
	read, err := store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(read.Records) != 2 {
		t.Fatalf("post-checkpoint Read = %+v, %v", read, err)
	}
	second := AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: 2, RequestID: []byte("second"), Producer: "test", Records: []InputRecord{{Payload: []byte("three")}}}
	if _, err = store.Append(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if store.state.TailCatalog == nil {
		t.Fatal("Tail Catalog was not recovered")
	}
	if !store.state.Registry.HasSnapshot() {
		t.Fatal("Registry Snapshot was not recovered")
	}
	if overlay := store.state.Registry.MappingsAfter(0); len(overlay) != 0 {
		t.Fatalf("Registry recovery retained %d checkpoint mappings in memory Overlay", len(overlay))
	}
	read, err = store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(read.Records) != 3 || string(read.Records[2].Payload) != "three" {
		t.Fatalf("restart Read = %+v, %v", read, err)
	}
}

func TestCheckpointAllowsAppendAndReadDuringFlush(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	firstRequest := AppendRequest{Namespace: "agent", Stream: "events", RequestID: []byte("first"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}}}
	first, err := store.Append(context.Background(), firstRequest)
	if err != nil {
		t.Fatal(err)
	}

	switched := make(chan struct{})
	releaseFlush := make(chan struct{})
	var switchOnce sync.Once
	store.checkpointHook = func(point string) error {
		if point == "after_memtable_switch" {
			switchOnce.Do(func() { close(switched) })
			<-releaseFlush
		}
		return nil
	}
	type checkpointResult struct {
		manifest format.Manifest
		created  bool
		err      error
	}
	checkpointDone := make(chan checkpointResult, 1)
	go func() {
		manifest, created, checkpointErr := store.Checkpoint()
		checkpointDone <- checkpointResult{manifest: manifest, created: created, err: checkpointErr}
	}()
	select {
	case <-switched:
	case <-time.After(5 * time.Second):
		close(releaseFlush)
		t.Fatal("checkpoint did not switch MemTables")
	}

	secondRequest := AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: 1, RequestID: []byte("second"), Producer: "test", Records: []InputRecord{{Payload: []byte("two")}}}
	appendDone := make(chan error, 1)
	go func() {
		_, appendErr := store.Append(context.Background(), secondRequest)
		appendDone <- appendErr
	}()
	select {
	case err = <-appendDone:
		if err != nil {
			close(releaseFlush)
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		close(releaseFlush)
		t.Fatal("Append remained blocked during checkpoint Flush")
	}

	result, err := store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(result.Records) != 2 || string(result.Records[0].Payload) != "one" || string(result.Records[1].Payload) != "two" {
		close(releaseFlush)
		t.Fatalf("layered Read = %+v, error = %v", result, err)
	}
	info, err := store.Inspect("agent", "events")
	if err != nil || info.RecordCount != 2 || info.FirstRecordedAt != first.FirstRecordedAt {
		close(releaseFlush)
		t.Fatalf("layered Inspect = %+v, error = %v", info, err)
	}
	sequence, _, found, err := store.ResolveTime("agent", "events", first.FirstRecordedAt, readstore.AtOrAfter)
	if err != nil || !found || sequence != 0 {
		close(releaseFlush)
		t.Fatalf("layered ResolveTime = %d found=%v error=%v", sequence, found, err)
	}
	deduplicated, err := store.Append(context.Background(), firstRequest)
	if err != nil || !deduplicated.Deduplicated || deduplicated.NextSequence != 1 {
		close(releaseFlush)
		t.Fatalf("layered deduplication = %+v, error = %v", deduplicated, err)
	}
	newStream := AppendRequest{Namespace: "agent", Stream: "new", RequestID: []byte("new"), Producer: "test", Records: []InputRecord{{Payload: []byte("created during Flush")}}}
	if _, err = store.Append(context.Background(), newStream); err != nil {
		close(releaseFlush)
		t.Fatal(err)
	}

	close(releaseFlush)
	checkpoint := <-checkpointDone
	if checkpoint.err != nil || !checkpoint.created {
		t.Fatalf("Checkpoint created=%v error=%v", checkpoint.created, checkpoint.err)
	}
	mapping, ok, err := store.state.Registry.Lookup("agent", "events")
	if err != nil || !ok {
		t.Fatalf("event mapping found=%v error=%v", ok, err)
	}
	newMapping, ok, err := store.state.Registry.Lookup("agent", "new")
	if err != nil || !ok {
		t.Fatalf("new mapping found=%v error=%v", ok, err)
	}
	result, err = store.Read("agent", "new", 0, 10, 0)
	if err != nil || len(result.Records) != 1 || string(result.Records[0].Payload) != "created during Flush" {
		t.Fatalf("new Stream after checkpoint = %+v, error = %v", result, err)
	}
	snapshots := store.state.MemTable.Snapshot()
	retained := make(map[uint64]int, len(snapshots))
	for _, snapshot := range snapshots {
		retained[snapshot.StreamID] = len(snapshot.Frames)
	}
	if len(snapshots) != 3 || retained[mapping.StreamID] != 1 || retained[newMapping.StreamID] != 1 || retained[registry.RegistryStreamID] != 1 {
		t.Fatalf("active MemTable retained unexpected checkpoint Tails: %+v", snapshots)
	}
}

func TestReplicatedCheckpointPersistsCommittedStateBeforeManifest(t *testing.T) {
	dir := t.TempDir()
	node := format.NodeIdentity{ClusterID: engineID(1), GroupID: engineID(2), NodeID: engineID(3), CreatedAt: 1}
	if err := os.MkdirAll(filepath.Join(dir, "meta"), 0750); err != nil {
		t.Fatal(err)
	}
	states, err := replicationstate.Open(dir, node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = states.Update(time.Unix(1, 0), func(header *format.ReplicationStateHeader) error {
		header.Term = 7
		header.Role = format.ReplicationRolePrimary
		header.Durability = format.ReplicationDurabilityStrict
		header.HasLeader = true
		header.LeaderID = node.NodeID
		header.HasLease = true
		header.LeaseExpiresAt = time.Unix(60, 0).UnixNano()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	store, err := OpenReplicated(dir, node, ReplicationOptions{Term: 7, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: &engineReplica{}, Guard: &engineGuard{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), AppendRequest{Namespace: "ha", Stream: "events", RequestID: []byte("one"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}}}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected crash after Manifest publish")
	store.checkpointHook = func(point string) error {
		if point == "after_manifest_publish" {
			return injected
		}
		return nil
	}
	if _, _, err = store.CheckpointReplicated(states); !errors.Is(err, injected) {
		t.Fatalf("CheckpointReplicated error = %v", err)
	}
	manifest, ok := store.state.Manifest.Current()
	if !ok {
		t.Fatal("Manifest was not published before injected crash")
	}
	state, ok := states.Current()
	if !ok || !state.Header.Committed.Present || state.Header.Committed.EntryID < manifest.Header.LastEntryID || !state.Header.Applied.Present || state.Header.Applied.EntryID < manifest.Header.LastEntryID {
		t.Fatalf("Replication State does not cover Manifest checkpoint: state=%+v manifest=%+v", state.Header, manifest.Header)
	}
	_ = store.Close()
}

func TestCompactSwitchesGenerationBeforeRetiringInputs(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	appendAndCheckpoint := func(expected uint64, requestID, payload string) format.Manifest {
		t.Helper()
		_, appendErr := store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: expected, RequestID: []byte(requestID), Producer: "test", Records: []InputRecord{{Payload: []byte(payload)}}})
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		manifest, created, checkpointErr := store.Checkpoint()
		if checkpointErr != nil || !created {
			t.Fatalf("Checkpoint created=%v error=%v", created, checkpointErr)
		}
		return manifest
	}
	appendAndCheckpoint(0, "first", "one")
	firstLocator := store.state.Locator
	before := appendAndCheckpoint(1, "second", "two")
	if _, _, lookupErr := firstLocator.LookupSequence(^uint64(0), 0); !errors.Is(lookupErr, os.ErrClosed) {
		t.Fatalf("replaced checkpoint Locator remains open: %v", lookupErr)
	}
	if len(before.SegmentReferences) != 2 {
		t.Fatalf("Segment count before Compact = %d", len(before.SegmentReferences))
	}

	stopReads := make(chan struct{})
	readErrors := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stopReads:
				readErrors <- nil
				return
			default:
				result, readErr := store.Read("agent", "events", 0, 10, 0)
				if readErr != nil || len(result.Records) != 2 {
					readErrors <- fmt.Errorf("concurrent Read records=%d: %w", len(result.Records), readErr)
					return
				}
			}
		}
	}()
	previousLocator := store.state.Locator
	compacted, err := store.Compact(CompactionOptions{MinSegments: 2, MaxInputSegments: 4, MaxInputBytes: 64 << 20})
	close(stopReads)
	if readErr := <-readErrors; readErr != nil {
		t.Fatal(readErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !compacted.Created || compacted.InputSegments != 2 || compacted.Manifest.Header.Generation != before.Header.Generation+1 || len(compacted.Manifest.SegmentReferences) != 1 {
		t.Fatalf("Compaction = %+v", compacted)
	}
	if _, _, lookupErr := previousLocator.LookupSequence(^uint64(0), 0); !errors.Is(lookupErr, os.ErrClosed) {
		t.Fatalf("replaced compaction Locator remains open: %v", lookupErr)
	}
	result, err := store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(result.Records) != 2 || string(result.Records[1].Payload) != "two" {
		t.Fatalf("post-Compact Read = %+v, %v", result, err)
	}
	for _, reference := range before.SegmentReferences {
		if _, statErr := os.Stat(filepath.Join(dir, reference.LocalPath)); !os.IsNotExist(statErr) {
			t.Fatalf("retired input Segment remains live: %v", statErr)
		}
	}
}

func TestPinnedManifestDefersCompactionRetirement(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i, payload := range []string{"one", "two"} {
		_, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: uint64(i), RequestID: []byte(payload), Producer: "test", Records: []InputRecord{{Payload: []byte(payload)}}})
		if err != nil {
			t.Fatal(err)
		}
		if _, created, checkpointErr := store.Checkpoint(); checkpointErr != nil || !created {
			t.Fatalf("Checkpoint created=%v error=%v", created, checkpointErr)
		}
	}
	pinned, _, release, err := store.CheckpointAndPin()
	if err != nil {
		t.Fatal(err)
	}
	pinnedPacks := store.state.Locator.PackArtifacts()
	if _, err = store.Compact(CompactionOptions{MinSegments: 2, MaxInputSegments: 4, MaxInputBytes: 64 << 20}); err != nil {
		t.Fatal(err)
	}
	for _, reference := range pinned.SegmentReferences {
		if _, err = os.Stat(filepath.Join(dir, reference.LocalPath)); err != nil {
			t.Fatalf("pinned Segment retired: %v", err)
		}
	}
	for _, reference := range pinned.ArtifactReferences {
		if _, err = os.Stat(filepath.Join(dir, reference.Path)); err != nil {
			t.Fatalf("pinned Artifact retired: %v", err)
		}
	}
	for _, reference := range pinnedPacks {
		if _, err = os.Stat(filepath.Join(dir, reference.Path)); err != nil {
			t.Fatalf("pinned Locator Pack retired: %v", err)
		}
	}
	release()
	for _, reference := range pinned.SegmentReferences {
		if _, err = os.Stat(filepath.Join(dir, reference.LocalPath)); !os.IsNotExist(err) {
			t.Fatalf("released Segment remains live: %v", err)
		}
	}
	for _, reference := range pinned.ArtifactReferences {
		if _, err = os.Stat(filepath.Join(dir, reference.Path)); !os.IsNotExist(err) {
			t.Fatalf("released Artifact remains live: %v", err)
		}
	}
	for _, reference := range pinnedPacks {
		if _, err = os.Stat(filepath.Join(dir, reference.Path)); !os.IsNotExist(err) {
			t.Fatalf("released Locator Pack remains live: %v", err)
		}
	}
}

func TestRecoveryRebuildsWhenTailCatalogIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", RequestID: []byte("one"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}}})
	if err != nil {
		t.Fatal(err)
	}
	manifest, created, err := store.Checkpoint()
	if err != nil || !created {
		t.Fatalf("Checkpoint created=%v error=%v", created, err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reference := manifest.ArtifactReferences[0]
	path := filepath.Join(dir, filepath.FromSlash(reference.Path))
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, 4096+16); err != nil {
		t.Fatal(err)
	}
	file.Close()
	store, err = Open(dir)
	if err != nil {
		t.Fatalf("recovery rejected reconstructible Tail corruption: %v", err)
	}
	defer store.Close()
	if store.state.TailCatalog == nil {
		t.Fatal("Tail Catalog fixed metadata was not installed lazily")
	}
	result, err := store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(result.Records) != 1 {
		t.Fatalf("rebuilt Read = %+v, %v", result, err)
	}
}

func TestRecoveryKeepsHistoricalTailsAndDirectoriesOutOfMemTable(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	const streamCount = 32
	for i := 0; i < streamCount; i++ {
		name := fmt.Sprintf("events-%d", i)
		if _, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: name, RequestID: []byte("one"), Producer: "test", Records: []InputRecord{{Payload: []byte(name)}}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, created, checkpointErr := store.Checkpoint(); checkpointErr != nil || !created {
		t.Fatalf("Checkpoint created=%v error=%v", created, checkpointErr)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if snapshots := store.state.MemTable.Snapshot(); len(snapshots) != 0 {
		t.Fatalf("recovery loaded %d checkpoint-only Stream tails", len(snapshots))
	}
	for _, descriptor := range store.state.Segments {
		if descriptor.Directories != nil {
			t.Fatal("recovery retained a Segment Directory")
		}
	}
	result, err := store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events-17", ExpectedSequence: 1, RequestID: []byte("two"), Producer: "test", Records: []InputRecord{{Payload: []byte("two")}}})
	if err != nil || result.FirstSequence != 1 || result.NextSequence != 2 {
		t.Fatalf("Append after lazy Tail resolution = %+v, %v", result, err)
	}
	if snapshots := store.state.MemTable.Snapshot(); len(snapshots) != 1 || snapshots[0].StreamID == 0 {
		t.Fatalf("active MemTable contains unexpected Streams: %+v", snapshots)
	}
}

func TestRecoveryDoesNotTouchSegmentDirectoryWithValidProjections(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", RequestID: []byte("one"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}}}); err != nil {
		t.Fatal(err)
	}
	firstManifest, created, err := store.Checkpoint()
	if err != nil || !created {
		t.Fatalf("Checkpoint created=%v error=%v", created, err)
	}
	if _, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: 1, RequestID: []byte("two"), Producer: "test", Records: []InputRecord{{Payload: []byte("two")}}}); err != nil {
		t.Fatal(err)
	}
	if _, created, err = store.Checkpoint(); err != nil || !created {
		t.Fatalf("second Checkpoint created=%v error=%v", created, err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	segmentPath := filepath.Join(dir, filepath.FromSlash(firstManifest.SegmentReferences[0].LocalPath))
	file, err := os.OpenFile(segmentPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, int64(format.SegmentSectionAlignment)); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatalf("startup touched a historical Segment Directory: %v", err)
	}
	defer store.Close()
	info, err := store.Inspect("agent", "events")
	if err != nil || !info.Exists || info.NextSequence != 2 {
		t.Fatalf("Inspect from projections = %+v, %v", info, err)
	}
	if _, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: 2, RequestID: []byte("three"), Producer: "test", Records: []InputRecord{{Payload: []byte("three")}}}); err != nil {
		t.Fatalf("Append from projected Tail: %v", err)
	}
}

func TestRecoveryPreservesRecordedAtClampFromLatestSegment(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return time.Unix(0, 1000) }
	if _, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", RequestID: []byte("one"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}}}); err != nil {
		t.Fatal(err)
	}
	if _, created, checkpointErr := store.Checkpoint(); checkpointErr != nil || !created {
		t.Fatalf("Checkpoint created=%v error=%v", created, checkpointErr)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.now = func() time.Time { return time.Unix(0, 1) }
	result, err := store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: 1, RequestID: []byte("two"), Producer: "test", Records: []InputRecord{{Payload: []byte("two")}}})
	if err != nil || result.LastRecordedAt != 1000 {
		t.Fatalf("Append after clock rollback = %+v, %v", result, err)
	}
}

func TestRecoveryReadsSegmentsWhenLocatorPackIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", RequestID: []byte("one"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}}}); err != nil {
		t.Fatal(err)
	}
	if _, created, checkpointErr := store.Checkpoint(); checkpointErr != nil || !created {
		t.Fatalf("Checkpoint created=%v error=%v", created, checkpointErr)
	}
	pack := store.state.Locator.PackArtifacts()[0]
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(dir, filepath.FromSlash(pack.Path)), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, int64(format.SegmentSectionAlignment+32)); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatalf("recovery rejected reconstructible Locator corruption: %v", err)
	}
	defer store.Close()
	result, err := store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(result.Records) != 1 || string(result.Records[0].Payload) != "one" {
		t.Fatalf("fallback Read = %+v, %v", result, err)
	}
}

func TestRecoveryRebuildsWhenLocatorSnapshotIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", RequestID: []byte("one"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}}}); err != nil {
		t.Fatal(err)
	}
	manifest, created, err := store.Checkpoint()
	if err != nil || !created {
		t.Fatalf("Checkpoint created=%v error=%v", created, err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	var reference format.ArtifactReference
	for _, artifact := range manifest.ArtifactReferences {
		if artifact.ArtifactType == format.ArtifactLocatorSnapshot {
			reference = artifact
		}
	}
	file, err := os.OpenFile(filepath.Join(dir, filepath.FromSlash(reference.Path)), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, 32); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatalf("recovery rejected reconstructible Locator Snapshot corruption: %v", err)
	}
	defer store.Close()
	if store.state.Locator != nil {
		t.Fatal("corrupt Locator Snapshot was installed")
	}
	result, err := store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(result.Records) != 1 {
		t.Fatalf("fallback Read = %+v, %v", result, err)
	}
}

func TestReadFallsBackWhenLocatorRootIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", RequestID: []byte("one"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}}}); err != nil {
		t.Fatal(err)
	}
	manifest, created, err := store.Checkpoint()
	if err != nil || !created {
		t.Fatalf("Checkpoint created=%v error=%v", created, err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	var reference format.ArtifactReference
	for _, artifact := range manifest.ArtifactReferences {
		if artifact.ArtifactType == format.ArtifactLocatorSnapshot {
			reference = artifact
		}
	}
	path := filepath.Join(dir, filepath.FromSlash(reference.Path))
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	packLength := binary.LittleEndian.Uint32(encoded[format.LocatorSnapshotHeaderLength : format.LocatorSnapshotHeaderLength+4])
	rootOffset := int64(format.LocatorSnapshotHeaderLength) + int64(packLength)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, rootOffset+8); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatalf("recovery rejected lazily validated Locator Root: %v", err)
	}
	defer store.Close()
	if store.state.Locator == nil {
		t.Fatal("Locator with intact metadata was not installed")
	}
	result, err := store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(result.Records) != 1 || string(result.Records[0].Payload) != "one" {
		t.Fatalf("fallback Read = %+v, %v", result, err)
	}
}

func TestRegistryBlockCorruptionFallsBackToRegistryStream(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", RequestID: []byte("one"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}}}); err != nil {
		t.Fatal(err)
	}
	manifest, created, err := store.Checkpoint()
	if err != nil || !created {
		t.Fatalf("Checkpoint created=%v error=%v", created, err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	var reference format.ArtifactReference
	for _, artifact := range manifest.ArtifactReferences {
		if artifact.ArtifactType == format.ArtifactRegistrySnapshot {
			reference = artifact
		}
	}
	path := filepath.Join(dir, filepath.FromSlash(reference.Path))
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	header, err := format.UnmarshalRegistrySnapshotHeader(encoded[:format.RegistrySnapshotHeaderLength])
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, int64(header.EntriesOffset+8)); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	result, err := store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(result.Records) != 1 {
		t.Fatalf("Registry fallback Read = %+v, %v", result, err)
	}
	if store.state.Registry.HasSnapshot() {
		t.Fatal("corrupt Registry Snapshot remained installed after fallback")
	}
}

func TestCompactionPreservesRegistryOverlayAfterCheckpoint(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i, payload := range []string{"one", "two"} {
		if _, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: uint64(i), RequestID: []byte(payload), Producer: "test", Records: []InputRecord{{Payload: []byte(payload)}}}); err != nil {
			t.Fatal(err)
		}
		if _, created, checkpointErr := store.Checkpoint(); checkpointErr != nil || !created {
			t.Fatalf("Checkpoint created=%v error=%v", created, checkpointErr)
		}
	}
	if _, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "active", RequestID: []byte("active"), Producer: "test", Records: []InputRecord{{Payload: []byte("active")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Compact(CompactionOptions{MinSegments: 2, MaxInputSegments: 4, MaxInputBytes: 64 << 20}); err != nil {
		t.Fatal(err)
	}
	result, err := store.Read("agent", "active", 0, 10, 0)
	if err != nil || len(result.Records) != 1 || string(result.Records[0].Payload) != "active" {
		t.Fatalf("active Registry overlay Read = %+v, %v", result, err)
	}
}

func TestCapacityCriticalRejectsAppendButPreservesReadAndMaintenance(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	request := AppendRequest{Namespace: "agent", Stream: "events", RequestID: []byte("first"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}}}
	if _, err = store.Append(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	stats := store.MaintenanceStats()
	if stats.MemTableRecords != 2 || stats.MemTableBytes == 0 || stats.ActiveWALBytes == 0 {
		t.Fatalf("maintenance stats = %+v", stats)
	}
	store.SetCapacityCritical(true)
	request.ExpectedSequence = 1
	request.RequestID = []byte("second")
	if _, err = store.Append(context.Background(), request); !errors.Is(err, errdefs.ErrCapacityCritical) {
		t.Fatalf("Append error = %v", err)
	}
	result, err := store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(result.Records) != 1 {
		t.Fatalf("Read during capacity critical = %+v, %v", result, err)
	}
	if _, created, checkpointErr := store.Checkpoint(); checkpointErr != nil || !created {
		t.Fatalf("Checkpoint during capacity critical created=%v error=%v", created, checkpointErr)
	}
	store.SetCapacityCritical(false)
	if _, err = store.Append(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}
