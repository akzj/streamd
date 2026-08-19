package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akzj/streamd/internal/storage/commit"
	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/errdefs"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/retention"
	"github.com/akzj/streamd/internal/storage/scrub"
	"github.com/akzj/streamd/internal/storage/wal"
)

type report struct {
	DurationSeconds  float64      `json:"duration_seconds"`
	SetupSeconds     float64      `json:"setup_seconds"`
	DrainSeconds     float64      `json:"drain_seconds"`
	Precreated       bool         `json:"precreated_streams"`
	Workers          int          `json:"workers"`
	Streams          int          `json:"streams"`
	BatchRecords     int          `json:"batch_records"`
	PayloadBytes     int          `json:"payload_bytes"`
	Requests         uint64       `json:"requests"`
	Records          uint64       `json:"records"`
	Bytes            uint64       `json:"bytes"`
	Errors           uint64       `json:"errors"`
	DeadlineExits    uint64       `json:"deadline_exits"`
	UncertainResults uint64       `json:"uncertain_results"`
	RequestsPerSec   float64      `json:"requests_per_second"`
	RecordsPerSec    float64      `json:"records_per_second"`
	MiBPerSec        float64      `json:"mib_per_second"`
	DataDirectory    string       `json:"data_directory,omitempty"`
	Scrubbed         bool         `json:"scrubbed"`
	ScrubSegments    uint64       `json:"scrub_segments,omitempty"`
	Mode             string       `json:"mode"`
	StandbyDirectory string       `json:"standby_directory,omitempty"`
	StandbyVerified  bool         `json:"standby_verified"`
	ReopenSeconds    float64      `json:"reopen_seconds,omitempty"`
	ReopenVerified   bool         `json:"reopen_verified"`
	ReopenHeapAlloc  uint64       `json:"reopen_heap_alloc_bytes,omitempty"`
	ReopenHeapSys    uint64       `json:"reopen_heap_sys_bytes,omitempty"`
	Checkpoints      uint64       `json:"checkpoints"`
	Compactions      uint64       `json:"compactions"`
	Snapshots        uint64       `json:"snapshots"`
	WALDeletedBytes  uint64       `json:"wal_deleted_bytes"`
	GroupCommit      commitReport `json:"group_commit"`
}

type commitReport struct {
	ConfiguredDelayMicros   float64 `json:"configured_delay_micros"`
	ConfiguredMaxRequests   int     `json:"configured_max_requests"`
	ConfiguredMaxBytes      uint64  `json:"configured_max_bytes"`
	ConfiguredQueueCapacity int     `json:"configured_queue_capacity"`
	Groups                  uint64  `json:"groups"`
	Requests                uint64  `json:"requests"`
	Entries                 uint64  `json:"entries"`
	Bytes                   uint64  `json:"bytes"`
	LocalSyncs              uint64  `json:"local_syncs"`
	ReplicatedGroups        uint64  `json:"replicated_groups"`
	AverageRequestsPerGroup float64 `json:"average_requests_per_group"`
	AverageEntriesPerGroup  float64 `json:"average_entries_per_group"`
	MaxRequestsPerGroup     uint64  `json:"max_requests_per_group"`
	MaxBytesPerGroup        uint64  `json:"max_bytes_per_group"`
	AverageQueueWaitMicros  float64 `json:"average_queue_wait_micros"`
	AverageCollectMicros    float64 `json:"average_collect_micros"`
	AppendSeconds           float64 `json:"append_seconds"`
	LocalSyncSeconds        float64 `json:"local_sync_seconds"`
	ReplicateSeconds        float64 `json:"replicate_seconds"`
	ApplySeconds            float64 `json:"apply_seconds"`
	ProcessSeconds          float64 `json:"process_seconds"`
	LocalSyncProcessRatio   float64 `json:"local_sync_process_ratio"`
	LocalSyncWallRatio      float64 `json:"local_sync_wall_ratio"`
	ReplicateProcessRatio   float64 `json:"replicate_process_ratio"`
	ProcessWallRatio        float64 `json:"process_wall_ratio"`
	FinalQueueDepth         uint64  `json:"final_queue_depth"`
	QueueCapacity           uint64  `json:"queue_capacity"`
}

type durableReplica struct{ log *wal.Log }

func (r durableReplica) Replicate(_ context.Context, encoded [][]byte) (uint64, error) {
	if err := r.log.Append(encoded...); err != nil {
		return 0, err
	}
	if err := r.log.Sync(); err != nil {
		return 0, err
	}
	entry, err := format.UnmarshalWALEntry(encoded[len(encoded)-1])
	return entry.EntryID, err
}

func (durableReplica) AdvanceCommit(context.Context, uint64) error { return nil }

type benchmarkGuard struct{}

func (benchmarkGuard) CanCommit() error { return nil }

func main() {
	duration := flag.Duration("duration", 30*time.Second, "benchmark or soak duration")
	workers := flag.Int("workers", 1, "concurrent writers")
	streams := flag.Int("streams", 100, "logical Streams distributed across writers")
	batch := flag.Int("batch", 1, "records per AppendBatch")
	payloadBytes := flag.Int("payload-bytes", 1024, "payload bytes per Record")
	checkpoint := flag.Duration("checkpoint-interval", time.Minute, "periodic checkpoint interval; 0 disables")
	data := flag.String("data", "", "new or empty data directory; temporary when omitted")
	keep := flag.Bool("keep", false, "keep an automatically-created temporary data directory")
	verify := flag.Bool("verify", true, "checkpoint and scrub the data after the timed run")
	mode := flag.String("mode", "single", "durability mode: single or strict")
	standbyData := flag.String("standby-data", "", "new or empty Standby WAL directory for strict mode")
	groupDelay := flag.Duration("group-delay", 250*time.Microsecond, "maximum time the first request waits for WAL group formation")
	groupRequests := flag.Int("group-requests", 64, "maximum requests per WAL group")
	groupBytes := flag.Uint64("group-bytes", 4<<20, "maximum encoded bytes per WAL group")
	queueCapacity := flag.Int("queue-capacity", 1024, "bounded WAL request queue capacity")
	precreate := flag.Bool("precreate-streams", false, "create every Stream before the timed steady-state run")
	verifyReopen := flag.Bool("verify-reopen", false, "close, reopen, and sample the checkpointed store before scrub (single mode)")
	maxRequestsPerSecond := flag.Int("max-requests-per-second", 0, "global request rate ceiling; 0 is unlimited")
	retentionInterval := flag.Duration("retention-interval", 0, "online linked Snapshot and WAL collection interval; 0 disables")
	maxRetainedWALBytes := flag.Uint64("max-retained-wal-bytes", 4<<30, "retained WAL budget used by online collection")
	flag.Parse()
	if *duration <= 0 || *workers <= 0 || *streams < *workers || *batch <= 0 || *payloadBytes < 0 || *checkpoint < 0 || *groupDelay <= 0 || *groupRequests <= 0 || *groupBytes == 0 || *queueCapacity <= 0 || *maxRequestsPerSecond < 0 || *retentionInterval < 0 || (*retentionInterval > 0 && *checkpoint == 0) || (*mode != "single" && *mode != "strict") {
		fatal(fmt.Errorf("invalid benchmark arguments"))
	}
	groupOptions := engine.GroupCommitOptions{MaxDelay: *groupDelay, MaxRequests: *groupRequests, MaxBytes: *groupBytes, QueueCapacity: *queueCapacity}
	dataPath := *data
	temporary := dataPath == ""
	if temporary {
		var err error
		dataPath, err = os.MkdirTemp("", "streamd-bench-")
		if err != nil {
			fatal(err)
		}
		if !*keep {
			defer os.RemoveAll(dataPath)
		}
	} else if err := requireEmpty(dataPath); err != nil {
		fatal(err)
	}
	identity, err := randomIdentity()
	if err != nil {
		fatal(err)
	}
	var standbyRoot *fsutil.Root
	var standbyLog *wal.Log
	standbyPath := ""
	standbyTemporary := false
	if *mode == "strict" {
		standbyPath = *standbyData
		if standbyPath == "" {
			standbyPath, err = os.MkdirTemp("", "streamd-bench-standby-")
			standbyTemporary = true
		} else {
			err = requireEmpty(standbyPath)
		}
		if err != nil {
			fatal(err)
		}
		if standbyTemporary && !*keep {
			defer os.RemoveAll(standbyPath)
		}
		standbyRoot, err = fsutil.OpenRoot(standbyPath)
		if err != nil {
			fatal(err)
		}
		standbyLog, err = wal.Create(standbyRoot.Path(), 0, 1, time.Now())
		if err != nil {
			fatal(err)
		}
	}
	var store *engine.Store
	if *mode == "strict" {
		store, err = engine.OpenReplicated(dataPath, identity, engine.ReplicationOptions{Term: 1, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: durableReplica{log: standbyLog}, Guard: benchmarkGuard{}, GroupCommit: groupOptions})
	} else {
		store, err = engine.OpenWithIdentityAndGroupCommit(dataPath, identity, groupOptions)
	}
	if err != nil {
		fatal(err)
	}
	var benchmarkStates *replicationstate.Store
	if *mode == "strict" {
		benchmarkStates, err = replicationstate.Open(dataPath, identity)
		if err == nil {
			_, err = benchmarkStates.Update(time.Now(), func(header *format.ReplicationStateHeader) error {
				header.Term = 1
				header.Role = format.ReplicationRolePrimary
				header.Durability = format.ReplicationDurabilityStrict
				header.HasLeader = true
				header.LeaderID = identity.NodeID
				header.HasLease = true
				header.LeaseExpiresAt = time.Now().Add(*duration + 24*time.Hour).UnixNano()
				return nil
			})
		}
		if err != nil {
			fatal(err)
		}
	}
	closed := false
	defer func() {
		if !closed {
			_ = store.Close()
		}
		if standbyLog != nil {
			_ = standbyLog.Close()
		}
		if standbyRoot != nil {
			_ = standbyRoot.Close()
		}
	}()
	setupStarted := time.Now()
	if *precreate {
		if err = precreateStreams(store, *streams, *workers); err != nil {
			fatal(err)
		}
		if err = store.CommitBarrier(context.Background()); err != nil {
			fatal(err)
		}
	}
	setupSeconds := time.Since(setupStarted).Seconds()
	commitBaseline := store.CommitStats()
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	var requests, records, bytesWritten, failures, deadlineExits, uncertainResults atomic.Uint64
	var checkpoints, compactions, snapshotsCreated, walDeletedBytes atomic.Uint64
	var pace <-chan time.Time
	var paceTicker *time.Ticker
	if *maxRequestsPerSecond > 0 {
		interval := time.Second / time.Duration(*maxRequestsPerSecond)
		if interval <= 0 {
			interval = time.Nanosecond
		}
		paceTicker = time.NewTicker(interval)
		defer paceTicker.Stop()
		pace = paceTicker.C
	}
	payload := make([]byte, *payloadBytes)
	var wait sync.WaitGroup
	started := time.Now()
	for worker := 0; worker < *workers; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			workerStreams := (*streams - worker + *workers - 1) / *workers
			sequences := make(map[int]uint64)
			initialSequence := uint64(0)
			if *precreate {
				initialSequence = 1
			}
			counter := uint64(0)
			for ctx.Err() == nil {
				if pace != nil {
					select {
					case <-ctx.Done():
						return
					case <-pace:
					}
				}
				streamIndex := worker + int(counter%uint64(workerStreams))**workers
				inputs := make([]engine.InputRecord, *batch)
				for i := range inputs {
					inputs[i].Payload = payload
				}
				requestID := make([]byte, 16)
				binary.LittleEndian.PutUint64(requestID[:8], uint64(worker))
				binary.LittleEndian.PutUint64(requestID[8:], counter)
				expected, present := sequences[streamIndex]
				if !present {
					expected = initialSequence
				}
				_, appendErr := store.Append(ctx, engine.AppendRequest{Namespace: "benchmark", Stream: fmt.Sprintf("stream-%08d", streamIndex), ExpectedSequence: expected, RequestID: requestID, Producer: "streamd-bench", Records: inputs})
				if appendErr != nil {
					var writeErr *errdefs.WriteError
					if errors.As(appendErr, &writeErr) && writeErr.ResultUncertain {
						uncertainResults.Add(1)
					}
					if ctx.Err() != nil {
						deadlineExits.Add(1)
					} else {
						failures.Add(1)
					}
					continue
				}
				sequences[streamIndex] = expected + uint64(*batch)
				requests.Add(1)
				records.Add(uint64(*batch))
				bytesWritten.Add(uint64(*batch * *payloadBytes))
				counter++
			}
		}(worker)
	}
	checkpointDone := make(chan struct{})
	go func() {
		defer close(checkpointDone)
		if *checkpoint == 0 {
			<-ctx.Done()
			return
		}
		ticker := time.NewTicker(*checkpoint)
		defer ticker.Stop()
		lastSnapshot := time.Now()
		previousSnapshot := ""
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, _, checkpointErr := store.Checkpoint(); checkpointErr != nil {
					fmt.Fprintf(os.Stderr, "streamd-bench: periodic checkpoint: %v\n", checkpointErr)
					failures.Add(1)
					continue
				}
				checkpoints.Add(1)
				compacted, compactErr := store.Compact(engine.CompactionOptions{MinSegments: 32, MaxInputSegments: 8, MaxInputBytes: 64 << 20})
				if compactErr != nil {
					fmt.Fprintf(os.Stderr, "streamd-bench: periodic compaction: %v\n", compactErr)
					failures.Add(1)
					continue
				}
				if compacted.Created {
					compactions.Add(1)
				}
				if *retentionInterval > 0 && time.Since(lastSnapshot) >= *retentionInterval {
					destination := filepath.Join(dataPath, "snapshots", fmt.Sprintf("soak-%020d", time.Now().UnixNano()))
					retained, snapshotErr := retention.CreateSnapshotAndCollect(store, benchmarkStates, destination, *maxRetainedWALBytes, time.Now())
					if snapshotErr != nil {
						fmt.Fprintf(os.Stderr, "streamd-bench: periodic snapshot retention: %v\n", snapshotErr)
						failures.Add(1)
						continue
					}
					snapshotsCreated.Add(1)
					walDeletedBytes.Add(retained.Collection.DeletedBytes)
					lastSnapshot = time.Now()
					if previousSnapshot != "" {
						_ = os.RemoveAll(previousSnapshot)
					}
					previousSnapshot = retained.Snapshot.Path
				}
			}
		}
	}()
	wait.Wait()
	<-checkpointDone
	elapsed := time.Since(started).Seconds()
	drainStarted := time.Now()
	if err = store.CommitBarrier(context.Background()); err != nil {
		fatal(err)
	}
	drainSeconds := time.Since(drainStarted).Seconds()
	commitStats := subtractCommitStats(store.CommitStats(), commitBaseline)
	output := report{DurationSeconds: elapsed, SetupSeconds: setupSeconds, DrainSeconds: drainSeconds, Precreated: *precreate, Workers: *workers, Streams: *streams, BatchRecords: *batch, PayloadBytes: *payloadBytes, Requests: requests.Load(), Records: records.Load(), Bytes: bytesWritten.Load(), Errors: failures.Load(), DeadlineExits: deadlineExits.Load(), UncertainResults: uncertainResults.Load(), DataDirectory: dataPath, Mode: *mode, StandbyDirectory: standbyPath}
	output.Checkpoints = checkpoints.Load()
	output.Compactions = compactions.Load()
	output.Snapshots = snapshotsCreated.Load()
	output.WALDeletedBytes = walDeletedBytes.Load()
	output.RequestsPerSec = float64(output.Requests) / elapsed
	output.RecordsPerSec = float64(output.Records) / elapsed
	output.MiBPerSec = float64(output.Bytes) / elapsed / (1 << 20)
	output.GroupCommit = buildCommitReport(commitStats, groupOptions, elapsed+drainSeconds)
	if *verify {
		if _, _, err = store.Checkpoint(); err != nil {
			fatal(err)
		}
		if err = store.Close(); err != nil {
			fatal(err)
		}
		closed = true
		if *verifyReopen {
			if *mode != "single" {
				fatal(fmt.Errorf("verify-reopen currently requires single mode"))
			}
			reopenStarted := time.Now()
			reopened, reopenErr := engine.OpenWithIdentity(dataPath, identity)
			if reopenErr != nil {
				fatal(reopenErr)
			}
			output.ReopenSeconds = time.Since(reopenStarted).Seconds()
			for _, index := range sampleStreamIndexes(*streams) {
				info, inspectErr := reopened.Inspect("benchmark", fmt.Sprintf("stream-%08d", index))
				if inspectErr != nil || !info.Exists || info.NextSequence == 0 {
					_ = reopened.Close()
					fatal(fmt.Errorf("reopen verification failed for Stream %d: info=%+v error=%w", index, info, inspectErr))
				}
			}
			runtime.GC()
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			output.ReopenHeapAlloc = memory.HeapAlloc
			output.ReopenHeapSys = memory.HeapSys
			output.ReopenVerified = true
			if err = reopened.Close(); err != nil {
				fatal(err)
			}
		}
		scrubReport, scrubErr := scrub.DataRoot(dataPath)
		if scrubErr != nil {
			fatal(scrubErr)
		}
		output.Scrubbed = true
		output.ScrubSegments = scrubReport.Segments
		if standbyLog != nil {
			if err = standbyLog.Close(); err != nil {
				fatal(err)
			}
			standbyLog = nil
			history, historyErr := wal.OpenHistory(standbyPath)
			if historyErr != nil {
				fatal(historyErr)
			}
			_, next, present := history.Bounds()
			output.StandbyVerified = present && next > 0
			if !output.StandbyVerified {
				fatal(fmt.Errorf("Standby WAL contains no durable Entries"))
			}
		}
	}
	if temporary && !*keep {
		output.DataDirectory = ""
	}
	if standbyTemporary && !*keep {
		output.StandbyDirectory = ""
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(output); err != nil {
		fatal(err)
	}
}

func precreateStreams(store *engine.Store, streams, workers int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var next atomic.Uint64
	errCh := make(chan error, 1)
	var wait sync.WaitGroup
	if workers > streams {
		workers = streams
	}
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for {
				stream := next.Add(1) - 1
				if stream >= uint64(streams) || ctx.Err() != nil {
					return
				}
				requestID := make([]byte, 16)
				binary.LittleEndian.PutUint64(requestID[:8], stream)
				copy(requestID[8:], "setup")
				_, err := store.Append(ctx, engine.AppendRequest{
					Namespace: "benchmark", Stream: fmt.Sprintf("stream-%08d", stream), RequestID: requestID,
					Producer: "streamd-bench/setup", Records: []engine.InputRecord{{}},
				})
				if err != nil {
					select {
					case errCh <- fmt.Errorf("precreate Stream %d: %w", stream, err):
						cancel()
					default:
					}
					return
				}
			}
		}()
	}
	wait.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func sampleStreamIndexes(streams int) []int {
	if streams <= 1 {
		return []int{0}
	}
	return []int{0, streams / 2, streams - 1}
}

func subtractCommitStats(after, before commit.Stats) commit.Stats {
	return commit.Stats{
		Groups: after.Groups - before.Groups, Requests: after.Requests - before.Requests,
		Entries: after.Entries - before.Entries, Bytes: after.Bytes - before.Bytes,
		LocalSyncs: after.LocalSyncs - before.LocalSyncs, ReplicatedGroups: after.ReplicatedGroups - before.ReplicatedGroups,
		MaxGroupRequests: after.MaxGroupRequests, MaxGroupBytes: after.MaxGroupBytes,
		QueueWaitNanos: after.QueueWaitNanos - before.QueueWaitNanos, CollectNanos: after.CollectNanos - before.CollectNanos,
		AppendNanos: after.AppendNanos - before.AppendNanos, LocalSyncNanos: after.LocalSyncNanos - before.LocalSyncNanos,
		ReplicateNanos: after.ReplicateNanos - before.ReplicateNanos, ApplyNanos: after.ApplyNanos - before.ApplyNanos,
		ProcessNanos: after.ProcessNanos - before.ProcessNanos, QueueDepth: after.QueueDepth, QueueCapacity: after.QueueCapacity,
	}
}

func buildCommitReport(stats commit.Stats, configured engine.GroupCommitOptions, elapsedSeconds float64) commitReport {
	report := commitReport{
		ConfiguredDelayMicros: float64(configured.MaxDelay) / float64(time.Microsecond), ConfiguredMaxRequests: configured.MaxRequests,
		ConfiguredMaxBytes: configured.MaxBytes, ConfiguredQueueCapacity: configured.QueueCapacity,
		Groups: stats.Groups, Requests: stats.Requests, Entries: stats.Entries, Bytes: stats.Bytes, LocalSyncs: stats.LocalSyncs,
		ReplicatedGroups: stats.ReplicatedGroups, MaxRequestsPerGroup: stats.MaxGroupRequests, MaxBytesPerGroup: stats.MaxGroupBytes,
		AppendSeconds: float64(stats.AppendNanos) / float64(time.Second), LocalSyncSeconds: float64(stats.LocalSyncNanos) / float64(time.Second),
		ReplicateSeconds: float64(stats.ReplicateNanos) / float64(time.Second), ApplySeconds: float64(stats.ApplyNanos) / float64(time.Second),
		ProcessSeconds: float64(stats.ProcessNanos) / float64(time.Second), FinalQueueDepth: stats.QueueDepth, QueueCapacity: stats.QueueCapacity,
	}
	if stats.Groups > 0 {
		report.AverageRequestsPerGroup = float64(stats.Requests) / float64(stats.Groups)
		report.AverageEntriesPerGroup = float64(stats.Entries) / float64(stats.Groups)
		report.AverageCollectMicros = float64(stats.CollectNanos) / float64(stats.Groups) / float64(time.Microsecond)
	}
	if stats.Requests > 0 {
		report.AverageQueueWaitMicros = float64(stats.QueueWaitNanos) / float64(stats.Requests) / float64(time.Microsecond)
	}
	if stats.ProcessNanos > 0 {
		report.LocalSyncProcessRatio = float64(stats.LocalSyncNanos) / float64(stats.ProcessNanos)
		report.ReplicateProcessRatio = float64(stats.ReplicateNanos) / float64(stats.ProcessNanos)
	}
	if elapsedSeconds > 0 {
		report.LocalSyncWallRatio = report.LocalSyncSeconds / elapsedSeconds
		report.ProcessWallRatio = report.ProcessSeconds / elapsedSeconds
	}
	return report
}

func requireEmpty(path string) error {
	entries, err := os.ReadDir(path)
	if os.IsNotExist(err) {
		return os.MkdirAll(path, 0750)
	}
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("benchmark data directory is not empty: %s", filepath.Clean(path))
	}
	return nil
}

func randomIdentity() (format.NodeIdentity, error) {
	var identity format.NodeIdentity
	for _, id := range []*format.UUID{&identity.ClusterID, &identity.GroupID, &identity.NodeID} {
		if _, err := rand.Read(id[:]); err != nil {
			return identity, err
		}
	}
	identity.CreatedAt = time.Now().UnixNano()
	return identity, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "streamd-bench:", err)
	os.Exit(1)
}
