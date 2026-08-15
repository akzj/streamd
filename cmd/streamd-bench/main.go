package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/scrub"
	"github.com/akzj/streamd/internal/storage/wal"
)

type report struct {
	DurationSeconds  float64 `json:"duration_seconds"`
	Workers          int     `json:"workers"`
	Streams          int     `json:"streams"`
	BatchRecords     int     `json:"batch_records"`
	PayloadBytes     int     `json:"payload_bytes"`
	Requests         uint64  `json:"requests"`
	Records          uint64  `json:"records"`
	Bytes            uint64  `json:"bytes"`
	Errors           uint64  `json:"errors"`
	RequestsPerSec   float64 `json:"requests_per_second"`
	RecordsPerSec    float64 `json:"records_per_second"`
	MiBPerSec        float64 `json:"mib_per_second"`
	DataDirectory    string  `json:"data_directory,omitempty"`
	Scrubbed         bool    `json:"scrubbed"`
	ScrubSegments    uint64  `json:"scrub_segments,omitempty"`
	Mode             string  `json:"mode"`
	StandbyDirectory string  `json:"standby_directory,omitempty"`
	StandbyVerified  bool    `json:"standby_verified"`
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
	flag.Parse()
	if *duration <= 0 || *workers <= 0 || *streams < *workers || *batch <= 0 || *payloadBytes < 0 || *checkpoint < 0 || (*mode != "single" && *mode != "strict") {
		fatal(fmt.Errorf("invalid benchmark arguments"))
	}
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
		store, err = engine.OpenReplicated(dataPath, identity, engine.ReplicationOptions{Term: 1, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: durableReplica{log: standbyLog}, Guard: benchmarkGuard{}})
	} else {
		store, err = engine.OpenWithIdentity(dataPath, identity)
	}
	if err != nil {
		fatal(err)
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
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	var requests, records, bytesWritten, failures atomic.Uint64
	payload := make([]byte, *payloadBytes)
	var wait sync.WaitGroup
	started := time.Now()
	for worker := 0; worker < *workers; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			workerStreams := (*streams - worker + *workers - 1) / *workers
			sequences := make(map[int]uint64)
			counter := uint64(0)
			for ctx.Err() == nil {
				streamIndex := worker + int(counter%uint64(workerStreams))**workers
				inputs := make([]engine.InputRecord, *batch)
				for i := range inputs {
					inputs[i].Payload = payload
				}
				requestID := make([]byte, 16)
				binary.LittleEndian.PutUint64(requestID[:8], uint64(worker))
				binary.LittleEndian.PutUint64(requestID[8:], counter)
				_, appendErr := store.Append(ctx, engine.AppendRequest{Namespace: "benchmark", Stream: fmt.Sprintf("stream-%08d", streamIndex), ExpectedSequence: sequences[streamIndex], RequestID: requestID, Producer: "streamd-bench", Records: inputs})
				if appendErr != nil {
					if ctx.Err() == nil {
						failures.Add(1)
					}
					continue
				}
				sequences[streamIndex] += uint64(*batch)
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
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, _, checkpointErr := store.Checkpoint(); checkpointErr != nil {
					failures.Add(1)
				}
			}
		}
	}()
	wait.Wait()
	<-checkpointDone
	elapsed := time.Since(started).Seconds()
	output := report{DurationSeconds: elapsed, Workers: *workers, Streams: *streams, BatchRecords: *batch, PayloadBytes: *payloadBytes, Requests: requests.Load(), Records: records.Load(), Bytes: bytesWritten.Load(), Errors: failures.Load(), DataDirectory: dataPath, Mode: *mode, StandbyDirectory: standbyPath}
	output.RequestsPerSec = float64(output.Requests) / elapsed
	output.RecordsPerSec = float64(output.Records) / elapsed
	output.MiBPerSec = float64(output.Bytes) / elapsed / (1 << 20)
	if *verify {
		if _, _, err = store.Checkpoint(); err != nil {
			fatal(err)
		}
		if err = store.Close(); err != nil {
			fatal(err)
		}
		closed = true
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
