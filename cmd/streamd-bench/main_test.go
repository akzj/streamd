package main

import (
	"math"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/commit"
	"github.com/akzj/streamd/internal/storage/engine"
)

func TestBuildCommitReport(t *testing.T) {
	stats := commit.Stats{
		Groups: 2, Requests: 8, Entries: 16, LocalSyncs: 2,
		MaxGroupRequests: 5, MaxGroupBytes: 4096,
		QueueWaitNanos: uint64(8 * time.Millisecond), CollectNanos: uint64(4 * time.Millisecond),
		AppendNanos: uint64(time.Second), LocalSyncNanos: uint64(2 * time.Second),
		ReplicateNanos: uint64(time.Second), ApplyNanos: uint64(time.Second), ProcessNanos: uint64(4 * time.Second),
	}
	configured := engine.GroupCommitOptions{MaxDelay: 250 * time.Microsecond, MaxRequests: 64, MaxBytes: 4 << 20, QueueCapacity: 1024}
	report := buildCommitReport(stats, configured, 8)
	if report.AverageRequestsPerGroup != 4 || report.AverageEntriesPerGroup != 8 || report.AverageQueueWaitMicros != 1000 || report.AverageCollectMicros != 2000 {
		t.Fatalf("averages = %+v", report)
	}
	if !closeEnough(report.LocalSyncProcessRatio, 0.5) || !closeEnough(report.LocalSyncWallRatio, 0.25) || !closeEnough(report.ReplicateProcessRatio, 0.25) || !closeEnough(report.ProcessWallRatio, 0.5) {
		t.Fatalf("ratios = %+v", report)
	}
}

func TestBuildCommitReportAvoidsZeroDenominators(t *testing.T) {
	report := buildCommitReport(commit.Stats{}, engine.GroupCommitOptions{}, 0)
	if report.AverageRequestsPerGroup != 0 || report.AverageQueueWaitMicros != 0 || report.LocalSyncProcessRatio != 0 || report.LocalSyncWallRatio != 0 {
		t.Fatalf("zero report = %+v", report)
	}
}

func TestSampleStreamIndexes(t *testing.T) {
	if got := sampleStreamIndexes(1); len(got) != 1 || got[0] != 0 {
		t.Fatalf("one Stream samples = %v", got)
	}
	got := sampleStreamIndexes(10)
	if len(got) != 3 || got[0] != 0 || got[1] != 5 || got[2] != 9 {
		t.Fatalf("ten Stream samples = %v", got)
	}
}

func closeEnough(got, want float64) bool { return math.Abs(got-want) < 1e-9 }
