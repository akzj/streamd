package observe

import (
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/commit"
	"github.com/prometheus/client_golang/prometheus"
)

type staticCommitStats struct{ stats commit.Stats }

func (s staticCommitStats) CommitStats() commit.Stats { return s.stats }

func TestCommitMetricsExposeCumulativeStagesAndQueuePressure(t *testing.T) {
	collector, err := NewCommitMetrics(staticCommitStats{stats: commit.Stats{
		Groups: 3, Requests: 12, Entries: 18, Bytes: 4096, LocalSyncs: 3, ReplicatedGroups: 3,
		QueueWaitNanos: uint64(12 * time.Millisecond), CollectNanos: uint64(3 * time.Millisecond),
		AppendNanos: uint64(2 * time.Millisecond), LocalSyncNanos: uint64(9 * time.Millisecond),
		ReplicateNanos: uint64(8 * time.Millisecond), ApplyNanos: uint64(time.Millisecond), ProcessNanos: uint64(21 * time.Millisecond),
		QueueDepth: 7, QueueCapacity: 1024,
	}})
	if err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	assertCounter(t, families, "streamd_commit_groups_total", nil, 3)
	assertCounter(t, families, "streamd_commit_requests_total", nil, 12)
	assertCounter(t, families, "streamd_commit_queue_wait_seconds_total", nil, 0.012)
	assertCounter(t, families, "streamd_commit_stage_seconds_total", map[string]string{"stage": "local_sync"}, 0.009)
	assertMetric(t, families, "streamd_commit_queue_depth", nil, 7)
	assertMetric(t, families, "streamd_commit_queue_capacity", nil, 1024)
}

func TestCommitMetricsRequireProvider(t *testing.T) {
	if _, err := NewCommitMetrics(nil); err == nil {
		t.Fatal("accepted nil Commit Stats Provider")
	}
}
