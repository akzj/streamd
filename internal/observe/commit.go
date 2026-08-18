package observe

import (
	"fmt"
	"time"

	"github.com/akzj/streamd/internal/storage/commit"
	"github.com/prometheus/client_golang/prometheus"
)

type CommitStatsProvider interface {
	CommitStats() commit.Stats
}

// CommitMetrics exports the cumulative counters owned by the Primary/Single
// Committer. The Standby Receiver has a separate WAL path and is deliberately
// not represented by this collector.
type CommitMetrics struct {
	provider   CommitStatsProvider
	groups     *prometheus.Desc
	requests   *prometheus.Desc
	entries    *prometheus.Desc
	bytes      *prometheus.Desc
	localSyncs *prometheus.Desc
	replicates *prometheus.Desc
	queueWait  *prometheus.Desc
	stage      *prometheus.Desc
	queueDepth *prometheus.Desc
	queueCap   *prometheus.Desc
}

func NewCommitMetrics(provider CommitStatsProvider) (*CommitMetrics, error) {
	if provider == nil {
		return nil, fmt.Errorf("Commit Stats Provider is required")
	}
	return &CommitMetrics{
		provider:   provider,
		groups:     prometheus.NewDesc("streamd_commit_groups_total", "WAL groups processed by the Committer.", nil, nil),
		requests:   prometheus.NewDesc("streamd_commit_requests_total", "Requests processed in WAL groups, including internal Registry requests.", nil, nil),
		entries:    prometheus.NewDesc("streamd_commit_entries_total", "WAL Entries processed in groups.", nil, nil),
		bytes:      prometheus.NewDesc("streamd_commit_bytes_total", "Encoded WAL bytes processed in groups.", nil, nil),
		localSyncs: prometheus.NewDesc("streamd_commit_local_sync_total", "Local WAL Sync calls, including failed calls.", nil, nil),
		replicates: prometheus.NewDesc("streamd_commit_replicate_total", "Strict replication calls, including failed calls.", nil, nil),
		queueWait:  prometheus.NewDesc("streamd_commit_queue_wait_seconds_total", "Cumulative request time from queue admission attempt until group processing.", nil, nil),
		stage:      prometheus.NewDesc("streamd_commit_stage_seconds_total", "Cumulative Committer stage time.", []string{"stage"}, nil),
		queueDepth: prometheus.NewDesc("streamd_commit_queue_depth", "Requests currently waiting in the bounded Committer channel.", nil, nil),
		queueCap:   prometheus.NewDesc("streamd_commit_queue_capacity", "Capacity of the bounded Committer channel.", nil, nil),
	}, nil
}

func (m *CommitMetrics) Describe(output chan<- *prometheus.Desc) {
	for _, descriptor := range []*prometheus.Desc{m.groups, m.requests, m.entries, m.bytes, m.localSyncs, m.replicates, m.queueWait, m.stage, m.queueDepth, m.queueCap} {
		output <- descriptor
	}
}

func (m *CommitMetrics) Collect(output chan<- prometheus.Metric) {
	stats := m.provider.CommitStats()
	output <- prometheus.MustNewConstMetric(m.groups, prometheus.CounterValue, float64(stats.Groups))
	output <- prometheus.MustNewConstMetric(m.requests, prometheus.CounterValue, float64(stats.Requests))
	output <- prometheus.MustNewConstMetric(m.entries, prometheus.CounterValue, float64(stats.Entries))
	output <- prometheus.MustNewConstMetric(m.bytes, prometheus.CounterValue, float64(stats.Bytes))
	output <- prometheus.MustNewConstMetric(m.localSyncs, prometheus.CounterValue, float64(stats.LocalSyncs))
	output <- prometheus.MustNewConstMetric(m.replicates, prometheus.CounterValue, float64(stats.ReplicatedGroups))
	output <- prometheus.MustNewConstMetric(m.queueWait, prometheus.CounterValue, durationSeconds(stats.QueueWaitNanos))
	for stage, nanos := range map[string]uint64{
		"collect": stats.CollectNanos, "append": stats.AppendNanos, "local_sync": stats.LocalSyncNanos,
		"replicate": stats.ReplicateNanos, "apply": stats.ApplyNanos, "process": stats.ProcessNanos,
	} {
		output <- prometheus.MustNewConstMetric(m.stage, prometheus.CounterValue, durationSeconds(nanos), stage)
	}
	output <- prometheus.MustNewConstMetric(m.queueDepth, prometheus.GaugeValue, float64(stats.QueueDepth))
	output <- prometheus.MustNewConstMetric(m.queueCap, prometheus.GaugeValue, float64(stats.QueueCapacity))
}

func durationSeconds(nanos uint64) float64 {
	return float64(nanos) / float64(time.Second)
}
