package observe

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/diagnostics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestNodeMetricsExposeBoundedStateAndStorage(t *testing.T) {
	root := t.TempDir()
	writeMetricFile(t, filepath.Join(root, "wal", "WAL.log"), []byte("wal"))
	writeMetricFile(t, filepath.Join(root, "segments", "SEG.seg"), []byte("segment"))
	writeMetricFile(t, filepath.Join(root, "snapshots", "snap", "segments", "SEG.seg"), []byte("snapshot"))
	writeMetricFile(t, filepath.Join(root, "CURRENT"), []byte("other"))
	now := time.Unix(1000, 0)
	leaseEnd := now.Add(12 * time.Second)
	collector, err := NewNodeMetrics(root, diagnostics.ProviderFunc(func() diagnostics.Snapshot {
		return diagnostics.Snapshot{
			SchemaVersion: "v1", Status: diagnostics.StatusReadyWrite, Ready: true, WriteReady: true,
			Role: "primary", Durability: "replicated_strict", Term: 9, LeaseExpiresAt: &leaseEnd,
			Watermarks: diagnostics.Watermarks{
				Appended: metricUint64(12), LocalDurable: metricUint64(12), Replicated: metricUint64(10),
				Committed: metricUint64(10), Applied: metricUint64(8),
			}, ReplicationLagEntries: 2, ApplyLagEntries: 2, Reasons: []diagnostics.Reason{},
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	collector.now = func() time.Time { return now }
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	assertMetric(t, families, "streamd_node_info", map[string]string{"role": "primary", "durability": "replicated_strict"}, 1)
	assertMetric(t, families, "streamd_leadership_term", nil, 9)
	assertMetric(t, families, "streamd_lease_remaining_seconds", nil, 12)
	assertMetric(t, families, "streamd_write_ready", nil, 1)
	assertMetric(t, families, "streamd_watermark_entry_id", map[string]string{"stage": "applied"}, 8)
	assertMetric(t, families, "streamd_watermark_present", map[string]string{"stage": "appended"}, 1)
	assertMetric(t, families, "streamd_replication_lag_entries", nil, 2)
	assertMetric(t, families, "streamd_apply_lag_entries", nil, 2)
	assertMetric(t, families, "streamd_storage_files", map[string]string{"kind": "wal"}, 1)
	assertMetric(t, families, "streamd_storage_bytes", map[string]string{"kind": "wal"}, 3)
	assertMetric(t, families, "streamd_storage_bytes", map[string]string{"kind": "snapshot"}, 8)
	assertMetric(t, families, "streamd_storage_files", map[string]string{"kind": "other"}, 1)
	assertMetric(t, families, "streamd_observer_collection_success", nil, 1)
}

func TestNodeMetricsPreserveEntryZeroAndRejectInvalidWatermarks(t *testing.T) {
	valid, err := NewNodeMetrics(t.TempDir(), diagnostics.ProviderFunc(func() diagnostics.Snapshot {
		return diagnostics.Snapshot{SchemaVersion: "v1", Status: diagnostics.StatusReadyWrite, Ready: true, WriteReady: true, Role: "single", Durability: "single_sync", Reasons: []diagnostics.Reason{}, Watermarks: diagnostics.Watermarks{
			Appended: metricUint64(0), LocalDurable: metricUint64(0), Committed: metricUint64(0), Applied: metricUint64(0),
		}}
	}))
	if err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(valid)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	assertMetric(t, families, "streamd_watermark_entry_id", map[string]string{"stage": "appended"}, 0)
	assertMetric(t, families, "streamd_watermark_present", map[string]string{"stage": "appended"}, 1)

	invalid, err := NewNodeMetrics(t.TempDir(), diagnostics.ProviderFunc(func() diagnostics.Snapshot {
		return diagnostics.Snapshot{SchemaVersion: "v1", Status: diagnostics.StatusReadyRead, Role: "primary", Durability: "replicated_strict", Reasons: []diagnostics.Reason{}, Watermarks: diagnostics.Watermarks{
			Appended: metricUint64(2), LocalDurable: metricUint64(2), Replicated: metricUint64(0), Committed: metricUint64(1),
		}}
	}))
	if err != nil {
		t.Fatal(err)
	}
	registry = prometheus.NewPedanticRegistry()
	registry.MustRegister(invalid)
	families, err = registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	assertMetric(t, families, "streamd_observer_collection_success", nil, 0)
	if metricFamily(families, "streamd_node_info") != nil {
		t.Fatal("invalid state was exported as healthy node information")
	}
}

func metricUint64(value uint64) *uint64 { return &value }

func writeMetricFile(t *testing.T, path string, contents []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, contents, 0640); err != nil {
		t.Fatal(err)
	}
}

func assertMetric(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string, want float64) {
	t.Helper()
	family := metricFamily(families, name)
	if family == nil {
		t.Fatalf("metric family %s is missing", name)
	}
	for _, metric := range family.Metric {
		if labelsMatch(metric.Label, labels) {
			value := metric.GetGauge().GetValue()
			if value != want {
				t.Fatalf("metric %s labels %v = %v, want %v", name, labels, value, want)
			}
			return
		}
	}
	t.Fatalf("metric %s labels %v is missing", name, labels)
}

func metricFamily(families []*dto.MetricFamily, name string) *dto.MetricFamily {
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	return nil
}

func labelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
	if len(pairs) != len(want) {
		return false
	}
	for _, pair := range pairs {
		if want[pair.GetName()] != pair.GetValue() {
			return false
		}
	}
	return true
}
