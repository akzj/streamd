package observe

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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
	collector, err := NewNodeMetrics(root, func() (NodeState, error) {
		return NodeState{
			Role: "primary", Durability: "replicated_strict", Term: 9,
			LeaseExpiresAt: now.Add(12 * time.Second), WriteReady: true,
			Watermarks: [len(watermarkStages)]Watermark{
				{Present: true, EntryID: 12}, {Present: true, EntryID: 12},
				{Present: true, EntryID: 10}, {Present: true, EntryID: 10},
				{Present: true, EntryID: 8},
			},
		}, nil
	})
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
	valid, err := NewNodeMetrics(t.TempDir(), func() (NodeState, error) {
		return NodeState{Role: "single", Durability: "single_sync", WriteReady: true, Watermarks: [len(watermarkStages)]Watermark{
			{Present: true}, {Present: true}, {}, {Present: true}, {Present: true},
		}}, nil
	})
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

	invalid, err := NewNodeMetrics(t.TempDir(), func() (NodeState, error) {
		return NodeState{Role: "primary", Durability: "replicated_strict", Watermarks: [len(watermarkStages)]Watermark{
			{Present: true, EntryID: 2}, {Present: true, EntryID: 2},
			{Present: true, EntryID: 0}, {Present: true, EntryID: 1},
		}}, nil
	})
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
