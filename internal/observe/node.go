package observe

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/akzj/streamd/internal/diagnostics"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"
)

var watermarkStages = [...]string{"appended", "local_durable", "replicated", "committed", "applied"}
var storageKinds = [...]string{"wal", "segment", "snapshot", "manifest", "staging", "trash", "other"}

type NodeMetrics struct {
	root     string
	state    diagnostics.Provider
	now      func() time.Time
	nodeInfo *prometheus.Desc
	term     *prometheus.Desc
	leaseEnd *prometheus.Desc
	leaseTTL *prometheus.Desc
	ready    *prometheus.Desc
	water    *prometheus.Desc
	present  *prometheus.Desc
	replLag  *prometheus.Desc
	applyLag *prometheus.Desc
	files    *prometheus.Desc
	bytes    *prometheus.Desc
	diskCap  *prometheus.Desc
	diskFree *prometheus.Desc
	success  *prometheus.Desc
}

func NewNodeMetrics(root string, state diagnostics.Provider) (*NodeMetrics, error) {
	if root == "" || state == nil {
		return nil, fmt.Errorf("data root and state Provider are required")
	}
	return &NodeMetrics{
		root: root, state: state, now: time.Now,
		nodeInfo: prometheus.NewDesc("streamd_node_info", "Current streamd role and durability mode.", []string{"role", "durability"}, nil),
		term:     prometheus.NewDesc("streamd_leadership_term", "Current durable HA Term.", nil, nil),
		leaseEnd: prometheus.NewDesc("streamd_lease_expires_unixtime_seconds", "Primary Lease expiration as Unix time.", nil, nil),
		leaseTTL: prometheus.NewDesc("streamd_lease_remaining_seconds", "Seconds until the Primary Lease expires.", nil, nil),
		ready:    prometheus.NewDesc("streamd_write_ready", "Whether public Appends can be accepted safely.", nil, nil),
		water:    prometheus.NewDesc("streamd_watermark_entry_id", "Highest Entry ID at a data path stage.", []string{"stage"}, nil),
		present:  prometheus.NewDesc("streamd_watermark_present", "Whether a data path stage has a watermark.", []string{"stage"}, nil),
		replLag:  prometheus.NewDesc("streamd_replication_lag_entries", "Locally durable Entries not yet replicated.", nil, nil),
		applyLag: prometheus.NewDesc("streamd_apply_lag_entries", "Committed Entries not yet applied.", nil, nil),
		files:    prometheus.NewDesc("streamd_storage_files", "Files in the local data root by bounded kind.", []string{"kind"}, nil),
		bytes:    prometheus.NewDesc("streamd_storage_bytes", "Bytes in the local data root by bounded kind.", []string{"kind"}, nil),
		diskCap:  prometheus.NewDesc("streamd_disk_capacity_bytes", "Capacity of the data root filesystem.", nil, nil),
		diskFree: prometheus.NewDesc("streamd_disk_available_bytes", "Bytes available to an unprivileged process on the data filesystem.", nil, nil),
		success:  prometheus.NewDesc("streamd_observer_collection_success", "Whether the latest node and storage collection succeeded.", nil, nil),
	}, nil
}

func (m *NodeMetrics) Describe(output chan<- *prometheus.Desc) {
	for _, descriptor := range []*prometheus.Desc{m.nodeInfo, m.term, m.leaseEnd, m.leaseTTL, m.ready, m.water, m.present, m.replLag, m.applyLag, m.files, m.bytes, m.diskCap, m.diskFree, m.success} {
		output <- descriptor
	}
}

func (m *NodeMetrics) Collect(output chan<- prometheus.Metric) {
	success := true
	state := m.state.Snapshot()
	if diagnostics.Validate(state) != nil {
		success = false
	} else {
		m.collectState(output, state)
	}
	counts, sizes, capacity, available, err := collectStorage(m.root)
	if err != nil {
		success = false
	} else {
		for index, kind := range storageKinds {
			output <- prometheus.MustNewConstMetric(m.files, prometheus.GaugeValue, float64(counts[index]), kind)
			output <- prometheus.MustNewConstMetric(m.bytes, prometheus.GaugeValue, float64(sizes[index]), kind)
		}
		output <- prometheus.MustNewConstMetric(m.diskCap, prometheus.GaugeValue, float64(capacity))
		output <- prometheus.MustNewConstMetric(m.diskFree, prometheus.GaugeValue, float64(available))
	}
	output <- prometheus.MustNewConstMetric(m.success, prometheus.GaugeValue, boolFloat(success))
}

func (m *NodeMetrics) collectState(output chan<- prometheus.Metric, state diagnostics.Snapshot) {
	output <- prometheus.MustNewConstMetric(m.nodeInfo, prometheus.GaugeValue, 1, state.Role, state.Durability)
	output <- prometheus.MustNewConstMetric(m.term, prometheus.GaugeValue, float64(state.Term))
	leaseEnd, leaseRemaining := float64(0), float64(0)
	if state.LeaseExpiresAt != nil {
		leaseEnd = float64(state.LeaseExpiresAt.UnixNano()) / float64(time.Second)
		leaseRemaining = max(0, state.LeaseExpiresAt.Sub(m.now()).Seconds())
	}
	output <- prometheus.MustNewConstMetric(m.leaseEnd, prometheus.GaugeValue, leaseEnd)
	output <- prometheus.MustNewConstMetric(m.leaseTTL, prometheus.GaugeValue, leaseRemaining)
	output <- prometheus.MustNewConstMetric(m.ready, prometheus.GaugeValue, boolFloat(state.WriteReady))
	watermarks := [...]*uint64{state.Watermarks.Appended, state.Watermarks.LocalDurable, state.Watermarks.Replicated, state.Watermarks.Committed, state.Watermarks.Applied}
	for index, stage := range watermarkStages {
		value := float64(0)
		if watermarks[index] != nil {
			value = float64(*watermarks[index])
		}
		output <- prometheus.MustNewConstMetric(m.water, prometheus.GaugeValue, value, stage)
		output <- prometheus.MustNewConstMetric(m.present, prometheus.GaugeValue, boolFloat(watermarks[index] != nil), stage)
	}
	output <- prometheus.MustNewConstMetric(m.replLag, prometheus.GaugeValue, float64(state.ReplicationLagEntries))
	output <- prometheus.MustNewConstMetric(m.applyLag, prometheus.GaugeValue, float64(state.ApplyLagEntries))
}

func collectStorage(root string) ([len(storageKinds)]uint64, [len(storageKinds)]uint64, uint64, uint64, error) {
	var counts, sizes [len(storageKinds)]uint64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if path == root || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		kind := storageKind(root, path)
		counts[kind]++
		if info.Size() > 0 {
			sizes[kind] += uint64(info.Size())
		}
		return nil
	})
	if err != nil {
		return counts, sizes, 0, 0, err
	}
	var stats unix.Statfs_t
	if err = unix.Statfs(root, &stats); err != nil {
		return counts, sizes, 0, 0, err
	}
	capacity := saturatingMultiply(stats.Blocks, uint64(stats.Bsize))
	available := saturatingMultiply(stats.Bavail, uint64(stats.Bsize))
	return counts, sizes, capacity, available, nil
}

func storageKind(root, path string) int {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return len(storageKinds) - 1
	}
	first := relative
	if index := strings.IndexRune(first, filepath.Separator); index >= 0 {
		first = first[:index]
	}
	switch first {
	case "wal":
		return 0
	case "segments":
		return 1
	case "snapshots":
		return 2
	case "manifests":
		return 3
	case "staging":
		return 4
	case "trash":
		return 5
	default:
		return 6
	}
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
func saturatingMultiply(a, b uint64) uint64 {
	if a != 0 && b > math.MaxUint64/a {
		return math.MaxUint64
	}
	return a * b
}
