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

	"github.com/akzj/streamd/internal/leadership"
	"github.com/akzj/streamd/internal/replication"
	"github.com/akzj/streamd/internal/storage/commit"
	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"
)

var watermarkStages = [...]string{"appended", "local_durable", "replicated", "committed", "applied"}
var storageKinds = [...]string{"wal", "segment", "snapshot", "manifest", "staging", "trash", "other"}

type Watermark struct {
	Present bool
	EntryID uint64
}

type NodeState struct {
	Role           string
	Durability     string
	Term           uint64
	LeaseExpiresAt time.Time
	WriteReady     bool
	Watermarks     [len(watermarkStages)]Watermark
}

type StateProvider func() (NodeState, error)

type NodeMetrics struct {
	root     string
	state    StateProvider
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

func NewNodeMetrics(root string, state StateProvider) (*NodeMetrics, error) {
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
	state, err := m.state()
	if err != nil || validateNodeState(state) != nil {
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

func (m *NodeMetrics) collectState(output chan<- prometheus.Metric, state NodeState) {
	output <- prometheus.MustNewConstMetric(m.nodeInfo, prometheus.GaugeValue, 1, state.Role, state.Durability)
	output <- prometheus.MustNewConstMetric(m.term, prometheus.GaugeValue, float64(state.Term))
	leaseEnd, leaseRemaining := float64(0), float64(0)
	if !state.LeaseExpiresAt.IsZero() {
		leaseEnd = float64(state.LeaseExpiresAt.UnixNano()) / float64(time.Second)
		leaseRemaining = max(0, state.LeaseExpiresAt.Sub(m.now()).Seconds())
	}
	output <- prometheus.MustNewConstMetric(m.leaseEnd, prometheus.GaugeValue, leaseEnd)
	output <- prometheus.MustNewConstMetric(m.leaseTTL, prometheus.GaugeValue, leaseRemaining)
	output <- prometheus.MustNewConstMetric(m.ready, prometheus.GaugeValue, boolFloat(state.WriteReady))
	for index, stage := range watermarkStages {
		watermark := state.Watermarks[index]
		output <- prometheus.MustNewConstMetric(m.water, prometheus.GaugeValue, float64(watermark.EntryID), stage)
		output <- prometheus.MustNewConstMetric(m.present, prometheus.GaugeValue, boolFloat(watermark.Present), stage)
	}
	output <- prometheus.MustNewConstMetric(m.replLag, prometheus.GaugeValue, lag(state.Watermarks[1], state.Watermarks[2]))
	output <- prometheus.MustNewConstMetric(m.applyLag, prometheus.GaugeValue, lag(state.Watermarks[3], state.Watermarks[4]))
}

func EngineStateProvider(store *engine.Store, controller *leadership.Controller, ready func() bool) StateProvider {
	return func() (NodeState, error) {
		if store == nil || ready == nil {
			return NodeState{}, fmt.Errorf("Engine and readiness Provider are required")
		}
		health := store.Health()
		role, err := roleName(health.Role)
		if err != nil {
			return NodeState{}, err
		}
		durability, err := durabilityName(health.Durability)
		if err != nil {
			return NodeState{}, err
		}
		state := NodeState{Role: role, Durability: durability, Term: health.Term, WriteReady: ready(), Watermarks: commitWatermarks(health.Watermarks)}
		if controller != nil {
			leadershipState := controller.Snapshot()
			if leadershipState.Term != state.Term {
				return NodeState{}, fmt.Errorf("Engine Term %d differs from leadership Term %d", state.Term, leadershipState.Term)
			}
			state.LeaseExpiresAt = leadershipState.ExpiresAt
		}
		return state, nil
	}
}

func StandbyStateProvider(receiver *replication.Receiver) StateProvider {
	return func() (NodeState, error) {
		if receiver == nil {
			return NodeState{}, fmt.Errorf("Standby Receiver is required")
		}
		state, err := receiver.State()
		if err != nil {
			return NodeState{}, err
		}
		return NodeState{Role: "standby", Durability: "replicated_strict", Term: state.Term, Watermarks: [len(watermarkStages)]Watermark{
			positionWatermark(state.LastAppended), positionWatermark(state.LocalDurable), {}, positionWatermark(state.Committed), positionWatermark(state.Applied),
		}}, nil
	}
}

func validateNodeState(state NodeState) error {
	if state.Role != "single" && state.Role != "primary" && state.Role != "standby" && state.Role != "recovering" {
		return fmt.Errorf("unknown role %q", state.Role)
	}
	if state.Durability != "single_sync" && state.Durability != "replicated_strict" {
		return fmt.Errorf("unknown durability %q", state.Durability)
	}
	ordered := []int{0, 1, 3, 4}
	for index := 1; index < len(ordered); index++ {
		previous, current := state.Watermarks[ordered[index-1]], state.Watermarks[ordered[index]]
		if current.Present && (!previous.Present || current.EntryID > previous.EntryID) {
			return fmt.Errorf("watermark %s is ahead of %s", watermarkStages[ordered[index]], watermarkStages[ordered[index-1]])
		}
	}
	if state.Role == "primary" {
		local, replicated, committed := state.Watermarks[1], state.Watermarks[2], state.Watermarks[3]
		if replicated.Present && (!local.Present || replicated.EntryID > local.EntryID) {
			return fmt.Errorf("replicated watermark is ahead of local durable watermark")
		}
		if committed.Present && (!replicated.Present || committed.EntryID > replicated.EntryID) {
			return fmt.Errorf("committed watermark is ahead of replicated watermark")
		}
	} else if state.Watermarks[2].Present {
		return fmt.Errorf("replicated watermark is only valid on a Primary")
	}
	return nil
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

func commitWatermarks(value commit.Watermarks) [len(watermarkStages)]Watermark {
	return [len(watermarkStages)]Watermark{
		{Present: value.HasValue, EntryID: value.Appended},
		{Present: value.HasLocalDurable, EntryID: value.LocalDurable},
		{Present: value.HasReplicated, EntryID: value.Replicated},
		{Present: value.HasCommitted, EntryID: value.Committed},
		{Present: value.HasApplied, EntryID: value.Applied},
	}
}

func positionWatermark(value replication.Position) Watermark {
	return Watermark{Present: value.Valid, EntryID: value.EntryID}
}

func roleName(value format.ReplicationRole) (string, error) {
	switch value {
	case format.ReplicationRoleSingle:
		return "single", nil
	case format.ReplicationRolePrimary:
		return "primary", nil
	case format.ReplicationRoleStandby:
		return "standby", nil
	case format.ReplicationRoleRecovering:
		return "recovering", nil
	default:
		return "", fmt.Errorf("unknown replication role %d", value)
	}
}

func durabilityName(value format.ReplicationDurability) (string, error) {
	switch value {
	case format.ReplicationDurabilitySingleSync:
		return "single_sync", nil
	case format.ReplicationDurabilityStrict:
		return "replicated_strict", nil
	default:
		return "", fmt.Errorf("unknown replication durability %d", value)
	}
}

func lag(a, b Watermark) float64 {
	if !a.Present || !b.Present || b.EntryID >= a.EntryID {
		return 0
	}
	return float64(a.EntryID - b.EntryID)
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
