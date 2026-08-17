package diagnostics

import (
	"fmt"
	"time"

	"github.com/akzj/streamd/internal/replication"
	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
)

type Status string
type ReasonCode string

const (
	StatusStarting   Status = "starting"
	StatusReadyRead  Status = "ready_read"
	StatusReadyWrite Status = "ready_write"
	StatusDegraded   Status = "degraded"
	StatusFailed     Status = "failed"

	ReasonCommitCoreFailed            ReasonCode = "commit_core_failed"
	ReasonServerDraining              ReasonCode = "server_draining"
	ReasonWriteGuardUnavailable       ReasonCode = "write_guard_unavailable"
	ReasonLeaseUnsafe                 ReasonCode = "lease_unsafe"
	ReasonReplicationStateUnavailable ReasonCode = "replication_state_unavailable"
	ReasonStateInconsistent           ReasonCode = "state_inconsistent"
)

type Reason struct {
	Code    ReasonCode `json:"code"`
	Message string     `json:"message"`
}

type Watermarks struct {
	Appended     *uint64 `json:"appended"`
	LocalDurable *uint64 `json:"local_durable"`
	Replicated   *uint64 `json:"replicated"`
	Committed    *uint64 `json:"committed"`
	Applied      *uint64 `json:"applied"`
}

type Snapshot struct {
	SchemaVersion         string     `json:"schema_version"`
	Status                Status     `json:"status"`
	Ready                 bool       `json:"ready"`
	WriteReady            bool       `json:"write_ready"`
	Role                  string     `json:"role"`
	Durability            string     `json:"durability"`
	Term                  uint64     `json:"term"`
	LeaseExpiresAt        *time.Time `json:"lease_expires_at,omitempty"`
	Watermarks            Watermarks `json:"watermarks"`
	ReplicationLagEntries uint64     `json:"replication_lag_entries"`
	ApplyLagEntries       uint64     `json:"apply_lag_entries"`
	Reasons               []Reason   `json:"reasons"`
}

type Provider interface {
	Snapshot() Snapshot
}

type ProviderFunc func() Snapshot

func (f ProviderFunc) Snapshot() Snapshot { return f() }

type LeaseState struct {
	Term      uint64
	ExpiresAt time.Time
	Unsafe    bool
}

type LeaseProvider func() LeaseState

func EngineSnapshot(health engine.Health, draining bool, lease LeaseProvider) Snapshot {
	role, roleOK := roleName(health.Role)
	durability, durabilityOK := durabilityName(health.Durability)
	snapshot := Snapshot{
		SchemaVersion: "v1", Status: StatusReadyWrite, Ready: true, WriteReady: true,
		Role: role, Durability: durability, Term: health.Term, Watermarks: engineWatermarks(health), Reasons: []Reason{},
	}
	setLags(&snapshot)
	if health.Role == format.ReplicationRoleRecovering {
		snapshot.Status, snapshot.Ready, snapshot.WriteReady = StatusStarting, false, false
	} else if health.Fatal != nil {
		fail(&snapshot, ReasonCommitCoreFailed)
	} else if draining {
		snapshot.Status, snapshot.Ready, snapshot.WriteReady = StatusReadyRead, false, false
		snapshot.Reasons = []Reason{reason(ReasonServerDraining)}
	} else if health.WriteUnavailable != nil {
		snapshot.Status, snapshot.Ready, snapshot.WriteReady = StatusReadyRead, false, false
		snapshot.Reasons = []Reason{reason(ReasonWriteGuardUnavailable)}
	}
	if lease != nil {
		state := lease()
		snapshot.LeaseExpiresAt = timePtr(state.ExpiresAt)
		if state.Term != snapshot.Term && snapshot.Status != StatusFailed {
			fail(&snapshot, ReasonStateInconsistent)
		} else if state.Unsafe && snapshot.Status != StatusFailed {
			snapshot.Status, snapshot.Ready, snapshot.WriteReady = StatusReadyRead, false, false
			snapshot.Reasons = []Reason{reason(ReasonLeaseUnsafe)}
		}
	} else if health.Role == format.ReplicationRolePrimary {
		fail(&snapshot, ReasonStateInconsistent)
	}
	if !roleOK || !durabilityOK || Validate(snapshot) != nil {
		fail(&snapshot, ReasonStateInconsistent)
	}
	return snapshot
}

type standbyProvider struct{ receiver *replication.Receiver }

func NewStandbyProvider(receiver *replication.Receiver) (Provider, error) {
	if receiver == nil {
		return nil, fmt.Errorf("Standby Receiver is required")
	}
	return standbyProvider{receiver: receiver}, nil
}

func (p standbyProvider) Snapshot() Snapshot {
	state, err := p.receiver.State()
	snapshot := Snapshot{
		SchemaVersion: "v1", Status: StatusReadyRead, Ready: err == nil, WriteReady: false,
		Role: "standby", Durability: "replicated_strict", Term: state.Term, Reasons: []Reason{},
		Watermarks: Watermarks{
			Appended: position(state.LastAppended), LocalDurable: position(state.LocalDurable),
			Committed: position(state.Committed), Applied: position(state.Applied),
		},
	}
	setLags(&snapshot)
	if err != nil {
		fail(&snapshot, ReasonReplicationStateUnavailable)
	} else if Validate(snapshot) != nil {
		fail(&snapshot, ReasonStateInconsistent)
	}
	return snapshot
}

func Validate(snapshot Snapshot) error {
	if snapshot.SchemaVersion != "v1" {
		return fmt.Errorf("unknown diagnostics schema")
	}
	if snapshot.Role != "single" && snapshot.Role != "primary" && snapshot.Role != "standby" && snapshot.Role != "recovering" {
		return fmt.Errorf("unknown role")
	}
	if snapshot.Durability != "single_sync" && snapshot.Durability != "replicated_strict" {
		return fmt.Errorf("unknown durability")
	}
	switch snapshot.Status {
	case StatusStarting, StatusReadyRead, StatusReadyWrite, StatusDegraded, StatusFailed:
	default:
		return fmt.Errorf("unknown status")
	}
	if snapshot.Ready && snapshot.Status != StatusReadyRead && snapshot.Status != StatusReadyWrite {
		return fmt.Errorf("ready snapshot has inconsistent status")
	}
	if snapshot.WriteReady && (!snapshot.Ready || snapshot.Status != StatusReadyWrite) {
		return fmt.Errorf("write-ready snapshot has inconsistent status")
	}
	if snapshot.Status == StatusReadyWrite && !snapshot.WriteReady {
		return fmt.Errorf("ready-write status is not write-ready")
	}
	if snapshot.Role == "standby" && snapshot.WriteReady {
		return fmt.Errorf("Standby cannot be write-ready")
	}
	if snapshot.Role == "single" && (snapshot.Term != 0 || snapshot.Durability != "single_sync" || snapshot.LeaseExpiresAt != nil) {
		return fmt.Errorf("Single state has HA fields")
	}
	if (snapshot.Role == "primary" || snapshot.Role == "standby") && (snapshot.Term == 0 || snapshot.Durability != "replicated_strict") {
		return fmt.Errorf("HA state has invalid Term or durability")
	}
	if snapshot.Role != "primary" && snapshot.LeaseExpiresAt != nil {
		return fmt.Errorf("non-Primary state has a Lease")
	}
	ordered := []*uint64{snapshot.Watermarks.Appended, snapshot.Watermarks.LocalDurable, snapshot.Watermarks.Committed, snapshot.Watermarks.Applied}
	for index := 1; index < len(ordered); index++ {
		if ordered[index] != nil && (ordered[index-1] == nil || *ordered[index] > *ordered[index-1]) {
			return fmt.Errorf("watermark order is invalid")
		}
	}
	if snapshot.Role == "primary" {
		if snapshot.Watermarks.Replicated != nil && (snapshot.Watermarks.LocalDurable == nil || *snapshot.Watermarks.Replicated > *snapshot.Watermarks.LocalDurable) {
			return fmt.Errorf("replicated watermark exceeds local durable")
		}
		if snapshot.Watermarks.Committed != nil && (snapshot.Watermarks.Replicated == nil || *snapshot.Watermarks.Committed > *snapshot.Watermarks.Replicated) {
			return fmt.Errorf("committed watermark exceeds replicated")
		}
	} else if snapshot.Watermarks.Replicated != nil {
		return fmt.Errorf("replicated watermark is only valid on Primary")
	}
	return nil
}

func engineWatermarks(health engine.Health) Watermarks {
	watermarks := health.Watermarks
	result := Watermarks{}
	if watermarks.HasValue {
		result.Appended = uint64Ptr(watermarks.Appended)
	}
	if watermarks.HasLocalDurable {
		result.LocalDurable = uint64Ptr(watermarks.LocalDurable)
	}
	if watermarks.HasReplicated {
		result.Replicated = uint64Ptr(watermarks.Replicated)
	}
	if watermarks.HasCommitted {
		result.Committed = uint64Ptr(watermarks.Committed)
	}
	if watermarks.HasApplied {
		result.Applied = uint64Ptr(watermarks.Applied)
	}
	return result
}

func setLags(snapshot *Snapshot) {
	if snapshot.Watermarks.LocalDurable != nil && snapshot.Watermarks.Replicated != nil && *snapshot.Watermarks.LocalDurable >= *snapshot.Watermarks.Replicated {
		snapshot.ReplicationLagEntries = *snapshot.Watermarks.LocalDurable - *snapshot.Watermarks.Replicated
	}
	if snapshot.Watermarks.Committed != nil && snapshot.Watermarks.Applied != nil && *snapshot.Watermarks.Committed >= *snapshot.Watermarks.Applied {
		snapshot.ApplyLagEntries = *snapshot.Watermarks.Committed - *snapshot.Watermarks.Applied
	}
}

func roleName(value format.ReplicationRole) (string, bool) {
	switch value {
	case format.ReplicationRoleSingle:
		return "single", true
	case format.ReplicationRolePrimary:
		return "primary", true
	case format.ReplicationRoleStandby:
		return "standby", true
	case format.ReplicationRoleRecovering:
		return "recovering", true
	default:
		return "", false
	}
}

func durabilityName(value format.ReplicationDurability) (string, bool) {
	switch value {
	case format.ReplicationDurabilitySingleSync:
		return "single_sync", true
	case format.ReplicationDurabilityStrict:
		return "replicated_strict", true
	default:
		return "", false
	}
}

func position(value replication.Position) *uint64 {
	if !value.Valid {
		return nil
	}
	return uint64Ptr(value.EntryID)
}
func uint64Ptr(value uint64) *uint64 { return &value }
func timePtr(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

func fail(snapshot *Snapshot, code ReasonCode) {
	snapshot.Status, snapshot.Ready, snapshot.WriteReady = StatusFailed, false, false
	snapshot.Reasons = []Reason{reason(code)}
}

func reason(code ReasonCode) Reason {
	messages := map[ReasonCode]string{
		ReasonCommitCoreFailed:            "commit core is in a fatal state",
		ReasonServerDraining:              "server is draining",
		ReasonWriteGuardUnavailable:       "write guard is unavailable",
		ReasonLeaseUnsafe:                 "primary lease is not safe for writes",
		ReasonReplicationStateUnavailable: "replication state is unavailable",
		ReasonStateInconsistent:           "runtime state is internally inconsistent",
	}
	return Reason{Code: code, Message: messages[code]}
}
