package node

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/akzj/streamd/internal/access"
	"github.com/akzj/streamd/internal/service"
	"github.com/akzj/streamd/internal/storage/format"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	ListenAddress        string                      `json:"listen_address"`
	AdminAddress         string                      `json:"admin_address"`
	DataDirectory        string                      `json:"data_directory"`
	ClusterID            string                      `json:"cluster_id"`
	GroupID              string                      `json:"group_id"`
	NodeID               string                      `json:"node_id"`
	ShutdownTimeout      string                      `json:"shutdown_timeout"`
	SubscribeSendTimeout string                      `json:"subscribe_send_timeout"`
	CheckpointInterval   string                      `json:"checkpoint_interval"`
	Maintenance          MaintenanceConfig           `json:"maintenance,omitempty"`
	Compaction           CompactionConfig            `json:"compaction,omitempty"`
	TLS                  TLSConfig                   `json:"tls"`
	PrincipalsByURI      map[string]access.Principal `json:"principals_by_uri"`
	Authorization        []access.Rule               `json:"authorization"`
	Limits               service.Limits              `json:"limits"`
	OTLPTraceEndpoint    string                      `json:"otlp_trace_endpoint,omitempty"`
	Replication          ReplicationConfig           `json:"replication,omitempty"`
}

type MaintenanceConfig struct {
	CheckInterval         string `json:"check_interval,omitempty"`
	MemTableBytes         uint64 `json:"memtable_bytes,omitempty"`
	ActiveWALBytes        uint64 `json:"active_wal_bytes,omitempty"`
	DiskHighPercent       uint32 `json:"disk_high_percent,omitempty"`
	DiskCriticalPercent   uint32 `json:"disk_critical_percent,omitempty"`
	MinimumAvailableBytes uint64 `json:"minimum_available_bytes,omitempty"`
}

type maintenanceLimits struct {
	checkInterval         time.Duration
	memTableBytes         uint64
	activeWALBytes        uint64
	diskHighPercent       uint32
	diskCriticalPercent   uint32
	minimumAvailableBytes uint64
}

type CompactionConfig struct {
	MinSegments      int    `json:"min_segments,omitempty"`
	MaxInputSegments int    `json:"max_input_segments,omitempty"`
	MaxInputBytes    uint64 `json:"max_input_bytes,omitempty"`
}

type ReplicationConfig struct {
	Role              string     `json:"role,omitempty"`
	PeerAddress       string     `json:"peer_address,omitempty"`
	PeerServerName    string     `json:"peer_server_name,omitempty"`
	PeerNodeID        string     `json:"peer_node_id,omitempty"`
	LeaseTTL          string     `json:"lease_ttl,omitempty"`
	LeaseSafetyMargin string     `json:"lease_safety_margin,omitempty"`
	RenewInterval     string     `json:"renew_interval,omitempty"`
	MaxEntries        int        `json:"max_entries,omitempty"`
	MaxBytes          uint64     `json:"max_bytes,omitempty"`
	Etcd              EtcdConfig `json:"etcd,omitempty"`
}

type EtcdConfig struct {
	Endpoints       []string `json:"endpoints,omitempty"`
	Prefix          string   `json:"prefix,omitempty"`
	DialTimeout     string   `json:"dial_timeout,omitempty"`
	ServerName      string   `json:"server_name,omitempty"`
	CertificateFile string   `json:"certificate_file,omitempty"`
	PrivateKeyFile  string   `json:"private_key_file,omitempty"`
	CAFile          string   `json:"ca_file,omitempty"`
}

type TLSConfig struct {
	CertificateFile string `json:"certificate_file"`
	PrivateKeyFile  string `json:"private_key_file"`
	ClientCAFile    string `json:"client_ca_file"`
}

func LoadConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return Config{}, err
	}
	if info.Size() > 4<<20 {
		return Config{}, fmt.Errorf("configuration exceeds 4 MiB")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 4<<20))
	decoder.DisallowUnknownFields()
	var config Config
	if err = decoder.Decode(&config); err != nil {
		return Config{}, err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return Config{}, fmt.Errorf("configuration contains trailing JSON values")
	}
	if err = config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) Validate() error {
	if c.ListenAddress == "" || c.AdminAddress == "" || c.DataDirectory == "" {
		return fmt.Errorf("listen_address, admin_address, and data_directory are required")
	}
	if _, err := c.nodeIdentity(); err != nil {
		return err
	}
	if c.ListenAddress == c.AdminAddress {
		return fmt.Errorf("gRPC and admin addresses must differ")
	}
	if _, _, err := net.SplitHostPort(c.ListenAddress); err != nil {
		return fmt.Errorf("listen_address is invalid: %w", err)
	}
	adminHost, _, err := net.SplitHostPort(c.AdminAddress)
	if err != nil {
		return fmt.Errorf("admin_address is invalid: %w", err)
	}
	adminIP := net.ParseIP(adminHost)
	if adminHost != "localhost" && (adminIP == nil || !adminIP.IsLoopback()) {
		return fmt.Errorf("admin_address must bind to a loopback address")
	}
	if c.TLS.CertificateFile == "" || c.TLS.PrivateKeyFile == "" || c.TLS.ClientCAFile == "" {
		return fmt.Errorf("server certificate, private key, and client CA are required")
	}
	if _, _, _, err := c.compactionLimits(); err != nil {
		return err
	}
	if _, err := c.maintenanceLimits(); err != nil {
		return err
	}
	if len(c.PrincipalsByURI) == 0 || len(c.Authorization) == 0 {
		return fmt.Errorf("at least one client Principal and authorization rule are required")
	}
	for identity, principal := range c.PrincipalsByURI {
		parsed, err := url.Parse(identity)
		if err != nil || parsed.Scheme == "" {
			return fmt.Errorf("Principal URI %q is invalid", identity)
		}
		if err = principal.Validate(); err != nil {
			return fmt.Errorf("Principal URI %q: %w", identity, err)
		}
	}
	for i, rule := range c.Authorization {
		if rule.Tenant == "" || rule.Service == "" || rule.Namespace == "" || len(rule.Operations) == 0 {
			return fmt.Errorf("authorization rule %d is incomplete", i)
		}
		for _, operation := range rule.Operations {
			if operation != access.Append && operation != access.Read && operation != access.Subscribe && operation != access.Inspect {
				return fmt.Errorf("authorization rule %d has unknown operation %q", i, operation)
			}
		}
	}
	if _, err := c.shutdownDuration(); err != nil {
		return err
	}
	if _, err := c.subscribeSendDuration(); err != nil {
		return err
	}
	if _, err := c.checkpointDuration(); err != nil {
		return err
	}
	if err := c.Replication.validate(); err != nil {
		return err
	}
	if c.Replication.Role == "primary" {
		peerID, _ := parseUUID(c.Replication.PeerNodeID)
		identityValue, _ := c.nodeIdentity()
		if peerID == identityValue.NodeID {
			return fmt.Errorf("replication peer_node_id must differ from node_id")
		}
	}
	return nil
}

func (c Config) maintenanceLimits() (maintenanceLimits, error) {
	limits := maintenanceLimits{
		checkInterval: time.Second, memTableBytes: 64 << 20, activeWALBytes: 256 << 20,
		diskHighPercent: 85, diskCriticalPercent: 95, minimumAvailableBytes: 1 << 30,
	}
	if c.Maintenance.CheckInterval != "" {
		value, err := time.ParseDuration(c.Maintenance.CheckInterval)
		if err != nil || value <= 0 {
			return limits, fmt.Errorf("maintenance.check_interval must be a positive duration")
		}
		limits.checkInterval = value
	}
	if c.Maintenance.MemTableBytes != 0 {
		limits.memTableBytes = c.Maintenance.MemTableBytes
	}
	if c.Maintenance.ActiveWALBytes != 0 {
		limits.activeWALBytes = c.Maintenance.ActiveWALBytes
	}
	if c.Maintenance.DiskHighPercent != 0 {
		limits.diskHighPercent = c.Maintenance.DiskHighPercent
	}
	if c.Maintenance.DiskCriticalPercent != 0 {
		limits.diskCriticalPercent = c.Maintenance.DiskCriticalPercent
	}
	if c.Maintenance.MinimumAvailableBytes != 0 {
		limits.minimumAvailableBytes = c.Maintenance.MinimumAvailableBytes
	}
	if limits.memTableBytes < 1<<20 || limits.activeWALBytes < 1<<20 {
		return limits, fmt.Errorf("maintenance MemTable and active WAL thresholds must be at least 1 MiB")
	}
	if limits.diskHighPercent == 0 || limits.diskHighPercent >= limits.diskCriticalPercent || limits.diskCriticalPercent >= 100 {
		return limits, fmt.Errorf("maintenance disk watermarks must satisfy 0 < high < critical < 100")
	}
	if limits.minimumAvailableBytes < 64<<20 {
		return limits, fmt.Errorf("maintenance.minimum_available_bytes must be at least 64 MiB")
	}
	return limits, nil
}

func (c Config) compactionLimits() (int, int, uint64, error) {
	minSegments := c.Compaction.MinSegments
	if minSegments == 0 {
		minSegments = 32
	}
	maxInputSegments := c.Compaction.MaxInputSegments
	if maxInputSegments == 0 {
		maxInputSegments = 8
	}
	maxInputBytes := c.Compaction.MaxInputBytes
	if maxInputBytes == 0 {
		maxInputBytes = 64 << 20
	}
	if minSegments < 2 {
		return 0, 0, 0, fmt.Errorf("compaction.min_segments must be at least 2")
	}
	if maxInputSegments < 2 || maxInputSegments > minSegments {
		return 0, 0, 0, fmt.Errorf("compaction.max_input_segments must be between 2 and min_segments")
	}
	if maxInputBytes < 1<<20 {
		return 0, 0, 0, fmt.Errorf("compaction.max_input_bytes must be at least 1 MiB")
	}
	return minSegments, maxInputSegments, maxInputBytes, nil
}

func (c ReplicationConfig) validate() error {
	if c.Role == "" || c.Role == "single" {
		if c.PeerAddress != "" || len(c.Etcd.Endpoints) != 0 {
			return fmt.Errorf("single replication role cannot configure peer or coordinator")
		}
		return nil
	}
	if c.Role != "primary" && c.Role != "standby" {
		return fmt.Errorf("replication.role must be single, primary, or standby")
	}
	if c.Role == "primary" && (c.PeerAddress == "" || c.PeerServerName == "" || c.PeerNodeID == "") {
		return fmt.Errorf("primary replication requires peer_address, peer_server_name, and peer_node_id")
	}
	if len(c.Etcd.Endpoints) == 0 {
		return fmt.Errorf("replicated role requires etcd endpoints")
	}
	if c.MaxEntries < 0 {
		return fmt.Errorf("replication.max_entries cannot be negative")
	}
	maxInt := uint64(^uint(0) >> 1)
	if c.MaxBytes > maxInt-(1<<20) {
		return fmt.Errorf("replication.max_bytes exceeds platform gRPC limit")
	}
	if c.Role == "primary" {
		if _, err := parseUUID(c.PeerNodeID); err != nil {
			return fmt.Errorf("replication.peer_node_id: %w", err)
		}
	}
	for _, value := range []string{c.Etcd.CertificateFile, c.Etcd.PrivateKeyFile, c.Etcd.CAFile, c.Etcd.ServerName} {
		if value == "" {
			return fmt.Errorf("replication etcd mTLS files and server_name are required")
		}
	}
	ttl, safety, renew, err := c.durations()
	if err != nil {
		return err
	}
	if safety*2 >= ttl || renew >= ttl-safety {
		return fmt.Errorf("replication Lease timing has no safe renewal window")
	}
	return nil
}

func (c ReplicationConfig) durations() (time.Duration, time.Duration, time.Duration, error) {
	parse := func(value string, fallback time.Duration, name string) (time.Duration, error) {
		if value == "" {
			return fallback, nil
		}
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return 0, fmt.Errorf("%s must be a positive duration", name)
		}
		return duration, nil
	}
	ttl, err := parse(c.LeaseTTL, 15*time.Second, "replication.lease_ttl")
	if err != nil {
		return 0, 0, 0, err
	}
	safety, err := parse(c.LeaseSafetyMargin, 3*time.Second, "replication.lease_safety_margin")
	if err != nil {
		return 0, 0, 0, err
	}
	renew, err := parse(c.RenewInterval, 3*time.Second, "replication.renew_interval")
	return ttl, safety, renew, err
}

func (c Config) nodeIdentity() (format.NodeIdentity, error) {
	cluster, err := parseUUID(c.ClusterID)
	if err != nil {
		return format.NodeIdentity{}, fmt.Errorf("cluster_id: %w", err)
	}
	group, err := parseUUID(c.GroupID)
	if err != nil {
		return format.NodeIdentity{}, fmt.Errorf("group_id: %w", err)
	}
	node, err := parseUUID(c.NodeID)
	if err != nil {
		return format.NodeIdentity{}, fmt.Errorf("node_id: %w", err)
	}
	identity := format.NodeIdentity{ClusterID: cluster, GroupID: group, NodeID: node, CreatedAt: time.Now().UnixNano()}
	if _, err = format.MarshalNodeIdentity(identity); err != nil {
		return format.NodeIdentity{}, err
	}
	return identity, nil
}

func parseUUID(value string) (format.UUID, error) {
	var id format.UUID
	compact := strings.ReplaceAll(value, "-", "")
	if len(compact) != 32 {
		return id, fmt.Errorf("must contain 32 hexadecimal digits")
	}
	decoded, err := hex.DecodeString(compact)
	if err != nil {
		return id, fmt.Errorf("must be hexadecimal")
	}
	copy(id[:], decoded)
	return id, nil
}

func (c Config) checkpointDuration() (time.Duration, error) {
	if c.CheckpointInterval == "" {
		return time.Minute, nil
	}
	value, err := time.ParseDuration(c.CheckpointInterval)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("checkpoint_interval must be a positive duration")
	}
	return value, nil
}

func (c Config) shutdownDuration() (time.Duration, error) {
	if c.ShutdownTimeout == "" {
		return 30 * time.Second, nil
	}
	value, err := time.ParseDuration(c.ShutdownTimeout)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("shutdown_timeout must be a positive duration")
	}
	return value, nil
}

func (c Config) subscribeSendDuration() (time.Duration, error) {
	if c.SubscribeSendTimeout == "" {
		return 30 * time.Second, nil
	}
	value, err := time.ParseDuration(c.SubscribeSendTimeout)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("subscribe_send_timeout must be a positive duration")
	}
	return value, nil
}

func (c Config) serverCredentials() (credentials.TransportCredentials, error) {
	keyInfo, err := os.Stat(c.TLS.PrivateKeyFile)
	if err != nil {
		return nil, err
	}
	if !keyInfo.Mode().IsRegular() || keyInfo.Mode().Perm()&0007 != 0 {
		return nil, fmt.Errorf("private key must be a regular file inaccessible to other users")
	}
	certificate, err := tls.LoadX509KeyPair(c.TLS.CertificateFile, c.TLS.PrivateKeyFile)
	if err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(c.TLS.ClientCAFile)
	if err != nil {
		return nil, err
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("client CA file contains no certificates")
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		ClientCAs:    clientCAs,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}), nil
}
