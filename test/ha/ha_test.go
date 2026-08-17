//go:build integration

package ha_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	streamdv1 "github.com/akzj/streamd/api/streamd/v1"
	"github.com/akzj/streamd/internal/diagnostics"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	primaryAddress = "streamd-primary:7443"
	primaryMetrics = "http://streamd-primary:19090/metrics"
	standbyMetrics = "http://streamd-standby:19090/metrics"
	proxyAPI       = "http://toxiproxy:8474"
)

func TestComposeStrictHA(t *testing.T) {
	switch os.Getenv("HA_SCENARIO") {
	case "needs-snapshot":
		testNeedsSnapshotDiagnostics(t)
		return
	case "recovery-lease-loss":
		testRecoveryLeaseLoss(t)
		return
	case "log-diverged":
		testLogDivergedDiagnostics(t)
		return
	}
	client, closeClient := streamClient(t)
	defer closeClient()
	switch os.Getenv("HA_SCENARIO") {
	case "single-member-loss":
		testSingleMemberLoss(t, client)
		return
	case "quorum-loss":
		testQuorumLoss(t, client)
		return
	case "quorum-recovered":
		testQuorumRecovered(t, client)
		return
	case "standby-partition":
		testStandbyPartition(t, client)
		return
	case "before-failover":
		testBeforeFailover(t, client)
		return
	case "after-failover":
		testAfterFailover(t, client)
		return
	case "after-failback":
		testAfterFailback(t, client)
		return
	case "before-snapshot":
		testBeforeSnapshot(t, client)
		return
	case "after-snapshot":
		testAfterSnapshot(t, client)
		return
	case "after-divergence-recovery":
		testAfterDivergenceRecovery(t, client)
		return
	case "":
	default:
		t.Fatalf("unknown HA_SCENARIO %q", os.Getenv("HA_SCENARIO"))
	}

	t.Run("replicated append is readable and idempotent", func(t *testing.T) {
		waitReady(t, client)
		request := &streamdv1.AppendRequest{Stream: &streamdv1.StreamRef{Namespace: "ha", Stream: "events"}, ExpectedSequence: 0, RequestId: []byte("ha-request-0001"), Record: &streamdv1.InputRecord{Payload: []byte("replicated")}, RequiredDurability: streamdv1.Durability_DURABILITY_REPLICATED_STRICT}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		response, err := client.Append(ctx, request)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if response.Sequence != 0 || response.NextSequence != 1 || response.Durability != streamdv1.Durability_DURABILITY_REPLICATED_STRICT || response.Deduplicated {
			t.Fatalf("Append response = %+v", response)
		}
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		repeated, err := client.Append(ctx, request)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if !repeated.Deduplicated || repeated.NextSequence != 1 || repeated.StorageEntryId != response.StorageEntryId {
			t.Fatalf("idempotent Append response = %+v", repeated)
		}
		assertRecords(t, client, 1, "replicated")
	})

	t.Run("metrics expose strict replication state", func(t *testing.T) {
		deadline := time.Now().Add(10 * time.Second)
		for {
			primary, primaryErr := fetchMetrics(primaryMetrics)
			standby, standbyErr := fetchMetrics(standbyMetrics)
			if primaryErr == nil && standbyErr == nil && strictMetricsReady(primary, standby) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("Strict HA metrics did not converge: primary_error=%v standby_error=%v", primaryErr, standbyErr)
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	t.Run("diagnostics share strict replication state", func(t *testing.T) {
		primary, err := fetchDiagnostics("http://streamd-primary:19090/diagnostics")
		if err != nil {
			t.Fatal(err)
		}
		standby, err := fetchDiagnostics("http://streamd-standby:19090/diagnostics")
		if err != nil {
			t.Fatal(err)
		}
		if !primary.Ready || !primary.WriteReady || primary.Status != diagnostics.StatusReadyWrite || primary.Role != "primary" || primary.Term == 0 || primary.LeaseExpiresAt == nil || len(primary.Reasons) != 0 {
			t.Fatalf("Primary diagnostics = %+v", primary)
		}
		if !standby.Ready || standby.WriteReady || standby.Status != diagnostics.StatusReadyRead || standby.Role != "standby" || standby.Term != primary.Term || len(standby.Reasons) != 0 {
			t.Fatalf("Standby diagnostics = %+v", standby)
		}
		if primary.Watermarks.Committed == nil || standby.Watermarks.Committed == nil || *standby.Watermarks.Committed != *primary.Watermarks.Committed || standby.Watermarks.Replicated != nil {
			t.Fatalf("diagnostic watermarks differ: primary=%+v standby=%+v", primary.Watermarks, standby.Watermarks)
		}
	})

}

func testLogDivergedDiagnostics(t *testing.T) {
	expectedEntryID := envUint64(t, "HA_DIVERGED_ENTRY_ID")
	expectedCRC := uint32(envUint64(t, "HA_DIVERGED_CRC32C"))
	connection, err := net.DialTimeout("tcp", primaryAddress, time.Second)
	if err == nil {
		_ = connection.Close()
		t.Fatal("log-diverged Primary public gRPC listener is open")
	}
	deadline := time.Now().Add(30 * time.Second)
	var snapshot diagnostics.Snapshot
	for {
		snapshot, err = fetchDiagnostics("http://streamd-primary:19090/diagnostics")
		if err == nil && snapshot.Recovery != nil && snapshot.Recovery.Reason == diagnostics.RecoveryLogDiverged {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("log-diverged diagnostics did not become available: snapshot=%+v error=%v", snapshot, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	task := snapshot.Recovery
	if snapshot.Ready || snapshot.WriteReady || snapshot.Status != diagnostics.StatusDegraded || snapshot.Role != "primary" || !hasReason(snapshot, diagnostics.ReasonSnapshotRequired) || task.Action != diagnostics.RecoveryInstallSnapshot || task.SourceNodeID != "33333333333333333333333333333333" || task.TargetNodeID != "44444444444444444444444444444444" || task.TargetDurableEntryID == nil || *task.TargetDurableEntryID != expectedEntryID || task.TargetDurableCRC32C == nil || *task.TargetDurableCRC32C != expectedCRC || len(task.TaskID) != 64 {
		t.Fatalf("log-diverged recovery diagnostics = %+v", snapshot)
	}
	repeated, repeatErr := fetchDiagnostics("http://streamd-primary:19090/diagnostics")
	if repeatErr != nil || repeated.Recovery == nil || repeated.Recovery.TaskID != task.TaskID {
		t.Fatalf("repeated log-diverged diagnostics = %+v, error = %v", repeated, repeatErr)
	}
	t.Logf("RECOVERY_TERM=%d RECOVERY_TASK_ID=%s", task.Term, task.TaskID)
}

func testAfterDivergenceRecovery(t *testing.T, client streamdv1.StreamServiceClient) {
	waitReady(t, client)
	assertStreamRecords(t, client, "snapshot-events", "before-snapshot", "after-snapshot")
	appendRecord(t, client, "divergence-events", 0, "ha-divergence-recovered", "after-divergence-recovery")
	assertStreamRecords(t, client, "divergence-events", "after-divergence-recovery")
}

func testRecoveryLeaseLoss(t *testing.T) {
	expectedTaskID := env("HA_RECOVERY_TASK_ID", "")
	if expectedTaskID == "" {
		t.Fatal("HA_RECOVERY_TASK_ID is required")
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		snapshot, err := fetchDiagnostics("http://streamd-primary:19090/diagnostics")
		if err == nil && snapshot.Status == diagnostics.StatusFailed && snapshot.Role == "recovering" && !snapshot.Ready && !snapshot.WriteReady && snapshot.Recovery != nil && snapshot.Recovery.TaskID == expectedTaskID && hasReason(snapshot, diagnostics.ReasonLeaseUnsafe) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovery diagnostics did not reflect Lease loss: snapshot=%+v error=%v", snapshot, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func hasReason(snapshot diagnostics.Snapshot, code diagnostics.ReasonCode) bool {
	for _, reason := range snapshot.Reasons {
		if reason.Code == code {
			return true
		}
	}
	return false
}

func envUint64(t *testing.T, name string) uint64 {
	t.Helper()
	value := env(name, "")
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		t.Fatalf("%s=%q is not a uint64: %v", name, value, err)
	}
	return parsed
}

func testNeedsSnapshotDiagnostics(t *testing.T) {
	connection, err := net.DialTimeout("tcp", primaryAddress, time.Second)
	if err == nil {
		_ = connection.Close()
		t.Fatal("recovery-blocked Primary public gRPC listener is open")
	}
	deadline := time.Now().Add(30 * time.Second)
	var snapshot diagnostics.Snapshot
	err = nil
	for {
		snapshot, err = fetchDiagnostics("http://streamd-primary:19090/diagnostics")
		if err == nil && snapshot.Recovery != nil && snapshot.Reasons[0].Code == diagnostics.ReasonSnapshotRequired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovery diagnostics did not become available: snapshot=%+v error=%v", snapshot, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if snapshot.Ready || snapshot.WriteReady || snapshot.Status != diagnostics.StatusDegraded || snapshot.Role != "primary" || snapshot.Recovery.Action != diagnostics.RecoveryInstallSnapshot || snapshot.Recovery.Reason != diagnostics.RecoverySnapshotOffered || snapshot.Recovery.SourceNodeID != "33333333333333333333333333333333" || snapshot.Recovery.TargetNodeID != "44444444444444444444444444444444" || snapshot.Recovery.SnapshotID == "" || snapshot.Recovery.SnapshotCheckpoint == nil || len(snapshot.Recovery.TaskID) != 64 {
		t.Fatalf("recovery diagnostics = %+v", snapshot)
	}
	readyResponse, err := (&http.Client{Timeout: 3 * time.Second}).Get("http://streamd-primary:19090/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = readyResponse.Body.Close()
	if readyResponse.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("recovery readiness status = %s", readyResponse.Status)
	}
	t.Logf("RECOVERY_TERM=%d RECOVERY_TASK_ID=%s", snapshot.Recovery.Term, snapshot.Recovery.TaskID)
	repeated, err := fetchDiagnostics("http://streamd-primary:19090/diagnostics")
	if err != nil || repeated.Recovery == nil || repeated.Recovery.TaskID != snapshot.Recovery.TaskID {
		t.Fatalf("repeated recovery diagnostics = %+v, error = %v", repeated, err)
	}
}

func testBeforeSnapshot(t *testing.T, client streamdv1.StreamServiceClient) {
	waitReady(t, client)
	appendRecord(t, client, "snapshot-events", 0, "ha-snapshot-0001", "before-snapshot")
}

func testAfterSnapshot(t *testing.T, client streamdv1.StreamServiceClient) {
	waitReady(t, client)
	assertStreamRecords(t, client, "snapshot-events", "before-snapshot")
	appendRecord(t, client, "snapshot-events", 1, "ha-snapshot-0002", "after-snapshot")
	assertStreamRecords(t, client, "snapshot-events", "before-snapshot", "after-snapshot")
}

func testBeforeFailover(t *testing.T, client streamdv1.StreamServiceClient) {
	waitReady(t, client)
	appendRecord(t, client, "failover-events", 0, "ha-failover-0001", "before-failover")
}

func testAfterFailover(t *testing.T, client streamdv1.StreamServiceClient) {
	waitReady(t, client)
	assertStreamRecords(t, client, "failover-events", "before-failover")
	appendRecord(t, client, "failover-events", 1, "ha-failover-0002", "after-failover")
	assertStreamRecords(t, client, "failover-events", "before-failover", "after-failover")
}

func testAfterFailback(t *testing.T, client streamdv1.StreamServiceClient) {
	waitReady(t, client)
	assertStreamRecords(t, client, "failover-events", "before-failover", "after-failover")
	appendRecord(t, client, "failover-events", 2, "ha-failover-0003", "after-failback")
	assertStreamRecords(t, client, "failover-events", "before-failover", "after-failover", "after-failback")
}

func testSingleMemberLoss(t *testing.T, client streamdv1.StreamServiceClient) {
	waitReady(t, client)
	appendRecord(t, client, "quorum-single-member", 0, "ha-quorum-single", "one-member-down")
	assertStreamRecords(t, client, "quorum-single-member", "one-member-down")
}

func testQuorumLoss(t *testing.T, client streamdv1.StreamServiceClient) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		health, err := client.Health(ctx, &streamdv1.HealthRequest{})
		cancel()
		if err == nil && health.Status != streamdv1.HealthStatus_HEALTH_STATUS_READY_WRITE {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Primary remained READY_WRITE without etcd quorum: health=%+v error=%v", health, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := client.Append(ctx, appendRequest("quorum-fenced", 0, "ha-quorum-fenced", "must-not-commit"))
	cancel()
	if err == nil {
		t.Fatal("Strict Append succeeded after coordinator quorum loss")
	}
}

func testQuorumRecovered(t *testing.T, client streamdv1.StreamServiceClient) {
	waitReady(t, client)
	appendRecord(t, client, "quorum-recovered", 0, "ha-quorum-recovered", "recovered")
	assertStreamRecords(t, client, "quorum-recovered", "recovered")
}

func testStandbyPartition(t *testing.T, client streamdv1.StreamServiceClient) {
	waitReady(t, client)
	appendRecord(t, client, "partition-events", 0, "ha-partition-0001", "replicated-before-partition")
	setStandbyProxy(t, false)
	t.Cleanup(func() { setStandbyProxyBestEffort(true) })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := client.Append(ctx, appendRequest("partition-events", 1, "ha-partition-0002", "must-not-ack"))
	cancel()
	if err == nil {
		t.Fatal("Strict Append succeeded while the Standby link was partitioned")
	}
	assertStreamRecords(t, client, "partition-events", "replicated-before-partition")
}

func appendRecord(t *testing.T, client streamdv1.StreamServiceClient, stream string, expected uint64, requestID, payload string) *streamdv1.AppendResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	response, err := client.Append(ctx, appendRequest(stream, expected, requestID, payload))
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func appendRequest(stream string, expected uint64, requestID, payload string) *streamdv1.AppendRequest {
	return &streamdv1.AppendRequest{Stream: &streamdv1.StreamRef{Namespace: "ha", Stream: stream}, ExpectedSequence: expected, RequestId: []byte(requestID), Record: &streamdv1.InputRecord{Payload: []byte(payload)}, RequiredDurability: streamdv1.Durability_DURABILITY_REPLICATED_STRICT}
}

func streamClient(t *testing.T) (streamdv1.StreamServiceClient, func()) {
	t.Helper()
	certificateDirectory := env("HA_CERT_DIR", "/ha/certs")
	certificate, err := tls.LoadX509KeyPair(filepath.Join(certificateDirectory, "client.crt"), filepath.Join(certificateDirectory, "client.key"))
	if err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(filepath.Join(certificateDirectory, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("test CA contains no certificates")
	}
	connection, err := grpc.NewClient(env("HA_PRIMARY_ADDRESS", primaryAddress), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: roots, ServerName: env("HA_PRIMARY_SERVER_NAME", "streamd-primary")})))
	if err != nil {
		t.Fatal(err)
	}
	return streamdv1.NewStreamServiceClient(connection), func() { _ = connection.Close() }
}

func waitReady(t *testing.T, client streamdv1.StreamServiceClient) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		health, err := client.Health(ctx, &streamdv1.HealthRequest{})
		cancel()
		if err == nil && health.Status == streamdv1.HealthStatus_HEALTH_STATUS_READY_WRITE && health.Role == streamdv1.NodeRole_NODE_ROLE_PRIMARY && health.Durability == streamdv1.Durability_DURABILITY_REPLICATED_STRICT {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Primary did not become READY_WRITE: health=%+v error=%v", health, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func assertRecords(t *testing.T, client streamdv1.StreamServiceClient, count int, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	response, err := client.Read(ctx, &streamdv1.ReadRequest{Stream: &streamdv1.StreamRef{Namespace: "ha", Stream: "events"}, FromSequence: 0, MaxRecords: 10, MaxBytes: 1 << 20})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Records) != count || count > 0 && string(response.Records[count-1].Payload) != payload {
		t.Fatalf("Read response = %+v", response)
	}
}

func assertStreamRecords(t *testing.T, client streamdv1.StreamServiceClient, stream string, payloads ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	response, err := client.Read(ctx, &streamdv1.ReadRequest{Stream: &streamdv1.StreamRef{Namespace: "ha", Stream: stream}, FromSequence: 0, MaxRecords: 100, MaxBytes: 1 << 20})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Records) != len(payloads) {
		t.Fatalf("Read %q returned %d records, want %d", stream, len(response.Records), len(payloads))
	}
	for index, payload := range payloads {
		if response.Records[index].Sequence != uint64(index) || string(response.Records[index].Payload) != payload {
			t.Fatalf("Read %q record %d = %+v", stream, index, response.Records[index])
		}
	}
}

func setStandbyProxy(t *testing.T, enabled bool) {
	t.Helper()
	if err := updateStandbyProxy(enabled); err != nil {
		t.Fatal(err)
	}
}

func setStandbyProxyBestEffort(enabled bool) { _ = updateStandbyProxy(enabled) }

func updateStandbyProxy(enabled bool) error {
	body, err := json.Marshal(map[string]any{"name": "standby", "listen": "0.0.0.0:17443", "upstream": "streamd-standby:7443", "enabled": enabled})
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 3 * time.Second}
	request, err := http.NewRequest(http.MethodPost, env("TOXIPROXY_API", proxyAPI)+"/proxies/standby", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("update Standby proxy: %s", response.Status)
	}
	return nil
}

func fetchMetrics(address string) (map[string]*dto.MetricFamily, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(address)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", address, response.Status)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	return parser.TextToMetricFamilies(response.Body)
}

func fetchDiagnostics(address string) (diagnostics.Snapshot, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(address)
	if err != nil {
		return diagnostics.Snapshot{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return diagnostics.Snapshot{}, fmt.Errorf("GET %s: %s", address, response.Status)
	}
	var snapshot diagnostics.Snapshot
	if err = json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
		return diagnostics.Snapshot{}, err
	}
	return snapshot, nil
}

func strictMetricsReady(primary, standby map[string]*dto.MetricFamily) bool {
	primaryTerm, ok := gaugeValue(primary, "streamd_leadership_term", nil)
	if !ok || primaryTerm <= 0 || !gaugeEquals(primary, "streamd_node_info", map[string]string{"role": "primary", "durability": "replicated_strict"}, 1) ||
		!gaugeEquals(primary, "streamd_write_ready", nil, 1) || !gaugeEquals(primary, "streamd_observer_collection_success", nil, 1) {
		return false
	}
	standbyTerm, ok := gaugeValue(standby, "streamd_leadership_term", nil)
	if !ok || standbyTerm != primaryTerm || !gaugeEquals(standby, "streamd_node_info", map[string]string{"role": "standby", "durability": "replicated_strict"}, 1) ||
		!gaugeEquals(standby, "streamd_write_ready", nil, 0) || !gaugeEquals(standby, "streamd_observer_collection_success", nil, 1) {
		return false
	}
	primaryCommitted, ok := gaugeValue(primary, "streamd_watermark_entry_id", map[string]string{"stage": "committed"})
	if !ok || !gaugeEquals(primary, "streamd_watermark_present", map[string]string{"stage": "committed"}, 1) ||
		!gaugeEquals(primary, "streamd_watermark_entry_id", map[string]string{"stage": "appended"}, primaryCommitted) ||
		!gaugeEquals(primary, "streamd_watermark_entry_id", map[string]string{"stage": "local_durable"}, primaryCommitted) ||
		!gaugeEquals(primary, "streamd_watermark_entry_id", map[string]string{"stage": "replicated"}, primaryCommitted) ||
		!gaugeEquals(primary, "streamd_watermark_entry_id", map[string]string{"stage": "applied"}, primaryCommitted) {
		return false
	}
	return gaugeEquals(standby, "streamd_watermark_present", map[string]string{"stage": "replicated"}, 0) &&
		gaugeEquals(standby, "streamd_watermark_entry_id", map[string]string{"stage": "appended"}, primaryCommitted) &&
		gaugeEquals(standby, "streamd_watermark_entry_id", map[string]string{"stage": "local_durable"}, primaryCommitted) &&
		gaugeEquals(standby, "streamd_watermark_entry_id", map[string]string{"stage": "committed"}, primaryCommitted) &&
		gaugeEquals(standby, "streamd_watermark_entry_id", map[string]string{"stage": "applied"}, primaryCommitted)
}

func gaugeEquals(families map[string]*dto.MetricFamily, name string, labels map[string]string, want float64) bool {
	value, ok := gaugeValue(families, name, labels)
	return ok && value == want
}

func gaugeValue(families map[string]*dto.MetricFamily, name string, labels map[string]string) (float64, bool) {
	family := families[name]
	if family == nil {
		return 0, false
	}
	for _, metric := range family.Metric {
		if metricLabelsMatch(metric.Label, labels) && metric.Gauge != nil {
			return metric.Gauge.GetValue(), true
		}
	}
	return 0, false
}

func metricLabelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
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

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
