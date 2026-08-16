//go:build integration

package ha_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	streamdv1 "github.com/akzj/streamd/api/streamd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	primaryAddress = "streamd-primary:7443"
	proxyAPI       = "http://toxiproxy:8474"
)

func TestComposeStrictHA(t *testing.T) {
	client, closeClient := streamClient(t)
	defer closeClient()

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

	t.Run("standby partition cannot acknowledge strict append", func(t *testing.T) {
		setStandbyProxy(t, false)
		t.Cleanup(func() { setStandbyProxyBestEffort(true) })
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := client.Append(ctx, &streamdv1.AppendRequest{Stream: &streamdv1.StreamRef{Namespace: "ha", Stream: "events"}, ExpectedSequence: 1, RequestId: []byte("ha-request-0002"), Record: &streamdv1.InputRecord{Payload: []byte("must-not-ack")}, RequiredDurability: streamdv1.Durability_DURABILITY_REPLICATED_STRICT})
		cancel()
		if err == nil {
			t.Fatal("Strict Append succeeded while the Standby link was partitioned")
		}
		assertRecords(t, client, 1, "replicated")
	})
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
	connection, err := grpc.NewClient(env("HA_PRIMARY_ADDRESS", primaryAddress), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: roots, ServerName: "streamd-primary"})))
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

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
