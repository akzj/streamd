package node

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/streamd/internal/access"
)

const validConfig = `{
  "listen_address":"127.0.0.1:7443",
  "admin_address":"127.0.0.1:9090",
  "data_directory":"/var/lib/streamd",
  "cluster_id":"11111111-1111-1111-1111-111111111111",
  "group_id":"22222222-2222-2222-2222-222222222222",
  "node_id":"33333333-3333-3333-3333-333333333333",
  "tls":{"certificate_file":"server.crt","private_key_file":"server.key","client_ca_file":"ca.crt"},
  "principals_by_uri":{"spiffe://example/workload":{"tenant":"t","service":"s"}},
  "authorization":[{"tenant":"t","service":"s","namespace":"n","stream_prefix":"","operations":["read"]}]
}`

func TestLoadConfigStrictly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streamd.json")
	if err := os.WriteFile(path, []byte(validConfig), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.ListenAddress != "127.0.0.1:7443" || len(config.Authorization) != 1 {
		t.Fatalf("config = %+v", config)
	}
	unknown := filepath.Join(t.TempDir(), "unknown.json")
	data := []byte(validConfig[:len(validConfig)-2] + `,"unknown":true}`)
	if err = os.WriteFile(unknown, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadConfig(unknown); err == nil {
		t.Fatal("unknown configuration field was accepted")
	}
}

func TestConfigRequiresLoopbackAdmin(t *testing.T) {
	config := Config{ListenAddress: "0.0.0.0:7443", AdminAddress: "0.0.0.0:9090", DataDirectory: "/data", ClusterID: "11111111111111111111111111111111", GroupID: "22222222222222222222222222222222", NodeID: "33333333333333333333333333333333", TLS: TLSConfig{CertificateFile: "c", PrivateKeyFile: "k", ClientCAFile: "ca"}}
	config.PrincipalsByURI = map[string]access.Principal{"spiffe://example/a": {Tenant: "t", Service: "s"}}
	config.Authorization = []access.Rule{{Tenant: "t", Service: "s", Namespace: "n", Operations: []access.Operation{access.Read}}}
	if err := config.Validate(); err == nil {
		t.Fatal("non-loopback admin address was accepted")
	}
}
