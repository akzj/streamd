package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	clusterID     = "11111111-1111-1111-1111-111111111111"
	groupID       = "22222222-2222-2222-2222-222222222222"
	primaryNodeID = "33333333-3333-3333-3333-333333333333"
	standbyNodeID = "44444444-4444-4444-4444-444444444444"
	clientURI     = "spiffe://streamd.test/client/ha"
)

type authority struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	certPEM     []byte
}

type nodeConfig struct {
	ListenAddress        string               `json:"listen_address"`
	AdminAddress         string               `json:"admin_address"`
	DataDirectory        string               `json:"data_directory"`
	ClusterID            string               `json:"cluster_id"`
	GroupID              string               `json:"group_id"`
	NodeID               string               `json:"node_id"`
	ShutdownTimeout      string               `json:"shutdown_timeout"`
	SubscribeSendTimeout string               `json:"subscribe_send_timeout"`
	CheckpointInterval   string               `json:"checkpoint_interval"`
	TLS                  tlsConfig            `json:"tls"`
	PrincipalsByURI      map[string]principal `json:"principals_by_uri"`
	Authorization        []authorizationRule  `json:"authorization"`
	Replication          replicationConfig    `json:"replication"`
}

type tlsConfig struct {
	CertificateFile string `json:"certificate_file"`
	PrivateKeyFile  string `json:"private_key_file"`
	ClientCAFile    string `json:"client_ca_file"`
}

type principal struct {
	Tenant  string `json:"tenant"`
	Service string `json:"service"`
}

type authorizationRule struct {
	Tenant       string   `json:"tenant"`
	Service      string   `json:"service"`
	Namespace    string   `json:"namespace"`
	StreamPrefix string   `json:"stream_prefix"`
	Operations   []string `json:"operations"`
}

type replicationConfig struct {
	Role              string     `json:"role"`
	PeerAddress       string     `json:"peer_address,omitempty"`
	PeerServerName    string     `json:"peer_server_name,omitempty"`
	PeerNodeID        string     `json:"peer_node_id,omitempty"`
	LeaseTTL          string     `json:"lease_ttl"`
	LeaseSafetyMargin string     `json:"lease_safety_margin"`
	RenewInterval     string     `json:"renew_interval"`
	MaxEntries        int        `json:"max_entries"`
	MaxBytes          uint64     `json:"max_bytes"`
	Etcd              etcdConfig `json:"etcd"`
}

type etcdConfig struct {
	Endpoints       []string `json:"endpoints"`
	Prefix          string   `json:"prefix"`
	DialTimeout     string   `json:"dial_timeout"`
	ServerName      string   `json:"server_name"`
	CertificateFile string   `json:"certificate_file"`
	PrivateKeyFile  string   `json:"private_key_file"`
	CAFile          string   `json:"ca_file"`
}

func main() {
	out := flag.String("out", "", "empty output directory")
	flag.Parse()
	if *out == "" {
		fatal(fmt.Errorf("-out is required"))
	}
	if err := prepare(*out); err != nil {
		fatal(err)
	}
}

func prepare(out string) error {
	if entries, err := os.ReadDir(out); err == nil && len(entries) != 0 {
		return fmt.Errorf("output directory %q is not empty", out)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, directory := range []string{"certs", "configs"} {
		if err := os.MkdirAll(filepath.Join(out, directory), 0750); err != nil {
			return err
		}
	}
	ca, err := newAuthority()
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(out, "certs", "ca.crt"), ca.certPEM, 0644); err != nil {
		return err
	}
	certificates := []struct {
		name string
		dns  []string
		uris []string
		both bool
	}{
		{name: "etcd", dns: []string{"etcd", "etcd-1", "etcd-2", "etcd-3"}, both: true},
		{name: "etcd-client"},
		{name: "primary", dns: []string{"streamd-primary"}, uris: []string{nodeURI(clusterID, groupID, primaryNodeID)}, both: true},
		{name: "standby", dns: []string{"streamd-standby"}, uris: []string{nodeURI(clusterID, groupID, standbyNodeID)}, both: true},
		{name: "client", uris: []string{clientURI}},
	}
	for _, certificate := range certificates {
		if err = ca.issue(filepath.Join(out, "certs"), certificate.name, certificate.dns, certificate.uris, certificate.both); err != nil {
			return err
		}
	}
	if err = writeConfig(filepath.Join(out, "configs", "primary.json"), makeConfig("primary", primaryNodeID)); err != nil {
		return err
	}
	return writeConfig(filepath.Join(out, "configs", "standby.json"), makeConfig("standby", standbyNodeID))
}

func newAuthority() (*authority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: "streamd HA test CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &authority{certificate: certificate, key: key, certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

func (a *authority) issue(directory, name string, dns, uriStrings []string, both bool) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	var uris []*url.URL
	for _, value := range uriStrings {
		parsed, parseErr := url.Parse(value)
		if parseErr != nil {
			return parseErr
		}
		uris = append(uris, parsed)
	}
	usage := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	if len(dns) != 0 || both {
		usage = append(usage, x509.ExtKeyUsageServerAuth)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: name}, DNSNames: dns, IPAddresses: []net.IP{}, URIs: uris, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(12 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usage}
	der, err := x509.CreateCertificate(rand.Reader, template, a.certificate, &key.PublicKey, a.key)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(directory, name+".crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(directory, name+".key"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600)
}

func makeConfig(role, nodeID string) nodeConfig {
	etcdPorts := []int{12379, 12380, 12381}
	config := nodeConfig{
		ListenAddress: "0.0.0.0:7443", AdminAddress: "127.0.0.1:9090", DataDirectory: "/var/lib/streamd", ClusterID: clusterID, GroupID: groupID, NodeID: nodeID,
		ShutdownTimeout: "5s", SubscribeSendTimeout: "5s", CheckpointInterval: "1s",
		TLS:             tlsConfig{CertificateFile: "/etc/streamd/tls/" + role + ".crt", PrivateKeyFile: "/etc/streamd/tls/" + role + ".key", ClientCAFile: "/etc/streamd/tls/ca.crt"},
		PrincipalsByURI: map[string]principal{clientURI: {Tenant: "ha", Service: "test"}},
		Authorization:   []authorizationRule{{Tenant: "ha", Service: "test", Namespace: "ha", StreamPrefix: "", Operations: []string{"append", "read", "subscribe", "inspect"}}},
	}
	if role == "standby" {
		etcdPorts = []int{22379, 22380, 22381}
	}
	endpoints := make([]string, 0, len(etcdPorts))
	for _, port := range etcdPorts {
		endpoints = append(endpoints, fmt.Sprintf("https://toxiproxy:%d", port))
	}
	config.Replication = replicationConfig{Role: role, LeaseTTL: "15s", LeaseSafetyMargin: "3s", RenewInterval: "3s", MaxEntries: 1024, MaxBytes: 16 << 20, Etcd: etcdConfig{Endpoints: endpoints, Prefix: "/streamd/ha-test", DialTimeout: "3s", ServerName: "etcd", CertificateFile: "/etc/streamd/etcd/etcd-client.crt", PrivateKeyFile: "/etc/streamd/etcd/etcd-client.key", CAFile: "/etc/streamd/etcd/ca.crt"}}
	if role == "primary" {
		config.Replication.PeerAddress = "toxiproxy:17443"
		config.Replication.PeerServerName = "streamd-standby"
		config.Replication.PeerNodeID = standbyNodeID
	}
	return config
}

func writeConfig(path string, config nodeConfig) error {
	encoded, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	return os.WriteFile(path, encoded, 0640)
}

func nodeURI(cluster, group, node string) string {
	return "spiffe://streamd/cluster/" + compact(cluster) + "/group/" + compact(group) + "/node/" + compact(node)
}

func compact(value string) string {
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	if err != nil || len(decoded) != 16 {
		panic("invalid fixed UUID")
	}
	return hex.EncodeToString(decoded)
}

func serial() *big.Int {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	value, err := rand.Int(rand.Reader, limit)
	if err != nil {
		panic(err)
	}
	return value
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "prepare HA test environment:", err)
	os.Exit(1)
}
