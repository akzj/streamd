package node

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/akzj/streamd/internal/access"
	"github.com/akzj/streamd/internal/service"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	ListenAddress        string                      `json:"listen_address"`
	AdminAddress         string                      `json:"admin_address"`
	DataDirectory        string                      `json:"data_directory"`
	ShutdownTimeout      string                      `json:"shutdown_timeout"`
	SubscribeSendTimeout string                      `json:"subscribe_send_timeout"`
	TLS                  TLSConfig                   `json:"tls"`
	PrincipalsByURI      map[string]access.Principal `json:"principals_by_uri"`
	Authorization        []access.Rule               `json:"authorization"`
	Limits               service.Limits              `json:"limits"`
	OTLPTraceEndpoint    string                      `json:"otlp_trace_endpoint,omitempty"`
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
	return nil
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
