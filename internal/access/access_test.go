package access

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func TestMTLSIdentityAndPrefixPolicy(t *testing.T) {
	identity, _ := url.Parse("spiffe://example.test/workload/aegis-1")
	principal := Principal{Tenant: "prod", Service: "aegis", Instance: "one"}
	certificate := &x509.Certificate{URIs: []*url.URL{identity}}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tlsState(certificate)}})
	controller := Controller{
		Authenticator: MTLSAuthenticator{PrincipalsByURI: map[string]Principal{identity.String(): principal}},
		Policy:        StaticPolicy{Rules: []Rule{{Tenant: "prod", Service: "aegis", Namespace: "agent", StreamPrefix: "events/", Operations: []Operation{Append, Read}}}},
	}
	got, err := controller.Authorize(ctx, "agent", "events/run-1", Append)
	if err != nil || got != principal || got.Producer() != "prod/aegis/one" {
		t.Fatalf("Principal = %+v, error = %v", got, err)
	}
	if _, err = controller.Authorize(ctx, "agent", "private/run-1", Read); err == nil {
		t.Fatal("prefix escape was authorized")
	}
	if _, err = controller.Authorize(ctx, "agent", "events/run-1", Subscribe); err == nil {
		t.Fatal("unauthorized operation was accepted")
	}
}

func TestMTLSRejectsUnverifiedCertificate(t *testing.T) {
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{}})
	if _, err := (MTLSAuthenticator{}).Authenticate(ctx); err == nil {
		t.Fatal("unverified certificate was accepted")
	}
}

func tlsState(certificate *x509.Certificate) (state tls.ConnectionState) {
	state.VerifiedChains = [][]*x509.Certificate{{certificate}}
	return state
}
