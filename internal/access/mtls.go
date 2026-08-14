package access

import (
	"context"
	"crypto/x509"
	"fmt"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// MTLSAuthenticator maps an exact URI SAN from a verified client certificate
// to a deployment-defined Principal. It deliberately does not invent a URI
// path schema for tenant, service, and instance.
type MTLSAuthenticator struct {
	PrincipalsByURI map[string]Principal
}

func (a MTLSAuthenticator) Authenticate(ctx context.Context) (Principal, error) {
	remote, ok := peer.FromContext(ctx)
	if !ok || remote.AuthInfo == nil {
		return Principal{}, fmt.Errorf("%w: peer TLS information is missing", ErrUnauthenticated)
	}
	info, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.VerifiedChains[0]) == 0 {
		return Principal{}, fmt.Errorf("%w: client certificate is not verified", ErrUnauthenticated)
	}
	return a.mapCertificate(info.State.VerifiedChains[0][0])
}

func (a MTLSAuthenticator) mapCertificate(certificate *x509.Certificate) (Principal, error) {
	var matched *Principal
	for _, uri := range certificate.URIs {
		principal, ok := a.PrincipalsByURI[uri.String()]
		if !ok {
			continue
		}
		if matched != nil && *matched != principal {
			return Principal{}, fmt.Errorf("%w: certificate maps to multiple Principals", ErrUnauthenticated)
		}
		copy := principal
		matched = &copy
	}
	if matched == nil {
		return Principal{}, fmt.Errorf("%w: certificate URI SAN is not registered", ErrUnauthenticated)
	}
	if err := matched.Validate(); err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	return *matched, nil
}
