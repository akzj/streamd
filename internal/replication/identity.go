package replication

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/url"

	"github.com/akzj/streamd/internal/storage/format"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// NodeURI is the canonical certificate URI SAN for a streamd data node.
// The TLS layer verifies the certificate chain; this identity check binds the
// authenticated leaf certificate to the replication envelope.
func NodeURI(clusterID, groupID, nodeID format.UUID) string {
	return (&url.URL{Scheme: "spiffe", Host: "streamd", Path: "/cluster/" + hex.EncodeToString(clusterID[:]) + "/group/" + hex.EncodeToString(groupID[:]) + "/node/" + hex.EncodeToString(nodeID[:])}).String()
}

type MTLSPeerAuthenticator struct {
	ClusterID      format.UUID
	GroupID        format.UUID
	ExpectedNodeID format.UUID
}

func (a MTLSPeerAuthenticator) Authenticate(ctx context.Context, groupID, nodeID format.UUID) error {
	if zeroUUID(a.ClusterID) || zeroUUID(a.GroupID) || groupID != a.GroupID || zeroUUID(nodeID) {
		return fmt.Errorf("replication certificate identity is outside the configured group")
	}
	if !zeroUUID(a.ExpectedNodeID) && nodeID != a.ExpectedNodeID {
		return fmt.Errorf("replication node is not the configured peer")
	}
	remote, ok := peer.FromContext(ctx)
	if !ok {
		return fmt.Errorf("replication connection has no authenticated peer")
	}
	tlsInfo, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.PeerCertificates) == 0 {
		return fmt.Errorf("replication connection is not verified mTLS")
	}
	want := NodeURI(a.ClusterID, a.GroupID, nodeID)
	for _, identity := range tlsInfo.State.PeerCertificates[0].URIs {
		if identity.String() == want {
			return nil
		}
	}
	return fmt.Errorf("replication certificate URI SAN does not match node identity")
}
