package node

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc/credentials"
)

type recordingCredentials struct{ authority string }

func (c *recordingCredentials) ClientHandshake(_ context.Context, authority string, connection net.Conn) (net.Conn, credentials.AuthInfo, error) {
	c.authority = authority
	return connection, nil, nil
}

func (*recordingCredentials) ServerHandshake(connection net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return connection, nil, nil
}

func (*recordingCredentials) Info() credentials.ProtocolInfo { return credentials.ProtocolInfo{} }

func (c *recordingCredentials) Clone() credentials.TransportCredentials {
	return &recordingCredentials{authority: c.authority}
}

func (*recordingCredentials) OverrideServerName(string) error { return nil }

func TestFixedServerNameCredentialsIgnoreResolverAuthority(t *testing.T) {
	base := &recordingCredentials{}
	wrapped := fixedServerNameCredentials{TransportCredentials: base, serverName: "etcd.internal"}
	if _, _, err := wrapped.ClientHandshake(context.Background(), "transport-proxy:2379", nil); err != nil {
		t.Fatal(err)
	}
	if base.authority != "etcd.internal" {
		t.Fatalf("TLS authority = %q", base.authority)
	}
	if err := wrapped.OverrideServerName("other.internal"); err == nil {
		t.Fatal("fixed TLS server name was overridden")
	}
}
