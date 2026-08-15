package replication

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"testing"

	streamdv1 "github.com/akzj/streamd/api/streamd/v1"
	"github.com/akzj/streamd/internal/storage/format"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/test/bufconn"
)

type allowPeer struct{}

func (allowPeer) Authenticate(context.Context, format.UUID, format.UUID) error { return nil }

func TestRPCTransportReplicatesAndCommits(t *testing.T) {
	log := &primaryTestLog{}
	receiver, err := NewReceiver(log, ReceiverConfig{
		GroupID: uuid(1), NodeID: uuid(3), State: ReceiverState{Term: 7, LeaderID: uuid(2)},
		ChecksumAt: func(entryID uint64) (uint32, bool) {
			entry, ok := log.entryAt(entryID)
			return entry.CRC32C, ok
		},
		EntryAt: log.entryAt, ObserveTerm: func(uint64, format.UUID) error { return nil }, ApplyThrough: func(uint64, uint64) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewRPCServer(receiver, nil, allowPeer{}, TransportLimits{MaxEntries: 4, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	client, stop := startReplicationRPC(t, server)
	defer stop()
	peer, err := NewRPCPeer(client, TransportLimits{MaxEntries: 4, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	primary, err := NewPrimary(uuid(1), uuid(2), 7, peer)
	if err != nil {
		t.Fatal(err)
	}
	last, err := primary.Replicate(context.Background(), encodedEntriesFor(t, 7, "rpc", 2, 0, 0))
	if err != nil || last != 1 || log.syncs != 1 {
		t.Fatalf("last = %d, syncs = %d, error = %v", last, log.syncs, err)
	}
	if err = primary.AdvanceCommit(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	state, err := receiver.State()
	if err != nil || !state.Applied.Valid || state.Applied.EntryID != last {
		t.Fatalf("receiver state = %+v, error = %v", state, err)
	}
}

func TestRPCTransportEnforcesBatchBounds(t *testing.T) {
	peer, err := NewRPCPeer(&stubReplicationClient{}, TransportLimits{MaxEntries: 1, MaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	err = peer.Append(context.Background(), AppendEntries{Entries: [][]byte{{1}, {2}}})
	if !IsCode(err, ErrInvalidState) {
		t.Fatalf("Entry bound error = %v", err)
	}
	err = peer.Append(context.Background(), AppendEntries{Entries: [][]byte{{1, 2}}})
	if !IsCode(err, ErrInvalidState) {
		t.Fatalf("byte bound error = %v", err)
	}
}

func TestRPCNegotiateMapsPlan(t *testing.T) {
	planner := func(hello ReplicaHello) (ReplicationPlan, error) {
		return ReplicationPlan{Term: 9, LeaderID: uuid(2), Mode: PlanIncremental, StartEntryID: 4, EarliestWAL: 2, Committed: position(3, 103)}, nil
	}
	server, err := NewRPCServer(nil, planner, allowPeer{}, TransportLimits{})
	if err != nil {
		t.Fatal(err)
	}
	groupID, nodeID := uuid(1), uuid(3)
	response, err := server.Negotiate(context.Background(), &streamdv1.ReplicationServiceNegotiateRequest{ProtocolVersion: 1, GroupId: groupID[:], NodeId: nodeID[:]})
	if err != nil || response.Term != 9 || response.StartEntryId != 4 || response.Committed.EntryId != 3 {
		t.Fatalf("response = %+v, error = %v", response, err)
	}
}

func startReplicationRPC(t *testing.T, service *RPCServer) (streamdv1.ReplicationServiceClient, func()) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	streamdv1.RegisterReplicationServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		server.Stop()
		t.Fatal(err)
	}
	return streamdv1.NewReplicationServiceClient(connection), func() {
		_ = connection.Close()
		server.Stop()
		_ = listener.Close()
	}
}

type stubReplicationClient struct {
	streamdv1.ReplicationServiceClient
}

func TestMTLSPeerAuthenticatorBindsNodeURI(t *testing.T) {
	cluster, group, node := uuid(1), uuid(2), uuid(3)
	identity, err := url.Parse(NodeURI(cluster, group, node))
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{URIs: []*url.URL{identity}}
	ctx := peerContext(certificate)
	auth := MTLSPeerAuthenticator{ClusterID: cluster, GroupID: group}
	if err = auth.Authenticate(ctx, group, node); err != nil {
		t.Fatal(err)
	}
	if err = auth.Authenticate(ctx, group, uuid(4)); err == nil {
		t.Fatal("mismatched certificate node was accepted")
	}
}

func peerContext(certificate *x509.Certificate) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}, VerifiedChains: [][]*x509.Certificate{{certificate}}}}})
}
