package security

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// newAuthTransport stands up a real hashicorp/raft NetworkTransport over an
// authenticated plaintext TCP layer on loopback — the production stack minus
// the raft node — so tests exercise raft's own listen loop and its accept
// backoff.
func newAuthTransport(t *testing.T) (*raft.NetworkTransport, *AuthenticatedStreamLayer) {
	t.Helper()
	inner, err := NewPlainTCPStreamLayer("127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	layer := NewAuthenticatedStreamLayer(inner, testSecret, testCluster)
	return raft.NewNetworkTransport(layer, 1, 2*time.Second, io.Discard), layer
}

// serveAppendEntries answers every AppendEntries the transport receives until
// the test ends.
func serveAppendEntries(t *testing.T, trans *raft.NetworkTransport) {
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		for {
			select {
			case rpc := <-trans.Consumer():
				rpc.Respond(&raft.AppendEntriesResponse{Term: 1, Success: true}, nil)
			case <-done:
				return
			}
		}
	}()
}

// roundTrip sends one AppendEntries from client to target and waits for the
// answer. A successful Dial alone would not prove raft received the conn: the
// handshake completes before the conn is handed to Accept.
func roundTrip(client *raft.NetworkTransport, target raft.ServerAddress) error {
	var resp raft.AppendEntriesResponse
	return client.AppendEntries("server", target, &raft.AppendEntriesRequest{Term: 1}, &resp)
}

// TestRaftAuth_StalledNodeAcceptsAfterAbandonedBacklog is the #1431 regression.
// A node stalled for 15s+ resumes with its accept backlog full of dials whose
// dialers already gave up. When Accept returned each failed handshake as an
// error, raft's listen loop backed off up to 1s per failure, every newer dial
// outlived its 400ms handshake deadline in the backlog, and the node never
// rejoined. The first fresh dial must now complete.
func TestRaftAuth_StalledNodeAcceptsAfterAbandonedBacklog(t *testing.T) {
	inner, err := NewPlainTCPStreamLayer("127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	addr := inner.Addr().String()

	// The stall: peers connect (the kernel completes the TCP handshake) and
	// give up before anything accepts.
	for i := 0; i < 16; i++ {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		c.Close()
	}

	// The node resumes: raft's listen loop starts draining the backlog.
	server := raft.NewNetworkTransport(NewAuthenticatedStreamLayer(inner, testSecret, testCluster), 1, 2*time.Second, io.Discard)
	defer server.Close()
	serveAppendEntries(t, server)

	client, _ := newAuthTransport(t)
	defer client.Close()

	// The FIRST attempt must succeed: on the old code a later attempt
	// eventually gets through once the stale conns drain (~9s of backoff
	// here), while in production new abandoned dials kept the backlog full.
	start := time.Now()
	if err := roundTrip(client, raft.ServerAddress(addr)); err != nil {
		t.Fatalf("first AppendEntries after the stall failed after %v: %v", time.Since(start), err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("first AppendEntries after the stall took %v", d)
	}
}

// TestRaftAuth_SilentPeerDoesNotBlockOthers: connections that never speak hold
// their handshake for the full raftAuthDeadline. They must not hold up a
// legitimate peer queued behind them, which serial in-Accept handshakes did
// (400ms each, past the dialer's own 400ms deadline).
func TestRaftAuth_SilentPeerDoesNotBlockOthers(t *testing.T) {
	server, layer := newAuthTransport(t)
	defer server.Close()
	serveAppendEntries(t, server)
	addr := layer.Addr().String()

	for i := 0; i < 4; i++ {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close() // silent: never answers the challenge
	}

	client, _ := newAuthTransport(t)
	defer client.Close()
	start := time.Now()
	if err := roundTrip(client, raft.ServerAddress(addr)); err != nil {
		t.Fatalf("AppendEntries behind silent peers failed after %v: %v", time.Since(start), err)
	}
	if d := time.Since(start); d >= raftAuthDeadline {
		t.Fatalf("AppendEntries behind silent peers took %v, want < %v", d, raftAuthDeadline)
	}
}
