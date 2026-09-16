package cluster

// The leader closes a forwarded-apply connection that has carried no message
// for forwardConnIdleTimeout. The client cached that connection and only found
// out by writing into a dead socket, which failed the caller's command (#851).
// Callers with a retry loop recovered; a single-shot caller such as a token
// creation returned 500 to the user.

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// idleLeader accepts connections and records how many it has seen, so a test
// can tell a reused connection from a redial.
type idleLeader struct {
	ln       net.Listener
	accepted chan net.Conn
}

func startIdleLeader(t *testing.T) *idleLeader {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	l := &idleLeader{ln: ln, accepted: make(chan net.Conn, 16)}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			l.accepted <- conn
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return l
}

func (l *idleLeader) addr() string { return l.ln.Addr().String() }

// waitAccepted waits for the listener goroutine to have accepted exactly n
// connections, then reports the count. Polling avoids racing the accept.
func (l *idleLeader) waitAccepted(t *testing.T, n int) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(l.accepted) >= n {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	return len(l.accepted)
}

func newForwardCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	local := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	return &Coordinator{
		cfg:       &config.ClusterConfig{SharedSecret: "test-cluster-secret-32-bytes-long!", ClusterName: "test-cluster"},
		registry:  NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()}),
		localNode: local,
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}
}

func TestGetOrDialLeader_ReusesAFreshConnection(t *testing.T) {
	leader := startIdleLeader(t)
	c := newForwardCoordinator(t)
	ctx := context.Background()

	first, reused, err := c.getOrDialLeader(ctx, "leader-1", leader.addr())
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	if reused {
		t.Error("the first connection was reported as reused")
	}
	second, reused, err := c.getOrDialLeader(ctx, "leader-1", leader.addr())
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if !reused || second != first {
		t.Error("a warm connection was not reused")
	}
	if got := leader.waitAccepted(t, 1); got != 1 {
		t.Errorf("leader accepted %d connections; want 1", got)
	}
	c.closeForwardConn()
}

// A connection idle past forwardConnIdleRefresh is redialled rather than
// handed back, so the caller never writes into one the leader has dropped.
func TestGetOrDialLeader_RedialsAnIdleConnection(t *testing.T) {
	leader := startIdleLeader(t)
	c := newForwardCoordinator(t)
	ctx := context.Background()

	first, _, err := c.getOrDialLeader(ctx, "leader-1", leader.addr())
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	// Age the cached connection past the refresh threshold.
	c.forwardConnMu.Lock()
	c.forwardConnUsedAt = time.Now().Add(-forwardConnIdleRefresh - time.Second)
	c.forwardConnMu.Unlock()

	second, reused, err := c.getOrDialLeader(ctx, "leader-1", leader.addr())
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if reused {
		t.Error("an idle connection was reported as reused")
	}
	if second == first {
		t.Error("an idle connection was handed back instead of redialled")
	}
	if got := leader.waitAccepted(t, 2); got != 2 {
		t.Errorf("leader accepted %d connections; want 2", got)
	}
	c.closeForwardConn()
}

func TestForwardConnRefresh_LeavesRoomForAWholeRoundTrip(t *testing.T) {
	if forwardConnIdleRefresh >= forwardConnIdleTimeout {
		t.Fatalf("client refresh %v must be below the leader's idle timeout %v, or the race is never avoided", forwardConnIdleRefresh, forwardConnIdleTimeout)
	}
	// A connection handed out just under the refresh threshold must still have
	// time for a whole round trip before the leader would drop it, or a
	// long-running forward could be closed under itself.
	if margin := forwardConnIdleTimeout - forwardConnIdleRefresh; margin < 2*forwardApplyTimeout {
		t.Errorf("margin %v is below one round trip (%v); a forward started just before the refresh could outlive the leader's timeout", margin, 2*forwardApplyTimeout)
	}
}

// A send failure is retryable because the leader cannot have applied a command
// it never received; a receive failure is not, because the command may have
// been applied and only the ack lost.
func TestForwardApply_OnlySendFailuresAreMarkedRetryable(t *testing.T) {
	leader := startIdleLeader(t)
	c := newForwardCoordinator(t)
	ctx := context.Background()

	conn, _, err := c.getOrDialLeader(ctx, "leader-1", leader.addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// Close the socket under the round trip: the write fails, and the error
	// must carry the retryable marker.
	conn.Close()
	_, err = c.forwardApplyRoundTrip(ctx, conn, signedForwardRequest(t, c))
	if err == nil {
		t.Fatal("writing to a closed connection succeeded")
	}
	if !errors.Is(err, errForwardSendFailed) {
		t.Errorf("send failure not marked retryable: %v", err)
	}

	// A leader that accepts and then closes without answering fails on the
	// RECEIVE, which must NOT be marked retryable.
	//
	// The first phase's peer is still queued on the listener channel; take it
	// out of the way first, or the reader goroutine below would serve THAT
	// connection and this phase would pass on its context deadline rather
	// than on the leader hanging up.
	select {
	case stale := <-leader.accepted:
		stale.Close()
	case <-time.After(3 * time.Second):
		t.Fatal("the listener never accepted the first connection")
	}
	c.closeForwardConn()
	conn2, _, err := c.getOrDialLeader(ctx, "leader-1", leader.addr())
	if err != nil {
		t.Fatalf("dial 2: %v", err)
	}
	hungUp := make(chan struct{})
	go func() {
		defer close(hungUp)
		// Read the WHOLE frame before hanging up, so the client's send has
		// certainly completed and the failure it sees is a receive failure.
		// Reading only the first bytes would sometimes close under the
		// client's second write and produce a send failure instead.
		select {
		case peer := <-leader.accepted:
			_ = peer.SetReadDeadline(time.Now().Add(3 * time.Second))
			var hdr [5]byte // 4-byte length + 1-byte type
			if _, err := io.ReadFull(peer, hdr[:]); err == nil {
				n := binary.BigEndian.Uint32(hdr[:4])
				if n > 0 && n < 1<<20 {
					_, _ = io.ReadFull(peer, make([]byte, n-1))
				}
			}
			peer.Close()
		case <-time.After(3 * time.Second):
		}
	}()
	// A generous deadline: the failure must come from the leader closing, not
	// from this timeout. The check below confirms which it was.
	rctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	start := time.Now()
	_, err = c.forwardApplyRoundTrip(rctx, conn2, signedForwardRequest(t, c))
	if err == nil {
		t.Fatal("a round trip with no ack succeeded")
	}
	<-hungUp
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("the round trip failed after %v, which is the deadline rather than the leader hanging up", elapsed)
	}
	if errors.Is(err, errForwardSendFailed) {
		t.Errorf("a receive failure was marked retryable, which would risk applying the command twice: %v", err)
	}
}

// signedForwardRequest builds a request the leader would accept, so the test
// exercises the transport rather than the auth path.
func signedForwardRequest(t *testing.T, c *Coordinator) *protocol.ForwardApplyRequest {
	t.Helper()
	cmdJSON, err := json.Marshal(&raft.Command{Type: raft.CommandRegisterFile})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	nonce, err := security.GenerateNonce()
	if err != nil {
		t.Fatalf("nonce: %v", err)
	}
	ts := time.Now().Unix()
	return &protocol.ForwardApplyRequest{
		CommandJSON: cmdJSON,
		NodeID:      c.localNode.ID,
		Nonce:       nonce,
		Timestamp:   ts,
		HMAC:        security.ComputeForwardHMAC(c.cfg.SharedSecret, nonce, c.localNode.ID, c.cfg.ClusterName, cmdJSON, ts),
	}
}

// deadConn is a connection whose writes fail the way a socket the peer has
// already reset does. It makes the retry decision deterministic, which a real
// socket cannot: a peer that closed gracefully usually accepts the write and
// fails the read instead, and that case is deliberately not retried.
type deadConn struct{ net.Conn }

func (d *deadConn) Write([]byte) (int, error) {
	return 0, &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}
}

// answeringLeader accepts one connection, reads the forwarded request and
// replies with a correctly signed ok ack, as a real leader would.
func answeringLeader(t *testing.T, secret, cluster string) (addr string, dials *int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var count int32
	signer := &Coordinator{cfg: &config.ClusterConfig{SharedSecret: secret, ClusterName: cluster}, logger: zerolog.Nop()}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			atomic.AddInt32(&count, 1)
			go func(conn net.Conn) {
				defer conn.Close()
				msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
				if err != nil {
					return
				}
				req, ok := msg.Payload.(*protocol.ForwardApplyRequest)
				if !ok {
					return
				}
				ack := &protocol.ForwardApplyAck{Status: "ok"}
				signer.signForwardAck(ack, req.Nonce)
				_ = protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgForwardApplyAck, Payload: ack}, 5*time.Second)
			}(conn)
		}
	}()
	return ln.Addr().String(), &count
}

// The issue's headline path: a cached connection the leader has dropped, a
// send that fails on it, and the command succeeding on a fresh connection
// rather than being handed back to the caller as an error.
func TestForwardApplyToLeader_RetriesOnceAfterAStaleConnectionFailsToSend(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft node")
	}
	const secret = "test-cluster-secret-32-bytes-long!"
	leaderAddr, dials := answeringLeader(t, secret, "test-cluster")

	// A bootstrapped node whose id differs from ours gives a stable leader
	// that is never us, so forwardApplyToLeader takes the forwarding path.
	raftNode := startRaftNode(t, "forward-leader", allocFreePort(t), true)
	t.Cleanup(func() { _ = raftNode.Stop() })
	if err := raftNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	local := NewNode("forward-follower", "forward-follower", RoleWriter, "test-cluster")
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	leader := NewNode("forward-leader", "forward-leader", RoleWriter, "test-cluster")
	leader.Address = leaderAddr
	if err := reg.Register(leader); err != nil {
		t.Fatalf("register leader: %v", err)
	}
	c := &Coordinator{
		cfg:       &config.ClusterConfig{SharedSecret: secret, ClusterName: "test-cluster"},
		registry:  reg,
		raftNode:  raftNode,
		localNode: local,
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}

	// Prime the cache with a connection that cannot be written to, exactly as
	// a leader that dropped an idle one leaves it.
	primed := atomic.LoadInt32(dials)
	real, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		t.Fatalf("prime dial: %v", err)
	}
	// Wait for the listener to count the priming dial, or the baseline below
	// is sampled before it lands and the retry looks like two connections.
	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadInt32(dials) == primed && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	c.forwardConnMu.Lock()
	c.forwardConn = &deadConn{Conn: real}
	c.forwardConnLeader = "forward-leader"
	c.forwardConnUsedAt = time.Now()
	c.forwardConnMu.Unlock()
	before := atomic.LoadInt32(dials)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.forwardApplyToLeader(ctx, &raft.Command{Type: raft.CommandRegisterFile}); err != nil {
		t.Fatalf("forwardApplyToLeader = %v; want success after one retry on a fresh connection", err)
	}
	if got := atomic.LoadInt32(dials) - before; got != 1 {
		t.Errorf("leader saw %d new connections; want exactly 1 (the retry)", got)
	}
	c.closeForwardConn()
}

// The marker must name socket failures only. An oversized frame never reaches
// the wire, so redialling for it would drop a healthy connection and fail the
// same way again.
func TestWireSendFailure_MarksOnlySocketFailures(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"broken pipe", &net.OpError{Op: "write", Err: syscall.EPIPE}, true},
		{"connection reset", &net.OpError{Op: "write", Err: syscall.ECONNRESET}, true},
		{"use of closed connection", net.ErrClosed, true},
		{"closed pipe", io.ErrClosedPipe, true},
		{"wrapped socket error", fmt.Errorf("failed to write payload: %w", &net.OpError{Op: "write", Err: syscall.EPIPE}), true},
		{"frame refused before the wire", errors.New("message too large: 2000000 bytes (max 1048576)"), false},
		{"marshal failure", errors.New("failed to marshal payload: unsupported type"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := wireSendFailure(tc.err); got != tc.want {
				t.Errorf("wireSendFailure(%v) = %v; want %v", tc.err, got, tc.want)
			}
		})
	}
}

// The marker must not leak into what an operator reads.
func TestForwardSendError_KeepsTheMarkerOutOfTheMessage(t *testing.T) {
	base := &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}
	wrapped := fmt.Errorf("forward apply: send: %w", &forwardSendError{err: base})
	if !errors.Is(wrapped, errForwardSendFailed) {
		t.Error("the send marker is not matchable")
	}
	if !errors.Is(wrapped, syscall.EPIPE) {
		t.Error("the underlying socket error is no longer matchable")
	}
	if msg := wrapped.Error(); strings.Contains(msg, "send failed") || strings.Contains(msg, "\n") {
		t.Errorf("operator-facing message carries internal bookkeeping: %q", msg)
	}
}
