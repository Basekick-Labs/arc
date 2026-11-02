package security

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

const testSecret = "test-cluster-secret-32-bytes-long!"
const testCluster = "test-cluster"

// pipeStreamLayer is an in-memory raft.StreamLayer backed by net.Pipe, so the
// handshake can be exercised without binding a real port. Accept returns the
// server end of the most recent Dial; Close unblocks it.
type pipeStreamLayer struct {
	accept    chan net.Conn
	done      chan struct{}
	closeOnce sync.Once
}

func newPipeStreamLayer() *pipeStreamLayer {
	return &pipeStreamLayer{accept: make(chan net.Conn, 1), done: make(chan struct{})}
}

func (p *pipeStreamLayer) Accept() (net.Conn, error) {
	select {
	case c := <-p.accept:
		return c, nil
	case <-p.done:
		return nil, net.ErrClosed
	}
}
func (p *pipeStreamLayer) Close() error {
	p.closeOnce.Do(func() { close(p.done) })
	return nil
}
func (p *pipeStreamLayer) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1} }
func (p *pipeStreamLayer) Dial(_ raft.ServerAddress, _ time.Duration) (net.Conn, error) {
	client, server := net.Pipe()
	p.accept <- server
	return client, nil
}

// handshakePair runs a server Accept and client Dial concurrently over the same
// pipe and returns the resulting conns (or the errors).
func handshakePair(t *testing.T, serverLayer, clientLayer *AuthenticatedStreamLayer) (net.Conn, error, net.Conn, error) {
	t.Helper()
	type res struct {
		conn net.Conn
		err  error
	}
	srvCh := make(chan res, 1)
	go func() {
		c, err := serverLayer.Accept()
		srvCh <- res{c, err}
	}()
	cliConn, cliErr := clientLayer.Dial("ignored", time.Second)
	sr := <-srvCh
	return sr.conn, sr.err, cliConn, cliErr
}

// expectRejected dials serverLayer with clientLayer and asserts the server
// never hands the connection to its Accept caller (a rejected peer is closed
// inside the layer, not returned as an error — #1431), and that the client
// Dial fails.
func expectRejected(t *testing.T, base *pipeStreamLayer, server, client *AuthenticatedStreamLayer) {
	t.Helper()
	accepted := make(chan net.Conn, 1)
	go func() {
		if c, err := server.Accept(); err == nil {
			accepted <- c
		}
	}()
	cliConn, cliErr := client.Dial("ignored", time.Second)
	if cliErr == nil {
		cliConn.Close()
		t.Error("client Dial succeeded against a peer that should have refused it")
	}
	select {
	case c := <-accepted:
		c.Close()
		t.Fatal("server Accept returned a connection that failed authentication")
	case <-time.After(200 * time.Millisecond):
	}
	server.Close()
	base.Close()
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newLoggedLayer(inner raft.StreamLayer, secret, cluster string) (*AuthenticatedStreamLayer, *syncBuffer) {
	logs := &syncBuffer{}
	return NewAuthenticatedStreamLayer(inner, secret, cluster).WithLogger(zerolog.New(logs).Level(zerolog.DebugLevel)), logs
}

// waitForLog polls logs for substr; the rejecting goroutine logs after the
// client has already seen its Dial fail.
func waitForLog(t *testing.T, logs *syncBuffer, substr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(logs.String(), substr) {
		if time.Now().After(deadline) {
			t.Fatalf("log never contained %q; got:\n%s", substr, logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRaftAuth_MutualSuccess(t *testing.T) {
	base := newPipeStreamLayer()
	layer := NewAuthenticatedStreamLayer(base, testSecret, testCluster)

	srvConn, srvErr, cliConn, cliErr := handshakePair(t, layer, layer)
	if srvErr != nil || cliErr != nil {
		t.Fatalf("handshake failed: server=%v client=%v", srvErr, cliErr)
	}
	defer srvConn.Close()
	defer cliConn.Close()

	// CRITICAL (B1): the very first bytes written after the handshake must
	// reach the peer intact — the handshake must not over-read into the raft
	// stream. Write a sentinel from client to server and read it back.
	want := []byte{0x00, 0xDE, 0xAD, 0xBE, 0xEF}
	go func() { cliConn.Write(want) }()
	got := make([]byte, len(want))
	srvConn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(srvConn, got); err != nil {
		t.Fatalf("post-handshake read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("post-handshake stream desync: got %x want %x", got, want)
	}
}

func TestRaftAuth_WrongSecretClientRejected(t *testing.T) {
	base := newPipeStreamLayer()
	server, logs := newLoggedLayer(base, testSecret, testCluster)
	client := NewAuthenticatedStreamLayer(base, "WRONG-SECRET", testCluster)

	expectRejected(t, base, server, client)
	// A wrong MAC is the security signal: Warn, with the peer's address.
	waitForLog(t, logs, `"level":"warn"`)
	waitForLog(t, logs, "failed shared-secret authentication")
}

func TestRaftAuth_WrongSecretServerRejected(t *testing.T) {
	base := newPipeStreamLayer()
	server, _ := newLoggedLayer(base, "WRONG-SECRET", testCluster)
	client := NewAuthenticatedStreamLayer(base, testSecret, testCluster)

	expectRejected(t, base, server, client)
}

func TestRaftAuth_WrongClusterNameRejected(t *testing.T) {
	base := newPipeStreamLayer()
	server, logs := newLoggedLayer(base, testSecret, "cluster-a")
	client := NewAuthenticatedStreamLayer(base, testSecret, "cluster-b")

	expectRejected(t, base, server, client)
	waitForLog(t, logs, "failed shared-secret authentication")
}

func TestRaftAuth_TruncatedHandshakeRejected(t *testing.T) {
	// A peer that connects and immediately closes must not authenticate, and
	// is routine (a dialer that gave up), so it is logged at Debug, not Warn.
	base := newPipeStreamLayer()
	server, logs := newLoggedLayer(base, testSecret, testCluster)
	defer base.Close()
	defer server.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		if c, err := server.Accept(); err == nil {
			accepted <- c
		}
	}()
	client, srv := net.Pipe()
	base.accept <- srv
	client.Close() // hang up before sending anything

	waitForLog(t, logs, "dropped during authentication handshake")
	select {
	case c := <-accepted:
		c.Close()
		t.Fatal("server authenticated a peer that sent no handshake")
	default:
	}
	if strings.Contains(logs.String(), `"level":"warn"`) {
		t.Fatalf("a peer that hung up was logged at Warn:\n%s", logs.String())
	}
}

func TestRaftAuth_AuthFailureWarnIsRateLimited(t *testing.T) {
	base := newPipeStreamLayer()
	server, logs := newLoggedLayer(base, testSecret, testCluster)
	client := NewAuthenticatedStreamLayer(base, "WRONG-SECRET", testCluster)
	defer base.Close()
	defer server.Close()
	go func() {
		for {
			c, err := server.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	for i := 0; i < 5; i++ {
		if c, err := client.Dial("ignored", time.Second); err == nil {
			c.Close()
			t.Fatal("wrong-secret client authenticated")
		}
	}
	waitForLog(t, logs, "failed shared-secret authentication")
	deadline := time.Now().Add(2 * time.Second)
	for server.suppressedAuthFailures.Load() != 4 {
		if time.Now().After(deadline) {
			t.Fatalf("suppressed = %d, want 4", server.suppressedAuthFailures.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := strings.Count(logs.String(), "failed shared-secret authentication"); n != 1 {
		t.Fatalf("logged %d auth-failure warnings within the interval, want 1", n)
	}
}

func TestRaftAuth_CloseUnblocksAccept(t *testing.T) {
	base := newPipeStreamLayer()
	layer := NewAuthenticatedStreamLayer(base, testSecret, testCluster)
	errCh := make(chan error, 1)
	go func() {
		_, err := layer.Accept()
		errCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	layer.Close()
	select {
	case err := <-errCh:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Accept after Close = %v, want net.ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Accept")
	}
	// Close before (another) Accept: still returns, never blocks.
	if _, err := layer.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Accept on a closed layer = %v, want net.ErrClosed", err)
	}
}

func TestRaftAuth_ReplayedResponseRejected(t *testing.T) {
	// Capture a legitimate client's response to one server challenge, then
	// present it to a FRESH server challenge. Because the challenge nonce is
	// generated per-connection by the verifier, the captured MAC must not
	// authenticate against the new challenge.
	layer := NewAuthenticatedStreamLayer(newPipeStreamLayer(), testSecret, testCluster)

	// First connection: capture what a real client sends.
	c1client, c1server := net.Pipe()
	captured := make([]byte, raftAuthMACLen+raftAuthNonceLen)
	go func() {
		// Act as the server: send a known nonce, read the client's reply.
		c1server.SetDeadline(time.Now().Add(time.Second))
		nonceS := make([]byte, raftAuthNonceLen)
		c1server.Write(nonceS) // all-zero challenge (deterministic)
		io.ReadFull(c1server, captured)
		c1server.Close()
	}()
	// Real client answers the all-zero challenge.
	_ = layer.clientHandshake(c1client)
	c1client.Close()

	// Second connection: real server with a FRESH random challenge; replay the
	// captured client bytes.
	c2client, c2server := net.Pipe()
	srvErrCh := make(chan error, 1)
	go func() {
		c, err := layer.Accept2(c2server)
		if c != nil {
			c.Close()
		}
		srvErrCh <- err
	}()
	// Replay captured bytes as the "client".
	go func() {
		// Discard the server's real (random) challenge, then replay.
		io.CopyN(io.Discard, c2client, raftAuthNonceLen)
		c2client.Write(captured)
	}()
	if err := <-srvErrCh; err == nil {
		t.Fatal("replayed response authenticated against a fresh challenge")
	}
	c2client.Close()
}

// Accept2 runs the server handshake against a caller-supplied conn — a test
// seam so the replay test can drive a raw pipe without the pipeStreamLayer
// Accept channel.
func (a *AuthenticatedStreamLayer) Accept2(conn net.Conn) (net.Conn, error) {
	if err := a.serverHandshake(conn); err != nil {
		return nil, err
	}
	return conn, nil
}

func TestRaftAuth_DeadlineBelowHeartbeat(t *testing.T) {
	// Guard the constant so a future edit can't push the handshake deadline
	// above the Raft heartbeat: the dial side runs inline in raft's per-peer
	// heartbeat goroutine, so a deadline past the heartbeat lets one slow peer
	// miss heartbeats (#671).
	if raftAuthDeadline >= 500*time.Millisecond {
		t.Fatalf("raftAuthDeadline %v must be below the 500ms default HeartbeatTimeout", raftAuthDeadline)
	}
}
