package security

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

const testSecret = "test-cluster-secret-32-bytes-long!"
const testCluster = "test-cluster"

// pipeStreamLayer is an in-memory raft.StreamLayer backed by net.Pipe, so the
// handshake can be exercised without binding a real port. Accept returns the
// server end of the most recent Dial.
type pipeStreamLayer struct {
	accept chan net.Conn
}

func newPipeStreamLayer() *pipeStreamLayer { return &pipeStreamLayer{accept: make(chan net.Conn, 1)} }

func (p *pipeStreamLayer) Accept() (net.Conn, error) { return <-p.accept, nil }
func (p *pipeStreamLayer) Close() error              { return nil }
func (p *pipeStreamLayer) Addr() net.Addr            { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1} }
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
	server := NewAuthenticatedStreamLayer(base, testSecret, testCluster)
	client := NewAuthenticatedStreamLayer(base, "WRONG-SECRET", testCluster)

	srvConn, srvErr, _, _ := handshakePair(t, server, client)
	if srvErr == nil {
		srvConn.Close()
		t.Fatal("server accepted a client with the wrong secret")
	}
}

func TestRaftAuth_WrongSecretServerRejected(t *testing.T) {
	base := newPipeStreamLayer()
	server := NewAuthenticatedStreamLayer(base, "WRONG-SECRET", testCluster)
	client := NewAuthenticatedStreamLayer(base, testSecret, testCluster)

	_, _, cliConn, cliErr := handshakePair(t, server, client)
	if cliErr == nil {
		cliConn.Close()
		t.Fatal("client accepted a server with the wrong secret")
	}
}

func TestRaftAuth_WrongClusterNameRejected(t *testing.T) {
	base := newPipeStreamLayer()
	server := NewAuthenticatedStreamLayer(base, testSecret, "cluster-a")
	client := NewAuthenticatedStreamLayer(base, testSecret, "cluster-b")

	srvConn, srvErr, _, _ := handshakePair(t, server, client)
	if srvErr == nil {
		srvConn.Close()
		t.Fatal("server accepted a peer bound to a different cluster name")
	}
}

func TestRaftAuth_TruncatedHandshakeRejected(t *testing.T) {
	// A peer that connects and immediately closes must not authenticate.
	base := newPipeStreamLayer()
	server := NewAuthenticatedStreamLayer(base, testSecret, testCluster)

	srvCh := make(chan error, 1)
	go func() {
		c, err := server.Accept()
		if c != nil {
			c.Close()
		}
		srvCh <- err
	}()
	client, srv := net.Pipe()
	base.accept <- srv
	client.Close() // hang up before sending anything

	if err := <-srvCh; err == nil {
		t.Fatal("server authenticated a peer that sent no handshake")
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
	// above the Raft heartbeat and reintroduce the serial-accept election-storm
	// risk.
	if raftAuthDeadline >= 500*time.Millisecond {
		t.Fatalf("raftAuthDeadline %v must be below the 500ms default HeartbeatTimeout", raftAuthDeadline)
	}
}
