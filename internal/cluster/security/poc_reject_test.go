package security

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

// TestRaftForgePOCRejected reproduces exactly what cmd/raftforge-poc does — a
// raw TCP dial that writes an rpcType byte + a msgpack AppendEntriesRequest,
// without performing any handshake — and asserts the AuthenticatedStreamLayer
// rejects it, so no forged entry ever reaches raft. Since #1431 a rejected
// connection is closed inside the layer rather than returned as an Accept
// error, so the test asserts all three halves: Accept never yields the PoC
// connection, the server hangs up on it, and a legitimate peer still gets
// through afterwards (a layer that accepted nothing would pass the first two).
func TestRaftForgePOCRejected(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Wrap a real listener in the auth layer (via a tiny adapter).
	base := &listenerStreamLayer{ln: ln}
	layer := NewAuthenticatedStreamLayer(base, testSecret, testCluster)
	defer layer.Close()

	accepted := make(chan net.Conn, 2)
	go func() {
		for {
			c, err := layer.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()

	// The PoC: raw dial, write rpcType + AppendEntries. No handshake.
	conn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	// rpcAppendEntries = 0
	conn.Write([]byte{0})
	enc := codec.NewEncoder(conn, &codec.MsgpackHandle{})
	enc.Encode(&struct {
		Term   uint64
		Leader []byte
	}{Term: 99, Leader: []byte("evil")})

	// The server sends its challenge, rejects the garbage "response", and
	// hangs up: everything after the nonce is EOF (or a reset).
	if _, err := io.ReadFull(conn, make([]byte, raftAuthNonceLen)); err != nil {
		t.Fatalf("reading server challenge: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("server kept the unauthenticated PoC connection open")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("server neither rejected nor closed the PoC connection")
	}

	// A legitimate peer is still accepted, and it is the ONLY conn Accept
	// ever returns.
	good, err := layer.Dial(raft.ServerAddress(ln.Addr().String()), time.Second)
	if err != nil {
		t.Fatalf("legitimate dial after the PoC: %v", err)
	}
	defer good.Close()
	select {
	case c := <-accepted:
		defer c.Close()
		if c.RemoteAddr().String() != good.LocalAddr().String() {
			t.Fatalf("VULNERABLE: Accept returned %v, the PoC connection is %v", c.RemoteAddr(), conn.LocalAddr())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("legitimate connection never reached Accept")
	}
	select {
	case c := <-accepted:
		c.Close()
		t.Fatal("VULNERABLE: Accept returned a second connection")
	case <-time.After(100 * time.Millisecond):
	}
}

type listenerStreamLayer struct{ ln net.Listener }

func (l *listenerStreamLayer) Accept() (net.Conn, error) { return l.ln.Accept() }
func (l *listenerStreamLayer) Close() error              { return l.ln.Close() }
func (l *listenerStreamLayer) Addr() net.Addr            { return l.ln.Addr() }
func (l *listenerStreamLayer) Dial(a raft.ServerAddress, t time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(a), t)
}
