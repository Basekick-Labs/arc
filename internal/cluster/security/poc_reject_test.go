package security

import (
	"net"
	"testing"
	"time"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

// TestRaftForgePOCRejected reproduces exactly what cmd/raftforge-poc does — a
// raw TCP dial that writes an rpcType byte + a msgpack AppendEntriesRequest,
// without performing any handshake — and asserts the AuthenticatedStreamLayer
// rejects it at Accept, so no forged entry ever reaches raft.
func TestRaftForgePOCRejected(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Wrap a real listener in the auth layer (via a tiny adapter).
	base := &listenerStreamLayer{ln: ln}
	layer := NewAuthenticatedStreamLayer(base, testSecret, testCluster)

	acceptErr := make(chan error, 1)
	go func() {
		c, err := layer.Accept()
		if c != nil {
			c.Close()
		}
		acceptErr <- err
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

	select {
	case err := <-acceptErr:
		if err == nil {
			t.Fatal("VULNERABLE: auth layer accepted the raw PoC connection")
		}
		t.Logf("PoC rejected as expected: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Accept did not return; handshake may be hanging")
	}
}

type listenerStreamLayer struct{ ln net.Listener }

func (l *listenerStreamLayer) Accept() (net.Conn, error) { return l.ln.Accept() }
func (l *listenerStreamLayer) Close() error              { return l.ln.Close() }
func (l *listenerStreamLayer) Addr() net.Addr            { return l.ln.Addr() }
func (l *listenerStreamLayer) Dial(a raft.ServerAddress, t time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(a), t)
}
