package originstream

import (
	"bytes"
	"net"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

type addrConn struct {
	net.Conn
	remote net.Addr
}

func (c *addrConn) RemoteAddr() net.Addr { return c.remote }

func tcpAddr(s string) net.Addr {
	a, err := net.ResolveTCPAddr("tcp", s)
	if err != nil {
		panic(err)
	}
	return a
}

// A session replaced by a connection from a different address is still
// replaced (the primary cannot tell a live old session from a half-open
// one), and the move is logged. A reconnect from the same host on a new
// port is not.
func TestAcceptLogsSessionMovingAddress(t *testing.T) {
	for _, tc := range []struct {
		name     string
		from, to string
		wantWarn bool
	}{
		{"different host", "10.0.0.1:5000", "10.0.0.2:6000", true},
		{"same host, new port", "10.0.0.1:5000", "10.0.0.1:6000", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			srv := NewServer(0, func() *Floors { return nil }, zerolog.New(&buf))
			srv.Start()
			defer srv.Stop()
			a1, b1 := net.Pipe()
			defer b1.Close()
			a2, b2 := net.Pipe()
			defer b2.Close()
			srv.Accept("s", &addrConn{Conn: a1, remote: tcpAddr(tc.from)}, testKey)
			srv.Accept("s", &addrConn{Conn: a2, remote: tcpAddr(tc.to)}, testKey)

			srv.mu.Lock()
			cur := srv.sessions["s"]
			srv.mu.Unlock()
			if cur == nil || cur.conn.RemoteAddr().String() != tcpAddr(tc.to).String() {
				t.Fatalf("new connection did not replace the session")
			}
			got := strings.Contains(buf.String(), "replaced by a connection from a different address")
			if got != tc.wantWarn {
				t.Fatalf("warn logged = %v, want %v; log:\n%s", got, tc.wantWarn, buf.String())
			}
		})
	}
}
