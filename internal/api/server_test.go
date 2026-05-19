package api

import (
	"net"
	"strconv"
	"testing"

	"github.com/rs/zerolog"
)

// TestListenAddrForHost pins the listener-address construction shape. The
// production listener uses net.JoinHostPort(s.host, strconv.Itoa(s.port))
// at server.go's Start path; this test asserts the addresses produced
// for every shape an operator might configure.
//
// The "" case is load-bearing for backward compatibility: it must
// produce ":<port>" (byte-identical to the historical fmt.Sprintf(":%d",
// port)) so existing deployments upgrade with zero behavioral change
// on the listener — specifically preserving Linux dual-stack
// (IPv4 + IPv6 via IPv4-mapped addresses) binding semantics.
func TestListenAddrForHost(t *testing.T) {
	const port = 8000
	cases := []struct {
		name string
		host string
		want string
	}{
		{
			name: "empty preserves historical wildcard (dual-stack)",
			host: "",
			want: ":8000",
		},
		{
			name: "explicit ipv4 wildcard binds v4 only",
			host: "0.0.0.0",
			want: "0.0.0.0:8000",
		},
		{
			name: "explicit ipv6 wildcard with bracketing",
			host: "::",
			want: "[::]:8000",
		},
		{
			name: "ipv4 loopback",
			host: "127.0.0.1",
			want: "127.0.0.1:8000",
		},
		{
			name: "ipv6 loopback bracketed correctly",
			host: "::1",
			want: "[::1]:8000",
		},
		{
			name: "named host passed through",
			host: "arc.internal",
			want: "arc.internal:8000",
		},
		{
			name: "specific ipv4 interface",
			host: "192.0.2.10",
			want: "192.0.2.10:8000",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := net.JoinHostPort(tc.host, strconv.Itoa(port))
			if got != tc.want {
				t.Errorf("net.JoinHostPort(%q, %q) = %q; want %q", tc.host, strconv.Itoa(port), got, tc.want)
			}
		})
	}
}

// TestServerCapturesHostFromConfig pins that NewServer threads
// ServerConfig.Host through to the Server struct's host field —
// without this, the listener at Start() would always see the zero
// value and silently fall back to wildcard regardless of what the
// operator configured.
func TestServerCapturesHostFromConfig(t *testing.T) {
	cases := []struct {
		name string
		host string
	}{
		{"empty default", ""},
		{"ipv4 loopback", "127.0.0.1"},
		{"ipv6 loopback", "::1"},
		{"explicit v4 wildcard", "0.0.0.0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &ServerConfig{
				Host:           tc.host,
				Port:           0,
				MaxPayloadSize: 1024 * 1024,
			}
			// NewServer's logger argument is required but we don't
			// care about its output here — zerolog.Nop() is the
			// convention used by every other test in this package.
			s := NewServer(cfg, zerolog.Nop())
			if s.host != tc.host {
				t.Errorf("Server.host = %q; want %q (NewServer dropped Host on the floor)", s.host, tc.host)
			}
		})
	}
}
