package api

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// TestListenAddrForHost pins the listener-address construction shape
// the Start path uses. Every shape an operator might configure is
// exercised here.
//
// The "" case is load-bearing for backward compatibility: it must
// produce ":<port>" (byte-identical to the historical fmt.Sprintf(":%d",
// port)) so existing deployments upgrade with zero behavioral change
// on the listener — specifically preserving Linux dual-stack
// (IPv4 + IPv6 via IPv4-mapped addresses) binding semantics.
//
// The bracketed-IPv6 case is the defensive strip: a user who follows
// docs that show bracketed addresses (or copies one from `ss`/`netstat`
// output) must not produce an invalid "[[::1]]:8000".
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
			name: "already-bracketed ipv6 is stripped before joining",
			host: "[::1]",
			want: "[::1]:8000",
		},
		{
			name: "already-bracketed ipv6 wildcard is stripped before joining",
			host: "[::]",
			want: "[::]:8000",
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
			_, got := ListenAddr(tc.host, port)
			if got != tc.want {
				t.Errorf("ListenAddr(%q, %d) = %q; want %q", tc.host, port, got, tc.want)
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

// TestReadyHandler_StartsNotReady pins the readiness contract: a freshly
// constructed Server returns /ready=503 until MarkReady() is called. This
// is load-bearing for Pattern 2 shared-storage multi-writer mode — the
// load balancer must NOT route writes to a writer that hasn't finished
// WAL recovery (cmd/arc/main.go:701-724), and the same gate also protects
// single-writer deployments behind Kubernetes readiness probes from
// receiving traffic during startup.
//
// See internal/api/server.go::readyHandler for the contract.
func TestReadyHandler_StartsNotReady(t *testing.T) {
	s := NewServer(&ServerConfig{Port: 0, MaxPayloadSize: 1024}, zerolog.Nop())
	s.RegisterRoutes()

	req := httptest.NewRequest("GET", "/ready", nil)
	resp, err := s.app.Test(req)
	if err != nil {
		t.Fatalf("/ready Test failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Errorf("freshly constructed Server /ready status = %d; want 503 (startup not complete)", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "not_ready") {
		t.Errorf("/ready body before MarkReady() = %q; want substring \"not_ready\"", string(body))
	}
}

// TestReadyHandler_MarkReadyFlipsTo200 pins the flip from 503 to 200 once
// MarkReady() is called. main.go invokes this after WAL recovery completes
// and all background work is initialised — the load balancer then sees
// /ready=200 and starts routing traffic.
func TestReadyHandler_MarkReadyFlipsTo200(t *testing.T) {
	s := NewServer(&ServerConfig{Port: 0, MaxPayloadSize: 1024}, zerolog.Nop())
	s.RegisterRoutes()
	s.MarkReady()

	req := httptest.NewRequest("GET", "/ready", nil)
	resp, err := s.app.Test(req)
	if err != nil {
		t.Fatalf("/ready Test failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("after MarkReady() /ready status = %d; want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "\"status\":\"ready\"") {
		t.Errorf("/ready body after MarkReady() = %q; want substring \"status\":\"ready\"", string(body))
	}
}

// TestReadyHandler_MarkNotReadyDrainsLB pins the graceful-shutdown
// contract: MarkNotReady() must flip /ready back to 503 so the load
// balancer drains the node BEFORE the HTTP listener actually closes.
// main.go registers this as priority 5 (lower than PriorityHTTPServer=10)
// so it fires first in the shutdown sequence.
func TestReadyHandler_MarkNotReadyDrainsLB(t *testing.T) {
	s := NewServer(&ServerConfig{Port: 0, MaxPayloadSize: 1024}, zerolog.Nop())
	s.RegisterRoutes()

	// Simulate the normal startup → traffic → graceful-shutdown sequence.
	s.MarkReady()
	s.MarkNotReady()

	req := httptest.NewRequest("GET", "/ready", nil)
	resp, err := s.app.Test(req)
	if err != nil {
		t.Fatalf("/ready Test failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Errorf("after MarkNotReady() /ready status = %d; want 503 (drain signal)", resp.StatusCode)
	}
}

// TestHealthHandler_AlwaysOK pins that /health (liveness) stays 200
// regardless of the ready flag. Operators check /health for "is the
// process alive" and /ready for "should requests go here." Conflating
// the two would mean a graceful shutdown that flips /ready=503 would
// also flip /health=503, causing Kubernetes liveness probes to kill
// the pod mid-drain.
func TestHealthHandler_AlwaysOK(t *testing.T) {
	s := NewServer(&ServerConfig{Port: 0, MaxPayloadSize: 1024}, zerolog.Nop())
	s.RegisterRoutes()

	// Before MarkReady (mid-startup): /health is still 200.
	req := httptest.NewRequest("GET", "/health", nil)
	resp, err := s.app.Test(req)
	if err != nil {
		t.Fatalf("/health Test (pre-ready) failed: %v", err)
	}
	// Explicit Close instead of defer: resp is reassigned below.
	if resp.StatusCode != 200 {
		t.Errorf("pre-MarkReady /health status = %d; want 200 (liveness != readiness)", resp.StatusCode)
	}
	resp.Body.Close()

	// After MarkNotReady (graceful shutdown): /health is still 200.
	s.MarkReady()
	s.MarkNotReady()
	req = httptest.NewRequest("GET", "/health", nil)
	resp, err = s.app.Test(req)
	if err != nil {
		t.Fatalf("/health Test (post-drain) failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("post-MarkNotReady /health status = %d; want 200 (liveness != readiness)", resp.StatusCode)
	}
}
