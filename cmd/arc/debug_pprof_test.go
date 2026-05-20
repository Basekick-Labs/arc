package main

import (
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/rs/zerolog"
)

// pickFreeLocalPort asks the kernel for an ephemeral port on 127.0.0.1
// and returns it as a "127.0.0.1:NNNN" string. Two-step ask-then-close
// gives the kernel a few microseconds to reuse the port; we accept the
// tiny race window because the alternative (hard-coding a port) makes
// the test flake on shared CI hosts.
func pickFreeLocalPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("kernel could not allocate a free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// TestStartDebugPprofIfEnabled_NoopWhenDisabled asserts the security
// invariant: when ARC_DEBUG_PPROF is unset (or evaluates to false), the
// helper opens no listener, registers no shutdown hook, and an attacker
// cannot reach pprof in principle.
func TestStartDebugPprofIfEnabled_NoopWhenDisabled(t *testing.T) {
	t.Setenv(debugPprofEnvVar, "")
	addr := pickFreeLocalPort(t)
	t.Setenv(debugPprofAddrEnvVar, addr)

	coord := shutdown.New(5*time.Second, zerolog.Nop())
	startDebugPprofIfEnabled(coord, zerolog.Nop())

	// If a listener were running, this Dial would succeed. We give it a
	// short timeout so the test isn't slowed by the inevitable connection
	// refused on a free port.
	c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err == nil {
		_ = c.Close()
		t.Errorf("listener accepted a connection on %s but ARC_DEBUG_PPROF is unset; pprof must be a true no-op when disabled", addr)
	}
}

// TestStartDebugPprofIfEnabled_BindsAndServes asserts that with the env
// var set, pprof comes up on the configured address, /debug/pprof/ is
// reachable, and a non-pprof path on the same listener 404s (the helper
// uses a private *http.ServeMux, so it must not respond to arbitrary
// URLs).
func TestStartDebugPprofIfEnabled_BindsAndServes(t *testing.T) {
	addr := pickFreeLocalPort(t)
	t.Setenv(debugPprofEnvVar, "1")
	t.Setenv(debugPprofAddrEnvVar, addr)

	coord := shutdown.New(5*time.Second, zerolog.Nop())
	startDebugPprofIfEnabled(coord, zerolog.Nop())

	// Wait until the listener is actually accepting. The goroutine inside
	// startDebugPprofIfEnabled is async; without this poll the test is
	// flaky on a busy CI host.
	client := &http.Client{Timeout: 2 * time.Second}
	url := "http://" + addr + "/debug/pprof/"
	deadline := time.Now().Add(2 * time.Second)
	var resp *http.Response
	var err error
	for time.Now().Before(deadline) {
		resp, err = client.Get(url)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("pprof listener never came up at %s: %v", addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/debug/pprof/ on %s: expected 200, got %d", addr, resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	// The pprof index page lists the available profiles by name. If we
	// got something else, the wrong handler is registered.
	if !strings.Contains(string(body), "Profile Descriptions") && !strings.Contains(string(body), "pprof") {
		t.Errorf("/debug/pprof/ body does not look like the pprof index page: %q", string(body[:min(200, len(body))]))
	}

	// The four non-runtime-profile endpoints (cmdline, profile, symbol,
	// trace) MUST be reachable. These do NOT live in the runtime/pprof
	// Lookup table, so pprof.Index alone would return 404 "Unknown
	// profile" for them — they need explicit HandleFunc registrations
	// next to pprof.Index. This sub-test prevents a future "simplification"
	// that drops the explicit registrations (e.g. PR #443 review G3
	// suggested it; empirically refuted).
	//
	// Skip /debug/pprof/profile and /debug/pprof/trace because those
	// actively capture data over seconds; the test would either take
	// 30+s or have to fight the seconds query param. Cmdline and symbol
	// are O(1) and prove the registrations work.
	for _, p := range []string{"/debug/pprof/cmdline", "/debug/pprof/symbol"} {
		r, err := client.Get("http://" + addr + p)
		if err != nil {
			t.Errorf("GET %s on pprof listener: %v", p, err)
			continue
		}
		_ = r.Body.Close()
		if r.StatusCode == http.StatusNotFound {
			t.Errorf("GET %s on pprof listener: 404 — pprof.Index alone does not cover this endpoint; explicit HandleFunc(%q, ...) registration is required", p, p)
		}
	}

	// Non-pprof path must NOT be served by this listener: we registered a
	// fresh *http.ServeMux, not the default mux. A request for
	// "/something-else" must 404, proving we did not leak handlers onto
	// http.DefaultServeMux.
	resp2, err := client.Get("http://" + addr + "/")
	if err != nil {
		t.Fatalf("GET / on pprof listener: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("GET / on pprof listener: expected 404 (fresh ServeMux must not serve arbitrary paths), got %d", resp2.StatusCode)
	}

	// Graceful shutdown closes the listener and the goroutine exits. The
	// coordinator carries its own timeout (set at New) so Shutdown takes
	// no args.
	if err := coord.Shutdown(); err != nil {
		t.Errorf("pprof listener shutdown returned error: %v", err)
	}

	// After shutdown, the same port should no longer accept connections.
	c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err == nil {
		_ = c.Close()
		t.Errorf("pprof listener at %s still accepts connections after shutdown", addr)
	}
}

// TestIsTruthy pins the env-var truthiness contract. The matcher is
// case-insensitive across the canonical forms (1/true/yes/on); anything
// else — including numeric-other-than-1 — is false.
func TestIsTruthy(t *testing.T) {
	t.Parallel()
	truthy := []string{
		"1", "true", "TRUE", "True", "tRuE",
		"yes", "YES", "Yes",
		"on", "ON", "On",
	}
	falsy := []string{
		"", "0", "false", "no", "off",
		"FALSE", "No", "Off",
		"anything-else", "2", "enabled", "y", "t",
	}
	for _, s := range truthy {
		if !isTruthy(s) {
			t.Errorf("isTruthy(%q) = false, want true", s)
		}
	}
	for _, s := range falsy {
		if isTruthy(s) {
			t.Errorf("isTruthy(%q) = true, want false", s)
		}
	}
}

// TestIsLoopbackBindAddr pins the loopback-detection contract. Drives the
// debugPprofAllowNonLoopbackEnvVar fail-closed branch in
// startDebugPprofIfEnabled.
func TestIsLoopbackBindAddr(t *testing.T) {
	t.Parallel()
	loopback := []string{
		"127.0.0.1:6060",
		"localhost:6060",
		"[::1]:6060",
		"127.0.0.5:6060",
	}
	nonLoopback := []string{
		":6060",                // wildcard
		"0.0.0.0:6060",         // v4 wildcard
		"[::]:6060",            // v6 wildcard
		"192.168.1.5:6060",     // LAN
		"10.0.0.1:6060",        // RFC 1918
		"example.com:6060",     // unresolved hostname — fail-closed
		"not-a-valid-addr",     // SplitHostPort fails — fail-closed
	}
	for _, s := range loopback {
		if !isLoopbackBindAddr(s) {
			t.Errorf("isLoopbackBindAddr(%q) = false, want true", s)
		}
	}
	for _, s := range nonLoopback {
		if isLoopbackBindAddr(s) {
			t.Errorf("isLoopbackBindAddr(%q) = true, want false", s)
		}
	}
}
