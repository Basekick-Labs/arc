package main

import (
	"context"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/rs/zerolog"
)

// debugPprofEnvVar is the env-var operators set to opt into the localhost
// pprof listener. Any value matching the isTruthy contract turns it on;
// everything else (including unset) leaves it off.
const debugPprofEnvVar = "ARC_DEBUG_PPROF"

// debugPprofAddrEnvVar lets operators override the bind address. Default is
// "127.0.0.1:6060". A non-loopback override (e.g. "0.0.0.0:6060") additionally
// requires debugPprofAllowNonLoopbackEnvVar=1 — see startDebugPprofIfEnabled
// for the rationale.
const debugPprofAddrEnvVar = "ARC_DEBUG_PPROF_ADDR"

// debugPprofAllowNonLoopbackEnvVar is the explicit second opt-in required
// before pprof binds to a non-loopback interface. Two env vars instead of
// one because "exposing pprof to the network" is a deliberate decision
// that should not happen by typo in the bind-address env var.
const debugPprofAllowNonLoopbackEnvVar = "ARC_DEBUG_PPROF_ALLOW_NON_LOOPBACK"

// debugPprofDefaultAddr is the loopback-only default. Picked over the
// Go-runtime convention `:6060` so a misconfigured firewall cannot make
// the endpoint reachable cross-host.
const debugPprofDefaultAddr = "127.0.0.1:6060"

// debugPprofReadHeaderTimeout caps how long a client can dribble headers
// in. Slowloris mitigation on the debug surface.
const debugPprofReadHeaderTimeout = 5 * time.Second

// debugPprofWriteTimeout caps how long the server will spend writing the
// response. pprof captures legitimately run long (`profile?seconds=N`),
// so the bound is intentionally generous — but finite, so a slow reader
// cannot pin a connection forever.
const debugPprofWriteTimeout = 10 * time.Minute

// debugPprofIdleTimeout closes keep-alive connections that the operator
// leaves open. 60 seconds is short enough to recycle file descriptors
// promptly but long enough that an interactive `curl` workflow doesn't
// fight the listener.
const debugPprofIdleTimeout = 60 * time.Second

// startDebugPprofIfEnabled starts an opt-in pprof HTTP server on a separate
// listener when ARC_DEBUG_PPROF is truthy. The listener is bound to
// localhost by default and is NEVER attached to the public Fiber app —
// that's the whole point of the gate. Closes audit finding #2 from
// 2026-05-19 (GHSA-j93g-rp6m-j32m).
//
// When the env var is unset or evaluates to false, this is a no-op: pprof
// handlers are not registered on this listener, no socket is opened, no
// goroutine is spawned. An attacker on the data port cannot reach
// `/debug/pprof/*` even in principle.
//
// Note on http.DefaultServeMux pollution: `import "net/http/pprof"` runs
// init() which DOES register handlers on http.DefaultServeMux — there is
// no way to opt out of that side effect. The private *http.ServeMux below
// protects THIS listener from collisions with the default mux; it does
// NOT undo the default-mux registration. Arc safely avoids the side
// effect because the binary contains zero callers of
// http.ListenAndServe(_, nil) or any other path that serves
// http.DefaultServeMux — verified at PR time. A future PR that introduces
// such a caller would inherit pprof handlers on the default mux; track
// that by greppping for `http.DefaultServeMux` and unnamed `http.Server`
// handler fields.
func startDebugPprofIfEnabled(coord *shutdown.Coordinator, logger zerolog.Logger) {
	// The caller (cmd/arc/main.go) passes logger.Get("debug-pprof"), which
	// already carries the component field. Don't re-attach it; zerolog
	// accumulates fields and emitting "component" twice is noise.
	if !isTruthy(os.Getenv(debugPprofEnvVar)) {
		return
	}

	addr := os.Getenv(debugPprofAddrEnvVar)
	if addr == "" {
		addr = debugPprofDefaultAddr
	}

	// Require a second opt-in for non-loopback exposure. "0.0.0.0:6060" is
	// a single typo away from the default; making it a deliberate two-step
	// matches the fail-closed precedent set by PR #442's sandbox.
	if !isLoopbackBindAddr(addr) && !isTruthy(os.Getenv(debugPprofAllowNonLoopbackEnvVar)) {
		logger.Error().
			Str("addr", addr).
			Msg(debugPprofEnvVar + "=1 with a non-loopback " + debugPprofAddrEnvVar + " requires " + debugPprofAllowNonLoopbackEnvVar + "=1; refusing to start pprof listener")
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{
		// Addr is intentionally not set — srv.Serve(ln) below uses the
		// listener's address, and a stale Addr field would mislead anyone
		// reading the struct.
		Handler:           mux,
		ReadHeaderTimeout: debugPprofReadHeaderTimeout,
		WriteTimeout:      debugPprofWriteTimeout,
		IdleTimeout:       debugPprofIdleTimeout,
	}

	// Bind the listener up-front so a port conflict surfaces as a startup
	// error rather than a goroutine that silently fails behind a log line.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error().Err(err).Str("addr", addr).Msg(debugPprofEnvVar + "=1 but failed to bind pprof listener; continuing without pprof")
		return
	}

	logLevel := logger.Warn()
	if !isLoopbackBindAddr(addr) {
		// Upgrade to Error so default alerting policies notice that pprof
		// is exposed cross-host on this node. The operator already opted
		// in via ARC_DEBUG_PPROF_ALLOW_NON_LOOPBACK so this isn't a
		// surprise — just a higher-visibility breadcrumb.
		logLevel = logger.Error()
	}
	logLevel.
		Str("addr", addr).
		Msg(debugPprofEnvVar + " is set — pprof endpoints are exposed on this address. Restrict access via firewall or unset " + debugPprofEnvVar + " in production.")

	go func() {
		// Belt-and-suspenders: close the listener when the goroutine exits.
		// srv.Serve(ln) closes ln on return, but if a future refactor swaps
		// Serve for something else this defer keeps the fd from leaking.
		// Also closes the race window where coord.Shutdown could fire
		// between net.Listen returning and Serve being entered — a tiny
		// window (microseconds), but `defer ln.Close()` makes it safe.
		defer ln.Close()
		// Serve returns http.ErrServerClosed on graceful Close/Shutdown.
		// Anything else is unexpected and worth logging.
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("pprof listener stopped unexpectedly")
		}
	}()

	// PriorityHTTPServer (10) — same band as the main HTTP server: stop
	// accepting new debug requests early in shutdown.
	//
	// Use srv.Close() (force-shutdown) instead of srv.Shutdown(ctx) here.
	// Shutdown waits for in-flight handlers to drain, and a long-running
	// pprof capture (e.g. /debug/pprof/profile?seconds=300) would hold the
	// coordinator's shared 30s budget for its full duration. Every
	// downstream hook (http-server, compaction, wal, database, storage,
	// auth, ...) would then be skipped when the budget exhausted — that's
	// a data-loss path on what the operator expected to be a graceful
	// exit. Force-close is the right tradeoff for a debug surface: pprof
	// captures aren't load-bearing, killing the connection immediately is
	// fine.
	coord.RegisterHook("debug-pprof", func(_ context.Context) error {
		return srv.Close()
	}, shutdown.PriorityHTTPServer)
}

// isLoopbackBindAddr returns true when addr binds only to a loopback
// interface. Used to decide whether ARC_DEBUG_PPROF_ALLOW_NON_LOOPBACK is
// required. Accepts every shape `net.Listen("tcp", addr)` accepts:
//
//	"127.0.0.1:6060"     → loopback
//	"localhost:6060"     → loopback (resolves to 127.0.0.1 / ::1)
//	"[::1]:6060"         → loopback
//	":6060"              → NOT loopback (binds all interfaces; rejected)
//	"0.0.0.0:6060"       → NOT loopback (binds all v4 interfaces)
//	"192.168.1.5:6060"   → NOT loopback
//
// On lookup failure (e.g. unresolvable name), treat as non-loopback so
// the second opt-in is required. Fail closed.
func isLoopbackBindAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "" {
		// ":6060" binds the wildcard address — not loopback.
		return false
	}
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	// Hostname that isn't "localhost" — could resolve to anything. Don't
	// trust the resolver here; require the explicit opt-in.
	return false
}

// isTruthy treats common affirmative env-var values as true. The set is
// deliberately small and case-insensitive on the canonical forms so
// operator config is forgiving without admitting weird shapes like
// "tRuE".
func isTruthy(s string) bool {
	switch strings.ToLower(s) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
