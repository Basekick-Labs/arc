package api

import (
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// TestServer_PprofNotRegisteredOnPublicApp is the regression test for audit
// finding #2 (GHSA-j93g-rp6m-j32m): the previous default mounted
// `/debug/pprof/*` on the public Fiber app and added `/debug/pprof` to
// PublicPrefixes, so any network-reachable caller could fetch heap dumps
// or pin a CPU core via `/debug/pprof/profile?seconds=30`.
//
// After the fix, the public Fiber app must NEVER serve `/debug/pprof/*`.
// pprof is opt-in via ARC_DEBUG_PPROF=1 and binds to a separate
// localhost-only listener — that's tested in cmd/arc/debug_pprof_test.go
// (the binary's integration surface). This test pins the library
// invariant: NewServer's app does not mount pprof handlers.
//
// Probed paths cover the surface gofiber/fiber/v2/middleware/pprof used
// to register: index, heap, profile, goroutine, allocs, mutex, block,
// trace, cmdline, symbol. Each must 404 — Fiber returns 404 for any
// route the app doesn't know about, including before the auth
// middleware fires, which is the security guarantee we want.
func TestServer_PprofNotRegisteredOnPublicApp(t *testing.T) {
	t.Parallel()
	srv := NewServer(DefaultServerConfig(), zerolog.Nop())
	app := srv.GetApp()

	pprofPaths := []string{
		"/debug/pprof/",
		"/debug/pprof/heap",
		"/debug/pprof/profile",
		"/debug/pprof/profile?seconds=1",
		"/debug/pprof/goroutine",
		"/debug/pprof/allocs",
		"/debug/pprof/mutex",
		"/debug/pprof/block",
		"/debug/pprof/threadcreate",
		"/debug/pprof/trace",
		"/debug/pprof/cmdline",
		"/debug/pprof/symbol",
	}
	for _, p := range pprofPaths {
		t.Run(p, func(t *testing.T) {
			req := httptest.NewRequest("GET", p, nil)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			// Anything other than 404 means pprof is reachable on the
			// public app — that's the bug. We accept 404; we reject 200,
			// 500, 302, etc. (Fiber's default not-found returns 404.)
			if resp.StatusCode != fiber.StatusNotFound {
				t.Errorf("path %q: expected 404 (pprof must not be mounted), got %d",
					p, resp.StatusCode)
			}
		})
	}
}
