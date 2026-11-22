package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/audit"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func noop(c *fiber.Ctx) error     { return c.SendStatus(fiber.StatusOK) }
func passAuth(c *fiber.Ctx) error { return c.Next() }

// newCoverageApp builds the stack the way main does: public routes, then the
// auth middleware, then audit, then the logs and metrics routes and the admin
// and API routes. misorder registers the token routes before audit, as main
// did until #1630.
func newCoverageApp(misorder bool, audits int) (*fiber.App, fiber.Handler) {
	app := fiber.New()
	mw := audit.Middleware(nil, false) // never invoked: identity is all the check needs
	app.Get("/health", noop)
	app.Get("/ready/write", noop)
	app.Use(passAuth)
	if misorder {
		app.Post("/api/v1/auth/tokens", noop)
		app.Post("/api/v1/auth/tokens/:id/revoke", noop)
	}
	for i := 0; i < audits; i++ {
		app.Use(mw)
	}
	app.Get("/api/v1/logs", noop)
	app.Get("/metrics", noop)
	app.Get("/api/v1/metrics/timeseries/:type", noop)
	if !misorder {
		app.Post("/api/v1/auth/tokens", noop)
		app.Post("/api/v1/auth/tokens/:id/revoke", noop)
	}
	rbac := app.Group("/api/v1/rbac")
	rbac.Use(passAuth)
	rbac.Post("/organizations", noop)
	rbac.Patch("/teams/:id", noop)
	app.Post("/api/v1/write/msgpack", noop)
	return app, mw
}

func TestAuditCoverageAcceptsMainsOrder(t *testing.T) {
	for _, metricsPublic := range []bool{false, true} {
		routes, prefixes := authPublicPaths(metricsPublic)
		app, mw := newCoverageApp(false, 1)
		cov := auditCoverageOf(app, mw, routes, prefixes)
		if cov.unreadable {
			t.Fatal("Fiber's route layout could not be read: the check would be skipped in production")
		}
		if len(cov.gaps) != 0 || cov.maxEntries != 1 || len(cov.missing) != 0 {
			t.Fatalf("metricsPublic %v: coverage = %+v, want no gaps and exactly one audit entry", metricsPublic, cov)
		}
	}
}

// Metrics routes registered ahead of audit while they need a token are
// reported: that order would also have put them ahead of the auth middleware
// (GHSA-m3qr-fvp4-78xj).
func TestAuditCoverageReportsMetricsAheadOfAudit(t *testing.T) {
	routes, prefixes := authPublicPaths(false)
	app := fiber.New()
	mw := audit.Middleware(nil, false)
	app.Get("/metrics", noop)
	app.Use(passAuth)
	app.Use(mw)
	cov := auditCoverageOf(app, mw, routes, prefixes)
	if strings.Join(cov.gaps, ",") != "GET /metrics" {
		t.Fatalf("gaps = %v, want GET /metrics", cov.gaps)
	}
	// Public, the same order is not a gap.
	routes, prefixes = authPublicPaths(true)
	if cov := auditCoverageOf(app, mw, routes, prefixes); len(cov.gaps) != 0 {
		t.Fatalf("gaps with metrics public = %v", cov.gaps)
	}
}

// The #1630 shape: admin routes ahead of audit are reported, by method and
// path. Public routes ahead of it are not.
func TestAuditCoverageReportsRoutesAheadOfAudit(t *testing.T) {
	routes, prefixes := authPublicPaths(false)
	app, mw := newCoverageApp(true, 1)
	cov := auditCoverageOf(app, mw, routes, prefixes)
	want := []string{"POST /api/v1/auth/tokens", "POST /api/v1/auth/tokens/:id/revoke"}
	if strings.Join(cov.gaps, ",") != strings.Join(want, ",") {
		t.Fatalf("gaps = %v, want %v", cov.gaps, want)
	}

	var buf bytes.Buffer
	checkAuditCoverage(app, mw, routes, prefixes, zerolog.New(&buf))
	if out := buf.String(); !strings.Contains(out, `"level":"error"`) || !strings.Contains(out, "POST /api/v1/auth/tokens/:id/revoke") {
		t.Errorf("log = %s, want an error naming the routes", out)
	}
}

func TestAuditCoverageReportsADuplicateAuditEntry(t *testing.T) {
	routes, prefixes := authPublicPaths(false)
	app, mw := newCoverageApp(false, 2)
	if cov := auditCoverageOf(app, mw, routes, prefixes); cov.maxEntries != 2 {
		t.Fatalf("maxEntries = %d, want 2", cov.maxEntries)
	}
}

func TestAuditCoverageReportsMissingAudit(t *testing.T) {
	routes, prefixes := authPublicPaths(false)
	app, mw := newCoverageApp(false, 0)
	cov := auditCoverageOf(app, mw, routes, prefixes)
	if len(cov.missing) == 0 || len(cov.gaps) == 0 {
		t.Fatalf("coverage = %+v, want missing methods and gaps", cov)
	}
}

// A public prefix matches at a segment boundary only, as the auth middleware
// matches it: /api/v1/metricsfoo is not public.
func TestAuditCoveragePublicPrefixBoundary(t *testing.T) {
	routes, prefixes := authPublicPaths(true)
	for path, want := range map[string]bool{
		"/api/v1/metrics":                   true,
		"/api/v1/metrics/memory":            true,
		"/api/v1/metricsfoo":                false,
		"/metrics":                          true,
		"/health":                           true,
		"/api/v1/auth/verify":               true,
		"/api/v1/auth/tokens":               false,
		"/api/v1/internal/cache/invalidate": true,
	} {
		if got := isPublicPath(path, routes, prefixes); got != want {
			t.Errorf("isPublicPath(%s) = %v, want %v", path, got, want)
		}
	}
	// Without auth.metrics_public the metrics paths need a token.
	routes, prefixes = authPublicPaths(false)
	for _, path := range []string{"/metrics", "/api/v1/metrics", "/api/v1/metrics/memory"} {
		if isPublicPath(path, routes, prefixes) {
			t.Errorf("isPublicPath(%s) = true without auth.metrics_public", path)
		}
	}
	if !isPublicPath("/health", routes, prefixes) {
		t.Error("/health is not public without auth.metrics_public")
	}
}

// One matcher: an empty or "/" prefix is ignored by the auth middleware's rule
// and therefore by the check (it would otherwise make every route public).
func TestAuditCoverageIgnoresEmptyPublicPrefix(t *testing.T) {
	for _, prefixes := range [][]string{{""}, {"/"}} {
		if isPublicPath("/api/v1/auth/tokens", nil, prefixes) {
			t.Errorf("prefixes %q made a token route public", prefixes)
		}
	}
	app, mw := newCoverageApp(true, 1)
	if cov := auditCoverageOf(app, mw, nil, []string{""}); len(cov.gaps) == 0 {
		t.Error("an empty public prefix hid the routes ahead of audit")
	}
}
