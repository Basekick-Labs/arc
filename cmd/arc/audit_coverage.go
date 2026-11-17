package main

import (
	"reflect"
	"sort"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// authPublicPaths returns the routes and path prefixes the auth middleware
// lets through without a token. main hands them to the auth middleware and to
// checkAuditCoverage, so the two cannot drift apart.
func authPublicPaths() (routes, prefixes []string) {
	cfg := auth.DefaultMiddlewareConfig()
	// Add public routes that don't need auth
	// Note: /api/v1/internal/cache/invalidate is public because cluster peers
	// call it without an auth token after compaction. Access is gated by
	// HMAC-SHA256 validation in the handler (see handleCacheInvalidate); the
	// receiver refuses every request when cluster.shared_secret is empty.
	routes = append(cfg.PublicRoutes, "/health", "/ready", "/ready/write", "/api/v1/auth/verify", api.CacheInvalidatePath)
	// /metrics stays public — Prometheus scrapers expect it. It is
	// already in auth.DefaultMiddlewareConfig().PublicPrefixes, so no
	// further append is needed here. /debug/pprof is intentionally NOT
	// here: pprof is no longer mounted on the public Fiber app
	// (internal/api/server.go). The opt-in localhost pprof listener
	// runs on a separate port; see startDebugPprofIfEnabled.
	//
	// The JSON metrics endpoints (/api/v1/metrics and its sub-paths) are
	// deliberately public, equivalent to the existing Prometheus /metrics
	// surface: aggregate operational counters plus Go runtime/GC stats and
	// host-arch fingerprinting (go_version/os/arch). They expose no queries,
	// database/measurement names, file paths, or credentials, and monitoring
	// dashboards poll them without a token. Whitelist them explicitly so
	// their public status is an auditable decision, not an accident of
	// registration order (they were previously reachable only because
	// RegisterRoutes runs before this middleware; see GHSA-m3qr-fvp4-78xj).
	// /api/v1/logs, by contrast, is NOT public — it is registered with admin
	// auth after the auth middleware.
	prefixes = append(cfg.PublicPrefixes, "/api/v1/metrics")
	return routes, prefixes
}

// auditCoverage is what checkAuditCoverage found in the Fiber stack.
type auditCoverage struct {
	// gaps are "METHOD /path" routes that run before the audit middleware and
	// are not public: requests to them are never audited.
	gaps []string
	// maxEntries is the largest number of audit middleware handlers in one
	// method's stack. More than one records a request twice.
	maxEntries int
	// missing names methods whose stack has routes but no audit entry.
	missing []string
	// unreadable is set when the Fiber route layout could not be read; the
	// check then proves nothing.
	unreadable bool
}

// auditCoverageOf walks app's route stack in registration order, which is the
// order Fiber runs it (#1630: the auth and RBAC routes sat before the audit
// middleware for months and were never audited). A route counts as covered
// when an audit entry comes before it in its method's stack, or when it is
// public. HEAD is skipped: Fiber adds it for every GET. Only routes are
// checked: a Use middleware ahead of audit that answers a request itself
// (the auth middleware's 401) is not reported as a gap.
func auditCoverageOf(app *fiber.App, auditHandler fiber.Handler, publicRoutes, publicPrefixes []string) auditCoverage {
	var out auditCoverage
	auditPtr := reflect.ValueOf(auditHandler).Pointer()
	seen := map[string]bool{}
	for _, stack := range app.Stack() {
		entries, routes := 0, 0
		method := ""
		for _, r := range stack {
			use, ok := isUseRoute(r)
			if !ok {
				out.unreadable = true
				return out
			}
			method = r.Method
			if use {
				// Fiber merges consecutive Use calls on one path into a
				// single entry (router.go addRoute), so count handlers.
				entries += countHandler(r, auditPtr)
				continue
			}
			if r.Method == fiber.MethodHead {
				continue
			}
			routes++
			if entries == 0 && !isPublicPath(r.Path, publicRoutes, publicPrefixes) {
				key := r.Method + " " + r.Path
				if !seen[key] {
					seen[key] = true
					out.gaps = append(out.gaps, key)
				}
			}
		}
		if entries > out.maxEntries {
			out.maxEntries = entries
		}
		if routes > 0 && entries == 0 && method != fiber.MethodHead {
			out.missing = append(out.missing, method)
		}
	}
	sort.Strings(out.gaps)
	sort.Strings(out.missing)
	return out
}

// checkAuditCoverage logs, at Error, every route that runs ahead of the audit
// middleware and every duplicate audit entry. It never fails startup: an
// unaudited route is a gap to fix, not a reason to stop serving.
func checkAuditCoverage(app *fiber.App, auditHandler fiber.Handler, publicRoutes, publicPrefixes []string, logger zerolog.Logger) {
	cov := auditCoverageOf(app, auditHandler, publicRoutes, publicPrefixes)
	switch {
	case cov.unreadable:
		logger.Warn().Msg("Audit coverage check skipped: the HTTP router's route layout could not be read")
		return
	case len(cov.gaps) > 0:
		logger.Error().Strs("routes", cov.gaps).
			Msg("Audit logging is enabled but these routes run before the audit middleware and are never audited")
	}
	if cov.maxEntries > 1 {
		logger.Error().Int("audit_middleware_entries", cov.maxEntries).
			Msg("The audit middleware is installed more than once: every request is recorded several times")
	}
	if len(cov.missing) > 0 {
		logger.Error().Strs("methods", cov.missing).
			Msg("Audit logging is enabled but the audit middleware is not installed for these methods")
	}
	if len(cov.gaps) == 0 && cov.maxEntries == 1 && len(cov.missing) == 0 {
		logger.Debug().Msg("Audit coverage check passed: every non-public route runs after the audit middleware")
	}
}

// isUseRoute reads Fiber's unexported Route.use flag: Stack() exposes no
// other way to tell a middleware entry from a route. ok is false when the
// field is missing or not a bool (a Fiber upgrade changed the layout).
func isUseRoute(r *fiber.Route) (use, ok bool) {
	if r == nil {
		return false, false
	}
	f := reflect.ValueOf(r).Elem().FieldByName("use")
	if !f.IsValid() || f.Kind() != reflect.Bool {
		return false, false
	}
	return f.Bool(), true
}

// countHandler counts r's handlers whose code pointer is ptr. A closure's
// pointer is that of its function literal, so every closure audit.Middleware
// returns matches; main builds exactly one, so a count above one is a
// duplicate install.
func countHandler(r *fiber.Route, ptr uintptr) int {
	n := 0
	for _, h := range r.Handlers {
		if reflect.ValueOf(h).Pointer() == ptr {
			n++
		}
	}
	return n
}

// isPublicPath is the auth middleware's own rule (auth.IsPublicPath) applied
// to a route pattern, so the check and the middleware cannot disagree.
func isPublicPath(routePath string, routes, prefixes []string) bool {
	return auth.IsPublicPath(routePath, routes, prefixes)
}
