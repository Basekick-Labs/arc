package audit

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/gofiber/fiber/v2"
	"github.com/valyala/fasthttp"
)

// #1630: every route in internal/api/rbac_routes.go and every token route in
// internal/api/auth_routes.go, named by its own resource and verb. Before, a
// PATCH was a "read", and a nested create was filed under its parent
// (a team create as rbac.org.created).
func TestClassifyEveryAuthAndRBACRoute(t *testing.T) {
	for _, tc := range []struct {
		method, path, want string
	}{
		// rbac_routes.go: organizations
		{"GET", "/api/v1/rbac/organizations", "rbac.org.read"},
		{"POST", "/api/v1/rbac/organizations", "rbac.org.created"},
		{"GET", "/api/v1/rbac/organizations/7", "rbac.org.read"},
		{"PATCH", "/api/v1/rbac/organizations/7", "rbac.org.updated"},
		{"DELETE", "/api/v1/rbac/organizations/7", "rbac.org.deleted"},
		// teams, nested and direct
		{"GET", "/api/v1/rbac/organizations/7/teams", "rbac.team.read"},
		{"POST", "/api/v1/rbac/organizations/7/teams", "rbac.team.created"},
		{"GET", "/api/v1/rbac/teams/3", "rbac.team.read"},
		{"PATCH", "/api/v1/rbac/teams/3", "rbac.team.updated"},
		{"DELETE", "/api/v1/rbac/teams/3", "rbac.team.deleted"},
		// roles, nested and direct
		{"GET", "/api/v1/rbac/teams/3/roles", "rbac.role.read"},
		{"POST", "/api/v1/rbac/teams/3/roles", "rbac.role.created"},
		{"GET", "/api/v1/rbac/roles/9", "rbac.role.read"},
		{"PATCH", "/api/v1/rbac/roles/9", "rbac.role.updated"},
		{"DELETE", "/api/v1/rbac/roles/9", "rbac.role.deleted"},
		// measurement permissions
		{"GET", "/api/v1/rbac/roles/9/measurements", "rbac.measurement_permission.read"},
		{"POST", "/api/v1/rbac/roles/9/measurements", "rbac.measurement_permission.created"},
		{"DELETE", "/api/v1/rbac/measurement-permissions/4", "rbac.measurement_permission.deleted"},
		// an ID that happens to equal a resource name is still an ID position,
		// but the last known segment wins: /teams/roles is a team named "roles"
		{"PATCH", "/api/v1/rbac/teams/roles", "rbac.role.updated"},
		{"PUT", "/api/v1/rbac/roles/9", "rbac.role.updated"},
		{"POST", "/api/v1/rbac/nothing-known", "rbac.unknown.created"},
		// auth_routes.go: tokens and memberships
		{"POST", "/api/v1/auth/tokens", "token.created"},
		{"PATCH", "/api/v1/auth/tokens/5", "token.updated"},
		{"DELETE", "/api/v1/auth/tokens/5", "token.deleted"},
		{"POST", "/api/v1/auth/tokens/5/rotate", "token.rotated"},
		{"POST", "/api/v1/auth/tokens/5/revoke", "token.revoked"},
		{"POST", "/api/v1/auth/tokens/5/teams", "token.team_added"},
		{"DELETE", "/api/v1/auth/tokens/5/teams/3", "token.team_removed"},
		{"GET", "/api/v1/auth/tokens", "api.get"},
		{"GET", "/api/v1/auth/tokens/5", "api.get"},
		{"GET", "/api/v1/auth/tokens/5/teams", "api.get"},
		{"GET", "/api/v1/auth/tokens/5/permissions", "api.get"},
		{"GET", "/api/v1/auth/verify", "api.get"},
		{"GET", "/api/v1/auth/cache/stats", "api.get"},
		{"POST", "/api/v1/auth/cache/invalidate", "api.post"},
		// refusals keep their own name
		{"POST", "/api/v1/rbac/organizations", "auth.failed"},
	} {
		status := 200
		if tc.want == "auth.failed" {
			status = 403
		}
		if got := classifyEvent(tc.method, tc.path, status); got != tc.want {
			t.Errorf("%s %s (%d) = %q, want %q", tc.method, tc.path, status, got, tc.want)
		}
	}
}

// Request-supplied fields are bounded, and a cut never splits a UTF-8
// sequence.
func TestMiddlewareBoundsRecordedFields(t *testing.T) {
	logger := &Logger{eventCh: make(chan *AuditEvent, 1)}
	app := fiber.New()
	app.Use(Middleware(logger, true))
	app.Post("/api/v1/databases/*", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusNoContent) })

	long := strings.Repeat("€", 300) // 900 bytes, three per rune: 256 falls mid-rune
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fiber.MethodPost)
	ctx.Request.SetRequestURI("/api/v1/databases/" + strings.Repeat("p", 2000) + "?db=" + long)
	ctx.Request.Header.Set("x-arc-measurement", long)
	app.Handler()(ctx)
	event := <-logger.eventCh
	for name, v := range map[string]string{"path": event.Path, "database": event.Database, "measurement": event.Measurement} {
		if len(v) == 0 || len(v) > maxRecordedField {
			t.Errorf("%s: %d bytes, want 1..%d", name, len(v), maxRecordedField)
		}
		if !utf8.ValidString(v) {
			t.Errorf("%s: cut inside a UTF-8 sequence", name)
		}
	}

	// The header takes precedence over ?db=, and is bounded too.
	ctx = &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fiber.MethodPost)
	ctx.Request.SetRequestURI("/api/v1/databases/x")
	ctx.Request.Header.Set("x-arc-database", strings.Repeat("d", 5000))
	app.Handler()(ctx)
	if event = <-logger.eventCh; len(event.Database) != maxRecordedField {
		t.Errorf("x-arc-database: %d bytes, want %d", len(event.Database), maxRecordedField)
	}
}

// A handler or the router that returns an error has not set the status yet:
// the event records the status the client gets, not 200.
func TestMiddlewareRecordsTheStatusOfAReturnedError(t *testing.T) {
	logger := &Logger{eventCh: make(chan *AuditEvent, 1)}
	app := fiber.New()
	app.Use(Middleware(logger, true))
	app.Post("/api/v1/rbac/teams/:id", func(c *fiber.Ctx) error { return fiber.NewError(fiber.StatusNotFound, "no such team") })
	app.Delete("/api/v1/rbac/roles/:id", func(c *fiber.Ctx) error { return errors.New("boom") })
	app.Get("/api/v1/rbac/organizations", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusOK) })

	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{fiber.MethodPost, "/api/v1/rbac/teams/3", fiber.StatusNotFound},
		{fiber.MethodDelete, "/api/v1/rbac/roles/9", fiber.StatusInternalServerError},
		{fiber.MethodPut, "/api/v1/rbac/organizations", fiber.StatusMethodNotAllowed},
		{fiber.MethodPost, "/api/v1/rbac/unrouted", fiber.StatusNotFound},
	} {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod(tc.method)
		ctx.Request.SetRequestURI(tc.path)
		app.Handler()(ctx)
		event := <-logger.eventCh
		if event.StatusCode != tc.want || ctx.Response.StatusCode() != tc.want {
			t.Errorf("%s %s: recorded %d, client got %d, want %d", tc.method, tc.path, event.StatusCode, ctx.Response.StatusCode(), tc.want)
		}
	}
}

// #1630 review: the event is classified on the full path. A route ID padded
// with zeros (strconv.ParseInt accepts them) used to push /rotate past the
// 256-byte cut, filing the rotate as api.post. Only the stored path is cut.
func TestMiddlewareClassifiesOnTheFullPath(t *testing.T) {
	logger := &Logger{eventCh: make(chan *AuditEvent, 1)}
	app := fiber.New()
	app.Use(Middleware(logger, false))
	app.Post("/api/v1/auth/tokens/:id/rotate", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusOK) })
	app.Post("/api/v1/rbac/organizations/:org_id/teams", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusCreated) })
	pad := strings.Repeat("0", 300)
	for path, want := range map[string]string{
		"/api/v1/auth/tokens/" + pad + "5/rotate":       "token.rotated",
		"/api/v1/rbac/organizations/" + pad + "7/teams": "rbac.team.created",
	} {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod(fiber.MethodPost)
		ctx.Request.SetRequestURI(path)
		app.Handler()(ctx)
		event := <-logger.eventCh
		if event.EventType != want {
			t.Errorf("%d-byte path: %q, want %q", len(path), event.EventType, want)
		}
		if len(event.Path) != maxRecordedField {
			t.Errorf("stored path %d bytes, want %d", len(event.Path), maxRecordedField)
		}
	}
}

// #1630 review: the public verify route is never recorded (no identity, and
// polled), and HEAD and OPTIONS are reads like GET: skipped without
// include_reads.
func TestMiddlewareSkipsVerifyAndReadMethods(t *testing.T) {
	ok := func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusOK) }
	for _, includeReads := range []bool{false, true} {
		logger := &Logger{eventCh: make(chan *AuditEvent, 4)}
		app := fiber.New()
		app.Use(Middleware(logger, includeReads))
		app.Get("/api/v1/auth/verify", ok)
		app.Get("/api/v1/compaction/status", ok)
		app.Options("/api/v1/query", ok)
		requests := []struct{ method, path string }{
			{fiber.MethodGet, "/api/v1/auth/verify"},
			{fiber.MethodHead, "/api/v1/auth/verify"},
			{fiber.MethodHead, "/api/v1/auth/verify/"},
		}
		if !includeReads {
			requests = append(requests,
				struct{ method, path string }{fiber.MethodHead, "/api/v1/compaction/status"},
				struct{ method, path string }{fiber.MethodOptions, "/api/v1/query"})
		}
		for _, r := range requests {
			ctx := &fasthttp.RequestCtx{}
			ctx.Request.Header.SetMethod(r.method)
			ctx.Request.SetRequestURI(r.path)
			app.Handler()(ctx)
		}
		select {
		case event := <-logger.eventCh:
			t.Errorf("include_reads=%v: unexpected event %s %s %s", includeReads, event.Method, event.Path, event.EventType)
		default:
		}
	}
}

// The recorded status is the client's: a *fiber.Error wrapped in another
// error is a 500 to customErrorHandler's plain type assertion, so here too.
func TestMiddlewareStatusOfAWrappedFiberError(t *testing.T) {
	logger := &Logger{eventCh: make(chan *AuditEvent, 1)}
	app := fiber.New(fiber.Config{ErrorHandler: func(c *fiber.Ctx, err error) error {
		code := fiber.StatusInternalServerError
		if e, ok := err.(*fiber.Error); ok {
			code = e.Code
		}
		return c.SendStatus(code)
	}})
	app.Use(Middleware(logger, false))
	app.Post("/api/v1/rbac/teams/:id", func(c *fiber.Ctx) error {
		return fmt.Errorf("lookup: %w", fiber.NewError(fiber.StatusNotFound, "no such team"))
	})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fiber.MethodPost)
	ctx.Request.SetRequestURI("/api/v1/rbac/teams/3")
	app.Handler()(ctx)
	event := <-logger.eventCh
	if event.StatusCode != ctx.Response.StatusCode() || event.StatusCode != fiber.StatusInternalServerError {
		t.Errorf("recorded %d, client got %d, want both 500", event.StatusCode, ctx.Response.StatusCode())
	}
}

// The JSON metrics family is never recorded, like /metrics: dashboards poll
// it every few seconds, and include_reads would record every poll. A path
// that only starts with the same letters is not part of it.
func TestMiddlewareSkipsTheMetricsFamily(t *testing.T) {
	ok := func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusOK) }
	logger := &Logger{eventCh: make(chan *AuditEvent, 8)}
	app := fiber.New()
	app.Use(Middleware(logger, true))
	for _, p := range []string{"/metrics", "/api/v1/metrics", "/api/v1/metrics/memory", "/api/v1/metrics/timeseries/system", "/api/v1/metricsfoo"} {
		app.Get(p, ok)
	}
	for _, p := range []string{"/metrics", "/api/v1/metrics", "/api/v1/metrics/", "/api/v1/metrics/memory", "/api/v1/metrics/timeseries/system"} {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod(fiber.MethodGet)
		ctx.Request.SetRequestURI(p)
		app.Handler()(ctx)
	}
	select {
	case event := <-logger.eventCh:
		t.Fatalf("a metrics poll was recorded: %s %s", event.Method, event.Path)
	default:
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fiber.MethodGet)
	ctx.Request.SetRequestURI("/api/v1/metricsfoo")
	app.Handler()(ctx)
	select {
	case <-logger.eventCh:
	default:
		t.Fatal("/api/v1/metricsfoo was skipped as a metrics path")
	}
	// A write to the family is audited: only polls are skipped.
	app.Post("/api/v1/metrics/reset", ok)
	ctx = &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fiber.MethodPost)
	ctx.Request.SetRequestURI("/api/v1/metrics/reset")
	app.Handler()(ctx)
	select {
	case <-logger.eventCh:
	default:
		t.Fatal("a write to the metrics family was not audited")
	}
}
