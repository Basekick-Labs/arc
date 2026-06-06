package api

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
	recoverMiddleware "github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

// mockRBACChecker implements RBACChecker for testing RBAC enforcement
// in query endpoints.
type mockRBACChecker struct {
	enabled      bool
	allowAll     bool
	deniedReason string
}

func (m *mockRBACChecker) IsRBACEnabled() bool {
	return m.enabled
}

func (m *mockRBACChecker) CheckPermission(req *auth.PermissionCheckRequest) *auth.PermissionCheckResult {
	if !m.enabled {
		return &auth.PermissionCheckResult{Allowed: true, Source: "token"}
	}
	if m.allowAll {
		return &auth.PermissionCheckResult{Allowed: true, Source: "rbac"}
	}
	return &auth.PermissionCheckResult{
		Allowed: false,
		Source:  "rbac",
		Reason:  m.deniedReason,
	}
}

func (m *mockRBACChecker) CheckPermissionsBatch(reqs []*auth.PermissionCheckRequest) []*auth.PermissionCheckResult {
	results := make([]*auth.PermissionCheckResult, len(reqs))
	for i, req := range reqs {
		results[i] = m.CheckPermission(req)
	}
	return results
}

// setupQueryRBACTest creates a Fiber app with a QueryHandler configured
// with a mock RBAC checker. The handler is constructed directly (not via
// NewQueryHandler) so that nil db/storage are tolerated — RBAC denial
// paths fire before any DB/storage access, so those fields are never
// dereferenced in denial tests.
//
// The tokenFn runs as the first middleware on every request, simulating
// what the auth middleware does: injects a token into c.Locals. Pass nil
// to skip token injection (simulates auth-disabled or no-token scenarios).
func setupQueryRBACTest(t *testing.T, rbac *mockRBACChecker, tokenFn fiber.Handler) *fiber.App {
	t.Helper()
	metrics.Init(zerolog.Nop())

	h := &QueryHandler{
		logger:      zerolog.Nop(),
		rbacManager: rbac,
	}

	app := fiber.New(fiber.Config{
		// Disable startup message in tests
		DisableStartupMessage: true,
	})

	// Token middleware MUST be registered before routes so it runs first.
	// In Fiber, middleware stack order = registration order.
	if tokenFn != nil {
		app.Use(tokenFn)
	}

	// Recovery middleware to catch panics in "allowed" tests that proceed
	// past RBAC into nil DB/storage access.
	app.Use(recoverMiddleware.New())

	h.RegisterRoutes(app)

	return app
}

// tokenMiddleware returns a Fiber middleware that injects a fake token
// into c.Locals, simulating what the real auth middleware does.
func tokenMiddleware(tokenID int64, tokenName string) func(c *fiber.Ctx) error {
	return func(c *fiber.Ctx) error {
		c.Locals("token_info", &auth.TokenInfo{
			ID:   tokenID,
			Name: tokenName,
		})
		return c.Next()
	}
}

// =============================================================================
// estimateQuery RBAC tests
// =============================================================================

func TestEstimateQuery_RBAC_Denied(t *testing.T) {
	rbac := &mockRBACChecker{
		enabled:      true,
		allowAll:     false,
		deniedReason: "no read permission for db.cpu",
	}
	app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "test-token"))

	body := strings.NewReader(`{"sql": "SELECT * FROM cpu WHERE host = 'server1'"}`)
	req := httptest.NewRequest("POST", "/api/v1/query/estimate", body)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != fiber.StatusForbidden {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 403 Forbidden, got %d: %s", resp.StatusCode, string(bodyBytes))
	}
}

func TestEstimateQuery_RBAC_Allowed(t *testing.T) {
	rbac := &mockRBACChecker{
		enabled:  true,
		allowAll: true,
	}
	app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "test-token"))

	body := strings.NewReader(`{"sql": "SELECT * FROM cpu"}`)
	req := httptest.NewRequest("POST", "/api/v1/query/estimate", body)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}

	// RBAC allows, but the query will fail because there's no DuckDB —
	// so we expect NOT a 403.
	if resp.StatusCode == fiber.StatusForbidden {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected non-403 (RBAC allowed), got 403: %s", string(bodyBytes))
	}
}

func TestEstimateQuery_RBAC_Disabled(t *testing.T) {
	rbac := &mockRBACChecker{
		enabled: false,
	}
	app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "test-token"))

	body := strings.NewReader(`{"sql": "SELECT * FROM cpu"}`)
	req := httptest.NewRequest("POST", "/api/v1/query/estimate", body)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}

	// RBAC is disabled, so we skip the check entirely.
	if resp.StatusCode == fiber.StatusForbidden {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected non-403 (RBAC disabled), got 403: %s", string(bodyBytes))
	}
}

func TestEstimateQuery_RBAC_NoToken(t *testing.T) {
	rbac := &mockRBACChecker{
		enabled:  true,
		allowAll: false,
	}
	// nil tokenFn → no token injected; RBAC check should skip (nil token = pass)
	app := setupQueryRBACTest(t, rbac, nil)

	body := strings.NewReader(`{"sql": "SELECT * FROM cpu"}`)
	req := httptest.NewRequest("POST", "/api/v1/query/estimate", body)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode == fiber.StatusForbidden {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected non-403 (no token = skip RBAC), got 403: %s", string(bodyBytes))
	}
}

// =============================================================================
// listMeasurements RBAC tests
// =============================================================================

func TestListMeasurements_RBAC_Denied(t *testing.T) {
	rbac := &mockRBACChecker{
		enabled:      true,
		allowAll:     false,
		deniedReason: "no read permission for *.*",
	}
	app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "test-token"))

	req := httptest.NewRequest("GET", "/api/v1/measurements", nil)

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != fiber.StatusForbidden {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 403 Forbidden, got %d: %s", resp.StatusCode, string(bodyBytes))
	}
}

func TestListMeasurements_RBAC_Allowed(t *testing.T) {
	rbac := &mockRBACChecker{
		enabled:  true,
		allowAll: true,
	}
	app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "test-token"))

	req := httptest.NewRequest("GET", "/api/v1/measurements", nil)

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}

	// RBAC allows, but storage is nil → basePath is empty → returns success with empty list.
	if resp.StatusCode == fiber.StatusForbidden {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected non-403 (RBAC allowed), got 403: %s", string(bodyBytes))
	}
}

func TestListMeasurements_RBAC_Disabled(t *testing.T) {
	rbac := &mockRBACChecker{
		enabled: false,
	}
	app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "test-token"))

	req := httptest.NewRequest("GET", "/api/v1/measurements", nil)

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode == fiber.StatusForbidden {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected non-403 (RBAC disabled), got 403: %s", string(bodyBytes))
	}
}

func TestListMeasurements_RBAC_NoToken(t *testing.T) {
	rbac := &mockRBACChecker{
		enabled:  true,
		allowAll: false,
	}
	// nil tokenFn → no token injected; RBAC check should skip.
	app := setupQueryRBACTest(t, rbac, nil)

	req := httptest.NewRequest("GET", "/api/v1/measurements", nil)

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode == fiber.StatusForbidden {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected non-403 (no token = skip RBAC), got 403: %s", string(bodyBytes))
	}
}

// =============================================================================
// Benchmarks — measure RBAC check overhead on the hot query path
// =============================================================================

// setupBenchHandler creates a QueryHandler with the given RBAC checker and
// a token injected into the Fiber context. Used by benchmarks.
func setupBenchHandler(b *testing.B, rbac *mockRBACChecker, withToken bool) (*QueryHandler, *fiber.Ctx) {
	b.Helper()

	h := &QueryHandler{
		logger:      zerolog.Nop(),
		rbacManager: rbac,
	}

	app := fiber.New()
	c := app.AcquireCtx(&fasthttp.RequestCtx{})
	if withToken {
		c.Locals("token_info", &auth.TokenInfo{
			ID:   1,
			Name: "bench-token",
		})
	}

	return h, c
}

// BenchmarkCheckMeasurementPermission_RBACDisabled measures the overhead
// when RBAC is disabled — this should be near-zero (a single nil + bool check).
func BenchmarkCheckMeasurementPermission_RBACDisabled(b *testing.B) {
	rbac := &mockRBACChecker{enabled: false}
	h, c := setupBenchHandler(b, rbac, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.checkMeasurementPermission(c, "db", "cpu", "read")
	}
}

// BenchmarkCheckMeasurementPermission_Allowed measures the overhead when
// RBAC is enabled and the check passes.
func BenchmarkCheckMeasurementPermission_Allowed(b *testing.B) {
	rbac := &mockRBACChecker{enabled: true, allowAll: true}
	h, c := setupBenchHandler(b, rbac, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.checkMeasurementPermission(c, "db", "cpu", "read")
	}
}

// BenchmarkCheckMeasurementPermission_Denied measures the overhead when
// RBAC is enabled and the check fails.
func BenchmarkCheckMeasurementPermission_Denied(b *testing.B) {
	rbac := &mockRBACChecker{enabled: true, allowAll: false, deniedReason: "no permission"}
	h, c := setupBenchHandler(b, rbac, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.checkMeasurementPermission(c, "db", "cpu", "read")
	}
}

// BenchmarkCheckQueryPermissions_SimpleSQL measures overhead of extracting
// table references from a simple SQL and checking permissions.
func BenchmarkCheckQueryPermissions_SimpleSQL(b *testing.B) {
	rbac := &mockRBACChecker{enabled: true, allowAll: true}
	h, c := setupBenchHandler(b, rbac, true)

	sql := "SELECT * FROM cpu WHERE host = 'server1'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.checkQueryPermissions(c, sql, "read")
	}
}

// BenchmarkCheckQueryPermissions_ComplexSQL measures overhead with a
// multi-table JOIN query that exercises the regex extraction path.
func BenchmarkCheckQueryPermissions_ComplexSQL(b *testing.B) {
	rbac := &mockRBACChecker{enabled: true, allowAll: true}
	h, c := setupBenchHandler(b, rbac, true)

	sql := `SELECT a.host, b.region, AVG(a.value)
FROM metrics.cpu a
JOIN analytics.regions b ON a.host = b.host
WHERE a.time > '2024-01-01'
GROUP BY a.host, b.region
ORDER BY AVG(a.value) DESC`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.checkQueryPermissions(c, sql, "read")
	}
}

// BenchmarkCheckQueryPermissions_RBACDisabled measures overhead when RBAC
// is disabled — should be the fastest path (single nil+bool check).
func BenchmarkCheckQueryPermissions_RBACDisabled(b *testing.B) {
	rbac := &mockRBACChecker{enabled: false}
	h, c := setupBenchHandler(b, rbac, true)

	sql := "SELECT * FROM cpu WHERE host = 'server1'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.checkQueryPermissions(c, sql, "read")
	}
}

// BenchmarkExtractTableReferences isolates the regex-based SQL parsing cost.
func BenchmarkExtractTableReferences(b *testing.B) {
	sql := `SELECT a.host, b.region, AVG(a.value)
FROM metrics.cpu a
JOIN analytics.regions b ON a.host = b.host
WHERE a.time > '2024-01-01'
GROUP BY a.host, b.region`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = extractTableReferences(sql)
	}
}
