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
//
// Three modes:
//   - enabled=false: IsRBACEnabled returns false (OSS/no-RBAC mode)
//   - enabled=true + allowAll=true: every permission check passes
//   - enabled=true + allowAll=false + allowedDBs: only the listed databases pass;
//     everything else is denied (default denyReason is used).
type mockRBACChecker struct {
	enabled      bool
	allowAll     bool
	allowedDBs   map[string]bool // databases for which read permission is granted
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
	// Only match "*" (wildcard) or databases explicitly in allowedDBs.
	// Wildcard "*" matching means the user needs *.*:read — denied here
	// unless allowAll is true.
	if req.Database == "*" {
		return &auth.PermissionCheckResult{
			Allowed: false,
			Source:  "rbac",
			Reason:  m.deniedReason,
		}
	}
	if m.allowedDBs != nil && m.allowedDBs[req.Database] {
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

// TestListMeasurements_RBAC_DbFilterScoped verifies Gemini finding #2 fix:
// a user with mydb.*:read should be able to list measurements scoped to
// mydb via ?database=mydb, even though they lack *.*:read.
func TestListMeasurements_RBAC_DbFilterScoped(t *testing.T) {
	rbac := &mockRBACChecker{
		enabled:      true,
		allowAll:     false,
		allowedDBs:   map[string]bool{"mydb": true},
		deniedReason: "no read permission",
	}
	app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "scoped-token"))

	// With ?database=mydb, the RBAC check should use "mydb" not "*",
	// and since mydb is in allowedDBs, it passes.
	req := httptest.NewRequest("GET", "/api/v1/measurements?database=mydb", nil)

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode == fiber.StatusForbidden {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected non-403 (scoped db filter allowed), got 403: %s", string(bodyBytes))
	}
}

// TestEstimateQuery_RBAC_HeaderBypassFixed verifies Gemini finding #1 fix:
// when x-arc-database header is set, the RBAC check must resolve "default"
// table references against the header database, not literal "default".
func TestEstimateQuery_RBAC_HeaderBypassFixed(t *testing.T) {
	// User has read on default.* but NOT on otherdb.*
	rbac := &mockRBACChecker{
		enabled:      true,
		allowAll:     false,
		allowedDBs:   map[string]bool{"default": true},
		deniedReason: "no read permission",
	}
	app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "default-only"))

	// SQL references bare table "cpu" (resolves to "default.cpu" by extractTableReferences).
	// With x-arc-database: otherdb, the fixed checkQueryPermissions should remap
	// default → otherdb, causing the RBAC check to fail (otherdb not in allowedDBs).
	body := strings.NewReader(`{"sql": "SELECT * FROM cpu"}`)
	req := httptest.NewRequest("POST", "/api/v1/query/estimate", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-arc-database", "otherdb")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != fiber.StatusForbidden {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 403 (bypass prevented: header otherdb not allowed), got %d: %s",
			resp.StatusCode, string(bodyBytes))
	}
}

// TestEstimateQuery_CrossDBSyntaxRejected verifies Gemini finding #3 fix:
// when x-arc-database header is set, cross-database syntax (db.table) must
// be rejected with 400, matching executeQuery behavior.
func TestEstimateQuery_CrossDBSyntaxRejected(t *testing.T) {
	rbac := &mockRBACChecker{
		enabled:  true,
		allowAll: true,
	}
	app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "test-token"))

	body := strings.NewReader(`{"sql": "SELECT * FROM otherdb.cpu"}`)
	req := httptest.NewRequest("POST", "/api/v1/query/estimate", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-arc-database", "mydb")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != fiber.StatusBadRequest {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400 (cross-database syntax rejected), got %d: %s",
			resp.StatusCode, string(bodyBytes))
	}
}

// TestListMeasurements_InvalidDbFilter verifies Gemini finding #4 fix:
// an invalid database query parameter should be rejected with 400 before
// reaching the RBAC check.
func TestListMeasurements_InvalidDbFilter(t *testing.T) {
	rbac := &mockRBACChecker{
		enabled:  true,
		allowAll: true,
	}
	app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "test-token"))

	// Database name with invalid characters
	req := httptest.NewRequest("GET", "/api/v1/measurements?database=bad;name", nil)

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != fiber.StatusBadRequest {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400 (invalid db filter), got %d: %s",
			resp.StatusCode, string(bodyBytes))
	}
}

// TestHasCrossDatabaseSyntax verifies Gemini finding #3 hardening:
// the hasCrossDatabaseSyntax helper correctly handles arbitrary whitespace
// (newlines, tabs, multiple spaces) between FROM/JOIN and the db.table ref.
func TestHasCrossDatabaseSyntax(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		// Baseline: space-separated (original behaviour)
		{name: "space after FROM", sql: "SELECT * FROM otherdb.cpu", expected: true},
		{name: "space after JOIN", sql: "SELECT * FROM cpu JOIN otherdb.mem", expected: true},
		{name: "no dot = single table", sql: "SELECT * FROM cpu", expected: false},
		{name: "no FROM", sql: "SELECT 1", expected: false},

		// Gemini R3: newline / tab / multiple spaces after keyword
		{name: "newline after FROM", sql: "SELECT * FROM\notherdb.cpu", expected: true},
		{name: "tab after FROM", sql: "SELECT * FROM\totherdb.cpu", expected: true},
		{name: "multiple spaces after FROM", sql: "SELECT * FROM   otherdb.cpu", expected: true},
		{name: "newline after JOIN", sql: "JOIN\notherdb.cpu", expected: true},
		{name: "tab after JOIN", sql: "JOIN\totherdb.cpu", expected: true},

		// Keyword boundary: should NOT match "fromage" or "joiner"
		{name: "from embedded in identifier", sql: "SELECT fromage FROM cpu", expected: false},
		{name: "join embedded in identifier", sql: "SELECT * FROM cpu JOINER x", expected: false},

		// Mixed whitespace
		{name: "mixed whitespace", sql: "SELECT * FROM \t \n otherdb.cpu", expected: true},

		// Case insensitivity
		{name: "uppercase FROM", sql: "SELECT * FROM otherdb.cpu", expected: true},
		{name: "mixed case From", sql: "SELECT * From otherdb.cpu", expected: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasCrossDatabaseSyntax(tt.sql)
			if got != tt.expected {
				t.Errorf("hasCrossDatabaseSyntax(%q) = %v, want %v", tt.sql, got, tt.expected)
			}
		})
	}
}

// TestEstimateQuery_CrossDBSyntaxRejected_Newline verifies Gemini R3:
// cross-database syntax with newline after FROM is still rejected in the
// estimateQuery endpoint when x-arc-database header is set.
func TestEstimateQuery_CrossDBSyntaxRejected_Newline(t *testing.T) {
	rbac := &mockRBACChecker{
		enabled:  true,
		allowAll: true,
	}
	app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "test-token"))

	// FROM\notherdb.cpu — newline between keyword and table ref
	body := strings.NewReader("{\"sql\": \"SELECT * FROM\\notherdb.cpu\"}")
	req := httptest.NewRequest("POST", "/api/v1/query/estimate", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-arc-database", "mydb")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != fiber.StatusBadRequest {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400 (cross-database with newline rejected), got %d: %s",
			resp.StatusCode, string(bodyBytes))
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
