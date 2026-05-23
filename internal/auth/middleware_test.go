package auth

import (
	"context"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// setupMiddlewareTest creates a test AuthManager and Fiber app with middleware
func setupMiddlewareTest(t *testing.T, config MiddlewareConfig) (*AuthManager, *fiber.App, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "arc-middleware-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "auth.db")
	logger := zerolog.Nop()

	am, err := NewAuthManager(dbPath, 5*time.Minute, 100, logger)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create AuthManager: %v", err)
	}

	config.AuthManager = am

	app := fiber.New()
	app.Use(NewMiddleware(config))

	// Add a test endpoint
	app.Get("/test", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})

	// Add endpoint that returns token info
	app.Get("/whoami", func(c *fiber.Ctx) error {
		info := GetTokenInfo(c)
		if info == nil {
			return c.JSON(fiber.Map{"authenticated": false})
		}
		return c.JSON(fiber.Map{
			"authenticated": true,
			"name":          info.Name,
			"permissions":   info.Permissions,
		})
	})

	cleanup := func() {
		am.Close()
		os.RemoveAll(tmpDir)
	}

	return am, app, cleanup
}

// TestMiddleware_BearerToken tests Authorization: Bearer token extraction
func TestMiddleware_BearerToken(t *testing.T) {
	config := DefaultMiddlewareConfig()
	am, app, cleanup := setupMiddlewareTest(t, config)
	defer cleanup()

	// Create a valid token
	token, _ := am.CreateToken(context.Background(), "bearer-test", "Test", "read", nil)

	t.Run("valid bearer token", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		if resp.StatusCode != fiber.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
		}
	})

	t.Run("invalid bearer token", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer invalid-token")

		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		if resp.StatusCode != fiber.StatusUnauthorized {
			t.Errorf("Expected status 401, got %d", resp.StatusCode)
		}
	})
}

// TestMiddleware_PlainToken tests plain Authorization header token
func TestMiddleware_PlainToken(t *testing.T) {
	config := DefaultMiddlewareConfig()
	am, app, cleanup := setupMiddlewareTest(t, config)
	defer cleanup()

	token, _ := am.CreateToken(context.Background(), "plain-test", "Test", "read", nil)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", token) // Plain token, no Bearer prefix

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
	}
}

// TestMiddleware_ApiKeyHeader tests x-api-key header token extraction
func TestMiddleware_ApiKeyHeader(t *testing.T) {
	config := DefaultMiddlewareConfig()
	am, app, cleanup := setupMiddlewareTest(t, config)
	defer cleanup()

	token, _ := am.CreateToken(context.Background(), "apikey-test", "Test", "read", nil)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("x-api-key", token)

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
	}
}

// TestMiddleware_PublicRoutes tests that public routes bypass auth
func TestMiddleware_PublicRoutes(t *testing.T) {
	config := DefaultMiddlewareConfig()
	config.PublicRoutes = []string{"/health", "/ready", "/public"}

	_, app, cleanup := setupMiddlewareTest(t, config)
	defer cleanup()

	// Add public routes
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})
	app.Get("/public", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "public"})
	})

	tests := []struct {
		path           string
		expectedStatus int
	}{
		{"/health", fiber.StatusOK},
		{"/public", fiber.StatusOK},
		{"/test", fiber.StatusUnauthorized}, // Not public, no token
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("Path %s: expected status %d, got %d", tt.path, tt.expectedStatus, resp.StatusCode)
			}
		})
	}
}

// TestMiddleware_PublicPrefixes tests that public prefixes bypass auth
func TestMiddleware_PublicPrefixes(t *testing.T) {
	config := DefaultMiddlewareConfig()
	config.PublicPrefixes = []string{"/metrics", "/api/public"}

	_, app, cleanup := setupMiddlewareTest(t, config)
	defer cleanup()

	// Add routes with prefixes
	app.Get("/metrics", func(c *fiber.Ctx) error {
		return c.SendString("metrics")
	})
	app.Get("/metrics/prometheus", func(c *fiber.Ctx) error {
		return c.SendString("prometheus metrics")
	})
	app.Get("/api/public/info", func(c *fiber.Ctx) error {
		return c.SendString("public info")
	})

	tests := []struct {
		path           string
		expectedStatus int
	}{
		{"/metrics", fiber.StatusOK},
		{"/metrics/prometheus", fiber.StatusOK},
		{"/api/public/info", fiber.StatusOK},
		{"/test", fiber.StatusUnauthorized}, // Not a public prefix
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("Path %s: expected status %d, got %d", tt.path, tt.expectedStatus, resp.StatusCode)
			}
		})
	}
}

// TestMiddleware_PublicPrefixes_AnchoredMatch is the regression test for the
// gemini-flagged anchoring gap. Before the fix, strings.HasPrefix(path, "/metrics")
// would silently let /metricsX, /metrics-secret, /metricsadmin bypass auth —
// any route happening to share the byte prefix slipped through. The fix
// requires exact-equal or true-subdirectory (`prefix + "/"`) matching after
// path.Clean normalisation, so non-canonical shapes like `/metrics//foo`,
// `/metrics/./x`, and `/metrics/../X` cannot pollute the bypass branch
// either.
//
// We assert against status, not against registered handlers. Bypass paths
// (200 == handler ran) get a real handler; non-bypass paths (401 == auth
// returned without c.Next) do not — Fiber's middleware short-circuits
// before route lookup.
func TestMiddleware_PublicPrefixes_AnchoredMatch(t *testing.T) {
	config := DefaultMiddlewareConfig()
	// Inject an empty string and a non-empty prefix together. The empty
	// entry tests the empty-prefix guard (must NOT make every request a
	// bypass); the non-empty entry tests the anchored-match contract.
	config.PublicPrefixes = []string{"", "/metrics"}

	_, app, cleanup := setupMiddlewareTest(t, config)
	defer cleanup()

	// Only register the bypass paths — 401 paths short-circuit before
	// route dispatch, so handlers aren't required.
	for _, p := range []string{
		"/metrics", "/metrics/", "/metrics/prometheus", "/metrics/sub/leaf",
	} {
		app.Get(p, func(c *fiber.Ctx) error { return c.SendString("ok") })
	}

	tests := []struct {
		name           string
		path           string
		expectedStatus int
		why            string
	}{
		// Positive: must bypass.
		{"exact match", "/metrics", fiber.StatusOK, "exact-equal must bypass"},
		{"trailing-slash match", "/metrics/", fiber.StatusOK, "prefix + '/' must bypass"},
		{"true subdirectory", "/metrics/prometheus", fiber.StatusOK, "true subdir must bypass"},
		{"deep subdirectory", "/metrics/sub/leaf", fiber.StatusOK, "deep subdir must bypass"},

		// Negative: sibling shapes that share the prefix bytes but aren't subdirectories.
		{"sibling path with same prefix bytes", "/metricsX", fiber.StatusUnauthorized, "/metricsX is NOT a subdir of /metrics"},
		{"sibling with separator-shaped suffix", "/metrics-secret", fiber.StatusUnauthorized, "/metrics-secret is NOT a subdir of /metrics"},
		{"sibling alphanum suffix", "/metricsadmin", fiber.StatusUnauthorized, "/metricsadmin is NOT a subdir of /metrics"},

		// Negative: parent-traversal shapes that path.Clean must normalise
		// AWAY from the /metrics tree before the bypass branch checks
		// them. The middleware sees the normalised form and correctly
		// rejects the request (401). We only test escape-the-prefix
		// shapes here because non-escaping shapes like `/metrics//foo` and
		// `/metrics/./foo` — while correctly bypassed by the middleware —
		// hit a Fiber router that DOES NOT normalise, so they 404 instead
		// of reaching the registered handler. Asserting 404 would couple
		// the test to Fiber's routing behaviour; asserting NOT-401 (i.e.
		// auth bypass happened) is what we actually care about, and the
		// security-relevant assertion is that escape shapes return 401.
		{"parent-traversal escapes the prefix", "/metrics/../sensitive", fiber.StatusUnauthorized, "after path.Clean → /sensitive, NOT under /metrics"},
		{"parent-traversal escapes via leading dot-dot", "/metrics/sub/../../etc", fiber.StatusUnauthorized, "after path.Clean → /etc, NOT under /metrics"},

		// Empty-prefix guard: an empty entry in PublicPrefixes must NOT
		// cause `/any-path` to be treated as bypass. If the guard breaks,
		// /any-other-path would return 200 instead of 401.
		{"empty prefix entry does not match unrelated paths", "/some/random/api", fiber.StatusUnauthorized, "empty-string prefix must be skipped, NOT match every path"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("path %q: expected status %d, got %d (%s)",
					tt.path, tt.expectedStatus, resp.StatusCode, tt.why)
			}
		})
	}
}

// TestMiddleware_PublicPrefixes_TrailingSlashNormalisation is the
// regression test for the gemini-r2 finding: a configured prefix with a
// trailing slash (e.g. `/metrics/`) used to break the anchored match
// because `prefix + "/"` produced `/metrics//`, which lexically matches
// no real request path. The fix strips exactly one trailing slash from
// the prefix before the anchored check, so configured `/metrics/` and
// configured `/metrics` are byte-identical at match time. Test both
// shapes against the same set of request paths to pin that equivalence.
func TestMiddleware_PublicPrefixes_TrailingSlashNormalisation(t *testing.T) {
	configs := []struct {
		name   string
		prefix string
	}{
		{"prefix without trailing slash", "/metrics"},
		{"prefix WITH trailing slash", "/metrics/"},
	}
	for _, cfg := range configs {
		cfg := cfg // capture for parallel subtests
		t.Run(cfg.name, func(t *testing.T) {
			mw := DefaultMiddlewareConfig()
			mw.PublicPrefixes = []string{cfg.prefix}

			_, app, cleanup := setupMiddlewareTest(t, mw)
			defer cleanup()
			app.Get("/metrics", func(c *fiber.Ctx) error { return c.SendString("ok") })
			app.Get("/metrics/prometheus", func(c *fiber.Ctx) error { return c.SendString("ok") })

			tests := []struct {
				name           string
				path           string
				expectedStatus int
			}{
				{"exact match", "/metrics", fiber.StatusOK},
				{"true subdirectory", "/metrics/prometheus", fiber.StatusOK},
				{"sibling byte-prefix", "/metricsX", fiber.StatusUnauthorized},
				{"parent traversal escape", "/metrics/../sensitive", fiber.StatusUnauthorized},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					req := httptest.NewRequest("GET", tt.path, nil)
					resp, err := app.Test(req)
					if err != nil {
						t.Fatalf("Request failed: %v", err)
					}
					if resp.StatusCode != tt.expectedStatus {
						t.Errorf("prefix=%q path=%q: expected status %d, got %d",
							cfg.prefix, tt.path, tt.expectedStatus, resp.StatusCode)
					}
				})
			}
		})
	}
}

// TestMiddleware_NoToken tests request without any token
func TestMiddleware_NoToken(t *testing.T) {
	config := DefaultMiddlewareConfig()
	_, app, cleanup := setupMiddlewareTest(t, config)
	defer cleanup()

	req := httptest.NewRequest("GET", "/test", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", resp.StatusCode)
	}
}

// TestMiddleware_ExpiredToken tests expired token handling
func TestMiddleware_ExpiredToken(t *testing.T) {
	config := DefaultMiddlewareConfig()
	am, app, cleanup := setupMiddlewareTest(t, config)
	defer cleanup()

	// Create expired token
	expiresAt := time.Now().Add(-1 * time.Hour)
	token, _ := am.CreateToken(context.Background(), "expired-test", "Test", "read", &expiresAt)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusUnauthorized {
		t.Errorf("Expected status 401 for expired token, got %d", resp.StatusCode)
	}
}

// TestMiddleware_ContextToken tests that token info is stored in context
func TestMiddleware_ContextToken(t *testing.T) {
	config := DefaultMiddlewareConfig()
	am, app, cleanup := setupMiddlewareTest(t, config)
	defer cleanup()

	token, _ := am.CreateToken(context.Background(), "context-test", "Test", "read,write", nil)

	req := httptest.NewRequest("GET", "/whoami", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !contains(bodyStr, `"authenticated":true`) {
		t.Error("Expected authenticated:true in response")
	}
	if !contains(bodyStr, `"name":"context-test"`) {
		t.Error("Expected name:context-test in response")
	}
}

// TestMiddleware_Skip tests Skip configuration
func TestMiddleware_Skip(t *testing.T) {
	config := DefaultMiddlewareConfig()
	config.Skip = true

	_, app, cleanup := setupMiddlewareTest(t, config)
	defer cleanup()

	// Request without any token should succeed when Skip is true
	req := httptest.NewRequest("GET", "/test", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusOK {
		t.Errorf("Expected status 200 when Skip=true, got %d", resp.StatusCode)
	}
}

// TestMiddleware_NoAuthManager tests behavior when AuthManager is nil
func TestMiddleware_NoAuthManager(t *testing.T) {
	config := DefaultMiddlewareConfig()
	config.AuthManager = nil

	app := fiber.New()
	app.Use(NewMiddleware(config))
	app.Get("/test", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})

	// Request should succeed when no AuthManager is configured
	req := httptest.NewRequest("GET", "/test", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusOK {
		t.Errorf("Expected status 200 when AuthManager=nil, got %d", resp.StatusCode)
	}
}

// TestRequirePermission tests permission-specific middleware
func TestRequirePermission(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "arc-perm-test-*")
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "auth.db")
	logger := zerolog.Nop()

	am, err := NewAuthManager(dbPath, 5*time.Minute, 100, logger)
	if err != nil {
		t.Fatalf("failed to create AuthManager: %v", err)
	}
	defer am.Close()

	// Create tokens with different permissions
	readToken, _ := am.CreateToken(context.Background(), "read-only", "Test", "read", nil)
	writeToken, _ := am.CreateToken(context.Background(), "read-write", "Test", "read,write", nil)
	adminToken, _ := am.CreateToken(context.Background(), "admin", "Test", "admin", nil)

	// Setup app with auth middleware and permission middleware
	config := DefaultMiddlewareConfig()
	config.AuthManager = am

	app := fiber.New()
	app.Use(NewMiddleware(config))

	// Endpoint requiring write permission
	app.Post("/data", RequireWrite(am), func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "written"})
	})

	// Endpoint requiring admin permission
	app.Delete("/admin", RequireAdmin(am), func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "deleted"})
	})

	tests := []struct {
		name           string
		method         string
		path           string
		token          string
		expectedStatus int
	}{
		{"read token cannot write", "POST", "/data", readToken, fiber.StatusForbidden},
		{"write token can write", "POST", "/data", writeToken, fiber.StatusOK},
		{"admin token can write", "POST", "/data", adminToken, fiber.StatusOK},
		{"read token cannot admin", "DELETE", "/admin", readToken, fiber.StatusForbidden},
		{"write token cannot admin", "DELETE", "/admin", writeToken, fiber.StatusForbidden},
		{"admin token can admin", "DELETE", "/admin", adminToken, fiber.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Header.Set("Authorization", "Bearer "+tt.token)

			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}

			if resp.StatusCode != tt.expectedStatus {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("Expected status %d, got %d: %s", tt.expectedStatus, resp.StatusCode, string(body))
			}
		})
	}
}

// TestRequirePermission_NoToken tests permission middleware without token in context
func TestRequirePermission_NoToken(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "arc-perm-test-*")
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "auth.db")
	logger := zerolog.Nop()

	am, _ := NewAuthManager(dbPath, 5*time.Minute, 100, logger)
	defer am.Close()

	app := fiber.New()
	// No main auth middleware, just permission check
	app.Post("/data", RequireWrite(am), func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})

	req := httptest.NewRequest("POST", "/data", nil)
	resp, _ := app.Test(req)

	if resp.StatusCode != fiber.StatusUnauthorized {
		t.Errorf("Expected 401 when no token in context, got %d", resp.StatusCode)
	}
}

// TestGetTokenInfo tests the GetTokenInfo helper function
func TestGetTokenInfo(t *testing.T) {
	config := DefaultMiddlewareConfig()
	am, app, cleanup := setupMiddlewareTest(t, config)
	defer cleanup()

	token, _ := am.CreateToken(context.Background(), "info-test", "Test", "read", nil)

	t.Run("with valid token", func(t *testing.T) {
		var capturedInfo *TokenInfo
		app.Get("/capture", func(c *fiber.Ctx) error {
			capturedInfo = GetTokenInfo(c)
			return c.SendString("ok")
		})

		req := httptest.NewRequest("GET", "/capture", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		app.Test(req)

		if capturedInfo == nil {
			t.Error("GetTokenInfo should return token info")
		}
		if capturedInfo != nil && capturedInfo.Name != "info-test" {
			t.Errorf("Name = %s, want info-test", capturedInfo.Name)
		}
	})
}

// TestDefaultMiddlewareConfig tests default configuration values
func TestDefaultMiddlewareConfig(t *testing.T) {
	config := DefaultMiddlewareConfig()

	if config.Skip != false {
		t.Error("Default Skip should be false")
	}

	// Check default public routes
	expectedRoutes := []string{"/health", "/ready", "/api/v1/auth/verify"}
	for _, route := range expectedRoutes {
		found := false
		for _, r := range config.PublicRoutes {
			if r == route {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected %s in default public routes", route)
		}
	}

	// Check default public prefixes
	foundMetrics := false
	for _, prefix := range config.PublicPrefixes {
		if prefix == "/metrics" {
			foundMetrics = true
		}
	}
	if !foundMetrics {
		t.Error("Expected /metrics in default public prefixes")
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
