package auth

import (
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
	token, _ := am.CreateToken("bearer-test", "Test", "read", nil)

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

	token, _ := am.CreateToken("plain-test", "Test", "read", nil)

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

	token, _ := am.CreateToken("apikey-test", "Test", "read", nil)

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
	token, _ := am.CreateToken("expired-test", "Test", "read", &expiresAt)

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

	token, _ := am.CreateToken("context-test", "Test", "read,write", nil)

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
	readToken, _ := am.CreateToken("read-only", "Test", "read", nil)
	writeToken, _ := am.CreateToken("read-write", "Test", "read,write", nil)
	adminToken, _ := am.CreateToken("admin", "Test", "admin", nil)

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

	token, _ := am.CreateToken("info-test", "Test", "read", nil)

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
