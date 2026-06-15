package api

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// setupLogsAuthServer builds a Server with a real auth manager, mounts the
// global auth middleware exactly as cmd/arc/main.go does, then registers the
// logs route via RegisterLogsRoute AFTER the middleware — mirroring production
// ordering. Returns the fiber app and auth manager.
func setupLogsAuthServer(t *testing.T) (*fiber.App, *auth.AuthManager, func()) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "logs-auth-test-*")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	am, err := auth.NewAuthManager(filepath.Join(tmpDir, "auth.db"), 1*time.Second, 100, logger)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("NewAuthManager: %v", err)
	}

	srv := &Server{app: fiber.New(), logger: logger}
	// Mirror main.go: global auth middleware first, then the admin-gated
	// logs route registered after it. /api/v1/metrics is whitelisted as a
	// public prefix (parity with the Prometheus /metrics endpoint).
	mwCfg := auth.DefaultMiddlewareConfig()
	mwCfg.AuthManager = am
	mwCfg.PublicPrefixes = append(mwCfg.PublicPrefixes, "/api/v1/metrics")
	srv.app.Use(auth.NewMiddleware(mwCfg))
	srv.RegisterLogsRoute(am)
	// A representative public route + a public metrics route to assert they
	// remain reachable. (/health is in DefaultMiddlewareConfig.PublicRoutes.)
	srv.app.Get("/health", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusOK) })
	srv.app.Get("/api/v1/metrics", func(c *fiber.Ctx) error { return c.JSON(fiber.Map{"ok": true}) })

	cleanup := func() { am.Close(); os.RemoveAll(tmpDir) }
	return srv.app, am, cleanup
}

// TestLogsEndpoint_RequiresAdmin is the regression test for
// GHSA-m3qr-fvp4-78xj: GET /api/v1/logs was reachable unauthenticated because
// it was registered among the public base routes before the global auth
// middleware. It must now require an admin token.
func TestLogsEndpoint_RequiresAdmin(t *testing.T) {
	app, am, cleanup := setupLogsAuthServer(t)
	defer cleanup()

	adminToken := mustCreateToken(t, am, "admin", "read,write,delete,admin")
	readToken := mustCreateToken(t, am, "read", "read")

	get := func(path, token string) int {
		req := httptest.NewRequest("GET", path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := app.Test(req, -1)
		if err != nil {
			t.Fatalf("app.Test(%s): %v", path, err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	tests := []struct {
		name  string
		path  string
		token string
		want  int
	}{
		{"logs no token rejected", "/api/v1/logs?limit=5", "", fiber.StatusUnauthorized},
		{"logs read token forbidden", "/api/v1/logs?limit=5", readToken, fiber.StatusForbidden},
		{"logs admin token ok", "/api/v1/logs?limit=5", adminToken, fiber.StatusOK},
		{"health stays public", "/health", "", fiber.StatusOK},
		{"metrics stays public", "/api/v1/metrics", "", fiber.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := get(tt.path, tt.token); got != tt.want {
				t.Errorf("GET %s (hasToken=%t) = %d, want %d", tt.path, tt.token != "", got, tt.want)
			}
		})
	}
}

// TestLogsEndpoint_AuthDisabledStaysOpen pins that with no auth manager
// (authentication disabled), the logs route is still registered and reachable
// — no-auth deployments must not lose the endpoint.
func TestLogsEndpoint_AuthDisabledStaysOpen(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	srv := &Server{app: fiber.New(), logger: logger}
	srv.RegisterLogsRoute(nil) // auth disabled

	req := httptest.NewRequest("GET", "/api/v1/logs?limit=3", nil)
	resp, err := srv.app.Test(req, -1)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusOK {
		t.Errorf("auth-disabled logs = %d, want 200", resp.StatusCode)
	}
}
