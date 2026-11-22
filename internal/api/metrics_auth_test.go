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

func metricsAuthManager(t *testing.T) *auth.AuthManager {
	t.Helper()
	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Second, 100, zerolog.New(os.Stderr).Level(zerolog.Disabled))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	return am
}

func metricsStatus(t *testing.T, app *fiber.App, path, token string) int {
	t.Helper()
	req := httptest.NewRequest("GET", path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test(%s): %v", path, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

var metricsPaths = []string{
	"/metrics", "/api/v1/metrics", "/api/v1/metrics/memory", "/api/v1/metrics/query-pool",
	"/api/v1/metrics/endpoints", "/api/v1/metrics/timeseries/system",
}

// Secured (the default with auth on): every metrics path needs a token, and
// any valid token will do, a write-only one included (#1559's lesson: not a
// read-permission check).
func TestMetricsRoutesNeedAToken(t *testing.T) {
	am := metricsAuthManager(t)
	srv := &Server{app: fiber.New(), logger: zerolog.Nop()}
	mw := auth.DefaultMiddlewareConfig()
	mw.AuthManager = am
	srv.app.Use(auth.NewMiddleware(mw))
	srv.RegisterMetricsRoutes(am)
	read := mustCreateToken(t, am, "read", "read")
	write := mustCreateToken(t, am, "write", "write")
	for _, p := range metricsPaths {
		if got := metricsStatus(t, srv.app, p, ""); got != fiber.StatusUnauthorized {
			t.Errorf("GET %s without a token = %d, want 401", p, got)
		}
		for name, tok := range map[string]string{"read": read, "write-only": write} {
			if got := metricsStatus(t, srv.app, p, tok); got == fiber.StatusUnauthorized || got == fiber.StatusForbidden {
				t.Errorf("GET %s with a %s token = %d, want it served", p, name, got)
			}
		}
	}
}

// The routes' own guard fails closed: registered ahead of the auth
// middleware (the GHSA-m3qr-fvp4-78xj shape), they refuse every request
// instead of serving metrics without a token.
func TestMetricsRoutesRegisteredTooEarlyRefuse(t *testing.T) {
	am := metricsAuthManager(t)
	srv := &Server{app: fiber.New(), logger: zerolog.Nop()}
	srv.RegisterMetricsRoutes(am)
	mw := auth.DefaultMiddlewareConfig()
	mw.AuthManager = am
	srv.app.Use(auth.NewMiddleware(mw))
	read := mustCreateToken(t, am, "read", "read")
	for _, p := range metricsPaths {
		for _, tok := range []string{"", read} {
			if got := metricsStatus(t, srv.app, p, tok); got != fiber.StatusUnauthorized {
				t.Errorf("GET %s registered ahead of auth (token %v) = %d, want 401", p, tok != "", got)
			}
		}
	}
}

// auth.metrics_public: public prefixes and no guard; served without a token.
func TestMetricsRoutesPublic(t *testing.T) {
	am := metricsAuthManager(t)
	srv := &Server{app: fiber.New(), logger: zerolog.Nop()}
	mw := auth.DefaultMiddlewareConfig()
	mw.AuthManager = am
	mw.PublicPrefixes = append(mw.PublicPrefixes, "/metrics", "/api/v1/metrics")
	srv.app.Use(auth.NewMiddleware(mw))
	srv.RegisterMetricsRoutes(nil)
	for _, p := range metricsPaths {
		if got := metricsStatus(t, srv.app, p, ""); got == fiber.StatusUnauthorized || got == fiber.StatusForbidden {
			t.Errorf("GET %s public = %d, want it served", p, got)
		}
	}
}
