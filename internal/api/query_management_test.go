package api

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/queryregistry"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// setupQueryManagementTest creates a Fiber app with the query management handler.
// License middleware is bypassed for functional tests (tested separately).
func setupQueryManagementTest(t *testing.T) (*fiber.App, *queryregistry.Registry) {
	t.Helper()
	metrics.Init(zerolog.Nop())

	registry := queryregistry.NewRegistry(&queryregistry.RegistryConfig{HistorySize: 50}, zerolog.Nop())

	app := fiber.New()
	h := &QueryManagementHandler{
		registry: registry,
		logger:   zerolog.Nop(),
	}

	// Register routes without auth/license middleware for functional tests
	group := app.Group("/api/v1/queries")
	group.Get("/active", h.listActiveQueries)
	group.Get("/history", h.listQueryHistory)
	group.Get("/:id", h.getQuery)
	group.Delete("/:id", h.cancelQuery)

	return app, registry
}

func TestQueryManagement_ListActiveQueries_Empty(t *testing.T) {
	app, _ := setupQueryManagementTest(t)

	req, _ := http.NewRequest("GET", "/api/v1/queries/active", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) == "" {
		t.Fatal("expected non-empty response body")
	}
}

func TestQueryManagement_ListActiveQueries_WithRunning(t *testing.T) {
	app, registry := setupQueryManagementTest(t)

	// Register a query but don't complete it
	registry.Register(context.Background(), "SELECT * FROM test", 1, "test-token", "127.0.0.1", false, 0)

	req, _ := http.NewRequest("GET", "/api/v1/queries/active", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestQueryManagement_ListHistory_Empty(t *testing.T) {
	app, _ := setupQueryManagementTest(t)

	req, _ := http.NewRequest("GET", "/api/v1/queries/history", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestQueryManagement_ListHistory_WithCompleted(t *testing.T) {
	app, registry := setupQueryManagementTest(t)

	id, _ := registry.Register(context.Background(), "SELECT 1", 1, "test-token", "127.0.0.1", false, 0)
	registry.Complete(id, 10)

	req, _ := http.NewRequest("GET", "/api/v1/queries/history", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestQueryManagement_ListHistory_WithLimit(t *testing.T) {
	app, registry := setupQueryManagementTest(t)

	for i := 0; i < 5; i++ {
		id, _ := registry.Register(context.Background(), "SELECT 1", 1, "tok", "127.0.0.1", false, 0)
		registry.Complete(id, i)
	}

	req, _ := http.NewRequest("GET", "/api/v1/queries/history?limit=2", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestQueryManagement_GetQuery_Active(t *testing.T) {
	app, registry := setupQueryManagementTest(t)

	queryID, _ := registry.Register(context.Background(), "SELECT * FROM test", 1, "test-token", "127.0.0.1", false, 0)

	req, _ := http.NewRequest("GET", "/api/v1/queries/"+queryID, nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestQueryManagement_GetQuery_Completed(t *testing.T) {
	app, registry := setupQueryManagementTest(t)

	queryID, _ := registry.Register(context.Background(), "SELECT 1", 1, "test-token", "127.0.0.1", false, 0)
	registry.Complete(queryID, 5)

	req, _ := http.NewRequest("GET", "/api/v1/queries/"+queryID, nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestQueryManagement_GetQuery_NotFound(t *testing.T) {
	app, _ := setupQueryManagementTest(t)

	req, _ := http.NewRequest("GET", "/api/v1/queries/nonexistent", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestQueryManagement_CancelQuery(t *testing.T) {
	app, registry := setupQueryManagementTest(t)

	queryID, ctx := registry.Register(context.Background(), "SELECT slow()", 1, "test-token", "127.0.0.1", false, 0)

	req, _ := http.NewRequest("DELETE", "/api/v1/queries/"+queryID, nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Verify context was cancelled
	select {
	case <-ctx.Done():
		// expected
	case <-time.After(time.Second):
		t.Fatal("expected context to be cancelled")
	}

	// Verify query moved to history
	if registry.ActiveCount() != 0 {
		t.Fatalf("expected 0 active queries, got %d", registry.ActiveCount())
	}
}

func TestQueryManagement_CancelQuery_NotFound(t *testing.T) {
	app, _ := setupQueryManagementTest(t)

	req, _ := http.NewRequest("DELETE", "/api/v1/queries/nonexistent", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestQueryManagement_CancelQuery_AlreadyCompleted(t *testing.T) {
	app, registry := setupQueryManagementTest(t)

	queryID, _ := registry.Register(context.Background(), "SELECT 1", 1, "test-token", "127.0.0.1", false, 0)
	registry.Complete(queryID, 1)

	req, _ := http.NewRequest("DELETE", "/api/v1/queries/"+queryID, nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 409 {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}
}

func TestQueryManagement_LicenseRequired(t *testing.T) {
	metrics.Init(zerolog.Nop())
	registry := queryregistry.NewRegistry(nil, zerolog.Nop())

	// No license client — middleware should block
	app := fiber.New()
	handler := NewQueryManagementHandler(registry, nil, nil, zerolog.Nop())
	handler.RegisterRoutes(app)

	req, _ := http.NewRequest("GET", "/api/v1/queries/active", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 403 {
		t.Fatalf("expected 403 without license, got %d", resp.StatusCode)
	}
}
