//go:build duckdb_arrow

package api

import (
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func queryMetricInt(t *testing.T, key string) int64 {
	t.Helper()
	v, ok := metrics.Get().Snapshot()[key]
	if !ok {
		t.Fatalf("metric %q missing from snapshot", key)
	}
	n, ok := v.(int64)
	if !ok {
		t.Fatalf("metric %q is %T, want int64", key, v)
	}
	return n
}

// TestExecuteQueryArrow_CountsRequestAndSuccess pins that the Arrow query
// endpoint participates in the shared query counters.
//
// Regression test for #801: executeQueryArrow incremented arc_query_errors_total
// on failure but never arc_query_requests_total or arc_query_success_total. The
// obvious dashboard expression, errors/requests, could therefore exceed 1 — or
// divide by zero — on an Arrow-only workload, and successful Arrow queries were
// invisible to every query counter.
func TestExecuteQueryArrow_CountsRequestAndSuccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arrow-metrics-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	duckdb, err := database.New(&database.Config{
		MemoryLimit:      "256MB",
		ThreadCount:      2,
		MaxConnections:   2,
		LocalStorageRoot: tmpDir,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer duckdb.Close()

	h := &QueryHandler{
		db:         duckdb,
		logger:     logger,
		storage:    backend,
		queryCache: database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize),
		pruner:     pruning.NewPartitionPruner(zerolog.Nop()),
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Post("/api/v1/query/arrow", h.executeQueryArrow)

	requestsBefore := queryMetricInt(t, "query_requests_total")
	successBefore := queryMetricInt(t, "query_success_total")

	req := httptest.NewRequest("POST", "/api/v1/query/arrow", strings.NewReader(`{"sql":"SELECT 1 AS id"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	// Draining the body runs the stream-writer goroutine to completion, which
	// is where the success accounting happens.
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	if got := queryMetricInt(t, "query_requests_total"); got != requestsBefore+1 {
		t.Errorf("query_requests_total = %d, want %d: the Arrow endpoint does not count its requests, so errors/requests is unbounded for Arrow traffic (#801)",
			got, requestsBefore+1)
	}
	if got := queryMetricInt(t, "query_success_total"); got != successBefore+1 {
		t.Errorf("query_success_total = %d, want %d: a successful Arrow query is invisible to the query counters (#801)",
			got, successBefore+1)
	}
}
