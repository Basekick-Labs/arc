//go:build duckdb_arrow

package api

import (
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/queryregistry"
	"github.com/basekick-labs/arc/internal/storage"
)

// newArrowRegistryRig builds a QueryHandler with a query registry wired the
// way main.go wires it, and a Fiber app exposing only the Arrow endpoint.
func newArrowRegistryRig(t *testing.T, queryTimeout, slowThreshold time.Duration) (*fiber.App, *queryregistry.Registry) {
	t.Helper()
	tmpDir := t.TempDir()
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
	t.Cleanup(func() { duckdb.Close() })
	reg := queryregistry.NewRegistry(&queryregistry.RegistryConfig{HistorySize: 16}, logger)
	h := &QueryHandler{
		db:                 duckdb,
		logger:             logger,
		storage:            backend,
		queryCache:         database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize),
		pruner:             pruning.NewPartitionPruner(zerolog.Nop()),
		queryTimeout:       queryTimeout,
		slowQueryThreshold: slowThreshold,
	}
	h.SetQueryRegistry(reg)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Post("/api/v1/query/arrow", h.executeQueryArrow)
	return app, reg
}

// TestExecuteQueryArrow_RegistersAndCompletes (regression, #309): an Arrow
// query gets a registry entry, announces its id on X-Arc-Query-ID, and lands in
// history as completed with its row count. app.Test runs the stream writer to
// completion before returning, which is what guarantees Complete has run.
func TestExecuteQueryArrow_RegistersAndCompletes(t *testing.T) {
	app, reg := newArrowRegistryRig(t, 30*time.Second, 0)
	req := httptest.NewRequest("POST", "/api/v1/query/arrow", strings.NewReader(`{"sql":"SELECT * FROM range(5) AS t(id)"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d: %s", resp.StatusCode, body)
	}
	id := resp.Header.Get("X-Arc-Query-ID")
	if id == "" {
		t.Fatal("X-Arc-Query-ID header missing on the Arrow response")
	}
	if n := reg.ActiveCount(); n != 0 {
		t.Fatalf("active queries after completion = %d, want 0", n)
	}
	hist := reg.GetHistory(10)
	if len(hist) != 1 || hist[0].ID != id {
		t.Fatalf("history = %+v, want the one Arrow query %s", hist, id)
	}
	if hist[0].Status != queryregistry.StatusCompleted || hist[0].RowCount != 5 {
		t.Fatalf("history entry = status %q rows %d, want completed/5", hist[0].Status, hist[0].RowCount)
	}
}

// TestExecuteQueryArrow_FailIsRecorded (regression, #309): a query that fails
// before streaming leaves a failed entry, not a running one.
func TestExecuteQueryArrow_FailIsRecorded(t *testing.T) {
	app, reg := newArrowRegistryRig(t, 30*time.Second, 0)
	req := httptest.NewRequest("POST", "/api/v1/query/arrow", strings.NewReader(`{"sql":"SELECT * FROM this_measurement_does_not_exist"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 500 {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	if n := reg.ActiveCount(); n != 0 {
		t.Fatalf("active queries after failure = %d, want 0 (entry leaked as running)", n)
	}
	hist := reg.GetHistory(10)
	if len(hist) != 1 || hist[0].Status != queryregistry.StatusFailed {
		t.Fatalf("history = %+v, want one failed entry", hist)
	}
}

// TestExecuteQueryArrow_CancelViaRegistry (regression, #309): cancelling the
// registry entry stops a running Arrow query. go-duckdb materializes the
// result inside QueryContext, so the cancel surfaces as an error return (500,
// "Query cancelled") rather than a truncated stream; the entry is recorded as
// cancelled by the registry and not overwritten. The handler timeout is the
// backstop: if cancellation did not propagate, the query would end as
// timed_out after 15 s instead.
func TestExecuteQueryArrow_CancelViaRegistry(t *testing.T) {
	// query.timeout=0 exercises the WithCancel arm that exists only so a
	// registry cancel still propagates; app.Test's bound is the backstop there.
	for name, timeout := range map[string]time.Duration{"with_timeout": 15 * time.Second, "no_timeout": 0} {
		t.Run(name, func(t *testing.T) { runArrowCancelTest(t, timeout) })
	}
}

func runArrowCancelTest(t *testing.T, timeout time.Duration) {
	app, reg := newArrowRegistryRig(t, timeout, 0)
	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if active := reg.GetActive(); len(active) == 1 {
				time.Sleep(300 * time.Millisecond) // let execution pass the pre-check
				reg.Cancel(active[0].ID)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	start := time.Now()
	// CPU-bound: md5 over 60M rows takes ~7 s on 8 threads; 300M rows on the
	// rig's 2 threads cannot finish inside the cancel window on any runner.
	req := httptest.NewRequest("POST", "/api/v1/query/arrow", strings.NewReader(`{"sql":"SELECT count(*) FROM range(300000000) t WHERE md5(CAST(range AS VARCHAR)) LIKE '%zzzz%'"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 60000)
	elapsed := time.Since(start)
	<-done
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 500 || !strings.Contains(string(body), "Query cancelled") {
		t.Fatalf("status = %d body = %s, want 500 with \"Query cancelled\"", resp.StatusCode, body)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("query took %v after cancel; cancellation did not reach DuckDB", elapsed)
	}
	if n := reg.ActiveCount(); n != 0 {
		t.Fatalf("active queries after cancel = %d, want 0", n)
	}
	hist := reg.GetHistory(10)
	if len(hist) != 1 || hist[0].Status != queryregistry.StatusCancelled {
		t.Fatalf("history = %+v, want one cancelled entry", hist)
	}
}

// TestExecuteQueryArrow_SlowQueryLogged (regression, #309): the slow-query
// threshold applies to Arrow queries and moves the slow-query counter.
func TestExecuteQueryArrow_SlowQueryLogged(t *testing.T) {
	app, _ := newArrowRegistryRig(t, 30*time.Second, time.Nanosecond)
	before := queryMetricInt(t, "query_slow_total")
	req := httptest.NewRequest("POST", "/api/v1/query/arrow", strings.NewReader(`{"sql":"SELECT 1 AS id"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if after := queryMetricInt(t, "query_slow_total"); after != before+1 {
		t.Fatalf("query_slow_total = %d, want %d (Arrow query over the threshold was not logged as slow)", after, before+1)
	}
}

// Guard (passes on main): without a registry the endpoint is unchanged.
func TestExecuteQueryArrow_NoRegistryNoHeader(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, _ := storage.NewLocalBackend(tmpDir, logger)
	duckdb, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 2, LocalStorageRoot: tmpDir}, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { duckdb.Close() })
	h := &QueryHandler{db: duckdb, logger: logger, storage: backend, queryCache: database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize), pruner: pruning.NewPartitionPruner(zerolog.Nop())}
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Post("/api/v1/query/arrow", h.executeQueryArrow)
	req := httptest.NewRequest("POST", "/api/v1/query/arrow", strings.NewReader(`{"sql":"SELECT 1 AS id"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("X-Arc-Query-ID") != "" {
		t.Fatalf("status %d, header %q: no registry must mean no header", resp.StatusCode, resp.Header.Get("X-Arc-Query-ID"))
	}
}
