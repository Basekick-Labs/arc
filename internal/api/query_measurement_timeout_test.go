package api

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// TestQueryMeasurementAppliesQueryTimeout covers #308: GET /api/v1/query/:measurement
// previously executed against context.Background() and h.db.Query (no deadline).
// The handler must wrap UserContext with queryTimeout and pass the cancel func
// into the Arrow dispatch, matching POST /api/v1/query.
func TestQueryMeasurementAppliesQueryTimeout(t *testing.T) {
	metrics.Init(zerolog.Nop())

	orig := arrowJSONQueryFunc
	t.Cleanup(func() { arrowJSONQueryFunc = orig })

	const timeout = 5 * time.Second
	var gotCtx context.Context
	var gotCancel context.CancelFunc
	arrowJSONQueryFunc = func(h *QueryHandler, c *fiber.Ctx, ctx context.Context, cancel context.CancelFunc, convertedSQL string, profileMode bool, governanceMaxRows int, start time.Time, timestamp string, onComplete func(int), onFail func(string), onTimeout func()) (int, bool) {
		gotCtx = ctx
		gotCancel = cancel
		if cancel != nil {
			cancel()
		}
		return 0, true
	}

	h := &QueryHandler{
		logger:       zerolog.Nop(),
		queryTimeout: timeout,
		queryCache:   database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize),
		storage:      &mockLocalBackend{basePath: "./data"},
		pruner:       pruning.NewPartitionPruner(zerolog.Nop()),
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Get("/api/v1/query/:measurement", h.queryMeasurement)

	before := time.Now()
	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/query/cpu?database=default", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	resp.Body.Close()
	after := time.Now()

	if gotCtx == nil {
		t.Fatal("queryMeasurement did not invoke Arrow dispatch with a context")
	}
	if gotCancel == nil {
		t.Fatal("queryMeasurement did not pass a cancel func (timeout context was not applied)")
	}
	deadline, ok := gotCtx.Deadline()
	if !ok {
		t.Fatal("queryMeasurement context has no deadline; queryTimeout was not applied")
	}
	// Deadline should be approximately now+timeout, allowing for handler overhead.
	min := before.Add(timeout - 500*time.Millisecond)
	max := after.Add(timeout + 500*time.Millisecond)
	if deadline.Before(min) || deadline.After(max) {
		t.Errorf("deadline %v outside expected window [%v, %v]", deadline, min, max)
	}
}

// TestQueryMeasurementNoTimeoutKeepsUserContext covers queryTimeout=0 (disabled):
// still must not use context.Background(), so client disconnects can cancel work.
func TestQueryMeasurementNoTimeoutKeepsUserContext(t *testing.T) {
	metrics.Init(zerolog.Nop())

	orig := arrowJSONQueryFunc
	t.Cleanup(func() { arrowJSONQueryFunc = orig })

	var gotCtx context.Context
	var gotCancel context.CancelFunc
	arrowJSONQueryFunc = func(h *QueryHandler, c *fiber.Ctx, ctx context.Context, cancel context.CancelFunc, convertedSQL string, profileMode bool, governanceMaxRows int, start time.Time, timestamp string, onComplete func(int), onFail func(string), onTimeout func()) (int, bool) {
		gotCtx = ctx
		gotCancel = cancel
		return 0, true
	}

	h := &QueryHandler{
		logger:     zerolog.Nop(),
		queryCache: database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize),
		storage:    &mockLocalBackend{basePath: "./data"},
		pruner:     pruning.NewPartitionPruner(zerolog.Nop()),
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Get("/api/v1/query/:measurement", h.queryMeasurement)

	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/query/cpu?database=default", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	resp.Body.Close()

	if gotCtx == nil {
		t.Fatal("queryMeasurement did not invoke Arrow dispatch with a context")
	}
	if gotCancel != nil {
		t.Fatal("queryTimeout=0 should not allocate a timeout cancel func")
	}
	if _, ok := gotCtx.Deadline(); ok {
		t.Fatal("queryTimeout=0 must not attach a deadline")
	}
}
