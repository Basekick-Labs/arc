package api

import (
	"context"
	"database/sql"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	_ "github.com/mattn/go-sqlite3"
)

// Tests for #702: GET /api/v1/query/:measurement must enforce the same
// Enterprise query-governance policy set as POST /api/v1/query — rate
// limits, quotas, the MaxRows streaming cap, and the MaxDuration timeout
// override. The license gate is opened through the queryGovernanceLicensed
// seam because license.Client carries no injectable license.

func newGovernanceTestManager(t *testing.T, policy *governance.Policy) *governance.Manager {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	m, err := governance.NewManager(&governance.ManagerConfig{
		DB:     db,
		Config: &config.GovernanceConfig{},
		Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	m.Start()
	t.Cleanup(func() { m.Stop() })
	if policy != nil {
		if _, err := m.CreatePolicy(context.Background(), policy); err != nil {
			t.Fatal(err)
		}
	}
	return m
}

// newGovernanceTestApp opens the license seam, stubs the Arrow dispatch to
// capture the governance arguments, and returns an app whose requests carry
// the given token.
func newGovernanceTestApp(t *testing.T, h *QueryHandler, token *auth.TokenInfo, gotMaxRows *int, gotCtx *context.Context) *fiber.App {
	t.Helper()
	metrics.Init(zerolog.Nop())

	origLicensed := queryGovernanceLicensed
	queryGovernanceLicensed = func(*QueryHandler) bool { return true }
	t.Cleanup(func() { queryGovernanceLicensed = origLicensed })

	origArrow := arrowJSONQueryFunc
	arrowJSONQueryFunc = func(h *QueryHandler, c *fiber.Ctx, ctx context.Context, cancel context.CancelFunc, convertedSQL string, profileMode bool, governanceMaxRows int, start time.Time, timestamp string, onComplete func(int), onFail func(string), onTimeout func()) (int, bool) {
		if gotMaxRows != nil {
			*gotMaxRows = governanceMaxRows
		}
		if gotCtx != nil {
			*gotCtx = ctx
		}
		if cancel != nil {
			cancel()
		}
		return 0, true
	}
	t.Cleanup(func() { arrowJSONQueryFunc = origArrow })

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("token_info", token)
		return c.Next()
	})
	app.Get("/api/v1/query/:measurement", h.queryMeasurement)
	return app
}

func newGovernanceTestHandler(m *governance.Manager, queryTimeout time.Duration) *QueryHandler {
	return &QueryHandler{
		logger:            zerolog.Nop(),
		queryTimeout:      queryTimeout,
		queryCache:        database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize),
		storage:           &mockLocalBackend{basePath: "./data"},
		pruner:            pruning.NewPartitionPruner(zerolog.Nop()),
		governanceManager: m,
	}
}

func TestQueryMeasurementGovernanceRateLimit(t *testing.T) {
	m := newGovernanceTestManager(t, &governance.Policy{TokenID: 42, RateLimitPerMinute: 2})
	h := newGovernanceTestHandler(m, 0)
	app := newGovernanceTestApp(t, h, &auth.TokenInfo{ID: 42, Name: "limited"}, nil, nil)

	for i := 1; i <= 2; i++ {
		resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/query/cpu?database=default", nil))
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("request %d within the rate limit: got %d, want 200", i, resp.StatusCode)
		}
	}

	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/query/cpu?database=default", nil))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != fiber.StatusTooManyRequests {
		t.Fatalf("request over the rate limit: got %d, want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("429 response is missing the Retry-After header")
	}
}

func TestQueryMeasurementGovernanceQuota(t *testing.T) {
	m := newGovernanceTestManager(t, &governance.Policy{TokenID: 42, MaxQueriesPerHour: 1})
	h := newGovernanceTestHandler(m, 0)
	app := newGovernanceTestApp(t, h, &auth.TokenInfo{ID: 42, Name: "quotaed"}, nil, nil)

	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/query/cpu?database=default", nil))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("request within quota: got %d, want 200", resp.StatusCode)
	}

	resp, err = app.Test(httptest.NewRequest("GET", "/api/v1/query/cpu?database=default", nil))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != fiber.StatusTooManyRequests {
		t.Fatalf("request over quota: got %d, want 429", resp.StatusCode)
	}
}

func TestQueryMeasurementGovernanceMaxRowsAndTimeout(t *testing.T) {
	const globalTimeout = 5 * time.Minute
	m := newGovernanceTestManager(t, &governance.Policy{
		TokenID:            42,
		MaxRowsPerQuery:    7,
		MaxScanDurationSec: 2,
	})
	h := newGovernanceTestHandler(m, globalTimeout)

	var gotMaxRows int
	var gotCtx context.Context
	app := newGovernanceTestApp(t, h, &auth.TokenInfo{ID: 42, Name: "capped"}, &gotMaxRows, &gotCtx)

	before := time.Now()
	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/query/cpu?database=default", nil))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if gotMaxRows != 7 {
		t.Errorf("governanceMaxRows passed to Arrow dispatch: got %d, want 7", gotMaxRows)
	}
	if gotCtx == nil {
		t.Fatal("Arrow dispatch was not invoked with a context")
	}
	deadline, ok := gotCtx.Deadline()
	if !ok {
		t.Fatal("context has no deadline; effectiveTimeout was not applied")
	}
	// The policy's 2s MaxDuration must override the 5m global timeout.
	if deadline.After(before.Add(10 * time.Second)) {
		t.Errorf("deadline %v reflects the global timeout; governance MaxDuration did not override", deadline)
	}
}

// TestEstimateQueryGovernanceRateLimit pins that POST /api/v1/query/estimate
// consults governance (#702): its COUNT(*) wrapper executes the full user
// subquery, so it must be rate-limited like any other user-SQL endpoint. The
// budget is pre-consumed through the manager so the handler rejects before
// ever touching h.db.
func TestEstimateQueryGovernanceRateLimit(t *testing.T) {
	m := newGovernanceTestManager(t, &governance.Policy{TokenID: 42, RateLimitPerMinute: 1})
	h := newGovernanceTestHandler(m, 0)
	app := newGovernanceTestApp(t, h, &auth.TokenInfo{ID: 42, Name: "limited"}, nil, nil)
	app.Post("/api/v1/query/estimate", h.estimateQuery)

	if res := m.CheckRateLimit(42); !res.Allowed {
		t.Fatal("precondition failed: first rate-limit slot should be allowed")
	}

	req := httptest.NewRequest("POST", "/api/v1/query/estimate", strings.NewReader(`{"sql":"SELECT * FROM default.cpu"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusTooManyRequests {
		t.Fatalf("estimate over the rate limit: got %d, want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("429 response is missing the Retry-After header")
	}
	// The 429 must use the endpoint's own error shape: warning_level is the
	// estimate endpoint's severity channel for every error class.
	if !strings.Contains(string(body), `"warning_level":"error"`) {
		t.Errorf("429 body missing warning_level=error: %s", body)
	}
}

func TestQueryMeasurementGovernanceNoToken(t *testing.T) {
	m := newGovernanceTestManager(t, &governance.Policy{TokenID: 42, RateLimitPerMinute: 1})
	h := newGovernanceTestHandler(m, 0)
	// nil token: internal paths carry no token_info; governance must be a
	// no-op rather than a rejection or a panic.
	var gotCtx context.Context
	app := newGovernanceTestApp(t, h, nil, nil, &gotCtx)

	for i := 1; i <= 3; i++ {
		resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/query/cpu?database=default", nil))
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("tokenless request %d: got %d, want 200", i, resp.StatusCode)
		}
	}
	if _, ok := gotCtx.Deadline(); ok {
		t.Error("tokenless request with queryTimeout=0 must not carry a deadline")
	}
}
