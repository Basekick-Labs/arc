//go:build duckdb_arrow

package api

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/queryregistry"
	"github.com/gofiber/fiber/v2"
	recovermw "github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/rs/zerolog"
)

// Tests for #731. GET /api/v1/query/:measurement never registered with the
// query registry, so it was invisible to /api/v1/queries and could not be
// cancelled. Registering creates the obligation these tests guard: every exit
// must dispose, or the entry sits in "running" forever holding its SQL and
// inflating the active gauge, which is the shape #717 found elsewhere.

// measurementRegistryApp wires the real handler with a registry and stubs the
// Arrow JSON dispatch, which is the path that serves this endpoint in a
// duckdb_arrow build.
func measurementRegistryApp(t *testing.T, policy *governance.Policy,
	arrow func(*QueryHandler, *fiber.Ctx, context.Context, context.CancelFunc, string, bool, int,
		time.Time, string, func(int), func(string), func()) (int, bool),
) (*fiber.App, *queryregistry.Registry) {
	t.Helper()
	metrics.Init(zerolog.Nop())

	m := newGovernanceTestManager(t, policy)
	h := newGovernanceTestHandler(m, 0)
	reg := queryregistry.NewRegistry(&queryregistry.RegistryConfig{HistorySize: 50}, zerolog.Nop())
	h.queryRegistry = reg

	origLicensed := queryGovernanceLicensed
	queryGovernanceLicensed = func(*QueryHandler) bool { return true }
	t.Cleanup(func() { queryGovernanceLicensed = origLicensed })

	origArrow := arrowJSONQueryFunc
	arrowJSONQueryFunc = arrow
	t.Cleanup(func() { arrowJSONQueryFunc = origArrow })

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	// The real server installs recover middleware, so a handler panic is
	// contained there rather than reaching the caller.
	app.Use(recovermw.New())
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("token_info", &auth.TokenInfo{ID: 42, Name: "t"})
		return c.Next()
	})
	app.Get("/api/v1/query/:measurement", h.queryMeasurement)
	return app, reg
}

func doMeasurement(t *testing.T, app *fiber.App, method string) {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest(method, "/api/v1/query/cpu?database=default", nil), 10000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	resp.Body.Close()
}

func TestQueryMeasurementRegistersWithTheRegistry(t *testing.T) {
	t.Run("a successful query completes in history", func(t *testing.T) {
		app, reg := measurementRegistryApp(t, nil, func(h *QueryHandler, c *fiber.Ctx, ctx context.Context,
			cancel context.CancelFunc, sql string, pm bool, maxRows int, start time.Time, ts string,
			onComplete func(int), onFail func(string), onTimeout func()) (int, bool) {
			if cancel != nil {
				cancel()
			}
			onComplete(5)
			return 5, true
		})
		doMeasurement(t, app, "GET")

		if n := reg.ActiveCount(); n != 0 {
			t.Errorf("%d entries still listed as running; the query finished", n)
		}
		hist := reg.GetHistory(10)
		if len(hist) != 1 {
			t.Fatalf("history has %d entries, want 1", len(hist))
		}
		if hist[0].Status != queryregistry.StatusCompleted || hist[0].RowCount != 5 {
			t.Errorf("entry = %s/%d rows, want completed/5", hist[0].Status, hist[0].RowCount)
		}
		// The registered statement must be the readable one, not the
		// transformed SQL full of resolved storage globs.
		if hist[0].SQL == "" || len(hist[0].SQL) > 200 {
			t.Errorf("unexpected registered SQL: %q", hist[0].SQL)
		}
	})

	t.Run("a failed query does not sit in running forever", func(t *testing.T) {
		app, reg := measurementRegistryApp(t, nil, func(h *QueryHandler, c *fiber.Ctx, ctx context.Context,
			cancel context.CancelFunc, sql string, pm bool, maxRows int, start time.Time, ts string,
			onComplete func(int), onFail func(string), onTimeout func()) (int, bool) {
			if cancel != nil {
				cancel()
			}
			onFail("boom")
			return 0, true
		})
		doMeasurement(t, app, "GET")

		if n := reg.ActiveCount(); n != 0 {
			t.Fatalf("%d phantom entries left running after a failure", n)
		}
		hist := reg.GetHistory(10)
		if len(hist) != 1 || hist[0].Status != queryregistry.StatusFailed {
			t.Fatalf("history = %+v, want one failed entry", hist)
		}
	})

	t.Run("a timeout is recorded as timed_out, matching POST /api/v1/query", func(t *testing.T) {
		app, reg := measurementRegistryApp(t, nil, func(h *QueryHandler, c *fiber.Ctx, ctx context.Context,
			cancel context.CancelFunc, sql string, pm bool, maxRows int, start time.Time, ts string,
			onComplete func(int), onFail func(string), onTimeout func()) (int, bool) {
			if cancel != nil {
				cancel()
			}
			onTimeout()
			return 0, true
		})
		doMeasurement(t, app, "GET")

		hist := reg.GetHistory(10)
		if len(hist) != 1 || hist[0].Status != queryregistry.StatusTimedOut {
			t.Fatalf("history = %+v, want one timed_out entry", hist)
		}
	})

	t.Run("a handler panic disposes the entry instead of stranding it", func(t *testing.T) {
		app, reg := measurementRegistryApp(t, nil, func(h *QueryHandler, c *fiber.Ctx, ctx context.Context,
			cancel context.CancelFunc, sql string, pm bool, maxRows int, start time.Time, ts string,
			onComplete func(int), onFail func(string), onTimeout func()) (int, bool) {
			if cancel != nil {
				cancel()
			}
			// The dispatch panics before firing any disposition, the shape
			// safeStream recovers from.
			panic(errors.New("simulated writer panic"))
		})
		doMeasurement(t, app, "GET")

		if n := reg.ActiveCount(); n != 0 {
			t.Fatalf("%d entries left running after a panic; fiber's recover keeps the process alive but the entry would leak forever", n)
		}
		hist := reg.GetHistory(10)
		if len(hist) != 1 || hist[0].Status != queryregistry.StatusFailed {
			t.Fatalf("history = %+v, want one failed entry", hist)
		}
	})

	t.Run("HEAD creates no entry", func(t *testing.T) {
		// Fiber routes HEAD to the GET handler and fasthttp discards the
		// body, so registering would leave an entry whose only outcome is a
		// spurious connection failure.
		app, reg := measurementRegistryApp(t, nil, func(h *QueryHandler, c *fiber.Ctx, ctx context.Context,
			cancel context.CancelFunc, sql string, pm bool, maxRows int, start time.Time, ts string,
			onComplete func(int), onFail func(string), onTimeout func()) (int, bool) {
			if cancel != nil {
				cancel()
			}
			if onComplete != nil {
				onComplete(1)
			}
			return 1, true
		})
		doMeasurement(t, app, "HEAD")

		if n := len(reg.GetHistory(10)); n != 0 {
			t.Errorf("HEAD produced %d history entries, want 0", n)
		}
		if n := reg.ActiveCount(); n != 0 {
			t.Errorf("HEAD left %d entries running", n)
		}
	})

	t.Run("a capped result records the cap", func(t *testing.T) {
		app, reg := measurementRegistryApp(t, &governance.Policy{TokenID: 42, MaxRowsPerQuery: 4},
			func(h *QueryHandler, c *fiber.Ctx, ctx context.Context, cancel context.CancelFunc, sql string,
				pm bool, maxRows int, start time.Time, ts string,
				onComplete func(int), onFail func(string), onTimeout func()) (int, bool) {
				if cancel != nil {
					cancel()
				}
				onComplete(maxRows) // stream stopped at the cap
				return maxRows, true
			})
		doMeasurement(t, app, "GET")

		hist := reg.GetHistory(10)
		if len(hist) != 1 {
			t.Fatalf("history has %d entries, want 1", len(hist))
		}
		if hist[0].RowCap != 4 {
			t.Errorf("RowCap = %d, want 4 (#728 parity for this endpoint)", hist[0].RowCap)
		}
	})
}

var _ = database.QueryCacheTTL
