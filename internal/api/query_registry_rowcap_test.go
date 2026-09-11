//go:build duckdb_arrow

package api

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/queryregistry"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Tests for #728. #724 made a governance row cap visible on the wire and in
// operator logs but left the query registry alone, so GET /api/v1/queries/history
// showed a capped query as an ordinary success carrying the truncated count.
//
// These drive the real handler rather than the registry directly: the failure
// mode is a wrong or missing cap at the call site, which a registry unit test
// cannot see (the argument is positional, so omitting it would not compile).

func newRowCapRegistryHandler(t *testing.T, policy *governance.Policy) (*QueryHandler, *queryregistry.Registry) {
	t.Helper()
	m := newGovernanceTestManager(t, policy)
	h := newGovernanceTestHandler(m, 0)
	reg := queryregistry.NewRegistry(&queryregistry.RegistryConfig{HistorySize: 50}, zerolog.Nop())
	h.queryRegistry = reg
	return h, reg
}

// runQueryReturningRows drives POST /api/v1/query with the Arrow JSON dispatch
// stubbed to report rowsReturned rows, which is how a real capped stream
// reports itself to the registry.
func runQueryReturningRows(t *testing.T, h *QueryHandler, rowsReturned int) {
	t.Helper()
	metrics.Init(zerolog.Nop())

	origLicensed := queryGovernanceLicensed
	queryGovernanceLicensed = func(*QueryHandler) bool { return true }
	t.Cleanup(func() { queryGovernanceLicensed = origLicensed })

	origArrow := arrowJSONQueryFunc
	arrowJSONQueryFunc = func(h *QueryHandler, c *fiber.Ctx, ctx context.Context, cancel context.CancelFunc,
		convertedSQL string, profileMode bool, governanceMaxRows int, start time.Time, timestamp string,
		onComplete func(int), onFail func(string), onTimeout func()) (int, bool) {
		if cancel != nil {
			cancel()
		}
		if onComplete != nil {
			onComplete(rowsReturned)
		}
		return rowsReturned, true
	}
	t.Cleanup(func() { arrowJSONQueryFunc = origArrow })

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("token_info", &auth.TokenInfo{ID: 42, Name: "capped-token"})
		return c.Next()
	})
	app.Post("/api/v1/query", h.executeQuery)

	req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(`{"sql":"SELECT 1 AS id"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	resp.Body.Close()
}

func TestQueryHistoryRecordsGovernanceRowCap(t *testing.T) {
	t.Run("a capped query carries the cap in history", func(t *testing.T) {
		h, reg := newRowCapRegistryHandler(t, &governance.Policy{TokenID: 42, MaxRowsPerQuery: 7})
		runQueryReturningRows(t, h, 7) // reached the cap exactly

		hist := reg.GetHistory(10)
		if len(hist) != 1 {
			t.Fatalf("history has %d entries, want 1", len(hist))
		}
		e := hist[0]
		if e.RowCap != 7 {
			t.Errorf("RowCap = %d, want 7; history still shows a capped query as an ordinary success", e.RowCap)
		}
		if e.RowCount != 7 {
			t.Errorf("RowCount = %d, want 7", e.RowCount)
		}
		// The cap is a property of the result, not a different outcome: the
		// query did finish, and every other status value means it did not.
		if e.Status != queryregistry.StatusCompleted {
			t.Errorf("Status = %q, want completed", e.Status)
		}
	})

	t.Run("an uncapped query records no cap", func(t *testing.T) {
		h, reg := newRowCapRegistryHandler(t, &governance.Policy{TokenID: 42, MaxRowsPerQuery: 7})
		runQueryReturningRows(t, h, 3) // well under the cap

		hist := reg.GetHistory(10)
		if len(hist) != 1 {
			t.Fatalf("history has %d entries, want 1", len(hist))
		}
		if hist[0].RowCap != 0 {
			t.Errorf("RowCap = %d, want 0 for a result that never reached the cap", hist[0].RowCap)
		}
	})

	t.Run("no policy means no cap recorded", func(t *testing.T) {
		h, reg := newRowCapRegistryHandler(t, nil)
		runQueryReturningRows(t, h, 5)

		hist := reg.GetHistory(10)
		if len(hist) != 1 {
			t.Fatalf("history has %d entries, want 1", len(hist))
		}
		if hist[0].RowCap != 0 {
			t.Errorf("RowCap = %d, want 0 when no governance policy applies", hist[0].RowCap)
		}
	})
}
