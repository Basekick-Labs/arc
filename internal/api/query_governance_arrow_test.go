//go:build duckdb_arrow

package api

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/gofiber/fiber/v2"
)

// TestExecuteQueryArrowGovernanceRateLimit pins that POST /api/v1/query/arrow
// consults governance (#702). The Arrow IPC endpoint previously executed
// arbitrary user SQL with no rate limit, quota, row cap, or policy timeout.
// The budget is pre-consumed through the manager so the handler rejects
// before ever touching h.db; the MaxRows slice in the IPC stream loop is
// covered by the live end-to-end run (it needs a real Arrow reader).
func TestExecuteQueryArrowGovernanceRateLimit(t *testing.T) {
	m := newGovernanceTestManager(t, &governance.Policy{TokenID: 42, RateLimitPerMinute: 1})
	h := newGovernanceTestHandler(m, 0)
	app := newGovernanceTestApp(t, h, &auth.TokenInfo{ID: 42, Name: "limited"}, nil, nil)
	app.Post("/api/v1/query/arrow", h.executeQueryArrow)

	if res := m.CheckRateLimit(42); !res.Allowed {
		t.Fatal("precondition failed: first rate-limit slot should be allowed")
	}

	req := httptest.NewRequest("POST", "/api/v1/query/arrow", strings.NewReader(`{"sql":"SELECT * FROM default.cpu"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != fiber.StatusTooManyRequests {
		t.Fatalf("arrow query over the rate limit: got %d, want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("429 response is missing the Retry-After header")
	}
}
