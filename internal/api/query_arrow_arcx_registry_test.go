//go:build duckdb_arrow

package api

import (
	"bufio"
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/basekick-labs/arc/internal/queryregistry"
)

// TestArcxArrowRegistryLifecycle exercises the hand-off without linking the
// experimental native engine. The stock Arrow registry tests run separately.
func TestArcxArrowRegistryLifecycle(t *testing.T) {
	cases := []struct {
		name        string
		disposition string
		want        queryregistry.QueryStatus
		rows        int
	}{
		{"success", "complete", queryregistry.StatusCompleted, 7},
		{"failure", "fail", queryregistry.StatusFailed, 0},
		{"operator_cancel", "cancel", queryregistry.StatusCancelled, 0},
		{"deadline", "timeout", queryregistry.StatusTimedOut, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			timeout := 30 * time.Second
			if tc.disposition == "timeout" {
				timeout = 250 * time.Millisecond
			}
			app, reg := newArrowRegistryRig(t, timeout, 0)

			original := arcxArrowDispatch
			t.Cleanup(func() { arcxArrowDispatch = original })

			var dispatchedID string
			arcxArrowDispatch = func(
				_ *QueryHandler,
				c *fiber.Ctx,
				ctx context.Context,
				cancel context.CancelFunc,
				_, _, _ string,
				onComplete func(int),
				onFail func(string),
			) bool {
				active := reg.GetActive()
				if len(active) != 1 {
					t.Errorf("active at dispatch = %d, want 1", len(active))
					return false
				}
				dispatchedID = active[0].ID

				c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
					if cancel != nil {
						defer cancel()
					}

					switch tc.disposition {
					case "complete":
						_, _ = w.WriteString("ok")
						onComplete(7)
					case "fail":
						onFail("arcx stream failed")
					case "cancel":
						if !reg.Cancel(dispatchedID) {
							t.Error("registry cancellation failed")
						}
						if err := ctx.Err(); err != nil {
							onFail(err.Error())
						}
					case "timeout":
						<-ctx.Done()
						onFail(ctx.Err().Error())
					}
				})
				return true
			}

			req := httptest.NewRequest(
				"POST",
				"/api/v1/query/arrow",
				strings.NewReader(`{"sql":"SELECT 1 AS id"}`),
			)
			req.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(req, 10000)
			if err != nil {
				t.Fatalf("app.Test: %v", err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()

			if resp.StatusCode != 200 {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			if got := resp.Header.Get("X-Arc-Query-ID"); got == "" || got != dispatchedID {
				t.Fatalf("query ID = %q, dispatched ID = %q", got, dispatchedID)
			}
			if got := reg.ActiveCount(); got != 0 {
				t.Fatalf("active after stream = %d, want 0", got)
			}

			history := reg.GetHistory(10)
			if len(history) != 1 {
				t.Fatalf("history entries = %d, want 1", len(history))
			}
			if history[0].ID != dispatchedID ||
				history[0].Status != tc.want ||
				history[0].RowCount != tc.rows {
				t.Fatalf("history entry = %+v; want status %q, rows %d",
					history[0], tc.want, tc.rows)
			}
		})
	}
}

// A declined arcx hook must fall through to DuckDB without registering a
// second query or replacing the ID already announced to the client.
func TestArcxArrowDeclineUsesSameRegistryEntry(t *testing.T) {
	app, reg := newArrowRegistryRig(t, 30*time.Second, 0)

	original := arcxArrowDispatch
	t.Cleanup(func() { arcxArrowDispatch = original })

	var dispatchID string
	arcxArrowDispatch = func(
		_ *QueryHandler,
		_ *fiber.Ctx,
		ctx context.Context,
		_ context.CancelFunc,
		_, _, _ string,
		_ func(int),
		_ func(string),
	) bool {
		active := reg.GetActive()
		if len(active) != 1 {
			t.Errorf("active at dispatch = %d, want 1", len(active))
			return false
		}
		dispatchID = active[0].ID
		if err := ctx.Err(); err != nil {
			t.Errorf("dispatch context already cancelled: %v", err)
		}
		return false
	}

	req := httptest.NewRequest(
		"POST",
		"/api/v1/query/arrow",
		strings.NewReader(`{"sql":"SELECT 1 AS id"}`),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	if dispatchID == "" {
		t.Fatal("arcx dispatch did not see a registered query")
	}
	if got := resp.Header.Get("X-Arc-Query-ID"); got != dispatchID {
		t.Fatalf("response query ID = %q, dispatch ID = %q", got, dispatchID)
	}
	if got := reg.ActiveCount(); got != 0 {
		t.Fatalf("active queries after fallback = %d, want 0", got)
	}

	history := reg.GetHistory(10)
	if len(history) != 1 {
		t.Fatalf("history entries = %d, want 1", len(history))
	}
	if history[0].ID != dispatchID ||
		history[0].Status != queryregistry.StatusCompleted ||
		history[0].RowCount != 1 {
		t.Fatalf("unexpected fallback history: %+v", history[0])
	}
}
