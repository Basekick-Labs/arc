package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// #1628: a run with nothing new to process (a second run within the same
// second, or a watermark up to cqEndTimeSkew ahead) answers 200 with 0
// records, writes no execution row and leaves the watermark. A watermark
// further ahead, or an empty window the caller asked for, is still refused.

// cqClockAt fixes the clock a run derives its window from, half a second into
// a whole second so truncation matters.
func cqClockAt(t *testing.T) time.Time {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute).Add(500 * time.Millisecond)
	prev := cqNow
	cqNow = func() time.Time { return now }
	t.Cleanup(func() { cqNow = prev })
	return now
}

// newStandaloneCQRig is newCQRig without a cluster source.
func newStandaloneCQRig(t *testing.T) *cqRig {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "arc.db")
	am, err := auth.NewAuthManager(dbPath, time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	h, err := NewContinuousQueryHandler(nil, nil, nil, &config.ContinuousQueryConfig{Enabled: true, DBPath: dbPath}, am, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	r := &cqRig{h: h}
	h.aggregateHook = func(_ context.Context, _ *ContinuousQuery, _ string, start time.Time) (int64, error) {
		r.runs = append(r.runs, start)
		return 1, nil
	}
	r.app = fiber.New()
	r.app.Post("/cq", h.handleCreate)
	r.app.Post("/cq/:id/execute", h.handleExecute)
	return r
}

// localWatermark is the standalone CQ's last_processed_time.
func (r *cqRig) localWatermark(t *testing.T, id int64) *time.Time {
	t.Helper()
	cq, err := r.h.getQuery(id)
	if err != nil {
		t.Fatal(err)
	}
	return lastProcessedOf(cq)
}

func (r *cqRig) setLocalWatermark(t *testing.T, id int64, w time.Time) {
	t.Helper()
	if _, err := r.h.sqliteDB.Exec("UPDATE continuous_queries SET last_processed_time = ? WHERE id = ?", w.Format(time.RFC3339), id); err != nil {
		t.Fatal(err)
	}
}

func (r *cqRig) executionRows(t *testing.T, id int64) int {
	t.Helper()
	var n int
	if err := r.h.sqliteDB.QueryRow("SELECT COUNT(*) FROM continuous_query_executions WHERE query_id = ?", id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// rawExecute posts an execute and returns the status and the raw body.
func (r *cqRig) rawExecute(t *testing.T, id, body string) (int, []byte) {
	t.Helper()
	req := httptest.NewRequest("POST", "/cq/"+id+"/execute", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// checkSDKShape pins the keys and JSON types the Python SDK's
// ExecuteCQResponse requires (arc-client-python models/continuous_query.py).
func checkSDKShape(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("body %s: %v", raw, err)
	}
	for _, k := range []string{"query_name", "execution_id", "status", "start_time", "end_time", "destination_measurement", "executed_at"} {
		if s, ok := out[k].(string); !ok || s == "" {
			t.Fatalf("%q missing or not a non-empty string in %s", k, raw)
		}
	}
	for _, k := range []string{"query_id", "records_written", "execution_time_seconds"} {
		if _, ok := out[k].(float64); !ok {
			t.Fatalf("%q missing or not a number in %s", k, raw)
		}
	}
	if v, ok := out["records_read"]; !ok || v != nil {
		t.Fatalf("records_read should be null: %s", raw)
	}
	if _, ok := out["dry_run"].(bool); !ok {
		t.Fatalf("dry_run missing: %s", raw)
	}
	return out
}

func TestCQSameSecondRerunIsAnEmptyWindow(t *testing.T) {
	for _, mode := range []string{"standalone", "cluster"} {
		t.Run(mode, func(t *testing.T) {
			now := cqClockAt(t)
			var r *cqRig
			if mode == "cluster" {
				r = newCQRig(t)
				r.src.seedCQ()
			} else {
				r = newStandaloneCQRig(t)
			}
			wm := func() *time.Time {
				if r.src != nil {
					return r.watermark(t, 1)
				}
				return r.localWatermark(t, 1)
			}
			proposals := func() int {
				if r.src != nil {
					return r.src.watermarkProposals
				}
				return 0
			}
			if code, body := r.do(t, "POST", "/cq", cqBody); code != fiber.StatusCreated {
				t.Fatalf("create: %d %v", code, body)
			}
			if code, raw := r.rawExecute(t, "1", `{}`); code != fiber.StatusOK {
				t.Fatalf("first run: %d %s", code, raw)
			}
			w := wm()
			if w == nil || !w.Equal(now.Truncate(time.Second)) {
				t.Fatalf("watermark %v after the first run", w)
			}
			rows, props := r.executionRows(t, 1), proposals()

			// The same second again: nothing new.
			code, raw := r.rawExecute(t, "1", `{}`)
			if code != fiber.StatusOK {
				t.Fatalf("same-second re-run: %d %s", code, raw)
			}
			out := checkSDKShape(t, raw)
			ws := w.Format(time.RFC3339)
			if out["records_written"].(float64) != 0 || out["status"] != "completed" || out["start_time"] != ws || out["end_time"] != ws || out["dry_run"] != false {
				t.Fatalf("same-second re-run: %s", raw)
			}
			if _, ok := out["watermark_status"]; ok {
				t.Fatalf("watermark_status on a skipped run: %s", raw)
			}
			if _, ok := out["Skipped"]; ok || out["skipped"] != nil {
				t.Fatalf("Skipped serialized: %s", raw)
			}

			// And the scheduled path, in the same second.
			resp, err := r.h.ExecuteCQ(context.Background(), 1)
			if err != nil || resp == nil || !resp.Skipped || resp.RecordsWritten != 0 || resp.StartTime != ws || resp.EndTime != ws || resp.Status != "completed" {
				t.Fatalf("scheduled same-second run: %+v %v", resp, err)
			}

			if len(r.runs) != 1 {
				t.Fatalf("%d aggregations, want only the first", len(r.runs))
			}
			if n := r.executionRows(t, 1); n != rows {
				t.Fatalf("%d execution rows, want %d (none for an empty window)", n, rows)
			}
			if p := proposals(); p != props {
				t.Fatalf("%d watermark proposals for empty windows", p-props)
			}
			if w2 := wm(); w2 == nil || !w2.Equal(*w) {
				t.Fatalf("watermark %v, want %v unchanged", w2, w)
			}
		})
	}
}

// A watermark ahead of now is quiet up to cqEndTimeSkew and never moved
// back; beyond it, the 400 and the scheduler's error stay as they were.
func TestCQFutureWatermarkBoundary(t *testing.T) {
	now := cqClockAt(t) // x.5 s; now + skew = second x+60, plus half a second
	base := now.Truncate(time.Second)
	for _, c := range []struct {
		name  string
		ahead time.Duration
		quiet bool
	}{
		{"30s", 30 * time.Second, true},
		{"just inside the skew", cqEndTimeSkew, true},
		{"just outside the skew", cqEndTimeSkew + time.Second, false},
		{"5m", 5 * time.Minute, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newStandaloneCQRig(t)
			r.do(t, "POST", "/cq", cqBody)
			w := base.Add(c.ahead)
			r.setLocalWatermark(t, 1, w)

			code, raw := r.rawExecute(t, "1", `{}`)
			resp, err := r.h.ExecuteCQ(context.Background(), 1)
			if c.quiet {
				if code != fiber.StatusOK {
					t.Fatalf("manual: %d %s", code, raw)
				}
				out := checkSDKShape(t, raw)
				if out["records_written"].(float64) != 0 || out["start_time"] != w.Format(time.RFC3339) || out["end_time"] != w.Format(time.RFC3339) {
					t.Fatalf("manual: %s", raw)
				}
				if err != nil || resp == nil || !resp.Skipped {
					t.Fatalf("scheduled: %+v %v", resp, err)
				}
			} else {
				if code != fiber.StatusBadRequest || string(raw) != `{"error":"start_time must be before end_time"}` {
					t.Fatalf("manual: %d %s, want today's 400", code, raw)
				}
				if err == nil || err.Error() != "start_time must be before end_time" || resp != nil {
					t.Fatalf("scheduled: %+v %v, want today's error", resp, err)
				}
			}
			if len(r.runs) != 0 || r.executionRows(t, 1) != 0 {
				t.Fatalf("runs %v rows %d", r.runs, r.executionRows(t, 1))
			}
			if got := r.localWatermark(t, 1); got == nil || !got.Equal(w) {
				t.Fatalf("watermark %v, want %v left as it is", got, w)
			}
		})
	}
}

// In a cluster a watermark ahead within the skew is neither proposed nor
// moved back.
func TestCQFutureWatermarkClusterNoProposal(t *testing.T) {
	now := cqClockAt(t)
	r := newCQRig(t)
	r.src.seedCQ()
	r.do(t, "POST", "/cq", cqBody)
	w := now.Truncate(time.Second).Add(30 * time.Second)
	r.src.must(raft.CommandSetCQWatermark, raft.SetCQWatermarkPayload{Proposer: "n1", ID: 1, To: w})
	before := r.src.watermarkProposals
	if code, raw := r.rawExecute(t, "1", `{}`); code != fiber.StatusOK {
		t.Fatalf("manual: %d %s", code, raw)
	}
	if resp, err := r.h.ExecuteCQ(context.Background(), 1); err != nil || !resp.Skipped {
		t.Fatalf("scheduled: %+v %v", resp, err)
	}
	if r.src.watermarkProposals != before || len(r.runs) != 0 || r.executionRows(t, 1) != 0 {
		t.Fatalf("proposals %d runs %d rows %d", r.src.watermarkProposals-before, len(r.runs), r.executionRows(t, 1))
	}
	if got := r.watermark(t, 1); got == nil || !got.Equal(w) {
		t.Fatalf("watermark %v, want %v", got, w)
	}
}

// An empty window the caller asked for keeps its 400 and text.
func TestCQExplicitEmptyWindowStillRefused(t *testing.T) {
	now := cqClockAt(t)
	base := now.Truncate(time.Second)
	r := newStandaloneCQRig(t)
	r.do(t, "POST", "/cq", cqBody)
	r.setLocalWatermark(t, 1, base)
	const want = `{"error":"start_time must be before end_time"}`
	for name, body := range map[string]string{
		"start equals end":                `{"start_time":"` + base.Add(-time.Hour).Format(time.RFC3339) + `","end_time":"` + base.Add(-time.Hour).Format(time.RFC3339) + `"}`,
		"end before the watermark":        `{"end_time":"` + base.Add(-time.Minute).Format(time.RFC3339) + `"}`,
		"start at the watermark, end now": `{"start_time":"` + base.Format(time.RFC3339) + `"}`,
	} {
		if code, raw := r.rawExecute(t, "1", body); code != fiber.StatusBadRequest || string(raw) != want {
			t.Fatalf("%s: %d %s", name, code, raw)
		}
	}
	if len(r.runs) != 0 || r.executionRows(t, 1) != 0 {
		t.Fatalf("runs %v rows %d", r.runs, r.executionRows(t, 1))
	}
}

// A dry run over an empty derived window shows the query over that window.
func TestCQDryRunEmptyWindow(t *testing.T) {
	now := cqClockAt(t)
	w := now.Truncate(time.Second)
	r := newStandaloneCQRig(t)
	r.do(t, "POST", "/cq", cqBody)
	r.setLocalWatermark(t, 1, w)
	code, raw := r.rawExecute(t, "1", `{"dry_run":true}`)
	if code != fiber.StatusOK {
		t.Fatalf("dry run: %d %s", code, raw)
	}
	out := checkSDKShape(t, raw)
	ws := w.Format(time.RFC3339)
	q, _ := out["executed_query"].(string)
	if out["status"] != "dry_run" || out["dry_run"] != true || out["start_time"] != ws || out["end_time"] != ws ||
		!strings.Contains(q, "time >= '"+ws+"' AND time < '"+ws+"'") {
		t.Fatalf("dry run: %s", raw)
	}
	if len(r.runs) != 0 || r.executionRows(t, 1) != 0 {
		t.Fatalf("runs %v rows %d", r.runs, r.executionRows(t, 1))
	}
}
