package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// fakeJobSyncer records what SyncJobs is handed.
type fakeJobSyncer struct {
	mu   sync.Mutex
	defs [][]CQJobDef
}

func (f *fakeJobSyncer) ReloadCQ(int64) error                             { return nil }
func (f *fakeJobSyncer) StartJobDirect(int64, string, string, bool) error { return nil }
func (f *fakeJobSyncer) SyncJobs(d []CQJobDef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.defs = append(f.defs, d)
}

type cqRig struct {
	h    *ContinuousQueryHandler
	src  *fsmSource
	app  *fiber.App
	runs []time.Time // window starts the aggregation saw
}

func newCQRig(t *testing.T) *cqRig {
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
	r := &cqRig{h: h, src: newFSMSource(t, "n1")}
	h.aggregateHook = func(_ context.Context, _ *ContinuousQuery, _ string, start time.Time) (int64, error) {
		r.runs = append(r.runs, start)
		return 1, nil
	}
	h.SetClusterSource(r.src)
	r.app = fiber.New()
	r.app.Post("/cq", h.handleCreate)
	r.app.Get("/cq", h.handleList)
	r.app.Get("/cq/:id", h.handleGet)
	r.app.Put("/cq/:id", h.handleUpdate)
	r.app.Delete("/cq/:id", h.handleDelete)
	r.app.Post("/cq/:id/execute", h.handleExecute)
	return r
}

func (r *cqRig) do(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func (r *cqRig) watermark(t *testing.T, id int64) *time.Time {
	t.Helper()
	e, ok := r.src.ClusterCQ(id)
	if !ok {
		t.Fatalf("CQ %d gone", id)
	}
	return e.LastProcessed
}

const cqBody = `{"name":"q","database":"db","source_measurement":"cpu","destination_measurement":"cpu_1h","query":"SELECT count(*) AS c FROM db.cpu WHERE time >= {start_time} AND time < {end_time}","interval":"1h"}`

// Once adopted the API reads and writes the cluster's CQs, and the run's
// watermark is the cluster's: a run starts where the cluster says, and moves
// it to its end.
func TestCQClusterAPIAndRun(t *testing.T) {
	r := newCQRig(t)
	r.src.seedCQ()
	code, body := r.do(t, "POST", "/cq", cqBody)
	if code != fiber.StatusCreated || body["id"].(float64) != 1 || body["is_active"] != true || body["last_processed_time"] != nil {
		t.Fatalf("create: %d %v", code, body)
	}
	if code, _ := r.do(t, "POST", "/cq", cqBody); code != fiber.StatusBadRequest {
		t.Fatalf("duplicate name: %d", code)
	}
	if code, body := r.do(t, "PUT", "/cq/1", `{"interval":"30m"}`); code != fiber.StatusOK || body["interval"] != "30m" || body["source_measurement"] != "cpu" {
		t.Fatalf("partial update: %d %v", code, body)
	}
	if code, _ := r.do(t, "PUT", "/cq/9", `{"interval":"30m"}`); code != fiber.StatusNotFound {
		t.Fatalf("missing: %d", code)
	}

	// The first run starts an hour back (no watermark), the second where the
	// first ended.
	if _, err := r.h.ExecuteCQ(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	w1 := r.watermark(t, 1)
	if w1 == nil || len(r.runs) != 1 || time.Since(r.runs[0]) < 59*time.Minute {
		t.Fatalf("first run: start %v, watermark %v", r.runs, w1)
	}
	if !w1.Equal(w1.Truncate(time.Second)) {
		t.Fatal("watermark not at whole seconds")
	}
	time.Sleep(1100 * time.Millisecond)
	if _, err := r.h.ExecuteCQ(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if !r.runs[1].Equal(*w1) {
		t.Fatalf("second run started at %v, want the cluster's watermark %v", r.runs[1], w1)
	}
	if w2 := r.watermark(t, 1); w2 == nil || !w2.After(*w1) {
		t.Fatalf("the second run did not move the watermark past %v (now %v)", w1, w2)
	}
	if code, body := r.do(t, "GET", "/cq/1", ""); code != fiber.StatusOK || body["last_processed_time"] != r.watermark(t, 1).Format(time.RFC3339) || body["last_execution_status"] != "completed" {
		t.Fatalf("get: %d %v", code, body)
	}
	if code, _ := r.do(t, "DELETE", "/cq/1", ""); code != fiber.StatusOK {
		t.Fatalf("delete: %d", code)
	}
	if code, _ := r.do(t, "GET", "/cq/1", ""); code != fiber.StatusNotFound {
		t.Fatalf("get after delete: %d", code)
	}
}

// A watermark the cluster did not take is settled before the next run: the
// run proposes it again first, and is skipped when it still cannot.
func TestCQPendingWatermarkSettledFirst(t *testing.T) {
	r := newCQRig(t)
	r.src.seedCQ()
	r.do(t, "POST", "/cq", cqBody)
	r.src.dropWatermarks = 1
	if _, err := r.h.ExecuteCQ(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if r.watermark(t, 1) != nil {
		t.Fatal("the dropped watermark reached the cluster")
	}
	// Still not reaching the cluster: the next run is skipped, not run from
	// the old watermark (a new primary would re-run both windows).
	r.src.dropWatermarks = 1
	if _, err := r.h.ExecuteCQ(context.Background(), 1); !errors.Is(err, errCQWatermarkPending) {
		t.Fatalf("run with an unsettled watermark: %v", err)
	}
	if len(r.runs) != 1 {
		t.Fatalf("%d runs, want the skipped one not to aggregate", len(r.runs))
	}
	// Reaching it: settled, then the run starts where the first run ended.
	time.Sleep(1100 * time.Millisecond) // a window of at least a second
	if _, err := r.h.ExecuteCQ(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if len(r.runs) != 2 || r.watermark(t, 1) == nil {
		t.Fatalf("runs %v watermark %v", r.runs, r.watermark(t, 1))
	}
}

// A watermark moved by someone else while a run was out (a manual execute's
// end_time, another primary) wins: the run's compare-and-set does not undo
// it, and no pending entry is left to re-propose it later.
func TestCQWatermarkMovedMeanwhileWins(t *testing.T) {
	r := newCQRig(t)
	r.src.seedCQ()
	r.do(t, "POST", "/cq", cqBody)
	reset := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Second)
	r.h.aggregateHook = func(context.Context, *ContinuousQuery, string, time.Time) (int64, error) {
		r.src.must(raft.CommandSetCQWatermark, raft.SetCQWatermarkPayload{Proposer: "n1", ID: 1, To: reset})
		return 1, nil
	}
	if _, err := r.h.ExecuteCQ(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if w := r.watermark(t, 1); w == nil || !w.Equal(reset) {
		t.Fatalf("watermark %v, want the reset %v kept", w, reset)
	}
	r.h.cl.runsMu.Lock()
	n := len(r.h.cl.pending)
	r.h.cl.runsMu.Unlock()
	if n != 0 {
		t.Fatal("a pending entry would re-propose the run's watermark over the reset")
	}
}

// A manual execute moves the watermark to its end_time, backwards included,
// and refuses an end_time more than a minute ahead.
func TestCQManualExecuteWatermark(t *testing.T) {
	r := newCQRig(t)
	r.src.seedCQ()
	r.do(t, "POST", "/cq", cqBody)
	future := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	if code, body := r.do(t, "POST", "/cq/1/execute", `{"end_time":"`+future+`"}`); code != fiber.StatusBadRequest || !strings.Contains(body["error"].(string), "future") {
		t.Fatalf("future end_time: %d %v", code, body)
	}
	if code, _ := r.do(t, "POST", "/cq/1/execute", `{"end_time":"`+future+`","dry_run":true}`); code != fiber.StatusOK {
		t.Fatalf("a dry run sets no watermark, so a future end is fine: %d", code)
	}
	end := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	start := end.Add(-time.Hour)
	if code, body := r.do(t, "POST", "/cq/1/execute", `{"start_time":"`+start.Format(time.RFC3339)+`","end_time":"`+end.Format(time.RFC3339)+`"}`); code != fiber.StatusOK {
		t.Fatalf("manual execute: %d %v", code, body)
	}
	if w := r.watermark(t, 1); w == nil || !w.Equal(end) {
		t.Fatalf("watermark %v, want %v", w, end)
	}
}

// The mirror writes the cluster's CQs and watermarks into this node's table,
// keeps rows it never wrote dormant with their history, and hands the
// scheduler the cluster's CQs.
func TestCQMirror(t *testing.T) {
	r := newCQRig(t)
	if _, err := r.h.sqliteDB.Exec(`INSERT INTO continuous_queries (id, name, database, source_measurement, destination_measurement, query, interval)
		VALUES (7, 'mine', 'db', 'cpu', 'o', 'q', '1h')`); err != nil {
		t.Fatal(err)
	}
	if _, err := r.h.sqliteDB.Exec(`INSERT INTO continuous_query_executions (query_id, execution_id, status, start_time, end_time) VALUES (7, 'x', 'completed', '2026-01-01T00:00:00Z', '2026-01-01T01:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	r.src.seedCQ()
	r.do(t, "POST", "/cq", cqBody)
	lpt := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	r.src.must(raft.CommandSetCQWatermark, raft.SetCQWatermarkPayload{Proposer: "n1", ID: 1, To: lpt})
	syncer := &fakeJobSyncer{}
	r.h.SetScheduler(syncer)
	if err := r.h.MirrorClusterPolicies(context.Background()); err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := r.h.sqliteDB.QueryRow(`SELECT last_processed_time FROM continuous_queries WHERE id = 1`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if got, _ := time.Parse(time.RFC3339, stored); !got.Equal(lpt) {
		t.Fatalf("mirrored watermark %q, want %v", stored, lpt)
	}
	var dormant, dormantExec int
	r.h.sqliteDB.QueryRow(`SELECT COUNT(*) FROM continuous_queries_dormant WHERE query_id = 7`).Scan(&dormant)
	r.h.sqliteDB.QueryRow(`SELECT COUNT(*) FROM continuous_query_executions_dormant WHERE query_id = 7`).Scan(&dormantExec)
	if dormant != 1 || dormantExec != 1 {
		t.Fatalf("dormant %d %d", dormant, dormantExec)
	}
	// Handed the cluster's CQs when it was set (already adopted) and again
	// by the mirror.
	if len(syncer.defs) != 2 || len(syncer.defs[1]) != 1 || syncer.defs[1][0].ID != 1 || syncer.defs[1][0].Interval != "1h" {
		t.Fatalf("scheduler handed %+v", syncer.defs)
	}
}

// A node whose tables already mirrored the cluster, restarted before it
// caught up, neither runs nor changes its own rows: 503, and the scheduler
// starts with no jobs.
func TestCQWaitsForTheClusterAfterARestart(t *testing.T) {
	r := newCQRig(t)
	r.src.seedCQ()
	r.do(t, "POST", "/cq", cqBody)
	if err := r.h.MirrorClusterPolicies(context.Background()); err != nil {
		t.Fatal(err)
	}
	h2, err := NewContinuousQueryHandler(nil, nil, nil, r.h.config, r.h.authManager, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	h2.SetClusterSource(newFSMSource(t, "n1"))
	r.h = h2
	r.app = fiber.New()
	r.app.Get("/cq", h2.handleList)
	r.app.Post("/cq", h2.handleCreate)
	r.app.Post("/cq/:id/execute", h2.handleExecute)
	for _, c := range []struct{ method, path, body string }{{"GET", "/cq", ""}, {"POST", "/cq", cqBody}, {"POST", "/cq/1/execute", "{}"}} {
		if code, body := r.do(t, c.method, c.path, c.body); code != fiber.StatusServiceUnavailable {
			t.Fatalf("%s %s: %d %v", c.method, c.path, code, body)
		}
	}
	if _, err := h2.GetActiveCQs(); !errors.Is(err, ErrCQNotCaughtUp) {
		t.Fatalf("scheduler load: %v", err)
	}
}

// The lister answers this node's CQs with completed runs (any time), its
// watermark and the highest ID it ever issued.
func TestCQLocalPolicyLister(t *testing.T) {
	r := newCQRig(t)
	for _, q := range []string{
		`INSERT INTO continuous_queries (id, name, database, source_measurement, destination_measurement, query, interval, tag_columns, last_processed_time)
			VALUES (3, 'a', 'db', 'cpu', 'o', 'q', '1h', '["host"]', '2026-10-01T12:00:00Z')`,
		`INSERT INTO continuous_query_executions (query_id, execution_id, status, start_time, end_time) VALUES (3, 'x', 'completed', 'a', 'b'), (3, 'y', 'failed', 'a', 'b')`,
		`INSERT INTO continuous_queries (id, name, database, source_measurement, destination_measurement, query, interval) VALUES (9, 'gone', 'db', 'cpu', 'o', 'q', '1h')`,
		`DELETE FROM continuous_queries WHERE id = 9`,
	} {
		if _, err := r.h.sqliteDB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := r.h.LocalPolicyLister()(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var rows cluster.CQLocalRows
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows.Policies) != 1 || rows.Policies[0].Executions != 1 || rows.Policies[0].LastProcessed == nil ||
		len(rows.Policies[0].TagColumns) != 1 || rows.MaxIssuedID != 9 {
		t.Fatalf("rows %+v", rows)
	}
}

// Once adopted, the list filters (and the scheduler's active-only load) read
// the cluster's CQs.
func TestCQClusterListFilters(t *testing.T) {
	r := newCQRig(t)
	r.src.seedCQ()
	r.do(t, "POST", "/cq", cqBody)
	r.do(t, "POST", "/cq", strings.Replace(strings.Replace(cqBody, `"name":"q"`, `"name":"other"`, 1), `"database":"db"`, `"database":"db2"`, 1))
	r.do(t, "PUT", "/cq/2", `{"is_active":false}`)
	count := func(path string) int {
		resp, err := r.app.Test(httptest.NewRequest("GET", path, nil), -1)
		if err != nil {
			t.Fatal(err)
		}
		var out []map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return len(out)
	}
	if n := count("/cq"); n != 2 {
		t.Fatalf("all: %d", n)
	}
	if n := count("/cq?database=db2"); n != 1 {
		t.Fatalf("database filter: %d", n)
	}
	if n := count("/cq?is_active=true"); n != 1 {
		t.Fatalf("active filter: %d", n)
	}
	active, err := r.h.GetActiveCQs()
	if err != nil || len(active) != 1 || active[0].Name != "q" {
		t.Fatalf("scheduler load %+v %v", active, err)
	}
}

// One run of a CQ at a time: a second run waits for the first, and starts
// from the watermark the first set.
func TestCQRunsOfOneCQAreSerialized(t *testing.T) {
	r := newCQRig(t)
	r.src.seedCQ()
	r.do(t, "POST", "/cq", cqBody)
	entered := make(chan time.Time, 2)
	release := make(chan struct{})
	r.h.aggregateHook = func(_ context.Context, _ *ContinuousQuery, _ string, start time.Time) (int64, error) {
		entered <- start
		<-release
		return 1, nil
	}
	done := make(chan error, 2)
	go func() { _, err := r.h.ExecuteCQ(context.Background(), 1); done <- err }()
	first := <-entered
	time.Sleep(1100 * time.Millisecond)
	go func() { _, err := r.h.ExecuteCQ(context.Background(), 1); done <- err }()
	select {
	case <-entered:
		t.Fatal("a second run of the CQ started while the first ran")
	case <-time.After(300 * time.Millisecond):
	}
	release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	second := <-entered
	release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !second.After(first) {
		t.Fatalf("second run started at %v, want where the first ended (it started at %v)", second, first)
	}
}

// While the cluster collects the nodes' CQs, a local write answers 503; the
// lister reads under the lock local writes take.
func TestCQLocalWritesDuringTheCollect(t *testing.T) {
	r := newCQRig(t)
	r.src.collecting = true
	if code, body := r.do(t, "POST", "/cq", cqBody); code != fiber.StatusServiceUnavailable || !strings.Contains(body["error"].(string), "being adopted") {
		t.Fatalf("create while collecting: %d %v", code, body)
	}
	r.src.collecting = false
	r.h.cl.localMu.Lock()
	done := make(chan struct{})
	go func() {
		_, _ = r.h.LocalPolicyLister()(context.Background())
		close(done)
	}()
	select {
	case <-done:
		r.h.cl.localMu.Unlock()
		t.Fatal("the lister read the table while a local write held the lock")
	case <-time.After(200 * time.Millisecond):
	}
	r.h.cl.localMu.Unlock()
	<-done
}

// A run that cannot catch up with the leader is skipped: its watermark read
// could be older than one the cluster already holds.
func TestCQRunSkippedWithoutAReadBarrier(t *testing.T) {
	r := newCQRig(t)
	r.src.seedCQ()
	r.do(t, "POST", "/cq", cqBody)
	r.src.barrierErr = errors.New("no leader")
	if _, err := r.h.ExecuteCQ(context.Background(), 1); !errors.Is(err, errCQReadBarrier) {
		t.Fatalf("run without a barrier: %v", err)
	}
	if code, _ := r.do(t, "POST", "/cq/1/execute", `{}`); code != fiber.StatusServiceUnavailable {
		t.Fatalf("manual run without a barrier: %d", code)
	}
	if len(r.runs) != 0 {
		t.Fatal("a run aggregated without catching up")
	}
}

// While the cluster collects the nodes' own CQs, a node-local run is skipped:
// its watermark would land after the cluster read it, and be lost.
func TestCQLocalRunSkippedDuringTheCollect(t *testing.T) {
	r := newCQRig(t)
	if _, err := r.h.sqliteDB.Exec(`INSERT INTO continuous_queries (id, name, database, source_measurement, destination_measurement, query, interval)
		VALUES (1, 'q', 'db', 'cpu', 'o', 'SELECT 1 WHERE {start_time} < {end_time}', '1h')`); err != nil {
		t.Fatal(err)
	}
	r.src.collecting = true
	if _, err := r.h.ExecuteCQ(context.Background(), 1); !errors.Is(err, errCQSkippedDuringCollect) {
		t.Fatalf("local run during the collect: %v", err)
	}
	r.src.collecting = false
	if _, err := r.h.ExecuteCQ(context.Background(), 1); err != nil || len(r.runs) != 1 {
		t.Fatalf("local run after the collect: %v (%d runs)", err, len(r.runs))
	}
}

// A manual run says what it did to the watermark.
func TestCQManualExecuteReportsTheWatermark(t *testing.T) {
	r := newCQRig(t)
	r.src.seedCQ()
	r.do(t, "POST", "/cq", cqBody)
	if code, body := r.do(t, "POST", "/cq/1/execute", `{}`); code != fiber.StatusOK || body["watermark_status"] != "set" {
		t.Fatalf("manual run: %d %v", code, body)
	}
	r.h.aggregateHook = func(context.Context, *ContinuousQuery, string, time.Time) (int64, error) {
		r.src.must(raft.CommandSetCQWatermark, raft.SetCQWatermarkPayload{Proposer: "n1", ID: 1, From: r.watermark(t, 1), To: time.Now().Add(-5 * time.Hour)})
		return 1, nil
	}
	time.Sleep(1100 * time.Millisecond)
	if code, body := r.do(t, "POST", "/cq/1/execute", `{}`); code != fiber.StatusOK || body["watermark_status"] != "moved" {
		t.Fatalf("manual run whose watermark moved meanwhile: %d %v", code, body)
	}
}

// Losing the primary role forgets unsettled watermarks: a later promotion
// never proposes one over what the cluster did meanwhile.
func TestCQForgetPendingWatermarks(t *testing.T) {
	r := newCQRig(t)
	r.src.seedCQ()
	r.do(t, "POST", "/cq", cqBody)
	r.src.dropWatermarks = 1
	if _, err := r.h.ExecuteCQ(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	r.h.ForgetPendingWatermarks()
	before := r.src.watermarkProposals
	time.Sleep(1100 * time.Millisecond)
	if _, err := r.h.ExecuteCQ(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if r.src.watermarkProposals != before+1 {
		t.Fatalf("%d proposals, want only the run's own (the forgotten one re-proposed)", r.src.watermarkProposals-before)
	}
}

// A run whose context is done before it gets the CQ's run slot gives up.
func TestCQRunGivesUpWhenCancelled(t *testing.T) {
	r := newCQRig(t)
	release, err := r.h.acquireRun(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := r.h.acquireRun(ctx, 1); err == nil {
		t.Fatal("got the run slot while another run held it")
	}
	release()
	cancelled, cancel2 := context.WithCancel(context.Background())
	cancel2()
	// With the slot free and ctx done, select may pick either; the run must
	// give up every time.
	for i := 0; i < 20; i++ {
		if _, err := r.h.acquireRun(cancelled, 1); err == nil {
			t.Fatal("a cancelled run took the slot")
		}
	}
}
