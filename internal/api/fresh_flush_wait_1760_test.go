//go:build duckdb_arrow

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	recoverMiddleware "github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #1760 (c): x-arc-freshness: flush on /api/v1/query and /query/msgpack.

// stubWaiter stands in for the buffer's WaitKeysFlushed.
type stubWaiter struct {
	fn    func(ctx context.Context) error
	calls atomic.Int32
	mu    sync.Mutex
	keys  []string
}

func (s *stubWaiter) WaitKeysFlushed(ctx context.Context, keys []string, _ time.Duration) error {
	s.calls.Add(1)
	s.mu.Lock()
	s.keys = append([]string(nil), keys...)
	s.mu.Unlock()
	if s.fn == nil {
		return nil
	}
	return s.fn(ctx)
}

// freshOnRig: newFreshRig's shape (real auth, a stored sample.cpu, rows of
// sample.fresh left buffered) with the feature on. waiter nil: the real
// buffer, its ledger and tracking on, with a budget of two forced flushes.
type freshOnRig struct {
	*freshRig
	buf     *ingest.ArrowBuffer
	am      *auth.AuthManager
	userCtx context.Context // set as the request's user context when non-nil
}

func newFreshOnRig(t *testing.T, waiter FreshFlushWaiter, edit func(*FreshFlushConfig)) *freshOnRig {
	t.Helper()
	ctx := context.Background()
	logger := zerolog.Nop()
	metrics.Init(logger)
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 8, FlushQueueSize: 16, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, logger)
	buffer.EnableKeyFlushLedger(ingest.ForceBudget{})
	buffer.EnableKeyFlushTracking()
	t.Cleanup(func() { buffer.Close() })
	r := &freshOnRig{buf: buffer}
	r.write(t, "sample", "cpu", time.Now(), "stored")
	require.NoError(t, buffer.FlushAllAndWait(ctx))
	r.write(t, "sample", "fresh", time.Now(), "buffered")

	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { duck.Close() })
	am, err := auth.NewAuthManager(filepath.Join(root, "auth.db"), time.Minute, 100, logger)
	require.NoError(t, err)
	t.Cleanup(func() { am.Close() })
	read, err := am.CreateToken(ctx, "fresh-read", "", "read", nil)
	require.NoError(t, err)

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(requestLogger(logger))
	app.Use(recoverMiddleware.New()) // as the server: a panic is answered 500
	mw := auth.DefaultMiddlewareConfig()
	mw.AuthManager = am
	app.Use(auth.NewMiddleware(mw))
	app.Use(func(c *fiber.Ctx) error {
		if r.userCtx != nil {
			c.SetUserContext(r.userCtx)
		}
		return c.Next()
	})
	qh := NewQueryHandler(duck, backend, logger, 0, 0)
	qh.SetAuthAndRBAC(am, nil)
	qh.SetFileTimePruning(true, 300*time.Second) // the default
	cfg := FreshFlushConfig{Enabled: true, StorageBackend: "local", MinInterval: time.Second, MaxWaiters: 4, RetryAfter: 7 * time.Second}
	if edit != nil {
		edit(&cfg)
	}
	if waiter == nil {
		waiter = buffer
	}
	qh.SetFreshFlush(cfg, waiter)
	buffer.AddOnFilePublished(qh.FilePublished) // as main wires it (#1757)
	qh.RegisterRoutes(app)
	r.freshRig = &freshRig{app: app, qh: qh, dataDir: dataDir, read: read}
	r.am = am
	return r
}

func (r *freshOnRig) write(t *testing.T, db, meas string, at time.Time, host string) {
	t.Helper()
	require.NoError(t, r.buf.WriteTypedColumnarDirect(context.Background(), db, meas, &ingest.TypedColumnBatch{
		Data: map[string]interface{}{"time": []int64{at.UTC().UnixMicro()}, "host": []string{host}, "value": []int64{1}},
	}, 1))
}

// query sends sql to path with the header (fresh) and x-arc-database (db).
func (r *freshOnRig) query(t *testing.T, path, sql, db string, fresh bool) (freshAnswer, http.Header) {
	t.Helper()
	req := httptest.NewRequest("POST", path, strings.NewReader(`{"sql":`+strconv.Quote(sql)+`}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.read)
	if fresh {
		req.Header.Set(FreshnessHeader, "flush")
	}
	if db != "" {
		req.Header.Set("x-arc-database", db)
	}
	resp, err := r.app.Test(req, -1)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	_, present := resp.Header[http.CanonicalHeaderKey(FreshnessAnswerHeader)]
	return freshAnswer{status: resp.StatusCode, fresh: resp.Header.Get(FreshnessAnswerHeader),
		reason: resp.Header.Get(FreshnessReasonHeader), body: string(b), contentPresent: present}, resp.Header
}

// count runs a count(*) query and returns its value.
func (r *freshOnRig) count(t *testing.T, path, sql, db string, fresh bool) (int64, freshAnswer) {
	t.Helper()
	a, _ := r.query(t, path, sql, db, fresh)
	require.Equal(t, http.StatusOK, a.status, a.body)
	var resp struct{ Data [][]int64 }
	require.NoError(t, json.Unmarshal([]byte(a.body), &resp), a.body)
	require.Len(t, resp.Data, 1, a.body)
	return resp.Data[0][0], a
}

// A row acknowledged before a fresh query is in its answer: a measurement
// never flushed before, after a plain query warmed the caches, in hours
// other than the current one, by header database and by db.table, in
// MessagePack, with file-time pruning on (the default) and off.
func TestFreshFlush_ReturnsRowsAcknowledgedBefore(t *testing.T) {
	for _, pruning := range []bool{true, false} {
		t.Run(fmt.Sprintf("file_time_pruning=%v", pruning), func(t *testing.T) {
			r := newFreshOnRig(t, nil, nil)
			r.qh.SetFileTimePruning(pruning, 300*time.Second)
			const q = "/api/v1/query"

			// Never flushed: no directory exists. Control first: a plain
			// query does not see the buffered row.
			r.write(t, "sample", "never", time.Now(), "a")
			a, _ := r.query(t, q, "SELECT count(*) FROM never", "sample", false)
			require.NotContains(t, a.body, `"data":[[1]]`, "control: the buffered row is visible without a flush")
			n, a := r.count(t, q, "SELECT count(*) FROM never", "sample", true)
			require.Equal(t, int64(1), n)
			require.Equal(t, "flushed", a.fresh)

			// Warm caches, then a new row: db.table syntax.
			n, _ = r.count(t, q, "SELECT count(*) FROM sample.never", "", false)
			require.Equal(t, int64(1), n)
			r.write(t, "sample", "never", time.Now(), "b")
			n, _ = r.count(t, q, "SELECT count(*) FROM sample.never", "", true)
			require.Equal(t, int64(2), n, "a row written after a warm cache")

			// Other hours, with the range warmed first.
			now := time.Now()
			rng := "SELECT count(*) FROM old WHERE time >= '" + now.Add(-72*time.Hour).UTC().Format("2006-01-02 15:04:05") + "'"
			r.write(t, "sample", "old", now.Add(-3*time.Hour), "a")
			n, _ = r.count(t, q, rng, "sample", true)
			require.Equal(t, int64(1), n)
			n, _ = r.count(t, q, rng, "sample", false)
			require.Equal(t, int64(1), n)
			r.write(t, "sample", "old", now.Add(-48*time.Hour), "b")
			r.write(t, "sample", "old", now.Add(-5*time.Hour), "c")
			n, _ = r.count(t, q, rng, "sample", true)
			require.Equal(t, int64(3), n, "rows outside the current hour after a warm cache")

			// The live hour with a range inside it: file-time pruning lists
			// that hour's files when the SQL is built, so the SQL must be
			// built after the flush.
			r.write(t, "sample", "live", time.Now(), "a")
			require.NoError(t, r.buf.FlushAllAndWait(context.Background()))
			recent := "SELECT count(*) FROM live WHERE time >= '" + time.Now().Add(-10*time.Minute).UTC().Format("2006-01-02 15:04:05") + "'"
			n, _ = r.count(t, q, recent, "sample", false)
			require.Equal(t, int64(1), n)
			r.write(t, "sample", "live", time.Now(), "b")
			n, _ = r.count(t, q, recent, "sample", true)
			require.Equal(t, int64(2), n, "a row in the live hour, after a file of that hour was listed")

			// One past hour, its absence listed and cached by a plain query
			// before its first file: the SQL must be built after the flush.
			hour := now.Add(-5 * time.Hour).Truncate(time.Hour)
			narrow := "SELECT count(*) FROM gap WHERE time >= '" + hour.UTC().Format("2006-01-02 15:04:05") + "' AND time < '" + hour.Add(time.Hour).UTC().Format("2006-01-02 15:04:05") + "'"
			r.write(t, "sample", "gap", now, "a")
			require.NoError(t, r.buf.FlushAllAndWait(context.Background()))
			n, _ = r.count(t, q, narrow, "sample", false)
			require.Equal(t, int64(0), n)
			r.write(t, "sample", "gap", hour.Add(30*time.Minute), "b")
			n, _ = r.count(t, q, narrow, "sample", true)
			require.Equal(t, int64(1), n, "a row in a past hour whose absence was listed before")

			// MessagePack.
			r.write(t, "sample", "mp", time.Now(), "probe-1760-mp")
			a, _ = r.query(t, "/api/v1/query/msgpack", "SELECT host FROM mp", "sample", true)
			require.Equal(t, http.StatusOK, a.status, a.body)
			require.Equal(t, "flushed", a.fresh)
			require.Contains(t, a.body, "probe-1760-mp")
		})
	}
}

// freshReasons is every reason executeQuery and the refusals can answer.
var freshReasons = []string{
	freshnessReasonInvalidValue, freshnessReasonDisabled, freshnessReasonUnsupportedRoute,
	freshnessReasonCluster, freshnessReasonStorage, freshnessReasonTiering, freshnessReasonShow,
	freshnessReasonForbidden, freshnessReasonTooMany, freshnessReasonBusy, freshnessReasonQueryError,
	freshnessReasonWALReplay, freshnessReasonRecoveryPending, freshnessReasonFlushFailed,
	freshnessReasonNotConfirmed, freshnessReasonTimeout, freshnessReasonShutdown,
	freshnessReasonCancelled, freshnessReasonUnavailable, freshnessFlushed,
}

// Every reason is a label of arc_query_fresh_flush_total, so the two
// vocabularies are one.
func TestFreshFlush_ReasonsAreMetricLabels(t *testing.T) {
	for _, r := range freshReasons {
		require.True(t, metrics.QueryFreshFlushOutcome(r), r)
	}
}

func freshOutcomes() map[string]int64 {
	out := map[string]int64{}
	for _, r := range freshReasons {
		out[r] = metrics.Get().QueryFreshFlushCount(r)
	}
	return out
}

// The exit table (H2): every exit of a request carrying the header, with its
// status, reason, counters, and one count in arc_query_fresh_flush_total
// under its reason.
func TestFreshFlush_ExitTable(t *testing.T) {
	type counts struct{ requests, errors, timeouts int64 }
	join := "SELECT count(*) FROM (SELECT time FROM sample.cpu UNION ALL SELECT time FROM sample.fresh) s"
	cases := []struct {
		name       string
		path, sql  string
		db         string
		cfg        func(*FreshFlushConfig)
		setup      func(t *testing.T, r *freshOnRig, w *stubWaiter)
		status     int
		reason     string // "" with fresh "flushed"
		retryAfter string
		delta      counts
		waited     bool
	}{
		{name: "success", path: "/api/v1/query", sql: "SELECT * FROM sample.cpu", status: 200, delta: counts{1, 0, 0}, waited: true},
		{name: "msgpack success", path: "/api/v1/query/msgpack", sql: "SELECT * FROM cpu", db: "sample", status: 200, delta: counts{1, 0, 0}, waited: true},
		{name: "no measurement read: no wait", path: "/api/v1/query", sql: "SELECT 1", status: 200, delta: counts{1, 0, 0}},
		{name: "cluster", path: "/api/v1/query", sql: "SELECT 1", cfg: func(c *FreshFlushConfig) { c.Clustered = true }, status: 400, reason: "cluster"},
		{name: "object storage", path: "/api/v1/query", sql: "SELECT 1", cfg: func(c *FreshFlushConfig) { c.StorageBackend = "s3" }, status: 400, reason: "storage_not_supported"},
		{name: "tiering active", path: "/api/v1/query", sql: "SELECT 1", setup: func(t *testing.T, r *freshOnRig, _ *stubWaiter) {
			r.qh.tieringManager = &tiering.Manager{}
			t.Cleanup(func() { r.qh.tieringManager = nil })
		}, status: 400, reason: "tiering_not_supported"},
		{name: "arrow", path: "/api/v1/query/arrow", sql: "SELECT * FROM sample.cpu", status: 400, reason: "unsupported_route"},
		{name: "estimate", path: "/api/v1/query/estimate", sql: "SELECT * FROM sample.cpu", status: 400, reason: "unsupported_route"},
		{name: "governance 429", path: "/api/v1/query", sql: "SELECT * FROM sample.cpu", setup: func(t *testing.T, r *freshOnRig, _ *stubWaiter) {
			old := queryGovernanceLicensed
			queryGovernanceLicensed = func(*QueryHandler) bool { return true }
			t.Cleanup(func() { queryGovernanceLicensed = old })
			info := r.am.VerifyToken(r.read)
			require.NotNil(t, info)
			r.qh.governanceManager = newGovernanceTestManager(t, &governance.Policy{TokenID: info.ID, RateLimitPerMinute: 1})
			a, _ := r.query(t, "/api/v1/query", "SELECT 1", "", false) // the one admitted query
			require.Equal(t, http.StatusOK, a.status, a.body)
		}, status: 429, reason: "query_error", retryAfter: "*", delta: counts{1, 1, 0}},
		{name: "bad body", path: "/api/v1/query", sql: "\x00", status: 400, reason: "query_error", delta: counts{1, 1, 0}},
		{name: "SHOW", path: "/api/v1/query", sql: "SHOW TABLES FROM sample", status: 400, reason: "show_not_supported", delta: counts{1, 1, 0}},
		{name: "RBAC", path: "/api/v1/query", sql: "SELECT * FROM sample.fresh", setup: func(t *testing.T, r *freshOnRig, _ *stubWaiter) {
			r.qh.rbacManager = &mockRBACChecker{enabled: true, allowedDBs: map[string]bool{"sample": true}, deniedMeasurements: map[string]bool{"sample.fresh": true}, deniedReason: "no"}
		}, status: 403, reason: "forbidden", delta: counts{1, 1, 0}},
		{name: "too many measurements", path: "/api/v1/query", sql: join, setup: func(t *testing.T, _ *freshOnRig, _ *stubWaiter) {
			old := freshFlushMaxKeys
			freshFlushMaxKeys = 1
			t.Cleanup(func() { freshFlushMaxKeys = old })
		}, status: 400, reason: "too_many_measurements", delta: counts{1, 1, 0}},
		{name: "two measurements within the limit", path: "/api/v1/query", sql: join, setup: func(t *testing.T, _ *freshOnRig, _ *stubWaiter) {
			old := freshFlushMaxKeys
			freshFlushMaxKeys = 2
			t.Cleanup(func() { freshFlushMaxKeys = old })
		}, status: 200, delta: counts{1, 0, 0}, waited: true},
		{name: "latch: flush failure", path: "/api/v1/query", sql: "SELECT * FROM sample.cpu", setup: stubErr(&ingest.RecoveryPendingError{Reason: ingest.RecoveryReasonFlushFailure}),
			status: 503, reason: "flush_failed_wal_replay", retryAfter: "7", delta: counts{1, 1, 0}, waited: true},
		{name: "latch: recovery pending", path: "/api/v1/query", sql: "SELECT * FROM sample.cpu", setup: stubErr(&ingest.RecoveryPendingError{Reason: ingest.RecoveryReasonRecoveryPending}),
			status: 503, reason: "recovery_pending", retryAfter: "7", delta: counts{1, 1, 0}, waited: true},
		{name: "a waited flush failed", path: "/api/v1/query", sql: "SELECT * FROM sample.cpu", setup: stubErr(fmt.Errorf("k: %w", ingest.ErrKeyFlushFailed)),
			status: 503, reason: "flush_failed", delta: counts{1, 1, 0}, waited: true},
		{name: "not confirmed", path: "/api/v1/query", sql: "SELECT * FROM sample.cpu", setup: stubErr(fmt.Errorf("k: %w", ingest.ErrKeyFlushUnconfirmed)),
			status: 503, reason: "not_confirmed", delta: counts{1, 1, 0}, waited: true},
		{name: "buffer closing", path: "/api/v1/query", sql: "SELECT * FROM sample.cpu", setup: stubErr(ingest.ErrBufferClosing),
			status: 503, reason: "shutdown", delta: counts{1, 1, 0}, waited: true},
		{name: "ledger off", path: "/api/v1/query", sql: "SELECT * FROM sample.cpu", setup: stubErr(ingest.ErrLedgerOff),
			status: 503, reason: "fresh_flush_unavailable", delta: counts{1, 1, 0}, waited: true},
		{name: "the 30 s budget first", path: "/api/v1/query", sql: "SELECT * FROM sample.cpu", setup: func(t *testing.T, r *freshOnRig, w *stubWaiter) {
			old := freshFlushBudget
			freshFlushBudget = 50 * time.Millisecond
			t.Cleanup(func() { freshFlushBudget = old })
			w.fn = untilDone
		}, status: 504, reason: "timeout", delta: counts{1, 1, 0}, waited: true},
		{name: "the query deadline first", path: "/api/v1/query", sql: "SELECT * FROM sample.cpu", setup: func(t *testing.T, r *freshOnRig, w *stubWaiter) {
			r.qh.queryTimeout = 50 * time.Millisecond
			w.fn = untilDone
		}, status: 504, reason: "timeout", delta: counts{1, 1, 1}, waited: true},
		{name: "cancelled", path: "/api/v1/query", sql: "SELECT * FROM sample.cpu", setup: func(t *testing.T, r *freshOnRig, w *stubWaiter) {
			ctx, cancel := context.WithCancel(context.Background())
			r.userCtx = ctx
			w.fn = func(wctx context.Context) error { cancel(); return untilDone(wctx) }
		}, status: 503, reason: "cancelled", delta: counts{1, 1, 0}, waited: true},
		{name: "shutdown: the HTTP drain", path: "/api/v1/query", sql: "SELECT * FROM sample.cpu", setup: func(t *testing.T, r *freshOnRig, w *stubWaiter) {
			drain := make(chan struct{})
			old := freshDrainDone
			freshDrainDone = func(*fiber.Ctx) <-chan struct{} { return drain }
			t.Cleanup(func() { freshDrainDone = old })
			w.fn = func(wctx context.Context) error { close(drain); return untilDone(wctx) }
		}, status: 503, reason: "shutdown", delta: counts{1, 1, 0}, waited: true},
		{name: "shutdown: the query lifetime", path: "/api/v1/query", sql: "SELECT * FROM sample.cpu", setup: func(t *testing.T, r *freshOnRig, w *stubWaiter) {
			ctx, cancel := context.WithCancel(context.Background())
			r.qh.SetLifetime(ctx)
			w.fn = func(wctx context.Context) error { cancel(); return untilDone(wctx) }
		}, status: 503, reason: "shutdown", delta: counts{1, 1, 0}, waited: true},
		{name: "flushed, then the query fails", path: "/api/v1/query", sql: "SELECT no_such_column FROM sample.cpu", status: 500, reason: "query_error", delta: counts{1, 1, 0}, waited: true},
		{name: "flushed, then the msgpack query fails", path: "/api/v1/query/msgpack", sql: "SELECT no_such_column FROM sample.cpu", status: 500, reason: "query_error", delta: counts{1, 1, 0}, waited: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := &stubWaiter{}
			r := newFreshOnRig(t, w, tc.cfg)
			if tc.setup != nil {
				tc.setup(t, r, w)
			}
			before := counts{queryMetricInt(t, "query_requests_total"), queryMetricInt(t, "query_errors_total"), queryMetricInt(t, "query_timeouts_total")}
			outcomes := freshOutcomes()
			var a freshAnswer
			var hd http.Header
			if tc.sql == "\x00" {
				a = r.sendValues(t, "POST", tc.path, `{"sql":`, r.read, []string{"flush"})
			} else {
				a, hd = r.query(t, tc.path, tc.sql, tc.db, true)
			}
			require.Equal(t, tc.status, a.status, a.body)
			if tc.reason == "" {
				require.Equal(t, "flushed", a.fresh, a.body)
				require.Empty(t, a.reason)
				require.NotEmpty(t, hd.Get(FreshnessWaitHeader))
			} else {
				require.Equal(t, "false", a.fresh, a.body)
				require.Equal(t, tc.reason, a.reason, a.body)
			}
			if hd != nil && tc.retryAfter == "*" { // governance's own
				require.NotEmpty(t, hd.Get(fiber.HeaderRetryAfter))
			} else if hd != nil {
				require.Equal(t, tc.retryAfter, hd.Get(fiber.HeaderRetryAfter))
			}
			after := counts{queryMetricInt(t, "query_requests_total"), queryMetricInt(t, "query_errors_total"), queryMetricInt(t, "query_timeouts_total")}
			require.Equal(t, tc.delta, counts{after.requests - before.requests, after.errors - before.errors, after.timeouts - before.timeouts}, "requests, errors, timeouts")
			require.Equal(t, tc.waited, w.calls.Load() == 1, "waiter called %d times", w.calls.Load())
			label := tc.reason
			if label == "" {
				label = freshnessFlushed
			}
			now := freshOutcomes()
			for k, v := range now {
				want := int64(0)
				if k == label {
					want = 1
				}
				require.Equal(t, want, v-outcomes[k], "arc_query_fresh_flush_total{outcome=%q}", k)
			}
		})
	}
}

func stubErr(err error) func(*testing.T, *freshOnRig, *stubWaiter) {
	return func(_ *testing.T, _ *freshOnRig, w *stubWaiter) { w.fn = func(context.Context) error { return err } }
}

func untilDone(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

// The waiter is handed the buffer keys of the measurements the query reads:
// database/measurement, by header database, by db.table, and in "default".
func TestFreshFlush_KeysAreBufferKeys(t *testing.T) {
	w := &stubWaiter{}
	r := newFreshOnRig(t, w, nil)
	for _, tc := range []struct{ sql, db, want string }{
		{"SELECT * FROM cpu", "sample", "sample/cpu"},
		{"SELECT * FROM sample.cpu", "", "sample/cpu"},
		{"SELECT * FROM cpu", "", "default/cpu"},
	} {
		a, _ := r.query(t, "/api/v1/query", tc.sql, tc.db, true)
		require.Equal(t, "flushed", a.fresh, a.body)
		require.Equal(t, []string{tc.want}, w.keys, tc.sql)
	}
}

// Over query.fresh_flush_max_waiters: 503 busy with Retry-After, and the
// slot is released after the wait.
func TestFreshFlush_Busy(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	var held atomic.Bool
	w := &stubWaiter{fn: func(context.Context) error {
		if held.CompareAndSwap(false, true) { // only the first waits: a second admitted would answer at once
			entered <- struct{}{}
			<-release
		}
		return nil
	}}
	r := newFreshOnRig(t, w, func(c *FreshFlushConfig) { c.MaxWaiters = 1 })
	done := make(chan freshAnswer)
	go func() { a, _ := r.query(t, "/api/v1/query", "SELECT * FROM sample.cpu", "", true); done <- a }()
	<-entered
	errs := queryMetricInt(t, "query_errors_total")
	a, hd := r.query(t, "/api/v1/query", "SELECT * FROM sample.cpu", "", true)
	require.Equal(t, http.StatusServiceUnavailable, a.status, a.body)
	require.Equal(t, "busy", a.reason)
	require.Equal(t, "5", hd.Get(fiber.HeaderRetryAfter))
	require.Equal(t, errs+1, queryMetricInt(t, "query_errors_total"), "busy is counted as an error, as governance's 429")
	close(release)
	require.Equal(t, "flushed", (<-done).fresh)
	w.fn = nil
	a, _ = r.query(t, "/api/v1/query", "SELECT * FROM sample.cpu", "", true)
	require.Equal(t, "flushed", a.fresh, "the slot was not released")
}

// H1, M7: the execution clocks start after the wait, and a request that
// waited, whatever its outcome, is left out of the HTTP latency.
func TestFreshFlush_ClocksAndLatency(t *testing.T) {
	var fail atomic.Bool
	w := &stubWaiter{fn: func(context.Context) error {
		time.Sleep(300 * time.Millisecond)
		if fail.Load() {
			return ingest.ErrKeyFlushFailed
		}
		return nil
	}}
	r := newFreshOnRig(t, w, nil)
	for _, f := range []bool{false, true} {
		fail.Store(f)
		lat := queryMetricInt(t, "http_latency_count")
		a, hd := r.query(t, "/api/v1/query", "SELECT * FROM sample.cpu", "", true)
		require.Equal(t, lat, queryMetricInt(t, "http_latency_count"), "a request that waited (failed=%v) is in the HTTP latency", f)
		waitMS, err := strconv.Atoi(hd.Get(FreshnessWaitHeader))
		require.NoError(t, err)
		require.GreaterOrEqual(t, waitMS, 300)
		if !f {
			var resp QueryResponse
			require.NoError(t, json.Unmarshal([]byte(a.body), &resp), a.body)
			require.Less(t, resp.ExecutionTimeMs, float64(300), "execution_time_ms includes the wait")
		}
	}
	lat := queryMetricInt(t, "http_latency_count")
	a, _ := r.query(t, "/api/v1/query", "SELECT * FROM sample.cpu", "", false)
	require.Equal(t, http.StatusOK, a.status)
	require.Equal(t, lat+1, queryMetricInt(t, "http_latency_count"), "control: a plain query is in the HTTP latency")
}

// M2: a client that disconnects during the execution that follows a fresh
// wait is counted once: the wait's own disconnect watcher ended with the
// wait. Over a real connection, which the watcher peeks.
func TestFreshFlush_OneDisconnectCountedOnce(t *testing.T) {
	w := &stubWaiter{}
	r := newFreshOnRig(t, w, nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = r.app.Listener(ln) }()
	t.Cleanup(func() { _ = r.app.Shutdown() })

	before := queryMetricInt(t, "query_client_disconnects_sql_json_total")
	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	// Long enough to be cancelled by the disconnect, not to finish.
	body := `{"sql":"SELECT count(*) FROM sample.cpu, range(100000000000) t"}`
	_, err = fmt.Fprintf(conn, "POST /api/v1/query HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\n%s: flush\r\nContent-Length: %d\r\n\r\n%s",
		r.read, FreshnessHeader, len(body), body)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return w.calls.Load() == 1 }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(300 * time.Millisecond) // into the execution
	require.NoError(t, conn.Close())
	require.Eventually(t, func() bool {
		return queryMetricInt(t, "query_client_disconnects_sql_json_total") > before
	}, 5*time.Second, 20*time.Millisecond)
	time.Sleep(4 * clientDisconnectPollInterval) // a second watcher would have fired by now
	require.Equal(t, before+1, queryMetricInt(t, "query_client_disconnects_sql_json_total"), "one disconnect counted twice")
}

// A panic before or after the wait is answered 500 with false/query_error,
// never flushed, and returns the waiter slot and the read-gate entry.
func TestFreshFlush_PanicLeaksNothing(t *testing.T) {
	var boom atomic.Bool
	w := &stubWaiter{fn: func(context.Context) error {
		if boom.Load() {
			panic("wait")
		}
		return nil
	}}
	r := newFreshOnRig(t, w, func(c *FreshFlushConfig) { c.MaxWaiters = 1 })
	before := freshOutcomes()[freshnessReasonQueryError]
	boom.Store(true)
	a, _ := r.query(t, "/api/v1/query", "SELECT * FROM sample.cpu", "", true)
	require.Equal(t, http.StatusInternalServerError, a.status, a.body)
	require.Equal(t, "false", a.fresh)
	require.Equal(t, "query_error", a.reason)
	boom.Store(false)
	a, _ = r.query(t, "/api/v1/query", "SELECT * FROM sample.cpu", "", true)
	require.Equal(t, "flushed", a.fresh, "the panicked wait kept its slot: %s", a.body)

	// After the wait: the execution panics.
	old := arrowJSONQueryFunc
	arrowJSONQueryFunc = func(*QueryHandler, *fiber.Ctx, context.Context, context.CancelFunc, string, bool, int, time.Time, string, func(int), func(string), func()) (int, bool) {
		panic("execution")
	}
	a, _ = r.query(t, "/api/v1/query", "SELECT * FROM sample.cpu", "", true)
	arrowJSONQueryFunc = old
	require.Equal(t, http.StatusInternalServerError, a.status, a.body)
	require.Equal(t, "false", a.fresh, "a panic after the wait was answered flushed")
	require.Equal(t, "query_error", a.reason)
	require.Equal(t, before+2, freshOutcomes()[freshnessReasonQueryError])
	require.Zero(t, r.qh.reads.InFlight(), "a read-gate entry leaked")
}

// #764 staged as in TestEmittedScopeOutsideTheCheckedSetIsRefusedOnEveryEndpoint:
// the SQL names db1.cpu, which the token may read, and the cached transform
// reads db2.secret, which it may not. The dry transform's refs are checked
// before anything is flushed: 403 forbidden, and the waiter is never called.
func TestFreshFlush_EmittedRefsCheckedBeforeTheWait(t *testing.T) {
	metrics.Init(zerolog.Nop())
	h := newParityTestHandler()
	h.rbacManager = &mockRBACChecker{enabled: true, allowedDBs: map[string]bool{"db1": true}}
	h.queryCache.SetWithMetadata(h.queryTransformCacheKey("SELECT * FROM db1.cpu", ""),
		"SELECT * FROM read_parquet('./data/db2/secret/**/*.parquet')", []string{"db2\x00secret"})
	w := &stubWaiter{}
	h.SetFreshFlush(FreshFlushConfig{Enabled: true, StorageBackend: "local", MinInterval: time.Second, MaxWaiters: 4}, w)
	app := fiber.New()
	app.Use(tokenMiddleware(1, "reader"))
	app.Use(recoverMiddleware.New())
	h.RegisterRoutes(app)
	forbidden := metrics.Get().QueryFreshFlushCount(freshnessReasonForbidden)
	req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(`{"sql":"SELECT * FROM db1.cpu"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(FreshnessHeader, "flush")
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusForbidden, resp.StatusCode, string(body))
	require.Equal(t, "forbidden", resp.Header.Get(FreshnessReasonHeader), string(body))
	require.Contains(t, string(body), "db2.secret")
	require.Zero(t, w.calls.Load(), "a scope the token cannot read was flushed")
	require.Equal(t, forbidden+1, metrics.Get().QueryFreshFlushCount(freshnessReasonForbidden))
}
