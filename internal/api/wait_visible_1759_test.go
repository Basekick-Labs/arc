package api

// #1759: x-arc-wait: visible on the MessagePack and line-protocol writes.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type visRig struct {
	app  *fiber.App
	qapp *fiber.App // the query handler, built once: its caches stay warm
	buf  *ingest.ArrowBuffer
	dir  string
	wv   *WaitVisible
	be   storage.Backend
}

type visOpts struct {
	ageMS   int // 0: 600000 (no age flush during a test)
	backend func(storage.Backend) storage.Backend
	wv      *WaitVisibleConfig // nil: on, local, pruning with a 1 s margin
	noWV    bool               // SetWaitVisible never called
	decimal bool               // line protocol takes the record path
	router  *cluster.Router
}

var visLocal = WaitVisibleConfig{Enabled: true, StorageBackend: "local", FileTimePruning: true, FileTimePruningMargin: time.Second}

func newVisRig(t *testing.T, o visOpts) *visRig {
	t.Helper()
	dir := t.TempDir()
	local, err := storage.NewLocalBackend(dir, zerolog.Nop())
	require.NoError(t, err)
	var be storage.Backend = local
	if o.backend != nil {
		be = o.backend(local)
	}
	if o.ageMS == 0 {
		o.ageMS = 600000
	}
	cfg := &config.IngestConfig{MaxBufferSize: 1_000_000, MaxBufferAgeMS: o.ageMS, Compression: "snappy",
		FlushWorkers: 2, FlushQueueSize: 8, ShardCount: 2, DataPageVersion: "2.0"}
	if o.decimal {
		cfg.DecimalColumns = []string{"other:price=10,2"}
	}
	buf := ingest.NewArrowBuffer(cfg, be, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	if o.router != nil {
		buf.SetIngestGate(func() error { return fencedErr(t) }) // a standby refuses locally
	}
	// The production app: CORS, the request logger and its latency metric.
	app := NewServer(DefaultServerConfig(), zerolog.Nop()).GetApp()
	mp := NewMsgPackHandler(zerolog.Nop(), buf, 100<<20)
	lp := NewLineProtocolHandler(buf, zerolog.Nop())
	var wv *WaitVisible
	if !o.noWV {
		c := visLocal
		if o.wv != nil {
			c = *o.wv
		}
		wv = NewWaitVisible(c)
		mp.SetWaitVisible(wv)
		lp.SetWaitVisible(wv)
	}
	if o.router != nil {
		mp.SetRouter(o.router)
		lp.SetRouter(o.router)
	}
	mp.RegisterRoutes(app)
	lp.RegisterRoutes(app)
	NewTLEHandler(buf, zerolog.Nop()).RegisterRoutes(app)
	imp := NewImportHandler(zerolog.Nop())
	imp.SetArrowBuffer(buf)
	imp.RegisterRoutes(app)
	return &visRig{app: app, buf: buf, dir: dir, wv: wv, be: be}
}

// post sends body to path with x-arc-database: d and the header pairs hdr.
func (r *visRig) post(t *testing.T, path string, body []byte, hdr ...string) (*http.Response, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("x-arc-database", "d")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := r.app.Test(req, -1)
	require.NoError(t, err)
	out, _ := io.ReadAll(resp.Body)
	return resp, string(out)
}

// requireNothingWritten: nothing buffered, and no file after a flush.
func (r *visRig) requireNothingWritten(t *testing.T) {
	t.Helper()
	require.Zero(t, r.buf.BufferedBytes())
	require.NoError(t, r.buf.FlushAllAndWait(context.Background()))
	files, _ := filepath.Glob(filepath.Join(r.dir, "d", "*", "*", "*", "*", "*", "*.parquet"))
	require.Empty(t, files)
}

// count runs sql through one query handler per rig, with file-level time
// pruning on as in production, over the rig's storage.
func (r *visRig) count(t *testing.T, sql string) int64 {
	t.Helper()
	if r.qapp == nil {
		duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 2, LocalStorageRoot: r.dir}, zerolog.Nop())
		require.NoError(t, err)
		t.Cleanup(func() { duck.Close() })
		h := NewQueryHandler(duck, r.be, zerolog.Nop(), 0, 0)
		h.SetFileTimePruning(true, time.Second)
		r.buf.AddOnFilePublished(h.FilePublished) // as main.go wires it
		r.qapp = fiber.New()
		h.RegisterRoutes(r.qapp)
	}
	b, _ := json.Marshal(QueryRequest{SQL: sql})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/query", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.qapp.Test(req, -1)
	require.NoError(t, err)
	out, _ := io.ReadAll(resp.Body)
	var res struct {
		Success bool
		Data    [][]any
	}
	require.NoError(t, json.Unmarshal(out, &res), string(out))
	require.True(t, res.Success, string(out))
	n, _ := res.Data[0][0].(float64)
	return int64(n)
}

func mpCPU(t *testing.T, times ...time.Time) []byte {
	ts, vs := make([]int64, len(times)), make([]float64, len(times))
	for i, at := range times {
		ts[i], vs[i] = at.UnixMilli(), float64(i)
	}
	b, err := msgpack.Marshal(map[string]interface{}{"m": "cpu", "columns": map[string]interface{}{"time": ts, "v": vs}})
	require.NoError(t, err)
	return b
}

func lpCPU(times ...time.Time) []byte {
	var b strings.Builder
	for i, at := range times {
		fmt.Fprintf(&b, "cpu,host=h v=%di %d\n", i, at.UnixNano())
	}
	return []byte(b.String())
}

func sqlTime(at time.Time) string { return at.UTC().Format("2006-01-02 15:04:05") }

var waitVisibleCountRe = regexp.MustCompile(`arc_ingest_wait_visible_total\{outcome="([a-z_]+)"\} (\d+)`)

func waitVisibleCounts() map[string]int64 {
	out := map[string]int64{}
	for _, m := range waitVisibleCountRe.FindAllStringSubmatch(metrics.Get().PrometheusFormat(), -1) {
		out[m[1]], _ = strconv.ParseInt(m[2], 10, 64)
	}
	return out
}

func visible(h http.Header) (string, string) { return h.Get(VisibleHeader), h.Get(VisibleReasonHeader) }

// Without the header every answer is the one a handler without the feature
// gives: same status, same body, no X-Arc-Visible.
func TestWaitVisibleHeaderOffUnchanged1759(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name, path string
		body       []byte
		status     int
	}{
		{"msgpack", "/api/v1/write/msgpack", mpCPU(t, now), http.StatusNoContent},
		{"msgpack empty", "/api/v1/write/msgpack", []byte{0x90}, http.StatusNoContent},
		{"line protocol", "/api/v1/write/line-protocol", lpCPU(now), http.StatusNoContent},
		{"line protocol partial", "/api/v1/write/line-protocol", []byte("cpu v=1i\ncpu time=1i\n"), http.StatusBadRequest},
		{"influx v1", "/write?db=d", lpCPU(now), http.StatusNoContent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			on, off := newVisRig(t, visOpts{}), newVisRig(t, visOpts{noWV: true})
			r1, b1 := on.post(t, tc.path, tc.body)
			r2, b2 := off.post(t, tc.path, tc.body)
			require.Equal(t, tc.status, r1.StatusCode, b1)
			require.Equal(t, r2.StatusCode, r1.StatusCode)
			require.Equal(t, b2, b1)
			require.Empty(t, r1.Header.Values(VisibleHeader))
			require.Empty(t, r1.Header.Values(VisibleReasonHeader))
		})
	}
}

// Refused before anything is written, in the order value, config, cluster.
func TestWaitVisibleRefusals1759(t *testing.T) {
	now := time.Now()
	off := WaitVisibleConfig{StorageBackend: "local"}
	clustered := visLocal
	clustered.Clustered = true
	offClustered := off
	offClustered.Clustered = true
	cases := []struct {
		name   string
		o      visOpts
		path   string
		body   []byte
		value  string
		status int
		text   string
	}{
		{"msgpack disabled", visOpts{wv: &off}, "/api/v1/write/msgpack", mpCPU(t, now), "visible", 403, "ingest.wait_visible_enabled=true"},
		{"msgpack not set", visOpts{noWV: true}, "/api/v1/write/msgpack", mpCPU(t, now), "visible", 403, "ingest.wait_visible_enabled=true"},
		{"lp disabled", visOpts{wv: &off}, "/api/v1/write/line-protocol", lpCPU(now), "Visible", 403, "ingest.wait_visible_enabled=true"},
		{"msgpack bad value", visOpts{}, "/api/v1/write/msgpack", mpCPU(t, now), "yes", 400, `the only value is \"visible\"`},
		{"lp bad value, disabled", visOpts{wv: &off}, "/api/v1/write/line-protocol", lpCPU(now), "1", 400, `the only value is \"visible\"`},
		{"msgpack cluster", visOpts{wv: &clustered}, "/api/v1/write/msgpack", mpCPU(t, now), "visible", 400, "not supported on a cluster"},
		{"lp cluster", visOpts{wv: &clustered}, "/api/v1/write/line-protocol", lpCPU(now), "visible", 400, "not supported on a cluster"},
		{"cluster, disabled", visOpts{wv: &offClustered}, "/api/v1/write/msgpack", mpCPU(t, now), "visible", 403, "ingest.wait_visible_enabled=true"},
		{"influx v1", visOpts{}, "/write?db=d", lpCPU(now), "visible", 400, "supported only on"},
		{"influx v2", visOpts{}, "/api/v2/write?bucket=d", lpCPU(now), "visible", 400, "supported only on"},
		{"prometheus", visOpts{}, prometheusRemoteWritePath, []byte("x"), "visible", 400, "supported only on"},
		{"tle", visOpts{}, "/api/v1/write/tle", []byte("ISS\n1 25544U\n2 25544\n"), "visible", 400, "supported only on"},
		{"import lp", visOpts{}, "/api/v1/import/lp?db=d", lpCPU(now), "visible", 400, "supported only on"},
		{"import csv", visOpts{}, "/api/v1/import/csv?db=d", []byte("x"), "visible", 400, "supported only on"},
		{"import parquet", visOpts{}, "/api/v1/import/parquet?db=d", []byte("x"), "visible", 400, "supported only on"},
		{"import tle", visOpts{}, "/api/v1/import/tle?db=d", []byte("x"), "visible", 400, "supported only on"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newVisRig(t, tc.o)
			resp, out := r.post(t, tc.path, tc.body, WaitVisibleHeader, tc.value)
			require.Equal(t, tc.status, resp.StatusCode, out)
			require.Contains(t, out, tc.text)
			require.Contains(t, out, "nothing was written")
			r.requireNothingWritten(t)
		})
	}
}

// The cap: refused with 503 wait_visible_busy before admission, nothing
// written, and the slot accounting left as it was.
func TestWaitVisibleBusy1759(t *testing.T) {
	for _, path := range []string{"/api/v1/write/msgpack", "/api/v1/write/line-protocol"} {
		t.Run(path, func(t *testing.T) {
			r := newVisRig(t, visOpts{})
			body := lpCPU(time.Now())
			if strings.Contains(path, "msgpack") {
				body = mpCPU(t, time.Now())
			}
			r.wv.waiters.Store(waitVisibleMaxWaiters)
			before := waitVisibleCounts()["busy"]
			resp, out := r.post(t, path, body, WaitVisibleHeader, "visible")
			require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, out)
			require.Equal(t, ingest.RefusalWaitVisibleBusy, resp.Header.Get(IngestRefusedHeader))
			require.Equal(t, "5", resp.Header.Get(fiber.HeaderRetryAfter))
			require.Equal(t, waitVisibleMaxWaiters, r.wv.waiters.Load())
			require.Equal(t, before+1, waitVisibleCounts()["busy"])
			r.requireNothingWritten(t)
		})
	}
}

// On a cluster the header is refused before any forwarding decision: a
// standby writer that forwards every other write never forwards this one.
func TestWaitVisibleClusterRefusedBeforeForward1759(t *testing.T) {
	_, _ = captureRefusals(t)
	var hits atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer primary.Close()
	router := cluster.NewRouter(&cluster.RouterConfig{
		Registry:           cluster.NewRegistry(&cluster.RegistryConfig{Logger: zerolog.Nop()}),
		LocalNode:          cluster.NewNode("w-5", "w-5", cluster.RoleWriter, "c"),
		Logger:             zerolog.Nop(),
		StandbyWriteTarget: primaryAt(strings.TrimPrefix(primary.URL, "http://")),
		WriteTimeout:       5 * time.Second,
	})
	clustered := visLocal
	clustered.Clustered = true
	r := newVisRig(t, visOpts{wv: &clustered, router: router})
	for _, w := range []struct {
		path string
		body []byte
	}{{"/api/v1/write/msgpack", mpCPU(t, time.Now())}, {"/api/v1/write/line-protocol", lpCPU(time.Now())}} {
		resp, out := r.post(t, w.path, w.body, WaitVisibleHeader, "visible")
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, out)
		require.Contains(t, out, "not supported on a cluster")
		require.Zero(t, hits.Load(), "forwarded")
		resp, out = r.post(t, w.path, w.body) // the control: forwarded
		require.Equal(t, http.StatusNoContent, resp.StatusCode, out)
		require.Equal(t, int32(1), hits.Swap(0))
	}
}

// 204 X-Arc-Visible: true, and a query right after it (no flush in between)
// on a query handler that already served the measurement sees every row:
// the current hour, an earlier hour, the next hour.
func TestWaitVisible204ThenQuerySeesRows1759(t *testing.T) {
	now := time.Now().UTC()
	hourEnd := now.Truncate(time.Hour).Add(time.Hour)
	if hourEnd.Sub(now) < time.Minute || now.Sub(now.Truncate(time.Hour)) < time.Minute {
		t.Skip("too close to an hour boundary")
	}
	times := []time.Time{now.Add(-2 * time.Second), now.Add(-2 * time.Hour), hourEnd.Add(time.Minute)}
	noTime := func(times ...time.Time) []byte { // the decoder stamps them
		b, err := msgpack.Marshal(map[string]interface{}{"m": "cpu", "columns": map[string]interface{}{"v": make([]float64, len(times))}})
		require.NoError(t, err)
		return b
	}
	rows := func(times ...time.Time) []byte {
		items := make([]interface{}, len(times))
		for i, at := range times {
			items[i] = row("cpu", at.UnixMilli(), map[string]interface{}{"v": float64(i)}, nil)
		}
		b, err := msgpack.Marshal(items)
		require.NoError(t, err)
		return b
	}
	mp := func(times ...time.Time) []byte { return mpCPU(t, times...) }
	gzipped := func(times ...time.Time) []byte { return gz(t, string(mpCPU(t, times...))) }
	cases := []struct {
		name, path string
		decimal    bool
		body       func(...time.Time) []byte
		lastMinute int64 // of the three rows, those a query from a minute ago reads
	}{
		{"msgpack", "/api/v1/write/msgpack", false, mp, 2},
		{"msgpack without time", "/api/v1/write/msgpack", false, noTime, 3},
		{"msgpack rows", "/api/v1/write/msgpack", false, rows, 2},
		{"msgpack gzip", "/api/v1/write/msgpack", false, gzipped, 2},
		{"lp typed", "/api/v1/write/line-protocol", false, lpCPU, 2},
		{"lp record (decimal columns)", "/api/v1/write/line-protocol", true, lpCPU, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newVisRig(t, visOpts{ageMS: 200, decimal: tc.decimal})
			// A row stored and queried first, so the queries after the
			// 204 run on a handler that has served this measurement.
			resp, out := r.post(t, tc.path, tc.body(now.Add(-10*time.Second)))
			require.Equal(t, http.StatusNoContent, resp.StatusCode, out)
			require.NoError(t, r.buf.FlushAllAndWait(context.Background()))
			q3h := "SELECT COUNT(*) FROM d.cpu WHERE time >= '" + sqlTime(now.Add(-3*time.Hour)) + "'"
			q1m := "SELECT COUNT(*) FROM d.cpu WHERE time >= '" + sqlTime(now.Add(-time.Minute)) + "'"
			require.Equal(t, int64(1), r.count(t, q3h))
			require.Equal(t, int64(1), r.count(t, q1m))
			snap := metrics.Get().Snapshot()
			latency, requests := snap["http_latency_count"].(int64), snap["http_requests_total"]
			counts := waitVisibleCounts()
			resp, out = r.post(t, tc.path, tc.body(times...), WaitVisibleHeader, "visible")
			require.Equal(t, http.StatusNoContent, resp.StatusCode, out)
			v, reason := visible(resp.Header)
			require.Equal(t, "true", v)
			require.Empty(t, reason)
			require.Equal(t, int64(4), r.count(t, q3h))
			require.Equal(t, 1+tc.lastMinute, r.count(t, q1m))

			snap = metrics.Get().Snapshot()
			require.Equal(t, latency, snap["http_latency_count"].(int64), "a waited write is left out of the HTTP latency")
			require.NotEqual(t, requests, snap["http_requests_total"])
			require.Equal(t, counts["visible"]+1, waitVisibleCounts()["visible"])
			require.Zero(t, r.wv.waiters.Load(), "the slot is released")

			_, _ = r.post(t, tc.path, tc.body(now)) // the control: recorded
			require.Equal(t, latency+1, metrics.Get().Snapshot()["http_latency_count"].(int64))
		})
	}
}

// Accepted, not known to be visible: 202 with the reason and rows_written.
func TestWaitVisible202Reasons1759(t *testing.T) {
	now := time.Now().UTC()
	nearHourEnd := now.Truncate(time.Hour).Add(time.Hour).Sub(now) < 2*time.Minute
	s3 := visLocal
	s3.StorageBackend = "s3"
	cases := []struct {
		name   string
		o      visOpts
		budget time.Duration
		body   []byte
		reason ingest.VisibleReason
		lost   bool // the write loses its outcome on the way (a seam)
	}{
		{"timeout", visOpts{}, 200 * time.Millisecond, mpCPU(t, now, now), ingest.VisibleReasonTimeout, false},
		{"object storage", visOpts{ageMS: 100, wv: &s3}, 10 * time.Second, mpCPU(t, now, now), ingest.VisibleReasonStorageNotConfirmable, false},
		{"future stamped", visOpts{ageMS: 100}, 10 * time.Second, mpCPU(t, now, now.Add(30*time.Second)), ingest.VisibleReasonFutureStamped, false},
		{"outcome lost", visOpts{}, 10 * time.Second, mpCPU(t, now, now), ingest.VisibleReasonNotConfirmed, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.reason == ingest.VisibleReasonFutureStamped && nearHourEnd {
				t.Skip("too close to an hour boundary")
			}
			setVar(t, &waitVisibleBudget, tc.budget)
			if tc.lost {
				write := msgpackWrite
				t.Cleanup(func() { msgpackWrite = write })
				msgpackWrite = func(b *ingest.ArrowBuffer, _ context.Context, db string, records interface{}) error {
					return write(b, context.Background(), db, records)
				}
			}
			r := newVisRig(t, tc.o)
			start := time.Now()
			resp, out := r.post(t, "/api/v1/write/msgpack", tc.body, WaitVisibleHeader, "visible")
			require.Equal(t, http.StatusAccepted, resp.StatusCode, out)
			v, reason := visible(resp.Header)
			require.Equal(t, "false", v)
			require.Equal(t, string(tc.reason), reason)
			require.JSONEq(t, fmt.Sprintf(`{"visible":false,"reason":%q,"rows_written":2}`, tc.reason), out)
			require.Less(t, time.Since(start), 5*time.Second)
			if tc.reason == ingest.VisibleReasonFutureStamped {
				// The reason is real: with pruning on, a query from 20 s
				// ahead misses the stored row stamped 30 s ahead once
				// another file of the hour survives the pass (alone, the
				// hour's glob is kept). That file: a row at now, named as
				// if flushed 25 s from now.
				hourDir := filepath.Join(r.dir, "d", "cpu", now.Format("2006/01/02/15"))
				first, _ := filepath.Glob(filepath.Join(hourDir, "*.parquet"))
				require.Len(t, first, 1)
				resp, out = r.post(t, "/api/v1/write/line-protocol", lpCPU(now))
				require.Equal(t, http.StatusNoContent, resp.StatusCode, out)
				require.NoError(t, r.buf.FlushAllAndWait(context.Background()))
				files, _ := filepath.Glob(filepath.Join(hourDir, "*.parquet"))
				require.Len(t, files, 2)
				later := files[0]
				if later == first[0] {
					later = files[1]
				}
				require.NoError(t, os.Rename(later, filepath.Join(hourDir,
					fmt.Sprintf("cpu_%s_%09d.parquet", now.Add(25*time.Second).Format("20060102_150405"), 0))))
				require.Equal(t, int64(3), r.count(t, "SELECT COUNT(*) FROM d.cpu WHERE time >= '"+sqlTime(now.Add(-time.Minute))+"'"))
				require.Zero(t, r.count(t, "SELECT COUNT(*) FROM d.cpu WHERE time >= '"+sqlTime(now.Add(20*time.Second))+"'"))
			}
		})
	}
}

// A failed flush of rows without a WAL copy: 500, as an import answers it.
func TestWaitVisibleRowsAtRisk500_1759(t *testing.T) {
	setVar(t, &waitVisibleBudget, 10*time.Second)
	r := newVisRig(t, visOpts{ageMS: 100, backend: func(b storage.Backend) storage.Backend {
		return &failPathBackend{Backend: b, fail: "/cpu/"}
	}})
	resp, out := r.post(t, "/api/v1/write/line-protocol", lpCPU(time.Now(), time.Now(), time.Now()), WaitVisibleHeader, "visible")
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode, out)
	v, reason := visible(resp.Header)
	require.Equal(t, "false", v)
	require.Equal(t, string(ingest.VisibleReasonRowsAtRisk), reason)
	var body map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &body))
	require.Equal(t, float64(3), body["rows_at_risk"])
	require.Equal(t, float64(3), body["rows_written"])
	require.Contains(t, body["error"], "may still be stored, check before resending")
}

// Partial and stopped writes keep their status and are not waited on; an
// empty MessagePack write keeps its 204 and is visible at once.
func TestWaitVisibleNotWaited1759(t *testing.T) {
	setVar(t, &waitVisibleBudget, 10*time.Second)
	r := newVisRig(t, visOpts{})
	start := time.Now()
	resp, out := r.post(t, "/api/v1/write/line-protocol", []byte("cpu v=1i\ncpu time=1i\n"), WaitVisibleHeader, "visible")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, out)
	require.Contains(t, out, "partial write")
	v, reason := visible(resp.Header)
	require.Equal(t, []string{"false", visibleReasonPartial}, []string{v, reason})

	resp, out = r.post(t, "/api/v1/write/msgpack", []byte{0x90}, WaitVisibleHeader, "visible")
	require.Equal(t, http.StatusNoContent, resp.StatusCode, out)
	v, reason = visible(resp.Header)
	require.Equal(t, []string{"true", ""}, []string{v, reason})

	write := msgpackWrite
	t.Cleanup(func() { msgpackWrite = write })
	msgpackWrite = func(*ingest.ArrowBuffer, context.Context, string, interface{}) error {
		return &ingest.WriteStoppedError{Err: ingest.ErrWALNotDurable, RowsWritten: 1, Written: []string{"aaa"}, StoppedAt: "bbb"}
	}
	resp, out = r.post(t, "/api/v1/write/msgpack", mpCPU(t, time.Now()), WaitVisibleHeader, "visible")
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode, out)
	require.Contains(t, out, "stopped_at_measurement")
	v, reason = visible(resp.Header)
	require.Equal(t, []string{"false", visibleReasonStopped}, []string{v, reason})
	require.Less(t, time.Since(start), 5*time.Second)
	require.Zero(t, r.wv.waiters.Load())
}

// The HTTP drain answers a waiting write at once: 202 shutdown.
func TestWaitVisibleShutdown1759(t *testing.T) {
	setVar(t, &waitVisibleBudget, 15*time.Second)
	r := newVisRig(t, visOpts{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = r.app.Listener(ln) }()
	type answer struct {
		status int
		header http.Header
		err    error
	}
	got := make(chan answer, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+"/api/v1/write/msgpack", bytes.NewReader(mpCPU(t, time.Now())))
		req.Header.Set("x-arc-database", "d")
		req.Header.Set(WaitVisibleHeader, "visible")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			got <- answer{err: err}
			return
		}
		_ = resp.Body.Close()
		got <- answer{status: resp.StatusCode, header: resp.Header}
	}()
	require.Eventually(t, func() bool { return r.wv.waiters.Load() == 1 && r.buf.BufferedBytes() > 0 }, 5*time.Second, 5*time.Millisecond)
	start := time.Now()
	go func() { _ = r.app.ShutdownWithTimeout(10 * time.Second) }()
	a := <-got
	require.NoError(t, a.err)
	require.Equal(t, http.StatusAccepted, a.status)
	require.Equal(t, string(ingest.VisibleReasonShutdown), a.header.Get(VisibleReasonHeader))
	require.Less(t, time.Since(start), 5*time.Second)
}

// Every verdict maps to one status: 204 only for visible, 500 only for rows
// at risk, 202 for the rest, an unknown reason included.
func TestRespondVisibleMapping1759(t *testing.T) {
	cases := []struct {
		v      ingest.VisibleVerdict
		status int
		reason string
	}{
		{ingest.VisibleVerdict{Visible: true}, 204, ""},
		{ingest.VisibleVerdict{Visible: true, Empty: true}, 204, ""},
		{ingest.VisibleVerdict{Reason: ingest.VisibleReasonRowsAtRisk, RowsAtRisk: 4}, 500, "flush_failed_rows_at_risk"},
		{ingest.VisibleVerdict{Reason: ingest.VisibleReasonWALReplay}, 202, "flush_failed_wal_replay"},
		{ingest.VisibleVerdict{Reason: ingest.VisibleReasonShutdown}, 202, "shutdown"},
		{ingest.VisibleVerdict{Reason: ingest.VisibleReasonTimeout}, 202, "timeout"},
		{ingest.VisibleVerdict{Reason: ingest.VisibleReasonNotConfirmed}, 202, "not_confirmed"},
		{ingest.VisibleVerdict{Reason: ingest.VisibleReasonStorageNotConfirmable}, 202, "storage_not_confirmable"},
		{ingest.VisibleVerdict{Reason: ingest.VisibleReasonFutureStamped}, 202, "future_stamped"},
		{ingest.VisibleVerdict{Reason: "some_later_reason"}, 202, "some_later_reason"},
		{ingest.VisibleVerdict{}, 202, "not_confirmed"},
	}
	for _, tc := range cases {
		app := fiber.New()
		app.Get("/", func(c *fiber.Ctx) error { return respondVisible(c, tc.v, 7) })
		resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil), -1)
		require.NoError(t, err)
		out, _ := io.ReadAll(resp.Body)
		require.Equal(t, tc.status, resp.StatusCode, "%+v", tc.v)
		v, reason := visible(resp.Header)
		require.Equal(t, strconv.FormatBool(tc.status == 204), v)
		require.Equal(t, tc.reason, reason)
		switch tc.status {
		case 202:
			require.JSONEq(t, fmt.Sprintf(`{"visible":false,"reason":%q,"rows_written":7}`, tc.reason), string(out))
		case 500:
			require.Contains(t, string(out), `"rows_at_risk":4`)
		}
	}
}

// A browser may send x-arc-wait and read the answer's headers.
func TestWaitVisibleCORS1759(t *testing.T) {
	app := NewServer(DefaultServerConfig(), zerolog.Nop()).GetApp()
	app.Post("/w", func(c *fiber.Ctx) error { return c.SendStatus(http.StatusNoContent) })
	req := httptest.NewRequest(http.MethodOptions, "/w", nil)
	req.Header.Set("Origin", "https://app.example")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "x-arc-wait")
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	require.Contains(t, strings.ToLower(resp.Header.Get("Access-Control-Allow-Headers")), "x-arc-wait")
	req = httptest.NewRequest(http.MethodPost, "/w", nil)
	req.Header.Set("Origin", "https://app.example")
	resp, err = app.Test(req, -1)
	require.NoError(t, err)
	require.Equal(t, VisibleHeader+","+VisibleReasonHeader, resp.Header.Get("Access-Control-Expose-Headers"))
}
