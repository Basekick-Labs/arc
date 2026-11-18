package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestQuantilesNearestRank(t *testing.T) {
	var d []time.Duration
	for i := 1; i <= 100; i++ {
		d = append(d, time.Duration(i)*time.Millisecond)
	}
	// Shuffled order must not matter.
	d[0], d[99] = d[99], d[0]
	q := quantilesOf(d)
	if q.N != 100 || q.P50 != 50 || q.P95 != 95 || q.P99 != 99 || q.Max != 100 {
		t.Errorf("quantiles of 1..100 ms = %+v", q)
	}
	if q := quantilesOf([]time.Duration{7 * time.Millisecond}); q.P50 != 7 || q.P99 != 7 || q.Max != 7 {
		t.Errorf("quantiles of one value = %+v", q)
	}
	if q := quantilesOf(nil); q.N != 0 || q.String() != "n=0" {
		t.Errorf("quantiles of nothing = %+v", q)
	}
}

func TestTrackerBracketsVisibility(t *testing.T) {
	t0 := time.Unix(1000, 0)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	tr := newTracker()
	tr.acked(1, at(0))
	tr.acked(2, at(10))
	tr.acked(3, at(20))
	if tr.pending() != 3 {
		t.Fatalf("pending = %d, want 3", tr.pending())
	}
	// Probe sent at 30 (previous one sent at 5) returns marker 2 only: rows
	// do not have to become visible in the order they were written.
	tr.seen([]int64{2}, at(5), at(40))
	// A later probe returns 1 and 2 again; 2's first hit must stand.
	tr.seen([]int64{1, 2}, at(30), at(70))
	// A marker a probe returns before its ack is recorded still counts.
	tr.seen([]int64{9}, at(60), at(90))
	tr.acked(9, at(95))

	upper, lower, tracked, visible := tr.lags()
	if tracked != 4 || visible != 3 || tr.pending() != 1 {
		t.Fatalf("tracked=%d visible=%d pending=%d, want 4, 3, 1", tracked, visible, tr.pending())
	}
	got := map[time.Duration]time.Duration{}
	for i := range upper {
		got[upper[i]] = lower[i]
	}
	want := map[time.Duration]time.Duration{
		30 * time.Millisecond: 0,                     // marker 2: hit at 40, acked at 10; last miss sent at 5, before the ack
		70 * time.Millisecond: 30 * time.Millisecond, // marker 1: hit at 70, acked at 0; last miss sent at 30
		0:                     0,                     // marker 9: seen before its ack; never negative
	}
	if len(got) != len(want) {
		t.Fatalf("lags = %v, want %v", got, want)
	}
	for u, l := range want {
		if got[u] != l {
			t.Errorf("upper %v: lower = %v, want %v (all: %v)", u, got[u], l, got)
		}
	}
}

func TestLineProtocol(t *testing.T) {
	at := time.Unix(0, 1700000000123456789)
	got := string(lineProtocol("m", "r1", 42, 2, at))
	want := "m,run=r1 marker=42i 1700000000123456789\n" +
		"m,run=r1 marker=0i,f=0i 1700000000123456789\n" +
		"m,run=r1 marker=0i,f=1i 1700000000123456789\n"
	if got != want {
		t.Errorf("line protocol =\n%s\nwant\n%s", got, want)
	}
}

func TestParseMetricsAndDelta(t *testing.T) {
	const before = `# HELP arc_buffer_flushes_total x
arc_buffer_flushes_total 10
arc_ingest_publish_delay_seconds_bucket{le="5"} 2
arc_ingest_publish_delay_seconds_bucket{le="5.5"} 7
arc_ingest_publish_delay_seconds_bucket{le="+Inf"} 8
arc_ingest_publish_delay_seconds_sum 40
arc_ingest_publish_delay_seconds_count 8
arc_ingest_publish_delay_max_seconds 9.5
`
	const after = `arc_buffer_flushes_total 14
arc_ingest_publish_delay_seconds_bucket{le="5"} 2
arc_ingest_publish_delay_seconds_bucket{le="5.5"} 10
arc_ingest_publish_delay_seconds_bucket{le="+Inf"} 12
arc_ingest_publish_delay_seconds_sum 61.5
arc_ingest_publish_delay_seconds_count 12
arc_ingest_publish_delay_max_seconds 9.5
`
	b, err := parseMetrics(strings.NewReader(before))
	if err != nil {
		t.Fatal(err)
	}
	a, err := parseMetrics(strings.NewReader(after))
	if err != nil {
		t.Fatal(err)
	}
	d := a.minus(b)
	if d.Flushes != 4 || d.PublishCount != 4 || d.PublishSumS != 21.5 || d.PublishMaxS != 9.5 {
		t.Errorf("delta = %+v", d)
	}
	if got := d.bucketLine(); got != "5.5: 3  +Inf: 1" {
		t.Errorf("bucket line = %q, want the per-bucket (not cumulative) change", got)
	}
	if got := d.meanMS(); got != 5375 {
		t.Errorf("mean = %v ms, want 5375", got)
	}
	// A server without the metric (an older binary) reads as zeros.
	old, err := parseMetrics(strings.NewReader("arc_buffer_flushes_total 3\n"))
	if err != nil || old.publishCount != 0 || len(old.minus(old).Buckets) != 0 {
		t.Errorf("metrics without the histogram: %+v, %v", old, err)
	}
}

// fakeArc is a stand-in for Arc: writes go to a buffer that becomes visible
// to queries when flush is called.
type fakeArc struct {
	mu       sync.Mutex
	buffered []int64
	visible  []int64
	queries  []string
	flushes  int
}

func (f *fakeArc) flushNow() {
	f.mu.Lock()
	f.visible = append(f.visible, f.buffered...)
	f.buffered = nil
	f.flushes++
	f.mu.Unlock()
}

func (f *fakeArc) handler(t *testing.T) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/write/line-protocol", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" || r.Header.Get("x-arc-database") == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			var m int64
			fields := strings.Fields(line)
			if len(fields) != 3 {
				t.Errorf("bad line %q", line)
				continue
			}
			for _, kv := range strings.Split(fields[1], ",") {
				if v, ok := strings.CutPrefix(kv, "marker="); ok {
					_ = json.Unmarshal([]byte(strings.TrimSuffix(v, "i")), &m)
				}
			}
			if m > 0 {
				f.buffered = append(f.buffered, m)
			}
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /api/v1/write/line-protocol/flush", func(w http.ResponseWriter, r *http.Request) {
		f.flushNow()
		_, _ = w.Write([]byte(`{"status":"success"}`))
	})
	snapshot := func(sql string) []int64 {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.queries = append(f.queries, sql)
		return append([]int64(nil), f.visible...)
	}
	rows := func(w http.ResponseWriter, markers []int64, cols []string) {
		data := make([][]any, len(markers))
		for i, m := range markers {
			data[i] = []any{"2026-01-01T00:00:00Z", m}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "columns": cols, "data": data})
	}
	sqlOf := func(r *http.Request) string {
		var req struct{ SQL string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		return req.SQL
	}
	mux.HandleFunc("POST /api/v1/query", func(w http.ResponseWriter, r *http.Request) {
		markers := snapshot(sqlOf(r))
		if len(markers) == 0 {
			// What Arc answers for a measurement with no file yet.
			_, _ = w.Write([]byte(`{"success":true,"columns":[],"data":[],"row_count":0}`))
			return
		}
		rows(w, markers, []string{"time", "marker"})
	})
	mux.HandleFunc("GET /api/v1/query/{measurement}", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("limit") == "" || q.Get("order_by") != "time ASC" || q.Get("database") == "" {
			http.Error(w, "the endpoint's defaults (100 rows, newest first) would truncate the probe", http.StatusBadRequest)
			return
		}
		rows(w, snapshot(q.Get("where")), []string{"time", "marker"})
	})
	mux.HandleFunc("POST /api/v1/query/msgpack", func(w http.ResponseWriter, r *http.Request) {
		markers := snapshot(sqlOf(r))
		_ = msgpack.NewEncoder(w).Encode(map[string]any{"success": true, "columns": []string{"marker"}, "data": [][]int64{markers}})
	})
	mux.HandleFunc("POST /api/v1/query/arrow", func(w http.ResponseWriter, r *http.Request) {
		markers := snapshot(sqlOf(r))
		schema := arrow.NewSchema([]arrow.Field{{Name: "marker", Type: arrow.PrimitiveTypes.Int64}}, nil)
		bld := array.NewInt64Builder(memory.DefaultAllocator)
		bld.AppendValues(markers, nil)
		col := bld.NewArray()
		rec := array.NewRecord(schema, []arrow.Array{col}, int64(len(markers)))
		wr := ipc.NewWriter(w, ipc.WithSchema(schema))
		_ = wr.Write(rec)
		_ = wr.Close()
		rec.Release()
		col.Release()
		bld.Release()
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		n := f.flushes
		f.mu.Unlock()
		_, _ = w.Write([]byte("arc_buffer_flushes_total " + strconv.Itoa(n) + "\n"))
	})
	return mux
}

// Every endpoint's response is decoded to the same list of markers.
func TestProbesDecodeEveryEndpoint(t *testing.T) {
	f := &fakeArc{visible: []int64{3, 1, 2}}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()
	cl := &client{base: srv.URL, token: "tok", http: srv.Client()}
	from := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	for _, endpoint := range []string{"json", "msgpack", "arrow", "measurement"} {
		p, err := newProbe(endpoint, "db", "m", "r1", from, from.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		got, err := p.run(context.Background(), cl)
		if err != nil {
			t.Fatalf("%s: %v", endpoint, err)
		}
		if len(got) != 3 || got[0] != 3 || got[1] != 1 || got[2] != 2 {
			t.Errorf("%s: markers = %v, want [3 1 2]", endpoint, got)
		}
	}
	if _, err := newProbe("csv", "db", "m", "r1", from, from); err == nil {
		t.Error("an unknown endpoint was accepted")
	}
	// The time range is a pair of quoted literals as bare conjuncts: the one
	// form Arc's partition pruner reads.
	want := "SELECT marker FROM db.m WHERE time >= '2026-11-18T10:00:00Z' AND time < '2026-11-18T11:00:00Z' AND run = 'r1' AND marker > 0"
	if f.queries[0] != want {
		t.Errorf("probe SQL =\n%s\nwant\n%s", f.queries[0], want)
	}
	// A measurement with no file yet answers with no columns: no rows, no error.
	f.mu.Lock()
	f.visible = nil
	f.mu.Unlock()
	if got, err := cl.queryMarkers(context.Background(), "SELECT 1"); err != nil || len(got) != 0 {
		t.Errorf("empty result: %v, %v", got, err)
	}
}

// End to end against the stand-in: rows become visible only when it flushes,
// so the lag is at least the time to the next flush, and every row is seen.
func TestSteadyAgainstAStandIn(t *testing.T) {
	f := &fakeArc{}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(40 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				f.flushNow()
			}
		}
	}()
	out := t.TempDir() + "/report.json"
	var buf bytes.Buffer
	err := steadyCmd(context.Background(), []string{
		"-http", srv.URL, "-token", "tok", "-rate", "200", "-duration", "300ms", "-poll", "1ms",
		"-timeout", "10s", "-prequery", "-out", out,
	}, &buf)
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	var rep steadyReport
	data, err := io.ReadAll(mustOpen(t, out))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Acked == 0 || rep.WriteErrors != 0 || rep.ProbeErrors != 0 {
		t.Fatalf("acked=%d write_errors=%d probe_errors=%d\n%s", rep.Acked, rep.WriteErrors, rep.ProbeErrors, buf.String())
	}
	if rep.Visible != rep.Acked || rep.NeverVisible != 0 {
		t.Errorf("visible=%d of %d acked, never_visible=%d\n%s", rep.Visible, rep.Acked, rep.NeverVisible, buf.String())
	}
	if rep.LagMS.N != rep.Acked || rep.LagMS.Max <= 0 || rep.LagLowerMS.Max > rep.LagMS.Max {
		t.Errorf("lag = %+v, lower bound = %+v", rep.LagMS, rep.LagLowerMS)
	}
	if rep.Server == nil || rep.Server.Flushes == 0 {
		t.Errorf("server delta = %+v (%s)", rep.Server, rep.ServerScrapeEr)
	}
	// The probe's SQL text is the same for the whole run.
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, q := range f.queries {
		if q != f.queries[0] {
			t.Fatalf("the probe SQL changed during the run:\n%s\n%s", f.queries[0], q)
		}
	}
}

// The late mode writes the late row only after the query that finds its hour
// empty, and polls both a repeated and a never-seen SQL text.
func TestLateAgainstAStandIn(t *testing.T) {
	f := &fakeArc{}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()
	var buf bytes.Buffer
	err := lateCmd(context.Background(), []string{
		"-http", srv.URL, "-token", "tok", "-trials", "2", "-delays", "0s", "-poll", "1ms", "-timeout", "10s",
	}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "not visible within 10s: 0 of 2 trials") {
		t.Errorf("report:\n%s", buf.String())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.flushes != 4 {
		t.Errorf("%d flushes for 2 trials, want 4 (seed and late row each)", f.flushes)
	}
	var unique int
	for _, q := range f.queries {
		if strings.Contains(q, "marker <> -") {
			unique++
		}
	}
	if unique < 2 {
		t.Errorf("%d never-seen SQL texts sent, want one per trial at least", unique)
	}
	if err := lateCmd(context.Background(), []string{"-http", srv.URL, "-token", "tok", "-offset", "-10m"}, io.Discard); err == nil {
		t.Error("an offset inside the current hour was accepted")
	}
}

func mustOpen(t *testing.T, path string) io.Reader {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(data)
}
