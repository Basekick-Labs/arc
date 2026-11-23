//go:build duckdb_arrow

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/basekick-labs/arc/internal/storage"
)

// probeOpen stands for a job's registry: it sees what a read looks like from
// inside the moment the read asks what to leave out.
type probeOpen struct {
	mu       sync.Mutex
	open     bool
	inFlight []int
	onAsk    func() []string // run when a read asks for its exclusions
	h        *QueryHandler
	gate     func() int
}

func (p *probeOpen) Open(string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inFlight = append(p.inFlight, p.gate())
	return p.open
}

func (p *probeOpen) ExcludedFor(context.Context, string) []string {
	if p.onAsk != nil {
		return p.onAsk()
	}
	return nil
}

func (p *probeOpen) Generation() uint64 { return 1 }

func (p *probeOpen) seen() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int(nil), p.inFlight...)
}

func writeRow1825(t *testing.T, r *overflowRig, key, ts string, n int) {
	t.Helper()
	full := filepath.Join(r.data, key)
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
	_, err := r.duck.Exec(fmt.Sprintf(`COPY (SELECT TIMESTAMPTZ '%s+00' AS "time", %d AS n) TO '%s' (FORMAT PARQUET)`, ts, n, full))
	require.NoError(t, err)
}

func count1825(t *testing.T, resp *http.Response) int {
	t.Helper()
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	require.Equal(t, 200, resp.StatusCode, "body: %s", raw)
	var doc struct {
		Data [][]float64 `json:"data"`
	}
	require.NoError(t, json.Unmarshal(raw, &doc), "body: %s", raw)
	require.Len(t, doc.Data, 1, "body: %s", raw)
	return int(doc.Data[0][0])
}

// Every query route counts as a read in flight while its SQL is built, and
// not once the response is done: a job that swaps files waits for exactly
// the reads that could still bind with the exclusions from before its switch.
func TestQueriesAreReadsInFlight1825(t *testing.T) {
	r := newOverflowRig(t)
	writeRow1825(t, r, "db/cpu/2026/11/18/23/a.parquet", "2026-11-18 23:10:00", 1)
	probe := &probeOpen{gate: r.h.Reads().InFlight}
	r.h.SetOpenCompactions(probe)

	for _, route := range []string{"/api/v1/query", "/api/v1/query/msgpack", "/api/v1/query/arrow", "/api/v1/query/estimate"} {
		before := len(probe.seen())
		// A statement no route has transformed yet: a cached transform never asks.
		resp := r.post(route, fmt.Sprintf("SELECT count(*) AS c FROM db.cpu WHERE n < %d", 100+before))
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode, route)
		seen := probe.seen()[before:]
		require.NotEmpty(t, seen, "%s never asked the registry", route)
		for _, n := range seen {
			require.Equal(t, 1, n, "%s: reads in flight while its SQL was built", route)
		}
		require.Eventually(t, func() bool { return r.h.Reads().InFlight() == 0 }, 5*time.Second, 10*time.Millisecond,
			"%s: still counted in flight after its response", route)
	}
	// The per-measurement route.
	before := len(probe.seen())
	resp, err := r.client.Get(r.base + "/api/v1/query/cpu?database=db&limit=1")
	require.NoError(t, err)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode, "body: %s", raw)
	seen := probe.seen()[before:]
	require.NotEmpty(t, seen, "the measurement route never asked the registry")
	for _, n := range seen {
		require.Equal(t, 1, n, "the measurement route: reads in flight while its SQL was built")
	}
	require.Eventually(t, func() bool { return r.h.Reads().InFlight() == 0 }, 5*time.Second, 10*time.Millisecond)
}

// A wait started while a query is building its SQL returns only once that
// query's statement is bound.
func TestWaitCoversAQueryFromBeforeTheFlip1825(t *testing.T) {
	r := newOverflowRig(t)
	writeRow1825(t, r, "db/cpu/2026/11/18/23/a.parquet", "2026-11-18 23:10:00", 1)
	gate := r.h.Reads()
	waited := make(chan time.Time, 1)
	release := make(chan struct{})
	probe := &probeOpen{open: true, gate: gate.InFlight}
	probe.onAsk = func() []string {
		ended := gate.Flip() // a job switches while this read builds its SQL
		go func() {
			_ = gate.Wait(context.Background(), ended)
			waited <- time.Now()
		}()
		<-release
		return nil
	}
	r.h.SetOpenCompactions(probe)
	done := make(chan time.Time, 1)
	go func() {
		resp := r.post("/api/v1/query", "SELECT count(*) AS c FROM db.cpu")
		_ = count1825(t, resp)
		done <- time.Now()
	}()
	select {
	case <-waited:
		t.Fatal("the wait returned while the query was still building its SQL")
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	select {
	case <-waited:
	case <-time.After(10 * time.Second):
		t.Fatal("the wait never returned")
	}
	<-done
}

// The exclusions are read before the path list is built. A job that puts a
// file in a directory that did not exist and switches reads to it, between
// the two, must not leave a read with neither the old file nor the new one.
func TestExclusionsAreReadBeforeThePathList1825(t *testing.T) {
	r := newOverflowRig(t)
	const x = "db/cpu/2026/11/18/23/x.parquet"
	writeRow1825(t, r, x, "2026-11-19 05:00:00", 1) // misfiled: its row belongs in 11-19/05
	probe := &probeOpen{open: true, gate: r.h.Reads().InFlight}
	var once sync.Once
	probe.onAsk = func() []string {
		// The job's switch, at the moment the read asks: the output lands in
		// a directory that did not exist, and reads now leave X out.
		once.Do(func() { writeRow1825(t, r, "db/cpu/2026/11/19/05/out.parquet", "2026-11-19 05:00:00", 1) })
		return []string{x}
	}
	r.h.SetOpenCompactions(probe)
	resp := r.post("/api/v1/query", "SELECT count(*) AS c FROM db.cpu WHERE time >= '2026-11-18 00:00:00' AND time < '2026-11-20 00:00:00'")
	require.Equal(t, 1, count1825(t, resp), "the read must see the row once: through the new file")
}

// The same, for the read built from a database and a measurement (a query
// that names its database in the x-arc-database header).
func TestExclusionsAreReadBeforeThePathListHeaderDatabase1825(t *testing.T) {
	r := newOverflowRig(t)
	const x = "db/cpu/2026/11/18/23/x.parquet"
	writeRow1825(t, r, x, "2026-11-19 05:00:00", 1)
	probe := &probeOpen{open: true, gate: r.h.Reads().InFlight}
	var once sync.Once
	probe.onAsk = func() []string {
		once.Do(func() { writeRow1825(t, r, "db/cpu/2026/11/19/05/out.parquet", "2026-11-19 05:00:00", 1) })
		return []string{x}
	}
	r.h.SetOpenCompactions(probe)
	sql := "SELECT count(*) AS c FROM cpu WHERE time >= '2026-11-18 00:00:00' AND time < '2026-11-20 00:00:00'"
	from := r.h.buildReadParquetExprForMeasurement(context.Background(), "db", "cpu", sql, "FROM")
	var n int
	require.NoError(t, r.duck.DB().QueryRow("SELECT count(*) "+from+" WHERE time >= '2026-11-18 00:00:00' AND time < '2026-11-20 00:00:00'").Scan(&n), "read: %s", from)
	require.Equal(t, 1, n, "the read must see the row once, through the new file: %s", from)
	require.NotEmpty(t, probe.seen(), "the read never asked the registry")
}

// A continuous query's read is in flight until its rows are in memory, and
// its statement runs under the run's context.
func TestContinuousQueryIsAReadInFlight1825(t *testing.T) {
	r := newOverflowRig(t)
	writeRow1825(t, r, "db/cpu/2026/11/18/23/a.parquet", "2026-11-18 23:10:00", 1)
	backend, err := storage.NewLocalBackend(r.data, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	probe := &probeOpen{gate: r.h.Reads().InFlight}
	h := &ContinuousQueryHandler{logger: zerolog.Nop(), db: r.duck, storage: backend, openCompactions: probe}
	h.SetReads(r.h.Reads())
	cq := &ContinuousQuery{Name: "cq", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "cpu_1h"}
	now := time.Now().UTC()

	// A run whose context is already over does not run its statement.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = h.executeAggregation(ctx, cq, "SELECT time, count(*) AS c FROM db.cpu GROUP BY time", now, now)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "query execution failed") || strings.Contains(err.Error(), "context"), "err = %v", err)
	seen := probe.seen()
	require.NotEmpty(t, seen, "the run never asked the registry")
	require.Equal(t, 1, seen[0], "reads in flight while the run read its exclusions")
	require.Equal(t, 0, r.h.Reads().InFlight(), "still in flight after the run ended")
}
