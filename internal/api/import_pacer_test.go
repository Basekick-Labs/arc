package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/stretchr/testify/require"
)

// setPacer replaces the node's import pacer for one test.
func setPacer(t *testing.T, p *importPacer) {
	t.Helper()
	old := importPacerCurrent.Load()
	importPacerCurrent.Store(p)
	t.Cleanup(func() { importPacerCurrent.Store(old) })
}

func TestImportAutoConcurrency(t *testing.T) {
	const mib = 1 << 20
	for _, tc := range []struct {
		name      string
		mem       uint64
		allowance int64
		procs     int
		want      int
	}{
		{"memory unknown", 0, 256 * mib, 16, 1},
		{"3 GiB pod (#1296)", 3 << 30, 384 * mib, 8, 1},
		{"16 GiB", 16 << 30, 512 * mib, 14, 4},
		{"320 GiB, 46 cores", 320 << 30, 512 * mib, 46, 46},
		{"small memory", 256 * mib, 64 * mib, 8, 1},
		{"one core", 64 << 30, 512 * mib, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, importAutoConcurrency(tc.mem, tc.allowance, tc.procs))
		})
	}
}

// One pass: its start plus its allowance, as before #1429. Several: the
// lowest start plus every allowance, the same for all of them; a pass that
// leaves takes its share and, if it was the oldest, its baseline.
func TestImportPacerSharedLimit(t *testing.T) {
	p := newImportPacer(3, 0)
	a, b, c := &importWriter{}, &importWriter{}, &importWriter{}
	at := func(n int64) func() int64 { return func() int64 { return n } }

	require.NoError(t, p.acquire(context.Background(), a, at(100), 10))
	require.Equal(t, int64(110), p.limit())
	require.NoError(t, p.acquire(context.Background(), b, at(500), 20))
	require.NoError(t, p.acquire(context.Background(), c, at(300), 30))
	require.Equal(t, int64(100+10+20+30), p.limit())

	p.releaseSlot()
	p.leave(a)
	require.Equal(t, int64(300+20+30), p.limit())
	p.releaseSlot()
	p.leave(c)
	require.Equal(t, int64(500+20), p.limit())
}

func TestImportPacerSlots(t *testing.T) {
	p := newImportPacer(2, 40*time.Millisecond)
	zero := func() int64 { return 0 }
	require.NoError(t, p.acquire(context.Background(), &importWriter{}, zero, 1))
	require.NoError(t, p.acquire(context.Background(), &importWriter{}, zero, 1))

	start := time.Now()
	err := p.acquire(context.Background(), &importWriter{}, zero, 1)
	require.ErrorIs(t, err, ingest.ErrImportQueueTimeout)
	require.True(t, ingest.IsWriteRefusal(err))
	require.GreaterOrEqual(t, time.Since(start), 35*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, p.acquire(ctx, &importWriter{}, zero, 1), context.Canceled)

	// A freed slot is taken by the next waiter.
	done := make(chan error, 1)
	go func() { done <- newImportPacer(0, 0).acquire(context.Background(), &importWriter{}, zero, 1) }()
	require.NoError(t, <-done, "a pacer asked for 0 slots still has one")
	p.releaseSlot()
	require.NoError(t, p.acquire(context.Background(), &importWriter{}, zero, 1))
}

// No timeout: a waiter waits as long as its context, then gets its error,
// not a refusal.
func TestImportPacerNoQueueTimeout(t *testing.T) {
	p := newImportPacer(1, 0)
	zero := func() int64 { return 0 }
	require.NoError(t, p.acquire(context.Background(), &importWriter{}, zero, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, p.acquire(ctx, &importWriter{}, zero, 1), context.DeadlineExceeded)
}

// Every slot busy past the queue timeout: the import is refused with the
// refusal headers and nothing is written (#1429; before, it waited for as
// long as the request lived, and a client that gave up could not stop it).
func TestImportRefusedWhenEverySlotStaysBusy(t *testing.T) {
	setPacer(t, newImportPacer(1, 50*time.Millisecond))
	ci := newChunkedImport(t, 1000)
	holder, err := newImportWriter(context.Background(), ci.buf, "other")
	require.NoError(t, err)
	defer holder.close()

	csv := []byte("time,v\n1700000000,1\n1700000001,2\n")
	for _, tc := range []struct {
		name, path, ct string
		content        []byte
	}{
		{"lp", "/api/v1/import/lp?db=d", "", []byte("cpu v=1 1700000000000000000\ncpu v=2 1700000001000000000\n")},
		{"csv", "/api/v1/import/csv?db=d&measurement=cpu&time_format=epoch_s", "", csv},
		{"parquet", "/api/v1/import/parquet?db=d&measurement=cpu", "", parquetFile(t, 10, -1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, ct := multipartUpload(t, tc.content)
			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(body))
			req.Header.Set("Content-Type", ct)
			resp, err := ci.app.Test(req, -1)
			require.NoError(t, err)
			require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
			require.Equal(t, ingest.RefusalImportQueue, resp.Header.Get(IngestRefusedHeader))
			require.NotEmpty(t, resp.Header.Get("Retry-After"))
		})
	}
	require.Empty(t, ci.writes, "a refused import wrote rows")
	require.Zero(t, ci.buf.BufferedBytes())
}

// Two slots: an import runs while another pass holds one.
func TestImportRunsBesideAnotherPass(t *testing.T) {
	setPacer(t, newImportPacer(2, 50*time.Millisecond))
	ci := newChunkedImport(t, 1000)
	holder, err := newImportWriter(context.Background(), ci.buf, "other")
	require.NoError(t, err)
	defer holder.close()

	status, body := ci.post(t, "/api/v1/import/lp?db=d", []byte("cpu v=1 1700000000000000000\n"))
	require.Equal(t, http.StatusOK, status, body)
}

// The final flush runs with the slot given back and the pass still counted
// in the shared limit, observed from inside that window; after the answer the
// pass is no longer counted (#1429).
func TestImportFinalFlushWithoutSlotButCounted(t *testing.T) {
	p := newImportPacer(1, 0)
	setPacer(t, p)
	ci := newChunkedImport(t, 1000)
	var saw bool
	setVar(t, &importBeforeFinalFlushHook, func(w *importWriter) {
		saw = true
		require.Len(t, p.slots, 0, "slot still held during the final flush")
		p.mu.Lock()
		_, counted := p.active[w]
		p.mu.Unlock()
		require.True(t, counted, "pass no longer counted before its rows are flushed")
	})
	for _, tc := range []struct {
		path    string
		content []byte
	}{
		{"/api/v1/import/lp?db=d", []byte("cpu v=1 1700000000000000000\n")},
		{"/api/v1/import/csv?db=d&measurement=cpu&time_format=epoch_s", []byte("time,v\n1700000000,1\n")},
		{"/api/v1/import/parquet?db=d&measurement=pq", parquetFile(t, 10, -1)},
	} {
		saw = false
		status, body := ci.post(t, tc.path, tc.content)
		require.Equal(t, http.StatusOK, status, body)
		require.True(t, saw, tc.path)
		p.mu.Lock()
		require.Empty(t, p.active)
		p.mu.Unlock()
	}
}

// The final flush covers the import's own measurements, not every buffer on
// the node: another writer's buffered rows stay buffered (#1429).
func TestImportFinalFlushIsScopedToTheImport(t *testing.T) {
	ci := newChunkedImport(t, 100000)
	batch := &ingest.TypedColumnBatch{Data: map[string]interface{}{
		"time": []int64{1700000000000000},
		"v":    []float64{1},
	}}
	require.NoError(t, ci.buf.WriteTypedColumnarDirect(context.Background(), "d", "streamed", batch, 1))
	require.NoError(t, ci.buf.WriteTypedColumnarDirect(context.Background(), "x", "cpu", batch, 1))

	status, body := ci.post(t, "/api/v1/import/lp?db=d", []byte("cpu v=1 1700000000000000000\n"))
	require.Equal(t, http.StatusOK, status, body)

	has := func(rel string) bool {
		var found bool
		_ = filepath.WalkDir(filepath.Join(ci.dir, rel), func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(p, ".parquet") {
				found = true
			}
			return nil
		})
		return found
	}
	require.Eventually(t, func() bool { return has("d/cpu") }, 5*time.Second, 10*time.Millisecond, "the import's rows were not flushed")
	require.False(t, has("d/streamed"), "the import flushed another measurement's buffer")
	require.False(t, has("x/cpu"), "the import flushed another database's buffer")
}

// A batch refused for schema churn is written again, up to the retry bound;
// past it the import stops (#1429).
func TestImportRetriesSchemaChurn(t *testing.T) {
	setVar(t, &importChurnBackoff, time.Millisecond)
	for _, tc := range []struct {
		name     string
		failures int
		ok       bool
	}{
		{"recovers", importChurnRetries, true},
		{"gives up", importChurnRetries + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ci := newChunkedImport(t, 1000)
			calls := 0
			real := importWriteDirect
			setVar(t, &importWriteDirect, func(b *ingest.ArrowBuffer, ctx context.Context, db, m string, batch *ingest.TypedColumnBatch, n int) error {
				calls++
				if calls <= tc.failures {
					return ingest.ErrSchemaChurnExceeded
				}
				return real(b, ctx, db, m, batch, n)
			})
			status, body := ci.post(t, "/api/v1/import/lp?db=d", []byte("cpu v=1 1700000000000000000\n"))
			if tc.ok {
				require.Equal(t, http.StatusOK, status, body)
				require.Equal(t, importChurnRetries+1, calls)
				return
			}
			require.Equal(t, http.StatusServiceUnavailable, status, body)
			require.Equal(t, importChurnRetries+1, calls)
		})
	}
}
