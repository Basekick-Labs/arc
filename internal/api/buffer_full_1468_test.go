package api

// #1468: while the rows waiting to be flushed are at ingest.max_buffered_bytes,
// every HTTP write path answers 503 + Retry-After + X-Arc-Ingest-Refused:
// buffer_full and writes nothing; imports are refused from 90% of the limit,
// other writes only at it.

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

const testMaxBufferedBytes = 1 << 20

// newPrefilledBuffer returns a buffer with a 1 MiB limit already holding
// about fill bytes, which stay buffered (no size or age flush).
func newPrefilledBuffer(t *testing.T, fill int) *ingest.ArrowBuffer {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1_000_000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
		MaxBufferedBytes: testMaxBufferedBytes,
	}, backend, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	if err := buf.WriteColumnarDirect(context.Background(), "other", "fill", map[string][]interface{}{
		"time": {time.Now().UnixMicro()},
		"blob": {strings.Repeat("x", fill)},
	}); err != nil {
		t.Fatal(err)
	}
	return buf
}

func TestBufferFullWritePathsTable(t *testing.T) {
	for _, tc := range writePathCases() {
		t.Run(tc.name, func(t *testing.T) {
			refusals, _ := captureRefusals(t)
			handlerLogs := &syncBuf{}
			app := fiber.New(fiber.Config{DisableStartupMessage: true})
			buf := newPrefilledBuffer(t, testMaxBufferedBytes)
			tc.register(app, buf, zerolog.New(handlerLogs))
			held := buf.BufferedBytes()
			beforeErrors := metrics.Get().Snapshot()["ingest_errors_total"]
			beforeRejections := metrics.Get().Snapshot()["ingest_buffer_full_rejections"].(int64)

			resp, err := app.Test(tc.req(), 10000)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != fiber.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "5" ||
				resp.Header.Get(IngestRefusedHeader) != ingest.RefusalBufferFull {
				t.Fatalf("status %d retry=%q refused=%q body %s", resp.StatusCode, resp.Header.Get("Retry-After"), resp.Header.Get(IngestRefusedHeader), body)
			}
			if resp.Header.Get(IngestFencedHeader) != "" {
				t.Fatal("a buffer-full refusal carries the fence header: a forwarder would read it as a failover")
			}
			if got := buf.BufferedBytes(); got != held {
				t.Fatalf("a refused request was buffered: %d -> %d bytes", held, got)
			}
			if strings.Contains(handlerLogs.String(), `"level":"error"`) {
				t.Fatalf("handler logged the refusal as an error:\n%s", handlerLogs.String())
			}
			if !strings.Contains(refusals.String(), `"reason":"buffer_full"`) ||
				!strings.Contains(refusals.String(), "arc_ingest_buffer_full_rejections_total") {
				t.Fatalf("refusal not logged by its reason and metric:\n%s", refusals.String())
			}
			if after := metrics.Get().Snapshot()["ingest_errors_total"]; after != beforeErrors {
				t.Fatalf("counted as an ingest error (%v -> %v)", beforeErrors, after)
			}
			if after := metrics.Get().Snapshot()["ingest_buffer_full_rejections"].(int64); after != beforeRejections+1 {
				t.Fatalf("rejections %d -> %d, want +1", beforeRejections, after)
			}
		})
	}
}

// Between 90% of the limit and the limit, imports are refused and other
// writes are not.
func TestBufferFullImportBand(t *testing.T) {
	for _, tc := range writePathCases() {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New(fiber.Config{DisableStartupMessage: true})
			buf := newPrefilledBuffer(t, testMaxBufferedBytes/100*93)
			if b := buf.BufferedBytes(); b < testMaxBufferedBytes/100*90 || b >= testMaxBufferedBytes {
				t.Fatalf("prefill %d bytes is outside the import band", b)
			}
			tc.register(app, buf, zerolog.Nop())
			resp, err := app.Test(tc.req(), 10000)
			if err != nil {
				t.Fatal(err)
			}
			refused := resp.Header.Get(IngestRefusedHeader) == ingest.RefusalBufferFull
			isImport := strings.Contains(tc.name, "import")
			if refused != isImport {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status %d refused=%v, want refused=%v; body %s", resp.StatusCode, refused, isImport, body)
			}
		})
	}
}

// With ingest.max_buffered_bytes on, imports pace below 90% of it, but a pass
// never below its own baseline (it would wait for a level other writers may
// keep it from reaching, then fail).
func TestImportPacerCeiling(t *testing.T) {
	require.Equal(t, int64(0), importCeiling(0))
	require.Equal(t, int64(900), importCeiling(1000))

	p := newImportPacer(2, 0)
	p.ceiling = 900
	at := func(n int64) func() int64 { return func() int64 { return n } }
	a, b := &importWriter{}, &importWriter{}

	require.NoError(t, p.acquire(context.Background(), a, at(100), 500))
	require.Equal(t, int64(600), p.limit(), "under the ceiling: unchanged")
	require.NoError(t, p.acquire(context.Background(), b, at(200), 500))
	require.Equal(t, int64(900), p.limit(), "capped at the ceiling")
	for _, w := range []*importWriter{a, b} {
		p.releaseSlot(w)
		p.leave(w)
	}

	c := &importWriter{}
	require.NoError(t, p.acquire(context.Background(), c, at(950), 100))
	require.Equal(t, int64(950), p.limit(), "a pass that started above the ceiling paces at its baseline")

	q := newImportPacer(1, 0) // no limit: no ceiling
	require.NoError(t, q.acquire(context.Background(), &importWriter{}, at(950), 100))
	require.Equal(t, int64(1050), q.limit())
}
