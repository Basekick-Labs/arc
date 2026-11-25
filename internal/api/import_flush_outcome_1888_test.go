package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// failPathBackend fails writes whose path contains fail.
type failPathBackend struct {
	storage.Backend
	fail string
}

func (b *failPathBackend) Write(ctx context.Context, path string, data []byte) error {
	if strings.Contains(path, b.fail) {
		return errors.New("injected storage failure")
	}
	return b.Backend.Write(ctx, path, data)
}

func (b *failPathBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	if strings.Contains(path, b.fail) {
		return errors.New("injected storage failure")
	}
	return b.Backend.WriteReader(ctx, path, r, size)
}

func failingImport(t *testing.T) *chunkedImport {
	t.Helper()
	setVar(t, &lpChunkBytes, 64)
	dir := t.TempDir()
	local, err := storage.NewLocalBackend(dir, zerolog.Nop())
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		// 5 rows per buffer: the import's size-triggered flushes run on the
		// workers, the path the import used not to wait for.
		MaxBufferSize: 5, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 2, FlushQueueSize: 16, ShardCount: 1, DataPageVersion: "2.0",
	}, &failPathBackend{Backend: local, fail: "/fail_m/"}, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	h := NewImportHandler(zerolog.Nop())
	h.SetArrowBuffer(buf)
	return &chunkedImport{app: importApp(t, h), buf: buf, dir: dir}
}

func lpRows(measurement string, n int) []byte {
	var lp strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&lp, "%s v=%di %d\n", measurement, i, 1700000000000000000+int64(i)*1e9)
	}
	return []byte(lp.String())
}

// #1888: with the WAL off, an import whose own flushes fail answers 500,
// not 200: its rows were held only in memory. It used to answer 200 while a
// size-triggered flush of its rows was still running or had failed.
func TestLPImportFailsWhenItsOwnFlushFails1888(t *testing.T) {
	ci := failingImport(t)
	status, body := ci.post(t, "/api/v1/import/lp?db=d", lpRows("fail_m", 23))
	require.Equal(t, http.StatusInternalServerError, status, body)
	msg := fmt.Sprint(body["error"])
	require.Contains(t, msg, "may still be stored")
	require.Contains(t, msg, "had no WAL copy")
}

// The same import of a measurement whose flushes succeed answers 200, after
// its files are written.
func TestLPImportSucceedsWhenItsFlushesSucceed1888(t *testing.T) {
	ci := failingImport(t)
	status, body := ci.post(t, "/api/v1/import/lp?db=d", lpRows("ok_m", 23))
	require.Equal(t, http.StatusOK, status, body)
	// Answered after the flushes: the files are there now, without a barrier.
	n := 0
	walkParquet(t, ci.dir, func(rec arrow.RecordBatch) { n += int(rec.NumRows()) })
	require.Equal(t, 23, n, "the import answered before its files were written")
}
