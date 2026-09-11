//go:build duckdb_arrow

package api

import (
	"bufio"
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Tests for #733. The Arrow IPC writer used to free its resources from two
// places: straight-line at the end of the writer, and again from safeStream's
// onPanic. A panic after the straight-line call therefore ran cleanup twice.
//
// That was harmless in practice, but only because all three resources tolerate
// it: the DuckDB reader's Release has an explicit refCount <= 0 guard,
// *sql.Conn.Close returns ErrConnDone, and cancel is idempotent by contract.
// The array.RecordReader interface promises none of that, and arrow-go's own
// readers guard over-release behind debug.Assert, which is compiled out unless
// built with -tags assert. So no test and no CI job could ever have detected
// the invariant being lost, which is why it is now structural: one defer.

// panicOnMessageHook panics the first time zerolog emits the given message,
// which is the only injection point that lands AFTER the old straight-line
// release and before the writer returns.
//
// Keyed on the message rather than the level, because releaseArrowStreamResources
// logs at Error on its own recovery path and would re-enter the hook.
type panicOnMessageHook struct {
	msg   string
	fired atomic.Bool
}

func (h *panicOnMessageHook) Run(_ *zerolog.Event, _ zerolog.Level, msg string) {
	if msg == h.msg && h.fired.CompareAndSwap(false, true) {
		panic("injected panic after the Arrow IPC stream writer finished")
	}
}

func TestExecuteQueryArrowReleasesExactlyOnce(t *testing.T) {
	run := func(t *testing.T, hook zerolog.Hook) int32 {
		t.Helper()
		metrics.Init(zerolog.Nop())

		tmpDir := t.TempDir()
		logger := zerolog.Nop()
		if hook != nil {
			logger = zerolog.New(io.Discard).Hook(hook)
		}

		backend, err := storage.NewLocalBackend(tmpDir, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		duckdb, err := database.New(&database.Config{
			MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 2, LocalStorageRoot: tmpDir,
		}, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { duckdb.Close() })

		h := newGovernanceTestHandler(newGovernanceTestManager(t, nil), 0)
		h.db = duckdb
		h.storage = backend
		h.logger = logger

		origStream := streamArrowIPCFunc
		t.Cleanup(func() { streamArrowIPCFunc = origStream })
		streamArrowIPCFunc = func(_ context.Context, w *bufio.Writer, _ array.RecordReader, _ *arrow.Schema,
			_ *decimalCastInfo, _ bool, _ string, _ int, _ zerolog.Logger) (int64, error) {
			_, _ = w.Write([]byte{0})
			_ = w.Flush()
			return 1, nil
		}

		var releases int32
		origRelease := releaseArrowStreamResourcesFunc
		t.Cleanup(func() { releaseArrowStreamResourcesFunc = origRelease })
		releaseArrowStreamResourcesFunc = func(r array.RecordReader, c interface{ Close() error },
			cancel context.CancelFunc, lg zerolog.Logger) {
			atomic.AddInt32(&releases, 1)
			origRelease(r, c, cancel, lg)
		}

		app := fiber.New(fiber.Config{DisableStartupMessage: true})
		app.Post("/api/v1/query/arrow", h.executeQueryArrow)
		req := httptest.NewRequest("POST", "/api/v1/query/arrow", strings.NewReader(`{"sql":"SELECT 1 AS id"}`))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req, 10000)
		if err != nil {
			t.Fatalf("app.Test: %v", err)
		}
		if _, err := io.ReadAll(resp.Body); err != nil {
			t.Fatalf("draining body: %v", err)
		}
		resp.Body.Close()
		return atomic.LoadInt32(&releases)
	}

	t.Run("normal completion releases once", func(t *testing.T) {
		if got := run(t, nil); got != 1 {
			t.Errorf("cleanup ran %d times, want exactly 1", got)
		}
	})

	// The regression proper: on 17aaaef this reports 2, because the panic is
	// raised after the straight-line release and onPanic then runs it again.
	t.Run("a panic after the stream finishes still releases once", func(t *testing.T) {
		hook := &panicOnMessageHook{msg: "Arrow streaming query completed"}
		got := run(t, hook)
		if !hook.fired.Load() {
			t.Fatal("the injected panic never fired, so this asserts nothing")
		}
		if got != 1 {
			t.Errorf("cleanup ran %d times, want exactly 1", got)
		}
	})

}
