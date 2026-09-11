package api

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Tests for #717. fasthttp runs body-stream writers on a bare goroutine with
// no recovery, so a panic in one is a process crash rather than a failed
// request. safeStream is what stands between the two.

func TestSafeStreamRecoversAndRunsOnPanic(t *testing.T) {
	metrics.Init(zerolog.Nop())
	h := &QueryHandler{logger: zerolog.Nop()}

	cleaned := false
	wrapped := h.safeStream("test", func() { cleaned = true }, func(*bufio.Writer) {
		panic("boom")
	})

	// The point of the wrapper: this call returns instead of killing the
	// process. TestSafeStreamIsLoadBearing proves the negative direction.
	wrapped(bufio.NewWriter(&bytes.Buffer{}))

	if !cleaned {
		t.Error("onPanic did not run, so resources and registry entries would leak")
	}
}

func TestSafeStreamLeavesTheHappyPathAlone(t *testing.T) {
	metrics.Init(zerolog.Nop())
	h := &QueryHandler{logger: zerolog.Nop()}

	ran := false
	onPanicRan := false
	var buf bytes.Buffer
	wrapped := h.safeStream("test", func() { onPanicRan = true }, func(w *bufio.Writer) {
		ran = true
		w.WriteString("payload")
		w.Flush()
	})
	wrapped(bufio.NewWriter(&buf))

	if !ran {
		t.Fatal("the wrapped writer never ran")
	}
	if onPanicRan {
		t.Error("onPanic ran on the success path; it is for the panic path only")
	}
	if buf.String() != "payload" {
		t.Errorf("body = %q, want %q", buf.String(), "payload")
	}
}

func TestSafeStreamContainsAPanicFromOnPanic(t *testing.T) {
	metrics.Init(zerolog.Nop())
	h := &QueryHandler{logger: zerolog.Nop()}

	// Cleanup runs while a panic is already in flight. If it panics too and
	// that escapes, the process dies anyway and the wrapper is worthless.
	wrapped := h.safeStream("test", func() { panic("cleanup exploded") }, func(*bufio.Writer) {
		panic("original")
	})
	wrapped(bufio.NewWriter(&bytes.Buffer{}))
}

func TestSafeStreamNilOnPanicIsFine(t *testing.T) {
	metrics.Init(zerolog.Nop())
	h := &QueryHandler{logger: zerolog.Nop()}
	h.safeStream("test", nil, func(*bufio.Writer) { panic("boom") })(bufio.NewWriter(&bytes.Buffer{}))
}

// TestSafeStreamIsLoadBearing proves the negative direction the positive tests
// cannot: without the wrapper the panic is fatal. It has to run in a
// subprocess, because an unrecovered panic on a stream-writer goroutine takes
// the whole test binary down with it and reports no per-test failure.
func TestSafeStreamIsLoadBearing(t *testing.T) {
	if os.Getenv("ARC_STREAM_PANIC_CHILD") == "1" {
		// Unwrapped, exactly as every one of these writers looked before
		// this change.
		func(sw func(*bufio.Writer)) { sw(bufio.NewWriter(&bytes.Buffer{})) }(func(*bufio.Writer) {
			panic("unwrapped stream writer")
		})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestSafeStreamIsLoadBearing$", "-test.v")
	cmd.Env = append(os.Environ(), "ARC_STREAM_PANIC_CHILD=1")
	out, err := cmd.CombinedOutput()

	if err == nil {
		t.Fatal("an unwrapped panicking stream writer exited cleanly; the premise of #717 no longer holds and this test needs revisiting")
	}
	if !strings.Contains(string(out), "unwrapped stream writer") {
		t.Errorf("child died for the wrong reason:\n%s", out)
	}
}

// TestExecuteQueryReturnsConnectionOnStreamPanic exercises a converted site
// end to end. POST /api/v1/query held rows, an optional profiling connection
// and cancel in straight-line code after the stream call, so a panic stranded
// a pooled connection. Reverting that cleanup to straight-line fails this test.
func TestExecuteQueryReturnsConnectionOnStreamPanic(t *testing.T) {
	metrics.Init(zerolog.Nop())

	tmpDir, err := os.MkdirTemp("", "arc-stream-safety-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	duckdb, err := database.New(&database.Config{
		MemoryLimit:      "256MB",
		ThreadCount:      2,
		MaxConnections:   2,
		LocalStorageRoot: tmpDir,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer duckdb.Close()

	h := &QueryHandler{
		db:         duckdb,
		logger:     logger,
		storage:    backend,
		queryCache: database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize),
		pruner:     pruning.NewPartitionPruner(zerolog.Nop()),
	}

	// streamed guards against a vacuous pass: if the request fails before the
	// body writer runs, the pool is trivially idle and the test would report
	// success while proving nothing.
	// Force the database/sql fallback: with the duckdb_arrow build the Arrow
	// path handles the query and the JSON stream writer never runs. handled
	//=false is the driver-does-not-support-Arrow signal the handler already
	// falls back on.
	origArrow := arrowJSONQueryFunc
	defer func() { arrowJSONQueryFunc = origArrow }()
	arrowJSONQueryFunc = func(*QueryHandler, *fiber.Ctx, context.Context, context.CancelFunc, string, bool, int,
		time.Time, string, func(int), func(string), func()) (int, bool) {
		return 0, false
	}

	streamed := false
	orig := streamTypedJSONFunc
	defer func() { streamTypedJSONFunc = orig }()
	streamTypedJSONFunc = func(context.Context, *bufio.Writer, []string, []colType, rowScanner, int,
		*database.QueryProfile, time.Time, string) (int, error) {
		streamed = true
		panic("simulated failure inside the JSON stream")
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Post("/api/v1/query", h.executeQuery)

	req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(`{"sql":"SELECT 1 AS id"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if !streamed {
		t.Fatalf("the body stream writer never ran, so this proves nothing: status=%d body=%s", resp.StatusCode, body)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if duckdb.Stats().InUse == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("connection never returned to the pool after the stream panicked: InUse=%d", duckdb.Stats().InUse)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
