//go:build duckdb_arrow

package api

import (
	"bufio"
	"context"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Tests for #716: a panic inside the Arrow IPC stream writer is recovered so
// the process survives, but the cleanup that followed it in straight-line code
// was skipped, stranding the DuckDB reader, leaking a pooled connection for the
// life of the process, and leaving the query timeout timer running.

// recordingConn counts Close calls so a test can tell "released once" from
// "never released" and from "double released".
type recordingConn struct{ closes int }

func (c *recordingConn) Close() error { c.closes++; return nil }

// panicOnReleaseReader lets a test drive the cleanup helper's own recover.
type panicOnReleaseReader struct {
	array.RecordReader
	released bool
}

func (r *panicOnReleaseReader) Release() { r.released = true; panic("release exploded") }

func TestReleaseArrowStreamResources(t *testing.T) {
	t.Run("releases everything exactly once", func(t *testing.T) {
		alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer alloc.AssertSize(t, 0)
		schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
		reader := newSimpleRecordReader(schema, []arrow.Record{
			buildArrowBatch(alloc, schema, [][]interface{}{{int64(1)}}),
		})
		conn := &recordingConn{}
		cancelled := false

		releaseArrowStreamResources(reader, conn, func() { cancelled = true }, zerolog.Nop())

		if conn.closes != 1 {
			t.Errorf("conn.Close called %d times, want 1", conn.closes)
		}
		if !cancelled {
			t.Error("cancel was not called, the timeout timer would leak")
		}
	})

	t.Run("tolerates nil reader, conn and cancel", func(t *testing.T) {
		// conn is held as an interface, so a typed-nil pointer is non-nil
		// here; the guard has to be on the value, not just the interface.
		releaseArrowStreamResources(nil, nil, nil, zerolog.Nop())
	})

	t.Run("contains a panic raised while releasing", func(t *testing.T) {
		reader := &panicOnReleaseReader{}
		conn := &recordingConn{}
		// Must not propagate: on the panic path this runs on a bare
		// fasthttp goroutine where a second panic kills the process.
		releaseArrowStreamResources(reader, conn, nil, zerolog.Nop())
		if !reader.released {
			t.Fatal("Release was not attempted")
		}
	})
}

// TestExecuteQueryArrowReturnsConnectionOnPanic is the #716 regression proper.
// It runs the real handler against a real DuckDB, forces the stream writer to
// panic, and asserts the pooled connection goes back to the pool. Reverting the
// cleanup to straight-line code leaves InUse at 1 and fails this test.
func TestExecuteQueryArrowReturnsConnectionOnPanic(t *testing.T) {
	metrics.Init(zerolog.Nop())

	tmpDir, err := os.MkdirTemp("", "arc-arrow-cleanup-*")
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

	orig := streamArrowIPCFunc
	defer func() { streamArrowIPCFunc = orig }()
	streamArrowIPCFunc = func(context.Context, *bufio.Writer, array.RecordReader, *arrow.Schema,
		*decimalCastInfo, bool, string, int, zerolog.Logger) (int64, error) {
		panic("simulated failure inside the Arrow IPC stream")
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Post("/api/v1/query/arrow", h.executeQueryArrow)

	before := duckdb.Stats()
	req := httptest.NewRequest("POST", "/api/v1/query/arrow", strings.NewReader(`{"sql":"SELECT 1 AS id"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	// Draining the body runs the stream writer goroutine to completion, which
	// is where the panic and the recovery happen.
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// The pool is the operational impact: a leaked connection is gone for the
	// life of the process, so repeated panics starve the endpoint.
	deadline := time.Now().Add(5 * time.Second)
	for {
		inUse := duckdb.Stats().InUse
		if inUse == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("connection never returned to the pool after the stream panicked: InUse=%d (was %d before the request)", inUse, before.InUse)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
