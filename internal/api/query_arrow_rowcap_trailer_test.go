//go:build duckdb_arrow

package api

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"net/textproto"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// TestExecuteQueryArrowSetsRowsCappedTrailer is the #724 regression proper for
// the Arrow IPC path: it drives the real handler, through real governance, to a
// real chunked HTTP response, and reads the trailer off the wire. Arrow IPC has
// no envelope to carry the marker, so this trailer is the whole signal on that
// endpoint; without a test at this level, deleting either the AddTrailer or the
// Set leaves the suite green and every Arrow IPC client silently loses it.
//
// streamArrowIPCFunc is stubbed so the row count is exact and the test does not
// depend on how DuckDB batches a result.
func TestExecuteQueryArrowSetsRowsCappedTrailer(t *testing.T) {
	if raceDetectorEnabled {
		// Any success-path request to this handler trips the pre-existing
		// trailer race described in #729, including with the #724 trailer
		// removed, because Arc-Execution-Time-Ms is set unconditionally.
		t.Skip("blocked by #729: Arrow IPC trailer Set races with fasthttp header serialisation")
	}
	const cap = 10000

	newApp := func(t *testing.T, policy *governance.Policy, rowsStreamed int64, streamErr error) *fiber.App {
		t.Helper()
		metrics.Init(zerolog.Nop())

		tmpDir := t.TempDir()
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
		t.Cleanup(func() { duckdb.Close() })

		h := newGovernanceTestHandler(newGovernanceTestManager(t, policy), 0)
		h.db = duckdb
		h.storage = backend

		origLicensed := queryGovernanceLicensed
		queryGovernanceLicensed = func(*QueryHandler) bool { return true }
		t.Cleanup(func() { queryGovernanceLicensed = origLicensed })

		orig := streamArrowIPCFunc
		t.Cleanup(func() { streamArrowIPCFunc = orig })
		streamArrowIPCFunc = func(_ context.Context, w *bufio.Writer, _ array.RecordReader, _ *arrow.Schema,
			_ *decimalCastInfo, _ bool, _ string, maxRows int, _ zerolog.Logger) (int64, error) {
			// Write and flush at least one byte, as the real streamArrowIPC
			// always does (it emits the schema message before anything else).
			// The flush is what orders this goroutine after fasthttp's header
			// serialisation: fasthttp writes the response head, then reads the
			// body pipe, so a stream writer that never writes has no
			// happens-before edge to the header and any trailer Set it makes
			// races with formatStatusLine. A stub that writes nothing would
			// report a data race that production code cannot hit.
			_, _ = w.Write([]byte{0})
			_ = w.Flush()
			return rowsStreamed, streamErr
		}

		app := fiber.New(fiber.Config{DisableStartupMessage: true})
		app.Use(func(c *fiber.Ctx) error {
			c.Locals("token_info", &auth.TokenInfo{ID: 42, Name: "capped"})
			return c.Next()
		})
		app.Post("/api/v1/query/arrow", h.executeQueryArrow)
		return app
	}

	// trailerOf drains the body first: Go populates Response.Trailer only once
	// the chunked body has been read to EOF.
	trailerOf := func(t *testing.T, app *fiber.App) string {
		t.Helper()
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
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		// Presence is checked separately from value: fasthttp emits every
		// REGISTERED trailer on every response, empty when never Set, and the
		// client contract rests on that. Get() alone cannot tell an empty
		// trailer from a missing one.
		if _, ok := resp.Trailer[textproto.CanonicalMIMEHeaderKey(arrowRowsCappedTrailer)]; !ok {
			t.Errorf("%s is absent from the trailers; it must be registered on every response", arrowRowsCappedTrailer)
		}
		return resp.Trailer.Get(arrowRowsCappedTrailer)
	}

	t.Run("a result that reached the cap carries the cap in the trailer", func(t *testing.T) {
		app := newApp(t, &governance.Policy{TokenID: 42, MaxRowsPerQuery: cap}, cap, nil)
		if got := trailerOf(t, app); got != "10000" {
			t.Errorf("%s = %q, want \"10000\"", arrowRowsCappedTrailer, got)
		}
	})

	t.Run("a result short of the cap leaves the trailer empty", func(t *testing.T) {
		app := newApp(t, &governance.Policy{TokenID: 42, MaxRowsPerQuery: cap}, cap-1, nil)
		// Empty, not absent: the trailer is registered on every response, so
		// the client contract is "non-empty means capped".
		if got := trailerOf(t, app); got != "" {
			t.Errorf("%s = %q on an under-cap result, want empty", arrowRowsCappedTrailer, got)
		}
	})

	t.Run("no policy cap leaves the trailer empty", func(t *testing.T) {
		app := newApp(t, nil, 5_000_000, nil)
		if got := trailerOf(t, app); got != "" {
			t.Errorf("%s = %q with no policy, want empty", arrowRowsCappedTrailer, got)
		}
	})

	t.Run("a failed stream is not marked capped", func(t *testing.T) {
		// The body is poisoned and Arc-Stream-Truncated says do not trust it,
		// which subsumes the cap. Marking both would tell the client the body
		// is valid up to the cap when it is not decodable at all.
		app := newApp(t, &governance.Policy{TokenID: 42, MaxRowsPerQuery: cap}, cap, errors.New("boom"))
		if got := trailerOf(t, app); got != "" {
			t.Errorf("%s = %q on a failed stream, want empty", arrowRowsCappedTrailer, got)
		}
	})
}
