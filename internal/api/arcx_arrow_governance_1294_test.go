//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/queryregistry"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

// Tests for the ArcX Arrow IPC path carried from #1294 (#727). The native
// engine is not linked in this build, so the hook is replaced by a dispatcher
// that serves a synthetic reader through the same helper the hook calls.

func arcxTestRecords(t *testing.T, batches, rowsPerBatch int) (*arrow.Schema, []arrow.Record, *memory.CheckedAllocator) {
	t.Helper()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	records := make([]arrow.Record, batches)
	for b := range records {
		rows := make([][]interface{}, rowsPerBatch)
		for r := range rows {
			rows[r] = []interface{}{int64(b*rowsPerBatch + r)}
		}
		records[b] = buildArrowBatch(alloc, schema, rows)
	}
	return schema, records, alloc
}

func metricValue(t *testing.T, name string) int64 {
	t.Helper()
	v, ok := metrics.Get().Snapshot()[name].(int64)
	if !ok {
		t.Fatalf("metric %s missing from the snapshot", name)
	}
	return v
}

// The handler passes its governance cap to the ArcX hook, and the hook's
// response carries the cap end to end: the body stops at the cap, the
// Arc-Rows-Capped trailer names it, and the registry records it. Before #727
// the ArcX path never received the cap and streamed every row.
func TestArcxArrowHandlerAppliesGovernanceCap(t *testing.T) {
	const rowCap = 150
	_, app, reg := newArcxArrowHandlerRig(t, &governance.Policy{TokenID: 42, MaxRowsPerQuery: rowCap}, 0)

	schema, records, alloc := arcxTestRecords(t, 3, 100)
	defer alloc.AssertSize(t, 0)
	reader := &releaseCountingReader{simpleRecordReader: newSimpleRecordReader(schema, records)}

	original := arcxArrowDispatch
	t.Cleanup(func() { arcxArrowDispatch = original })
	dispatched := 0
	var dispatchStart, registeredAt time.Time
	arcxArrowDispatch = func(
		h *QueryHandler, c *fiber.Ctx, ctx context.Context, cancel context.CancelFunc,
		start time.Time, _, _, _ string, governanceMaxRows int,
		onRows func(int64), onDisconnect func(), onComplete func(int), onFail func(string),
	) bool {
		dispatched++
		dispatchStart = start
		if active := reg.GetActive(); len(active) == 1 {
			registeredAt = active[0].StartTime
		}
		return h.serveArcxArrowIPCResult(
			c.Context(), ctx, cancel, reader, governanceMaxRows, start,
			onRows, onDisconnect, onComplete, onFail,
		)
	}

	resp, body := postArrowQuery(t, app)
	if resp.StatusCode != fiber.StatusOK || dispatched != 1 {
		t.Fatalf("status %d, dispatched %d; want 200 and one ArcX dispatch", resp.StatusCode, dispatched)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/vnd.apache.arrow.stream" {
		t.Errorf("Content-Type = %q", got)
	}
	for _, name := range arrowTrailerNames {
		if _, ok := resp.Trailer[http.CanonicalHeaderKey(name)]; !ok {
			t.Errorf("%s is not declared; a #1570 forward relays only declared trailers", name)
		}
	}
	if got := resp.Trailer.Get(arrowRowsCappedTrailer); got != strconv.Itoa(rowCap) {
		t.Errorf("%s = %q, want %d", arrowRowsCappedTrailer, got, rowCap)
	}
	if got := resp.Trailer.Get(arrowExecutionTimeTrailer); got == "" {
		t.Errorf("%s is empty", arrowExecutionTimeTrailer)
	}
	if got := resp.Trailer.Get(arrowStreamTruncatedTrailer); got != "" {
		t.Errorf("%s = %q on a valid capped result", arrowStreamTruncatedTrailer, got)
	}
	if decoded, err := decodeStream(t, body); err != nil || decoded != rowCap {
		t.Errorf("decoded %d rows (err %v), want %d", decoded, err, rowCap)
	}

	id := resp.Header.Get("X-Arc-Query-ID")
	history := reg.GetHistory(10)
	if id == "" || len(history) != 1 || history[0].ID != id {
		t.Fatalf("query ID %q, history %+v", id, history)
	}
	if q := history[0]; q.Status != queryregistry.StatusCompleted || q.RowCount != rowCap || q.RowCap != rowCap {
		t.Errorf("history = status %q rows %d row_cap %d; want completed, %d, %d", q.Status, q.RowCount, q.RowCap, rowCap, rowCap)
	}
	// The stream stopped at the cap, before EOF: the helper must still
	// release the reader (an arcx C stream otherwise keeps its current batch
	// and its engine workers).
	if reader.releases != 1 {
		t.Errorf("reader released %d times, want 1", reader.releases)
	}
	// Arc-Execution-Time-Ms and the latency histogram measure from the
	// handler's start, as on DuckDB: the start handed to the hook predates the
	// registry entry the handler creates before dispatching.
	if dispatchStart.IsZero() || registeredAt.IsZero() || dispatchStart.After(registeredAt) {
		t.Errorf("start passed to the arcx hook = %v, registry entry started %v; want the handler's start, before registration",
			dispatchStart, registeredAt)
	}
}

// A deadline that expires while the engine runs (QueryStream takes no
// context) must not become a 200 with a poisoned body. The helper declines,
// releasing the reader, and the DuckDB fallback answers 504 before any body,
// with the registry entry timed out, as a DuckDB timeout does.
func TestArcxArrowExpiredBeforeStreamDeclines(t *testing.T) {
	_, app, reg := newArcxArrowHandlerRig(t, nil, 300*time.Millisecond)
	schema, records, alloc := arcxTestRecords(t, 1, 3)
	defer alloc.AssertSize(t, 0)
	reader := &releaseCountingReader{simpleRecordReader: newSimpleRecordReader(schema, records)}

	original := arcxArrowDispatch
	t.Cleanup(func() { arcxArrowDispatch = original })
	var served bool
	arcxArrowDispatch = func(
		h *QueryHandler, c *fiber.Ctx, ctx context.Context, cancel context.CancelFunc,
		start time.Time, _, _, _ string, governanceMaxRows int,
		onRows func(int64), onDisconnect func(), onComplete func(int), onFail func(string),
	) bool {
		<-ctx.Done() // the engine outlived the query's deadline
		served = h.serveArcxArrowIPCResult(
			c.Context(), ctx, cancel, reader, governanceMaxRows, start,
			onRows, onDisconnect, onComplete, onFail,
		)
		return served
	}

	timeoutsBefore := metricValue(t, "query_timeouts_total")
	resp, _ := postArrowQuery(t, app)
	if served {
		t.Error("the helper served a stream whose context had already ended")
	}
	if resp.StatusCode != fiber.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", resp.StatusCode)
	}
	if reader.releases != 1 {
		t.Errorf("reader released %d times, want 1", reader.releases)
	}
	if d := metricValue(t, "query_timeouts_total") - timeoutsBefore; d != 1 {
		t.Errorf("query_timeouts_total moved by %d, want 1", d)
	}
	history := reg.GetHistory(10)
	if len(history) != 1 || history[0].Status != queryregistry.StatusTimedOut {
		t.Errorf("history = %+v, want one timed-out entry", history)
	}
}

// The helper owns the arcx reader and must release it exactly once on every
// way the stream ends, including the ones that stop before EOF. The checked
// allocator additionally proves the batches were freed.
func TestArcxArrowHelperReleasesReader(t *testing.T) {
	cases := []struct {
		name    string
		maxRows int
		cancel  bool // cancel the context after the first batch
		panics  bool
	}{
		{name: "drained to EOF"},
		{name: "stopped at the governance cap", maxRows: 150},
		{name: "cancelled mid-stream", cancel: true},
		{name: "writer panicked", panics: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			metrics.Init(zerolog.Nop())
			schema, records, alloc := arcxTestRecords(t, 3, 100)
			defer alloc.AssertSize(t, 0)
			counting := &releaseCountingReader{simpleRecordReader: newSimpleRecordReader(schema, records)}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var reader array.RecordReader = counting
			if tc.cancel {
				reader = &cancelOnRecordReader{RecordReader: counting, cancel: cancel}
			}
			if tc.panics {
				orig := streamArrowIPCFunc
				t.Cleanup(func() { streamArrowIPCFunc = orig })
				streamArrowIPCFunc = func(context.Context, *bufio.Writer, array.RecordReader, *arrow.Schema,
					*decimalCastInfo, bool, string, int, zerolog.Logger) (int64, error) {
					panic("boom")
				}
			}
			writeArcxArrowResponse(t, reader, ctx, tc.maxRows, nil, nil, nil, nil)
			if counting.releases != 1 {
				t.Errorf("reader released %d times, want 1", counting.releases)
			}
		})
	}
}

// releaseCountingReader counts Release calls on a reader the helper owns.
type releaseCountingReader struct {
	*simpleRecordReader
	releases int
}

func (r *releaseCountingReader) Release() {
	r.releases++
	r.simpleRecordReader.Release()
}

// newArcxArrowHandlerRig builds the real Arrow handler with governance
// licensed, an optional policy for token 42, a query registry and DuckDB for
// the transform and any fallback. Dispatch is the caller's to replace.
func newArcxArrowHandlerRig(t *testing.T, policy *governance.Policy, queryTimeout time.Duration) (*QueryHandler, *fiber.App, *queryregistry.Registry) {
	t.Helper()
	metrics.Init(zerolog.Nop())
	tmpDir := t.TempDir()
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	duckdb, err := database.New(&database.Config{
		MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 2, LocalStorageRoot: tmpDir,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { duckdb.Close() })

	h := newGovernanceTestHandler(newGovernanceTestManager(t, policy), queryTimeout)
	h.db = duckdb
	h.storage = backend
	reg := queryregistry.NewRegistry(&queryregistry.RegistryConfig{HistorySize: 16}, logger)
	h.SetQueryRegistry(reg)

	origLicensed := queryGovernanceLicensed
	queryGovernanceLicensed = func(*QueryHandler) bool { return true }
	t.Cleanup(func() { queryGovernanceLicensed = origLicensed })

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("token_info", &auth.TokenInfo{ID: 42, Name: "capped"})
		return c.Next()
	})
	app.Post("/api/v1/query/arrow", h.executeQueryArrow)
	return h, app, reg
}

func postArrowQuery(t *testing.T, app *fiber.App) (*http.Response, []byte) {
	t.Helper()
	req := httptest.NewRequest("POST", "/api/v1/query/arrow", strings.NewReader(`{"sql":"SELECT 1 AS id"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, body
}

// A panic in the ArcX stream writer must poison a body that already carries a
// decodable batch, set the truncation trailer, fail the registry entry exactly
// once and release the query context (#717, #731). The panic callback can only
// poison the body through the writer captured as streamW.
func TestArcxArrowStreamPanicPoisonsAndFails(t *testing.T) {
	metrics.Init(zerolog.Nop())
	schema, records, alloc := arcxTestRecords(t, 1, 5)
	defer alloc.AssertSize(t, 0)
	reader := newSimpleRecordReader(schema, records)

	orig := streamArrowIPCFunc
	t.Cleanup(func() { streamArrowIPCFunc = orig })
	streamArrowIPCFunc = func(_ context.Context, w *bufio.Writer, r array.RecordReader, s *arrow.Schema,
		_ *decimalCastInfo, _ bool, _ string, _ int, _ zerolog.Logger) (int64, error) {
		// A schema and a whole batch, flushed and never closed: on its own this
		// decodes as a complete five-row result.
		iw := ipc.NewWriter(w, ipc.WithSchema(s))
		r.Next()
		if err := iw.Write(r.RecordBatch()); err != nil {
			t.Errorf("write batch: %v", err)
		}
		_ = w.Flush()
		panic("arcx encoder fault")
	}

	ctx, cancel := context.WithCancel(context.Background())
	var completed, failed int
	var failure string
	resp, body := writeArcxArrowResponse(t, reader, ctx, 0, nil, nil,
		func(int) { completed++ },
		func(msg string) { failed++; failure = msg },
	)
	cancel() // cancel ownership is pinned by TestArcxArrowWriterOwnsCancel
	if completed != 0 || failed != 1 || failure != "stream writer panicked" {
		t.Fatalf("completed %d, failed %d (%q); want one failure from the panic", completed, failed, failure)
	}
	if got := resp.Trailer.Get(arrowStreamTruncatedTrailer); got != "stream writer panicked" {
		t.Errorf("%s = %q, want \"stream writer panicked\"", arrowStreamTruncatedTrailer, got)
	}
	if got := resp.Trailer.Get(arrowRowsCappedTrailer); got != "" {
		t.Errorf("%s = %q on a panicked stream", arrowRowsCappedTrailer, got)
	}
	if _, err := decodeStream(t, body); err == nil {
		t.Error("a stream cut by a panic decoded as a complete result")
	}
}

// The async writer owns cancel: it must call it once the stream has ended,
// on success and on a panic alike, and not before the stream starts.
func TestArcxArrowWriterOwnsCancel(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%v", panics), func(t *testing.T) {
			metrics.Init(zerolog.Nop())
			schema, records, alloc := arcxTestRecords(t, 1, 3)
			defer alloc.AssertSize(t, 0)
			reader := newSimpleRecordReader(schema, records)

			ctx, cancelCtx := context.WithCancel(context.Background())
			defer cancelCtx()
			cancels := 0
			cancel := func() { cancels++; cancelCtx() }

			orig := streamArrowIPCFunc
			t.Cleanup(func() { streamArrowIPCFunc = orig })
			var ctxErrAtStart error
			streamArrowIPCFunc = func(c context.Context, w *bufio.Writer, r array.RecordReader, s *arrow.Schema,
				ci *decimalCastInfo, d bool, comp string, maxRows int, l zerolog.Logger) (int64, error) {
				ctxErrAtStart = c.Err()
				if panics {
					panic("boom")
				}
				return orig(c, w, r, s, ci, d, comp, maxRows, l)
			}

			h := &QueryHandler{logger: zerolog.Nop()}
			resp := runArcxHelper(t, h, ctx, cancel, reader)
			if ctxErrAtStart != nil {
				t.Errorf("stream context already %v when the writer started", ctxErrAtStart)
			}
			if cancels == 0 {
				t.Error("the stream writer never cancelled the query context")
			}
			if !panics {
				if got := resp.Trailer.Get(arrowStreamTruncatedTrailer); got != "" {
					t.Errorf("%s = %q on a clean stream", arrowStreamTruncatedTrailer, got)
				}
			}
		})
	}
}

// A hangup the writer sees as a failed write or flush (errClientDisconnected)
// gets no truncation trailer, since nobody is listening; the disconnect
// recorder fires once, the registry entry fails, and the request counts as an
// error. A hangup the disconnect watcher sees first cancels the context
// instead, and that stream is poisoned and carries the trailer, as on DuckDB.
func TestArcxArrowClientDisconnectIsRecorded(t *testing.T) {
	metrics.Init(zerolog.Nop())
	schema, records, alloc := arcxTestRecords(t, 1, 3)
	defer alloc.AssertSize(t, 0)
	reader := newSimpleRecordReader(schema, records)

	orig := streamArrowIPCFunc
	t.Cleanup(func() { streamArrowIPCFunc = orig })
	streamArrowIPCFunc = func(context.Context, *bufio.Writer, array.RecordReader, *arrow.Schema,
		*decimalCastInfo, bool, string, int, zerolog.Logger) (int64, error) {
		return 2, fmt.Errorf("stream flush failed at row 2: %w: %w", errClientDisconnected, io.ErrClosedPipe)
	}

	errorsBefore := metricValue(t, "query_errors_total")
	var disconnects, completed, failed int
	resp, _ := writeArcxArrowResponse(t, reader, context.Background(), 0, nil,
		func() { disconnects++ },
		func(int) { completed++ },
		func(string) { failed++ },
	)
	if disconnects != 1 || completed != 0 || failed != 1 {
		t.Fatalf("disconnects %d, completed %d, failed %d; want 1, 0, 1", disconnects, completed, failed)
	}
	if got := resp.Trailer.Get(arrowStreamTruncatedTrailer); got != "" {
		t.Errorf("%s = %q after a client disconnect, want empty", arrowStreamTruncatedTrailer, got)
	}
	if d := metricValue(t, "query_errors_total") - errorsBefore; d != 1 {
		t.Errorf("query_errors_total moved by %d, want 1", d)
	}
}

// A query deadline that expires mid-stream is a timeout: the body is
// poisoned, the trailer says why, the failure callback runs, and the timeout
// counter moves, as on the DuckDB path.
func TestArcxArrowDeadlineIsTimeout(t *testing.T) {
	metrics.Init(zerolog.Nop())
	schema, records, alloc := arcxTestRecords(t, 2, 3)
	defer alloc.AssertSize(t, 0)
	// The deadline expires while the first batch is being read, so the stream
	// has started; a context already over before the stream is declined
	// (TestArcxArrowExpiredBeforeStreamDeclines).
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	reader := &waitForDoneReader{RecordReader: newSimpleRecordReader(schema, records), ctx: ctx}
	timeoutsBefore := metricValue(t, "query_timeouts_total")
	var completed, failed int
	resp, body := writeArcxArrowResponse(t, reader, ctx, 0, nil, nil,
		func(int) { completed++ },
		func(string) { failed++ },
	)
	if completed != 0 || failed != 1 {
		t.Fatalf("completed %d, failed %d; want a failure", completed, failed)
	}
	if got := resp.Trailer.Get(arrowStreamTruncatedTrailer); !strings.Contains(got, "deadline exceeded") {
		t.Errorf("%s = %q, want the deadline", arrowStreamTruncatedTrailer, got)
	}
	if d := metricValue(t, "query_timeouts_total") - timeoutsBefore; d != 1 {
		t.Errorf("query_timeouts_total moved by %d, want 1", d)
	}
	if _, err := decodeStream(t, body); err == nil {
		t.Error("a timed-out stream decoded as complete")
	}
}

// waitForDoneReader holds its first batch until ctx ends.
type waitForDoneReader struct {
	array.RecordReader
	ctx context.Context
}

func (r *waitForDoneReader) RecordBatch() arrow.RecordBatch {
	<-r.ctx.Done()
	return r.RecordReader.RecordBatch()
}

func (r *waitForDoneReader) Record() arrow.RecordBatch { return r.RecordBatch() }

// contextArrowRecordReader must count rows read through the deprecated
// Record() as well as RecordBatch(); otherwise a deadline that expires after
// a capped result read through Record() is reported as a truncation.
func TestContextArrowRecordReaderCountsDeprecatedRecord(t *testing.T) {
	schema, records, alloc := arcxTestRecords(t, 1, 10)
	defer alloc.AssertSize(t, 0)
	base := newSimpleRecordReader(schema, records)
	defer base.Release()

	ctx, cancel := context.WithCancel(context.Background())
	r := &contextArrowRecordReader{RecordReader: base, ctx: ctx, maxRows: 10}
	if !r.Next() || r.Record() == nil {
		t.Fatal("no batch")
	}
	cancel()
	if err := r.Err(); err != nil {
		t.Errorf("Err() = %v after the cap was read through Record(); want nil", err)
	}
}

// arcx_hook.go builds only with the arcx_engine tag, which no CI job uses, so
// an import its code no longer references is a compile error nobody sees
// until a release build (#1294 left sqlutil imported after moving its only
// uses to query_arrow.go). Every named import must be referenced.
func TestArcxHookImportsAreUsed(t *testing.T) {
	for _, file := range []string{"arcx_hook.go", "arcx_hook_stub.go"} {
		f, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		used := map[string]bool{}
		ast.Inspect(f, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok {
					used[id.Name] = true
				}
			}
			return true
		})
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			name := path.Base(p)
			if strings.HasPrefix(name, "v") && len(name) > 1 && strings.Trim(name[1:], "0123456789") == "" {
				name = path.Base(path.Dir(p)) // github.com/gofiber/fiber/v2 -> fiber
			}
			if imp.Name != nil {
				if imp.Name.Name == "_" || imp.Name.Name == "." {
					continue
				}
				name = imp.Name.Name
			}
			if !used[name] {
				t.Errorf("%s imports %q as %s but never uses it; the arcx_engine build will not compile", file, p, name)
			}
		}
	}
}

// runArcxHelper drives serveArcxArrowIPCResult with a real cancel func and
// returns the parsed response; writeArcxArrowResponse passes a nil cancel.
func runArcxHelper(t *testing.T, h *QueryHandler, ctx context.Context, cancel context.CancelFunc, reader array.RecordReader) *http.Response {
	t.Helper()
	var requestCtx fasthttp.RequestCtx
	requestCtx.Response.Header.SetStatusCode(fasthttp.StatusOK)
	h.serveArcxArrowIPCResult(&requestCtx, ctx, cancel, reader, 0, time.Now(), nil, nil, nil, nil)
	var wire bytes.Buffer
	w := bufio.NewWriter(&wire)
	if err := requestCtx.Response.Write(w); err != nil {
		t.Fatalf("write response: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush response: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(wire.Bytes())), nil)
	if err != nil {
		t.Fatalf("parse response: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp
}
