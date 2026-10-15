package api

// #676: a write refused for WAL volume disk pressure is answered 503 +
// Retry-After + X-Arc-Ingest-Refused: wal_disk on every write path, logged by
// reason and never per request, and not counted as an ingest error. A request
// admitted up front is never half written when pressure starts mid-request.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// newDiskGatedBuffer returns a buffer whose disk gate admits the first
// admitFirst checks and reports pressure after, and the check counter.
func newDiskGatedBuffer(t *testing.T, admitFirst int64) (*ingest.ArrowBuffer, *atomic.Int64) {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	var calls atomic.Int64
	buf.SetWALDiskGate(ingest.PressureFunc(func() bool { return calls.Add(1) > admitFirst }))
	return buf, &calls
}

func multipartFile(name, content string) (*bytes.Buffer, string) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	fw, _ := w.CreateFormFile("file", name)
	_, _ = fw.Write([]byte(content))
	_ = w.Close()
	return &b, w.FormDataContentType()
}

const (
	testLP  = "cpu,host=a value=1 1700000000000000000\nmem,host=a value=2 1700000000000000000\n"
	testTLE = "ISS (ZARYA)\n1 25544U 98067A   08264.51782528 -.00002182  00000-0 -11606-4 0  2927\n2 25544  51.6416 247.4627 0006703 130.5360 325.0288 15.72125391563537\n"
)

type writePathCase struct {
	name       string
	admitFirst int64
	register   func(*fiber.App, *ingest.ArrowBuffer, zerolog.Logger)
	req        func() *http.Request
}

// writePathCases is every HTTP write path, each with a small valid request
// (an unparseable one for CSV and Parquet, refused before parsing).
func writePathCases() []writePathCase {
	importHandler := func(app *fiber.App, b *ingest.ArrowBuffer, lg zerolog.Logger) {
		h := NewImportHandler(lg)
		h.SetArrowBuffer(b)
		h.RegisterRoutes(app)
	}
	upload := func(path, name, content string) func() *http.Request {
		return func() *http.Request {
			body, ct := multipartFile(name, content)
			r := httptest.NewRequest("POST", path, body)
			r.Header.Set("Content-Type", ct)
			r.Header.Set("x-arc-database", "db")
			return r
		}
	}
	plain := func(path, body string) func() *http.Request {
		return func() *http.Request {
			r := httptest.NewRequest("POST", path, strings.NewReader(body))
			r.Header.Set("x-arc-database", "db")
			return r
		}
	}
	return []writePathCase{
		{"msgpack", 0, func(app *fiber.App, b *ingest.ArrowBuffer, lg zerolog.Logger) {
			NewMsgPackHandler(lg, b, 10<<20).RegisterRoutes(app)
		}, func() *http.Request {
			body, _ := msgpack.Marshal(map[string]interface{}{
				"m": "cpu", "columns": map[string]interface{}{"time": []int64{time.Now().UnixMicro()}, "value": []float64{1}},
			})
			r := httptest.NewRequest("POST", "/api/v1/write/msgpack", bytes.NewReader(body))
			r.Header.Set("Content-Type", "application/msgpack")
			r.Header.Set("x-arc-database", "db")
			return r
		}},
		{"line protocol", 0, func(app *fiber.App, b *ingest.ArrowBuffer, lg zerolog.Logger) {
			NewLineProtocolHandler(b, lg).RegisterRoutes(app)
		}, plain("/api/v1/write/line-protocol", testLP)},
		{"tle", 0, func(app *fiber.App, b *ingest.ArrowBuffer, lg zerolog.Logger) {
			NewTLEHandler(b, lg).RegisterRoutes(app)
		}, plain("/api/v1/write/tle", testTLE)},
		{"lp import", 0, importHandler, upload("/api/v1/import/lp?db=db", "data.lp", testLP)},
		{"tle import", 0, importHandler, upload("/api/v1/import/tle?db=db", "data.tle", testTLE)},
		// Unparseable: answered 503 only if the refusal comes before parsing.
		{"csv import, refused before parsing", 0, importHandler,
			upload("/api/v1/import/csv?db=db&measurement=m", "data.csv", "no_time_column\n1\n")},
		{"parquet import, refused before parsing", 0, importHandler,
			upload("/api/v1/import/parquet?db=db&measurement=m", "data.parquet", "not parsed: refused first")},
	}
}

func TestWALDiskPressureWritePathsTable(t *testing.T) {
	cases := writePathCases()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			refusals, _ := captureRefusals(t)
			handlerLogs, reqLogs := &syncBuf{}, &syncBuf{}
			app := fiber.New(fiber.Config{DisableStartupMessage: true})
			app.Use(requestLogger(zerolog.New(reqLogs)))
			buf, _ := newDiskGatedBuffer(t, tc.admitFirst)
			tc.register(app, buf, zerolog.New(handlerLogs))
			before := metrics.Get().Snapshot()["ingest_errors_total"]
			resp, err := app.Test(tc.req(), 10000)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != fiber.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "5" ||
				resp.Header.Get(IngestRefusedHeader) != ingest.RefusalWALDisk {
				t.Fatalf("status %d retry=%q refused=%q body %s", resp.StatusCode, resp.Header.Get("Retry-After"), resp.Header.Get(IngestRefusedHeader), body)
			}
			if resp.Header.Get(IngestFencedHeader) != "" {
				t.Fatal("a disk refusal carries the fence header: a forwarder would read it as a failover")
			}
			if !strings.Contains(string(body), `"error"`) {
				t.Fatalf("no JSON error: %s", body)
			}
			if strings.Contains(handlerLogs.String(), `"level":"error"`) {
				t.Fatalf("handler logged the refusal as an error:\n%s", handlerLogs.String())
			}
			if strings.Contains(reqLogs.String(), "HTTP request error") {
				t.Fatalf("logged per request:\n%s", reqLogs.String())
			}
			if !strings.Contains(refusals.String(), `"reason":"wal_disk"`) {
				t.Fatalf("refusal not logged by its reason:\n%s", refusals.String())
			}
			if after := metrics.Get().Snapshot()["ingest_errors_total"]; after != before {
				t.Fatalf("counted as an ingest error (%v -> %v)", before, after)
			}
		})
	}
}

// Pressure that starts after the handler admitted the request, between its
// measurements, does not split it: both measurements are written.
func TestWALDiskPressureNeverHalfAcceptsARequest(t *testing.T) {
	cases := []struct {
		name     string
		register func(*fiber.App, *ingest.ArrowBuffer)
		req      func() *http.Request
	}{
		{"line protocol", func(app *fiber.App, b *ingest.ArrowBuffer) {
			NewLineProtocolHandler(b, zerolog.Nop()).RegisterRoutes(app)
		}, func() *http.Request {
			r := httptest.NewRequest("POST", "/api/v1/write/line-protocol", strings.NewReader(testLP))
			r.Header.Set("x-arc-database", "db")
			return r
		}},
		{"lp import", func(app *fiber.App, b *ingest.ArrowBuffer) {
			h := NewImportHandler(zerolog.Nop())
			h.SetArrowBuffer(b)
			h.RegisterRoutes(app)
		}, func() *http.Request {
			body, ct := multipartFile("data.lp", testLP)
			r := httptest.NewRequest("POST", "/api/v1/import/lp?db=db", body)
			r.Header.Set("Content-Type", ct)
			return r
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Admit the entry check only: every later check reports pressure.
			buf, calls := newDiskGatedBuffer(t, 1)
			app := fiber.New(fiber.Config{DisableStartupMessage: true})
			tc.register(app, buf)
			resp, err := app.Test(tc.req(), 10000)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode >= 300 {
				t.Fatalf("status %d %s: pressure starting mid-request refused part of it", resp.StatusCode, body)
			}
			if got := buf.GetStats()["total_records_buffered"]; got != int64(2) {
				t.Fatalf("buffered %v records, want both measurements", got)
			}
			if calls.Load() < 1 {
				t.Fatal("the request was never checked")
			}
		})
	}
}

// A forwarding node copies a disk refusal back without a per-request line,
// logged by the reason the peer sent; an unexpected header value is not
// taken as a log key.
func TestForwardedDiskRefusalNotLoggedPerRequest(t *testing.T) {
	refusals, _ := captureRefusals(t)
	logs := &syncBuf{}
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(requestLogger(zerolog.New(logs)))
	reply := func(v string) fiber.Handler {
		return func(c *fiber.Ctx) error {
			h := http.Header{}
			h.Set(IngestRefusedHeader, v)
			return CopyResponse(c, &http.Response{StatusCode: http.StatusServiceUnavailable, Header: h, Body: io.NopCloser(strings.NewReader(`{}`))})
		}
	}
	app.Post("/disk", reply(ingest.RefusalWALDisk))
	app.Post("/odd", reply("Bad Value\n"))
	for _, p := range []string{"/disk", "/odd"} {
		resp, err := app.Test(httptest.NewRequest("POST", p, nil))
		if err != nil {
			t.Fatal(err)
		}
		if p == "/disk" && resp.Header.Get(IngestRefusedHeader) != ingest.RefusalWALDisk {
			t.Fatal("the reason header was not passed back to the client")
		}
	}
	if logs.count("HTTP request error") != 0 {
		t.Fatalf("a forwarded refusal was logged per request:\n%s", logs.String())
	}
	if !strings.Contains(refusals.String(), `"reason":"forwarded_wal_disk"`) || !strings.Contains(refusals.String(), `"reason":"forwarded"`) {
		t.Fatalf("refusal reasons:\n%s", refusals.String())
	}
}

func TestForwardedRefusalReason(t *testing.T) {
	for in, want := range map[string]string{
		"wal_disk":              "forwarded_wal_disk",
		"":                      "forwarded",
		"WAL":                   "forwarded",
		"a b":                   "forwarded",
		strings.Repeat("a", 33): "forwarded",
	} {
		if got := forwardedRefusalReason(in); got != want {
			t.Errorf("forwardedRefusalReason(%q) = %q, want %q", in, got, want)
		}
	}
}

// The request logger skips a refused write and still logs another 503.
func TestRequestLoggerSkipsDiskRefusals(t *testing.T) {
	_, _ = captureRefusals(t)
	logs := &syncBuf{}
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(requestLogger(zerolog.New(logs)))
	app.Post("/refused", func(c *fiber.Ctx) error { return respondWriteRefused(c, ingest.ErrWALDiskPressure) })
	app.Post("/other", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusServiceUnavailable) })
	for _, p := range []string{"/refused", "/other"} {
		if _, err := app.Test(httptest.NewRequest("POST", p, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if n := logs.count("HTTP request error"); n != 1 || !strings.Contains(logs.String(), `"path":"/other"`) {
		t.Fatalf("want only the other 503 logged:\n%s", logs.String())
	}
}

// A continuous query whose output would be refused fails before it queries
// (the aggregation can spill to the WAL volume); the caller records a failed
// run and the next schedule covers the window. The handler has no DuckDB:
// passing the check would panic.
func TestContinuousQueryRefusedBeforeItQueries(t *testing.T) {
	buf, _ := newDiskGatedBuffer(t, 0)
	h := &ContinuousQueryHandler{logger: zerolog.Nop(), arrowBuffer: buf}
	cq := &ContinuousQuery{Name: "cq", Database: "db1", SourceMeasurement: "cpu", DestinationMeasurement: "cpu_1h"}
	now := time.Now().UTC()
	_, err := h.executeAggregation(context.Background(), cq, "SELECT count(*) AS c FROM db1.cpu", now, now)
	if !errors.Is(err, ingest.ErrWALDiskPressure) || !ingest.IsWriteRefusal(err) {
		t.Fatalf("err = %v, want a wal_disk refusal", err)
	}
}

// budgetGate is a WALDiskGate with a byte budget, the contract of
// wal.DiskMonitor.Reserve: concurrent reservations see each other.
type budgetGate struct {
	mu       sync.Mutex
	budget   int64
	reserved int64
	// barrier, when set, holds every Reserve until that many have been
	// decided, so concurrent requests' admissions overlap.
	barrier int
	arrived int
	all     chan struct{}
}

func (g *budgetGate) Pressure() bool { return false }
func (g *budgetGate) Reserve(n int64) bool {
	g.mu.Lock()
	ok := g.reserved+n <= g.budget
	if ok {
		g.reserved += n
	}
	if g.barrier > 0 {
		g.arrived++
		if g.arrived == g.barrier {
			close(g.all)
		}
		all := g.all
		g.mu.Unlock()
		select {
		case <-all:
		case <-time.After(5 * time.Second):
		}
		return ok
	}
	g.mu.Unlock()
	return ok
}
func (g *budgetGate) Release(n int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reserved -= n
}

// Two concurrent large requests near the reserve: each reserves its body at
// admission, so at most one gets through when the budget holds one, both
// when it holds both, and the reservations are all returned.
func TestConcurrentLargeRequestsReserveTheirSize(t *testing.T) {
	var body strings.Builder
	for i := 0; body.Len() < 64<<10; i++ {
		fmt.Fprintf(&body, "cpu,host=h%d value=%d 1700000000000000000\n", i%50, i)
	}
	size := int64(body.Len())
	for _, tc := range []struct {
		name   string
		budget int64
		want   int
	}{
		{"budget for one", size + size/2, 1},
		{"budget for both", 2 * size, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for round := 0; round < 20; round++ {
				backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
				if err != nil {
					t.Fatal(err)
				}
				buf := ingest.NewArrowBuffer(&config.IngestConfig{
					MaxBufferSize: 1 << 20, MaxBufferAgeMS: 600000, Compression: "snappy",
					FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
				}, backend, zerolog.Nop())
				g := &budgetGate{budget: tc.budget, barrier: 2, all: make(chan struct{})}
				buf.SetWALDiskGate(g)
				app := fiber.New(fiber.Config{DisableStartupMessage: true})
				NewLineProtocolHandler(buf, zerolog.Nop()).RegisterRoutes(app)
				// The gate holds both admissions until both are decided, so
				// their reservations overlap.
				var ok, refused atomic.Int32
				var wg sync.WaitGroup
				start := make(chan struct{})
				for i := 0; i < 2; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						r := httptest.NewRequest("POST", "/api/v1/write/line-protocol", strings.NewReader(body.String()))
						r.Header.Set("x-arc-database", "db")
						resp, err := app.Test(r, 30000)
						if err != nil {
							t.Error(err)
							return
						}
						switch {
						case resp.StatusCode < 300:
							ok.Add(1)
						case resp.StatusCode == fiber.StatusServiceUnavailable && resp.Header.Get(IngestRefusedHeader) == ingest.RefusalWALDisk:
							refused.Add(1)
						default:
							t.Errorf("status %d", resp.StatusCode)
						}
					}()
				}
				close(start)
				wg.Wait()
				buf.Close()
				if tc.want == 2 && ok.Load() != 2 {
					t.Fatalf("round %d: %d admitted, want both", round, ok.Load())
				}
				if tc.want == 1 && (ok.Load() != 1 || refused.Load() != 1) {
					t.Fatalf("round %d: %d admitted, %d refused; want exactly one each", round, ok.Load(), refused.Load())
				}
				if g.reserved != 0 {
					t.Fatalf("round %d: %d bytes still reserved after both requests ended", round, g.reserved)
				}
			}
		})
	}
}

// A request bigger than the room left is refused whole, with the marked 503.
func TestLargeRequestRefusedAtAdmission(t *testing.T) {
	_, _ = captureRefusals(t)
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	buf.SetWALDiskGate(&budgetGate{budget: int64(len(testLP)) - 1})
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	NewLineProtocolHandler(buf, zerolog.Nop()).RegisterRoutes(app)
	r := httptest.NewRequest("POST", "/api/v1/write/line-protocol", strings.NewReader(testLP))
	r.Header.Set("x-arc-database", "db")
	resp, err := app.Test(r, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusServiceUnavailable || resp.Header.Get(IngestRefusedHeader) != ingest.RefusalWALDisk {
		t.Fatalf("status %d, %q", resp.StatusCode, resp.Header.Get(IngestRefusedHeader))
	}
	if got := buf.GetStats()["total_records_buffered"]; got != int64(0) {
		t.Fatalf("buffered %v records of a refused request", got)
	}
}

// The import error path answers a disk refusal (a fence-free one reaching the
// buffer) with the refusal headers and 503, not 500.
func TestImportErrorResponseForDiskRefusal(t *testing.T) {
	_, _ = captureRefusals(t)
	h := NewImportHandler(zerolog.Nop())
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Post("/x", func(c *fiber.Ctx) error {
		return h.importErrorResponse(c, &importError{StatusCode: importBufferStatus(ingest.ErrWALDiskPressure), Message: "failed to ingest CSV data", Err: ingest.ErrWALDiskPressure})
	})
	resp, err := app.Test(httptest.NewRequest("POST", "/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusServiceUnavailable || resp.Header.Get(IngestRefusedHeader) != ingest.RefusalWALDisk || resp.Header.Get("Retry-After") != "5" {
		t.Fatalf("status %d refused=%q retry=%q", resp.StatusCode, resp.Header.Get(IngestRefusedHeader), resp.Header.Get("Retry-After"))
	}
}

func newCQHandlerWithQuery(t *testing.T, buf *ingest.ArrowBuffer) (*ContinuousQueryHandler, int64) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")
	am, err := auth.NewAuthManager(dbPath, time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	h, err := NewContinuousQueryHandler(nil, nil, buf, &config.ContinuousQueryConfig{DBPath: dbPath}, am, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	res, err := h.sqliteDB.Exec(`INSERT INTO continuous_queries (name, database, source_measurement, destination_measurement, query, interval)
		VALUES ('cq', 'db1', 'cpu', 'cpu_1h', 'SELECT count(*) AS c FROM db1.cpu WHERE time >= {start_time} AND time < {end_time}', '1h')`)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return h, id
}

func cqExecutions(t *testing.T, h *ContinuousQueryHandler) int {
	t.Helper()
	var n int
	if err := h.sqliteDB.QueryRow(`SELECT count(*) FROM continuous_query_executions`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A scheduled run under disk pressure is skipped: refused (so the scheduler
// does not log an Error), no failed-execution row on the pressured volume,
// last_processed_time left for the next run.
func TestScheduledCQSkippedUnderDiskPressure(t *testing.T) {
	buf, _ := newDiskGatedBuffer(t, 0)
	h, id := newCQHandlerWithQuery(t, buf)
	_, err := h.ExecuteCQ(context.Background(), id)
	if !ingest.IsWriteRefusal(err) {
		t.Fatalf("err = %v, want a refusal", err)
	}
	if n := cqExecutions(t, h); n != 0 {
		t.Fatalf("%d execution rows written for a skipped run", n)
	}
	cq, err := h.getQuery(id)
	if err != nil {
		t.Fatal(err)
	}
	if cq.LastProcessedTime != nil {
		t.Fatal("a skipped run advanced last_processed_time")
	}
}

// A manual execute under disk pressure answers the retryable 503.
func TestManualCQExecuteUnderDiskPressureIs503(t *testing.T) {
	_, _ = captureRefusals(t)
	buf, _ := newDiskGatedBuffer(t, 0)
	h, id := newCQHandlerWithQuery(t, buf)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Post("/api/v1/continuous_queries/:id/execute", h.handleExecute) // without the admin middleware
	r := httptest.NewRequest("POST", fmt.Sprintf("/api/v1/continuous_queries/%d/execute", id), strings.NewReader(`{}`))
	r.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(r, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusServiceUnavailable || resp.Header.Get(IngestRefusedHeader) != ingest.RefusalWALDisk {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d refused=%q body %s", resp.StatusCode, resp.Header.Get(IngestRefusedHeader), b)
	}
}

// Every write path reserves its request's size at admission: with room for
// one byte, every one of them is refused before writing, so none admits a
// request at size 0.
func TestEveryWritePathReservesItsSize(t *testing.T) {
	for _, tc := range writePathCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, _ = captureRefusals(t)
			backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			buf := ingest.NewArrowBuffer(&config.IngestConfig{
				MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
				FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
			}, backend, zerolog.Nop())
			t.Cleanup(func() { buf.Close() })
			g := &budgetGate{budget: 1}
			buf.SetWALDiskGate(g)
			app := fiber.New(fiber.Config{DisableStartupMessage: true})
			tc.register(app, buf, zerolog.Nop())
			resp, err := app.Test(tc.req(), 10000)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != fiber.StatusServiceUnavailable || resp.Header.Get(IngestRefusedHeader) != ingest.RefusalWALDisk {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("status %d refused=%q body %s: admitted without reserving its size", resp.StatusCode, resp.Header.Get(IngestRefusedHeader), b)
			}
			if g.reserved != 0 {
				t.Fatalf("%d bytes left reserved", g.reserved)
			}
		})
	}
}
