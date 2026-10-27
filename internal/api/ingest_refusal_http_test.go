package api

// #1268: a write refused by the ingest gate is logged per reason, at most once
// every fenceLogEvery, and never as a per-request ERR — on the refusing node or
// on a node that forwarded the write to it.

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// syncBuf is a goroutine-safe log sink.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func (s *syncBuf) count(sub string) int { return strings.Count(s.String(), sub) }

// captureRefusals points the ingest refusal log at a buffer and a fake clock
// for the test.
func captureRefusals(t *testing.T) (*syncBuf, *time.Time) {
	t.Helper()
	buf := &syncBuf{}
	now := time.Unix(1_000_000, 0)
	t.Cleanup(ingest.CaptureRefusalLog(zerolog.New(buf), func() time.Time { return now }))
	return buf, &now
}

// fencedErr builds an error the way the cluster gate does (not_designated).
func fencedErr(t *testing.T) error {
	t.Helper()
	return fmt.Errorf("%w: %w", ingest.ErrIngestFenced, &ingest.RefusalError{Reason: ingest.RefusalNotDesignated, Text: "not the designated primary"})
}

// The refusing node's request logger skips its per-request line for a fence
// refusal and still logs any other 503.
func TestRequestLoggerSkipsFenceRefusals(t *testing.T) {
	_, _ = captureRefusals(t)
	logs := &syncBuf{}
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(requestLogger(zerolog.New(logs)))
	app.Post("/fenced", func(c *fiber.Ctx) error { return respondIngestFenced(c, fencedErr(t)) })
	app.Post("/closing", func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "shutting down"})
	})
	resp, err := app.Test(httptest.NewRequest("POST", "/fenced", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusServiceUnavailable || resp.Header.Get(IngestFencedHeader) == "" {
		t.Fatalf("status %d, %s %q", resp.StatusCode, IngestFencedHeader, resp.Header.Get(IngestFencedHeader))
	}
	if n := logs.count("HTTP request error"); n != 0 {
		t.Fatalf("a fence refusal was logged per request:\n%s", logs.String())
	}
	if _, err := app.Test(httptest.NewRequest("POST", "/closing", nil)); err != nil {
		t.Fatal(err)
	}
	if n := logs.count("HTTP request error"); n != 1 {
		t.Fatalf("another 503 must still be logged: %d lines", n)
	}
}

// A node that forwarded a write copies the primary's fence refusal back
// without logging it per request; a forwarded 503 of another kind is logged.
func TestForwardedFenceRefusalNotLoggedPerRequest(t *testing.T) {
	refusals, _ := captureRefusals(t)
	logs := &syncBuf{}
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(requestLogger(zerolog.New(logs)))
	reply := func(fenced bool) fiber.Handler {
		return func(c *fiber.Ctx) error {
			h := http.Header{}
			if fenced {
				h.Set(IngestFencedHeader, "1")
			}
			return CopyResponse(c, &http.Response{StatusCode: http.StatusServiceUnavailable, Header: h, Body: io.NopCloser(strings.NewReader(`{}`))})
		}
	}
	app.Post("/fenced", reply(true))
	app.Post("/other", reply(false))
	for _, p := range []string{"/fenced", "/other"} {
		if _, err := app.Test(httptest.NewRequest("POST", p, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if n := logs.count("HTTP request error"); n != 1 || !strings.Contains(logs.String(), `"path":"/other"`) {
		t.Fatalf("want exactly the non-fence 503 logged, got:\n%s", logs.String())
	}
	if !strings.Contains(refusals.String(), `"reason":"forwarded"`) {
		t.Fatalf("the forwarding node keeps no trace of the refusal:\n%s", refusals.String())
	}
}

// The msgpack handler answers a fenced write with the marked 503, logs no ERR
// for it and does not count it as an ingest error.
func TestMsgPackFencedWriteIsNotAnIngestError(t *testing.T) {
	refusals, _ := captureRefusals(t)
	dir := t.TempDir()
	nop := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dir, nop)
	if err != nil {
		t.Fatal(err)
	}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, nop)
	t.Cleanup(func() { buf.Close() })
	buf.SetIngestGate(func() error { return fencedErr(t) })

	handlerLogs := &syncBuf{}
	h := NewMsgPackHandler(zerolog.New(handlerLogs), buf, 10<<20)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	reqLogs := &syncBuf{}
	app.Use(requestLogger(zerolog.New(reqLogs)))
	h.RegisterRoutes(app)

	body, err := msgpack.Marshal(map[string]interface{}{
		"m":       "cpu",
		"columns": map[string]interface{}{"time": []int64{time.Now().UnixMicro()}, "value": []float64{1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	before := metrics.Get().Snapshot()["ingest_errors_total"]
	req := httptest.NewRequest("POST", "/api/v1/write/msgpack", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/msgpack")
	req.Header.Set("x-arc-database", "db")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusServiceUnavailable || resp.Header.Get(IngestFencedHeader) == "" {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d header %q body %s", resp.StatusCode, resp.Header.Get(IngestFencedHeader), b)
	}
	if strings.Contains(handlerLogs.String(), "Failed to write to Arrow buffer") {
		t.Fatalf("handler logged the refusal as an error:\n%s", handlerLogs.String())
	}
	if reqLogs.count("HTTP request error") != 0 {
		t.Fatalf("request logged per request:\n%s", reqLogs.String())
	}
	if refusals.count("Writes refused") != 1 {
		t.Fatalf("refusal not logged by reason:\n%s", refusals.String())
	}
	if after := metrics.Get().Snapshot()["ingest_errors_total"]; after != before {
		t.Fatalf("a fence refusal counted as an ingest error (%v -> %v)", before, after)
	}
}

// Every write path answers a fence refusal the same way: 503 marked as a fence
// refusal, no per-request line, no ingest error counted, the refusal logged by
// its reason. LP refuses up front (CheckIngestGate) and per measurement (a
// gate that closes after the check); both are covered.
func TestFencedWritePathsTable(t *testing.T) {
	newBuf := func(t *testing.T, admitFirst int) *ingest.ArrowBuffer {
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
		var mu sync.Mutex
		calls := 0
		buf.SetIngestGate(func() error {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if calls <= admitFirst {
				return nil
			}
			return fencedErr(t)
		})
		return buf
	}
	lp := "cpu,host=a value=1 1700000000000000000\nmem,host=a value=2 1700000000000000000\n"
	tle := "ISS (ZARYA)\n1 25544U 98067A   08264.51782528 -.00002182  00000-0 -11606-4 0  2927\n2 25544  51.6416 247.4627 0006703 130.5360 325.0288 15.72125391563537\n"
	multipartLP := func() (*bytes.Buffer, string) {
		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		fw, _ := w.CreateFormFile("file", "data.lp")
		_, _ = fw.Write([]byte(lp))
		_ = w.Close()
		return &b, w.FormDataContentType()
	}
	cases := []struct {
		name       string
		admitFirst int
		register   func(*fiber.App, *ingest.ArrowBuffer, zerolog.Logger)
		req        func() *http.Request
	}{
		{"line protocol, refused up front", 0,
			func(app *fiber.App, b *ingest.ArrowBuffer, lg zerolog.Logger) {
				NewLineProtocolHandler(b, lg).RegisterRoutes(app)
			},
			func() *http.Request {
				r := httptest.NewRequest("POST", "/api/v1/write/line-protocol", strings.NewReader(lp))
				r.Header.Set("x-arc-database", "db")
				return r
			}},
		{"line protocol, refused per measurement", 1,
			func(app *fiber.App, b *ingest.ArrowBuffer, lg zerolog.Logger) {
				NewLineProtocolHandler(b, lg).RegisterRoutes(app)
			},
			func() *http.Request {
				r := httptest.NewRequest("POST", "/api/v1/write/line-protocol", strings.NewReader(lp))
				r.Header.Set("x-arc-database", "db")
				return r
			}},
		{"tle", 0,
			func(app *fiber.App, b *ingest.ArrowBuffer, lg zerolog.Logger) {
				NewTLEHandler(b, lg).RegisterRoutes(app)
			},
			func() *http.Request {
				r := httptest.NewRequest("POST", "/api/v1/write/tle", strings.NewReader(tle))
				r.Header.Set("x-arc-database", "db")
				return r
			}},
		{"lp import, refused per measurement", 1,
			func(app *fiber.App, b *ingest.ArrowBuffer, lg zerolog.Logger) {
				h := NewImportHandler(lg)
				h.SetArrowBuffer(b)
				h.RegisterRoutes(app)
			},
			func() *http.Request {
				body, ct := multipartLP()
				r := httptest.NewRequest("POST", "/api/v1/import/lp?db=db", body)
				r.Header.Set("Content-Type", ct)
				r.Header.Set("x-arc-database", "db")
				return r
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			refusals, _ := captureRefusals(t)
			handlerLogs, reqLogs := &syncBuf{}, &syncBuf{}
			app := fiber.New(fiber.Config{DisableStartupMessage: true})
			app.Use(requestLogger(zerolog.New(reqLogs)))
			tc.register(app, newBuf(t, tc.admitFirst), zerolog.New(handlerLogs))
			before := metrics.Get().Snapshot()["ingest_errors_total"]
			resp, err := app.Test(tc.req(), 10000)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != fiber.StatusServiceUnavailable || resp.Header.Get(IngestFencedHeader) == "" || resp.Header.Get("Retry-After") == "" {
				t.Fatalf("status %d fenced=%q retry=%q body %s", resp.StatusCode, resp.Header.Get(IngestFencedHeader), resp.Header.Get("Retry-After"), body)
			}
			if strings.Contains(handlerLogs.String(), `"level":"error"`) {
				t.Fatalf("handler logged the refusal as an error:\n%s", handlerLogs.String())
			}
			if strings.Contains(reqLogs.String(), "HTTP request error") {
				t.Fatalf("logged per request:\n%s", reqLogs.String())
			}
			if !strings.Contains(refusals.String(), `"reason":"not_designated"`) {
				t.Fatalf("refusal not logged by its reason:\n%s", refusals.String())
			}
			if after := metrics.Get().Snapshot()["ingest_errors_total"]; after != before {
				t.Fatalf("counted as an ingest error (%v -> %v)", before, after)
			}
		})
	}
}
