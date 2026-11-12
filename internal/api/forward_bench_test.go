package api

// Before/after benchmarks for #1629: the local ingest and query paths (which
// must not reach the forward code), the forward setup itself, and the
// forwarded write, import and query. Driven through app.Handler() on a
// fasthttp.RequestCtx built once, so a large payload is not copied by the
// test harness on every iteration. The forward setup goes through
// buildForwardForBench (forward_bench_shim_test.go), the one piece that
// differs between trees.

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

// benchRequest is a reusable request for app.Handler(); body is set raw
// (not copied).
// Init (once, outside the timed loop) gives the context fasthttp's fake
// server, which the local write path needs (RequestCtx.Done).
func benchRequest(method, uri string, body []byte, headers map[string]string) *fasthttp.RequestCtx {
	var req fasthttp.Request
	req.Header.SetMethod(method)
	req.SetRequestURI(uri)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if body != nil {
		req.SetBodyRaw(body)
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Init(&req, nil, nil)
	return ctx
}

func runBench(b *testing.B, h fasthttp.RequestHandler, ctx *fasthttp.RequestCtx, want int) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.Response.Reset()
		h(ctx)
		if got := ctx.Response.StatusCode(); got != want {
			b.Fatalf("status %d, want %d: %s", got, want, ctx.Response.Body())
		}
	}
}

// drainPeer answers status after reading the whole body.
func drainPeer(b *testing.B, status int, answer string) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if answer != "" {
			w.Header().Set("Content-Type", "application/json")
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, answer)
	}))
	b.Cleanup(srv.Close)
	return srv
}

// benchRouter is a router on a node of role local whose only peer, of role
// peerRole, is peerURL.
func benchRouter(b *testing.B, local cluster.NodeRole, peerRole cluster.NodeRole, peerURL string) *cluster.Router {
	b.Helper()
	localNode := cluster.NewNode("local", "Local", local, "bench")
	registry := cluster.NewRegistry(&cluster.RegistryConfig{LocalNode: localNode, Logger: zerolog.Nop()})
	peer := cluster.NewNode("peer", "Peer", peerRole, "bench")
	peer.UpdateState(cluster.StateHealthy)
	peer.APIAddress = strings.TrimPrefix(peerURL, "http://")
	if err := registry.Register(peer); err != nil {
		b.Fatal(err)
	}
	return cluster.NewRouter(&cluster.RouterConfig{
		Timeout:         30 * time.Second,
		Registry:        registry,
		LocalNode:       localNode,
		Logger:          zerolog.Nop(),
		WriteTimeout:    5 * time.Minute,
		ImportTimeout:   5 * time.Minute,
		PeerReadQueries: func(string) bool { return true },
	})
}

func benchBuffer(b *testing.B) *ingest.ArrowBuffer {
	b.Helper()
	backend, err := storage.NewLocalBackend(b.TempDir(), zerolog.Nop())
	if err != nil {
		b.Fatal(err)
	}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 2, FlushQueueSize: 64, ShardCount: 4, DataPageVersion: "2.0",
	}, backend, zerolog.Nop())
	b.Cleanup(func() { buf.Close() })
	return buf
}

func benchQueryHandler(b *testing.B) *QueryHandler {
	b.Helper()
	dir := b.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		b.Fatal(err)
	}
	duck, err := database.New(&database.Config{
		MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: filepath.Join(dir),
	}, zerolog.Nop())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { duck.Close() })
	return NewQueryHandler(duck, backend, zerolog.Nop(), 0, 0)
}

func lpPayload(lines int) []byte {
	var sb strings.Builder
	base := int64(1700000000000000000)
	for i := 0; i < lines; i++ {
		fmt.Fprintf(&sb, "cpu,host=h%d,region=r%d usage=%d.5,idle=%di %d\n", i%100, i%7, i%97, i%13, base+int64(i)*1000)
	}
	return []byte(sb.String())
}

// BenchmarkLocalWrite: a standalone node (no router) ingesting 10k lines of
// line protocol. The forward code must not be reached.
func BenchmarkLocalWrite(b *testing.B) {
	metrics.Init(zerolog.Nop())
	h := NewLineProtocolHandler(benchBuffer(b), zerolog.Nop())
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)
	body := lpPayload(10000)
	b.SetBytes(int64(len(body)))
	ctx := benchRequest("POST", "/api/v1/write/line-protocol", body, map[string]string{"x-arc-database": "bench"})
	runBench(b, app.Handler(), ctx, http.StatusNoContent)
}

// BenchmarkLocalQuery: a standalone node (no router) answering a query.
func BenchmarkLocalQuery(b *testing.B) {
	metrics.Init(zerolog.Nop())
	qh := benchQueryHandler(b)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	qh.RegisterRoutes(app)
	ctx := benchRequest("POST", "/api/v1/query", []byte(`{"sql":"SELECT 42 AS x"}`), map[string]string{"Content-Type": "application/json"})
	runBench(b, app.Handler(), ctx, http.StatusOK)
}

// BenchmarkBuildForwardRequest: the forward setup alone (context and
// request), as a forwarded query builds it.
func BenchmarkBuildForwardRequest(b *testing.B) {
	srv := drainPeer(b, http.StatusOK, "")
	router := benchRouter(b, cluster.RoleCompactor, cluster.RoleReader, srv.URL)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	rctx := benchRequest("POST", "/api/v1/query", []byte(`{"sql":"SELECT 42"}`), map[string]string{
		"Content-Type": "application/json", "Authorization": "Bearer x", "Accept": "application/json",
	})
	c := app.AcquireCtx(rctx)
	defer app.ReleaseCtx(c)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, done, err := buildForwardForBench(c, router)
		if err != nil || req == nil {
			b.Fatal(err)
		}
		done()
	}
}

// BenchmarkForwardWrite: a reader forwarding a line-protocol write to the
// writer, the body streamed.
func BenchmarkForwardWrite(b *testing.B) {
	metrics.Init(zerolog.Nop())
	for _, size := range []struct {
		name string
		n    int
	}{{"1KB", 1 << 10}, {"4KB", 4 << 10}, {"8KB", 8 << 10}, {"64KB", 64 << 10}, {"1MB", 1 << 20}, {"64MB", 64 << 20}} {
		b.Run(size.name, func(b *testing.B) {
			srv := drainPeer(b, http.StatusNoContent, "")
			h := NewLineProtocolHandler(benchBuffer(b), zerolog.Nop())
			h.SetRouter(benchRouter(b, cluster.RoleReader, cluster.RoleWriter, srv.URL))
			app := fiber.New(fiber.Config{DisableStartupMessage: true})
			h.RegisterRoutes(app)
			body := make([]byte, size.n)
			for i := range body {
				body[i] = 'a' + byte(i%26)
			}
			b.SetBytes(int64(size.n))
			ctx := benchRequest("POST", "/api/v1/write/line-protocol", body, map[string]string{"x-arc-database": "bench"})
			runBench(b, app.Handler(), ctx, http.StatusNoContent)
		})
	}
}

// BenchmarkForwardImport: a reader forwarding an import (the data-migration
// API) to the writer. 448MB stays under maxForwardedImportBody (500MB + 1MB).
func BenchmarkForwardImport(b *testing.B) {
	metrics.Init(zerolog.Nop())
	for _, size := range []struct {
		name string
		n    int
	}{{"64MB", 64 << 20}, {"448MB", 448 << 20}} {
		b.Run(size.name, func(b *testing.B) {
			srv := drainPeer(b, http.StatusOK, `{"status":"ok"}`)
			h := NewImportHandler(zerolog.Nop())
			h.SetRouter(benchRouter(b, cluster.RoleReader, cluster.RoleWriter, srv.URL))
			app := fiber.New(fiber.Config{DisableStartupMessage: true})
			h.RegisterRoutes(app)
			body := make([]byte, size.n)
			for i := range body {
				body[i] = 'a' + byte(i%26)
			}
			b.SetBytes(int64(size.n))
			ctx := benchRequest("POST", "/api/v1/import/lp", body, map[string]string{
				"Content-Type": "multipart/form-data; boundary=x", "x-arc-database": "bench",
			})
			runBench(b, app.Handler(), ctx, http.StatusOK)
		})
	}
}

// BenchmarkForwardQuery: a compactor forwarding /api/v1/query to a reader.
func BenchmarkForwardQuery(b *testing.B) {
	metrics.Init(zerolog.Nop())
	srv := drainPeer(b, http.StatusOK, `{"success":true,"columns":["x"],"data":[[42]],"row_count":1}`)
	qh := benchQueryHandler(b)
	qh.SetRouter(benchRouter(b, cluster.RoleCompactor, cluster.RoleReader, srv.URL))
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	qh.RegisterRoutes(app)
	ctx := benchRequest("POST", "/api/v1/query", []byte(`{"sql":"SELECT 42 AS x"}`), map[string]string{"Content-Type": "application/json"})
	runBench(b, app.Handler(), ctx, http.StatusOK)
}

// runParallelBench is runBench from every goroutine, each with its own
// request (a fasthttp.RequestCtx serves one request at a time).
func runParallelBench(b *testing.B, h fasthttp.RequestHandler, newCtx func() *fasthttp.RequestCtx, want int) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := newCtx()
		for pb.Next() {
			ctx.Response.Reset()
			h(ctx)
			if got := ctx.Response.StatusCode(); got != want {
				b.Errorf("status %d, want %d", got, want)
				return
			}
		}
	})
}

// BenchmarkForwardWriteParallel: concurrent forwarded writes, every one
// rooted on the same router (its lifetime context and transport).
func BenchmarkForwardWriteParallel(b *testing.B) {
	metrics.Init(zerolog.Nop())
	for _, size := range []struct {
		name string
		n    int
	}{{"1KB", 1 << 10}, {"64KB", 64 << 10}} {
		b.Run(size.name, func(b *testing.B) {
			srv := drainPeer(b, http.StatusNoContent, "")
			h := NewLineProtocolHandler(benchBuffer(b), zerolog.Nop())
			h.SetRouter(benchRouter(b, cluster.RoleReader, cluster.RoleWriter, srv.URL))
			app := fiber.New(fiber.Config{DisableStartupMessage: true})
			h.RegisterRoutes(app)
			body := make([]byte, size.n)
			for i := range body {
				body[i] = 'a' + byte(i%26)
			}
			b.SetBytes(int64(size.n))
			runParallelBench(b, app.Handler(), func() *fasthttp.RequestCtx {
				return benchRequest("POST", "/api/v1/write/line-protocol", body, map[string]string{"x-arc-database": "bench"})
			}, http.StatusNoContent)
		})
	}
}

// BenchmarkForwardQueryParallel: concurrent forwarded queries, each with its
// own child of the router's lifetime.
func BenchmarkForwardQueryParallel(b *testing.B) {
	metrics.Init(zerolog.Nop())
	srv := drainPeer(b, http.StatusOK, `{"success":true,"columns":["x"],"data":[[42]],"row_count":1}`)
	qh := benchQueryHandler(b)
	qh.SetRouter(benchRouter(b, cluster.RoleCompactor, cluster.RoleReader, srv.URL))
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	qh.RegisterRoutes(app)
	runParallelBench(b, app.Handler(), func() *fasthttp.RequestCtx {
		return benchRequest("POST", "/api/v1/query", []byte(`{"sql":"SELECT 42 AS x"}`), map[string]string{"Content-Type": "application/json"})
	}, http.StatusOK)
}
