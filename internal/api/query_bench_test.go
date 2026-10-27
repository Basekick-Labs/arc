//go:build duckdb_arrow

package api

// Query benchmarks: whole requests through the JSON, MessagePack and Arrow
// IPC handlers over ~2M rows of local Parquet, with the response body read
// to the end (streamed bodies encode as they are read).

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

const queryBenchRows = 2_000_000

var (
	queryBenchOnce sync.Once
	queryBenchURL  string
	queryBenchErr  error
	queryBenchRoot string
)

// TestMain removes the benchmark's data directory (shared by every
// BenchmarkQuery run in the process) once the run ends.
func TestMain(m *testing.M) {
	code := m.Run()
	if queryBenchRoot != "" {
		_ = os.RemoveAll(queryBenchRoot)
	}
	os.Exit(code)
}

// queryBenchSetup starts the handlers on a loopback listener, as a client
// sees them (streamed bodies go to a socket), and returns the base URL.
func queryBenchSetup(b *testing.B) string {
	b.Helper()
	queryBenchOnce.Do(func() {
		root, err := os.MkdirTemp("", "arc-query-bench-")
		if err != nil {
			queryBenchErr = err
			return
		}
		queryBenchRoot = root
		dataDir := filepath.Join(root, "data")
		_ = os.MkdirAll(dataDir, 0o755)
		logger := zerolog.Nop()
		backend, err := storage.NewLocalBackend(dataDir, logger)
		if err != nil {
			queryBenchErr = err
			return
		}
		buf := ingest.NewArrowBuffer(&config.IngestConfig{
			MaxBufferSize: 500_000, MaxBufferAgeMS: 600000, Compression: "snappy",
			FlushWorkers: 4, FlushQueueSize: 16, ShardCount: 4, DataPageVersion: "2.0",
		}, backend, logger)
		rng := rand.New(rand.NewSource(1))
		base := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC).UnixMicro()
		const batch = 100_000
		for off := 0; off < queryBenchRows; off += batch {
			times := make([]int64, batch)
			hosts := make([]string, batch)
			v1, v2, v3 := make([]float64, batch), make([]float64, batch), make([]float64, batch)
			n := make([]int64, batch)
			for i := 0; i < batch; i++ {
				times[i] = base + int64(off+i)*3_000 // ~1.7 h of data
				hosts[i] = fmt.Sprintf("server%02d", rng.Intn(100))
				v1[i], v2[i], v3[i] = rng.Float64()*100, rng.Float64()*100, rng.Float64()*100
				n[i] = int64(rng.Intn(1 << 20))
			}
			tb := &ingest.TypedColumnBatch{
				Data:       map[string]interface{}{"time": times, "host": hosts, "usage_idle": v1, "usage_user": v2, "usage_system": v3, "n": n},
				TagColumns: []string{"host"},
			}
			if err := buf.WriteTypedColumnarDirect(context.Background(), "bench", "cpu", tb, batch); err != nil {
				queryBenchErr = err
				return
			}
		}
		if err := buf.FlushAllAndWait(context.Background()); err != nil {
			queryBenchErr = err
			return
		}
		buf.Close()
		duck, err := database.New(&database.Config{
			MemoryLimit: "4GB", ThreadCount: 8, MaxConnections: 8, LocalStorageRoot: dataDir,
		}, logger)
		if err != nil {
			queryBenchErr = err
			return
		}
		h := NewQueryHandler(duck, backend, logger, 0, 0)
		app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
		h.RegisterRoutes(app)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			queryBenchErr = err
			return
		}
		go func() { _ = app.Listener(ln) }()
		queryBenchURL = "http://" + ln.Addr().String()
	})
	if queryBenchErr != nil {
		b.Fatal(queryBenchErr)
	}
	return queryBenchURL
}

func BenchmarkQuery(b *testing.B) {
	base := queryBenchSetup(b)
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 4, DisableCompression: true}}
	queries := []struct{ name, sql string }{
		{"count", "SELECT COUNT(*) FROM bench.cpu"},
		{"limit10k", "SELECT * FROM bench.cpu LIMIT 10000"},
		{"limit100k", "SELECT * FROM bench.cpu LIMIT 100000"},
		{"limit1m", "SELECT * FROM bench.cpu LIMIT 1000000"},
		{"groupby_host", "SELECT host, AVG(usage_idle), MAX(n) FROM bench.cpu GROUP BY host"},
	}
	// encoding is the Accept-Encoding header: the JSON and msgpack endpoints
	// compress the stream (Arrow compresses inside IPC instead).
	endpoints := []struct{ name, path, encoding string }{
		{"json", "/api/v1/query", ""},
		{"json+gzip", "/api/v1/query", "gzip"},
		{"json+zstd", "/api/v1/query", "zstd"},
		{"msgpack", "/api/v1/query/msgpack", ""},
		{"msgpack+zstd", "/api/v1/query/msgpack", "zstd"},
		{"arrow", "/api/v1/query/arrow", ""},
	}
	for _, q := range queries {
		for _, ep := range endpoints {
			b.Run(q.name+"/"+ep.name, func(b *testing.B) {
				body := []byte(fmt.Sprintf(`{"sql": %q}`, q.sql))
				var bytesOut int64
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					req, err := http.NewRequest(http.MethodPost, base+ep.path, bytes.NewReader(body))
					if err != nil {
						b.Fatal(err)
					}
					req.Header.Set("Content-Type", "application/json")
					if ep.encoding != "" {
						req.Header.Set("Accept-Encoding", ep.encoding)
					}
					resp, err := client.Do(req)
					if err != nil {
						b.Fatal(err)
					}
					n, err := io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					if err != nil || resp.StatusCode != 200 {
						b.Fatalf("status %d err %v", resp.StatusCode, err)
					}
					bytesOut = n
				}
				b.ReportMetric(float64(bytesOut), "resp_bytes")
			})
		}
	}
}

// BenchmarkQueryConcurrent runs many queries at once (b.RunParallel, one per
// GOMAXPROCS by default): the per-request latency of a loaded server, where
// parallel JSON encoding must not take cores from concurrent queries.
func BenchmarkQueryConcurrent(b *testing.B) {
	base := queryBenchSetup(b)
	for _, q := range []struct{ name, sql string }{
		{"count", "SELECT COUNT(*) FROM bench.cpu"},
		{"limit10k", "SELECT * FROM bench.cpu LIMIT 10000"},
		{"limit100k", "SELECT * FROM bench.cpu LIMIT 100000"},
	} {
		b.Run(q.name+"/json", func(b *testing.B) {
			body := []byte(fmt.Sprintf(`{"sql": %q}`, q.sql))
			client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64, DisableCompression: true}}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					resp, err := client.Post(base+"/api/v1/query", "application/json", bytes.NewReader(body))
					if err != nil {
						b.Error(err)
						return
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			})
		})
	}
}
