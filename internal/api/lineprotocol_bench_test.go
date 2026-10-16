package api

// Line-protocol write benchmarks: whole requests through the handler into a
// real buffer, in the shapes arc-benchmarks' sustained_bench sends (1000
// lines per request), with the WAL off and on.

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

func lpBenchBodies(shape string, n int) [][]byte {
	rng := rand.New(rand.NewSource(1))
	bodies := make([][]byte, n)
	ts := int64(1700000000000000000)
	for i := range bodies {
		var b strings.Builder
		for j := 0; j < 1000; j++ {
			ts += 1000
			switch shape {
			case "iot":
				fmt.Fprintf(&b, "cpu,host=server%02d value=%.4f,cpu_idle=%.4f,cpu_user=%.4f %d\n",
					rng.Intn(100), rng.Float64()*100, rng.Float64()*100, rng.Float64()*100, ts)
			case "financial":
				p := 10 + rng.Float64()*490
				fmt.Fprintf(&b, "trades,symbol=S%03d,exchange=X%d price=%.4f,bid=%.4f,ask=%.4f,bid_size=%di,ask_size=%di,volume=%di,trade_id=%di %d\n",
					rng.Intn(500), rng.Intn(5), p, p-0.02, p+0.02, 100+rng.Intn(9900), 100+rng.Intn(9900), 1+rng.Intn(999), 1000000+rng.Intn(8999999), ts)
			case "mixed":
				fmt.Fprintf(&b, "m%d,host=h%02d,dc=d%d v=%.3f,n=%di,s=\"st%d\",ok=%t %d\n",
					j%3, rng.Intn(50), rng.Intn(3), rng.Float64(), rng.Intn(1000), rng.Intn(10), rng.Intn(2) == 0, ts)
			}
		}
		bodies[i] = []byte(b.String())
	}
	return bodies
}

func BenchmarkLPWrite(b *testing.B) {
	for _, withWAL := range []bool{false, true} {
		for _, shape := range []string{"iot", "financial", "mixed"} {
			b.Run(fmt.Sprintf("wal=%t/%s", withWAL, shape), func(b *testing.B) {
				dir := b.TempDir()
				backend, err := storage.NewLocalBackend(filepath.Join(dir, "data"), zerolog.Nop())
				if err != nil {
					b.Fatal(err)
				}
				buf := ingest.NewArrowBuffer(&config.IngestConfig{
					MaxBufferSize: 50000, MaxBufferAgeMS: 5000, Compression: "snappy",
					FlushWorkers: 4, FlushQueueSize: 64, ShardCount: 32, DataPageVersion: "2.0",
				}, backend, zerolog.Nop())
				defer buf.Close()
				if withWAL {
					w, err := wal.NewWriter(&wal.WriterConfig{WALDir: filepath.Join(dir, "wal"), SyncMode: wal.SyncModeAsync, Logger: zerolog.Nop()})
					if err != nil {
						b.Fatal(err)
					}
					defer w.Close()
					buf.SetWAL(w)
				}
				app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
				NewLineProtocolHandler(buf, zerolog.Nop()).RegisterRoutes(app)
				handler := app.Handler()
				bodies := lpBenchBodies(shape, 64)
				b.SetBytes(int64(len(bodies[0])))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					var ctx fasthttp.RequestCtx
					ctx.Init(&fasthttp.Request{}, nil, nil) // a server for ctx.Err()
					i := 0
					for pb.Next() {
						ctx.Request.Reset()
						ctx.Response.Reset()
						ctx.Request.Header.SetMethod("POST")
						ctx.Request.SetRequestURI("/api/v1/write/line-protocol")
						ctx.Request.Header.Set("x-arc-database", "production")
						ctx.Request.SetBody(bodies[i%len(bodies)])
						i++
						handler(&ctx)
						if sc := ctx.Response.StatusCode(); sc != fasthttp.StatusNoContent {
							b.Fatalf("status %d: %s", sc, ctx.Response.Body())
						}
					}
				})
				b.ReportMetric(float64(b.N)*1000/b.Elapsed().Seconds(), "rows/s")
			})
		}
	}
}
