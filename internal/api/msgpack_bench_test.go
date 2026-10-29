package api

// MessagePack write benchmarks: whole requests through the handler into a
// real buffer, in the payload shapes clients send (1000 rows per request),
// with the WAL off and on.

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/gofiber/fiber/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

func msgpackBenchColumnar(rng *rand.Rand, m string, n int, ts *int64, mixed bool) map[string]interface{} {
	times := make([]int64, n)
	hosts := make([]string, n)
	v1, v2, v3 := make([]float64, n), make([]float64, n), make([]float64, n)
	for j := 0; j < n; j++ {
		*ts += 1
		times[j] = *ts
		hosts[j] = fmt.Sprintf("server%02d", rng.Intn(100))
		v1[j], v2[j], v3[j] = rng.Float64()*100, rng.Float64()*100, rng.Float64()*100
	}
	cols := map[string]interface{}{"time": times, "host": hosts, "value": v1, "cpu_idle": v2, "cpu_user": v3}
	p := map[string]interface{}{"m": m, "columns": cols}
	if mixed {
		ints := make([]interface{}, n)
		strs := make([]string, n)
		bools := make([]bool, n)
		for j := 0; j < n; j++ {
			if j%10 != 0 {
				ints[j] = int64(rng.Intn(1000))
			}
			strs[j] = fmt.Sprintf("st%d", rng.Intn(10))
			bools[j] = rng.Intn(2) == 0
		}
		cols["n"], cols["status"], cols["ok"] = ints, strs, bools
		cols["dc"] = hosts
		p["tag_keys"] = []string{"host", "dc"}
	}
	return p
}

func msgpackBenchBodies(shape string, count int) [][]byte {
	rng := rand.New(rand.NewSource(1))
	ts := int64(1700000000000000)
	enc, _ := zstd.NewWriter(nil)
	bodies := make([][]byte, count)
	for i := range bodies {
		var payload interface{}
		switch shape {
		case "columnar", "columnar_zstd":
			payload = msgpackBenchColumnar(rng, "cpu", 1000, &ts, false)
		case "columnar_overlap":
			// sustained_bench: each worker's request starts at its own "now",
			// so concurrent requests' times overlap and the flush must sort.
			start := int64(1700000000000000) + int64(i%12)*7
			payload = msgpackBenchColumnar(rng, "cpu", 1000, &start, false)
		case "columnar_mixed":
			payload = msgpackBenchColumnar(rng, "cpu", 1000, &ts, true)
		case "batch":
			payload = map[string]interface{}{"batch": []interface{}{
				msgpackBenchColumnar(rng, "cpu", 334, &ts, false),
				msgpackBenchColumnar(rng, "mem", 333, &ts, false),
				msgpackBenchColumnar(rng, "disk", 333, &ts, false),
			}}
		case "telegraf":
			// Telegraf's Arc output: a top-level array of columnar maps, one
			// per measurement, []interface{} columns, ms time, nil where a
			// metric lacks a field.
			var msgs []interface{}
			for k, m := range []string{"cpu", "mem", "disk", "net", "system"} {
				n := 200
				cols := map[string][]interface{}{}
				for j := 0; j < n; j++ {
					ts++
					cols["time"] = append(cols["time"], ts/1000)
					cols["host"] = append(cols["host"], fmt.Sprintf("server%02d", rng.Intn(20)))
					for f := 0; f < 4+k; f++ {
						name := fmt.Sprintf("f%d", f)
						if f == 1 && j%7 == 0 {
							cols[name] = append(cols[name], nil)
						} else if f%2 == 0 {
							cols[name] = append(cols[name], rng.Float64()*100)
						} else {
							cols[name] = append(cols[name], int64(rng.Intn(1<<20)))
						}
					}
				}
				msgs = append(msgs, map[string]interface{}{"m": m, "columns": cols})
			}
			payload = msgs
		case "rows":
			rows := make([]interface{}, 1000)
			for j := range rows {
				ts++
				rows[j] = map[string]interface{}{
					"m": "cpu", "t": ts / 1000,
					"tags":   map[string]interface{}{"host": fmt.Sprintf("server%02d", rng.Intn(100)), "region": "us-east"},
					"fields": map[string]interface{}{"value": rng.Float64() * 100, "cpu_idle": rng.Float64() * 100, "cpu_user": rng.Float64() * 100},
				}
			}
			payload = rows
		}
		b, err := msgpack.Marshal(payload)
		if err != nil {
			panic(err)
		}
		if shape == "columnar_zstd" {
			b = enc.EncodeAll(b, nil)
		}
		bodies[i] = b
	}
	return bodies
}

func BenchmarkMsgPackWrite(b *testing.B) {
	for _, withWAL := range []bool{false, true} {
		for _, shape := range []string{"columnar", "columnar_overlap", "columnar_mixed", "columnar_zstd", "batch", "telegraf", "rows"} {
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
				NewMsgPackHandler(zerolog.Nop(), buf, 100<<20).RegisterRoutes(app)
				handler := app.Handler()
				bodies := msgpackBenchBodies(shape, 64)
				b.SetBytes(int64(len(bodies[0])))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					var ctx fasthttp.RequestCtx
					ctx.Init(&fasthttp.Request{}, nil, nil)
					i := 0
					for pb.Next() {
						ctx.Request.Reset()
						ctx.Response.Reset()
						ctx.Request.Header.SetMethod("POST")
						ctx.Request.SetRequestURI("/api/v1/write/msgpack")
						ctx.Request.Header.Set("x-arc-database", "production")
						ctx.Request.Header.SetContentType("application/msgpack")
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
