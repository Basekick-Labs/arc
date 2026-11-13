package api

// Prometheus Remote Write benchmarks: whole requests through the handler into
// a real buffer, in the shape Prometheus sends a node_exporter scrape (2000
// samples per request, its default max_samples_per_send), against the same
// samples sent as line protocol.

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/golang/snappy"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
	"google.golang.org/protobuf/encoding/protowire"
)

const promBenchSamples = 2000

type promBenchSeries struct {
	labels [][2]string // sorted by name, __name__ included
	value  float64
	ts     int64
}

// promBenchBatches builds node_exporter-shaped batches: 50 metric names, each
// with instance/job and one to three of cpu, mode, device labels, one sample
// per series per batch.
func promBenchBatches(n int) [][]promBenchSeries {
	rng := rand.New(rand.NewSource(1))
	extra := [][]string{{"cpu", "mode"}, {"device"}, {"cpu"}, {"device", "fstype", "mountpoint"}}
	batches := make([][]promBenchSeries, n)
	ts := int64(1700000000000)
	for i := range batches {
		ts += 15000
		batch := make([]promBenchSeries, 0, promBenchSamples)
		for j := 0; j < promBenchSamples; j++ {
			metric := j % 50
			labels := [][2]string{
				{"__name__", fmt.Sprintf("node_metric_%02d_total", metric)},
				{"instance", fmt.Sprintf("host-%02d:9100", j%20)},
				{"job", "node"},
			}
			for _, name := range extra[metric%len(extra)] {
				labels = append(labels, [2]string{name, fmt.Sprintf("%s%d", name, j%8)})
			}
			sort.Slice(labels, func(a, b int) bool { return labels[a][0] < labels[b][0] })
			batch = append(batch, promBenchSeries{labels: labels, value: rng.Float64() * 1e6, ts: ts})
		}
		batches[i] = batch
	}
	return batches
}

func promBenchRemoteWrite(batch []promBenchSeries) []byte {
	var req []byte
	for _, s := range batch {
		var ts []byte
		for _, l := range s.labels {
			var lb []byte
			lb = protowire.AppendTag(lb, 1, protowire.BytesType)
			lb = protowire.AppendString(lb, l[0])
			lb = protowire.AppendTag(lb, 2, protowire.BytesType)
			lb = protowire.AppendString(lb, l[1])
			ts = protowire.AppendTag(ts, 1, protowire.BytesType)
			ts = protowire.AppendBytes(ts, lb)
		}
		var sb []byte
		sb = protowire.AppendTag(sb, 1, protowire.Fixed64Type)
		sb = protowire.AppendFixed64(sb, math.Float64bits(s.value))
		sb = protowire.AppendTag(sb, 2, protowire.VarintType)
		sb = protowire.AppendVarint(sb, uint64(s.ts))
		ts = protowire.AppendTag(ts, 2, protowire.BytesType)
		ts = protowire.AppendBytes(ts, sb)
		req = protowire.AppendTag(req, 1, protowire.BytesType)
		req = protowire.AppendBytes(req, ts)
	}
	return snappy.Encode(nil, req)
}

func promBenchLineProtocol(batch []promBenchSeries) []byte {
	var b strings.Builder
	for _, s := range batch {
		name := ""
		var tags []string
		for _, l := range s.labels {
			if l[0] == "__name__" {
				name = l[1]
				continue
			}
			tags = append(tags, l[0]+"="+strings.ReplaceAll(l[1], ":", "\\:"))
		}
		fmt.Fprintf(&b, "%s,%s value=%g %d\n", name, strings.Join(tags, ","), s.value, s.ts)
	}
	return []byte(b.String())
}

func BenchmarkPrometheusRemoteWriteVsLP(b *testing.B) {
	batches := promBenchBatches(32)
	for _, format := range []string{"remote_write", "line_protocol"} {
		b.Run(format, func(b *testing.B) {
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
			app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
			NewLineProtocolHandler(buf, zerolog.Nop()).RegisterRoutes(app)
			handler := app.Handler()
			bodies := make([][]byte, len(batches))
			for i, batch := range batches {
				if format == "remote_write" {
					bodies[i] = promBenchRemoteWrite(batch)
				} else {
					bodies[i] = promBenchLineProtocol(batch)
				}
			}
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
					if format == "remote_write" {
						ctx.Request.SetRequestURI(prometheusRemoteWritePath + "?db=production")
						ctx.Request.Header.Set("Content-Encoding", "snappy")
						ctx.Request.Header.Set("Content-Type", "application/x-protobuf")
						ctx.Request.Header.Set("X-Prometheus-Remote-Write-Version", prometheusRemoteWriteVersion)
						ctx.Request.Header.Set("User-Agent", "Prometheus/3.7.3")
					} else {
						ctx.Request.SetRequestURI("/api/v1/write/line-protocol?precision=ms")
						ctx.Request.Header.Set("x-arc-database", "production")
					}
					ctx.Request.SetBody(bodies[i%len(bodies)])
					i++
					handler(&ctx)
					if sc := ctx.Response.StatusCode(); sc != fasthttp.StatusNoContent {
						b.Fatalf("status %d: %s", sc, ctx.Response.Body())
					}
				}
			})
			b.ReportMetric(float64(b.N)*promBenchSamples/b.Elapsed().Seconds(), "samples/s")
			b.ReportMetric(float64(b.N)*promBenchSamples, "samples")
		})
	}
}
