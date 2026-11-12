package api

// Import handler benchmarks: whole multipart requests through /api/v1/import/lp and
// /api/v1/import/csv into a real buffer, so a per-request check on the import path shows up
// end to end (#1626).

import (
	"bytes"
	"fmt"
	"mime/multipart"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

func importBenchBody(format string, rows int) ([]byte, string) {
	var data strings.Builder
	ts := int64(1700000000000000000)
	if format == "csv" {
		data.WriteString("time,host,value,idle\n")
	}
	for i := 0; i < rows; i++ {
		ts += 1000
		if format == "csv" {
			fmt.Fprintf(&data, "%d,server%02d,%d.5,%d.25\n", ts, i%100, i%1000, i%97)
		} else {
			fmt.Fprintf(&data, "cpu,host=server%02d value=%d.5,idle=%d.25 %d\n", i%100, i%1000, i%97, ts)
		}
	}
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	fw, _ := w.CreateFormFile("file", "data."+format)
	_, _ = fw.Write([]byte(data.String()))
	_ = w.Close()
	return body.Bytes(), w.FormDataContentType()
}

func BenchmarkImportHandler(b *testing.B) {
	for _, format := range []string{"lp", "csv"} {
		b.Run(format, func(b *testing.B) {
			backend, err := storage.NewLocalBackend(filepath.Join(b.TempDir(), "data"), zerolog.Nop())
			if err != nil {
				b.Fatal(err)
			}
			buf := ingest.NewArrowBuffer(&config.IngestConfig{
				MaxBufferSize: 50000, MaxBufferAgeMS: 5000, Compression: "snappy",
				FlushWorkers: 4, FlushQueueSize: 64, ShardCount: 32, DataPageVersion: "2.0",
			}, backend, zerolog.Nop())
			defer buf.Close()
			app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
			h := NewImportHandler(zerolog.Nop())
			h.SetArrowBuffer(buf)
			h.RegisterRoutes(app)
			handler := app.Handler()
			const rows = 10000
			body, contentType := importBenchBody(format, rows)
			uri := "/api/v1/import/" + format
			if format == "csv" {
				uri += "?measurement=cpu"
			}
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var ctx fasthttp.RequestCtx
				ctx.Init(&fasthttp.Request{}, nil, nil)
				ctx.Request.Header.SetMethod("POST")
				ctx.Request.SetRequestURI(uri)
				ctx.Request.Header.SetContentType(contentType)
				ctx.Request.Header.Set("x-arc-database", "production")
				ctx.Request.SetBody(body)
				handler(&ctx)
				if sc := ctx.Response.StatusCode(); sc != fasthttp.StatusOK {
					b.Fatalf("status %d: %s", sc, ctx.Response.Body())
				}
			}
			b.ReportMetric(float64(b.N)*rows/b.Elapsed().Seconds(), "rows/s")
		})
	}
}
