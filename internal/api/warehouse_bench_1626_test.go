package api

// The writers' warehouse-name check with the feature on and off, in one binary (#1626): a line
// protocol write and a line protocol import, each with no warehouse name set (Iceberg off or the
// warehouse at the root) and with a subdirectory warehouse configured. The written database is
// not the warehouse, so both arms do the same work apart from the check.

import (
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

func BenchmarkWriteWarehouseName(b *testing.B) {
	for _, path := range []string{"lp", "import-lp"} {
		for _, name := range []string{"unset", "set"} {
			b.Run(path+"/warehouse="+name, func(b *testing.B) {
				if name == "set" {
					storage.SetWarehouseRootName("lv_wh")
					defer storage.SetWarehouseRootName("")
				}
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
				var bodies [][]byte
				var uri, contentType string
				want := fasthttp.StatusNoContent
				if path == "lp" {
					NewLineProtocolHandler(buf, zerolog.Nop()).RegisterRoutes(app)
					bodies, uri = lpBenchBodies("iot", 64), "/api/v1/write/line-protocol"
				} else {
					h := NewImportHandler(zerolog.Nop())
					h.SetArrowBuffer(buf)
					h.RegisterRoutes(app)
					body, ct := importBenchBody("lp", 1000)
					bodies, uri, contentType, want = [][]byte{body}, "/api/v1/import/lp", ct, fasthttp.StatusOK
				}
				handler := app.Handler()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var ctx fasthttp.RequestCtx
					ctx.Init(&fasthttp.Request{}, nil, nil)
					ctx.Request.Header.SetMethod("POST")
					ctx.Request.SetRequestURI(uri)
					if contentType != "" {
						ctx.Request.Header.SetContentType(contentType)
					}
					ctx.Request.Header.Set("x-arc-database", "production")
					ctx.Request.SetBody(bodies[i%len(bodies)])
					handler(&ctx)
					if sc := ctx.Response.StatusCode(); sc != want {
						b.Fatalf("status %d: %s", sc, ctx.Response.Body())
					}
				}
			})
		}
	}
}
