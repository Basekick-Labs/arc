package api

import (
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/valyala/fasthttp"
)

// The cost x-arc-decimals adds to a query that does not send it (#1784).
func benchmarkRequestDecimalMode(b *testing.B, value string) {
	app := fiber.New()
	fctx := &fasthttp.RequestCtx{}
	for k, v := range map[string]string{
		"Content-Type": "application/json", "Authorization": "Bearer x", "User-Agent": "bench",
		"Accept": "*/*", "x-arc-database": "sample",
	} {
		fctx.Request.Header.Set(k, v)
	}
	if value != "" {
		fctx.Request.Header.Set(decimalsHeader, value)
	}
	c := app.AcquireCtx(fctx)
	defer app.ReleaseCtx(c)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := requestDecimalMode(c); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRequestDecimalModeNoHeader(b *testing.B) { benchmarkRequestDecimalMode(b, "") }
func BenchmarkRequestDecimalModeExact(b *testing.B)    { benchmarkRequestDecimalMode(b, decimalModeExact) }
