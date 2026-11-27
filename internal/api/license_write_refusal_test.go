package api

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// On grace days 7-14 of an expired license a live write answers 503 with
// X-Arc-Ingest-Refused: license_grace and a long Retry-After: it lasts until
// the license is renewed, so clients keep the data and back off.
func TestLicenseGraceWriteRefusal(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	buf.SetLicenseWriteGate(func() bool { return true })
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	NewLineProtocolHandler(buf, zerolog.Nop()).RegisterRoutes(app)
	req := httptest.NewRequest("POST", "/api/v1/write/line-protocol", strings.NewReader("cpu,host=a v=1 1700000000000000000\n"))
	req.Header.Set("x-arc-database", "db")
	resp, err := app.Test(req, 20000)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusServiceUnavailable || resp.Header.Get(IngestRefusedHeader) != ingest.RefusalLicenseGrace || resp.Header.Get(fiber.HeaderRetryAfter) != "300" {
		t.Fatalf("status %d, refused %q, retry-after %q", resp.StatusCode, resp.Header.Get(IngestRefusedHeader), resp.Header.Get(fiber.HeaderRetryAfter))
	}
}
