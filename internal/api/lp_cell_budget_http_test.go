package api

import (
	"bytes"
	"fmt"
	"mime/multipart"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// A line protocol write whose points each bring a new key is answered 413
// (GHSA-gc2v-hfj6-mfpx).
func TestLineProtocolUniqueKeysAnswer413(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	NewLineProtocolHandler(buf, zerolog.Nop()).RegisterRoutes(app)
	var body strings.Builder
	for i := 0; i < 8000; i++ {
		fmt.Fprintf(&body, "m,k%d=x value=1 %d\n", i, 1700000000000000000+int64(i))
	}
	req := httptest.NewRequest("POST", "/api/v1/write/line-protocol", strings.NewReader(body.String()))
	req.Header.Set("x-arc-database", "db")
	resp, err := app.Test(req, 20000)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", resp.StatusCode)
	}
}

// A line protocol import of the same shape answers 413 too.
func TestLineProtocolImportUniqueKeysAnswer413(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h := NewImportHandler(zerolog.Nop())
	h.SetArrowBuffer(buf)
	h.RegisterRoutes(app)
	var lp strings.Builder
	for i := 0; i < 8000; i++ {
		fmt.Fprintf(&lp, "m,k%d=x value=1 %d\n", i, 1700000000000000000+int64(i))
	}
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	fw, _ := w.CreateFormFile("file", "data.lp")
	_, _ = fw.Write([]byte(lp.String()))
	_ = w.Close()
	req := httptest.NewRequest("POST", "/api/v1/import/lp?db=db", &body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("x-arc-database", "db")
	resp, err := app.Test(req, 30000)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", resp.StatusCode)
	}
}
