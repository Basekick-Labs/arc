package api

// #1207 S2: a write refused by ingest fencing is a retryable 503 with
// Retry-After, not a 500.

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/gofiber/fiber/v2"
)

func TestFencedWriteIsARetryable503(t *testing.T) {
	app := fiber.New()
	app.Post("/w", func(c *fiber.Ctx) error {
		return respondIngestFenced(c, fmt.Errorf("%w: test", ingest.ErrIngestFenced))
	})
	resp, err := app.Test(httptest.NewRequest("POST", "/w", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("status %d, Retry-After %q; want 503 with Retry-After", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if got := importBufferStatus(fmt.Errorf("%w: test", ingest.ErrIngestFenced)); got != fiber.StatusServiceUnavailable {
		t.Fatalf("import maps a fenced write to %d, want 503", got)
	}
}
