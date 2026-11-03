package api

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// A compressed body is refused before anything reads it, except on the write
// endpoints, which decode it themselves, bounded.
func TestEncodedRequestBodies(t *testing.T) {
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	app.Use(rejectEncodedBodies)
	read := 0
	h := func(c *fiber.Ctx) error {
		read += len(c.Body())
		return c.SendStatus(fiber.StatusNoContent)
	}
	app.Post("/api/v1/query", h)
	// Every endpoint that decodes a compressed body itself; Telegraf and
	// the InfluxDB clients send Content-Encoding: gzip to the first two.
	writes := []string{"/write", "/api/v2/write", "/api/v1/write/line-protocol", "/api/v1/write/msgpack", "/api/v1/write/tle"}
	for _, path := range writes {
		app.Post(path, func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusNoContent) })
	}

	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	_, _ = w.Write(make([]byte, 64<<20)) // 64 MB of zeros, ~64 KB compressed
	_ = w.Close()

	send := func(path, encoding string) int {
		req := httptest.NewRequest("POST", path, bytes.NewReader(gz.Bytes()))
		req.Header.Set("Content-Type", "application/json")
		if encoding != "" {
			req.Header.Set("Content-Encoding", encoding)
		}
		resp, err := app.Test(req, 10000)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	for _, enc := range []string{"gzip", "GZIP", "br", "deflate", "zstd"} {
		if code := send("/api/v1/query", enc); code != fiber.StatusUnsupportedMediaType {
			t.Errorf("query with Content-Encoding %s: %d, want 415", enc, code)
		}
	}
	if read != 0 {
		t.Fatalf("a refused body was read (%d bytes)", read)
	}
	if code := send("/api/v1/query", "identity"); code != fiber.StatusNoContent {
		t.Errorf("identity: %d", code)
	}
	for _, path := range append(writes, "/write/", "/WRITE") {
		if code := send(path, "gzip"); code != fiber.StatusNoContent {
			t.Errorf("write endpoint %s with gzip: %d, want 204", path, code)
		}
	}
}

// The real server installs the refusal ahead of every route.
func TestServerRefusesEncodedBodies(t *testing.T) {
	app := NewServer(nil, zerolog.Nop()).GetApp()
	read := false
	app.Post("/api/v1/query", func(c *fiber.Ctx) error {
		read = len(c.Body()) > 0
		return c.SendStatus(fiber.StatusNoContent)
	})
	req := httptest.NewRequest("POST", "/api/v1/query", bytes.NewReader([]byte{0x1f, 0x8b}))
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusUnsupportedMediaType || read {
		t.Fatalf("status %d, body read %v", resp.StatusCode, read)
	}
}

// Every Content-Encoding line counts: Fiber decodes with the last one, so a
// first "identity" (or empty) line must not let a later "gzip" through.
func TestEncodedRequestBodiesRepeatedHeader(t *testing.T) {
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	app.Use(rejectEncodedBodies)
	read := 0
	app.Post("/api/v1/query", func(c *fiber.Ctx) error {
		read += len(c.Body())
		return c.SendStatus(fiber.StatusNoContent)
	})
	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	_, _ = w.Write(make([]byte, 8<<20))
	_ = w.Close()
	for _, values := range [][]string{{"identity", "gzip"}, {"", "gzip"}, {"identity, gzip"}, {"gzip", "identity"}} {
		req := httptest.NewRequest("POST", "/api/v1/query", bytes.NewReader(gz.Bytes()))
		for _, v := range values {
			req.Header.Add("Content-Encoding", v)
		}
		resp, err := app.Test(req, 10000)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusUnsupportedMediaType {
			t.Errorf("Content-Encoding %q: %d, want 415", values, resp.StatusCode)
		}
	}
	if read != 0 {
		t.Fatalf("a refused body was read (%d bytes)", read)
	}
}
