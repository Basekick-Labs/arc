package api

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// HTTP/1.1 requires a Host header. fasthttp 1.76 (Arc 27.01.1) enforces it:
// a request without one is answered 400 and the connection closed (before,
// it was served). HTTP/1.0 requests, which have no such rule, are still
// served.
func TestRequestWithoutHostHeader(t *testing.T) {
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	app.Get("/ping", func(c *fiber.Ctx) error { return c.SendString("pong") })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Listener(ln) }()
	defer app.Shutdown()

	send := func(req string) string {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if _, err := io.WriteString(c, req); err != nil {
			t.Fatal(err)
		}
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		raw, _ := io.ReadAll(c)
		return string(raw)
	}
	if got := send("GET /ping HTTP/1.1\r\nConnection: close\r\n\r\n"); !strings.HasPrefix(got, "HTTP/1.1 400") {
		t.Errorf("HTTP/1.1 without Host: %q, want 400", got)
	}
	if got := send("GET /ping HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"); !strings.HasPrefix(got, "HTTP/1.1 200") || !strings.HasSuffix(got, "pong") {
		t.Errorf("HTTP/1.1 with Host: %q", got)
	}
	if got := send("GET /ping HTTP/1.0\r\n\r\n"); !strings.Contains(got, " 200 ") || !strings.HasSuffix(got, "pong") {
		t.Errorf("HTTP/1.0 without Host: %q", got)
	}
}
