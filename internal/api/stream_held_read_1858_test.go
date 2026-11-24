package api

import (
	"bufio"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/readgate"
)

// #1858: a read whose response streams after the handler returned stays in
// the read gate until the stream ends; one without a stream leaves with the
// handler.
func TestStreamedResponseHoldsItsRead(t *testing.T) {
	gate := &readgate.Gate{}
	h := &QueryHandler{logger: zerolog.Nop()}
	release := make(chan struct{})
	streaming := make(chan struct{})
	app := fiber.New()
	app.Get("/stream", func(c *fiber.Ctx) error {
		_, leave := gate.EnterAt()
		defer holdRead(c.Context(), leave)()
		h.setDirectBodyStream(c.Context(), nil, nil, "test", nil, func(w *bufio.Writer) {
			close(streaming)
			<-release
			_, _ = w.WriteString("done")
		})
		return nil
	})
	app.Get("/plain", func(c *fiber.Ctx) error {
		_, leave := gate.EnterAt()
		defer holdRead(c.Context(), leave)()
		return c.SendString("ok")
	})
	done := make(chan error, 1)
	go func() {
		resp, err := app.Test(httptest.NewRequest("GET", "/stream", nil), -1)
		if err == nil {
			resp.Body.Close()
		}
		done <- err
	}()
	<-streaming
	if n := gate.InFlight(); n != 1 {
		t.Fatalf("while streaming: %d reads in the gate, want 1", n)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && gate.InFlight() != 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if n := gate.InFlight(); n != 0 {
		t.Fatalf("after the stream: %d reads in the gate", n)
	}
	resp, err := app.Test(httptest.NewRequest("GET", "/plain", nil), -1)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if n := gate.InFlight(); n != 0 {
		t.Fatalf("after a plain response: %d reads in the gate", n)
	}
}
