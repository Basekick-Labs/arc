package api

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// The crafted zstd frame of GHSA-fpmh-frq8-7rj8, sent to every write endpoint
// that decodes zstd: each answers 400 and the server keeps serving. A decoder
// panic on a goroutine of its own would kill this test binary.
func TestWriteEndpointsRejectTheCraftedZstdFrame(t *testing.T) {
	nop := zerolog.Nop()
	backend, err := storage.NewLocalBackend(t.TempDir(), nop)
	if err != nil {
		t.Fatal(err)
	}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, nop)
	t.Cleanup(func() { buf.Close() })
	app := fiber.New(appConfig(DefaultServerConfig(), nop))
	NewMsgPackHandler(nop, buf, 1<<30).RegisterRoutes(app)
	NewLineProtocolHandler(buf, nop).RegisterRoutes(app)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Listener(ln) }()
	t.Cleanup(func() { _ = app.Shutdown() })
	base := "http://" + ln.Addr().String()
	client := &http.Client{Timeout: 10 * time.Second}

	post := func(path, contentType string, body []byte) (int, string) {
		req, err := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("Content-Encoding", "zstd")
		req.Header.Set("x-arc-database", "db")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v (the server died?)", path, err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	lp := zstdEncodeForTest(t, []byte(fmt.Sprintf("cpu,host=a v=1 %d\n", time.Now().UnixNano())))
	for _, ep := range []struct{ path, contentType string }{
		{"/api/v1/write/msgpack", "application/msgpack"},
		{"/api/v1/write/line-protocol", "text/plain"},
		{"/write?db=db", "text/plain"},
		{"/api/v2/write?bucket=db", "text/plain"},
	} {
		for i := 0; i < 2; i++ {
			if code, body := post(ep.path, ep.contentType, ghsaFpmhFrame); code != fiber.StatusBadRequest {
				t.Fatalf("%s: crafted frame got %d %s, want 400", ep.path, code, body)
			}
		}
	}
	// Still serving, and a valid zstd write still goes through.
	if code, body := post("/api/v1/write/line-protocol", "text/plain", lp); code != fiber.StatusNoContent {
		t.Fatalf("valid write after the crafted ones: %d %s", code, body)
	}
}
