package api

// #1270: a standby writer forwards a write to the primary instead of
// answering 503, once; the body is forwarded as received.

import (
	"bytes"
	"compress/gzip"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func standbyApp(t *testing.T, target func() (*cluster.Node, bool), writeTimeout time.Duration) *fiber.App {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	buf.SetIngestGate(func() error { return fencedErr(t) }) // a standby refuses locally
	local := cluster.NewNode("w-5", "w-5", cluster.RoleWriter, "c")
	router := cluster.NewRouter(&cluster.RouterConfig{
		Registry:           cluster.NewRegistry(&cluster.RegistryConfig{Logger: zerolog.Nop()}),
		LocalNode:          local,
		Logger:             zerolog.Nop(),
		StandbyWriteTarget: target,
		WriteTimeout:       writeTimeout,
	})
	h := NewLineProtocolHandler(buf, zerolog.Nop())
	h.SetRouter(router)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)
	mp := NewMsgPackHandler(zerolog.Nop(), buf, 10<<20)
	mp.SetRouter(router)
	mp.RegisterRoutes(app)
	tle := NewTLEHandler(buf, zerolog.Nop())
	tle.SetRouter(router)
	tle.RegisterRoutes(app)
	return app
}

func primaryAt(addr string) func() (*cluster.Node, bool) {
	return func() (*cluster.Node, bool) {
		n := cluster.NewNode("w-1", "w-1", cluster.RoleWriter, "c")
		n.UpdateState(cluster.StateHealthy)
		n.SetAddresses("", addr)
		return n, true
	}
}

func gz(t *testing.T, s string) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	_, _ = w.Write([]byte(s))
	_ = w.Close()
	return b.Bytes()
}

func TestStandbyForwardsWritesToThePrimary(t *testing.T) {
	_, _ = captureRefusals(t)
	body := gz(t, "cpu,host=a value=1 1700000000000000000\n")
	var hits atomic.Int32
	var got []byte
	var fwdBy string
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		got, _ = io.ReadAll(r.Body)
		fwdBy = r.Header.Get(ForwardedByHeader)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer primary.Close()
	app := standbyApp(t, primaryAt(strings.TrimPrefix(primary.URL, "http://")), 0)

	req := httptest.NewRequest("POST", "/api/v1/write/line-protocol", bytes.NewReader(body))
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("x-arc-database", "db")
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNoContent || hits.Load() != 1 {
		t.Fatalf("status %d, primary hits %d; want the primary's 204", resp.StatusCode, hits.Load())
	}
	if !bytes.Equal(got, body) {
		t.Fatal("the body was not forwarded as received (decompressed?)")
	}
	if fwdBy != "w-5" {
		t.Fatalf("X-Arc-Forwarded-By %q", fwdBy)
	}

	// Already forwarded: one hop at most; the local refusal answers.
	req = httptest.NewRequest("POST", "/api/v1/write/line-protocol", bytes.NewReader(body))
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("x-arc-database", "db")
	req.Header.Set(ForwardedByHeader, "w-9")
	resp, _ = app.Test(req, 10000)
	if resp.StatusCode != http.StatusServiceUnavailable || hits.Load() != 1 {
		t.Fatalf("second hop: status %d hits %d", resp.StatusCode, hits.Load())
	}
}

func TestStandbyForwardFailures(t *testing.T) {
	_, _ = captureRefusals(t)
	lp := "cpu,host=a value=1 1700000000000000000\n"
	// No target: the local 503.
	app := standbyApp(t, func() (*cluster.Node, bool) { return nil, false }, 0)
	req := httptest.NewRequest("POST", "/api/v1/write/line-protocol", strings.NewReader(lp))
	req.Header.Set("x-arc-database", "db")
	if resp, _ := app.Test(req, 10000); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("no target: %d", resp.StatusCode)
	}
	// Unreachable primary: 503 marked as a refusal (nothing written).
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := l.Addr().String()
	_ = l.Close()
	app = standbyApp(t, primaryAt(dead), 0)
	req = httptest.NewRequest("POST", "/api/v1/write/line-protocol", strings.NewReader(lp))
	req.Header.Set("x-arc-database", "db")
	resp, _ := app.Test(req, 10000)
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get(IngestFencedHeader) == "" || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("unreachable: %d fenced=%q", resp.StatusCode, resp.Header.Get(IngestFencedHeader))
	}
	// Sent, no answer in time: 504, never 502, never retried.
	var hits atomic.Int32
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		time.Sleep(400 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer slow.Close()
	app = standbyApp(t, primaryAt(strings.TrimPrefix(slow.URL, "http://")), 100*time.Millisecond)
	req = httptest.NewRequest("POST", "/api/v1/write/line-protocol", strings.NewReader(lp))
	req.Header.Set("x-arc-database", "db")
	resp, _ = app.Test(req, 10000)
	time.Sleep(500 * time.Millisecond)
	if resp.StatusCode != http.StatusGatewayTimeout || hits.Load() != 1 {
		t.Fatalf("ambiguous: status %d hits %d; want 504 and a single send", resp.StatusCode, hits.Load())
	}
}

// Every write route forwards: msgpack, the line-protocol endpoints, TLE.
func TestStandbyForwardsOnEveryWriteRoute(t *testing.T) {
	_, _ = captureRefusals(t)
	var hits atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer primary.Close()
	app := standbyApp(t, primaryAt(strings.TrimPrefix(primary.URL, "http://")), 0)
	for _, path := range []string{"/api/v1/write/msgpack", "/api/v1/write/line-protocol", "/write?db=db", "/api/v2/write?bucket=db", "/api/v1/write/tle"} {
		before := hits.Load()
		req := httptest.NewRequest("POST", path, strings.NewReader("x"))
		req.Header.Set("x-arc-database", "db")
		resp, err := app.Test(req, 10000)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusNoContent || hits.Load() != before+1 {
			t.Fatalf("%s: status %d, forwarded %d", path, resp.StatusCode, hits.Load()-before)
		}
	}
}
