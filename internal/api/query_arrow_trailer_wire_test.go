//go:build duckdb_arrow

package api

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// The Arrow IPC trailers as they appear on the wire. Go's client fills
// resp.Trailer with every name the Trailer header announces, so a test that
// reads resp.Trailer cannot tell a trailer that was sent from one that was
// only announced; this one reads the raw trailer section.
//
// Since fasthttp 1.76 (Arc 27.01.1) a trailer that was announced but never
// set is not sent at all (before, it was sent with an empty value). The
// client contract is unchanged: a non-empty value means it was set.
func TestArrowTrailersOnTheWire(t *testing.T) {
	dir := t.TempDir()
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := storage.NewLocalBackend(dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 2, LocalStorageRoot: dir}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	h := NewQueryHandler(duck, backend, logger, 0, 0)
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	h.RegisterRoutes(app)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Listener(ln) }()
	defer app.Shutdown()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	body := `{"sql":"SELECT 1 AS id"}`
	fmt.Fprintf(c, "POST /api/v1/query/arrow HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	raw, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	head, rest, ok := strings.Cut(string(raw), "\r\n\r\n")
	if !ok || !strings.HasPrefix(head, "HTTP/1.1 200") {
		t.Fatalf("response head: %q", head)
	}
	for _, name := range []string{arrowStreamTruncatedTrailer, arrowExecutionTimeTrailer, arrowRowsCappedTrailer} {
		if !strings.Contains(head, name) {
			t.Errorf("%s is not announced in the Trailer header: %q", name, head)
		}
	}
	// The trailer section follows the last chunk ("0\r\n").
	i := strings.LastIndex(rest, "\r\n0\r\n")
	if i < 0 {
		t.Fatalf("no last chunk in %q", rest)
	}
	trailers := map[string]string{}
	sc := bufio.NewScanner(strings.NewReader(rest[i+len("\r\n0\r\n"):]))
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), ":"); ok {
			trailers[k] = strings.TrimSpace(v)
		}
	}
	if trailers[arrowExecutionTimeTrailer] == "" {
		t.Errorf("%s not sent: %v", arrowExecutionTimeTrailer, trailers)
	}
	for _, name := range []string{arrowStreamTruncatedTrailer, arrowRowsCappedTrailer} {
		if v := trailers[name]; v != "" {
			t.Errorf("%s = %q on a complete, uncapped stream", name, v)
		}
	}
}
