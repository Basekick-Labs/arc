package api

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// directStreamServer serves GET /s with a direct body stream running sw, on a
// real listener, so the test reads exactly what a client gets.
func directStreamServer(t *testing.T, cancel func(), trailers *responseTrailers, onPanic func(), sw func(*bufio.Writer)) string {
	t.Helper()
	metrics.Init(zerolog.Nop())
	h := &QueryHandler{logger: zerolog.Nop()}
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	app.Get("/s", func(c *fiber.Ctx) error {
		if trailers != nil {
			if err := c.Context().Response.Header.AddTrailer("Arc-Test-Trailer"); err != nil {
				t.Error(err)
			}
		}
		h.setDirectBodyStream(c.Context(), cancel, trailers, "test", onPanic, sw)
		return nil
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Listener(ln) }()
	t.Cleanup(func() { _ = app.Shutdown() })
	return ln.Addr().String()
}

// rawRequest sends one request and returns the whole raw response.
func rawRequest(t *testing.T, addr, method string) string {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "%s /s HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n", method)
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	raw, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// chunkSizes parses a chunked body into its chunk sizes.
func chunkSizes(t *testing.T, raw string) []int {
	t.Helper()
	_, body, ok := strings.Cut(raw, "\r\n\r\n")
	if !ok {
		t.Fatalf("no head in %q", raw)
	}
	r := bufio.NewReader(strings.NewReader(body))
	var sizes []int
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("chunk header: %v", err)
		}
		n, err := strconv.ParseInt(strings.TrimSpace(line), 16, 64)
		if err != nil {
			t.Fatalf("chunk size %q: %v", line, err)
		}
		if n == 0 {
			return sizes
		}
		sizes = append(sizes, int(n))
		if _, err := r.Discard(int(n) + 2); err != nil {
			t.Fatal(err)
		}
	}
}

// The writer's flushes reach the wire as whole chunks: the fast path. Through
// the old pipe every chunk was at most 4 KB.
func TestDirectStreamWritesWholeFlushesAsChunks(t *testing.T) {
	var runs atomic.Int32
	payload := bytes.Repeat([]byte("x"), 200<<10)
	addr := directStreamServer(t, nil, nil, nil, func(w *bufio.Writer) {
		runs.Add(1)
		_, _ = w.Write(payload)
		_ = w.Flush()
		_, _ = w.Write(payload)
		_ = w.Flush()
	})
	raw := rawRequest(t, addr, "GET")
	if !strings.HasPrefix(raw, "HTTP/1.1 200") {
		t.Fatalf("head: %.200q", raw)
	}
	sizes := chunkSizes(t, raw)
	if len(sizes) != 2 || sizes[0] != len(payload) || sizes[1] != len(payload) {
		t.Fatalf("chunk sizes %v, want two of %d", sizes, len(payload))
	}
	if n := runs.Load(); n != 1 {
		t.Fatalf("writer ran %d times", n)
	}
}

// HEAD: fasthttp skips the body, so only Close runs the writer, after the
// query was cancelled, against a sink that refuses every write. The writer
// runs once, its cleanup runs, and it does not hold up the response.
func TestDirectStreamHEADRunsTheWriterOnceAfterCancel(t *testing.T) {
	var runs, cleanups atomic.Int32
	var cancelled atomic.Bool
	var sawCancelFirst atomic.Bool
	var writeErr atomic.Value
	addr := directStreamServer(t, func() { cancelled.Store(true) }, nil, nil, func(w *bufio.Writer) {
		defer cleanups.Add(1)
		runs.Add(1)
		sawCancelFirst.Store(cancelled.Load())
		_, _ = w.Write(bytes.Repeat([]byte("x"), 1<<10))
		if err := w.Flush(); err != nil {
			writeErr.Store(err)
		}
	})
	start := time.Now()
	raw := rawRequest(t, addr, "HEAD")
	if !strings.HasPrefix(raw, "HTTP/1.1 200") {
		t.Fatalf("head: %.200q", raw)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("HEAD took %v", d)
	}
	deadline := time.Now().Add(5 * time.Second)
	for cleanups.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if runs.Load() != 1 || cleanups.Load() != 1 {
		t.Fatalf("writer ran %d times, cleanup %d times; want once each", runs.Load(), cleanups.Load())
	}
	if !sawCancelFirst.Load() {
		t.Fatal("the query was not cancelled before the writer ran")
	}
	if err, _ := writeErr.Load().(error); !errors.Is(err, errResponseBodyNotSent) {
		t.Fatalf("writer saw %v, want errResponseBodyNotSent", err)
	}
}

// A client that goes away mid-stream makes the writer's next Flush fail: it
// stops within a few batches instead of encoding the whole result.
func TestDirectStreamStopsWhenTheClientGoesAway(t *testing.T) {
	var batches atomic.Int32
	done := make(chan struct{})
	batch := bytes.Repeat([]byte("x"), 64<<10)
	addr := directStreamServer(t, nil, nil, nil, func(w *bufio.Writer) {
		defer close(done)
		for i := 0; i < 100000; i++ {
			_, _ = w.Write(batch)
			if err := w.Flush(); err != nil {
				return
			}
			batches.Add(1)
		}
	})
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(c, "GET /s HTTP/1.1\r\nHost: x\r\n\r\n")
	if _, err := io.ReadFull(c, make([]byte, 1<<20)); err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the writer kept going after the client left")
	}
	// The socket buffers hold some batches the client never read; 100000
	// would mean the writer never saw the error.
	if n := batches.Load(); n > 2000 {
		t.Fatalf("%d batches flushed after the client left", n)
	}
}

// A panic in the writer is recovered by safeStream, onPanic runs, the trailers
// it set are published, and the connection is answered rather than crashed.
func TestDirectStreamPanicPublishesTrailers(t *testing.T) {
	trailers := newResponseTrailers()
	var onPanicRan atomic.Bool
	addr := directStreamServer(t, nil, trailers, func() {
		onPanicRan.Store(true)
		trailers.setIfAbsent("Arc-Test-Trailer", "stream writer panicked")
	}, func(w *bufio.Writer) {
		_, _ = w.Write([]byte("partial"))
		_ = w.Flush()
		panic("boom")
	})
	raw := rawRequest(t, addr, "GET")
	if !onPanicRan.Load() {
		t.Fatal("onPanic did not run")
	}
	if !strings.Contains(raw, "\r\n0\r\nArc-Test-Trailer: stream writer panicked\r\n") {
		t.Fatalf("trailer not on the wire: %q", raw)
	}
}

// Read is not how fasthttp consumes the stream; a caller that reads anyway
// gets an error, and Close still runs the writer once.
func TestDirectStreamReadThenClose(t *testing.T) {
	var runs atomic.Int32
	h := &QueryHandler{logger: zerolog.Nop()}
	metrics.Init(zerolog.Nop())
	d := &directBodyStream{sw: h.safeStream("test", nil, func(w *bufio.Writer) {
		runs.Add(1)
		_, _ = w.WriteString("x")
		_ = w.Flush()
	})}
	if _, err := d.Read(make([]byte, 8)); !errors.Is(err, errDirectStreamRead) {
		t.Fatalf("Read: %v", err)
	}
	_ = d.Close()
	_ = d.Close()
	if n, err := d.WriteTo(io.Discard); n != 0 || !errors.Is(err, errBodyStreamConsumed) {
		t.Fatalf("WriteTo after Close: %d %v, want errBodyStreamConsumed", n, err)
	}
	if n := runs.Load(); n != 1 {
		t.Fatalf("writer ran %d times", n)
	}
}

// A transport error reaches fasthttp from WriteTo, so it does not go on to
// write the last chunk and trailers, which would mark a cut body complete.
func TestDirectStreamWriteToReturnsTheTransportError(t *testing.T) {
	h := &QueryHandler{logger: zerolog.Nop()}
	metrics.Init(zerolog.Nop())
	broken := errors.New("connection reset")
	var flushErr error
	d := &directBodyStream{sw: h.safeStream("test", nil, func(w *bufio.Writer) {
		_, _ = w.WriteString("x")
		flushErr = w.Flush()
	})}
	if _, err := d.WriteTo(failAfter{err: broken}); !errors.Is(err, broken) {
		t.Fatalf("WriteTo returned %v, want the transport error", err)
	}
	if !errors.Is(flushErr, broken) {
		t.Fatalf("the writer's Flush saw %v, want the transport error", flushErr)
	}
}

type failAfter struct{ err error }

func (f failAfter) Write([]byte) (int, error) { return 0, f.err }
