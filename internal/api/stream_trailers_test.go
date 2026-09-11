package api

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/valyala/fasthttp"
)

// Tests for #729. Trailers on a streamed response used to be set from the
// body-writer goroutine, which races with fasthttp serialising the response
// head on the connection goroutine, and on a client disconnect could write
// into a Response already reset for the next request.

// TestFasthttpPublishesTrailersAfterBodyStream pins the fasthttp behaviour the
// whole fix rests on: the connection goroutine drains the body stream to
// io.EOF, and only then writes the trailer section.
//
// This is deliberately handler-free. Nothing in fasthttp's documented API
// promises this order, so a version bump could quietly move the trailer write
// ahead of the body and every trailer would go out empty with no panic, no
// error, and a green -race run. This test is what would fail instead.
func TestFasthttpPublishesTrailersAfterBodyStream(t *testing.T) {
	var resp fasthttp.Response
	resp.Header.SetStatusCode(fasthttp.StatusOK)
	if err := resp.Header.AddTrailer("X-Probe"); err != nil {
		t.Fatalf("AddTrailer: %v", err)
	}

	// Deliberately NOT publishTrailersOnEOF: this test must fail only when
	// fasthttp's ordering changes, so routing it through the production type
	// would make every local regression look like an upstream one.
	resp.SetBodyStream(&setsTrailerAtEOF{data: []byte("body-bytes"), header: &resp.Header}, -1)

	var buf bytes.Buffer
	bw := bufio.NewWriter(&buf)
	if err := resp.Write(bw); err != nil {
		t.Fatalf("Response.Write: %v", err)
	}
	if err := bw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "body-bytes") {
		t.Fatalf("body missing from the response:\n%q", out)
	}
	// The terminating zero-length chunk separates body from trailers.
	term := strings.Index(out, "\r\n0\r\n")
	if term < 0 {
		t.Fatalf("no terminating chunk, so this is not a chunked response:\n%q", out)
	}
	if !strings.Contains(out[term:], "X-Probe: published") {
		t.Errorf("trailer was not written after the body; fasthttp's order changed:\n%q", out)
	}
	if strings.Contains(out[:term], "X-Probe: published") {
		t.Errorf("trailer leaked into the header block:\n%q", out[:term])
	}
}

// TestPublishTrailersOnEOFPublishesExactlyOnce pins the two properties the
// wrapper has to hold regardless of who calls it: publish on io.EOF, and
// publish only once.
func TestPublishTrailersOnEOFPublishesExactlyOnce(t *testing.T) {
	var resp fasthttp.Response
	if err := resp.Header.AddTrailer("X-Count"); err != nil {
		t.Fatalf("AddTrailer: %v", err)
	}
	trailers := newResponseTrailers()
	trailers.set("X-Count", "one")

	p := &publishTrailersOnEOF{stream: io.NopCloser(strings.NewReader("ab")), header: &resp.Header, trailers: trailers}
	b := make([]byte, 1)
	for i := 0; i < 6; i++ {
		if _, err := p.Read(b); err == io.EOF {
			break
		}
	}
	if got := string(resp.Header.Peek("X-Count")); got != "one" {
		t.Errorf("trailer value = %q, want \"one\"", got)
	}
	// A second drain must not re-publish a value the writer has since changed.
	trailers.set("X-Count", "two")
	for i := 0; i < 3; i++ {
		_, _ = p.Read(b)
	}
	if got := string(resp.Header.Peek("X-Count")); got != "one" {
		t.Errorf("republished after EOF: value = %q, want the first publish to stand", got)
	}
}

// TestPublishTrailersOnEOFIgnoresNonEOFErrors pins that a non-EOF read error
// does not publish. fasthttp skips writeTrailer on those, and publishing early
// would let the `published` flag suppress the real publish at EOF.
func TestPublishTrailersOnEOFIgnoresNonEOFErrors(t *testing.T) {
	var resp fasthttp.Response
	if err := resp.Header.AddTrailer("X-Probe"); err != nil {
		t.Fatalf("AddTrailer: %v", err)
	}
	trailers := newResponseTrailers()
	trailers.set("X-Probe", "value")

	p := &publishTrailersOnEOF{stream: errReadCloser{err: io.ErrUnexpectedEOF}, header: &resp.Header, trailers: trailers}
	if _, err := p.Read(make([]byte, 4)); err != io.ErrUnexpectedEOF {
		t.Fatalf("Read error = %v, want io.ErrUnexpectedEOF", err)
	}
	if got := resp.Header.Peek("X-Probe"); len(got) != 0 {
		t.Errorf("published on a non-EOF error: %q", got)
	}
	if p.published {
		t.Error("published flag set by a non-EOF error, which would suppress the real publish at EOF")
	}
}

// TestResponseTrailersConcurrentSetAndPublish is the race regression at the
// unit level: the writer goroutine sets while the connection goroutine
// publishes. Run under -race.
func TestResponseTrailersConcurrentSetAndPublish(t *testing.T) {
	var resp fasthttp.Response
	trailers := newResponseTrailers()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			trailers.set("X-A", "v")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			trailers.publish(&resp.Header)
		}
	}()
	wg.Wait()
}

// setsTrailerAtEOF is a minimal body stream that sets a trailer at the moment
// it reports io.EOF, which is the only thing the ordering contract is about.
type setsTrailerAtEOF struct {
	data   []byte
	pos    int
	header *fasthttp.ResponseHeader
	fired  bool
}

func (r *setsTrailerAtEOF) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		if !r.fired {
			r.fired = true
			r.header.Set("X-Probe", "published")
		}
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *setsTrailerAtEOF) Close() error { return nil }

type errReadCloser struct{ err error }

func (e errReadCloser) Read([]byte) (int, error) { return 0, e.err }
func (e errReadCloser) Close() error             { return nil }
