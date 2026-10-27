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
// whole fix rests on: the connection goroutine calls the body stream's WriteTo
// to the end, and only then writes the trailer section.
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

	// Deliberately NOT directBodyStream: this test must fail only when
	// fasthttp's ordering changes, so routing it through the production type
	// would make every local regression look like an upstream one.
	resp.SetBodyStream(&setsTrailerAfterWriteTo{data: []byte("body-bytes"), header: &resp.Header}, -1)

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

// setsTrailerAfterWriteTo is a minimal body stream that fasthttp writes with
// WriteTo (as directBodyStream is) and that sets a trailer when it returns,
// which is the only thing the ordering contract is about.
type setsTrailerAfterWriteTo struct {
	data   []byte
	header *fasthttp.ResponseHeader
}

func (r *setsTrailerAfterWriteTo) SupportsBodyWriteTo() bool { return true }

func (r *setsTrailerAfterWriteTo) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(r.data)
	r.header.Set("X-Probe", "published")
	return int64(n), err
}

func (r *setsTrailerAfterWriteTo) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (r *setsTrailerAfterWriteTo) Close() error             { return nil }
