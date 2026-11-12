package api

import (
	"bufio"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/valyala/fasthttp"
)

// Streamed response bodies are written by the connection goroutine itself,
// straight to the socket.
//
// fasthttp's SetBodyStreamWriter runs the writer on a goroutine of its own
// that writes into an in-memory pipe, and the connection goroutine copies the
// pipe to the socket 4 KB at a time, flushing the socket after every 4 KB.
// On a 1M-row Arrow response that copy was more than half the request time.
// A body stream that implements fasthttp.BodyWriterTo (fasthttp >= 1.74) is
// instead handed the connection's chunk writer: every Flush of the stream's
// 256 KB buffer becomes one chunk on the wire, with no pipe, no copy and no
// second goroutine.
//
// Three consequences shape this type:
//
//   - The writer runs exactly once, whichever way fasthttp consumes or drops
//     the body. WriteTo runs it for real. Close runs it when nothing else did
//     (a HEAD request, whose body fasthttp skips; a connection that failed
//     before the body), against a sink that refuses every write, after
//     cancelling the query: its deferred cleanup (rows, connection, query
//     context, registry entry) lives inside the writer and must not be
//     skipped. The writer sees the refused write as a client disconnect,
//     which is also what the old pipe reported in that case.
//   - Trailers are published from WriteTo, after the writer returns and
//     before fasthttp writes the trailer section, on the same goroutine
//     (#729 needed a reader wrapper to get that ordering across goroutines).
//   - Read is never called: every fasthttp path that consumes a body stream
//     prefers WriteTo when SupportsBodyWriteTo is true.
//
// Two fasthttp features would drop the stream without closing it, so the
// writer would never run and its query's resources would leak: hijacking the
// connection, and TimeoutErrorWithResponse (fasthttp.TimeoutHandler). Arc uses
// neither; adding one in front of a streaming route needs a different design.
// (The old pipe started its goroutine when the stream was set, so it ran
// regardless.)
type directBodyStream struct {
	sw       func(*bufio.Writer) // wrapped in safeStream
	cancel   func()              // cancels the query; nil when there is none
	trailers *responseTrailers   // nil when the response has no trailers
	header   *fasthttp.ResponseHeader
	ran      atomic.Bool
	abort    *streamAbort // nil when the writer cannot abort the response
}

// streamAbort lets a writer end its response as a failed transfer: WriteTo
// returns the error, so fasthttp writes no terminating chunk and closes the
// connection, and the client reads a broken response, not a short complete
// one. For a body relayed from elsewhere (a forwarded Arrow query, #1567),
// where a cut can fall mid-message and no in-band marker is reliable.
type streamAbort struct {
	mu  sync.Mutex
	err error
}

// set records the first abort error.
func (a *streamAbort) set(err error) {
	a.mu.Lock()
	if a.err == nil {
		a.err = err
	}
	a.mu.Unlock()
}

func (a *streamAbort) get() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

// directStreamBufSize is the stream's buffer: writers flush it per batch (the
// client-disconnect signal), and each flush is one chunk on the wire.
const directStreamBufSize = 256 << 10

var directStreamBufPool = sync.Pool{
	New: func() any { return bufio.NewWriterSize(nil, directStreamBufSize) },
}

// errResponseBodyNotSent is what a writer sees when its body is dropped
// (Close without WriteTo): every write is refused.
var errResponseBodyNotSent = errors.New("response body not sent")

// errBodyStreamConsumed is returned by a second WriteTo.
var errBodyStreamConsumed = errors.New("response body already written")

// errDirectStreamRead is returned by Read, which fasthttp does not call.
var errDirectStreamRead = errors.New("direct body stream is written with WriteTo, not read")

// SupportsBodyWriteTo tells fasthttp to call WriteTo instead of Read.
func (d *directBodyStream) SupportsBodyWriteTo() bool { return true }

// WriteTo runs the writer once against w (fasthttp's chunk writer) and
// returns the bytes written and the first write error.
func (d *directBodyStream) WriteTo(w io.Writer) (int64, error) {
	if !d.ran.CompareAndSwap(false, true) {
		// Never reached: fasthttp writes a body once. An error, not an empty
		// success, so a second write could never frame a complete-looking
		// empty response.
		return 0, errBodyStreamConsumed
	}
	cw := &countingWriter{w: w}
	bw := directStreamBufPool.Get().(*bufio.Writer)
	bw.Reset(cw)
	// Deferred, so both run even if a panic gets past safeStream: the buffer
	// goes back to the pool only once nothing writes to it, and trailers are
	// published last, after the writer's own panic handling has set them.
	defer func() {
		bw.Reset(nil)
		directStreamBufPool.Put(bw)
	}()
	defer d.publishTrailers()
	d.sw(bw)
	_ = bw.Flush()
	if err := d.abort.get(); err != nil && cw.err == nil {
		return cw.n, err
	}
	return cw.n, cw.err
}

// Read is not used by fasthttp (see SupportsBodyWriteTo). A caller that reads
// anyway gets an error; the writer then runs at Close, so its cleanup does.
func (d *directBodyStream) Read([]byte) (int, error) {
	return 0, errDirectStreamRead
}

// Close runs the writer if nothing else did, so its cleanup runs: fasthttp
// closes every body stream once the response is done with it.
func (d *directBodyStream) Close() error {
	if !d.ran.CompareAndSwap(false, true) {
		return nil
	}
	if d.cancel != nil {
		d.cancel()
	}
	d.sw(bufio.NewWriterSize(refusingWriter{}, 64))
	return nil
}

func (d *directBodyStream) publishTrailers() {
	if d.trailers != nil {
		d.trailers.publish(d.header)
	}
}

// countingWriter counts what reached fasthttp and keeps the first error.
type countingWriter struct {
	w   io.Writer
	n   int64
	err error
}

func (c *countingWriter) Write(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	n, err := c.w.Write(p)
	c.n += int64(n)
	c.err = err
	return n, err
}

type refusingWriter struct{}

func (refusingWriter) Write([]byte) (int, error) { return 0, errResponseBodyNotSent }

// setDirectBodyStream installs sw, wrapped in safeStream, as the response
// body (see directBodyStream). cancel cancels the query when the body is
// never written; trailers, when not nil, are published after the body.
//
// This file is the only place in the package that installs a body stream, and
// each call is on one line with safeStream so the CI panic-safety guard can
// see both.
func (h *QueryHandler) setDirectBodyStream(fctx *fasthttp.RequestCtx, cancel func(), trailers *responseTrailers, stream string, onPanic func(), sw func(*bufio.Writer), panicMetric ...func()) {
	fctx.Response.SetBodyStream(&directBodyStream{sw: h.safeStream(stream, onPanic, sw, panicMetric...), cancel: cancel, trailers: trailers, header: &fctx.Response.Header}, -1)
}

// setAbortableBodyStream is setDirectBodyStream for a writer that may end
// the response as a failed transfer through abort (see streamAbort).
func (h *QueryHandler) setAbortableBodyStream(fctx *fasthttp.RequestCtx, cancel func(), trailers *responseTrailers, abort *streamAbort, stream string, onPanic func(), sw func(*bufio.Writer)) {
	fctx.Response.SetBodyStream(&directBodyStream{sw: h.safeStream(stream, onPanic, sw), cancel: cancel, trailers: trailers, abort: abort, header: &fctx.Response.Header}, -1)
}
