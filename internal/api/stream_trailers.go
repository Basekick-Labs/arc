package api

import (
	"bufio"
	"io"
	"sync"

	"github.com/valyala/fasthttp"
)

// HTTP trailers on a streamed response have to be filled in after the body is
// written, which is the whole reason they exist, but the goroutine that writes
// the body is not allowed to touch the response header (#729).
//
// fasthttp runs a body-stream writer on a goroutine of its own
// (fasthttp.NewStreamReader) while the connection goroutine serialises the
// response head. Both mutate the same ResponseHeader scratch buffer:
// formatStatusLine writes h.bufKV.value and Set writes h.bufKV through
// initHeaderKV. There is no happens-before edge between them, because the pipe
// NewStreamReader uses is buffered and a body write never blocks on the reader.
// Setting a trailer from the writer goroutine is therefore a data race, and it
// was one on every Arrow IPC response until this type existed.
//
// The way out is that fasthttp's connection goroutine does three things in a
// fixed order (http.go, Response.writeBodyStream):
//
//	resp.Header.Write(w)              // serialise the head
//	writeBodyChunked(w, bodyStream)   // call bodyStream.Read until io.EOF
//	resp.Header.writeTrailer(w)       // read the trailer values back
//
// and SetBodyStreamWriter(sw) is exactly SetBodyStream(NewStreamReader(sw), -1).
// So an io.Reader wrapped around the stream reader has its Read called on the
// connection goroutine, after the head is serialised and before the trailers
// are read back. Publishing from there is ordered by construction.

// responseTrailers collects trailer values on the body-writer goroutine and
// hands them to the connection goroutine.
//
// As used here the two sides are provably never concurrent: every set happens
// before the writer goroutine closes the pipe, and the publish happens after the
// reader observes that close. The mutex is kept anyway because that proof rests
// on who drains the stream. Anything that drains it off the connection goroutine
// (Response.Body() does exactly that, and requestLogger calls it for status
// >= 400) would make them concurrent again, and #729 came from relying on an
// ordering argument rather than on a lock.
type responseTrailers struct {
	mu     sync.Mutex
	values map[string]string
}

func newResponseTrailers() *responseTrailers {
	return &responseTrailers{values: make(map[string]string, 3)}
}

// set records a trailer value. Safe to call from the body-writer goroutine.
// A name that was never set is emitted empty by fasthttp, which is the
// "unknown" case in the client contract.
func (t *responseTrailers) set(name, value string) {
	t.mu.Lock()
	t.values[name] = value
	t.mu.Unlock()
}

// setIfAbsent records a value only when the name has none yet, so a generic
// reason cannot overwrite a specific one. The panic path uses it: a stream that
// failed with a real error and then panicked while logging should still report
// the error, not "stream writer panicked".
func (t *responseTrailers) setIfAbsent(name, value string) {
	t.mu.Lock()
	if _, ok := t.values[name]; !ok {
		t.values[name] = value
	}
	t.mu.Unlock()
}

// publish copies the collected values into the response header. Called only
// from publishTrailersOnEOF.Read, so only on the connection goroutine.
func (t *responseTrailers) publish(h *fasthttp.ResponseHeader) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for name, value := range t.values {
		h.Set(name, value)
	}
}

// publishTrailersOnEOF wraps a body stream and publishes the trailers when the
// body ends.
//
// It deliberately does NOT implement fasthttp's BodyWriterTo: newer fasthttp
// type-switches on that in writeBodyChunked and would take a path that never
// calls Read.
type publishTrailersOnEOF struct {
	stream    io.ReadCloser
	header    *fasthttp.ResponseHeader
	trailers  *responseTrailers
	published bool
}

// Read publishes once, on io.EOF.
//
// Keyed on io.EOF rather than any non-nil error: a pipe read can in principle
// fail with a timeout, and publishing there would both run while the writer
// goroutine is still filling values in and suppress the real publish at EOF.
// On any non-EOF error fasthttp skips writeTrailer anyway, so there is nothing
// to publish. Read is only ever called by one goroutine, so `published` needs
// no synchronisation of its own.
func (p *publishTrailersOnEOF) Read(b []byte) (int, error) {
	n, err := p.stream.Read(b)
	if err == io.EOF && !p.published {
		p.published = true
		p.trailers.publish(p.header)
	}
	return n, err
}

// Close forwards to the stream. fasthttp's closeBodyStream calls this, and
// NewStreamReader requires it or its writer goroutine leaks.
func (p *publishTrailersOnEOF) Close() error { return p.stream.Close() }

// setBodyStreamWithTrailers installs a panic-safe body stream whose trailers are
// published from the connection goroutine (#729).
//
// Preconditions, none of which hold by accident on POST /api/v1/query/arrow but
// none of which this function can enforce:
//
//   - The response must be a 200 with a body. fasthttp skips the body write for
//     HEAD, 1xx, 204 and 304 (Response.mustSkipBody) but still writes trailers,
//     so Read would never run and every trailer would go out empty.
//   - Response compression must stay off for this route. gzipBody replaces the
//     body stream with another NewStreamReader copying on a further goroutine,
//     which would put Read back on the wrong side of the race and outside
//     safeStream. The compression middleware in server.go is disabled.
//   - Nothing may call Response.Body() on this response, which drains the
//     stream on the handler goroutine. requestLogger does that for status >= 400.
//
// The body of this function is one line so the CI panic-safety guard can see
// SetBodyStream, NewStreamReader and safeStream together; it greps per line.
func (h *QueryHandler) setBodyStreamWithTrailers(fctx *fasthttp.RequestCtx, stream string, trailers *responseTrailers, onPanic func(), sw func(*bufio.Writer)) {
	//nolint:lll // see above: the guard matches per line
	fctx.Response.SetBodyStream(&publishTrailersOnEOF{stream: fasthttp.NewStreamReader(h.safeStream(stream, onPanic, sw)), header: &fctx.Response.Header, trailers: trailers}, -1)
}
