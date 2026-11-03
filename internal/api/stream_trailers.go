package api

import (
	"sync"

	"github.com/valyala/fasthttp"
)

// HTTP trailers on a streamed response have to be filled in after the body is
// written, which is the whole reason they exist, but nothing may touch the
// response header while fasthttp serialises it (#729).
//
// The writer collects values in a responseTrailers while it streams, and
// directBodyStream (stream_direct.go) publishes them into the response header
// when the writer returns. fasthttp's connection goroutine does three things in
// a fixed order (Response.writeBodyStream):
//
//	resp.Header.Write(w)              // serialise the head
//	writeBodyChunked(w, bodyStream)   // call bodyStream.WriteTo
//	resp.Header.writeTrailer(w)       // read the trailer values back
//
// and since the writer now runs inside WriteTo, on that same goroutine, the
// publish is ordered between the last two by construction.

// responseTrailers collects trailer values while the body is written and
// hands them to the response header after it.
//
// The writer and the publish now run on one goroutine, one after the other.
// The mutex is kept anyway: #729 came from relying on an ordering argument
// rather than on a lock, and a writer that sets a value from a goroutine of
// its own would make them concurrent again.
type responseTrailers struct {
	mu     sync.Mutex
	values map[string]string
}

func newResponseTrailers() *responseTrailers {
	return &responseTrailers{values: make(map[string]string, 3)}
}

// set records a trailer value. Safe to call from any goroutine.
// A name that was never set is announced in the Trailer header but not sent
// (since fasthttp 1.76, Arc 27.01.1), which a client reads as empty: the
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
// from directBodyStream.WriteTo, on the connection goroutine, after the body
// and before fasthttp writes the trailer section.
func (t *responseTrailers) publish(h *fasthttp.ResponseHeader) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for name, value := range t.values {
		h.Set(name, value)
	}
}
