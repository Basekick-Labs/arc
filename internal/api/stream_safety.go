package api

import (
	"bufio"

	"github.com/basekick-labs/arc/internal/metrics"
)

// safeStream wraps a response body-stream writer so a panic inside it cannot
// take the process down (#717).
//
// The writers run after the handler returned, on fasthttp's connection
// goroutine (directBodyStream.WriteTo), so Fiber's recover middleware does not
// cover them. fasthttp's writeBodyStream recovers a panic there, but only by
// dropping the connection: nothing would log the cause, count the error,
// release the query's resources or mark its registry entry. Before Arc
// 27.01.1 the writers ran on a bare goroutine (SetBodyStreamWriter), where an
// unrecovered panic was a process crash.
//
// onPanic runs only on the panic path, after the root cause has been logged,
// and is for work that has no meaning on the happy path: disposing of a
// query-registry entry that would otherwise sit in "running" forever, and
// marking a response the client would otherwise read as complete.
//
// Freeing resources is NOT its job. That belongs in an ordinary defer inside
// sw, which every caller now uses, and which covers strictly more than onPanic
// does: a runtime.Goexit runs defers but leaves recover returning nil, so
// onPanic never fires, and a panic inside onPanic ahead of a release would
// strand whatever it had not freed yet (#733).
//
// It runs inside its own recover, because it executes while a panic is already
// in flight: a second panic here would either kill the process or, being the
// most recent value, replace the root cause in the log.
//
// Every body stream in this package is installed by setDirectBodyStream, which
// applies this wrapper itself. CI fails on any SetBodyStreamWriter or
// NewStreamReader in the package, and on any SetBodyStream outside that
// helper.
//
// That includes the three writers in arcx_hook.go (#717). They build only
// under the arcx_engine tag, which no CI or release build uses, so the grep is
// the only automated check they get; they are compile-checked by hand against
// the arcx library before changes there merge.
func (h *QueryHandler) safeStream(stream string, onPanic func(), sw func(*bufio.Writer)) func(*bufio.Writer) {
	return func(w *bufio.Writer) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			metrics.Get().IncQueryErrors()
			h.logger.Error().Interface("panic", r).Str("stream", stream).
				Msg("Response stream writer panicked; the client receives a truncated response")
			if onPanic == nil {
				return
			}
			defer func() {
				if cr := recover(); cr != nil {
					h.logger.Error().Interface("panic", cr).Str("stream", stream).
						Msg("Stream panic cleanup panicked; resources may be leaked")
				}
			}()
			onPanic()
		}()
		sw(w)
	}
}
