package api

import (
	"bufio"

	"github.com/basekick-labs/arc/internal/metrics"
)

// safeStream wraps a response body-stream writer so a panic inside it cannot
// take the process down (#717).
//
// fasthttp runs these writers on a bare goroutine: SetBodyStreamWriter hands
// the callback to NewStreamReader, which does `go func() { sw(bw); ... }()`
// with no recovery of its own. Fiber's recover middleware does not help, since
// the handler returned before the writer ever runs. An unrecovered panic there
// is a process crash, not a failed request.
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

// Every body stream in this package goes through this wrapper, whether it is
// installed with SetBodyStreamWriter directly or through
// setBodyStreamWithTrailers (#729). CI greps for both, plus the underlying
// NewStreamReader, and fails on any that does not mention safeStream.
//
// One exception, deliberate and excluded from that grep: the three writers in
// arcx_hook.go. They build only under the arcx_engine tag, which no CI or
// release build uses, so they cannot be compile-checked here. They are
// genuinely unwrapped, and tracked separately.
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
