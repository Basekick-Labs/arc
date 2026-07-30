// Package throttle provides a small, allocation-free debounce primitive shared
// by the process-wide memory-release throttles (debug.FreeOSMemory, glibc
// malloc_trim). All three call sites previously duplicated the same eight lines
// of throttle bookkeeping; this centralizes them (#421).
//
// The pattern, and every subtlety it encodes, comes from the #420 review:
//
//   - The window is anchored to the MONOTONIC clock (Go's time.Since over a
//     process-start instant), so an NTP step or a manual `date` change cannot
//     move the throttle window. Storing wall-clock nanoseconds would let a clock
//     jump either fire early or wedge the throttle.
//
//   - The stored "last fired" value is nanoseconds-since-process-start in an
//     atomic.Int64, and a stored value of 0 means "never fired". This sentinel
//     is load-bearing, not cosmetic: because time.Since(processStart) is a SMALL
//     duration early in the process's life (a few seconds), `now - 0` is less
//     than any practical interval, so without the sentinel the very FIRST call
//     would be throttled instead of firing. That exact bug shipped once during
//     #420 and cost ~25 MB of idle RSS before it was caught by the memory repro.
//     The wall-clock version accidentally avoided it (now - 0 was ~1.78e18, huge);
//     the monotonic version must handle it explicitly.
package throttle

import (
	"sync/atomic"
	"time"
)

// Debouncer allows an action to proceed at most once per interval, process-wide.
// It is safe for concurrent use: many goroutines may call TryAcquire; at most
// one wins per interval. The zero value is not usable — construct with New.
type Debouncer struct {
	interval     time.Duration
	processStart time.Time
	lastNanos    atomic.Int64
}

// minInterval is the floor New clamps a non-positive interval up to. A zero or
// negative interval would make the time check degenerate (every call passes it,
// leaving only the CAS to gate — a race-per-call gate, not a throttle), which is
// never what a caller means. Clamping rather than panicking keeps a config
// mistake from crashing the process at init on a memory-release path.
const minInterval = time.Millisecond

// New returns a Debouncer that lets an action proceed at most once per interval.
// A non-positive interval is clamped up to a small floor (see minInterval). The
// monotonic anchor is captured now, at construction time.
func New(interval time.Duration) *Debouncer {
	if interval < minInterval {
		interval = minInterval
	}
	return &Debouncer{
		interval:     interval,
		processStart: time.Now(),
	}
}

// TryAcquire reports whether the caller may proceed. It returns true for the
// first caller in each interval and false for everyone else until the interval
// elapses. When it returns true it has already recorded the firing time, so the
// caller should perform the throttled action; when it returns false the caller
// must skip it.
//
// A lost CompareAndSwap (another goroutine acquired concurrently) also returns
// false — exactly one caller wins per interval.
func (d *Debouncer) TryAcquire() bool {
	now := time.Since(d.processStart).Nanoseconds()
	last := d.lastNanos.Load()
	// last==0 means "never fired" — the first call always proceeds. See the
	// package comment for why this sentinel is required with monotonic time.
	if last != 0 && now-last < int64(d.interval) {
		return false
	}
	return d.lastNanos.CompareAndSwap(last, now)
}

// Remaining reports how long until the next TryAcquire would be eligible on the
// time axis (it does not account for a concurrent winner). It returns 0 when a
// call would be eligible now — including before the first fire (last==0). Useful
// for building an HTTP 429 Retry-After; callers that need whole seconds should
// round up themselves, so a sub-second remainder never rounds to an immediate
// retry.
func (d *Debouncer) Remaining() time.Duration {
	last := d.lastNanos.Load()
	if last == 0 {
		return 0
	}
	now := time.Since(d.processStart).Nanoseconds()
	elapsed := time.Duration(now - last)
	if elapsed >= d.interval {
		return 0
	}
	return d.interval - elapsed
}
