package iceberg

import (
	"context"
	"time"
)

// Shutdown export (#1886).
//
// At shutdown the ingest buffer's final flush writes files after the loop
// has stopped: the shutdown hooks (where the loop stops) all run before the
// components (where the buffer flushes). Their export requests used to wait
// in the queue until the process exited, and the tables stayed behind their
// bucket until the next start's pass. So did requests that were queued but
// not yet due: ingest requests are spaced by ingestInterval.
//
// Now the loop is stopped early, as before (StopWithin, from a hook, so no
// timed pass competes with the final flush), and once the buffer and the WAL
// have closed a shutdown component serves what is queued (DrainStopped). The
// loop has ended by then, so the component's goroutine is the only one that
// touches the loop's state.
//
// The drain starts no timed pass of its own: it serves the requests that are
// queued, an append where one is possible and otherwise that table's own
// pass, with a commit reserve scaled to the time left (drainCommitReserve).
// What it does not finish, because its deadline passed or a table failed,
// is lost with the process; the next start's pass reconciles those tables,
// as it always has.
//
// The loop's stop is bounded (StopWithin). A pass that ignores cancellation
// (the leader's manifest barrier, a shared-bucket drop record's one-minute
// write) can outlive the wait: the drain and the export's own catalog handle
// are then left alone, and that pass may still be running while the cluster
// coordinator, auth and storage close after it. The old unbounded Stop could
// instead hold the ingest buffer's final flush for as long as such a pass ran.

// StopWithin cancels the loop, and the pass in flight with it, and waits at
// most d for the loop to end. It reports whether the loop has ended: false
// means it is still running, and nothing may touch the scheduler's state
// afterwards (DrainStopped refuses). A scheduler that never started reports
// true.
func (s *Scheduler) StopWithin(d time.Duration) bool {
	if s.cancel == nil {
		return true
	}
	s.cancel()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-s.done:
		return true
	case <-t.C:
		s.logger.Warn().Dur("waited", d).
			Msg("Iceberg export: the loop did not stop in time at shutdown; the final export is skipped and the next start reconciles the tables")
		return false
	}
}

// DrainStopped serves, on the caller's goroutine, the requests queued when
// the loop stopped, until none is left or ctx is done. It returns how many
// tables it exported.
//
// It does nothing unless the loop ran and has ended (StopWithin returned
// true), or while a restore holds the export paused. Order:
//   - database drops, whatever the gate, as the loop serves them;
//   - then, only where this node exports, the ingest requests, each table
//     at most once (again only if a file was reported after the attempt
//     began), whatever their due time: the spacing bounds how fast steady
//     ingest publishes versions, and at shutdown each table gets one more at
//     most;
//   - then the other requests (a compaction, DELETE, retention or tiering
//     change), which a timed pass of the next start also covers.
func (s *Scheduler) DrainStopped(ctx context.Context) int {
	if s.done == nil {
		return 0 // never started: the export was off
	}
	select {
	case <-s.done:
	default:
		return 0 // the loop still owns the state
	}
	if s.pausedForRestore() {
		return 0
	}
	s.serviceDrops(ctx)
	if !s.gateOpen() {
		s.dropIngest() // another node exports
		return 0
	}
	exported := 0
	attempted := map[string]time.Time{}
	for ctx.Err() == nil {
		m, key, ok := s.takeIngestForDrain(attempted)
		if !ok {
			break
		}
		if _, blocked := s.lastBlocked[key]; blocked {
			continue // awaiting a namespace migration: the next start's pass
		}
		if x, ok := s.source.(interface{ Exportable(Measurement) bool }); ok && !x.Exportable(m) {
			s.forgetIngest(key)
			continue
		}
		began := time.Now()
		attempted[key] = began
		mctx, cancel := context.WithTimeout(ctx, measurementTimeout)
		mctx = withCommitReserve(mctx, drainCommitReserve(mctx))
		outcome, err := s.exportIngest(mctx, m, key)
		cancel()
		s.afterIngest(m, key, outcome, err, time.Since(began))
		if err == nil && outcome == outcomeReconciled {
			exported++
		}
	}
	if ctx.Err() == nil {
		s.serviceRequests(ctx)
	}
	return exported
}

// drainCommitReserve is the commit reserve for one table's export in the
// drain: a third of what is left before ctx's deadline, at most the
// exporter's default. A pass reads files only with more than its reserve
// left; the default (20 s) is often more than the whole drain has, and a
// table on a shared bucket, or one exported for the first time, is a pass.
// A commit cut short by the deadline leaves what any cancelled commit
// leaves, which the next start's pass reconciles.
func drainCommitReserve(ctx context.Context) time.Duration {
	dl, ok := ctx.Deadline()
	if !ok {
		return defaultCommitReserve
	}
	return min(time.Until(dl)/3, defaultCommitReserve)
}

// takeIngestForDrain removes and returns a queued ingest request, the one
// queued longest first, whatever its due time: one whose table was not
// attempted in this drain, or was attempted before a later file report.
func (s *Scheduler) takeIngestForDrain(attempted map[string]time.Time) (Measurement, string, bool) {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	var key string
	var pick ingestRequest
	for k, r := range s.ingestDue {
		if at, tried := attempted[k]; tried && !s.ingestPending[k].last.After(at) {
			continue
		}
		if key == "" || r.due.Before(pick.due) || (r.due.Equal(pick.due) && k < key) {
			key, pick = k, r
		}
	}
	if key == "" {
		return Measurement{}, "", false
	}
	r := pick
	delete(s.ingestDue, key)
	s.ingestNext[key] = time.Now().Add(s.ingestInterval)
	return r.m, key, true
}
