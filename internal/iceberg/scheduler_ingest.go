package iceberg

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// Ingest-triggered export (#1750).
//
// An ingest flush that has written a file asks for its measurement's table
// (RequestIngestReconcile), so the file is exported after at most
// ingestInterval instead of at the next timed pass.
//
// These requests are a kind of their own, because they come at the flush
// rate (every few seconds per measurement, forever), which none of the
// others do:
//
//   - Spaced per table by ingestInterval, counted from the last ATTEMPT an
//     ingest request caused, whatever came of it. A table whose reconcile
//     fails (a schema conflict, a storage outage) or is busy would otherwise
//     be listed again at every flush. Spacing also bounds how fast a table
//     publishes versions: readers resolve a published version and open it a
//     moment later, and only retain_snapshots+1 are kept.
//   - Served after every other kind. A compaction's request takes deleted
//     inputs out of a table and must not queue behind a backlog of ingest
//     ones; inside a timed pass only one ingest request is served between two
//     measurements, so the pass still finishes.
//   - Timed by the loop's one timer (ingestDue holds when each is due), not by
//     a timer per table.
//
// ingestPending is the other half: when a file was first reported for a
// table that no reconcile has covered since. Its oldest entry is
// arc_iceberg_export_pending_seconds, the number that keeps growing when the
// export is stuck, which a histogram of completed exports cannot show.

// ingestBatch is how many due ingest requests the idle loop serves before it
// looks at its timers again: with more tables than it can keep up with, the
// timed pass must still get its turn.
const ingestBatch = 16

type ingestRequest struct {
	m   Measurement
	due time.Time
}

// RequestIngestReconcile asks for one measurement's table to be reconciled
// because an ingest flush has written a file of it. Safe from any goroutine,
// never blocks, and does nothing when the ingest trigger is off
// (iceberg.ingest_export_interval = 0). Requests for a table that is already
// waiting coalesce.
func (s *Scheduler) RequestIngestReconcile(database, measurement string) {
	if s == nil || s.ingestInterval <= 0 || database == "" || measurement == "" {
		return
	}
	key := database + "\x00" + measurement
	now := time.Now()
	s.reqMu.Lock()
	if _, waiting := s.ingestPending[key]; !waiting {
		s.ingestPending[key] = now
		s.publishIngestPendingLocked()
	}
	_, queued := s.ingestDue[key]
	if !queued {
		due := now
		if next := s.ingestNext[key]; next.After(due) {
			due = next
		}
		s.ingestDue[key] = ingestRequest{m: Measurement{Database: database, Measurement: measurement}, due: due}
	}
	s.reqMu.Unlock()
	if queued {
		return // the loop already knows when it is due
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// publishIngestPendingLocked sets the pending gauge from the oldest report.
// Caller holds reqMu.
func (s *Scheduler) publishIngestPendingLocked() {
	var oldest time.Time
	for _, t := range s.ingestPending {
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
		}
	}
	metrics.Get().SetIcebergOldestPendingExport(oldest)
}

// nextIngestDue is when the earliest ingest request is due (ok false: none).
func (s *Scheduler) nextIngestDue() (time.Time, bool) {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	var earliest time.Time
	for _, r := range s.ingestDue {
		if earliest.IsZero() || r.due.Before(earliest) {
			earliest = r.due
		}
	}
	return earliest, !earliest.IsZero()
}

// armIngestTimer sets t to fire when the earliest ingest request is due, or
// leaves it stopped when there is none.
func (s *Scheduler) armIngestTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	if due, ok := s.nextIngestDue(); ok {
		d := time.Until(due)
		if d < 0 {
			d = 0
		}
		t.Reset(d)
	}
}

// takeDueIngest removes and returns the ingest request that has been due the
// longest, and stamps its table's next allowed attempt: the spacing counts
// from the attempt, not from its outcome.
func (s *Scheduler) takeDueIngest(now time.Time) (Measurement, string, bool) {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	var key string
	var pick ingestRequest
	for k, r := range s.ingestDue {
		if r.due.After(now) {
			continue
		}
		if key == "" || r.due.Before(pick.due) || (r.due.Equal(pick.due) && k < key) {
			key, pick = k, r
		}
	}
	if key == "" {
		return Measurement{}, "", false
	}
	delete(s.ingestDue, key)
	s.ingestNext[key] = now.Add(s.ingestInterval)
	return pick.m, key, true
}

// requeueIngest asks again for a table whose ingest-served reconcile was busy
// or left files behind, at its next allowed attempt.
func (s *Scheduler) requeueIngest(m Measurement, key string) {
	s.reqMu.Lock()
	if _, queued := s.ingestDue[key]; !queued {
		s.ingestDue[key] = ingestRequest{m: m, due: s.ingestNext[key]}
	}
	s.reqMu.Unlock()
}

// dropIngest forgets every ingest request and report: this node does not
// export, and the node that does has its own.
func (s *Scheduler) dropIngest() {
	s.reqMu.Lock()
	clear(s.ingestDue)
	if len(s.ingestPending) > 0 {
		clear(s.ingestPending)
		s.publishIngestPendingLocked()
	}
	s.reqMu.Unlock()
}

// otherRequestsQueued reports whether a request of another kind, or a
// database drop, is waiting.
func (s *Scheduler) otherRequestsQueued() bool {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	return len(s.requested)+len(s.requestedPaths)+len(s.requestedHot)+len(s.drops) > 0
}

// noteIngestCovered records that a reconcile of key which started at
// startedAt went through. A file reported before it started is in the table
// now (or was already), so it no longer waits; one reported while it ran may
// have missed the listing and still does. Whatever changed the table also
// starts the spacing for the next ingest request.
func (s *Scheduler) noteIngestCovered(key string, startedAt time.Time, outcome reconcileOutcome, err error) {
	if s.ingestInterval <= 0 || err != nil {
		return
	}
	if outcome != outcomeReconciled && outcome != outcomeUnchanged {
		return // busy, or files left behind: the report still waits
	}
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	if outcome == outcomeReconciled {
		if next := time.Now().Add(s.ingestInterval); next.After(s.ingestNext[key]) {
			s.ingestNext[key] = next
		}
	}
	reported, waiting := s.ingestPending[key]
	if !waiting || !reported.Before(startedAt) {
		return
	}
	delete(s.ingestPending, key)
	delete(s.ingestDue, key) // nothing reported since: the request has been served
	s.publishIngestPendingLocked()
}

// serviceIngest reconciles the ingest requests that are due, the one due the
// longest first, at most max of them. Requests of the other kinds
// and database drops go first, before each one.
func (s *Scheduler) serviceIngest(ctx context.Context, max int) {
	if s.ingestInterval <= 0 {
		return
	}
	for served := 0; ctx.Err() == nil && served < max; served++ {
		if s.otherRequestsQueued() {
			s.serviceDrops(ctx)
			s.serviceRequests(ctx)
		}
		if _, due := s.nextIngestDue(); !due {
			return
		}
		if !s.gateOpen() {
			s.dropIngest() // another node exports
			return
		}
		m, key, ok := s.takeDueIngest(time.Now())
		if !ok {
			return // none due yet: the loop's timer comes back for it
		}
		if _, blocked := s.lastBlocked[key]; blocked {
			continue // awaiting a namespace migration: the pass handles it
		}
		mctx, cancel := context.WithTimeout(ctx, measurementTimeout)
		outcome, err := s.reconcileOneRecovered(mctx, m, key)
		cancel()
		s.noteOutcome(key, outcome)
		event := s.logger.Debug()
		if err != nil && outcome != outcomeBusy {
			event = logSchemaConflict(s.logger.Warn().Err(err), err)
		}
		event.Str("database", m.Database).Str("measurement", m.Measurement).Str("outcome", outcome.String()).
			Msg("Iceberg reconcile on request (an ingest flush)")
		if outcome == outcomeInProgress || outcome == outcomeBusy {
			s.requeueIngest(m, key)
		}
	}
}
