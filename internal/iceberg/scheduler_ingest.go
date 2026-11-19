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
//   - Spaced per table by ingestInterval, counted from the later of two
//     moments: the last ATTEMPT an ingest request caused, whatever came of
//     it, and the end of the last reconcile of any kind that changed the
//     table. The first, because a table whose reconcile fails (a schema
//     conflict, a storage outage) or is busy would otherwise be listed again
//     at every flush. The second, because spacing is also what bounds how
//     fast a table publishes versions: readers resolve a published version
//     and open it a moment later, and only retain_snapshots+1 are kept, so
//     an ingest request must not publish right behind a compaction's or a
//     timed pass's version.
//   - Served after every other kind. A compaction's request takes deleted
//     inputs out of a table and must not queue behind a backlog of ingest
//     ones; inside a timed pass only one ingest request is served between two
//     measurements, so the pass still finishes.
//   - Timed by the loop's one timer (ingestDue holds when each is due), not by
//     a timer per table.
//
// ingestPending is the other half: for each table, when a file was first and
// last reported that no reconcile has covered since. The oldest "first" is
// arc_iceberg_export_pending_seconds, the number that keeps growing when the
// export is stuck, which a histogram of completed exports cannot show. The
// "last" is what tells a reconcile whether it covered everything: a file
// reported after the reconcile started may have missed its listing, so its
// request must survive that reconcile.

// ingestBatch is how many due ingest requests the idle loop serves before it
// looks at its timers again: with more tables than it can keep up with, the
// timed pass must still get its turn.
const ingestBatch = 16

type ingestRequest struct {
	m   Measurement
	due time.Time
}

// ingestReports is when files of a table were first and last reported since
// the last reconcile that covered them all.
type ingestReports struct{ first, last time.Time }

// RequestIngestReconcile asks for one measurement's table to be reconciled
// because an ingest flush has written a file of it. Safe from any goroutine,
// never blocks, and does nothing when the ingest trigger is off
// (iceberg.ingest_export_interval = 0). Requests for a table that is already
// waiting coalesce.
func (s *Scheduler) RequestIngestReconcile(database, measurement string) {
	if s == nil || s.ingestInterval <= 0 || database == "" || measurement == "" {
		return
	}
	s.reqMu.Lock()
	queued := s.requestIngestLocked(database, measurement, time.Now())
	s.reqMu.Unlock()
	if queued {
		return // the loop already knows when it is due
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// requestIngestLocked records a report for the table at now and queues its
// request; it returns whether one was queued already. reqMu is held.
func (s *Scheduler) requestIngestLocked(database, measurement string, now time.Time) (queued bool) {
	key := database + "\x00" + measurement
	if reports, waiting := s.ingestPending[key]; waiting {
		reports.last = now
		s.ingestPending[key] = reports
	} else {
		s.ingestPending[key] = ingestReports{first: now, last: now}
		s.publishIngestPendingLocked()
	}
	_, queued = s.ingestDue[key]
	if !queued {
		due := now
		if next := s.ingestNext[key]; next.After(due) {
			due = next
		}
		s.ingestDue[key] = ingestRequest{m: Measurement{Database: database, Measurement: measurement}, due: due}
	}
	return queued
}

// publishIngestPendingLocked sets the pending gauge from the oldest report.
// Caller holds reqMu.
func (s *Scheduler) publishIngestPendingLocked() {
	var oldest time.Time
	for _, r := range s.ingestPending {
		if oldest.IsZero() || r.first.Before(oldest) {
			oldest = r.first
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
	// A spacing that has run out says nothing any more: forget it, so the
	// map holds only the tables written to within the last interval.
	now := time.Now()
	for key, next := range s.ingestNext {
		if !next.After(now) {
			delete(s.ingestNext, key)
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

// requeueIngest asks again for a table whose ingest-served reconcile did not
// settle, one interval after that attempt ENDED. The stamp takeDueIngest set
// counts from its start, and an attempt can outlast the interval (a slow
// listing, a commit that times out): from the start, the retry would be due
// the moment the attempt ended, and a table that keeps failing slowly would
// be retried back to back.
func (s *Scheduler) requeueIngest(m Measurement, key string) {
	next := time.Now().Add(s.ingestInterval)
	s.reqMu.Lock()
	if next.After(s.ingestNext[key]) {
		s.ingestNext[key] = next
	}
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
	clear(s.appendPending) // another node exports them (scheduler_append.go)
	if len(s.ingestPending) > 0 {
		clear(s.ingestPending)
		s.publishIngestPendingLocked()
	}
	s.reqMu.Unlock()
}

// forgetIngest drops what is kept for one table that will not be exported.
func (s *Scheduler) forgetIngest(key string) {
	s.reqMu.Lock()
	delete(s.ingestDue, key)
	delete(s.appendPending, key)
	if _, waiting := s.ingestPending[key]; waiting {
		delete(s.ingestPending, key)
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

// noteIngestCovered records what a reconcile of key which started at
// startedAt means for the ingest files reported for it.
//
// A reconcile that changed the table restarts the spacing from its end, for
// a request already queued as much as for the next one: it may have
// published a version.
//
// A reconcile that SETTLED covers the files reported before it started: they
// are in a published table version now (or already were), so they no longer
// wait. settled is the loop's own notion: the measurement's state is cached
// only when the listing was read, every file registered, and the discovery
// files published. A reconcile that committed and could not publish, lost
// the writer role, was busy, left files behind or failed covers nothing: the
// files are not readable yet. A file reported while a settled reconcile ran
// may have missed its listing: it still waits, counted from the reconcile's
// start (it was reported no earlier), and its request stays queued.
//
// Loop goroutine only (it reads s.state).
func (s *Scheduler) noteIngestCovered(key string, startedAt time.Time, outcome reconcileOutcome, err error) {
	if s.ingestInterval <= 0 {
		return
	}
	_, settled := s.state[key]
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	if outcome == outcomeReconciled {
		next := time.Now().Add(s.ingestInterval)
		if next.After(s.ingestNext[key]) {
			s.ingestNext[key] = next
		}
		if r, queued := s.ingestDue[key]; queued && r.due.Before(s.ingestNext[key]) {
			r.due = s.ingestNext[key]
			s.ingestDue[key] = r
		}
	}
	if err != nil || !settled || (outcome != outcomeReconciled && outcome != outcomeUnchanged) {
		return
	}
	reports, waiting := s.ingestPending[key]
	if !waiting || !reports.first.Before(startedAt) {
		return // nothing waits, or everything waiting was reported after it started
	}
	if reports.last.Before(startedAt) {
		delete(s.ingestPending, key)
		delete(s.ingestDue, key) // nothing reported since: the request has been served
	} else {
		reports.first = startedAt
		s.ingestPending[key] = reports
	}
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
		// The names come from the flush, not from the listing a pass
		// filters: one a pass would never export is not exported here.
		if x, ok := s.source.(interface{ Exportable(Measurement) bool }); ok && !x.Exportable(m) {
			s.forgetIngest(key)
			continue
		}
		mctx, cancel := context.WithTimeout(ctx, measurementTimeout)
		outcome, err := s.exportIngest(mctx, m, key)
		cancel()
		s.noteOutcome(key, outcome)
		event := s.logger.Debug()
		if err != nil && outcome != outcomeBusy {
			event = logSchemaConflict(s.logger.Warn().Err(err), err)
		}
		event.Str("database", m.Database).Str("measurement", m.Measurement).Str("outcome", outcome.String()).
			Msg("Iceberg reconcile on request (an ingest flush)")
		// Anything short of a settled reconcile comes back one interval
		// later, whether or not another flush asks: a failure, a busy table,
		// files left behind, a publish that did not land. Except a table the
		// timed passes have escalated as busy for too long: they own it now,
		// as for the other kinds of request (serviceRequests).
		if outcome == outcomeBusy {
			if since, started := s.busySince[key]; started && time.Since(since) >= busyEscalateAfter {
				continue
			}
		}
		if _, settled := s.state[key]; err != nil || !settled || outcome == outcomeInProgress || outcome == outcomeBusy {
			s.requeueIngest(m, key)
		}
	}
}
