package iceberg

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// A REST catalog table load asks for the files reported before it (#1752).
//
// With iceberg.load_export_min_interval (F) above 0, loadTable first waits,
// at most loadFreshBudget, until every ingest file reported for the table
// before the load is in the published version. It never commits itself:
// it pulls the table's queued ingest request forward, to no sooner than F
// after the table's last ingest export, and the loop serves that request as
// it serves any other (an append, or the pass that path already runs). So
// the table is exported at most once per F however many loads come, and a
// table nobody reads keeps its ingest_export_interval (I).
//
// Who decides what. The load (handler goroutine) only records that it
// waits and wakes the loop. The loop (serviceLoadWaiters, at the top of
// each turn) owns the state that says whether the table can be served
// soon: it pulls the request forward only for a table on the append path,
// whose last ingest export settled, not paused for a restore, while fewer
// than ingestConcurrency requests are due within the budget. Otherwise it
// lets the load go at once, to read the published version as before: a
// read-level caller never makes a table that needs a pass run it every F.
//
// Coverage: a file reported before the load is covered once ingestPending
// no longer holds a report older than the load (noteIngestCovered deletes
// the entry, or moves its first report to the start of the export that
// covered the earlier ones). A table the loop drops instead (another node
// exports, or the table is not exported) lets its loads go as skipped.

// loadFreshBudget is how long a load waits for its table's files. An append
// takes tens of milliseconds on local storage and a few hundred on object
// storage; a load that waits longer reads the published version, as before.
var loadFreshBudget = 2 * time.Second // a var for tests

// loadWaiter is one load waiting for its table. outcome is set once, under
// reqMu, when done is closed.
type loadWaiter struct {
	loadAt  time.Time
	done    chan struct{}
	outcome int
	closed  bool
	// decided: the loop has looked at this table for this waiter (pulled
	// its request forward); it waits for coverage now.
	decided bool
}

func (w *loadWaiter) finishLocked(outcome int) {
	if w.closed {
		return
	}
	w.closed, w.outcome = true, outcome
	close(w.done)
}

// AwaitFreshForRead waits, at most loadFreshBudget or until ctx is done,
// until the files reported for the table before the call are in its
// published version, or until the export says it will not serve them soon.
// It never fails: whatever it returns, the caller reads the published
// version. Safe from any goroutine. Off (and immediate) when
// iceberg.load_export_min_interval is 0.
func (s *Scheduler) AwaitFreshForRead(ctx context.Context, database, measurement string) {
	if s == nil || s.loadInterval <= 0 || s.ingestInterval <= 0 {
		return
	}
	key := database + "\x00" + measurement
	w := &loadWaiter{loadAt: time.Now(), done: make(chan struct{})}
	s.reqMu.Lock()
	if _, waiting := s.ingestPending[key]; !waiting {
		s.reqMu.Unlock()
		metrics.Get().IncIcebergLoadFresh(metrics.IcebergLoadFreshNothing)
		return
	}
	s.loadWaiters[key] = append(s.loadWaiters[key], w)
	s.reqMu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	t := time.NewTimer(loadFreshBudget)
	defer t.Stop()
	select {
	case <-w.done:
	case <-t.C:
	case <-ctx.Done():
	}
	s.reqMu.Lock()
	w.finishLocked(metrics.IcebergLoadFreshTimeout) // no-op when already finished
	s.removeLoadWaiterLocked(key, w)
	outcome := w.outcome
	s.reqMu.Unlock()
	metrics.Get().IncIcebergLoadFresh(outcome)
}

func (s *Scheduler) removeLoadWaiterLocked(key string, w *loadWaiter) {
	ws := s.loadWaiters[key]
	for i, x := range ws {
		if x == w {
			ws = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(ws) == 0 {
		delete(s.loadWaiters, key)
	} else {
		s.loadWaiters[key] = ws
	}
}

// skipLoadWaitersLocked lets the loads of key (every table: "") go at once,
// as skipped: the loop is dropping what is reported for it. reqMu is held.
func (s *Scheduler) skipLoadWaitersLocked(key string) {
	for k, ws := range s.loadWaiters {
		if key != "" && k != key {
			continue
		}
		for _, w := range ws {
			w.finishLocked(metrics.IcebergLoadFreshSkipped)
		}
	}
}

// loadCoveredLocked reports whether every file reported for key before at
// has been covered by an export. reqMu is held.
func (s *Scheduler) loadCoveredLocked(key string, at time.Time) bool {
	r, waiting := s.ingestPending[key]
	return !waiting || !r.first.Before(at)
}

// loadServableSoon reports whether the loop can serve key's ingest request
// with an append. Loop goroutine only (state, appendOK, lastBlocked).
func (s *Scheduler) loadServableSoon(key string) bool {
	if _, ok := s.source.(appendResolver); !ok {
		return false
	}
	if _, blocked := s.lastBlocked[key]; blocked {
		return false
	}
	_, settled := s.state[key]
	return settled && s.appendOK[key] && s.appendsSince[key] < appendMaxBetweenPasses
}

// serviceLoadWaiters lets the loads whose files are covered go, and decides
// for each table a load waits on whether to pull its request forward. Loop
// goroutine, at the top of each turn (the loop then arms its ingest timer
// on the new due time). Never asks the gate: a load must not move the
// export role's bookkeeping (gateOpen), and a node that does not export
// drops its requests, which lets the loads go (dropIngest).
func (s *Scheduler) serviceLoadWaiters() {
	if s.loadInterval <= 0 {
		return
	}
	s.reqMu.Lock()
	var undecided []string
	for key, ws := range s.loadWaiters {
		for _, w := range ws {
			if !w.closed && !w.decided {
				undecided = append(undecided, key)
				break
			}
		}
	}
	s.reqMu.Unlock()
	servable := make(map[string]bool, len(undecided))
	if len(undecided) > 0 && !s.pausedForRestore() {
		for _, key := range undecided {
			servable[key] = s.loadServableSoon(key)
		}
	}

	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	now := time.Now()
	horizon := now.Add(loadFreshBudget)
	dueSoon := 0
	for _, r := range s.ingestDue {
		if !r.due.After(horizon) {
			dueSoon++
		}
	}
	for key, ws := range s.loadWaiters {
		pulled := false
		for _, w := range ws {
			if w.closed {
				continue
			}
			if s.loadCoveredLocked(key, w.loadAt) {
				w.finishLocked(metrics.IcebergLoadFreshCovered)
				continue
			}
			if w.decided {
				if s.loadUnsettled[key] {
					// The export it waited for did not settle: the next try
					// is a full interval away (requeueIngest).
					w.finishLocked(metrics.IcebergLoadFreshSkipped)
				}
				continue
			}
			if !pulled && !s.pullForLoadLocked(key, servable[key], now, horizon, &dueSoon) {
				w.finishLocked(metrics.IcebergLoadFreshSkipped)
				continue
			}
			pulled = true
			w.decided = true
		}
	}
}

// pullForLoadLocked moves key's queued ingest request to no later than
// max(now, the table's next allowed attempt − (I − F)), when the loop can
// serve it within the budget. It reports whether the load should wait.
// reqMu is held.
func (s *Scheduler) pullForLoadLocked(key string, servable bool, now, horizon time.Time, dueSoon *int) bool {
	if !servable || s.loadUnsettled[key] {
		return false
	}
	r, queued := s.ingestDue[key]
	if !queued {
		return false // reported, but nothing queued: the loop holds it back (a namespace migration)
	}
	// F counts from the same stamp as the table's own spacing: every
	// attempt and every reconcile that changed the table moves ingestNext
	// (takeDueIngest, noteIngestCovered), so a load never exports the
	// table sooner than F after either.
	target := now
	if next, ok := s.ingestNext[key]; ok {
		if t := next.Add(-(s.ingestInterval - s.loadInterval)); t.After(target) {
			target = t
		}
	}
	if target.After(horizon) {
		return false
	}
	if r.due.After(horizon) {
		// Pulled into the budget's window: bounded by how many requests
		// that window already holds, so a poller over many tables does not
		// queue every table's export behind its loads.
		if *dueSoon >= s.ingestConcurrency {
			return false
		}
		*dueSoon++
	}
	if target.Before(r.due) {
		r.due = target
		s.ingestDue[key] = r
	}
	return true
}
