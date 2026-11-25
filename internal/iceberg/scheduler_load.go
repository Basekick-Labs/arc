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
// whose last ingest export settled and whose last append did not fall back
// to a pass, not paused for a restore, while fewer than ingestConcurrency
// requests are due within the budget and loads have pulled fewer than
// ingestConcurrency exports in the last budget (all tables together).
// Otherwise it lets the load go, to read the published version as before:
// a read-level caller never makes a table that needs a pass run it every F.
// serviceLoadWaiters itself never asks the gate; the wake a load sends runs
// the loop's usual services, which do.
//
// Coverage: a file reported before the load is covered once ingestPending
// no longer holds a report older than the load (noteIngestCovered deletes
// the entry, or moves its first report to the start of the export that
// covered the earlier ones). noteIngestCovered lets the table's loads go
// as soon as that happens, wherever the loop is. A table the loop drops
// instead (another node exports, the table is not exported, its database
// is dropped) lets its loads go as skipped.
//
// What it costs: up to ingestConcurrency extra exports per loadFreshBudget
// in all, and per table at most one per F. A table polled by a reader takes
// a full pass every appendMaxBetweenPasses appends (120), sooner than at I.

// loadFreshBudget is how long a load waits for its table's files. An append
// takes tens of milliseconds on local storage and a few hundred on object
// storage; a load that waits longer reads the published version, as before.
var loadFreshBudget = 2 * time.Second // a var for tests

// loadAfterSnapshot, when set (tests only), runs between serviceLoadWaiters'
// two locked sections.
var loadAfterSnapshot func()

// The most loads that wait at once, per table and in all; more read the
// published version at once.
const (
	loadMaxWaitersPerTable = 64
	loadMaxWaiters         = 1024
)

// timedPass runs a timed pass with inTimedPass set: a load cannot get its
// table's export ahead of a pass, so it does not wait for one.
func (s *Scheduler) timedPass(ctx context.Context) bool {
	s.inTimedPass.Store(true)
	defer s.inTimedPass.Store(false)
	return s.runPass(ctx)
}

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
	s.reqMu.Lock()
	if _, waiting := s.ingestPending[key]; !waiting {
		s.reqMu.Unlock()
		metrics.Get().IncIcebergLoadFresh(metrics.IcebergLoadFreshNothing)
		return
	}
	if s.inTimedPass.Load() || s.loadUnsettled[key] || s.loadWaiterN >= loadMaxWaiters ||
		len(s.loadWaiters[key]) >= loadMaxWaitersPerTable {
		s.reqMu.Unlock()
		metrics.Get().IncIcebergLoadFresh(metrics.IcebergLoadFreshSkipped)
		return
	}
	w := &loadWaiter{loadAt: time.Now(), done: make(chan struct{})}
	// A closed waiter stays listed until its own load removes it: only an
	// open one means the loop already has this table in hand.
	first := true
	for _, x := range s.loadWaiters[key] {
		if !x.closed {
			first = false
			break
		}
	}
	s.loadWaiters[key] = append(s.loadWaiters[key], w)
	s.loadWaiterN++
	s.reqMu.Unlock()
	if first {
		// The loop needs one turn per table, not one per load.
		select {
		case s.wake <- struct{}{}:
		default:
		}
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
			s.loadWaiterN--
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

// releaseLoadWaitersLocked lets key's loads go whose files an export has
// just covered, and, when that export did not settle, the ones the loop had
// pulled it for (the next try is a full interval away). From
// noteIngestCovered, wherever the loop is. reqMu is held.
func (s *Scheduler) releaseLoadWaitersLocked(key string, unsettled bool) {
	for _, w := range s.loadWaiters[key] {
		switch {
		case w.closed:
		case s.loadCoveredLocked(key, w.loadAt):
			w.finishLocked(metrics.IcebergLoadFreshCovered)
		case unsettled && w.decided:
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
	return settled && s.appendOK[key] && !s.loadFellBack[key] && s.appendsSince[key] < appendMaxBetweenPasses
}

// serviceLoadWaiters lets the loads whose files are covered go, and decides
// for each table a load waits on whether to pull its request forward. Loop
// goroutine, at the top of each turn (the loop then arms its ingest timer
// on the new due time). It does not ask the gate (gateOpen moves the export
// role's bookkeeping); a node that does not export drops its requests in
// the services that follow, which lets the loads go (dropIngest).
func (s *Scheduler) serviceLoadWaiters() {
	if s.loadInterval <= 0 {
		return
	}
	s.reqMu.Lock()
	if len(s.loadWaiters) == 0 {
		s.reqMu.Unlock()
		return
	}
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
	// Every table looked at here gets an entry; a load that arrives for
	// another table between the two locked sections is decided on the next
	// turn (its wake brings one), not skipped for want of an answer.
	servable := make(map[string]bool, len(undecided))
	paused := len(undecided) > 0 && s.pausedForRestore()
	for _, key := range undecided {
		servable[key] = !paused && s.loadServableSoon(key)
	}

	if loadAfterSnapshot != nil {
		loadAfterSnapshot()
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
			ok, looked := servable[key]
			if w.decided {
				if s.loadUnsettled[key] {
					// The export it waited for did not settle: the next try
					// is a full interval away (requeueIngest).
					w.finishLocked(metrics.IcebergLoadFreshSkipped)
				}
				continue
			}
			if !looked {
				continue // arrived after the snapshot above: the next turn decides
			}
			if !pulled && !s.pullForLoadLocked(key, ok, now, horizon, &dueSoon) {
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
	if !target.Before(r.due) {
		return true // already due as soon as F allows: nothing to pull
	}
	// Pulled earlier: bounded by how many requests the budget's window
	// already holds, and by how many exports loads pulled in the last
	// budget over all tables, so a poller over many tables neither queues
	// every table's export behind its loads nor adds more than
	// ingestConcurrency exports per budget.
	if r.due.After(horizon) && *dueSoon >= s.ingestConcurrency {
		return false
	}
	recent := s.loadPulls[:0]
	for _, t := range s.loadPulls {
		if now.Sub(t) < loadFreshBudget {
			recent = append(recent, t)
		}
	}
	s.loadPulls = recent
	if len(s.loadPulls) >= s.ingestConcurrency {
		return false
	}
	s.loadPulls = append(s.loadPulls, now)
	if r.due.After(horizon) {
		*dueSoon++
	}
	r.due = target
	s.ingestDue[key] = r
	return true
}
