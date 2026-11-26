package ingest

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// A write that asked to wait until its rows are visible (x-arc-wait: visible,
// #1759) carries a visible FlushOutcome. Each batch it buffers is marked at
// the append, under the shard lock, and ends in exactly one terminal state in
// the flush that carries it:
//
//   - settled: its file is stored and, with tiering, registered, and its
//     rows are known (noteOutcomeSettled, after completeFlush);
//   - failed, with or without a WAL copy (noteOutcomeFailure, at every
//     markFlushFailure site, and Close abandoning a queued task);
//   - unconfirmed: the flush succeeded, but its tiering registration failed
//     or the batch's rows are not known.
//
// A flush that panics, or rows Close had no budget for, leave the batch in no
// terminal state: the outcome never resolves, and the waiter answers on its
// budget or at shutdown, never "visible". The waiter decides its verdict from
// its own outcome only: no flush-task registry, no barrier. To shorten the
// wait it may ask for a flush of the keys it wrote (ForceFlushKey, #1759): at
// most once per key per ingest.wait_visible_min_interval_ms, inside the
// forced-flush budget. That takes the key's ledger stripe lock, and only when
// a flush is due, the key's shard lock: read to check that a batch of this
// outcome is still buffered, then write to send. No lock is held while it
// waits.

// visibleOutcome is a visible write's bookkeeping. Counters only grow.
type visibleOutcome struct {
	markedBatches, markedRows   atomic.Int64
	settledBatches, settledRows atomic.Int64
	failedNoWALBatches          atomic.Int64
	failedWALBatches            atomic.Int64
	walRowsFailed               atomic.Int64
	unconfirmedBatches          atomic.Int64
	sealed                      atomic.Bool
	// overResolved latches the first time more batches resolved than were
	// marked: a bug (a batch counted twice), reported once per outcome.
	overResolved atomic.Bool
	// futureFloor is the admission time plus the file-time pruning margin,
	// in µs; 0: the future-stamp check is off. minFuture is the earliest row
	// stamped past it; math.MaxInt64: none.
	futureFloor int64
	minFuture   atomic.Int64
	// changed gets a token after every terminal update; capacity 1, sent
	// without blocking, so an outcome nobody waits on (an error or partial
	// answer) never holds a flush up.
	changed chan struct{}

	// keys are the buffer keys this outcome marked a batch in, each once,
	// in first-mark order: what WaitVisible may force. keySet exists once a
	// second key is seen; a write to one measurement, the common case,
	// pays a compare with the last key. Read after Seal.
	keysMu sync.Mutex
	keys   []string
	keySet map[string]struct{}
}

// noteVisibleKey records that o's write buffered a batch under key. Beside
// countOutcome at both append sites, under the shard lock; a live write or an
// import pays the nil tests.
func noteVisibleKey(o *FlushOutcome, key string) {
	if o == nil || o.vis == nil {
		return
	}
	o.vis.addKey(key)
}

func (v *visibleOutcome) addKey(key string) {
	v.keysMu.Lock()
	defer v.keysMu.Unlock()
	n := len(v.keys)
	if n > 0 && v.keys[n-1] == key {
		return
	}
	if n > 0 {
		if v.keySet == nil {
			v.keySet = make(map[string]struct{}, 4)
			for _, k := range v.keys {
				v.keySet[k] = struct{}{}
			}
		}
		if _, seen := v.keySet[key]; seen {
			return
		}
		v.keySet[key] = struct{}{}
	}
	v.keys = append(v.keys, key)
}

// markedKeys is a copy of the keys o marked a batch in.
func (v *visibleOutcome) markedKeys() []string {
	v.keysMu.Lock()
	defer v.keysMu.Unlock()
	return append([]string(nil), v.keys...)
}

// holdsOutcome is ForceFlushKey's want for o: whether a buffered batch is
// one of o's. Run under the shard read lock, where batch.outcome is stable.
func holdsOutcome(o *FlushOutcome) func(batches []interface{}) bool {
	return func(batches []interface{}) bool {
		for _, record := range batches {
			if batch, ok := record.(*TypedColumnBatch); ok && batch != nil && batch.outcome == o {
				return true
			}
		}
		return false
	}
}

// NewVisibleOutcome is the outcome of a write that waits until its rows are
// visible (#1759), admitted at tAdm. With futureCheck (file-level time pruning
// on, local storage) rows stamped more than margin past tAdm are tracked:
// pruning can hide them until their hour closes. margin is raised to a
// second, as the pruner and SetFutureStampMargin do.
func NewVisibleOutcome(tAdm time.Time, futureCheck bool, margin time.Duration) *FlushOutcome {
	v := &visibleOutcome{changed: make(chan struct{}, 1)}
	v.minFuture.Store(math.MaxInt64)
	if futureCheck {
		v.futureFloor, _ = futureStampWindow(tAdm, normalizeFutureMargin(margin))
	}
	return &FlushOutcome{vis: v}
}

// Seal says no batch will be marked on o any more: the write call under it
// has returned. Until then o is never resolved, since a write appends one
// unit at a time and a flush can settle its first batch before the next is
// marked. Idempotent; a no-op on an import's outcome.
func (o *FlushOutcome) Seal() {
	if o == nil || o.vis == nil {
		return
	}
	o.vis.sealed.Store(true)
	o.vis.signal()
}

// Resolved reports whether o is sealed and every batch marked on it reached
// a terminal state. False for an import's outcome.
func (o *FlushOutcome) Resolved() bool {
	if o == nil || o.vis == nil {
		return false
	}
	v := o.vis
	// Sealed first: once it is set every mark is in. Terminal counters
	// before marked: a batch is marked before any flush can carry it, so
	// this order never reads more resolved than marked by a race.
	if !v.sealed.Load() {
		return false
	}
	return v.resolvedBatches() == v.markedBatches.Load()
}

func (v *visibleOutcome) resolvedBatches() int64 {
	return v.settledBatches.Load() + v.failedNoWALBatches.Load() + v.failedWALBatches.Load() + v.unconfirmedBatches.Load()
}

// mark counts one batch of rows rows, and folds its earliest future-stamped
// row into minFuture. Under the shard lock, immediately before the append.
func (v *visibleOutcome) mark(rows int, minFuture int64) {
	v.markedBatches.Add(1)
	v.markedRows.Add(int64(rows))
	for {
		cur := v.minFuture.Load()
		if minFuture >= cur || v.minFuture.CompareAndSwap(cur, minFuture) {
			return
		}
	}
}

func (v *visibleOutcome) signal() {
	select {
	case v.changed <- struct{}{}:
	default:
	}
}

// visibleChanged follows every terminal update of o: it reports a batch
// counted twice (more resolved than marked), once, and wakes the waiter.
func (b *ArrowBuffer) visibleChanged(o *FlushOutcome) {
	v := o.vis
	resolved := v.resolvedBatches() // before marked: see Resolved
	if marked := v.markedBatches.Load(); resolved > marked && v.overResolved.CompareAndSwap(false, true) {
		metrics.Get().IncIngestWaitVisibleOverResolved()
		b.logger.Error().
			Int64("resolved_batches", resolved).
			Int64("marked_batches", marked).
			Msg("A visible write's outcome resolved more batches than it marked: a batch was counted twice. Its answer is not visible (bug, #1759)")
	}
	v.signal()
}

// visibleMinFuture is the earliest row of batch stamped past o's future
// floor, or math.MaxInt64. Read before the shard lock: no time column is
// scanned under it. A live write pays the nil test.
func visibleMinFuture(o *FlushOutcome, batch *TypedColumnBatch) int64 {
	if o == nil || o.vis == nil || o.vis.futureFloor == 0 {
		return math.MaxInt64
	}
	floor := o.vis.futureFloor
	least := int64(math.MaxInt64)
	times, _ := batch.Data["time"].([]int64)
	for _, t := range times {
		if t > floor && t < least {
			least = t
		}
	}
	return least
}

// normalizeFutureMargin raises a margin under a second to one, as the pruner
// does.
func normalizeFutureMargin(d time.Duration) time.Duration {
	if d < time.Second {
		return time.Second
	}
	return d
}

// futureStampWindow is the window, in µs, a row flushed at now must fall in
// (lo < t < hi) for file-level time pruning to hide it: stamped more than
// margin past now, and still in now's hour. A later hour's row lands in that
// hour's directory, where its file is kept as anomalous. Shared by
// countFutureStamped and the visible verdict, so the two cannot drift.
func futureStampWindow(now time.Time, margin time.Duration) (lo, hi int64) {
	return now.Add(margin).UnixMicro(), now.Truncate(time.Hour).Add(time.Hour).UnixMicro()
}

// VisibleReason says why a visible write's rows are not known to be visible;
// VisibleReasonNone when they are. The values are the X-Arc-Visible-Reason
// header values.
type VisibleReason string

// The reasons, in the verdict's order of precedence.
const (
	VisibleReasonNone VisibleReason = ""
	// VisibleReasonRowsAtRisk: a batch without a WAL copy was in a flush
	// that failed. Its rows may be lost or, after a partial failure, stored.
	VisibleReasonRowsAtRisk VisibleReason = "flush_failed_rows_at_risk"
	// VisibleReasonWALReplay: a batch with a WAL copy was in a flush that
	// failed; recovery replays it.
	VisibleReasonWALReplay VisibleReason = "flush_failed_wal_replay"
	// VisibleReasonShutdown: the server began shutting down first.
	VisibleReasonShutdown VisibleReason = "shutdown"
	// VisibleReasonTimeout: the budget ran out first.
	VisibleReasonTimeout VisibleReason = "timeout"
	// VisibleReasonNotConfirmed: every batch resolved, but one is not
	// known to be visible (a registration failed, rows not known or not
	// adding up).
	VisibleReasonNotConfirmed VisibleReason = "not_confirmed"
	// VisibleReasonStorageNotConfirmable: settled, on a storage backend
	// whose listing is not known to show a new file at once.
	VisibleReasonStorageNotConfirmable VisibleReason = "storage_not_confirmable"
	// VisibleReasonFutureStamped: settled, but a row is stamped far enough
	// ahead that file-level time pruning can hide it until its hour closes.
	VisibleReasonFutureStamped VisibleReason = "future_stamped"
)

// VisibleWait is what WaitVisible needs from the handler.
type VisibleWait struct {
	// WrittenRows is the rows the write call reported written;
	// WrittenRowsKnown false when it could not count them (not visible).
	WrittenRows      int64
	WrittenRowsKnown bool
	// Budget bounds the wait.
	Budget time.Duration
	// Done closes when the server begins shutting down (fasthttp's
	// RequestCtx.Done, read once by the caller). nil never fires.
	Done <-chan struct{}
	// StorageConfirmable: the backend's listing is known to show a new
	// file to the next query. The zero value is the safe one: not visible.
	StorageConfirmable bool
	// Forcer, when set with a positive ForceInterval, shortens the wait:
	// the keys the write marked are flushed early, at most once per
	// ForceInterval per key (ingest.wait_visible_min_interval_ms) and inside
	// the forced-flush budget. nil, or ForceInterval <= 0: never force; the
	// write waits for its rows' age or size flush.
	Forcer        KeyForcer
	ForceInterval time.Duration
}

// VisibleVerdict is a visible write's answer.
type VisibleVerdict struct {
	Visible bool
	Reason  VisibleReason
	// Empty: nothing was written or marked; visible at once.
	Empty bool
	// RowsAtRisk is o.RowsAtRisk() for VisibleReasonRowsAtRisk: rows
	// without a WAL copy in a failed flush (their known rows).
	RowsAtRisk int64
	// Waited is how long WaitVisible waited.
	Waited time.Duration
}

// visibleNow is the verdict's clock; a test sets it.
var visibleNow = time.Now

// WaitVisible seals o, waits until every batch marked on it is resolved, the
// budget runs out, w.Done closes or ctx ends, and returns the verdict. Call
// it after the write call under o returned, and never for a write that
// failed or stopped part-way (their answers do not wait). Holds no lock while
// it waits; a forcing round (ForceFlushKey) takes a key's stripe lock, and
// when that key is due, its shard lock briefly.
func WaitVisible(ctx context.Context, o *FlushOutcome, w VisibleWait) VisibleVerdict {
	start := time.Now()
	if o == nil || o.vis == nil {
		return VisibleVerdict{Reason: VisibleReasonNotConfirmed}
	}
	o.Seal()
	v := o.vis
	// Nothing written and nothing marked: there is nothing a query could
	// miss. Both conditions: rows written but never marked is an outcome
	// lost on the way (not visible); a marked batch of no rows still waits.
	if w.WrittenRowsKnown && w.WrittenRows == 0 && v.markedBatches.Load() == 0 {
		return VisibleVerdict{Visible: true, Empty: true}
	}
	ended := VisibleReasonNone
	if !o.Resolved() {
		timer := time.NewTimer(w.Budget)
		defer timer.Stop()
		var ctxDone <-chan struct{}
		if ctx != nil {
			ctxDone = ctx.Done()
		}
		f := newVisibleForcing(o, w)
		defer f.stop()
		if f != nil {
			// Seal's token: the first round runs below, so a second one
			// back to back would only ask an over-budget key again. The
			// loop re-reads Resolved before every sleep, and a later
			// resolution sends a new token.
			select {
			case <-v.changed:
			default:
			}
		}
		resolved := false // the last wake-up was a batch of o resolving
	wait:
		for !o.Resolved() && !v.overResolved.Load() {
			// A forcing round before each sleep, the first one included:
			// an idle key has no flush coming to wake the wait.
			dueC := f.round(resolved)
			resolved = false
			select {
			case <-v.changed:
				resolved = true
			case <-dueC:
			case <-timer.C:
				ended = VisibleReasonTimeout
				break wait
			case <-w.Done:
				ended = VisibleReasonShutdown
				break wait
			case <-ctxDone:
				ended = VisibleReasonShutdown
				break wait
			}
		}
		if ended != VisibleReasonNone && o.Resolved() {
			ended = VisibleReasonNone // resolved as the wait ended
		}
	}
	verdict := o.visibleVerdict(w, ended, visibleNow())
	verdict.Waited = time.Since(start)
	return verdict
}

// visibleForcing is one wait's forcing state (#1759): the keys still worth
// forcing, and one reusable timer for the earliest not-due key. nil: no
// forcing, and every method is a no-op.
type visibleForcing struct {
	forcer   KeyForcer
	interval time.Duration
	want     func(batches []interface{}) bool
	live     []forceKey
	timer    *time.Timer
}

// forceKey is a key still worth forcing; due is when ForceFlushKey last said
// it becomes due (zero: try at the next round).
type forceKey struct {
	key string
	due time.Time
}

// visibleForceMinDelay is the shortest sleep before a not-due key's retry, so
// a due time already past never makes a hot loop.
const visibleForceMinDelay = 5 * time.Millisecond

func newVisibleForcing(o *FlushOutcome, w VisibleWait) *visibleForcing {
	if w.Forcer == nil || w.ForceInterval <= 0 {
		return nil
	}
	keys := o.vis.markedKeys()
	if len(keys) == 0 {
		return nil
	}
	f := &visibleForcing{forcer: w.Forcer, interval: w.ForceInterval, want: holdsOutcome(o), live: make([]forceKey, len(keys))}
	for i, k := range keys {
		f.live[i].key = k
	}
	return f
}

// round asks once for each live key whose due time has come, and returns the
// channel of the timer armed for the earliest not-due key (nil: none).
//
// Per result: Forced, NothingToForce, LostRace and NotWanted drop the key.
// The wait runs after Seal, so no batch of this write is appended any more,
// and a detach takes a key's whole slice: once this write's batches of a key
// were sent (by this force or by another detach, LostRace's) or are not
// buffered, none will be again. NotDue keeps it until its due time.
// OverBudget keeps it with no timer, and ends the round with the keys after
// it kept un-asked: the budget
// is the node's, so they would be refused too. They are asked at a later
// round (a batch of this write resolved, or a key's due time), never in a
// loop, and otherwise wait for their natural flush. LedgerOff and Closing end
// forcing for this wait.
//
// afterResolution: the round was woken by a batch of this write resolving.
// That batch's flush may be this write's own forced one, and its forced slot
// is released only after the settle that woke the wait (runQueuedFlushTask's
// deferred releaseForcedTask), so an over-budget answer then may be that very
// slot. Such a round arms one retry after visibleForceMinDelay; the retry
// round is not after a resolution and arms none, so the calls stay O(rounds).
func (f *visibleForcing) round(afterResolution bool) <-chan time.Time {
	if f == nil || len(f.live) == 0 {
		return nil
	}
	now := time.Now()
	var next time.Time
	kept := f.live[:0]
keys:
	for i, k := range f.live {
		if !k.due.IsZero() && now.Before(k.due) {
			kept = append(kept, k)
			next = earliest(next, k.due)
			continue
		}
		rep := f.forcer.ForceFlushKey(k.key, f.interval, ForceWrite, f.want)
		switch rep.Result {
		case NotDue:
			k.due = rep.Due
			kept = append(kept, k)
			next = earliest(next, rep.Due)
		case OverBudget:
			k.due = time.Time{}
			kept = append(kept, k)
			for _, rest := range f.live[i+1:] {
				kept = append(kept, rest)
				if !rest.due.IsZero() && now.Before(rest.due) {
					next = earliest(next, rest.due)
				}
			}
			if afterResolution {
				next = earliest(next, now.Add(visibleForceMinDelay))
			}
			break keys
		case LedgerOff, Closing:
			f.live = nil
			return nil
		case Forced, NothingToForce, LostRace, NotWanted:
			// Dropped: not appended to kept.
		}
	}
	f.live = kept
	if next.IsZero() {
		return nil
	}
	d := time.Until(next)
	if d < visibleForceMinDelay {
		d = visibleForceMinDelay
	}
	if f.timer == nil {
		f.timer = time.NewTimer(d)
	} else {
		f.timer.Reset(d)
	}
	return f.timer.C
}

func (f *visibleForcing) stop() {
	if f != nil && f.timer != nil {
		f.timer.Stop()
	}
}

func earliest(a, b time.Time) time.Time {
	if a.IsZero() || b.Before(a) {
		return b
	}
	return a
}

// visibleVerdict decides, in order of precedence: a failure without a WAL
// copy; one with a copy; not resolved (ended); not confirmed; storage; rows
// stamped in the future; visible.
func (o *FlushOutcome) visibleVerdict(w VisibleWait, ended VisibleReason, now time.Time) VisibleVerdict {
	v := o.vis
	switch {
	case v.failedNoWALBatches.Load() > 0:
		return VisibleVerdict{Reason: VisibleReasonRowsAtRisk, RowsAtRisk: o.RowsAtRisk()}
	case v.failedWALBatches.Load() > 0:
		return VisibleVerdict{Reason: VisibleReasonWALReplay}
	case v.overResolved.Load():
		// A batch counted twice (a bug, logged): never visible, and no
		// reason to wait out the budget for it.
		return VisibleVerdict{Reason: VisibleReasonNotConfirmed}
	case ended != VisibleReasonNone || !o.Resolved():
		if ended == VisibleReasonNone {
			ended = VisibleReasonTimeout
		}
		return VisibleVerdict{Reason: ended}
	}
	marked := v.markedRows.Load()
	if v.unconfirmedBatches.Load() > 0 || v.overResolved.Load() ||
		!w.WrittenRowsKnown || marked != w.WrittenRows || v.settledRows.Load() != marked {
		return VisibleVerdict{Reason: VisibleReasonNotConfirmed}
	}
	if !w.StorageConfirmable {
		return VisibleVerdict{Reason: VisibleReasonStorageNotConfirmable}
	}
	if v.futureFloor != 0 {
		if _, hourEnd := futureStampWindow(now, 0); v.minFuture.Load() < hourEnd {
			return VisibleVerdict{Reason: VisibleReasonFutureStamped}
		}
	}
	return VisibleVerdict{Visible: true}
}
