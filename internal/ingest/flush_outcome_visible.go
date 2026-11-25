package ingest

import (
	"context"
	"math"
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
// budget or at shutdown, never "visible". The waiter reads only its own
// outcome: no shard lock, no flush-task registry, no barrier.

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
// failed or stopped part-way (their answers do not wait). Takes no lock.
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
	wait:
		for !o.Resolved() && !v.overResolved.Load() {
			select {
			case <-v.changed:
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
