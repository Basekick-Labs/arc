package ingest

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// The per-key flush ledger and the force primitive (#1760, shared with #1759).
//
// ForceFlushKey asks for one key's buffer to be flushed now instead of at its
// age or size trigger, at most once per interval per key, and only while the
// flush pipeline has spare room. It is the primitive behind a visible write
// that shortens its own wait (x-arc-wait: visible, #1759, which enables the
// ledger with ingest.wait_visible_enabled) and a fresh query
// (x-arc-freshness: flush, #1760, not wired yet). With the ledger off, every
// detach and deferral site pays one nil test.
//
// Lock order: shard.mu → stripe.mu. stripe.mu is a leaf; nothing is called
// while holding it except map operations.

// ForceSource says who asked for a forced flush: the source label of
// arc_ingest_forced_flushes_total.
type ForceSource uint8

const (
	// ForceRead: a fresh query (#1760).
	ForceRead ForceSource = iota + 1
	// ForceWrite: a visible write (#1759).
	ForceWrite
)

// ForceResult is what ForceFlushKey did.
type ForceResult uint8

const (
	// Forced: the key's buffer was handed to a flush worker.
	Forced ForceResult = iota + 1
	// NotDue: the key was detached less than the interval ago; the returned
	// time is when it becomes due.
	NotDue
	// OverBudget: the pipeline has no spare room (forcedFlushBudget), or the
	// queue filled before the send. Nothing was marked deferred: the key
	// waits for its natural flush. The caller must not retry in a loop.
	OverBudget
	// NothingToForce: nothing is buffered for the key, or another detach
	// took it after the due check.
	NothingToForce
	// NotWanted: the key is buffered, but want returned false.
	NotWanted
	// Closing: the buffer is shutting down.
	Closing
	// LedgerOff: forcing is off, because the ledger was never enabled or the
	// interval is not positive. The caller waits for the natural flush.
	LedgerOff
)

func (r ForceResult) String() string {
	switch r {
	case Forced:
		return "forced"
	case NotDue:
		return "not_due"
	case OverBudget:
		return "over_budget"
	case NothingToForce:
		return "nothing"
	case NotWanted:
		return "not_wanted"
	case Closing:
		return "closing"
	case LedgerOff:
		return "ledger_off"
	}
	return "unknown"
}

// KeyForcer is what a visible write's wait needs of the buffer to shorten
// itself (#1759): ArrowBuffer.ForceFlushKey.
type KeyForcer interface {
	ForceFlushKey(key string, interval time.Duration, src ForceSource,
		want func(batches []interface{}) bool) (ForceResult, time.Time)
}

// MaxForceInterval is the longest interval forcing is meant for: the ceiling
// of ingest.wait_visible_min_interval_ms (config.MaxWaitVisibleMinIntervalMS)
// and the ledger's default retention.
const MaxForceInterval = time.Hour

// ForceBudget configures the ledger. The zero value is the default:
// max(1, flush_workers/4) forced tasks in flight, last detaches kept an hour.
type ForceBudget struct {
	// MaxInFlight caps the forced tasks queued or being flushed at once.
	// 0 means max(1, flush_workers/4).
	MaxInFlight int
	// Retain is how long a key's last detach is kept after it happened;
	// 0 means MaxForceInterval. It must be at least every interval passed
	// to ForceFlushKey: an entry pruned sooner makes its key due sooner.
	Retain time.Duration
}

const ledgerStripeCount = 64

type keyFlushLedger struct {
	stripes [ledgerStripeCount]ledgerStripe

	// maxInFlight is the resolved ForceBudget.MaxInFlight.
	maxInFlight int64
	// inFlight counts forced-task slots held: reserved by ForceFlushKey,
	// released by ForceFlushKey itself unless the send succeeded, otherwise
	// by whoever takes the task off the queue (releaseForcedTask).
	inFlight atomic.Int64
	// lastDeferredNano is the time of the last deferral (markDeferredLocked),
	// in now()'s UnixNano; 0 = none since the ledger was enabled.
	lastDeferredNano atomic.Int64

	// retain is the resolved ForceBudget.Retain; pruneEvery is how often
	// maybePrune walks the stripes, and lastPrune when it last did. Only
	// periodicFlush's goroutine calls maybePrune, so lastPrune needs no lock.
	retain, pruneEvery time.Duration
	lastPrune          time.Time

	// now is time.Now; tests inject a clock (enableKeyFlushLedger).
	now func() time.Time
}

type ledgerStripe struct {
	mu sync.Mutex
	// keys holds each key's last detach, pruned retain after it
	// (maybePrune), so the map is bounded by the keys detached within the
	// last retain, not by every key ever written.
	keys map[string]keyDetach
}

// keyDetach is a key's last detach: when, and the async-flush task id that
// carried it. The id is global and never repeats (beginAsyncFlushTask), so an
// unchanged id means no detach happened in between.
type keyDetach struct {
	at time.Time
	id uint64
}

// ledgerPruneEvery is how often the ledger is pruned at most (with a
// shorter retention, every retention).
const ledgerPruneEvery = time.Minute

// EnableKeyFlushLedger turns forcing on. Callers enable it once, right after
// NewArrowBuffer and before startup recovery, so the limiter sees every
// detach; a detach before it is not recorded, which only makes that key due
// sooner. Later calls are no-ops.
func (b *ArrowBuffer) EnableKeyFlushLedger(budget ForceBudget) {
	b.enableKeyFlushLedger(budget, time.Now)
}

// enableKeyFlushLedger is EnableKeyFlushLedger with the ledger's clock, set
// before the ledger is published: periodicFlush may prune it at once.
func (b *ArrowBuffer) enableKeyFlushLedger(budget ForceBudget, now func() time.Time) {
	maxInFlight := budget.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = max(1, b.flushWorkers/4)
	}
	retain := budget.Retain
	if retain <= 0 {
		retain = MaxForceInterval
	}
	l := &keyFlushLedger{
		maxInFlight: int64(maxInFlight),
		retain:      retain,
		pruneEvery:  min(retain, ledgerPruneEvery),
		now:         now,
	}
	for i := range l.stripes {
		l.stripes[i].keys = make(map[string]keyDetach)
	}
	b.ledger.CompareAndSwap(nil, l)
}

func (l *keyFlushLedger) stripe(key string) *ledgerStripe {
	// FNV-1a, as getShard.
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return &l.stripes[hash%ledgerStripeCount]
}

// recordDetach records that key's buffer left its shard in task id. Called
// under that shard's mu (D1 sendFlushLocked, D2 extractBufferLocked). The map
// holds values, so a key already known costs no allocation.
func (l *keyFlushLedger) recordDetach(key string, id uint64) {
	now := l.now()
	s := l.stripe(key)
	s.mu.Lock()
	s.keys[key] = keyDetach{at: now, id: id}
	s.mu.Unlock()
}

func (l *keyFlushLedger) lastDetach(key string) (keyDetach, bool) {
	s := l.stripe(key)
	s.mu.Lock()
	d, ok := s.keys[key]
	s.mu.Unlock()
	return d, ok
}

// maybePrune deletes the last detaches older than retain, at most once per
// pruneEvery. periodicFlush calls it after flushAgedBuffers, so it runs
// outside every shard hold; it takes each stripe's lock alone, one at a time.
// A pruned key is due again, which it already was: its detach is older than
// any interval (retain is at least each). Returns the entries deleted.
func (l *keyFlushLedger) maybePrune() int {
	now := l.now()
	if !l.lastPrune.IsZero() && now.Sub(l.lastPrune) < l.pruneEvery {
		return 0
	}
	l.lastPrune = now
	cutoff := now.Add(-l.retain)
	deleted := 0
	for i := range l.stripes {
		s := &l.stripes[i]
		s.mu.Lock()
		for key, d := range s.keys {
			if d.at.Before(cutoff) {
				delete(s.keys, key)
				deleted++
			}
		}
		s.mu.Unlock()
	}
	return deleted
}

func (l *keyFlushLedger) noteDeferral() {
	l.lastDeferredNano.Store(l.now().UnixNano())
}

// releaseForcedTask frees the forced-task slot a queued task holds. Every site
// that takes a task off the queue calls it in a defer (the worker,
// runQueuedFlushTask; Close's drain, drainAndFlushQueue), so no exit leaks the
// slot: a leaked slot silently disables forcing for the rest of the process.
func (b *ArrowBuffer) releaseForcedTask(task flushTask) {
	if !task.forced {
		return
	}
	// task.forced is set only by ForceFlushKey, which requires the ledger,
	// and the ledger is never unset.
	b.ledger.Load().inFlight.Add(-1)
}

// reserveForcedSlot checks the forced-flush budget and, when it allows,
// reserves one forced-task slot. Called with no lock held: every condition
// is an atomic or channel-length read. It is advisory; the send's lost-slot
// branch is the backstop. Force only while ALL hold:
//  1. the flush queue is under half full (only when empty at capacity 1);
//  2. no buffer was deferred within the last max_buffer_age_ms;
//  3. buffered bytes are under half the resolved max_buffered_bytes (or the
//     limit is off);
//  4. fewer than maxInFlight forced tasks hold a slot.
func (b *ArrowBuffer) reserveForcedSlot(l *keyFlushLedger, now time.Time) bool {
	if len(b.flushQueue) >= max(1, cap(b.flushQueue)/2) {
		return false
	}
	// Wall-clock nanoseconds: a clock stepped back gives a negative distance,
	// which counts as expired rather than as a deferral that never ends.
	if last := l.lastDeferredNano.Load(); last != 0 {
		// max_buffer_age_ms <= 0 (never validated: every sweep then flushes
		// every buffer) would leave a zero window and silently turn this
		// condition off; it gets a 1 s floor instead.
		window := max(b.maxBufferAge, time.Second)
		if d := now.UnixNano() - last; d >= 0 && d < int64(window) {
			return false
		}
	}
	if limit := b.maxBufferedBytes; limit > 0 && b.bufferBytesBuffered.Load() >= limit/2 {
		return false
	}
	if l.inFlight.Add(1) > l.maxInFlight {
		l.inFlight.Add(-1)
		return false
	}
	return true
}

// Test seams, nil in production.
var (
	// forceAfterScanHook runs in ForceFlushKey after the want scan and before
	// the write lock, with no lock held.
	forceAfterScanHook func(key string)
	// forceBeforeSendHook runs in ForceFlushKey under the shard write lock,
	// right before the send.
	forceBeforeSendHook func(key string)
)

// ForceFlushKey flushes key ("database/measurement") now if it is due (not
// detached by anything within interval), the budget allows, something is
// buffered, and want (nil: anything buffered) accepts the buffered batches.
// For NotDue the time is when the key becomes due; otherwise it is zero.
//
// It never blocks on I/O and never marks a key deferred. Over budget, the key
// is left to its natural flush. Every result but LedgerOff is counted in
// arc_ingest_forced_flushes_total{source=src}.
//
// Locks, in order: the key's stripe alone (due check); none (budget); the
// shard read lock (want); the shard write lock, and under it the stripe (the
// re-check and the send). want runs under the shard read lock and must not
// take shard or stripe locks.
func (b *ArrowBuffer) ForceFlushKey(key string, interval time.Duration, src ForceSource,
	want func(batches []interface{}) bool) (ForceResult, time.Time) {
	r, due := b.forceFlushKey(key, interval, want)
	if r != LedgerOff {
		metrics.Get().IncIngestForcedFlush(forcedFlushSourceLabel(src), forcedFlushResultLabel(r))
	}
	return r, due
}

// forcedFlushSourceLabel and forcedFlushResultLabel map to the metric's
// fixed label set.
func forcedFlushSourceLabel(src ForceSource) int {
	if src == ForceWrite {
		return metrics.ForcedFlushSourceWrite
	}
	return metrics.ForcedFlushSourceRead
}

func forcedFlushResultLabel(r ForceResult) int {
	switch r {
	case Forced:
		return metrics.ForcedFlushForced
	case NotDue:
		return metrics.ForcedFlushNotDue
	case OverBudget:
		return metrics.ForcedFlushOverBudget
	case NotWanted:
		return metrics.ForcedFlushNotWanted
	case Closing:
		return metrics.ForcedFlushClosing
	}
	return metrics.ForcedFlushNothing
}

func (b *ArrowBuffer) forceFlushKey(key string, interval time.Duration,
	want func(batches []interface{}) bool) (ForceResult, time.Time) {
	l := b.ledger.Load()
	if l == nil || interval <= 0 {
		return LedgerOff, time.Time{}
	}
	if b.closing.Load() {
		return Closing, time.Time{}
	}

	// Due check, under the stripe lock alone. Most calls for a busy key end
	// here, without touching the shard.
	now := l.now()
	seen, _ := l.lastDetach(key) // zero id when never detached
	if !seen.at.IsZero() {
		if due := seen.at.Add(interval); now.Before(due) {
			return NotDue, due
		}
	}

	// Budget, before any lock.
	if !b.reserveForcedSlot(l, now) {
		return OverBudget, time.Time{}
	}
	// The slot is ours until the send succeeds, and given back on every other
	// exit, a panic in want included.
	reserved := true
	defer func() {
		if reserved {
			l.inFlight.Add(-1)
		}
	}()

	shard := b.getShard(key)
	if want != nil {
		buffered, wanted := scanWanted(shard, key, want)
		if !buffered {
			return NothingToForce, time.Time{}
		}
		if !wanted {
			return NotWanted, time.Time{}
		}
	}
	if forceAfterScanHook != nil {
		forceAfterScanHook(key)
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()
	if b.closing.Load() {
		return Closing, time.Time{}
	}
	// len, not presence: an existing but empty slice would flush nothing and
	// fail with "no batches to merge", setting the flush-failure latch.
	if len(shard.buffers[key]) == 0 {
		return NothingToForce, time.Time{}
	}
	// O(1) re-check: a detach since the due check recorded a new task id
	// (under a shard.mu hold, which this one now excludes). Unchanged means
	// no detach, so the key is still due and, since a detach takes the whole
	// slice and appends never remove batches, every batch want saw is still
	// buffered. It also stops two concurrent callers from both sending.
	//
	// An entry gone since the due check was pruned, not detached: a detach
	// always writes the entry, and pruning deletes only entries older than
	// retain. It counts as unchanged. Were it a detach and then a prune of
	// that detach, the detach was over retain ago, so the key is due anyway
	// and the batches want saw went with it; the send flushes what is left.
	if cur, ok := l.lastDetach(key); ok && cur.id != seen.id {
		return NothingToForce, time.Time{}
	}
	if forceBeforeSendHook != nil {
		forceBeforeSendHook(key)
	}
	database, measurement := splitDatabaseAndMeasurement(key)
	switch b.sendFlushLocked(shard, key, database, measurement, shard.bufferRecordCounts[key], true) {
	case flushSent:
		reserved = false // the task holds the slot now
		return Forced, time.Time{}
	case flushSendClosing:
		return Closing, time.Time{}
	default:
		// The queue filled after the budget check. Not deferred: the key
		// waits for its natural flush.
		return OverBudget, time.Time{}
	}
}

// scanWanted reports, under shard's read lock, whether key has buffered
// batches and whether want accepts them. The unlock is deferred so a panic in
// want cannot leave the shard locked.
func scanWanted(shard *bufferShard, key string, want func([]interface{}) bool) (buffered, wanted bool) {
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	batches := shard.buffers[key]
	if len(batches) == 0 {
		return false, false
	}
	return true, want(batches)
}
