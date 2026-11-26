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
// (x-arc-freshness: flush, #1760, with query.fresh_flush_enabled). Both share
// the one ledger and its budget. With the ledger off, every detach and
// deferral site pays one nil test.
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
	// NothingToForce: nothing is buffered for the key, and its last detach
	// is the one the due check saw (or was pruned since, which is no detach).
	NothingToForce
	// NotWanted: the key is buffered, but want returned false.
	NotWanted
	// Closing: the buffer is shutting down.
	Closing
	// LedgerOff: forcing is off, because the ledger was never enabled or the
	// interval is not positive. The caller waits for the natural flush.
	LedgerOff
	// LostRace: another detach of the key came after the due check, seen
	// where the key was found empty or at the re-check before the send.
	// Another flush took the rows; new rows may be buffered again, and are
	// left to their own flush. (A detach followed by rows want rejects still
	// answers NotWanted.)
	LostRace
)

// ForceReply is ForceFlushKey's answer.
//
// TaskID identifies a task but is not one to wait on alone: "every row
// acknowledged before X is stored" is the task tracker's job
// (WaitKeysFlushed joins the first detach after a waiter registered), and task
// ids are not ordered by completion.
type ForceReply struct {
	Result ForceResult
	// Due is set on NotDue only: when the key becomes due.
	Due time.Time
	// TaskID is set on Forced and LostRace only, and zero otherwise.
	// Forced: the task this call's send created. A detach between a caller's
	// own snapshot and the due check normally answers NotDue, but answers
	// Forced when that gap is at least the interval, so it is not
	// necessarily the first detach after the caller's snapshot, and rows may
	// have been buffered again since.
	// LostRace: the key's newest detach at the check; at least one detach
	// came after the due check, and the one that took the caller's rows may
	// be an earlier one (the ledger keeps only the last).
	TaskID uint64
}

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
	case LostRace:
		return "lost_race"
	}
	return "unknown"
}

// KeyForcer is what a visible write's wait needs of the buffer to shorten
// itself (#1759): ArrowBuffer.ForceFlushKey.
type KeyForcer interface {
	ForceFlushKey(key string, interval time.Duration, src ForceSource,
		want func(batches []interface{}) bool) ForceReply
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
	// lastDeferredMono is when the last deferral (markDeferredLocked)
	// happened, as sinceCreated() in nanoseconds plus 1; 0 = none since the
	// ledger was enabled (#1943).
	lastDeferredMono atomic.Int64
	// created is when the ledger was built, read from now before the ledger
	// is published. With time.Now it carries Go's monotonic reading, so
	// sinceCreated is immune to wall-clock steps.
	created time.Time

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
		// Never .UTC(), .Round, .Truncate or a time.Unix round trip: each
		// drops the monotonic reading, and sinceCreated falls back to the
		// wall clock.
		created: now(),
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

// sinceCreated is the ledger's age on its own clock: monotonic in production,
// where created and now() both carry Go's monotonic reading. Not
// time.Since(l.created): that bypasses an injected clock.
func (l *keyFlushLedger) sinceCreated() time.Duration {
	return l.now().Sub(l.created)
}

// noteDeferral stamps a deferral. A plain store, so of two concurrent
// deferrals the later store wins even if its stamp is older; the window then
// ends that read-to-store gap early. The age is clamped at 0: only a test
// clock set below created makes it negative, and at -1 ns the stamp would be
// 0, "none".
func (l *keyFlushLedger) noteDeferral() {
	l.lastDeferredMono.Store(int64(max(l.sinceCreated(), 0)) + 1)
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
// is an atomic or channel-length read, plus a clock read after a deferral.
// It is advisory; the send's lost-slot branch is the backstop. Force only
// while ALL hold:
//  1. fewer than max(1, ⌊cap/2⌋) tasks are queued (at capacity 1, only an
//     empty queue);
//  2. no buffer was deferred within the last max_buffer_age_ms;
//  3. buffered bytes are under half the resolved max_buffered_bytes (or the
//     limit is off);
//  4. fewer than maxInFlight forced tasks hold a slot.
func (b *ArrowBuffer) reserveForcedSlot(l *keyFlushLedger) bool {
	if len(b.flushQueue) >= max(1, cap(b.flushQueue)/2) {
		return false
	}
	// The stamp is loaded first and the clock read after it, both on the
	// ledger's monotonic base (#1943), so a deferral this load sees is never
	// later than the read. A negative distance, possible only on a test clock
	// stepped back, refuses rather than counting as expired. A deferral
	// stored after the load is not seen; the lost-slot branch covers it.
	if last := l.lastDeferredMono.Load(); last != 0 {
		// The window is max_buffer_age_ms with a 1 s floor. The age is
		// never validated: at <= 0 (every sweep then flushes every buffer)
		// a zero window would silently turn this condition off, and ages of
		// 1-999 ms are lifted to 1 s as well.
		window := max(b.maxBufferAge, time.Second)
		if l.sinceCreated()-time.Duration(last-1) < window {
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
	// forceAfterDueCheckHook runs in ForceFlushKey after the due and budget
	// checks and before the want scan, with no lock held: a detach in it is
	// seen at the scan (want non-nil) or under the write lock.
	forceAfterDueCheckHook func(key string)
	// forceAfterScanHook runs in ForceFlushKey after the want scan and before
	// the write lock, with no lock held: a detach in it is seen under the
	// write lock (empty key, or the re-check), never by the scan.
	forceAfterScanHook func(key string)
	// forceBeforeSendHook runs in ForceFlushKey under the shard write lock,
	// right before the send.
	forceBeforeSendHook func(key string)
)

// ForceFlushKey flushes key ("database/measurement") now if it is due (not
// detached by anything within interval), the budget allows, something is
// buffered, and want (nil: anything buffered) accepts the buffered batches.
// The reply's Due and TaskID are set as ForceReply says.
//
// It never blocks on I/O and never marks a key deferred. Over budget, the key
// is left to its natural flush. Every result but LedgerOff is counted in
// arc_ingest_forced_flushes_total{source=src}.
//
// Locks, in order: the key's stripe alone (due check); none (budget); the
// shard read lock (want, and under it the stripe when the key is empty); the
// shard write lock, and under it the stripe (the empty-key check, the
// re-check and the send). The caller must hold no shard or stripe lock: a
// held stripe deadlocks the due check, and a held shard lock, read or write,
// deadlocks the write lock or the scan (a recursive RLock waits behind a
// waiting writer). want runs under the shard read lock and must not take
// shard or stripe locks, nor keep the slice it is given: that slice goes to
// the flush task, and the flush nils out its entries.
func (b *ArrowBuffer) ForceFlushKey(key string, interval time.Duration, src ForceSource,
	want func(batches []interface{}) bool) ForceReply {
	rep := b.forceFlushKey(key, interval, want)
	if rep.Result == Forced {
		// As the write path does after its send (#1944), outside the shard
		// lock. The other results did not change the queue.
		metrics.Get().SetBufferQueueDepth(b.queueDepth.Load())
	}
	if rep.Result != LedgerOff {
		metrics.Get().IncIngestForcedFlush(forcedFlushSourceLabel(src), forcedFlushResultLabel(rep.Result))
	}
	return rep
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
	case LostRace:
		return metrics.ForcedFlushLostRace
	}
	return metrics.ForcedFlushNothing
}

func (b *ArrowBuffer) forceFlushKey(key string, interval time.Duration,
	want func(batches []interface{}) bool) ForceReply {
	l := b.ledger.Load()
	if l == nil || interval <= 0 {
		return ForceReply{Result: LedgerOff}
	}
	if b.closing.Load() {
		return ForceReply{Result: Closing}
	}

	// Due check, under the stripe lock alone. Most calls for a busy key end
	// here, without touching the shard.
	now := l.now()
	seen, _ := l.lastDetach(key) // zero id when never detached
	if !seen.at.IsZero() {
		if due := seen.at.Add(interval); now.Before(due) {
			return ForceReply{Result: NotDue, Due: due}
		}
	}

	// Budget, before any lock. It reads the clock itself, after loading the
	// deferral stamp: now, read above, may predate the newest deferral.
	if !b.reserveForcedSlot(l) {
		return ForceReply{Result: OverBudget}
	}
	// The slot is ours until the send succeeds, and given back on every other
	// exit, a panic in want included.
	reserved := true
	defer func() {
		if reserved {
			l.inFlight.Add(-1)
		}
	}()
	if forceAfterDueCheckHook != nil {
		forceAfterDueCheckHook(key)
	}

	// A detach since the due check recorded a new task id (under a shard.mu
	// hold, which each check below holds too). It is looked for at three
	// sites: the key found empty by the scan, found empty under the write
	// lock, and the re-check before the send. A changed id there answers
	// LostRace with that id; an unchanged one, NothingToForce at the first
	// two sites and the send at the third.
	//
	// An entry gone since the due check was pruned, not detached: a detach
	// always writes the entry, and pruning deletes only entries older than
	// retain. It counts as unchanged (hence ok && at each site). Were it a
	// detach and then a prune of that detach, the detach was over retain ago,
	// so the key is due anyway and the batches want saw went with it; an
	// empty key is NothingToForce, and the send flushes what is left.
	shard := b.getShard(key)
	if want != nil {
		buffered, wanted, cur, ok := scanWanted(shard, key, want, l)
		if !buffered {
			if ok && cur.id != seen.id {
				return ForceReply{Result: LostRace, TaskID: cur.id}
			}
			return ForceReply{Result: NothingToForce}
		}
		if !wanted {
			return ForceReply{Result: NotWanted}
		}
	}
	if forceAfterScanHook != nil {
		forceAfterScanHook(key)
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()
	if b.closing.Load() {
		return ForceReply{Result: Closing}
	}
	// len, not presence: an existing but empty slice would flush nothing and
	// fail with "no batches to merge", setting the flush-failure latch.
	if len(shard.buffers[key]) == 0 {
		if cur, ok := l.lastDetach(key); ok && cur.id != seen.id {
			return ForceReply{Result: LostRace, TaskID: cur.id}
		}
		return ForceReply{Result: NothingToForce}
	}
	// O(1) re-check. Unchanged means no detach, so the key is still due and,
	// since a detach takes the whole slice and appends never remove batches,
	// every batch want saw is still buffered. It also stops two concurrent
	// callers from both sending. Both detach sites load the ledger inside
	// their shard.mu hold, so even a detach racing EnableKeyFlushLedger
	// either ended before want's scan or was recorded.
	if cur, ok := l.lastDetach(key); ok && cur.id != seen.id {
		return ForceReply{Result: LostRace, TaskID: cur.id}
	}
	if forceBeforeSendHook != nil {
		forceBeforeSendHook(key)
	}
	database, measurement := splitDatabaseAndMeasurement(key)
	switch res, id := b.sendFlushLocked(shard, key, database, measurement, shard.bufferRecordCounts[key], true); res {
	case flushSent:
		reserved = false // the task holds the slot now
		return ForceReply{Result: Forced, TaskID: id}
	case flushSendClosing:
		return ForceReply{Result: Closing}
	default:
		// The queue filled after the budget check. Not deferred: the key
		// waits for its natural flush.
		return ForceReply{Result: OverBudget}
	}
}

// scanWanted reports, under shard's read lock, whether key has buffered
// batches and whether want accepts them. With none buffered it returns the
// key's last detach (lastDetach), read under the same lock: no detach runs
// while it is held, so the id is the one the empty slice reflects. The unlock
// is deferred so a panic in want cannot leave the shard locked.
func scanWanted(shard *bufferShard, key string, want func([]interface{}) bool,
	l *keyFlushLedger) (buffered, wanted bool, cur keyDetach, curOK bool) {
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	batches := shard.buffers[key]
	if len(batches) == 0 {
		cur, curOK = l.lastDetach(key)
		return false, false, cur, curOK
	}
	return true, want(batches), keyDetach{}, false
}
