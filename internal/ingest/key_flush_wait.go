package ingest

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Per-task tracking and the reader waiter (#1760 (a2)).
//
// WaitKeysFlushed returns once every row of the given keys that was
// acknowledged before the call is in a flush that ended settled (stored, and
// registered where registration applies), or with the reason it cannot say
// so. It is the ingest half of a fresh query (x-arc-freshness: flush,
// internal/api/fresh_flush.go); main enables tracking with
// query.fresh_flush_enabled, before startup WAL recovery.
//
// Tracking has its own switch, EnableKeyFlushTracking, separate from the
// ledger: the ledger alone (forcing, #1759's visible writes) never pays for
// it. With tracking off, every detach and resolution site pays one nil test.
//
// The argument (plan v2.2 §6.4): rows leave a shard's buffer only at a detach
// (D1 sendFlushLocked, D2 extractBufferLocked), both under shard.mu, and both
// record the task here under the key's tracking stripe before the task is
// handed to whoever resolves it. The waiter's snapshot takes the shard read
// lock and then the stripe, so at the snapshot every acknowledged row of the
// key is either buffered (the next detach joins the waiter), in a recorded
// task that has not resolved (the waiter needs it), or in a task that already
// resolved: settled, or failed. A failed flush sets the latch before its task
// completes, and the latch is read after the snapshot. The one failure that
// sets no latch is Close's drain abandoning a queued task (R4), which happens
// only after closing is set, so the wait re-reads closing after the snapshot.
// A task that stored a file but failed to register it (tiering) resolves
// unconfirmed and sets no latch either: the wait does not cover tiering.
//
// Lock order: shard.mu → track stripe. A track stripe is a leaf: under it
// only map, slice and non-blocking channel operations run, plus the flush
// queue's non-blocking send at D1. No ledger stripe is taken under a track
// stripe, nor a track stripe under a ledger stripe.

// ErrLedgerOff: the ledger or task tracking is not enabled, so no fresh wait
// is possible on this buffer.
var ErrLedgerOff = errors.New("fresh flush unavailable: key flush ledger or task tracking not enabled")

// ErrKeyFlushFailed: a flush the wait depended on failed.
var ErrKeyFlushFailed = errors.New("a flush of a waited key failed")

// ErrKeyFlushUnconfirmed: a flush the wait depended on ended without a
// confirmed store (a file it wrote was not registered).
var ErrKeyFlushUnconfirmed = errors.New("a flush of a waited key ended without a confirmed store")

// Reasons of a RecoveryPendingError.
const (
	// RecoveryReasonFlushFailure: a flush failed; its rows wait for WAL recovery.
	RecoveryReasonFlushFailure = "flush_failure"
	// RecoveryReasonRecoveryPending: a WAL recovery pass left work behind.
	RecoveryReasonRecoveryPending = "recovery_pending"
)

// RecoveryPendingError: the flush-failure latch is set (#1938) on a buffer
// with a WAL, so rows acknowledged before the wait may be waiting for WAL
// recovery rather than in storage.
type RecoveryPendingError struct {
	Reason string // RecoveryReasonFlushFailure or RecoveryReasonRecoveryPending
}

func (e *RecoveryPendingError) Error() string {
	return "rows may be waiting for WAL recovery: " + e.Reason
}

// taskOutcome is how a tracked flush task ended.
type taskOutcome uint8

const (
	outcomeSettled taskOutcome = iota + 1
	outcomeFailed
	outcomeUnconfirmed
)

// taskResult collects one task's marks. A coverage split flushes one task as
// several parts, one after another on the same goroutine, so plain bools
// suffice; every part marks it. The methods take a nil receiver: with
// tracking off the flush paths carry nil and each mark is one nil test.
type taskResult struct {
	failed, settled, unconfirmed bool
}

// fail marks a failed part. Called next to markFlushFailure.
func (r *taskResult) fail() {
	if r != nil {
		r.failed = true
	}
}

// settle marks a stored part. Called next to noteOutcomeSettled; registered
// is false when a file it wrote was not registered (tiering).
func (r *taskResult) settle(registered bool) {
	if r == nil {
		return
	}
	if registered {
		r.settled = true
	} else {
		r.unconfirmed = true
	}
}

// outcome classifies the marks: any failure wins; otherwise settled only if
// some part settled and none was unconfirmed; anything else (no mark at all
// included) is unconfirmed, never settled.
func (r *taskResult) outcome() taskOutcome {
	switch {
	case r.failed:
		return outcomeFailed
	case r.settled && !r.unconfirmed:
		return outcomeSettled
	}
	return outcomeUnconfirmed
}

type keyTaskTracker struct {
	stripes [ledgerStripeCount]trackStripe
}

type trackStripe struct {
	mu   sync.Mutex
	keys map[string]*trackedKey
}

// trackedKey exists only while it has a task in flight or a waiter: the last
// resolution or deregistration deletes it, so the map needs no pruning.
type trackedKey struct {
	inflight []uint64 // tasks detached and recorded, not yet resolved
	waiters  []*keyWaiter
	// inflightBuf backs inflight's first task, so a detach of an idle key
	// allocates the entry and nothing else.
	inflightBuf [1]uint64
}

// keyWaiter is one WaitKeysFlushed call's interest in one key. Every field
// but wake and joinWanted is guarded by the key's track stripe.
type keyWaiter struct {
	wake       chan struct{} // the call's; shared by its keys
	joinWanted bool          // the key was buffered at the snapshot
	joined     uint64        // the first detach after the snapshot; 0 = none yet
	need       []uint64      // tasks to resolve before the key is done
	bad        taskOutcome   // the first non-settled outcome; 0 = none
}

// EnableKeyFlushTracking turns per-task tracking on, for WaitKeysFlushed.
// Callers enable it once, before startup recovery, like the ledger. A task
// detached before it is not tracked, so a wait can answer without waiting
// for it: enabling it after rows were acknowledged would break the
// guarantee for those rows. Later calls are no-ops.
func (b *ArrowBuffer) EnableKeyFlushTracking() {
	t := &keyTaskTracker{}
	for i := range t.stripes {
		t.stripes[i].keys = make(map[string]*trackedKey)
	}
	b.tracking.CompareAndSwap(nil, t)
}

func (t *keyTaskTracker) stripe(key string) *trackStripe {
	// FNV-1a, as getShard and the ledger.
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return &t.stripes[hash%ledgerStripeCount]
}

// recordLocked records a detach of key in task id and joins every waiter
// still waiting for the key's next detach. Caller holds s.mu, and the key's
// shard.mu, so no snapshot is between "left the buffer" and "recorded".
func (s *trackStripe) recordLocked(key string, id uint64) {
	k := s.keys[key]
	if k == nil {
		k = &trackedKey{}
		k.inflight = k.inflightBuf[:0]
		s.keys[key] = k
	}
	k.inflight = append(k.inflight, id)
	for _, w := range k.waiters {
		if w.joinWanted && w.joined == 0 {
			w.joined = id
			w.need = append(w.need, id)
		}
	}
}

// record is recordLocked for D2, which has no send to hold the stripe across.
func (t *keyTaskTracker) record(key string, id uint64) {
	s := t.stripe(key)
	s.mu.Lock()
	s.recordLocked(key, id)
	s.mu.Unlock()
}

// resolve ends task id of key with outcome o and wakes every waiter that
// needed it. A task never recorded (detached before tracking was enabled) is
// a no-op: no waiter can need it. Never called from a defer: a panicking
// flush must stay unresolved, so its waiter times out instead of answering.
func (t *keyTaskTracker) resolve(key string, id uint64, o taskOutcome) {
	s := t.stripe(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.keys[key]
	if k == nil || !removeTaskID(&k.inflight, id) {
		return
	}
	for _, w := range k.waiters {
		if !removeTaskID(&w.need, id) {
			continue
		}
		if o != outcomeSettled && w.bad == 0 {
			w.bad = o
		}
		select {
		case w.wake <- struct{}{}:
		default:
		}
	}
	if len(k.inflight) == 0 && len(k.waiters) == 0 {
		delete(s.keys, key)
	}
}

func removeTaskID(ids *[]uint64, id uint64) bool {
	s := *ids
	for i, v := range s {
		if v == id {
			last := len(s) - 1
			s[i] = s[last]
			*ids = s[:last]
			return true
		}
	}
	return false
}

func removeWaiter(ws []*keyWaiter, w *keyWaiter) []*keyWaiter {
	for i, v := range ws {
		if v == w {
			last := len(ws) - 1
			ws[i] = ws[last]
			ws[last] = nil
			return ws[:last]
		}
	}
	return ws
}

// Test seams, nil in production.
var (
	// waitBeforeSnapshotHook runs in WaitKeysFlushed before the snapshot.
	waitBeforeSnapshotHook func()
	// waitAfterSnapshotHook runs in WaitKeysFlushed after every key's
	// snapshot and registration, before the latch read.
	waitAfterSnapshotHook func()
	// waitBeforeForceHook runs before each ForceFlushKey call of a wait.
	waitBeforeForceHook func(key string)
	// waitAfterForceHook runs right after each ForceFlushKey call of a wait,
	// with its reply and no lock held.
	waitAfterForceHook func(key string, rep ForceReply)
	// sendSentHook is the first statement of sendFlushLocked's successful
	// send arm (D1), under shard.mu and the track stripe; tracking on only,
	// so a buffer with tracking off pays no load for it.
	sendSentHook func(key string)
	// flushUnlockedHook runs in flushBufferLocked right after it releases
	// shard.mu, before the write (D2).
	flushUnlockedHook func(key string)
)

// forceState is where a key of a wait stands with forcing.
type forceState uint8

const (
	forceUntried forceState = iota
	forceNotDue             // retry once due
	forceDone               // never again in this wait
)

type waitKey struct {
	key   string
	s     *trackStripe
	w     *keyWaiter // nil: nothing to wait for (fast path)
	done  bool
	force forceState
	due   time.Time
}

// WaitKeysFlushed waits until every row of keys ("database/measurement")
// acknowledged before the call is in a settled flush, forcing each buffered
// key at most once per interval (ForceFlushKey, inside the forced-flush
// budget) and otherwise waiting for its natural flush. interval <= 0 never
// forces.
//
// Errors: ErrLedgerOff; *RecoveryPendingError (WAL on and the latch set,
// read once after every key's snapshot); ErrKeyFlushFailed and
// ErrKeyFlushUnconfirmed, wrapped with the key; ErrBufferClosing (closing is
// read before and again after the snapshot); ctx.Err(). Holds no lock while
// it waits.
//
// Not under tiered storage: a flush that stored a file but failed to register
// it before the call sets no latch and is in no task the wait can see, so the
// wait would answer nil without it. A caller must not use it while tiering is
// active (the fresh-query handler refuses then).
func (b *ArrowBuffer) WaitKeysFlushed(ctx context.Context, keys []string, interval time.Duration) error {
	l := b.ledger.Load()
	t := b.tracking.Load()
	if l == nil || t == nil {
		return ErrLedgerOff
	}
	if b.closing.Load() {
		return ErrBufferClosing
	}

	if waitBeforeSnapshotHook != nil {
		waitBeforeSnapshotHook()
	}
	wake := make(chan struct{}, 1)
	ws := b.registerWaiters(t, keys, wake)
	defer deregisterWaiters(ws)
	if waitAfterSnapshotHook != nil {
		waitAfterSnapshotHook()
	}
	// Again after the snapshot: Close's drain abandons queued tasks after it
	// sets closing, resolving them failed without the latch. One abandoned
	// before the snapshot left nothing to wait for; this read catches it.
	if b.closing.Load() {
		return ErrBufferClosing
	}

	// The latch, once, after every snapshot (#1938: one load). Without a WAL
	// there is no recovery: a failed flush's rows are gone (the documented
	// exception), and the latch, which only recovery clears, would refuse
	// every wait until a restart.
	if b.hasWAL.Load() {
		flushFailed, recoveryPending, _ := b.FlushFailureState()
		if flushFailed {
			return &RecoveryPendingError{Reason: RecoveryReasonFlushFailure}
		}
		if recoveryPending {
			return &RecoveryPendingError{Reason: RecoveryReasonRecoveryPending}
		}
	}

	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		// After every wake, in this order: outcomes, then forcing.
		pending := false
		for i := range ws {
			k := &ws[i]
			if k.done {
				continue
			}
			k.s.mu.Lock()
			bad := k.w.bad
			joined := k.w.joined != 0
			done := (!k.w.joinWanted || joined) && len(k.w.need) == 0
			k.s.mu.Unlock()
			switch bad {
			case outcomeFailed:
				return &keyFlushError{err: ErrKeyFlushFailed, key: k.key}
			case outcomeUnconfirmed:
				return &keyFlushError{err: ErrKeyFlushUnconfirmed, key: k.key}
			}
			if done {
				k.done = true
				continue
			}
			if joined {
				k.force = forceDone // its detach happened: nothing left to force
			}
			pending = true
		}
		if !pending {
			return nil
		}

		// One force round: each key at most once (R-M0).
		var next time.Time
		now := l.now()
		for i := range ws {
			k := &ws[i]
			if k.done || !k.w.joinWanted || k.force == forceDone {
				continue
			}
			if k.force == forceNotDue && now.Before(k.due) {
				next = earliest(next, k.due)
				continue
			}
			if waitBeforeForceHook != nil {
				waitBeforeForceHook(k.key)
			}
			rep := b.ForceFlushKey(k.key, interval, ForceRead, nil)
			if waitAfterForceHook != nil {
				waitAfterForceHook(k.key, rep)
			}
			// rep.TaskID is not waited on: the tracker joined the first
			// detach after this waiter registered, which a LostRace's id (the
			// newest) need not be (ForceReply).
			switch rep.Result {
			case NotDue:
				k.force, k.due = forceNotDue, rep.Due
				next = earliest(next, rep.Due)
			case Closing:
				return ErrBufferClosing
			case Forced, NothingToForce, LostRace:
				// Its detach (this send, or another) joined the waiter.
				k.force = forceDone
			case OverBudget:
				// Never retried: the natural detach (age, size, drainer)
				// joins the waiter.
				k.force = forceDone
			default: // LedgerOff (interval <= 0), NotWanted: no forcing
				k.force = forceDone
			}
		}

		var timerC <-chan time.Time
		if !next.IsZero() {
			d := next.Sub(l.now())
			if d < 0 {
				d = 0
			}
			if timer == nil {
				timer = time.NewTimer(d)
			} else {
				timer.Reset(d)
			}
			timerC = timer.C
		}
		select {
		case <-wake:
		case <-timerC:
		case <-ctx.Done():
			return ctx.Err()
		case <-b.ctx.Done():
			// Close has begun (it cancels b.ctx before its flushes). Keys
			// waiting on a natural flush or an over-budget force would never
			// see a Closing result.
			return ErrBufferClosing
		}
	}
}

// registerWaiters takes each key's snapshot and registers its waiter: under
// the shard read lock (no detach can run) and then the key's track stripe,
// read whether the key is buffered, copy its in-flight tasks, append the
// waiter, all in one hold. Duplicate keys are skipped.
func (b *ArrowBuffer) registerWaiters(t *keyTaskTracker, keys []string, wake chan struct{}) []waitKey {
	ws := make([]waitKey, 0, len(keys))
	var seen map[string]struct{}
	if len(keys) > 8 {
		seen = make(map[string]struct{}, len(keys))
	}
next:
	for _, key := range keys {
		if seen != nil {
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
		} else {
			for i := range ws {
				if ws[i].key == key {
					continue next
				}
			}
		}
		shard := b.getShard(key)
		s := t.stripe(key)
		shard.mu.RLock()
		s.mu.Lock()
		buffered := len(shard.buffers[key]) > 0
		k := s.keys[key]
		var w *keyWaiter
		if buffered || k != nil && len(k.inflight) > 0 {
			if k == nil {
				k = &trackedKey{}
				s.keys[key] = k
			}
			w = &keyWaiter{wake: wake, joinWanted: buffered, need: append([]uint64(nil), k.inflight...)}
			k.waiters = append(k.waiters, w)
		}
		s.mu.Unlock()
		shard.mu.RUnlock()
		ws = append(ws, waitKey{key: key, s: s, w: w, done: w == nil})
	}
	return ws
}

// deregisterWaiters removes every waiter of a wait, on every exit.
func deregisterWaiters(ws []waitKey) {
	for i := range ws {
		k := &ws[i]
		if k.w == nil {
			continue
		}
		k.s.mu.Lock()
		if tk := k.s.keys[k.key]; tk != nil {
			tk.waiters = removeWaiter(tk.waiters, k.w)
			if len(tk.inflight) == 0 && len(tk.waiters) == 0 {
				delete(k.s.keys, k.key)
			}
		}
		k.s.mu.Unlock()
	}
}

// keyFlushError names the key whose flush failed or was not confirmed.
type keyFlushError struct {
	err error
	key string
}

func (e *keyFlushError) Error() string { return e.err.Error() + ": " + e.key }
func (e *keyFlushError) Unwrap() error { return e.err }
