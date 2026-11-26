package ingest

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

// #1760 (a2): per-task tracking and the reader waiter. Unwired: the tests
// enable the ledger and tracking themselves and call WaitKeysFlushed.

// slowBackend is a gateBackend whose "slow*" measurements take delay per write.
type slowBackend struct {
	*gateBackend
	delay time.Duration
}

func (s *slowBackend) Write(ctx context.Context, path string, data []byte) error {
	if parts := strings.Split(path, "/"); len(parts) > 1 && strings.HasPrefix(parts[1], "slow") {
		time.Sleep(s.delay)
	}
	return s.gateBackend.Write(ctx, path, data)
}

func (s *slowBackend) WriteReader(ctx context.Context, path string, rd io.Reader, _ int64) error {
	_, _ = io.Copy(io.Discard, rd)
	return s.Write(ctx, path, nil)
}

// newWaitFixture: newForceFixture with tracking on (unless tracking is
// false), every #1760 seam reset at cleanup, and the ledger on the real clock
// when realClock: the waiter's timers are real, so a NotDue key on the frozen
// clock would re-arm forever.
func newWaitFixture(t *testing.T, cfg *config.IngestConfig, budget *ForceBudget, tracking, realClock bool) *forceFixture {
	t.Helper()
	f := newForceFixture(t, cfg, budget)
	if tracking {
		f.buf.EnableKeyFlushTracking()
	}
	if realClock && f.buf.ledger.Load() != nil {
		f.buf.ledger.Load().now = time.Now
	}
	// Registered after newForceFixture's cleanup, so it runs first: no seam
	// fires during that cleanup's Close.
	t.Cleanup(resetWaitSeams)
	return f
}

func resetWaitSeams() {
	waitBeforeSnapshotHook = nil
	waitAfterSnapshotHook = nil
	waitBeforeForceHook = nil
	sendSentHook = nil
	flushUnlockedHook = nil
	forceAfterScanHook = nil
	forceBeforeSendHook = nil
}

func (f *forceFixture) tracker() *keyTaskTracker { return f.buf.tracking.Load() }

// waiterStates copies the waiters registered on key.
func (f *forceFixture) waiterStates(key string) []keyWaiter {
	s := f.tracker().stripe(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.keys[key]
	if k == nil {
		return nil
	}
	out := make([]keyWaiter, len(k.waiters))
	for i, w := range k.waiters {
		out[i] = *w
		out[i].need = append([]uint64(nil), w.need...)
	}
	return out
}

func (f *forceFixture) inflightIDs(key string) []uint64 {
	s := f.tracker().stripe(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if k := s.keys[key]; k != nil {
		return append([]uint64(nil), k.inflight...)
	}
	return nil
}

// startWait runs WaitKeysFlushed on its own goroutine.
func (f *forceFixture) startWait(timeout, interval time.Duration, keys ...string) <-chan error {
	ch := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		ch <- f.buf.WaitKeysFlushed(ctx, keys, interval)
	}()
	return ch
}

func (f *forceFixture) waitResult(ch <-chan error) error {
	f.t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		f.t.Fatal("WaitKeysFlushed did not return")
		return nil
	}
}

func (f *forceFixture) assertWaiting(ch <-chan error, what string) {
	f.t.Helper()
	select {
	case err := <-ch:
		f.t.Fatalf("%s: the waiter returned %v before its rows settled", what, err)
	case <-time.After(100 * time.Millisecond):
	}
}

// waitRegistered waits until key has n waiters.
func (f *forceFixture) waitRegistered(key string, n int) {
	f.t.Helper()
	f.waitFor("a registered waiter", func() bool { return len(f.waiterStates(key)) >= n })
}

// waitTasksComplete waits until every async flush task registered so far has
// completed. Resolution comes before completion at R1–R3, so a pin is gone by
// the time this returns: callers assert it without polling.
func (f *forceFixture) waitTasksComplete() {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.buf.waitForAsyncFlushTasksThrough(ctx, f.buf.asyncFlushFence()); err != nil {
		f.t.Fatalf("flush tasks did not complete: %v", err)
	}
}

// tracked reports whether key has a tracking entry: a task in flight or a
// waiter. False with tracking off.
func (f *forceFixture) tracked(key string) bool {
	t := f.buf.tracking.Load()
	if t == nil {
		return false
	}
	s := t.stripe(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.keys[key]
	return ok
}

func (f *forceFixture) assertUntracked(key string) {
	f.t.Helper()
	if f.tracked(key) {
		f.t.Fatalf("%s is still tracked: inflight %v, waiters %d", key, f.inflightIDs(key), len(f.waiterStates(key)))
	}
}

// refuseBudget makes every force over budget (a deferral just now, against
// an hour-long max_buffer_age_ms) so keys wait for their natural detach.
func (f *forceFixture) refuseBudget() { f.buf.ledger.Load().noteDeferral() }

func (f *forceFixture) flushKey(key string) {
	f.t.Helper()
	if err := f.buf.flushMatching(context.Background(), func(k string) bool { return k == key }); err != nil {
		f.t.Fatalf("flush %s: %v", key, err)
	}
}

// otherShardMeasurement returns a measurement with prefix whose key is not in
// key's shard.
func (f *forceFixture) otherShardMeasurement(prefix, key string) string {
	f.t.Helper()
	for _, s := range []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"} {
		if f.buf.getShard("db/"+prefix+s) != f.buf.getShard(key) {
			return prefix + s
		}
	}
	f.t.Fatal("no measurement in another shard")
	return ""
}

// --- switches ---

// Tracking has its own switch: with the ledger on and tracking off, forcing
// works, no tracker exists, nothing is tracked and the waiter refuses.
func TestKeyFlushTracking_Switches(t *testing.T) {
	t.Run("ledger on, tracking off", func(t *testing.T) {
		f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, false, false)
		f.write("k", 10)
		f.expect("db/k", Forced)
		f.waitTasksComplete()
		if f.buf.tracking.Load() != nil {
			t.Fatal("tracking was built without EnableKeyFlushTracking")
		}
		if f.tracked("db/k") {
			t.Fatal("tracked with tracking off")
		}
		if err := f.buf.WaitKeysFlushed(context.Background(), []string{"db/k"}, time.Second); !errors.Is(err, ErrLedgerOff) {
			t.Fatalf("WaitKeysFlushed = %v, want ErrLedgerOff", err)
		}
	})
	t.Run("tracking on, ledger off", func(t *testing.T) {
		f := newWaitFixture(t, forceConfig(4, 8), nil, true, false)
		f.write("k", 10)
		if err := f.buf.WaitKeysFlushed(context.Background(), []string{"db/k"}, time.Second); !errors.Is(err, ErrLedgerOff) {
			t.Fatalf("WaitKeysFlushed = %v, want ErrLedgerOff", err)
		}
		f.assertUntracked("db/k")
	})
	t.Run("both on", func(t *testing.T) {
		f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
		f.write("k", 10)
		if err := f.buf.WaitKeysFlushed(context.Background(), []string{"db/k"}, time.Second); err != nil {
			t.Fatalf("WaitKeysFlushed = %v", err)
		}
		f.assertUntracked("db/k")
	})
}

// With tracking off, every detach and resolution site runs through its nil
// test and builds nothing: D1's three arms (sent, lost slot, ctx), D2 and R2
// (a key flush), R4 (Close drain abandon), R3 (Close drain flush), R1.
func TestKeyFlushTracking_OffAtEverySite(t *testing.T) {
	f := newWaitFixture(t, forceConfig(1, 1), &ForceBudget{MaxInFlight: 10}, false, false)
	f.occupyWorker() // D1 sent; R1 once released
	f.write("m", 10)
	filler := f.otherShardMeasurement("blocker", "db/m")
	forceBeforeSendHook = func(k string) {
		if k == "db/m" {
			f.write(filler, 100) // takes the queue's only slot
		}
	}
	f.expect("db/m", OverBudget) // D1 lost-slot arm
	forceBeforeSendHook = nil
	f.flushKey("db/m")                                        // D2 + R2
	f.buf.drainAndFlushQueue(time.Now().Add(-time.Second))    // R4: abandons the filler
	f.write("k3", 100)                                        // D1 sent
	f.buf.drainAndFlushQueue(time.Now().Add(5 * time.Second)) // R3
	f.write("m2", 10)
	forceBeforeSendHook = func(k string) {
		if k == "db/m2" {
			f.write(filler, 100)
			f.buf.cancel() // Close's cancellation, between the checks and the send
		}
	}
	f.expect("db/m2", Closing) // D1 ctx arm
	forceBeforeSendHook = nil
	f.store.releaseAll() // R1 (the blocked worker's task)
	if f.buf.tracking.Load() != nil {
		t.Fatal("tracking state was built with tracking off")
	}
}

// D1's failure arms release the track stripe: a send that finds Close's
// cancellation (ctx arm), with tracking on, leaves the stripe free.
func TestKeyFlushTracking_D1CtxArmReleasesTheStripe(t *testing.T) {
	f := newWaitFixture(t, forceConfig(1, 1), &ForceBudget{MaxInFlight: 10}, true, false)
	f.occupyWorker()
	f.write("m", 10)
	filler := f.otherShardMeasurement("blocker", "db/m")
	forceBeforeSendHook = func(k string) {
		if k == "db/m" {
			f.write(filler, 100)
			f.buf.cancel()
		}
	}
	f.expect("db/m", Closing)
	f.assertStripeFree("db/m")
	if ids := f.inflightIDs("db/m"); len(ids) != 0 {
		t.Fatalf("a cancelled send recorded %v", ids)
	}
}

func (f *forceFixture) assertStripeFree(key string) {
	f.t.Helper()
	s := f.tracker().stripe(key)
	if !s.mu.TryLock() {
		f.t.Fatalf("%s's track stripe is still held after a failed send", key)
	}
	s.mu.Unlock()
}

// --- R1–R4 and the outcome ---

// I1 and every resolution site: a waiter whose rows are in a task resolved at
// R1 (worker), R2 (writeExtracted), R3 (Close drain flush) or R4 (Close drain
// abandon) answers that task's outcome, and the key is untracked once the task
// completes.
func TestWaitKeysFlushed_EveryResolutionSite(t *testing.T) {
	type site struct {
		name    string
		meas    string
		run     func(f *forceFixture, key string) <-chan error // detaches key, registers a waiter, then resolves
		wantErr error
	}
	queued := func(f *forceFixture, key string) {
		f.occupyWorker()
		f.write(strings.TrimPrefix(key, "db/"), 10)
		f.expect(key, Forced)
	}
	sites := []site{
		{"R1 worker settled", "k", func(f *forceFixture, key string) <-chan error {
			queued(f, key)
			ch := f.startWaitAndRegister(key)
			f.store.releaseAll()
			return ch
		}, nil},
		{"R1 worker failed", "failer", func(f *forceFixture, key string) <-chan error {
			queued(f, key)
			ch := f.startWaitAndRegister(key)
			f.store.releaseAll()
			return ch
		}, ErrKeyFlushFailed},
		{"R2 writeExtracted settled", "k", func(f *forceFixture, key string) <-chan error {
			f.write("k", 10)
			e := f.extract(key)
			ch := f.startWaitAndRegister(key)
			_ = f.buf.writeExtracted(context.Background(), e, key, "db", "k")
			return ch
		}, nil},
		{"R2 writeExtracted failed", "failer", func(f *forceFixture, key string) <-chan error {
			f.write("failer", 10)
			e := f.extract(key)
			ch := f.startWaitAndRegister(key)
			_ = f.buf.writeExtracted(context.Background(), e, key, "db", "failer")
			return ch
		}, ErrKeyFlushFailed},
		{"R3 drain flush", "k", func(f *forceFixture, key string) <-chan error {
			queued(f, key)
			ch := f.startWaitAndRegister(key)
			f.buf.drainAndFlushQueue(time.Now().Add(5 * time.Second))
			return ch
		}, nil},
		{"R3 drain flush failed", "failer", func(f *forceFixture, key string) <-chan error {
			queued(f, key)
			ch := f.startWaitAndRegister(key)
			f.buf.drainAndFlushQueue(time.Now().Add(5 * time.Second))
			return ch
		}, ErrKeyFlushFailed},
		{"R4 drain abandon", "k", func(f *forceFixture, key string) <-chan error {
			queued(f, key)
			ch := f.startWaitAndRegister(key)
			f.buf.drainAndFlushQueue(time.Now().Add(-time.Second))
			return ch
		}, ErrKeyFlushFailed},
	}
	for _, s := range sites {
		t.Run(s.name, func(t *testing.T) {
			f := newWaitFixture(t, forceConfig(1, 4), &ForceBudget{MaxInFlight: 10}, true, false)
			key := "db/" + s.meas
			err := f.waitResult(s.run(f, key))
			if s.wantErr == nil && err != nil || s.wantErr != nil && !errors.Is(err, s.wantErr) {
				t.Fatalf("WaitKeysFlushed = %v, want %v", err, s.wantErr)
			}
			f.store.releaseAll()
			f.waitTasksComplete()
			f.assertUntracked(key)
		})
	}
}

// startWaitAndRegister starts a wait on key (hour interval, so it never
// forces) and waits until its waiter is registered.
func (f *forceFixture) startWaitAndRegister(key string) <-chan error {
	f.t.Helper()
	ch := f.startWait(5*time.Second, time.Hour, key)
	f.waitRegistered(key, 1)
	return ch
}

// extract detaches key as the synchronous paths do (D2).
func (f *forceFixture) extract(key string) *extractedFlush {
	f.t.Helper()
	shard := f.buf.getShard(key)
	shard.mu.Lock()
	e := f.buf.extractBufferLocked(shard, key)
	shard.mu.Unlock()
	if e == nil {
		f.t.Fatal("nothing extracted")
	}
	return e
}

// I7: the classification. A failure wins; settled needs a settle and no
// unconfirmed part; no mark at all is unconfirmed, never settled.
func TestTaskResult_Outcome(t *testing.T) {
	for _, tc := range []struct {
		name  string
		marks func(r *taskResult)
		want  taskOutcome
	}{
		{"no mark", func(*taskResult) {}, outcomeUnconfirmed},
		{"settled", func(r *taskResult) { r.settle(true) }, outcomeSettled},
		{"unregistered", func(r *taskResult) { r.settle(false) }, outcomeUnconfirmed},
		{"settled + unregistered part", func(r *taskResult) { r.settle(true); r.settle(false) }, outcomeUnconfirmed},
		{"settled + failed part", func(r *taskResult) { r.settle(true); r.fail() }, outcomeFailed},
		{"failed + settled part", func(r *taskResult) { r.fail(); r.settle(true) }, outcomeFailed},
	} {
		var r taskResult
		tc.marks(&r)
		if got := r.outcome(); got != tc.want {
			t.Errorf("%s: outcome %d, want %d", tc.name, got, tc.want)
		}
	}
	var nilRes *taskResult // tracking off: the marks are nil tests
	nilRes.fail()
	nilRes.settle(true)
}

// An unconfirmed task answers ErrKeyFlushUnconfirmed.
func TestWaitKeysFlushed_UnconfirmedTask(t *testing.T) {
	f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
	f.tracker().record("db/k", 999)
	waitAfterSnapshotHook = func() { f.tracker().resolve("db/k", 999, outcomeUnconfirmed) }
	err := f.buf.WaitKeysFlushed(context.Background(), []string{"db/k"}, time.Hour)
	if !errors.Is(err, ErrKeyFlushUnconfirmed) {
		t.Fatalf("WaitKeysFlushed = %v, want ErrKeyFlushUnconfirmed", err)
	}
	f.assertUntracked("db/k")
}

// I21: a coverage split is one task with one result: a failed part fails it,
// all parts settled settle it, on the synchronous (flushPartSync) and the
// asynchronous (flushRecordsAsync parts, through the worker) paths.
func TestWaitKeysFlushed_CoverageSplit(t *testing.T) {
	for _, syncPath := range []bool{true, false} {
		for _, failSecond := range []bool{false, true} {
			name := map[bool]string{true: "sync", false: "async"}[syncPath] + map[bool]string{true: " one part fails", false: " all settle"}[failSecond]
			t.Run(name, func(t *testing.T) {
				b, store := newSplitWaitBuffer(t)
				if failSecond {
					store.failFrom.Store(2)
				}
				ctx := context.Background()
				for i := 0; i < 3; i++ {
					if err := b.WriteColumnarRecord(ctx, "db", coverageRecord("cpu", float64(i))); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				if syncPath {
					shard := b.getShard("db/cpu")
					shard.mu.Lock()
					e := b.extractBufferLocked(shard, "db/cpu")
					shard.mu.Unlock()
					ch := make(chan error, 1)
					registered := make(chan struct{})
					waitAfterSnapshotHook = func() { close(registered) }
					t.Cleanup(resetWaitSeams)
					go func() { ch <- b.WaitKeysFlushed(ctx, []string{"db/cpu"}, time.Hour) }()
					<-registered
					_ = b.writeExtracted(ctx, e, "db/cpu", "db", "cpu")
					err = <-ch
				} else {
					wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					err = b.WaitKeysFlushed(wctx, []string{"db/cpu"}, time.Hour)
				}
				if got := store.writes.Load(); got != 2 {
					t.Fatalf("fixture: %d files written, want 2 (one per class)", got)
				}
				if failSecond && !errors.Is(err, ErrKeyFlushFailed) || !failSecond && err != nil {
					t.Fatalf("WaitKeysFlushed = %v (one part fails: %v)", err, failSecond)
				}
			})
		}
	}
}

// failFromBackend fails every write from the failFrom-th on (0: never).
type failFromBackend struct {
	mockStorageBackend
	writes   atomic.Int64
	failFrom atomic.Int64
}

func (c *failFromBackend) Write(context.Context, string, []byte) error {
	n := c.writes.Add(1)
	if from := c.failFrom.Load(); from > 0 && n >= from {
		return errors.New("injected write failure")
	}
	return nil
}

func (c *failFromBackend) WriteReader(ctx context.Context, path string, rd io.Reader, _ int64) error {
	_, _ = io.Copy(io.Discard, rd)
	return c.Write(ctx, path, nil)
}

// newSplitWaitBuffer: a covered buffer whose cseq-2 batch is claimed, so a
// flush of db/cpu splits into two classes, with the ledger and tracking on.
// hasWAL is cleared after SetWAL so the latch, which a failed part sets,
// does not answer before the task's own outcome.
func newSplitWaitBuffer(t *testing.T) (*ArrowBuffer, *failFromBackend) {
	t.Helper()
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: t.TempDir(), SyncMode: wal.SyncModeAsync, Logger: zerolog.New(io.Discard)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	store := &failFromBackend{}
	b := NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000000, MaxBufferAgeMS: noAgeFlushMS, Compression: "snappy",
		ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 64,
	}, store, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close() })
	b.SetWAL(w)
	b.SetFileRegistrar(&coverageRegistrar{})
	if err := b.EnableCoverage(); err != nil {
		t.Fatal(err)
	}
	b.SetClaimedBatch(func(_ uint64, _ string, cseq uint64) bool { return cseq == 2 })
	b.EnableKeyFlushLedger(ForceBudget{})
	b.EnableKeyFlushTracking()
	b.hasWAL.Store(false)
	return b, store
}

// --- B1 as a class: the record precedes every hand-off ---

// I15 (D1): the track stripe is held across the send, so the worker's
// resolution cannot run before the detach is recorded. The seam is the first
// statement of the successful send arm and waits for the task to complete.
// A failing flush is used because it reaches its resolution without any
// shard lock (a successful one first publishes buffer metrics, which read
// every shard and so would wait for this seam's shard.mu anyway). With the
// stripe held the task cannot complete during the seam; recorded after a
// re-lock instead, the worker resolves an unrecorded task, the record then
// leaks, and a later wait on the key never ends.
func TestWaitKeysFlushed_D1RecordsBeforeTheWorkerResolves(t *testing.T) {
	f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
	var completedInSeam atomic.Bool
	sendSentHook = func(key string) {
		if key != "db/failer" {
			return
		}
		deadline := time.Now().Add(300 * time.Millisecond)
		for time.Now().Before(deadline) {
			if !f.taskPending(key) {
				completedInSeam.Store(true)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	f.write("failer", 10)
	f.expect("db/failer", Forced)
	f.waitTasksComplete()
	if !f.buf.HasFlushFailure() {
		t.Fatal("fixture: the forced flush did not fail")
	}
	if completedInSeam.Load() {
		t.Fatal("the task completed while its send arm was still running: the worker resolved it before it was recorded")
	}
	f.assertUntracked("db/failer")
	sendSentHook = nil
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := f.buf.WaitKeysFlushed(ctx, []string{"db/failer"}, time.Hour); err != nil {
		t.Fatalf("a wait after the flush = %v, want nil (a leaked in-flight entry makes it time out)", err)
	}
}

// taskPending reports whether an async flush task of key has not completed.
func (f *forceFixture) taskPending(key string) bool {
	pending, _ := f.buf.asyncFlushState(f.buf.asyncFlushFence(), func(k string) bool { return k == key })
	return pending
}

// R1, R2 and R3 resolve before the task completes: with the key's track
// stripe held, the task cannot complete; once released, it does. So anyone
// who waited for a task's completion (waitTasksComplete) finds it resolved.
func TestKeyFlushTracking_ResolvedBeforeTheTaskCompletes(t *testing.T) {
	for _, site := range []string{"R1 worker", "R2 writeExtracted", "R3 drain"} {
		t.Run(site, func(t *testing.T) {
			f := newWaitFixture(t, forceConfig(1, 4), &ForceBudget{MaxInFlight: 10}, true, false)
			meas := "k"
			if site == "R1 worker" {
				meas = "blocker1" // the worker holds in the write until the stripe is ours
			}
			key := "db/" + meas
			if site == "R3 drain" {
				f.occupyWorker()
			}
			f.write(meas, 10)
			s := f.tracker().stripe(key)
			done := make(chan struct{})
			switch site {
			case "R1 worker":
				f.expect(key, Forced)
				f.waitFor("the worker in the write", func() bool { return f.store.blocked.Load() >= 1 })
				s.mu.Lock()
				f.store.releaseAll()
				close(done)
			case "R2 writeExtracted":
				e := f.extract(key)
				s.mu.Lock()
				go func() { _ = f.buf.writeExtracted(context.Background(), e, key, "db", meas); close(done) }()
			case "R3 drain":
				f.expect(key, Forced)
				s.mu.Lock()
				go func() { f.buf.drainAndFlushQueue(time.Now().Add(5 * time.Second)); close(done) }()
			}
			time.Sleep(150 * time.Millisecond)
			pending := f.taskPending(key)
			s.mu.Unlock()
			<-done
			if !pending {
				t.Fatal("the task completed while its key's track stripe was held: it completed before it resolved")
			}
			f.store.releaseAll() // R3's blocked worker
			f.waitTasksComplete()
			f.assertUntracked(key)
		})
	}
}

// I16 (D2): the record is inside the shard.mu hold of the extraction. A
// waiter whose snapshot runs right after flushBufferLocked releases shard.mu,
// before the write, must wait for that write.
func TestWaitKeysFlushed_D2RecordsBeforeTheShardUnlock(t *testing.T) {
	f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
	key := "db/blocker1"
	var waitCh <-chan error
	flushUnlockedHook = func(k string) {
		if k != key {
			return
		}
		registered := make(chan struct{})
		waitAfterSnapshotHook = func() { close(registered) }
		waitCh = f.startWait(5*time.Second, time.Hour, key)
		<-registered
	}
	f.write("blocker1", 10)
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- f.buf.flushMatching(context.Background(), func(k string) bool { return k == key })
	}()
	f.waitFor("the write to block", func() bool { return f.store.blocked.Load() >= 1 })
	f.assertWaiting(waitCh, "snapshot between the extraction's unlock and the write")
	f.store.releaseAll()
	if err := <-flushDone; err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := f.waitResult(waitCh); err != nil {
		t.Fatalf("WaitKeysFlushed = %v", err)
	}
	f.assertUntracked(key)
}

// I2: a send that loses its slot after the budget check records nothing and
// joins nothing; the waiter waits for the next real detach.
func TestWaitKeysFlushed_LostSlotRecordsNothing(t *testing.T) {
	f := newWaitFixture(t, forceConfig(1, 1), &ForceBudget{MaxInFlight: 10}, true, false)
	f.occupyWorker()
	filler := f.otherShardMeasurement("blocker", "db/k")
	forceBeforeSendHook = func(k string) {
		if k == "db/k" {
			f.write(filler, 100) // size trigger: takes the queue's only slot
		}
	}
	f.write("k", 10)
	ch := f.startWait(5*time.Second, time.Hour, "db/k")
	f.waitFor("the filler queued", func() bool { return len(f.buf.flushQueue) == 1 })
	f.assertWaiting(ch, "after a lost slot")
	f.assertStripeFree("db/k")
	if ids := f.inflightIDs("db/k"); len(ids) != 0 {
		t.Fatalf("a failed send recorded tasks %v", ids)
	}
	if ws := f.waiterStates("db/k"); len(ws) != 1 || ws[0].joined != 0 {
		t.Fatalf("a failed send joined the waiter: %+v", ws)
	}
	if f.buffered("db/k") == 0 {
		t.Fatal("fixture: k left the buffer")
	}
	forceBeforeSendHook = nil
	f.store.releaseAll()
	f.flushKey("db/k") // the next real detach
	if err := f.waitResult(ch); err != nil {
		t.Fatalf("WaitKeysFlushed = %v", err)
	}
}

// I6: a panicking flush is never resolved (the resolution is not deferred),
// so its waiter times out instead of answering, at R1 and R2. The key stays
// tracked by the unresolved task.
func TestWaitKeysFlushed_PanicLeavesTheTaskUnresolved(t *testing.T) {
	run := func(t *testing.T, f *forceFixture, flush func()) {
		ch := f.startWait(300*time.Millisecond, time.Hour, "db/panicker")
		f.waitRegistered("db/panicker", 1)
		func() {
			defer func() { _ = recover() }()
			flush()
			t.Fatal("fixture: the backend did not panic")
		}()
		if err := f.waitResult(ch); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("WaitKeysFlushed after a panicking flush = %v, want the context's deadline", err)
		}
		if !f.tracked("db/panicker") {
			t.Fatal("the panicking task was resolved")
		}
	}
	t.Run("R1 worker", func(t *testing.T) {
		f := newWaitFixture(t, forceConfig(1, 4), &ForceBudget{MaxInFlight: 10}, true, false)
		f.occupyWorker()
		f.write("panicker", 10)
		f.expect("db/panicker", Forced)
		task := <-f.buf.flushQueue // this goroutine is the worker
		run(t, f, func() { f.buf.runQueuedFlushTask(-1, task) })
	})
	t.Run("R2 writeExtracted", func(t *testing.T) {
		f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
		f.write("panicker", 10)
		e := f.extract("db/panicker")
		run(t, f, func() { _ = f.buf.writeExtracted(context.Background(), e, "db/panicker", "db", "panicker") })
	})
}

// I6, statically: no resolution runs inside a defer, in any file of the
// package that resolves.
func TestKeyFlushTracking_NoResolutionInADefer(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		node, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(node, func(n ast.Node) bool {
			d, ok := n.(*ast.DeferStmt)
			if !ok {
				return true
			}
			ast.Inspect(d, func(m ast.Node) bool {
				if call, ok := m.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "resolve" {
						t.Errorf("%s: a resolution inside a defer", fset.Position(call.Pos()))
					}
				}
				return true
			})
			return false
		})
	}
}

// --- the waiter ---

// I3 and I10: a waiter joins only the first detach after its snapshot. Two
// waiters registered before a detach share it; one registered after it, on
// re-buffered rows, needs that detach (in flight) and the next one, and does
// not return when the first settles.
func TestWaitKeysFlushed_JoinsOnlyADetachAfterTheSnapshot(t *testing.T) {
	f := newWaitFixture(t, forceConfig(1, 4), &ForceBudget{MaxInFlight: 10}, true, false)
	f.occupyWorker()
	f.refuseBudget() // nobody forces: only the test detaches
	f.write("k", 10)
	w1 := f.startWait(5*time.Second, time.Hour, "db/k")
	w2 := f.startWait(5*time.Second, time.Hour, "db/k")
	f.waitRegistered("db/k", 2)

	// T1: a size-triggered detach, queued behind the blocked worker.
	f.write("k", 100)
	ids := f.inflightIDs("db/k")
	if len(ids) != 1 {
		t.Fatalf("fixture: in flight %v, want one task", ids)
	}
	t1 := ids[0]
	for _, w := range f.waiterStates("db/k") {
		if w.joined != t1 {
			t.Fatalf("a waiter registered before T1 joined %d, want %d", w.joined, t1)
		}
	}

	f.write("k", 10) // re-buffered after T1
	w3 := f.startWait(5*time.Second, time.Hour, "db/k")
	f.waitRegistered("db/k", 3)
	for _, w := range f.waiterStates("db/k") {
		if w.joined == 0 && (len(w.need) != 1 || w.need[0] != t1 || !w.joinWanted) {
			t.Fatalf("the late waiter: %+v, want need [T1] and a join pending", w)
		}
	}

	f.store.releaseAll() // T1 settles
	if err := f.waitResult(w1); err != nil {
		t.Fatalf("w1 = %v", err)
	}
	if err := f.waitResult(w2); err != nil {
		t.Fatalf("w2 = %v", err)
	}
	f.assertWaiting(w3, "T1 settled, the late waiter's rows are still buffered")
	f.flushKey("db/k") // T2
	if err := f.waitResult(w3); err != nil {
		t.Fatalf("w3 = %v", err)
	}
	f.assertUntracked("db/k")
}

// I13: a buffered key whose force is over budget: the waiter returns only
// once the natural detach settles.
func TestWaitKeysFlushed_OverBudgetWaitsForTheNaturalDetach(t *testing.T) {
	f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
	f.write("k", 10)
	f.refuseBudget()
	ch := f.startWait(5*time.Second, time.Hour, "db/k")
	f.assertWaiting(ch, "over budget, nothing detached")
	f.waitRegistered("db/k", 1)
	f.flushKey("db/k")
	if err := f.waitResult(ch); err != nil {
		t.Fatalf("WaitKeysFlushed = %v", err)
	}
	f.assertUntracked("db/k")
}

// I17 (R-M0): an over-budget key is tried once per wait, not once per round:
// another key's resolution wakes the wait, and the key is not tried again.
func TestWaitKeysFlushed_OverBudgetIsTriedOnce(t *testing.T) {
	f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
	var attempts atomic.Int64
	waitBeforeForceHook = func(k string) {
		if k == "db/k" {
			attempts.Add(1)
		}
	}
	f.write("k", 10)
	f.refuseBudget()
	f.tracker().record("db/x", 77) // a task of another key, to wake the wait
	ch := f.startWait(5*time.Second, time.Hour, "db/k", "db/x")
	f.waitFor("the first force attempt", func() bool { return attempts.Load() == 1 })
	f.tracker().resolve("db/x", 77, outcomeSettled) // wakes the wait: a new round
	f.assertWaiting(ch, "k still buffered")
	if got := attempts.Load(); got != 1 {
		t.Fatalf("an over-budget key was tried %d times; once per wait", got)
	}
	f.flushKey("db/k")
	if err := f.waitResult(ch); err != nil {
		t.Fatalf("WaitKeysFlushed = %v", err)
	}
}

// I18: shutdown during a wait answers ErrBufferClosing: from Close's
// cancellation (a key waiting on its natural flush never sees a Closing
// result; its rows block in the write, so Close's own flush cannot settle it
// first), and from a Closing force result.
func TestWaitKeysFlushed_Closing(t *testing.T) {
	t.Run("Close during the wait", func(t *testing.T) {
		f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
		f.write("blocker1", 10)
		f.refuseBudget()
		ch := f.startWait(5*time.Second, time.Hour, "db/blocker1")
		f.waitRegistered("db/blocker1", 1)
		closed := make(chan struct{})
		go func() { _ = f.buf.Close(); close(closed) }()
		select {
		case err := <-ch:
			if !errors.Is(err, ErrBufferClosing) {
				t.Fatalf("WaitKeysFlushed = %v, want ErrBufferClosing", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("the wait did not end when Close began")
		}
		f.store.releaseAll()
		<-closed
	})
	t.Run("Closing force result", func(t *testing.T) {
		f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
		waitBeforeForceHook = func(string) { f.buf.closing.Store(true) }
		f.write("k", 10)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := f.buf.WaitKeysFlushed(ctx, []string{"db/k"}, time.Hour); !errors.Is(err, ErrBufferClosing) {
			t.Fatalf("WaitKeysFlushed = %v, want ErrBufferClosing", err)
		}
		f.assertUntracked("db/k")
	})
}

// I4: the latch is read once, after every snapshot. A task of K that fails
// and resolves before the snapshot leaves nothing to wait for; only the
// latch, set before the task completed, says so. Without a WAL the latch is
// not consulted (no recovery would ever clear it).
func TestWaitKeysFlushed_LatchReadAfterTheSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name    string
		wal     bool
		mark    func(b *ArrowBuffer)
		wantErr string // reason, or "" for nil
	}{
		{"flush failure", true, func(b *ArrowBuffer) { b.markFlushFailure() }, RecoveryReasonFlushFailure},
		{"recovery pending", true, func(b *ArrowBuffer) { b.MarkWALRecoveryPending() }, RecoveryReasonRecoveryPending},
		{"no WAL", false, func(b *ArrowBuffer) { b.markFlushFailure() }, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
			f.buf.hasWAL.Store(tc.wal)
			f.tracker().record("db/k", 999)
			waitBeforeSnapshotHook = func() {
				tc.mark(f.buf)
				f.tracker().resolve("db/k", 999, outcomeFailed)
			}
			err := f.buf.WaitKeysFlushed(context.Background(), []string{"db/k"}, time.Hour)
			var rp *RecoveryPendingError
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("WaitKeysFlushed = %v, want nil", err)
			case tc.wantErr != "" && (!errors.As(err, &rp) || rp.Reason != tc.wantErr):
				t.Fatalf("WaitKeysFlushed = %v, want reason %s", err, tc.wantErr)
			}
			f.assertUntracked("db/k")
		})
	}
}

// I14: keys due at different times share one loop: the wait costs the latest
// due time plus a flush, not the keys one after another.
func TestWaitKeysFlushed_KeysForcedInOneLoop(t *testing.T) {
	store := &slowBackend{gateBackend: newGateBackend(), delay: 500 * time.Millisecond}
	buf := NewArrowBuffer(forceConfig(4, 8), store, zerolog.New(io.Discard))
	buf.SetCloseBudget(10 * time.Second)
	buf.EnableKeyFlushLedger(ForceBudget{MaxInFlight: 4})
	buf.EnableKeyFlushTracking()
	t.Cleanup(func() { resetWaitSeams(); store.releaseAll(); _ = buf.Close() })
	l := buf.ledger.Load()
	const interval = 400 * time.Millisecond
	keys := []string{"db/slow1", "db/slow2", "db/slow3"}
	for i, k := range keys { // detached 300, 200 and 100 ms before the wait
		l.recordDetach(k, uint64(1000+i))
		time.Sleep(100 * time.Millisecond)
	}
	for _, k := range keys {
		if err := buf.WriteColumnarDirect(context.Background(), "db", strings.TrimPrefix(k, "db/"), makeColumns(10)); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := buf.WaitKeysFlushed(ctx, keys, interval); err != nil {
		t.Fatalf("WaitKeysFlushed = %v", err)
	}
	// Due at about +100, +200, +300 ms; each flush takes 500 ms. In one
	// loop: ~800 ms. One key after another: ~100+500, then +500, +500
	// (~1600 ms).
	if elapsed := time.Since(start); elapsed > 1200*time.Millisecond {
		t.Fatalf("three keys took %v; one loop over the keys takes about 800 ms", elapsed)
	}
}

// I22: a wait whose context ends while its key is not due leaves nothing
// behind: after the due time the key is still buffered, its last detach is
// unchanged, nothing is deferred and nothing pins it.
func TestWaitKeysFlushed_EndedWaitLeavesNoForce(t *testing.T) {
	f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, true)
	l := f.buf.ledger.Load()
	l.recordDetach("db/k", 5)
	f.write("k", 10)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := f.buf.WaitKeysFlushed(ctx, []string{"db/k"}, 200*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitKeysFlushed = %v, want the deadline", err)
	}
	time.Sleep(300 * time.Millisecond) // past the due time
	if f.buffered("db/k") == 0 {
		t.Fatal("the key was flushed after its wait ended")
	}
	if d, _ := l.lastDetach("db/k"); d.id != 5 {
		t.Fatalf("last detach moved to %d after the wait ended", d.id)
	}
	if n := f.buf.countDeferredBuffers(); n != 0 {
		t.Fatalf("%d buffer(s) deferred", n)
	}
	f.assertUntracked("db/k")
}

// A key's tracking entry in each state: waiter only, in flight only, both,
// after the last resolution, after deregistration, tracking off.
func TestKeyFlushTracking_EntryLifecycle(t *testing.T) {
	f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
	tr := f.tracker()

	tr.record("db/a", 1)
	if !f.tracked("db/a") {
		t.Fatal("in flight only: not tracked")
	}
	tr.resolve("db/a", 1, outcomeSettled)
	f.assertUntracked("db/a")

	f.write("b", 10)
	f.refuseBudget()
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- f.buf.WaitKeysFlushed(ctx, []string{"db/b"}, time.Hour) }()
	f.waitRegistered("db/b", 1)
	if !f.tracked("db/b") {
		t.Fatal("waiter only: not tracked")
	}
	cancel()
	if err := f.waitResult(ch); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitKeysFlushed = %v", err)
	}
	f.assertUntracked("db/b")

	off := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, false, false)
	if off.tracked("db/a") {
		t.Fatal("tracked with tracking off")
	}
}

// With tracking on, a detach and resolution of a key nobody waits on costs
// one allocation (its entry, deleted at the resolution), and none on a key
// whose entry is kept by a waiter.
func TestKeyFlushTracking_RecordAndResolveAllocations(t *testing.T) {
	f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
	tr := f.tracker()
	var id uint64
	if allocs := testing.AllocsPerRun(1000, func() {
		id++
		tr.record("db/m", id)
		tr.resolve("db/m", id, outcomeSettled)
	}); allocs != 1 {
		t.Fatalf("record+resolve of an idle key allocates %.1f, want 1 (the entry)", allocs)
	}
	s := tr.stripe("db/w")
	s.mu.Lock()
	s.keys["db/w"] = &trackedKey{waiters: []*keyWaiter{{wake: make(chan struct{}, 1)}}}
	s.mu.Unlock()
	if allocs := testing.AllocsPerRun(1000, func() {
		id++
		tr.record("db/w", id)
		tr.resolve("db/w", id, outcomeSettled)
	}); allocs != 0 {
		t.Fatalf("record+resolve of a key with an entry allocates %.1f, want 0", allocs)
	}
}

// The snapshot reads the buffer under the shard read lock: a D2 extraction
// deletes the key's batches before it records the task, both inside one
// shard.mu hold, so a snapshot outside that lock could see the key neither
// buffered nor in flight. Here the waiter is held at the key's track stripe
// with the shard read-locked, while a flush of the key waits for the shard;
// once the stripe frees, the waiter must still wait for that flush.
func TestWaitKeysFlushed_SnapshotUnderTheShardReadLock(t *testing.T) {
	f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
	key := "db/blocker1"
	f.write("blocker1", 10)
	f.refuseBudget()
	s := f.tracker().stripe(key)
	s.mu.Lock()
	ch := f.startWait(5*time.Second, time.Hour, key)
	time.Sleep(50 * time.Millisecond) // the waiter queues on the stripe first
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- f.buf.flushMatching(context.Background(), func(k string) bool { return k == key })
	}()
	time.Sleep(50 * time.Millisecond)
	s.mu.Unlock()
	f.waitFor("the write to block", func() bool { return f.store.blocked.Load() >= 1 })
	f.assertWaiting(ch, "a flush extracted the key while the snapshot waited")
	f.store.releaseAll()
	if err := <-flushDone; err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := f.waitResult(ch); err != nil {
		t.Fatalf("WaitKeysFlushed = %v", err)
	}
	f.assertUntracked(key)
}

// Duplicate keys register one waiter.
func TestWaitKeysFlushed_DuplicateKeys(t *testing.T) {
	f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
	f.write("k", 10)
	f.refuseBudget()
	keys := []string{"db/k", "db/k"}
	for i := 0; i < 10; i++ {
		keys = append(keys, "db/k")
	}
	for _, n := range []int{2, len(keys)} { // the slice scan and the map
		ch := f.startWait(5*time.Second, time.Hour, keys[:n]...)
		f.waitRegistered("db/k", 1)
		if got := len(f.waiterStates("db/k")); got != 1 {
			t.Fatalf("%d keys: %d waiters, want 1", n, got)
		}
		f.write("k", 1)
		f.flushKey("db/k")
		if err := f.waitResult(ch); err != nil {
			t.Fatalf("WaitKeysFlushed = %v", err)
		}
		f.write("k", 10)
	}
}

// Close's drain abandon (R4) resolves a task failed without setting the
// latch. One that happens between the wait's first closing check and its
// snapshot leaves nothing to wait for; the closing re-read after the snapshot
// answers ErrBufferClosing, not nil.
func TestWaitKeysFlushed_AbandonBeforeTheSnapshotIsClosing(t *testing.T) {
	f := newWaitFixture(t, forceConfig(1, 4), &ForceBudget{MaxInFlight: 10}, true, false)
	f.occupyWorker()
	f.write("k", 10)
	f.expect("db/k", Forced) // queued behind the blocked worker
	waitBeforeSnapshotHook = func() {
		f.buf.closing.Store(true)
		f.buf.drainAndFlushQueue(time.Now().Add(-time.Second)) // R4
	}
	err := f.buf.WaitKeysFlushed(context.Background(), []string{"db/k"}, time.Hour)
	if !errors.Is(err, ErrBufferClosing) {
		t.Fatalf("WaitKeysFlushed after an abandon before its snapshot = %v, want ErrBufferClosing", err)
	}
	f.assertUntracked("db/k")
}

// A wait that starts with the buffer closing touches nothing.
func TestWaitKeysFlushed_ClosingAtEntryRegistersNothing(t *testing.T) {
	f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
	f.write("k", 10)
	var snapshotted bool
	waitBeforeSnapshotHook = func() { snapshotted = true }
	f.buf.closing.Store(true)
	if err := f.buf.WaitKeysFlushed(context.Background(), []string{"db/k"}, time.Hour); !errors.Is(err, ErrBufferClosing) {
		t.Fatalf("WaitKeysFlushed while closing = %v, want ErrBufferClosing", err)
	}
	if snapshotted {
		t.Fatal("a wait started while closing went on to take a snapshot")
	}
	f.assertUntracked("db/k")
}

// A waiter whose key was only in flight at its snapshot (nothing buffered)
// joins no later detach: that detach carries rows written after its call. It
// returns when its own task settles, while the later task is still open.
func TestWaitKeysFlushed_InFlightOnlyJoinsNoLaterDetach(t *testing.T) {
	f := newWaitFixture(t, forceConfig(1, 4), &ForceBudget{MaxInFlight: 10}, true, false)
	f.occupyWorker()
	f.write("k", 10)
	f.expect("db/k", Forced) // T1, queued behind the blocked worker
	ch := f.startWait(5*time.Second, time.Hour, "db/k")
	f.waitRegistered("db/k", 1)
	if ws := f.waiterStates("db/k"); ws[0].joinWanted {
		t.Fatal("fixture: the key was buffered at the snapshot")
	}

	f.write("k", 10)
	e := f.extract("db/k") // T2: rows written after the wait began, not written yet
	f.store.releaseAll()   // T1 settles
	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("WaitKeysFlushed = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the waiter waited for a detach of rows written after its call")
	}
	_ = f.buf.writeExtracted(context.Background(), e, "db/k", "db", "k")
	f.assertUntracked("db/k")
}

// #1942's case, through the tracker. A waiter's force loses the race: between
// ForceFlushKey's due check and its write-lock re-check another detach takes
// the key's rows (here a synchronous extraction, made in the seam so the
// interleaving is fixed), so the force answers NothingToForce. The waiter
// registered before that detach, so the detach joined it: it waits for the
// winning task and answers with that task's outcome. No task id from
// ForceFlushKey is needed.
func TestWaitKeysFlushed_LostForceRaceWaitsForTheWinningDetach1942(t *testing.T) {
	for _, tc := range []struct {
		name, meas string
		wantErr    error
	}{
		{"winner settles", "k", nil},
		{"winner fails", "failer", ErrKeyFlushFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
			key := "db/" + tc.meas
			f.write(tc.meas, 10)
			won := make(chan *extractedFlush, 1)
			forceAfterScanHook = func(k string) {
				if k == key && len(won) == 0 {
					won <- f.extract(key) // the concurrent detach wins
				}
			}
			ch := f.startWait(5*time.Second, time.Hour, key)
			winner := <-won
			ws := f.waiterStates(key)
			if len(ws) != 1 || ws[0].joined != winner.taskID || len(ws[0].need) != 1 {
				t.Fatalf("the winning detach (task %d) did not join the waiter: %+v", winner.taskID, ws)
			}
			f.assertWaiting(ch, "the winning detach has not been written")
			_ = f.buf.writeExtracted(context.Background(), winner, key, "db", tc.meas)
			err := f.waitResult(ch)
			if tc.wantErr == nil && err != nil || tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("WaitKeysFlushed = %v, want %v", err, tc.wantErr)
			}
			f.assertUntracked(key)
			if got := f.store.files(tc.meas); tc.wantErr == nil && got != 1 {
				t.Fatalf("%d files stored, want 1: the losing force must not flush again", got)
			}
		})
	}
}

// #1942, rows buffered again. (1) The winning detach happened before the
// waiter arrived and new rows were buffered before it registered: the waiter
// needs the winning task and the new rows, so it returns only once both are
// stored, and does not hang (its force is not due until the interval after
// the winning detach, then goes ahead). (2) The new rows arrive after the
// waiter registered and lost its race: they are not its rows, so it returns
// once the winning task settles, without waiting for them.
func TestWaitKeysFlushed_LostRaceWithRowsBufferedAgain1942(t *testing.T) {
	t.Run("rows buffered before the waiter registered", func(t *testing.T) {
		f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, true)
		const interval = 500 * time.Millisecond
		f.write("k", 10)
		winner := f.extract("db/k") // detached (and recorded) before the waiter
		f.write("k", 10)            // new rows, acknowledged before the waiter
		ch := f.startWait(5*time.Second, interval, "db/k")
		f.waitRegistered("db/k", 1)
		ws := f.waiterStates("db/k")
		if !ws[0].joinWanted || len(ws[0].need) != 1 {
			t.Fatalf("fixture: want a waiter needing the winner and the buffered rows, got %+v", ws[0])
		}
		_ = f.buf.writeExtracted(context.Background(), winner, "db/k", "db", "k")
		f.assertWaiting(ch, "the winner settled, the new rows are still buffered")
		if err := f.waitResult(ch); err != nil { // the force goes ahead once due
			t.Fatalf("WaitKeysFlushed = %v", err)
		}
		if got := f.store.files("k"); got != 2 {
			t.Fatalf("returned with %d file(s) stored, want 2 (the winner's and the new rows')", got)
		}
		if f.buffered("db/k") != 0 {
			t.Fatal("returned with the new rows still buffered")
		}
	})
	t.Run("rows buffered after the waiter registered", func(t *testing.T) {
		f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
		f.write("k", 10)
		won := make(chan *extractedFlush, 1)
		forceAfterScanHook = func(k string) {
			if k == "db/k" && len(won) == 0 {
				e := f.extract("db/k")
				f.write("k", 10) // after the waiter's snapshot: not its rows
				won <- e
			}
		}
		ch := f.startWait(5*time.Second, time.Hour, "db/k")
		winner := <-won
		_ = f.buf.writeExtracted(context.Background(), winner, "db/k", "db", "k")
		if err := f.waitResult(ch); err != nil {
			t.Fatalf("WaitKeysFlushed = %v", err)
		}
		if f.buffered("db/k") == 0 {
			t.Fatal("fixture: the later rows were flushed")
		}
	})
}
