package compaction

// #220: the hourly and daily schedulers and the manual trigger share one cycle
// claim. A tick that finds it held runs once when it is released, unless the
// scheduler's next tick comes first, and a skip is loud and counted.

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/metrics"
)

// syncBuffer is a log sink a test can read while a scheduler writes to it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// leaseGate is a ClusterGate a test can flip while a tick runs.
type leaseGate struct {
	held   atomic.Bool
	paused atomic.Bool
}

func (g *leaseGate) CanCompact() bool { return g.held.Load() }
func (g *leaseGate) Role() string     { return "compactor" }
func (g *leaseGate) CompactionPauseReason() string {
	if g.paused.Load() {
		return "test pause"
	}
	return ""
}

// deferredTickRig is a manager with a claim already held, as by a running
// cycle, and a scheduler for one tier whose next tick is far off.
type deferredTickRig struct {
	manager *Manager
	claim   *CycleClaim
	sched   *Scheduler
	logs    *syncBuffer
	stop    chan struct{}
}

func newDeferredTickRig(t *testing.T, tier string, gate ClusterGate) *deferredTickRig {
	t.Helper()
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)
	manager.ManifestManager = nil
	manager.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) { return nil, nil }}}
	claim, err := manager.ClaimCycle()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(claim.Release)
	logs := &syncBuffer{}
	return &deferredTickRig{
		manager: manager,
		claim:   claim,
		logs:    logs,
		stop:    make(chan struct{}),
		sched: &Scheduler{
			manager:         manager,
			tierNames:       []string{tier},
			clusterGate:     gate,
			logger:          zerolog.New(logs),
			nextTickForTest: func() time.Time { return time.Now().Add(time.Hour) },
		},
	}
}

// tick runs a cron tick to its end, deferred part included, in the background.
func (r *deferredTickRig) tick() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.sched.tick(r.stop)
	}()
	return done
}

// awaitDeferred waits until the tick has logged that it is waiting for the
// claim. The tick has then taken its release signal, or is about to and will
// find the claim free.
func (r *deferredTickRig) awaitDeferred(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(r.logs.String(), "Scheduled compaction deferred") {
		if time.Now().After(deadline) {
			t.Fatal("the tick was not deferred")
		}
		time.Sleep(time.Millisecond)
	}
}

func awaitTick(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the tick did not end")
	}
}

// scheduledCycles lists the tiers of each cycle the schedulers ran.
func scheduledCycles(m *Manager) [][]string {
	var out [][]string
	for _, rec := range m.CyclePage(CycleHistoryLimit).Cycles {
		if rec.Source == cycleSourceScheduler {
			out = append(out, rec.Tiers)
		}
	}
	return out
}

// A daily tick during a running hourly cycle runs right after it, once.
func TestDeferredTickRunsOnceWhenTheClaimIsReleased(t *testing.T) {
	r := newDeferredTickRig(t, "daily", nil)
	skips := metrics.Get().CompactionScheduledSkipsFor("daily")

	done := r.tick()
	r.awaitDeferred(t)
	select {
	case <-done:
		t.Fatal("the tick ended with the claim still held")
	case <-time.After(50 * time.Millisecond):
	}
	if got := scheduledCycles(r.manager); len(got) != 0 {
		t.Fatalf("cycles ran with the claim held: %v", got)
	}

	r.claim.Release()
	awaitTick(t, done)
	got := scheduledCycles(r.manager)
	if len(got) != 1 || len(got[0]) != 1 || got[0][0] != "daily" {
		t.Fatalf("scheduled cycles = %v, want one of tier daily", got)
	}
	if r.manager.IsCycleRunning() {
		t.Fatal("the deferred cycle kept the claim")
	}
	if n := metrics.Get().CompactionScheduledSkipsFor("daily") - skips; n != 0 {
		t.Fatalf("a tick that ran counted %d skips", n)
	}
	if strings.Contains(r.logs.String(), `"level":"warn"`) {
		t.Fatalf("a tick followed by its run logged a Warn: %s", r.logs.String())
	}
}

// A tick that finds the claim free is not deferred at all.
func TestTickWithTheClaimFreeIsNotDeferred(t *testing.T) {
	r := newDeferredTickRig(t, "hourly", nil)
	r.claim.Release()
	awaitTick(t, r.tick())
	if got := scheduledCycles(r.manager); len(got) != 1 {
		t.Fatalf("scheduled cycles = %v, want one", got)
	}
	if strings.Contains(r.logs.String(), "deferred") {
		t.Fatalf("a tick with the claim free was deferred: %s", r.logs.String())
	}
}

// The deferred run does not survive past the next tick: the skip is a Warn and
// counted under the scheduler's tier, and a release afterwards runs nothing.
func TestDeferredTickGivesUpAtTheNextTick(t *testing.T) {
	r := newDeferredTickRig(t, "hourly", nil)
	r.sched.nextTickForTest = func() time.Time { return time.Now().Add(20 * time.Millisecond) }
	skips := metrics.Get().CompactionScheduledSkipsFor("hourly")

	awaitTick(t, r.tick())
	if n := metrics.Get().CompactionScheduledSkipsFor("hourly") - skips; n != 1 {
		t.Fatalf("skips counted = %d, want 1", n)
	}
	logged := r.logs.String()
	if !strings.Contains(logged, `"level":"warn"`) || !strings.Contains(logged, "Scheduled compaction skipped") {
		t.Fatalf("the skip was not logged at Warn: %s", logged)
	}
	r.claim.Release()
	if got := scheduledCycles(r.manager); len(got) != 0 {
		t.Fatalf("cycles ran after the tick gave up: %v", got)
	}
}

// A deferred run passes the per-tick gate when it finally runs: the compactor
// lease moved during the wait.
func TestDeferredTickChecksTheLeaseWhenItRuns(t *testing.T) {
	gate := &leaseGate{}
	gate.held.Store(true)
	r := newDeferredTickRig(t, "daily", gate)
	skips := metrics.Get().CompactionScheduledSkipsFor("daily")

	done := r.tick()
	r.awaitDeferred(t)
	gate.held.Store(false)
	r.claim.Release()
	awaitTick(t, done)
	if got := scheduledCycles(r.manager); len(got) != 0 {
		t.Fatalf("a node without the lease ran a deferred cycle: %v", got)
	}
	if n := metrics.Get().CompactionScheduledSkipsFor("daily") - skips; n != 0 {
		t.Fatalf("a gated tick counted %d skips", n)
	}
}

// And the cluster-wide pause (#1087): a pause during the wait ends it.
func TestDeferredTickChecksThePauseWhenItRuns(t *testing.T) {
	gate := &leaseGate{}
	gate.held.Store(true)
	r := newDeferredTickRig(t, "daily", gate)

	done := r.tick()
	r.awaitDeferred(t)
	gate.paused.Store(true)
	r.claim.Release()
	awaitTick(t, done)
	if got := scheduledCycles(r.manager); len(got) != 0 {
		t.Fatalf("a deferred cycle ran during a pause: %v", got)
	}
}

// Two deferred ticks both run: the one that loses the race for the released
// claim waits for the winner's cycle.
func TestDeferredTicksOfBothSchedulersBothRun(t *testing.T) {
	hourly := newDeferredTickRig(t, "hourly", nil)
	daily := &deferredTickRig{manager: hourly.manager, claim: hourly.claim, logs: &syncBuffer{}, stop: make(chan struct{})}
	daily.sched = &Scheduler{
		manager:         hourly.manager,
		tierNames:       []string{"daily"},
		logger:          zerolog.New(daily.logs),
		nextTickForTest: hourly.sched.nextTickForTest,
	}

	hourlyDone, dailyDone := hourly.tick(), daily.tick()
	hourly.awaitDeferred(t)
	daily.awaitDeferred(t)
	hourly.claim.Release()
	awaitTick(t, hourlyDone)
	awaitTick(t, dailyDone)

	seen := map[string]int{}
	for _, tiers := range scheduledCycles(hourly.manager) {
		seen[strings.Join(tiers, ",")]++
	}
	if seen["hourly"] != 1 || seen["daily"] != 1 || len(seen) != 2 {
		t.Fatalf("scheduled cycles by tier = %v, want one of each", seen)
	}
}

// Stop ends a wait without running or counting anything, and does not wait for
// the claim or the next tick to do so. Start after Stop arms a fresh stop.
func TestSchedulerStopEndsADeferredTick(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)
	manager.ManifestManager = nil
	claim, err := manager.ClaimCycle()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(claim.Release)
	logs := &syncBuffer{}
	sched, err := NewScheduler(&SchedulerConfig{Manager: manager, Schedule: "5 * * * *", TierNames: []string{"hourly"}, Enabled: true, Logger: zerolog.New(logs)})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 { // a lost and regained compactor lease stops and restarts it
		if err := sched.Start(); err != nil {
			t.Fatal(err)
		}
		skips := metrics.Get().CompactionScheduledSkipsFor("hourly")
		done := make(chan struct{})
		stop := sched.stopCh
		go func() {
			defer close(done)
			sched.tick(stop)
		}()
		deadline := time.Now().Add(10 * time.Second)
		for strings.Count(logs.String(), "Scheduled compaction deferred") != i+1 {
			if time.Now().After(deadline) {
				t.Fatal("the tick was not deferred")
			}
			time.Sleep(time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond) // into its wait, past the check before the attempt
		sched.Stop()
		awaitTick(t, done)
		if n := metrics.Get().CompactionScheduledSkipsFor("hourly") - skips; n != 0 {
			t.Fatalf("a stopped tick counted %d skips", n)
		}
	}
	if got := scheduledCycles(manager); len(got) != 0 {
		t.Fatalf("cycles ran after Stop: %v", got)
	}
}

// A tick does not need the lock Stop holds while it waits for running ticks:
// taking it deadlocked a tick that had just started against Stop.
func TestTickGateDoesNotTakeTheSchedulerLock(t *testing.T) {
	sched := &Scheduler{clusterGate: &stubGate{canCompact: false, role: "reader"}, logger: zerolog.Nop()}
	sched.mu.Lock()
	defer sched.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		sched.tick(nil)
	}()
	awaitTick(t, done)
}

// A release closes the signal taken before it, once, whoever holds the claim
// next; a second Release of a claim does not close a later claim's signal.
func TestCycleReleaseSignal(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)
	first, err := manager.ClaimCycle()
	if err != nil {
		t.Fatal(err)
	}
	firstSignal := manager.cycleReleaseSignal()
	first.Release()
	select {
	case <-firstSignal:
	default:
		t.Fatal("a release did not close the signal taken before it")
	}

	second, err := manager.ClaimCycle()
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	secondSignal := manager.cycleReleaseSignal()
	first.Release() // already released: a no-op
	select {
	case <-secondSignal:
		t.Fatal("a repeated Release closed the next claim's signal")
	default:
	}
}

// The release signal is taken before the claim attempt: a release that lands
// between a refused attempt and the wait still wakes the tick, which then
// runs. Taken after the attempt, the signal would be one no release closes,
// and the tick would wait for its next tick.
func TestDeferredTickSeesAReleaseBetweenItsAttemptAndItsWait(t *testing.T) {
	r := newDeferredTickRig(t, "daily", nil)
	r.sched.nextTickForTest = func() time.Time { return time.Now().Add(2 * time.Second) }
	var once sync.Once
	r.sched.afterClaimRefusedForTest = func() { once.Do(r.claim.Release) }
	awaitTick(t, r.tick())
	if got := scheduledCycles(r.manager); len(got) != 1 {
		t.Fatalf("cycles: %v, want the deferred tick's one", got)
	}
	if strings.Contains(r.logs.String(), "Scheduled compaction skipped") {
		t.Fatal("the tick waited for its next tick")
	}
}

// Stop and a release ready together: the check before each attempt stops the
// tick whichever the wait picked, so no cycle starts after Stop.
func TestStoppedDeferredTickNeverRunsOnARelease(t *testing.T) {
	for i := 0; i < 40; i++ {
		r := newDeferredTickRig(t, "hourly", nil)
		var once sync.Once
		r.sched.afterClaimRefusedForTest = func() {
			once.Do(func() {
				close(r.stop)
				r.claim.Release()
			})
		}
		awaitTick(t, r.tick())
		if got := scheduledCycles(r.manager); len(got) != 0 {
			t.Fatalf("run %d: a cycle started after Stop: %v", i, got)
		}
	}
}

// The deadline and a release ready together: the check before each attempt
// gives the tick up, so it never runs after its next tick has fired.
func TestDeferredTickPastItsNextTickNeverRuns(t *testing.T) {
	for i := 0; i < 40; i++ {
		r := newDeferredTickRig(t, "hourly", nil)
		r.sched.nextTickForTest = func() time.Time { return time.Now().Add(20 * time.Millisecond) }
		var once sync.Once
		r.sched.afterClaimRefusedForTest = func() {
			once.Do(func() {
				time.Sleep(40 * time.Millisecond) // past the next tick
				r.claim.Release()
			})
		}
		awaitTick(t, r.tick())
		if got := scheduledCycles(r.manager); len(got) != 0 {
			t.Fatalf("run %d: a tick ran after its next tick: %v", i, got)
		}
	}
}

// StopAll tells every scheduler to stop before waiting for any: when the
// first waits for its running cycle, whose end releases the claim, a deferred
// tick of the second must already have been told to stop, or it would start a
// cycle of its own during shutdown.
func TestStopAllSignalsEverySchedulerBeforeWaiting(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)
	manager.ManifestManager = nil
	newSched := func(tier string) *Scheduler {
		s, err := NewScheduler(&SchedulerConfig{Manager: manager, Schedule: "5 * * * *", TierNames: []string{tier}, Enabled: true, Logger: zerolog.New(&syncBuffer{})})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		return s
	}
	hourly, daily := newSched("hourly"), newSched("daily")
	dailyStop := daily.stopCh
	signalled := func(ch chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	var hourlyWaitSawDaily, dailyWaitSawHourly bool
	hourlyStop := hourly.stopCh
	hourly.beforeStopWaitForTest = func() { hourlyWaitSawDaily = signalled(dailyStop) }
	daily.beforeStopWaitForTest = func() { dailyWaitSawHourly = signalled(hourlyStop) }
	StopAll(hourly, daily)
	if !hourlyWaitSawDaily || !dailyWaitSawHourly {
		t.Fatalf("a scheduler waited before the other was told to stop (hourly saw daily %v, daily saw hourly %v)", hourlyWaitSawDaily, dailyWaitSawHourly)
	}
}
