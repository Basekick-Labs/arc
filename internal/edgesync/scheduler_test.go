package edgesync

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type schedulerRunner struct {
	call    func(context.Context, int) (*RunResult, error)
	calls   atomic.Int64
	started chan int
}

func (r *schedulerRunner) Run(ctx context.Context) (*RunResult, error) {
	n := int(r.calls.Add(1))
	if r.started != nil {
		r.started <- n
	}
	if r.call != nil {
		return r.call(ctx, n)
	}
	return &RunResult{}, nil
}

type schedulerGate struct {
	primary atomic.Bool
}

func (g *schedulerGate) IsPrimaryWriter() bool { return g.primary.Load() }
func (g *schedulerGate) Role() string          { return "writer" }

type schedulerMetrics struct {
	successes atomic.Int64
	failures  atomic.Int64
}

func (m *schedulerMetrics) RecordEdgeSyncSuccess(time.Time) { m.successes.Add(1) }
func (m *schedulerMetrics) IncEdgeSyncFailure()             { m.failures.Add(1) }

func waitSchedulerCall(t *testing.T, started <-chan int, want int, timeout time.Duration) {
	t.Helper()
	select {
	case got := <-started:
		if got != want {
			t.Fatalf("Run call = %d, want %d", got, want)
		}
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for Run call %d", want)
	}
}

func waitSchedulerMetric(t *testing.T, metric *atomic.Int64, want int64) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		if got := metric.Load(); got == want {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("metric = %d, want %d", metric.Load(), want)
		}
	}
}

func newTestSyncScheduler(t *testing.T, runner SyncRunner, interval, retry time.Duration, gate WriterGate, metrics SchedulerMetrics) *Scheduler {
	t.Helper()
	s, err := NewScheduler(SchedulerConfig{
		Agent:         runner,
		SyncInterval:  interval,
		RetryInterval: retry,
		Enabled:       true,
		ClusterGate:   gate,
		Metrics:       metrics,
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(s.Stop)
	return s
}

func TestSchedulerSkipsTicksWhilePassIsInFlight(t *testing.T) {
	started := make(chan int, 4)
	release := make(chan struct{})
	runner := &schedulerRunner{
		started: started,
		call: func(ctx context.Context, _ int) (*RunResult, error) {
			select {
			case <-release:
				return &RunResult{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	newTestSyncScheduler(t, runner, 15*time.Millisecond, 10*time.Millisecond, nil, nil)
	waitSchedulerCall(t, started, 1, time.Second)

	// Several timer expirations while Run is blocked must not start a second
	// pass or queue work for an immediate catch-up run.
	time.Sleep(60 * time.Millisecond)
	if got := runner.calls.Load(); got != 1 {
		t.Fatalf("Run calls while first pass is blocked = %d, want 1", got)
	}

	close(release)
	waitSchedulerCall(t, started, 2, time.Second)
}

func TestSchedulerRechecksPrimaryWriterOnEveryTick(t *testing.T) {
	started := make(chan int, 4)
	canceled := make(chan struct{})
	metrics := &schedulerMetrics{}
	runner := &schedulerRunner{
		started: started,
		call: func(ctx context.Context, n int) (*RunResult, error) {
			if n == 1 {
				<-ctx.Done()
				close(canceled)
				return nil, ctx.Err()
			}
			return &RunResult{}, nil
		},
	}
	gate := &schedulerGate{}
	gate.primary.Store(true)
	newTestSyncScheduler(t, runner, 20*time.Millisecond, 10*time.Millisecond, gate, metrics)
	waitSchedulerCall(t, started, 1, time.Second)
	gate.primary.Store(false)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("in-flight sync was not canceled after primary-writer demotion")
	}
	time.Sleep(70 * time.Millisecond)
	if got := runner.calls.Load(); got != 1 {
		t.Fatalf("Run calls while demoted = %d, want 1", got)
	}
	if got := metrics.failures.Load(); got != 0 {
		t.Errorf("demotion cancellations counted as hub failures = %d, want 0", got)
	}

	gate.primary.Store(true)
	waitSchedulerCall(t, started, 2, time.Second)
}

func TestSchedulerUsesRetryIntervalThenResetsAfterSuccess(t *testing.T) {
	started := make(chan int, 4)
	metrics := &schedulerMetrics{}
	runner := &schedulerRunner{
		started: started,
		call: func(_ context.Context, n int) (*RunResult, error) {
			if n == 1 {
				return nil, errors.New("hub unavailable")
			}
			return &RunResult{}, nil
		},
	}
	newTestSyncScheduler(t, runner, 150*time.Millisecond, 30*time.Millisecond, nil, metrics)
	waitSchedulerCall(t, started, 1, time.Second)
	waitSchedulerCall(t, started, 2, 100*time.Millisecond)
	if got := metrics.failures.Load(); got != 1 {
		t.Errorf("failure metric = %d, want 1", got)
	}
	if got := metrics.successes.Load(); got != 1 {
		t.Errorf("success metric = %d, want 1", got)
	}

	// A successful pass restores the normal interval instead of continuing to
	// poll at the shorter retry interval.
	select {
	case got := <-started:
		t.Fatalf("unexpected Run call %d before the normal interval", got)
	case <-time.After(70 * time.Millisecond):
	}
	waitSchedulerCall(t, started, 3, time.Second)
}

func TestSchedulerRetriesWhenPassReportsTransferFailures(t *testing.T) {
	started := make(chan int, 4)
	allowSuccess := make(chan struct{})
	metrics := &schedulerMetrics{}
	runner := &schedulerRunner{
		started: started,
		call: func(ctx context.Context, n int) (*RunResult, error) {
			if n == 1 {
				// Agent.Run returns per-file transfer errors in the result while
				// keeping its pass-level error nil.
				return &RunResult{Failed: 1}, nil
			}
			if n == 2 {
				select {
				case <-allowSuccess:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &RunResult{}, nil
		},
	}
	newTestSyncScheduler(t, runner, 150*time.Millisecond, 30*time.Millisecond, nil, metrics)
	waitSchedulerCall(t, started, 1, time.Second)
	waitSchedulerCall(t, started, 2, 100*time.Millisecond)
	waitSchedulerMetric(t, &metrics.failures, 1)
	if got := metrics.successes.Load(); got != 0 {
		t.Fatalf("failed pass updated last-success count = %d, want 0", got)
	}
	close(allowSuccess)
	waitSchedulerMetric(t, &metrics.successes, 1)

	// The pass after a reported transfer failure succeeds and restores the
	// normal interval instead of polling again at the retry cadence.
	select {
	case got := <-started:
		t.Fatalf("unexpected Run call %d before the normal interval", got)
	case <-time.After(70 * time.Millisecond):
	}
	waitSchedulerCall(t, started, 3, time.Second)
	waitSchedulerMetric(t, &metrics.successes, 2)
}

func TestSchedulerFailedPassKeepsCompactionDeferredUntilDelivery(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)
	path := "metrics/cpu/2026/08/07/14/scheduler-gate.parquet"
	rig.writeFile(t, path, []byte("parquet payload awaiting delivery"))
	if _, err := rig.agent.Discover(ctx); err != nil {
		t.Fatalf("discover pending file: %v", err)
	}

	epoch := time.Now().UTC().Add(-time.Hour)
	compactionEligible := NewCompactionEligibility(rig.ledger, DefaultHubID, epoch, zerolog.Nop())
	checkEligibility := func() bool {
		t.Helper()
		eligible, err := compactionEligible(ctx, []string{path})
		if err != nil {
			t.Fatalf("check compaction eligibility: %v", err)
		}
		return eligible[path]
	}
	if checkEligibility() {
		t.Fatal("pending file became eligible before delivery")
	}

	allowSuccess := make(chan struct{})
	secondRunEntered := make(chan struct{})
	successfulRun := make(chan struct{})
	started := make(chan int, 3)
	runner := &schedulerRunner{
		started: started,
		call: func(runCtx context.Context, n int) (*RunResult, error) {
			if n == 1 {
				return nil, errors.New("hub unavailable")
			}
			if n == 2 {
				close(secondRunEntered)
				select {
				case <-allowSuccess:
				case <-runCtx.Done():
					return nil, runCtx.Err()
				}
			}
			result, err := rig.agent.Run(runCtx)
			if n == 2 && err == nil {
				close(successfulRun)
			}
			return result, err
		},
	}
	metrics := &schedulerMetrics{}
	newTestSyncScheduler(t, runner, 100*time.Millisecond, 10*time.Millisecond, nil, metrics)
	waitSchedulerCall(t, started, 1, time.Second)
	waitSchedulerCall(t, started, 2, time.Second)
	select {
	case <-secondRunEntered:
	case <-time.After(time.Second):
		t.Fatal("retry pass did not start")
	}
	if got := metrics.failures.Load(); got != 1 {
		t.Fatalf("failure metric = %d, want 1 before retry succeeds", got)
	}
	if checkEligibility() {
		t.Fatal("failed scheduled pass released the compaction defer gate")
	}

	close(allowSuccess)
	select {
	case <-successfulRun:
	case <-time.After(time.Second):
		t.Fatal("successful retry pass did not finish")
	}
	if !checkEligibility() {
		t.Fatal("file remained ineligible after the agent delivered it")
	}
	waitSchedulerMetric(t, &metrics.successes, 1)
}

func TestSchedulerStopCancelsAndWaitsForPass(t *testing.T) {
	started := make(chan int, 1)
	finished := make(chan struct{})
	runner := &schedulerRunner{
		started: started,
		call: func(ctx context.Context, _ int) (*RunResult, error) {
			defer close(finished)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	s := newTestSyncScheduler(t, runner, 10*time.Millisecond, 10*time.Millisecond, nil, nil)
	waitSchedulerCall(t, started, 1, time.Second)
	s.Stop()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("Stop returned before the in-flight pass exited")
	}
	if s.IsRunning() {
		t.Fatal("scheduler is still running after Stop")
	}
}

func TestAgentRunRejectsConcurrentPass(t *testing.T) {
	agent := &Agent{}
	agent.runActive.Store(true)
	if _, err := agent.Run(context.Background()); !errors.Is(err, ErrAgentRunInProgress) {
		t.Fatalf("Run error = %v, want ErrAgentRunInProgress", err)
	}
}

func TestNewSchedulerRejectsNonPositiveIntervals(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		interval   time.Duration
		retry      time.Duration
	}{
		{name: "sync", want: "sync interval", interval: 0, retry: time.Second},
		{name: "retry", want: "retry interval", interval: time.Second, retry: -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewScheduler(SchedulerConfig{SyncInterval: tc.interval, RetryInterval: tc.retry})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NewScheduler error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}
