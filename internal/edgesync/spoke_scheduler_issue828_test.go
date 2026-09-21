package edgesync

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newSchedulerRigIssue828(t *testing.T, gate func() bool) *SpokeScheduler {
	t.Helper()
	rig := newAgentRig(t)

	s, err := NewSpokeScheduler(SpokeSchedulerConfig{
		Agent:         rig.agent,
		Interval:      5 * time.Minute,
		RetryInterval: 30 * time.Second,
		CanRun:        gate,
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	return s
}

func TestSpokeSchedulerValidatesConfigurationIssue828(t *testing.T) {
	rig := newAgentRig(t)

	cases := []struct {
		name     string
		interval time.Duration
		retry    time.Duration
	}{
		{"zero interval", 0, time.Second},
		{"short interval", time.Millisecond, time.Second},
		{"zero retry", time.Minute, 0},
		{"short retry", time.Minute, time.Millisecond},
		{"equal intervals", time.Minute, time.Minute},
		{"retry longer", time.Minute, 2 * time.Minute},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewSpokeScheduler(SpokeSchedulerConfig{
				Agent:         rig.agent,
				Interval:      tc.interval,
				RetryInterval: tc.retry,
			})
			if err == nil {
				t.Fatal("invalid scheduler configuration was accepted")
			}
		})
	}

	if _, err := NewSpokeScheduler(SpokeSchedulerConfig{
		Interval:      time.Minute,
		RetryInterval: time.Second,
	}); err == nil {
		t.Fatal("nil agent was accepted")
	}
}

func TestSpokeSchedulerRechecksRoleAndBacksOffIssue828(t *testing.T) {
	allowed := false
	s := newSchedulerRigIssue828(t, func() bool { return allowed })

	calls := 0
	s.run = func(context.Context) (*RunResult, error) {
		calls++
		switch calls {
		case 1, 2, 4:
			return nil, errors.New("hub unavailable")
		default:
			return &RunResult{}, nil
		}
	}

	ctx := context.Background()

	if got := s.runOnce(ctx); got != 5*time.Minute || calls != 0 {
		t.Fatalf("gated pass: delay=%s calls=%d", got, calls)
	}

	allowed = true

	if got := s.runOnce(ctx); got != 30*time.Second {
		t.Fatalf("first failure delay=%s, want 30s", got)
	}
	if got := s.runOnce(ctx); got != time.Minute {
		t.Fatalf("second failure delay=%s, want 1m", got)
	}

	allowed = false
	if got := s.runOnce(ctx); got != 5*time.Minute || calls != 2 {
		t.Fatalf("demoted pass: delay=%s calls=%d", got, calls)
	}

	allowed = true
	if got := s.runOnce(ctx); got != 5*time.Minute || s.failures != 0 {
		t.Fatalf("success: delay=%s failures=%d", got, s.failures)
	}

	if got := s.runOnce(ctx); got != 30*time.Second {
		t.Fatalf("backoff did not reset: delay=%s", got)
	}
}

func TestSpokeSchedulerClassifiesIncompletePassIssue828(t *testing.T) {
	s := newSchedulerRigIssue828(t, nil)

	s.run = func(context.Context) (*RunResult, error) {
		return &RunResult{Failed: 1, Partial: 1}, nil
	}

	if got := s.runOnce(context.Background()); got != 30*time.Second {
		t.Fatalf("incomplete pass delay=%s, want 30s", got)
	}
	if s.failures != 1 {
		t.Fatalf("failure count=%d, want 1", s.failures)
	}
}

func TestSpokeSchedulerSkipsConcurrentManualPassIssue828(t *testing.T) {
	rig := newAgentRig(t)

	s, err := NewSpokeScheduler(SpokeSchedulerConfig{
		Agent:         rig.agent,
		Interval:      time.Minute,
		RetryInterval: time.Second,
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()

	rig.agent.SetNamespaceExcluder(func(
		context.Context,
	) (map[string]struct{}, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		return nil, nil
	})

	done := make(chan error, 1)
	go func() {
		_, err := rig.agent.Run(context.Background())
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("manual pass did not start")
	}

	if got := s.runOnce(context.Background()); got != time.Minute {
		t.Fatalf("overlap delay=%s, want 1m", got)
	}
	if s.failures != 0 {
		t.Fatalf("overlap counted as failure: %d", s.failures)
	}

	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("manual pass: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("manual pass did not finish")
	}
}

func TestSpokeSchedulerShutdownJoinsActivePassIssue828(t *testing.T) {
	s := newSchedulerRigIssue828(t, nil)

	// Keep production minimums; shorten only this in-package test timer.
	s.interval = time.Millisecond

	started := make(chan struct{})
	release := make(chan struct{})
	s.run = func(ctx context.Context) (*RunResult, error) {
		close(started)
		<-ctx.Done()
		<-release
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("scheduled pass did not start")
	}

	cancel()

	select {
	case <-done:
		t.Fatal("scheduler returned before active pass exited")
	default:
	}

	close(release)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("scheduler did not join cancelled pass")
	}
}

func TestSpokeSchedulerOutcomeCallbacksIssue828(t *testing.T) {
	s := newSchedulerRigIssue828(t, nil)

	successes := 0
	failures := 0

	s.OnSuccess = func(at time.Time) {
		if at.IsZero() {
			t.Error("success callback received a zero timestamp")
		}
		successes++
	}
	s.OnFailure = func() {
		failures++
	}

	calls := 0
	s.run = func(context.Context) (*RunResult, error) {
		calls++
		switch calls {
		case 1:
			return nil, errors.New("hub unavailable")
		case 2:
			return &RunResult{Failed: 1}, nil
		case 3:
			return &RunResult{HubContacted: true}, nil
		default:
			return nil, ErrSyncAlreadyRunning
		}
	}

	for i := 0; i < 4; i++ {
		s.runOnce(context.Background())
	}

	if successes != 1 || failures != 2 {
		t.Fatalf(
			"callbacks: successes=%d failures=%d, want 1/2",
			successes, failures,
		)
	}
}

func TestSpokeSchedulerEmptyPassDoesNotMarkHubSuccessIssue828(t *testing.T) {
	s := newSchedulerRigIssue828(t, nil)

	successes := 0
	failures := 0
	s.OnSuccess = func(time.Time) { successes++ }
	s.OnFailure = func() { failures++ }

	s.run = func(context.Context) (*RunResult, error) {
		return &RunResult{}, nil
	}

	if got := s.runOnce(context.Background()); got != 5*time.Minute {
		t.Fatalf("empty pass delay = %s, want 5m", got)
	}
	if successes != 0 {
		t.Fatalf("empty pass advanced last-success callback: %d", successes)
	}
	if failures != 0 {
		t.Fatalf("empty pass incorrectly counted as failure: %d", failures)
	}
}
