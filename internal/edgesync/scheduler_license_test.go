package edgesync

import (
	"context"
	"errors"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/rs/zerolog"
	"sync/atomic"
	"testing"
	"time"
)

type changingSchedulerLicense struct {
	current atomic.Pointer[license.License]
}

func (l *changingSchedulerLicense) CanUseEdgeSyncScheduler() bool {
	return l.current.Load().CanUseEdgeSyncScheduler()
}
func TestSchedulerRefusesUnlicensedStartButManualSyncWorks(t *testing.T) {
	for _, status := range []string{"missing", "expired", "read_only"} {
		t.Run(status, func(t *testing.T) {
			rig := newAgentRig(t)
			var gate SchedulerLicense
			if status != "missing" {
				l := &changingSchedulerLicense{}
				l.current.Store(&license.License{Tier: license.TierEnterprise, Status: status})
				gate = l
			}
			s, err := NewScheduler(SchedulerConfig{Agent: rig.agent, Enabled: true, SyncInterval: time.Second, RetryInterval: time.Second, LicenseClient: gate, Logger: zerolog.Nop()})
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Start(); err != nil {
				t.Fatal(err)
			}
			defer s.Stop()
			if s.IsRunning() {
				t.Fatal("scheduler started without valid paid entitlement")
			}
			if _, err = rig.agent.Run(context.Background()); err != nil {
				t.Fatalf("OSS manual sync blocked: %v", err)
			}
		})
	}
}
func TestSchedulerLicenseLossCancelsAndRenewalResumes(t *testing.T) {
	for _, status := range []string{"expired", "read_only", "missing"} {
		t.Run(status, func(t *testing.T) {
			gate := &changingSchedulerLicense{}
			gate.current.Store(&license.License{Tier: license.TierStarter, Status: "active"})
			started := make(chan int, 16)
			cancelled := make(chan struct{})
			m := &schedulerMetrics{}
			runner := &schedulerRunner{started: started, call: func(ctx context.Context, n int) (*RunResult, error) {
				if n == 1 {
					<-ctx.Done()
					close(cancelled)
					return nil, ctx.Err()
				}
				return &RunResult{HubContacted: true}, nil
			}}
			s, err := NewScheduler(SchedulerConfig{Agent: runner, Enabled: true, SyncInterval: 10 * time.Millisecond, RetryInterval: 5 * time.Millisecond, LicenseClient: gate, Metrics: m, Logger: zerolog.Nop()})
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Start(); err != nil {
				t.Fatal(err)
			}
			defer s.Stop()
			waitSchedulerCall(t, started, 1, time.Second)
			if status == "missing" {
				gate.current.Store(nil)
			} else {
				gate.current.Store(&license.License{Tier: license.TierStarter, Status: status})
			}
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("license loss did not cancel active pass")
			}
			time.Sleep(50 * time.Millisecond)
			if n := runner.calls.Load(); n != 1 {
				t.Fatalf("%d scheduled calls after license loss", n)
			}
			if m.failures.Load() != 0 || m.successes.Load() != 0 {
				t.Fatal("license cancellation counted as hub outcome")
			}
			gate.current.Store(&license.License{Tier: license.TierProfessional, Status: "grace_period"})
			waitSchedulerCall(t, started, 2, time.Second)
		})
	}
}
func TestSchedulerIncompleteOutcomesDoNotRecordSuccess(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result *RunResult
	}{
		{"partial", &RunResult{HubContacted: true, Partial: 1}},
		{"conflict", &RunResult{HubContacted: true, Conflicts: []Conflict{{Path: agentPath}}}},
		{"nil result", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &schedulerMetrics{}
			runner := &schedulerRunner{call: func(context.Context, int) (*RunResult, error) { return tc.result, nil }}
			s := newTestSyncScheduler(t, runner, 100*time.Millisecond, 50*time.Millisecond, nil, m)
			waitSchedulerMetric(t, &m.failures, 1)
			s.Stop()
			if n := m.successes.Load(); n != 0 {
				t.Fatalf("incomplete pass recorded %d successes", n)
			}
		})
	}
}
func TestSchedulerEmptyBacklogDoesNotRecordHubSuccess(t *testing.T) {
	rig := newAgentRig(t)
	m := &schedulerMetrics{}
	runner := &schedulerRunner{started: make(chan int, 8), call: func(ctx context.Context, _ int) (*RunResult, error) { return rig.agent.Run(ctx) }}
	s := newTestSyncScheduler(t, runner, 10*time.Millisecond, 5*time.Millisecond, nil, m)
	waitSchedulerCall(t, runner.started, 1, time.Second)
	waitSchedulerCall(t, runner.started, 2, time.Second)
	s.Stop()
	if m.successes.Load() != 0 || m.failures.Load() != 0 {
		t.Fatal("empty pass without hub contact changed outcome metrics")
	}
}
func TestSchedulerRetryBackoffIsBounded(t *testing.T) {
	s := &Scheduler{syncInterval: 5 * time.Minute, retryInterval: 30 * time.Second}
	for i, want := range []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 5 * time.Minute, 5 * time.Minute} {
		if got := s.retryDelay(i + 1); got != want {
			t.Fatalf("failure %d delay=%s want %s", i+1, got, want)
		}
	}
	s.syncInterval = time.Duration(1<<63 - 1)
	s.retryInterval = s.syncInterval/2 + 1
	if got := s.retryDelay(64); got != s.syncInterval {
		t.Fatalf("large backoff overflow: %s", got)
	}
}

func TestSchedulerUsesExistingPaidLicenseEntitlement(t *testing.T) {
	for _, tier := range []license.Tier{license.TierStarter, license.TierProfessional, license.TierEnterprise, license.TierUnlimited} {
		for _, status := range []string{"active", "grace_period", "expired", "read_only"} {
			t.Run(string(tier)+"/"+status, func(t *testing.T) {
				l := &license.License{Tier: tier, Status: status}
				runner := &schedulerRunner{}
				s, err := NewScheduler(SchedulerConfig{Agent: runner, Enabled: true, LicenseClient: l, SyncInterval: time.Hour, RetryInterval: time.Minute, Logger: zerolog.Nop()})
				if err != nil {
					t.Fatal(err)
				}
				if err = s.Start(); err != nil {
					t.Fatal(err)
				}
				defer s.Stop()
				want := l.CanUseCQScheduler() && l.CanUseRetentionScheduler()
				if got := s.IsRunning(); got != want {
					t.Fatalf("scheduler running=%v, want existing CQ/retention entitlement=%v", got, want)
				}
			})
		}
	}
}

type failedDeliveryThenRetry struct {
	SyncTransport
	calls   atomic.Int64
	release chan struct{}
}

func (f *failedDeliveryThenRetry) Reconcile(ctx context.Context, hub string, pending []*LedgerEntry) (*ReconcileResult, error) {
	if f.calls.Add(1) == 1 {
		return nil, errors.New("hub unavailable")
	}
	select {
	case <-f.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return f.SyncTransport.Reconcile(ctx, hub, pending)
}
func TestSchedulerRealFailedDeliveryKeepsCompactionDeferred(t *testing.T) {
	rig := newAgentRig(t)
	rig.writeFile(t, agentPath, []byte("pending delivery"))
	ctx := context.Background()
	if _, err := rig.agent.Discover(ctx); err != nil {
		t.Fatal(err)
	}
	eligible := NewCompactionEligibility(rig.ledger, DefaultHubID, time.Now().UTC(), zerolog.Nop())
	check := func(want bool) {
		t.Helper()
		got, err := eligible(ctx, []string{agentPath})
		if err != nil {
			t.Fatal(err)
		}
		if got[agentPath] != want {
			t.Fatalf("compaction eligibility=%v, want %v", got[agentPath], want)
		}
	}
	check(false)
	transport := &failedDeliveryThenRetry{SyncTransport: rig.transport, release: make(chan struct{})}
	rig.agent.transport = transport
	m := &schedulerMetrics{}
	s := newTestSyncScheduler(t, rig.agent, 100*time.Millisecond, 20*time.Millisecond, nil, m)
	waitSchedulerMetric(t, &m.failures, 1)
	check(false)
	close(transport.release)
	waitSchedulerMetric(t, &m.successes, 1)
	s.Stop()
	check(true)
}
