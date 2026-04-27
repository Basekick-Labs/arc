package reconciliation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newSchedulerForTest(t *testing.T, cfg Config, schedule string) *Scheduler {
	t.Helper()
	r := newReconciler(t, cfg, newFakeCoordinator(), newFakeBackend(), &fakeGate{scan: true, sweep: true})
	s, err := NewScheduler(SchedulerConfig{
		Reconciler: r,
		Schedule:   schedule,
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	return s
}

func TestNewScheduler_RejectsBadSchedule(t *testing.T) {
	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared},
		newFakeCoordinator(), newFakeBackend(), &fakeGate{scan: true, sweep: true})
	_, err := NewScheduler(SchedulerConfig{
		Reconciler: r,
		Schedule:   "not a valid cron",
		Logger:     zerolog.Nop(),
	})
	if err == nil {
		t.Fatal("expected invalid schedule to error")
	}
}

func TestNewScheduler_RejectsNilReconciler(t *testing.T) {
	_, err := NewScheduler(SchedulerConfig{Logger: zerolog.Nop()})
	if err == nil {
		t.Fatal("expected nil reconciler to error")
	}
}

func TestNewScheduler_DefaultSchedule(t *testing.T) {
	s := newSchedulerForTest(t, Config{Enabled: true, BackendKind: BackendShared}, "")
	if s.Schedule() != "17 4 * * *" {
		t.Errorf("expected default schedule, got %q", s.Schedule())
	}
}

func TestScheduler_StartIgnoredWhenDisabled(t *testing.T) {
	// Disabled reconciler → Start returns nil but does not arm cron.
	s := newSchedulerForTest(t, Config{Enabled: false, BackendKind: BackendShared}, "")
	if err := s.Start(); err != nil {
		t.Fatalf("Start should be a no-op when disabled, got: %v", err)
	}
	if s.IsRunning() {
		t.Fatal("scheduler should not be marked running when disabled")
	}
	s.Stop() // also a no-op
}

func TestScheduler_StartStop(t *testing.T) {
	s := newSchedulerForTest(t, Config{Enabled: true, BackendKind: BackendShared}, "*/5 * * * *")
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !s.IsRunning() {
		t.Fatal("expected scheduler to be running after Start")
	}
	// Idempotent Start — second call is a no-op.
	if err := s.Start(); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	s.Stop()
	if s.IsRunning() {
		t.Fatal("expected scheduler to be stopped after Stop")
	}
	// Idempotent Stop.
	s.Stop()
}

func TestScheduler_TriggerNowDisabled(t *testing.T) {
	s := newSchedulerForTest(t, Config{Enabled: false, BackendKind: BackendShared}, "")
	_, err := s.TriggerNow(context.Background(), true)
	if !errors.Is(err, ErrDisabled) {
		t.Fatalf("expected ErrDisabled, got: %v", err)
	}
}

func TestScheduler_TriggerNowSucceeds(t *testing.T) {
	s := newSchedulerForTest(t, Config{Enabled: true, BackendKind: BackendShared}, "")
	run, err := s.TriggerNow(context.Background(), true)
	if err != nil {
		t.Fatalf("TriggerNow: %v", err)
	}
	if run == nil || run.ID == "" {
		t.Fatalf("expected a run with ID, got: %+v", run)
	}
}

func TestScheduler_StatusFields(t *testing.T) {
	s := newSchedulerForTest(t, Config{Enabled: true, BackendKind: BackendShared}, "*/5 * * * *")
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()
	// Trigger once so last_run is populated.
	if _, err := s.TriggerNow(context.Background(), true); err != nil {
		t.Fatalf("TriggerNow: %v", err)
	}
	st := s.Status()
	mustHave := []string{
		"enabled", "running", "schedule", "backend_kind", "grace_window",
		"clock_skew_allowance", "max_run_duration", "max_deletes_per_run",
		"manifest_only_dry_run", "in_flight", "next_run", "last_run", "recent_runs",
	}
	for _, k := range mustHave {
		if _, ok := st[k]; !ok {
			t.Errorf("expected status to include %q, got keys: %v", k, statusKeys(st))
		}
	}
}

func TestScheduler_FindRunDelegates(t *testing.T) {
	s := newSchedulerForTest(t, Config{Enabled: true, BackendKind: BackendShared}, "")
	run, err := s.TriggerNow(context.Background(), true)
	if err != nil {
		t.Fatalf("TriggerNow: %v", err)
	}
	got, ok := s.FindRun(run.ID)
	if !ok || got == nil {
		t.Fatalf("expected to find triggered run via scheduler, got: %+v", got)
	}
	if _, ok := s.FindRun("nonexistent"); ok {
		t.Fatal("expected miss for unknown id")
	}
}

func TestScheduler_TickRespectsManifestOnlyDryRun(t *testing.T) {
	// When ManifestOnlyDryRun=true, the cron tick must call Reconcile
	// with dryRun=true regardless of how the scheduler was triggered.
	cfg := Config{
		Enabled:            true,
		BackendKind:        BackendShared,
		ManifestOnlyDryRun: true,
	}
	r := newReconciler(t, cfg, newFakeCoordinator(), newFakeBackend(), &fakeGate{scan: true, sweep: true})
	s, err := NewScheduler(SchedulerConfig{
		Reconciler: r,
		Schedule:   "*/5 * * * *",
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	// Drive one tick directly to avoid waiting for cron.
	s.tick()
	last := r.LastRun()
	if last == nil {
		t.Fatal("expected a run after tick")
	}
	if !last.DryRun {
		t.Errorf("ManifestOnlyDryRun should force dry-run, but DryRun=%v", last.DryRun)
	}
}

func TestScheduler_TickHonorsGate(t *testing.T) {
	// A gated tick should not produce a run.
	r, _ := NewReconciler(
		Config{Enabled: true, BackendKind: BackendShared},
		newFakeCoordinator(), newFakeBackend(),
		&fakeGate{scan: false, sweep: false}, nil, zerolog.Nop(),
	)
	s, err := NewScheduler(SchedulerConfig{
		Reconciler: r,
		Schedule:   "*/5 * * * *",
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	s.tick()
	if r.LastRun() != nil {
		t.Errorf("gated tick should not record a run, got: %+v", r.LastRun())
	}
}

// ---- helpers ----

func statusKeys(m map[string]interface{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestScheduler_ContextTimeoutIsBounded(t *testing.T) {
	// Sanity check the tick honors MaxRunDuration. We can't easily
	// observe the bound from outside, but we can confirm a tick on a
	// reconciler with very small MaxRunDuration completes promptly.
	s := newSchedulerForTest(t,
		Config{Enabled: true, BackendKind: BackendShared, MaxRunDuration: 100 * time.Millisecond},
		"*/5 * * * *")
	start := time.Now()
	s.tick()
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("tick took too long: %v", elapsed)
	}
}
