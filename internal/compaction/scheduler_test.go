package compaction

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
)

// stubGate is a minimal ClusterGate for testing the role gate. Set canCompact
// directly to flip between allowed and gated states.
type stubGate struct {
	canCompact bool
	role       string
}

func (s *stubGate) CanCompact() bool { return s.canCompact }
func (s *stubGate) Role() string     { return s.role }

func TestScheduler_StartGatedWhenRoleRejects(t *testing.T) {
	gate := &stubGate{canCompact: false, role: "reader"}
	sched, err := NewScheduler(&SchedulerConfig{
		Manager:     nil, // not touched on the gated path
		Schedule:    "5 * * * *",
		Enabled:     true,
		ClusterGate: gate,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	if err := sched.Start(); err != nil {
		t.Fatalf("Start: %v (gated start should return nil, not error)", err)
	}
	if sched.IsRunning() {
		t.Error("IsRunning: got true, want false (gated scheduler must stay idle)")
	}
	status := sched.Status()
	if gated, _ := status["role_gated"].(bool); !gated {
		t.Errorf("Status[role_gated]: got %v, want true", status["role_gated"])
	}
	if role, _ := status["gate_role"].(string); role != "reader" {
		t.Errorf("Status[gate_role]: got %q, want %q", role, "reader")
	}
}

func TestScheduler_StartAllowedWhenRoleMatches(t *testing.T) {
	gate := &stubGate{canCompact: true, role: "compactor"}
	sched, err := NewScheduler(&SchedulerConfig{
		Manager:     nil,
		Schedule:    "5 * * * *",
		Enabled:     true,
		ClusterGate: gate,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	if err := sched.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer sched.Stop()

	if !sched.IsRunning() {
		t.Error("IsRunning: got false, want true (compactor role should be allowed)")
	}
	status := sched.Status()
	if gated, _ := status["role_gated"].(bool); gated {
		t.Errorf("Status[role_gated]: got true, want false for compactor role")
	}
}

func TestScheduler_StartNoGateMeansAllowed(t *testing.T) {
	// OSS / standalone path: no ClusterGate wired. Scheduler must start
	// exactly as it did pre-Phase-4, with no role gate visible in Status.
	sched, err := NewScheduler(&SchedulerConfig{
		Manager:  nil,
		Schedule: "5 * * * *",
		Enabled:  true,
		Logger:   zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	if err := sched.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer sched.Stop()

	if !sched.IsRunning() {
		t.Error("IsRunning: got false, want true (OSS path must start unconditionally)")
	}
	status := sched.Status()
	if _, ok := status["role_gated"]; ok {
		t.Errorf("Status[role_gated] should be absent when no gate is wired, got %v", status["role_gated"])
	}
}

func TestScheduler_TriggerNowRejectedWhenGated(t *testing.T) {
	gate := &stubGate{canCompact: false, role: "writer"}
	sched, err := NewScheduler(&SchedulerConfig{
		Manager:     nil,
		Schedule:    "5 * * * *",
		Enabled:     true,
		ClusterGate: gate,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	_, err = sched.TriggerNow(context.Background())
	if err == nil {
		t.Fatal("TriggerNow should reject gated trigger, got nil error")
	}
	if !errors.Is(err, ErrCompactionRoleGated) {
		t.Errorf("TriggerNow error: got %v, want ErrCompactionRoleGated", err)
	}
}

func TestScheduler_DisabledShortCircuitsBeforeGate(t *testing.T) {
	// When scheduler is disabled entirely, the gate should never be
	// consulted — `enabled=false` is the outermost check. This test
	// documents the ordering so a future refactor doesn't accidentally
	// make the gate fire for disabled schedulers.
	gate := &stubGate{canCompact: false, role: "reader"}
	sched, err := NewScheduler(&SchedulerConfig{
		Manager:     nil,
		Schedule:    "5 * * * *",
		Enabled:     false, // disabled
		ClusterGate: gate,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	if err := sched.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if sched.IsRunning() {
		t.Error("IsRunning: got true, want false")
	}
	// role_gated should NOT be set — we short-circuited before the gate.
	status := sched.Status()
	if gated, ok := status["role_gated"].(bool); ok && gated {
		t.Errorf("Status[role_gated]: got true, but scheduler was disabled before the gate check")
	}
}

// The cron path must consult the cluster gate on every tick, not only at
// Start().
//
// Start() runs before the cluster coordinator exists, so CanCompact falls back
// to the static role capability and a compactor node arms its cron
// unconditionally. If the compactor lease is then held by another node — the
// normal state on a cluster where a writer took it while the compactor was
// still joining — nothing stopped this cron: the lose-the-lease callback only
// fires on the node that HELD the lease. Both nodes compacted the same
// partitions, which is the duplicate-output hazard the lease exists to prevent.
//
// Manager is nil, so a tick that fails to consult the gate dereferences it and
// panics — which is the assertion.
func TestScheduler_CronTickRechecksTheGate(t *testing.T) {
	gate := &stubGate{canCompact: true, role: "compactor"}
	sched, err := NewScheduler(&SchedulerConfig{
		Manager:     nil, // must not be reached once the gate says no
		Schedule:    "5 * * * *",
		Enabled:     true,
		ClusterGate: gate,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	if err := sched.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { sched.Stop() })

	// The lease moves to another node after this scheduler started.
	gate.canCompact = false

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a cron tick ran compaction after the node lost the lease (panicked on the nil manager): %v", r)
		}
	}()
	sched.runCompaction()
}

// And it still runs when this node does hold the lease, so the check cannot be
// satisfied by refusing everything.
func TestScheduler_CronTickRunsWhenItHoldsTheLease(t *testing.T) {
	gate := &stubGate{canCompact: true, role: "compactor"}
	sched, err := NewScheduler(&SchedulerConfig{
		Manager:     nil,
		Schedule:    "5 * * * *",
		Enabled:     true,
		ClusterGate: gate,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	if err := sched.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { sched.Stop() })

	reached := false
	func() {
		defer func() {
			// Reaching the nil manager is the proof that the gate let it
			// through. Anything else is a real failure.
			if r := recover(); r != nil {
				reached = true
			}
		}()
		sched.runCompaction()
	}()

	if !reached {
		t.Error("a node holding the lease did not run compaction; the gate check is refusing everything")
	}
}
