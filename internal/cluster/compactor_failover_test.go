package cluster

import (
	"context"
	"testing"
	"time"

	arcRaft "github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

// stubFailoverFSM is a minimal ClusterFSM for failover tests.
func stubFailoverFSM() *arcRaft.ClusterFSM {
	return arcRaft.NewClusterFSM(zerolog.Nop())
}

// TestCompactorFailover_InitialAssignment verifies that when no active
// compactor is set and a healthy compactor-role node exists, the failover
// manager assigns the lease within a check cycle.
func TestCompactorFailover_InitialAssignment(t *testing.T) {
	registry := NewRegistry(&RegistryConfig{Logger: zerolog.Nop()})
	fsm := stubFailoverFSM()

	// Add a healthy compactor node to the registry.
	compactor := NewNode("compactor-1", "compactor-1", RoleCompactor, "test-cluster")
	compactor.UpdateState(StateHealthy)
	if err := registry.Register(compactor); err != nil {
		t.Fatal(err)
	}

	// We can't use a real Raft node in unit tests, so we verify the
	// selectNewCompactor logic directly.
	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry:           registry,
		RaftFSM:            fsm,
		CheckInterval:      100 * time.Millisecond,
		UnhealthyThreshold: 1,
		Logger:             zerolog.Nop(),
	})

	// selectNewCompactor should pick the healthy compactor.
	selected := mgr.selectNewCompactor("")
	if selected != "compactor-1" {
		t.Errorf("selectNewCompactor: got %q, want compactor-1", selected)
	}
}

// TestCompactorFailover_SelectPreference verifies that RoleCompactor nodes
// are preferred over RoleWriter nodes.
func TestCompactorFailover_SelectPreference(t *testing.T) {
	registry := NewRegistry(&RegistryConfig{Logger: zerolog.Nop()})

	writer := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	writer.UpdateState(StateHealthy)
	registry.Register(writer)

	compactor := NewNode("compactor-1", "compactor-1", RoleCompactor, "test-cluster")
	compactor.UpdateState(StateHealthy)
	registry.Register(compactor)

	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry: registry,
		RaftFSM:  stubFailoverFSM(),
		Logger:   zerolog.Nop(),
	})

	selected := mgr.selectNewCompactor("")
	if selected != "compactor-1" {
		t.Errorf("selectNewCompactor should prefer RoleCompactor, got %q", selected)
	}
}

// TestCompactorFailover_FallbackToWriter verifies that when no healthy
// RoleCompactor nodes exist, RoleWriter is selected.
func TestCompactorFailover_FallbackToWriter(t *testing.T) {
	registry := NewRegistry(&RegistryConfig{Logger: zerolog.Nop()})

	writer := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	writer.UpdateState(StateHealthy)
	registry.Register(writer)

	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry: registry,
		RaftFSM:  stubFailoverFSM(),
		Logger:   zerolog.Nop(),
	})

	selected := mgr.selectNewCompactor("")
	if selected != "writer-1" {
		t.Errorf("selectNewCompactor should fall back to writer, got %q", selected)
	}
}

// TestCompactorFailover_ExcludeFailedNode verifies that the failed
// compactor is excluded from the candidate list.
func TestCompactorFailover_ExcludeFailedNode(t *testing.T) {
	registry := NewRegistry(&RegistryConfig{Logger: zerolog.Nop()})

	compactor1 := NewNode("compactor-1", "compactor-1", RoleCompactor, "test-cluster")
	compactor1.UpdateState(StateHealthy)
	registry.Register(compactor1)

	writer := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	writer.UpdateState(StateHealthy)
	registry.Register(writer)

	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry: registry,
		RaftFSM:  stubFailoverFSM(),
		Logger:   zerolog.Nop(),
	})

	// Excluding compactor-1 should fall back to writer-1.
	selected := mgr.selectNewCompactor("compactor-1")
	if selected != "writer-1" {
		t.Errorf("selectNewCompactor excluding compactor-1: got %q, want writer-1", selected)
	}
}

// TestCompactorFailover_NoCandidate verifies that when no healthy nodes
// exist, selectNewCompactor returns empty.
func TestCompactorFailover_NoCandidate(t *testing.T) {
	registry := NewRegistry(&RegistryConfig{Logger: zerolog.Nop()})

	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry: registry,
		RaftFSM:  stubFailoverFSM(),
		Logger:   zerolog.Nop(),
	})

	selected := mgr.selectNewCompactor("")
	if selected != "" {
		t.Errorf("selectNewCompactor with no nodes: got %q, want empty", selected)
	}
}

// TestCompactorFailover_CooldownPreventsRapidCycling verifies that the
// cooldown period prevents repeated failovers.
func TestCompactorFailover_CooldownPreventsRapidCycling(t *testing.T) {
	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry:       NewRegistry(&RegistryConfig{Logger: zerolog.Nop()}),
		RaftFSM:        stubFailoverFSM(),
		CooldownPeriod: 1 * time.Hour,
		Logger:         zerolog.Nop(),
	})

	// Simulate a completed failover.
	mgr.lastFailoverAt = time.Now()

	// triggerFailoverLocked should skip due to cooldown.
	mgr.mu.Lock()
	mgr.triggerFailoverLocked("old-compactor")
	inProg := mgr.failoverInProg
	mgr.mu.Unlock()

	if inProg {
		t.Error("failover should be skipped during cooldown period")
	}
}

// TestCompactorFailover_StartStop verifies clean lifecycle.
func TestCompactorFailover_StartStop(t *testing.T) {
	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry:      NewRegistry(&RegistryConfig{Logger: zerolog.Nop()}),
		RaftFSM:       stubFailoverFSM(),
		CheckInterval: 50 * time.Millisecond,
		Logger:        zerolog.Nop(),
	})

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !mgr.running.Load() {
		t.Error("expected running=true after Start")
	}

	if err := mgr.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if mgr.running.Load() {
		t.Error("expected running=false after Stop")
	}
}

// TestCompactorFailover_ManualFailoverNoCompactor verifies that manual
// failover returns an error when no active compactor exists.
func TestCompactorFailover_ManualFailoverNoCompactor(t *testing.T) {
	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry: NewRegistry(&RegistryConfig{Logger: zerolog.Nop()}),
		RaftFSM:  stubFailoverFSM(),
		Logger:   zerolog.Nop(),
	})

	err := mgr.TriggerManualFailover()
	if err == nil {
		t.Error("expected error when no active compactor")
	}
}

// TestCompactorFailover_Stats verifies Stats returns expected fields.
func TestCompactorFailover_Stats(t *testing.T) {
	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry: NewRegistry(&RegistryConfig{Logger: zerolog.Nop()}),
		RaftFSM:  stubFailoverFSM(),
		Logger:   zerolog.Nop(),
	})

	stats := mgr.Stats()
	if _, ok := stats["running"]; !ok {
		t.Error("Stats missing 'running' field")
	}
	if _, ok := stats["active_compactor_id"]; !ok {
		t.Error("Stats missing 'active_compactor_id' field")
	}
	if _, ok := stats["cooldown_period"]; !ok {
		t.Error("Stats missing 'cooldown_period' field")
	}
}

// TestCompactorFailover_DefaultConfig verifies sensible defaults.
func TestCompactorFailover_DefaultConfig(t *testing.T) {
	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry: NewRegistry(&RegistryConfig{Logger: zerolog.Nop()}),
		RaftFSM:  stubFailoverFSM(),
		Logger:   zerolog.Nop(),
	})

	if mgr.cfg.CheckInterval != 10*time.Second {
		t.Errorf("CheckInterval: got %v, want 10s", mgr.cfg.CheckInterval)
	}
	if mgr.cfg.FailoverTimeout != 30*time.Second {
		t.Errorf("FailoverTimeout: got %v, want 30s", mgr.cfg.FailoverTimeout)
	}
	if mgr.cfg.CooldownPeriod != 60*time.Second {
		t.Errorf("CooldownPeriod: got %v, want 60s", mgr.cfg.CooldownPeriod)
	}
	if mgr.cfg.UnhealthyThreshold != 3 {
		t.Errorf("UnhealthyThreshold: got %d, want 3", mgr.cfg.UnhealthyThreshold)
	}
}
