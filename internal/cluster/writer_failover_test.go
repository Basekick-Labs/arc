package cluster

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func testLogger() zerolog.Logger {
	return zerolog.Nop()
}

func TestWriterFailoverManager_NewDefaults(t *testing.T) {
	reg := NewRegistry(&RegistryConfig{
		Logger: testLogger(),
	})

	mgr := NewWriterFailoverManager(&WriterFailoverConfig{
		Registry: reg,
		Logger:   testLogger(),
	})

	if mgr.cfg.HealthCheckInterval != 5*time.Second {
		t.Errorf("expected default health check interval 5s, got %v", mgr.cfg.HealthCheckInterval)
	}
	if mgr.cfg.FailoverTimeout != 30*time.Second {
		t.Errorf("expected default failover timeout 30s, got %v", mgr.cfg.FailoverTimeout)
	}
	if mgr.cfg.CooldownPeriod != 60*time.Second {
		t.Errorf("expected default cooldown 60s, got %v", mgr.cfg.CooldownPeriod)
	}
	if mgr.cfg.UnhealthyThreshold != 3 {
		t.Errorf("expected default unhealthy threshold 3, got %d", mgr.cfg.UnhealthyThreshold)
	}
}

func TestWriterFailoverManager_SelectNewPrimary(t *testing.T) {
	reg := NewRegistry(&RegistryConfig{
		Logger: testLogger(),
	})

	// Create primary writer (will be excluded)
	primary := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	primary.WriterSt = WriterStatePrimary
	primary.UpdateState(StateHealthy)
	reg.Register(primary)

	// Create standby writer
	standby := NewNode("writer-2", "writer-2", RoleWriter, "test-cluster")
	standby.WriterSt = WriterStateStandby
	standby.UpdateState(StateHealthy)
	reg.Register(standby)

	mgr := NewWriterFailoverManager(&WriterFailoverConfig{
		Registry: reg,
		Logger:   testLogger(),
	})

	// Should select writer-2 (standby) when excluding writer-1 (primary)
	selected := mgr.selectNewPrimary("writer-1")
	if selected != "writer-2" {
		t.Errorf("expected writer-2, got %s", selected)
	}

	// Should return empty when no other writers available
	selected = mgr.selectNewPrimary("writer-2")
	// writer-1 is primary, not standby, but it's healthy — falls back to any healthy writer
	if selected != "writer-1" {
		t.Errorf("expected writer-1 as fallback, got %s", selected)
	}
}

func TestWriterFailoverManager_SelectNewPrimary_NoStandby(t *testing.T) {
	reg := NewRegistry(&RegistryConfig{
		Logger: testLogger(),
	})

	// Only one writer, and it's the one we're excluding
	primary := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	primary.WriterSt = WriterStatePrimary
	primary.UpdateState(StateHealthy)
	reg.Register(primary)

	mgr := NewWriterFailoverManager(&WriterFailoverConfig{
		Registry: reg,
		Logger:   testLogger(),
	})

	selected := mgr.selectNewPrimary("writer-1")
	if selected != "" {
		t.Errorf("expected empty string (no standby), got %s", selected)
	}
}

func TestWriterFailoverManager_CooldownPreventsFlapping(t *testing.T) {
	reg := NewRegistry(&RegistryConfig{
		Logger: testLogger(),
	})

	mgr := NewWriterFailoverManager(&WriterFailoverConfig{
		Registry:       reg,
		CooldownPeriod: 1 * time.Hour, // Very long cooldown
		Logger:         testLogger(),
	})

	// Simulate a recent failover
	mgr.lastFailoverAt = time.Now()
	mgr.primaryID = "writer-1"
	mgr.consecutiveFails = 10 // Over threshold

	// Should not trigger because of cooldown
	mgr.triggerFailoverLocked()
	if mgr.failoverInProg {
		t.Error("failover should not have been triggered during cooldown")
	}
}

func TestWriterFailoverManager_HandleWriterUnhealthy_IgnoresNonWriter(t *testing.T) {
	reg := NewRegistry(&RegistryConfig{
		Logger: testLogger(),
	})

	mgr := NewWriterFailoverManager(&WriterFailoverConfig{
		Registry:           reg,
		UnhealthyThreshold: 1,
		Logger:             testLogger(),
	})

	// Reader node should be ignored
	reader := NewNode("reader-1", "reader-1", RoleReader, "test-cluster")
	reader.UpdateState(StateUnhealthy)

	mgr.HandleWriterUnhealthy(reader)

	if mgr.consecutiveFails != 0 {
		t.Errorf("expected 0 consecutive fails for non-writer, got %d", mgr.consecutiveFails)
	}
}

func TestWriterFailoverManager_HandleWriterUnhealthy_IgnoresStandby(t *testing.T) {
	reg := NewRegistry(&RegistryConfig{
		Logger: testLogger(),
	})

	mgr := NewWriterFailoverManager(&WriterFailoverConfig{
		Registry:           reg,
		UnhealthyThreshold: 1,
		Logger:             testLogger(),
	})

	// Standby writer going unhealthy should not trigger failover
	standby := NewNode("writer-2", "writer-2", RoleWriter, "test-cluster")
	standby.WriterSt = WriterStateStandby
	standby.UpdateState(StateUnhealthy)

	mgr.HandleWriterUnhealthy(standby)

	if mgr.consecutiveFails != 0 {
		t.Errorf("expected 0 consecutive fails for standby, got %d", mgr.consecutiveFails)
	}
}

func TestWriterState(t *testing.T) {
	node := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")

	// Default state
	if node.GetWriterState() != WriterStateNone {
		t.Errorf("expected empty writer state, got %s", node.GetWriterState())
	}

	// Set primary
	node.SetWriterState(WriterStatePrimary)
	if !node.IsPrimaryWriter() {
		t.Error("expected IsPrimaryWriter to be true")
	}

	// Set standby
	node.SetWriterState(WriterStateStandby)
	if node.IsPrimaryWriter() {
		t.Error("expected IsPrimaryWriter to be false for standby")
	}

	// Non-writer should not be primary
	reader := NewNode("reader-1", "reader-1", RoleReader, "test-cluster")
	reader.SetWriterState(WriterStatePrimary)
	if reader.IsPrimaryWriter() {
		t.Error("reader should not be considered primary writer")
	}
}

func TestRegistryGetPrimaryWriter(t *testing.T) {
	reg := NewRegistry(&RegistryConfig{
		Logger: testLogger(),
	})

	// No writers
	if p := reg.GetPrimaryWriter(); p != nil {
		t.Error("expected nil primary writer for empty registry")
	}

	// Add standby writer
	standby := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	standby.WriterSt = WriterStateStandby
	standby.UpdateState(StateHealthy)
	reg.Register(standby)

	if p := reg.GetPrimaryWriter(); p != nil {
		t.Error("expected nil primary writer when only standby exists")
	}

	// Add primary writer
	primary := NewNode("writer-2", "writer-2", RoleWriter, "test-cluster")
	primary.WriterSt = WriterStatePrimary
	primary.UpdateState(StateHealthy)
	reg.Register(primary)

	p := reg.GetPrimaryWriter()
	if p == nil {
		t.Fatal("expected primary writer")
	}
	if p.ID != "writer-2" {
		t.Errorf("expected writer-2, got %s", p.ID)
	}

	// Unhealthy primary should not be returned
	primary.UpdateState(StateUnhealthy)
	reg.Register(primary)
	if p := reg.GetPrimaryWriter(); p != nil {
		t.Error("expected nil for unhealthy primary writer")
	}
}

func TestRegistryGetStandbyWriters(t *testing.T) {
	reg := NewRegistry(&RegistryConfig{
		Logger: testLogger(),
	})

	primary := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	primary.WriterSt = WriterStatePrimary
	primary.UpdateState(StateHealthy)
	reg.Register(primary)

	s1 := NewNode("writer-2", "writer-2", RoleWriter, "test-cluster")
	s1.WriterSt = WriterStateStandby
	s1.UpdateState(StateHealthy)
	reg.Register(s1)

	s2 := NewNode("writer-3", "writer-3", RoleWriter, "test-cluster")
	s2.WriterSt = WriterStateStandby
	s2.UpdateState(StateHealthy)
	reg.Register(s2)

	standbys := reg.GetStandbyWriters()
	if len(standbys) != 2 {
		t.Errorf("expected 2 standby writers, got %d", len(standbys))
	}
}
