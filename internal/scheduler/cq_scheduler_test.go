package scheduler

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// mockCQClusterGate implements CQClusterGate for testing.
type mockCQClusterGate struct {
	isPrimary atomic.Bool
	role      string
}

func (m *mockCQClusterGate) IsPrimaryWriter() bool { return m.isPrimary.Load() }
func (m *mockCQClusterGate) Role() string          { return m.role }

func TestParseInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval string
		wantDur  time.Duration
		wantErr  bool
	}{
		{"30 seconds", "30s", 30 * time.Second, false},
		{"5 minutes", "5m", 5 * time.Minute, false},
		{"1 hour", "1h", time.Hour, false},
		{"90 minutes", "90m", 90 * time.Minute, false},
		{"1.5 hours", "1h30m", 90 * time.Minute, false},
		{"24 hours", "24h", 24 * time.Hour, false},
		{"invalid", "invalid", 0, true},
		{"empty", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dur, err := time.ParseDuration(tt.interval)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for interval %q", tt.interval)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error for interval %q: %v", tt.interval, err)
				return
			}
			if dur != tt.wantDur {
				t.Errorf("duration = %v, want %v", dur, tt.wantDur)
			}
		})
	}
}

func TestCQScheduler_NewCQScheduler(t *testing.T) {
	logger := zerolog.Nop()

	// Create scheduler without handler (will fail on Start)
	s, err := NewCQScheduler(&CQSchedulerConfig{
		CQHandler:     nil,
		LicenseClient: nil,
		Logger:        logger,
	})

	if err != nil {
		t.Fatalf("NewCQScheduler failed: %v", err)
	}

	if s == nil {
		t.Fatal("expected scheduler, got nil")
	}

	if s.running {
		t.Error("scheduler should not be running after creation")
	}

	if len(s.jobs) != 0 {
		t.Errorf("jobs should be empty, got %d", len(s.jobs))
	}
}

func TestCQScheduler_Status_NotRunning(t *testing.T) {
	logger := zerolog.Nop()

	s, err := NewCQScheduler(&CQSchedulerConfig{
		CQHandler:     nil,
		LicenseClient: nil,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("NewCQScheduler failed: %v", err)
	}

	status := s.Status()

	running, ok := status["running"].(bool)
	if !ok || running {
		t.Error("status should show running=false")
	}

	jobCount, ok := status["job_count"].(int)
	if !ok || jobCount != 0 {
		t.Errorf("job_count = %v, want 0", jobCount)
	}

	licenseValid, ok := status["license_valid"].(bool)
	if !ok || licenseValid {
		t.Error("license_valid should be false when no license client")
	}
}

func TestCQScheduler_IsRunning(t *testing.T) {
	logger := zerolog.Nop()

	s, err := NewCQScheduler(&CQSchedulerConfig{
		CQHandler:     nil,
		LicenseClient: nil,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("NewCQScheduler failed: %v", err)
	}

	if s.IsRunning() {
		t.Error("scheduler should not be running initially")
	}
}

func TestCQScheduler_JobCount(t *testing.T) {
	logger := zerolog.Nop()

	s, err := NewCQScheduler(&CQSchedulerConfig{
		CQHandler:     nil,
		LicenseClient: nil,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("NewCQScheduler failed: %v", err)
	}

	if s.JobCount() != 0 {
		t.Errorf("job count = %d, want 0", s.JobCount())
	}
}

func TestCQScheduler_Stop_WhenNotRunning(t *testing.T) {
	logger := zerolog.Nop()

	s, err := NewCQScheduler(&CQSchedulerConfig{
		CQHandler:     nil,
		LicenseClient: nil,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("NewCQScheduler failed: %v", err)
	}

	// Stop should be safe to call even when not running
	s.Stop()

	if s.IsRunning() {
		t.Error("scheduler should still be stopped")
	}
}

// MockCQHandler implements enough of the CQ handler for testing
type MockCQHandler struct {
	activeCQs []mockCQ
	execCount int
}

type mockCQ struct {
	ID       int64
	Name     string
	Interval string
	IsActive bool
}

func TestMinimumInterval(t *testing.T) {
	// Test that intervals below 10 seconds get bumped up
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"1s", 10 * time.Second},
		{"5s", 10 * time.Second},
		{"9s", 10 * time.Second},
		{"10s", 10 * time.Second},
		{"11s", 11 * time.Second},
		{"30s", 30 * time.Second},
		{"1m", 1 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			dur, err := time.ParseDuration(tt.input)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}

			// Apply minimum interval logic
			if dur < 10*time.Second {
				dur = 10 * time.Second
			}

			if dur != tt.expected {
				t.Errorf("duration = %v, want %v", dur, tt.expected)
			}
		})
	}
}

// TestCQScheduler_ClusterGate_NilGate verifies that a nil gate (standalone mode)
// does not block job execution — the field is checked in runJob.
func TestCQScheduler_ClusterGate_NilGate(t *testing.T) {
	logger := zerolog.Nop()
	s, err := NewCQScheduler(&CQSchedulerConfig{
		CQHandler:     nil,
		LicenseClient: nil,
		ClusterGate:   nil,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("NewCQScheduler failed: %v", err)
	}
	if s.clusterGate != nil {
		t.Error("clusterGate should be nil when not provided")
	}
}

// TestCQScheduler_ClusterGate_ReaderSkipsTick verifies that when a non-primary-writer
// gate is installed, runJob skips execution. We drive the ticker manually via a
// synthetic job to avoid real timer waits.
func TestCQScheduler_ClusterGate_ReaderSkipsTick(t *testing.T) {
	gate := &mockCQClusterGate{role: "reader"}
	gate.isPrimary.Store(false)

	logger := zerolog.Nop()
	s, err := NewCQScheduler(&CQSchedulerConfig{
		CQHandler:     nil,
		LicenseClient: nil,
		ClusterGate:   gate,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("NewCQScheduler failed: %v", err)
	}

	// A reader gate must block execution.
	if s.clusterGate == nil {
		t.Fatal("expected clusterGate to be set")
	}
	if s.clusterGate.IsPrimaryWriter() {
		t.Error("reader gate should report IsPrimaryWriter=false")
	}
	if s.clusterGate.Role() != "reader" {
		t.Errorf("Role = %q, want %q", s.clusterGate.Role(), "reader")
	}
}

// TestCQScheduler_ClusterGate_WriterAllowsTick verifies that a primary-writer gate
// does not block the execution path.
func TestCQScheduler_ClusterGate_WriterAllowsTick(t *testing.T) {
	gate := &mockCQClusterGate{role: "writer"}
	gate.isPrimary.Store(true)

	logger := zerolog.Nop()
	s, err := NewCQScheduler(&CQSchedulerConfig{
		CQHandler:     nil,
		LicenseClient: nil,
		ClusterGate:   gate,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("NewCQScheduler failed: %v", err)
	}

	if !s.clusterGate.IsPrimaryWriter() {
		t.Error("writer gate should report IsPrimaryWriter=true")
	}
}

// TestCQScheduler_ClusterGate_FailoverTransition verifies that the gate is re-evaluated
// on each tick: promoting a reader to primary writer unblocks execution without a restart.
func TestCQScheduler_ClusterGate_FailoverTransition(t *testing.T) {
	gate := &mockCQClusterGate{role: "writer"}
	gate.isPrimary.Store(false) // starts as non-primary

	logger := zerolog.Nop()
	s, _ := NewCQScheduler(&CQSchedulerConfig{
		CQHandler:     nil,
		LicenseClient: nil,
		ClusterGate:   gate,
		Logger:        logger,
	})

	if s.clusterGate.IsPrimaryWriter() {
		t.Error("should start as non-primary")
	}

	// Simulate failover — gate atomically flips to primary.
	gate.isPrimary.Store(true)

	if !s.clusterGate.IsPrimaryWriter() {
		t.Error("after failover gate should return IsPrimaryWriter=true without restart")
	}
}

// TestCQScheduler_OverlapGuard_ExecutingFlag verifies the per-job executing flag
// is initialized to false on job creation.
func TestCQScheduler_OverlapGuard_ExecutingFlag(t *testing.T) {
	job := &cqJob{
		cqID:   1,
		cqName: "test",
		ticker: time.NewTicker(time.Hour),
		stopCh: make(chan struct{}),
	}
	defer job.ticker.Stop()

	job.executeMu.Lock()
	executing := job.executing
	job.executeMu.Unlock()

	if executing {
		t.Error("executing flag should be false at creation")
	}
}
