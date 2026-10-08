package compaction

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// parkedBackend blocks inside ConfigJSON, which compactPartition calls while
// building the subprocess config -- after it has taken the partition lock and
// incremented the in-flight count, and before it spawns anything. That makes
// the counter's window observable without a real subprocess: the caller runs
// with an already-cancelled context, so exec.CommandContext.Run returns
// immediately without forking.
type parkedBackend struct {
	storage.Backend
	entered chan struct{}
	release chan struct{}
}

func (b *parkedBackend) ConfigJSON() string {
	close(b.entered)
	<-b.release
	return b.Backend.ConfigJSON()
}

func activeJobsRig(t *testing.T, backend storage.Backend) *Manager {
	t.Helper()
	return NewManager(&ManagerConfig{
		StorageBackend: backend,
		LockManager:    NewLockManager(),
		TempDirectory:  t.TempDir(),
		CycleTimeout:   time.Minute,
		Logger:         zerolog.Nop(),
	})
}

func localBackend(t *testing.T) storage.Backend {
	t.Helper()
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	return b
}

// TestActiveJobsCountsAnInFlightAttemptIssue1168 is the proof that the counter
// actually moves: 1 while an attempt is parked mid-flight, 0 once it returns.
// Before #1168 nothing produced this number at all.
func TestActiveJobsCountsAnInFlightAttemptIssue1168(t *testing.T) {
	parked := &parkedBackend{
		Backend: localBackend(t),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	m := activeJobsRig(t, parked)

	if got := m.ActiveJobs(); got != 0 {
		t.Fatalf("ActiveJobs() before any attempt = %d, want 0", got)
	}

	candidate := Candidate{
		Database:      "bench",
		Measurement:   "cpu",
		PartitionPath: "bench/cpu/2026/10/08/12",
		Files:         []string{"bench/cpu/2026/10/08/12/a.parquet"},
		FileCount:     1,
		Tier:          "hourly",
	}

	// Cancelled up front: the attempt runs its whole body and the subprocess
	// spawn fails instantly, so the test never waits on DuckDB.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- m.CompactPartition(ctx, candidate) }()

	select {
	case <-parked.entered:
	case err := <-done:
		t.Fatalf("attempt returned (%v) without reaching the seam", err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the attempt to reach the seam")
	}

	// The precondition that makes the assertion meaningful: this attempt is
	// genuinely mid-flight, holding its partition lock.
	lockKey := filepath.Join(candidate.Database, candidate.PartitionPath)
	if !m.LockManager.IsLocked(lockKey) {
		t.Fatalf("parked attempt does not hold the lock for %q", lockKey)
	}
	if got := m.ActiveJobs(); got != 1 {
		t.Fatalf("ActiveJobs() with one attempt in flight = %d, want 1", got)
	}
	if got, ok := m.Stats()["active_jobs"].(int64); !ok || got != 1 {
		t.Fatalf("Stats()[active_jobs] in flight = %v (int64=%t), want int64(1)", m.Stats()["active_jobs"], ok)
	}

	close(parked.release)

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the attempt to finish")
	}

	if got := m.ActiveJobs(); got != 0 {
		t.Fatalf("ActiveJobs() after the attempt returned = %d, want 0", got)
	}
	if m.LockManager.IsLocked(lockKey) {
		t.Fatalf("lock for %q still held after the attempt returned", lockKey)
	}
}

// TestActiveJobsIgnoresALockSkippedAttemptIssue1168 pins the increment BELOW
// the lock acquisition. A partition someone else is compacting returns without
// doing any work and is excluded from totalJobs* for the same reason, so it
// must not register as in flight either. This is the case the in-flight test
// above cannot see: moving the increment to the top of compactPartition keeps
// that test green and breaks only this one.
func TestActiveJobsIgnoresALockSkippedAttemptIssue1168(t *testing.T) {
	m := activeJobsRig(t, localBackend(t))

	candidate := Candidate{
		Database:      "bench",
		Measurement:   "cpu",
		PartitionPath: "bench/cpu/2026/10/08/13",
		Files:         []string{"bench/cpu/2026/10/08/13/a.parquet"},
		FileCount:     1,
		Tier:          "hourly",
	}
	lockKey := filepath.Join(candidate.Database, candidate.PartitionPath)

	if !m.LockManager.AcquireLock(lockKey) {
		t.Fatal("could not pre-acquire the partition lock")
	}
	defer m.LockManager.ReleaseLock(lockKey)

	if err := m.CompactPartition(context.Background(), candidate); err != nil {
		t.Fatalf("a lock-skipped attempt returned %v, want nil", err)
	}
	if got := m.ActiveJobs(); got != 0 {
		t.Fatalf("ActiveJobs() after a lock-skipped attempt = %d, want 0", got)
	}
}

// TestStatsCarriesActiveJobsIssue1168 covers the absence that was the bug:
// Stats() never set this key, so /status marshalled null and /jobs asserted a
// type off a nil interface and answered 0 forever.
func TestStatsCarriesActiveJobsIssue1168(t *testing.T) {
	m := activeJobsRig(t, localBackend(t))

	raw, present := m.Stats()["active_jobs"]
	if !present {
		t.Fatal("Stats() has no active_jobs key")
	}
	got, ok := raw.(int64)
	if !ok {
		t.Fatalf("Stats()[active_jobs] is %T, want int64", raw)
	}
	if got != 0 {
		t.Fatalf("Stats()[active_jobs] at rest = %d, want 0", got)
	}
}
