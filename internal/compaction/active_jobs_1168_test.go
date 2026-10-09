package compaction

import (
	"context"
	"io"
	"path/filepath"
	"sync"
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
//
// The seam depends on m.mu NOT being held at the ConfigJSON call: the tests
// read Stats() while parked. If a refactor takes m.mu around the whole config
// build, these tests deadlock into a package timeout rather than failing
// cleanly -- the bounded selects guard the channel waits, not Stats().
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
	t.Cleanup(func() { _ = b.Close() })
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

// TestActiveJobsIgnoresALockSkippedAttemptIssue1168 checks that a lock-skipped
// attempt leaves the count BALANCED. A partition someone else is compacting
// returns without doing any work and is excluded from totalJobs* for the same
// reason, so it must not leave a residue behind.
//
// It does NOT pin where the increment sits: an increment above the lock is
// undone by the same deferred decrement before the call returns, so the pair
// is invisible to any observer outside the call. That placement is pinned by
// TestActiveJobsWindowStartsAfterTheLockIssue1168, which observes the count
// from inside the skip path.
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

// windowProbe records what ActiveJobs() reported at the moment a given log
// line was written, which is how the ENDS of the in-flight window get pinned.
// Both ends are invisible from outside compactPartition: the increment and its
// deferred decrement are balanced, so a caller that reads the count only after
// the call returns cannot tell where either one sits.
//
// ActiveJobs() takes no lock -- it is a single atomic load -- so reading it
// from inside a zerolog hook cannot deadlock against m.mu, which compaction
// holds across parts of the same function.
type windowProbe struct {
	mu sync.Mutex
	m  *Manager
	at map[string]int64
}

func newWindowProbe() (*windowProbe, zerolog.Logger) {
	p := &windowProbe{at: map[string]int64{}}
	// Debug level: a disabled event never runs its hooks, and the
	// post-subprocess seam this probe relies on logs at Debug.
	return p, zerolog.New(io.Discard).Level(zerolog.DebugLevel).Hook(p)
}

func (p *windowProbe) Run(_ *zerolog.Event, _ zerolog.Level, msg string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.m == nil {
		return
	}
	if _, seen := p.at[msg]; !seen {
		p.at[msg] = p.m.ActiveJobs()
	}
}

func (p *windowProbe) observed(msg string) (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v, ok := p.at[msg]
	return v, ok
}

func (p *windowProbe) messages() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.at))
	for k := range p.at {
		out = append(out, k)
	}
	return out
}

// TestActiveJobsWindowStartsAfterTheLockIssue1168 pins the LEFT edge: the
// increment must sit below AcquireLock, so a partition that is skipped because
// another attempt holds its lock never registers as in flight even briefly.
// Observed from inside the skip path, which is the only place the difference
// is visible.
func TestActiveJobsWindowStartsAfterTheLockIssue1168(t *testing.T) {
	probe, logger := newWindowProbe()
	m := NewManager(&ManagerConfig{
		StorageBackend: localBackend(t),
		LockManager:    NewLockManager(),
		TempDirectory:  t.TempDir(),
		CycleTimeout:   time.Minute,
		Logger:         logger,
	})
	probe.m = m

	candidate := Candidate{
		Database:      "bench",
		Measurement:   "cpu",
		PartitionPath: "bench/cpu/2026/10/08/14",
		Files:         []string{"bench/cpu/2026/10/08/14/a.parquet"},
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

	const skipMsg = "Partition already locked, skipping"
	got, ok := probe.observed(skipMsg)
	if !ok {
		t.Fatalf("the skip path did not log %q; observed %v", skipMsg, probe.messages())
	}
	if got != 0 {
		t.Fatalf("ActiveJobs() inside the skip path = %d, want 0 -- the increment is above the lock", got)
	}
}

// TestActiveJobsWindowEndsAfterTheBookkeepingIssue1168 pins the RIGHT edge:
// the count must still include the attempt after the subprocess has returned,
// while the parent is still marking receipts, deleting the manifest and
// invalidating caches. Observed from the temp-directory cleanup log, which
// runs after the subprocess and before compactPartition returns.
func TestActiveJobsWindowEndsAfterTheBookkeepingIssue1168(t *testing.T) {
	probe, logger := newWindowProbe()
	m := NewManager(&ManagerConfig{
		StorageBackend: localBackend(t),
		LockManager:    NewLockManager(),
		TempDirectory:  t.TempDir(),
		CycleTimeout:   time.Minute,
		Logger:         logger,
	})
	probe.m = m

	candidate := Candidate{
		Database:      "bench",
		Measurement:   "cpu",
		PartitionPath: "bench/cpu/2026/10/08/15",
		Files:         []string{"bench/cpu/2026/10/08/15/a.parquet"},
		FileCount:     1,
		Tier:          "hourly",
	}

	// Cancelled: the subprocess spawn returns immediately without forking, so
	// the post-subprocess bookkeeping runs with no DuckDB involved.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.CompactPartition(ctx, candidate); err == nil {
		t.Fatal("expected the cancelled attempt to fail")
	}

	// Either cleanup outcome logs, and both sit after the subprocess call.
	for _, msg := range []string{
		"Cleaned up subprocess temp directory",
		"Failed to cleanup subprocess temp directory",
	} {
		if got, ok := probe.observed(msg); ok {
			if got != 1 {
				t.Fatalf("ActiveJobs() at %q = %d, want 1 -- the window ends before the bookkeeping", msg, got)
			}
			if after := m.ActiveJobs(); after != 0 {
				t.Fatalf("ActiveJobs() after the attempt returned = %d, want 0", after)
			}
			return
		}
	}
	t.Fatalf("no post-subprocess cleanup line was logged; observed %v", probe.messages())
}
