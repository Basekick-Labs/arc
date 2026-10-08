package compaction

// Per-cycle lookup (#1162): the manager retains a bounded history of cycle
// outcomes so a cycle id handed back by the trigger can be resolved later.
// Before this, the only retained outcome was the single global lastCycle.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestAppendCycleHistoryBoundsAtTheLimitIssue1162 pins the ring bound and that
// trimming drops the oldest -- the invariant the in-place finalizer update and
// RetainedCycleRange both rely on.
func TestAppendCycleHistoryBoundsAtTheLimitIssue1162(t *testing.T) {
	var history []CycleRecord
	total := CycleHistoryLimit + 25
	for i := 1; i <= total; i++ {
		history = appendCycleHistory(history, CycleRecord{CycleID: int64(i)})
	}

	if len(history) != CycleHistoryLimit {
		t.Fatalf("history length = %d, want %d", len(history), CycleHistoryLimit)
	}
	if got, want := history[len(history)-1].CycleID, int64(total); got != want {
		t.Errorf("newest retained = %d, want %d (newest must stay last)", got, want)
	}
	if got, want := history[0].CycleID, int64(total-CycleHistoryLimit+1); got != want {
		t.Errorf("oldest retained = %d, want %d", got, want)
	}
	for i := 1; i < len(history); i++ {
		if history[i].CycleID <= history[i-1].CycleID {
			t.Fatalf("history not monotonic at %d: %d then %d", i, history[i-1].CycleID, history[i].CycleID)
		}
	}
}

// historyRig is a manager whose single tier yields one candidate and whose
// batch body is supplied by the test.
//
// The db1/cpu directory has to exist on disk. RunCompactionCycleForDatabase
// filters to a database but still enumerates that database's MEASUREMENTS from
// storage, so over an empty root it finds none, never consults the tier, and
// never runs a batch -- a cycle that completes instantly having done nothing.
// Tests that block inside a batch would then wait forever on a batch that is
// never entered.
func historyRig(t *testing.T, compact func(context.Context, Candidate) error) *Manager {
	t.Helper()
	root := t.TempDir()
	partition := filepath.Join(root, "db1", "cpu", "2026", "10", "08", "09")
	if err := os.MkdirAll(partition, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partition, "a.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	manager := NewManager(&ManagerConfig{
		StorageBackend:   backend,
		LockManager:      NewLockManager(),
		MinAgeHours:      1,
		MinFiles:         2,
		MaxFilesPerBatch: 10,
		MaxConcurrent:    1,
		TempDirectory:    filepath.Join(root, "temp"),
		CycleTimeout:     time.Minute,
		Logger:           zerolog.Nop(),
	})
	manager.ManifestManager = nil
	manager.compactBatchForTest = compact
	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(context.Context, string, string) ([]Candidate, error) {
			return []Candidate{{
				Database:      "db1",
				Measurement:   "cpu",
				PartitionPath: "db1/cpu/2026/10/08/09",
				Files:         []string{"a.parquet", "b.parquet"},
				FileCount:     2,
				Tier:          "hourly",
			}}, nil
		},
	}}
	return manager
}

// awaitBatch fails loudly rather than hanging when a batch is never entered.
// A test that blocks inside a batch is meaningless if the batch never ran.
func awaitBatch(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("no batch was entered; the assertion that follows would be meaningless")
	}
}

// TestCycleHistoryRecordsRequestedScopeIssue1162 is the point of the feature:
// counters alone cannot tell one trigger's cycle from another's, so the record
// has to carry the scope that was asked for.
func TestCycleHistoryRecordsRequestedScopeIssue1162(t *testing.T) {
	manager := historyRig(t, func(context.Context, Candidate) error { return nil })

	cycleID, err := manager.RunCompactionCycleForMeasurement(context.Background(), "db1", "cpu", []string{"hourly"})
	if err != nil {
		t.Fatalf("cycle failed: %v", err)
	}

	rec, ok := manager.CycleByID(cycleID)
	if !ok {
		t.Fatalf("cycle %d not retained", cycleID)
	}
	if rec.Status != "completed" {
		t.Errorf("status = %q, want completed", rec.Status)
	}
	if len(rec.Databases) != 1 || rec.Databases[0] != "db1" {
		t.Errorf("databases = %v, want [db1]", rec.Databases)
	}
	if rec.Measurement != "cpu" {
		t.Errorf("measurement = %q, want cpu", rec.Measurement)
	}
	if len(rec.Tiers) != 1 || rec.Tiers[0] != "hourly" {
		t.Errorf("tiers = %v, want [hourly]", rec.Tiers)
	}
	if rec.StartedAt.IsZero() || rec.FinishedAt.IsZero() {
		t.Errorf("timestamps not set: started=%v finished=%v", rec.StartedAt, rec.FinishedAt)
	}
	if rec.FinishedAt.Before(rec.StartedAt) {
		t.Errorf("finished %v before started %v", rec.FinishedAt, rec.StartedAt)
	}
	// The public RunCompactionCycle* family has no production caller, so it
	// must not claim to be the scheduler.
	if rec.Source != cycleSourceUnspecified {
		t.Errorf("source = %q, want %q", rec.Source, cycleSourceUnspecified)
	}
}

// TestCycleHistorySourceDistinguishesCallersIssue1162 covers the field an
// operator uses to tell their own trigger's cycle from the scheduler's.
func TestCycleHistorySourceDistinguishesCallersIssue1162(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*Manager) (int64, error)
		want string
	}{
		{
			name: "api",
			run: func(m *Manager) (int64, error) {
				claim, err := m.ClaimCycle()
				if err != nil {
					return 0, err
				}
				defer claim.Release()
				return claim.ID, m.RunClaimedCycleForDatabase(context.Background(), claim, "db1", []string{"hourly"})
			},
			want: cycleSourceAPI,
		},
		{
			name: "scheduler",
			run: func(m *Manager) (int64, error) {
				return m.runCycleInternal(context.Background(), cycleSourceScheduler, nil, []string{"hourly"})
			},
			want: cycleSourceScheduler,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := historyRig(t, func(context.Context, Candidate) error { return nil })
			cycleID, err := tc.run(manager)
			if err != nil {
				t.Fatalf("cycle failed: %v", err)
			}
			rec, ok := manager.CycleByID(cycleID)
			if !ok {
				t.Fatalf("cycle %d not retained", cycleID)
			}
			if rec.Source != tc.want {
				t.Errorf("source = %q, want %q", rec.Source, tc.want)
			}
		})
	}
}

// TestRunningCycleIsVisibleWithLiveCountersIssue1162 is the lookup an operator
// actually makes: they trigger, are handed an id, and ask what it is doing. It
// must answer with scope AND progress, not just "running".
//
// Gated on the batch having genuinely entered -- a cycle over an empty data dir
// finishes before the assertion runs, which is how the #1153 concurrency test
// first passed for the wrong reason.
func TestRunningCycleIsVisibleWithLiveCountersIssue1162(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	manager := historyRig(t, func(ctx context.Context, _ Candidate) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})

	done := make(chan int64, 1)
	go func() {
		id, _ := manager.RunCompactionCycleForDatabase(context.Background(), "db1", []string{"hourly"})
		done <- id
	}()

	awaitBatch(t, entered)

	page := manager.CyclePage(1)
	if !page.HasRunning {
		t.Fatal("no cycle recorded as running while a batch is blocked inside one")
	}
	running := page.RunningCycleID
	rec, ok := manager.CycleByID(running)
	if !ok {
		t.Fatalf("running cycle %d not retained", running)
	}
	if rec.Status != "running" {
		t.Errorf("status = %q, want running", rec.Status)
	}
	if !rec.FinishedAt.IsZero() {
		t.Errorf("finished_at = %v, want zero while running", rec.FinishedAt)
	}
	if len(rec.Databases) != 1 || rec.Databases[0] != "db1" {
		t.Errorf("databases = %v, want [db1] while running", rec.Databases)
	}
	// Live counters: the batch has started but cannot have finished.
	if rec.Started != 1 {
		t.Errorf("started_batches = %d, want 1 (live counters must be readable)", rec.Started)
	}
	if rec.Succeeded != 0 {
		t.Errorf("succeeded_batches = %d, want 0 while the batch is blocked", rec.Succeeded)
	}

	close(release)
	finished := <-done
	if finished != running {
		t.Fatalf("finished cycle %d != running cycle %d", finished, running)
	}

	rec, ok = manager.CycleByID(finished)
	if !ok {
		t.Fatalf("cycle %d not retained after finishing", finished)
	}
	if rec.Status != "completed" {
		t.Errorf("status = %q, want completed", rec.Status)
	}
	if rec.FinishedAt.IsZero() {
		t.Error("finished_at still zero after the cycle finished")
	}
	if rec.Succeeded != 1 {
		t.Errorf("succeeded_batches = %d, want 1", rec.Succeeded)
	}
	if manager.CyclePage(1).HasRunning {
		t.Error("a finished cycle is still reported as running")
	}
}

// TestCycleHistoryGrowsByExactlyOnePerCycleIssue1162 guards the in-place
// update. If the finalizer matched the wrong id it would take the defensive
// append branch and write TWO records per cycle; CycleByID returns one of them,
// so every other test here would still pass.
func TestCycleHistoryGrowsByExactlyOnePerCycleIssue1162(t *testing.T) {
	manager := historyRig(t, func(context.Context, Candidate) error { return nil })

	for i := 1; i <= 5; i++ {
		if _, err := manager.RunCompactionCycleForDatabase(context.Background(), "db1", []string{"hourly"}); err != nil {
			t.Fatalf("cycle %d failed: %v", i, err)
		}
		if got := manager.CyclePage(0).Retained; got != i {
			t.Fatalf("after %d cycles the history holds %d records, want %d", i, got, i)
		}
	}
}

// TestRunningCycleDoesNotLeakIntoLastCycleIssue1162 pins the additive promise:
// the new "running" status lives only in the ring. last_cycle keeps describing
// the previous FINISHED cycle, which is what its existing consumers expect.
func TestRunningCycleDoesNotLeakIntoLastCycleIssue1162(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var blockSecond atomic.Bool
	var once sync.Once

	manager := historyRig(t, func(ctx context.Context, _ Candidate) error {
		if !blockSecond.Load() {
			return nil
		}
		once.Do(func() { close(entered) })
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})

	first, err := manager.RunCompactionCycleForDatabase(context.Background(), "db1", []string{"hourly"})
	if err != nil {
		t.Fatalf("first cycle failed: %v", err)
	}

	blockSecond.Store(true)
	go manager.RunCompactionCycleForDatabase(context.Background(), "db1", []string{"hourly"})

	awaitBatch(t, entered)

	outcome := cycleOutcomeIssue915(t, manager)
	if status := outcome["status"]; status == "running" {
		t.Error("last_cycle reports running; the new status must stay in the cycle ring")
	}
	if got := outcome["cycle_id"]; got != first {
		t.Errorf("last_cycle.cycle_id = %v, want %d (the previous finished cycle)", got, first)
	}

	close(release)
}

// TestCycleRecordSamplesFailedPartitionsIssue1162 covers the follow-up question
// after failed_batches: WHICH partitions. Job history carries no cycle id and
// Stats caps it at 10 entries, so the sample is the only answer.
func TestCycleRecordSamplesFailedPartitionsIssue1162(t *testing.T) {
	manager := historyRig(t, func(context.Context, Candidate) error {
		return errors.New("compaction exploded")
	})

	cycleID, err := manager.RunCompactionCycleForDatabase(context.Background(), "db1", []string{"hourly"})
	if err == nil {
		t.Fatal("cycle reported success despite a failing batch")
	}

	rec, ok := manager.CycleByID(cycleID)
	if !ok {
		t.Fatalf("cycle %d not retained", cycleID)
	}
	if rec.Status != "failed" {
		t.Errorf("status = %q, want failed", rec.Status)
	}
	if rec.Failed != 1 {
		t.Errorf("failed_batches = %d, want 1", rec.Failed)
	}
	if len(rec.FailedSample) != 1 || rec.FailedSample[0] != "db1/cpu/2026/10/08/09" {
		t.Errorf("failed_sample = %v, want the failing partition path", rec.FailedSample)
	}
	if rec.Err == "" {
		t.Error("cycle error not retained; a failure count with no reason is a dead end")
	}
}

// TestTruncateCycleErrorBoundsRetentionIssue1162 keeps a pathological error
// from being retained CycleHistoryLimit times over.
func TestTruncateCycleErrorBoundsRetentionIssue1162(t *testing.T) {
	short := "boom"
	if got := truncateCycleError(short); got != short {
		t.Errorf("truncateCycleError(%q) = %q, want it unchanged", short, got)
	}
	long := strings.Repeat("x", 5000)
	got := truncateCycleError(long)
	if len(got) >= len(long) {
		t.Errorf("long error not truncated: %d bytes", len(got))
	}
	if !strings.HasSuffix(got, "(truncated)") {
		t.Errorf("truncated error does not say so: %q", got[len(got)-30:])
	}
}

// TestRetainedCycleRangeReportsAbsenceIssue1162 pins that an empty history is
// distinguishable from a real range. Ids start at 1, so a 0/0 range would make
// every id look newer than the newest retained cycle.
func TestRetainedCycleRangeReportsAbsenceIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)

	if _, _, ok := manager.RetainedCycleRange(); ok {
		t.Error("an empty history reported a retained range")
	}
	empty := manager.CyclePage(10)
	if empty.Retained != 0 {
		t.Errorf("empty history count = %d, want 0", empty.Retained)
	}
	if empty.HasRange || empty.HasRunning {
		t.Errorf("empty history reported range=%v running=%v", empty.HasRange, empty.HasRunning)
	}
	if empty.Cycles != nil {
		t.Errorf("empty history returned %v cycles", empty.Cycles)
	}
	if rec, ok := manager.CycleByID(1); ok {
		t.Errorf("CycleByID on an empty history returned %+v", rec)
	}
}

// TestCycleHistoryNewestFirstIssue1162 pins the list ordering and the limit.
func TestCycleHistoryNewestFirstIssue1162(t *testing.T) {
	manager := historyRig(t, func(context.Context, Candidate) error { return nil })

	var ids []int64
	for i := 0; i < 4; i++ {
		id, err := manager.RunCompactionCycleForDatabase(context.Background(), "db1", []string{"hourly"})
		if err != nil {
			t.Fatalf("cycle failed: %v", err)
		}
		ids = append(ids, id)
	}

	page := manager.CyclePage(2).Cycles
	if len(page) != 2 {
		t.Fatalf("CyclePage(2) returned %d records", len(page))
	}
	if page[0].CycleID != ids[3] || page[1].CycleID != ids[2] {
		t.Errorf("got ids %d,%d, want %d,%d (newest first)", page[0].CycleID, page[1].CycleID, ids[3], ids[2])
	}

	oldest, newest, ok := manager.RetainedCycleRange()
	if !ok {
		t.Fatal("no retained range after four cycles")
	}
	if oldest != ids[0] || newest != ids[3] {
		t.Errorf("range = %d..%d, want %d..%d", oldest, newest, ids[0], ids[3])
	}
}

// TestCycleLookupIsRaceFreeIssue1162 reads a running cycle's live counters
// while its workers mutate them.
func TestCycleLookupIsRaceFreeIssue1162(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once

	manager := historyRig(t, func(ctx context.Context, _ Candidate) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})

	done := make(chan struct{})
	go func() {
		manager.RunCompactionCycleForDatabase(context.Background(), "db1", []string{"hourly"})
		close(done)
	}()

	awaitBatch(t, entered)
	var readers sync.WaitGroup
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 50; j++ {
				if p := manager.CyclePage(10); p.HasRunning {
					manager.CycleByID(p.RunningCycleID)
				}
				manager.RetainedCycleRange()
			}
		}()
	}
	readers.Wait()
	close(release)
	<-done
}
