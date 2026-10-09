package compaction

// Per-cycle lookup (#1162): the manager retains a bounded history of cycle
// outcomes so a cycle id handed back by the trigger can be resolved later.
// Before this, the only retained outcome was the single global lastCycle.

import (
	"context"
	"errors"
	"fmt"
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
// CycleLookup both rely on.
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

// cycleByID and retainedRange narrow the production lookup for tests that
// care about only one of its results.
func cycleByID(t interface{ Helper() }, m *Manager, id int64) (CycleRecord, bool) {
	t.Helper()
	rec, found, _, _, _ := m.CycleLookup(id)
	return rec, found
}

func retainedRange(t interface{ Helper() }, m *Manager) (int64, int64, bool) {
	t.Helper()
	_, _, oldest, newest, hasRange := m.CycleLookup(-1)
	return oldest, newest, hasRange
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

	rec, ok := cycleByID(t, manager, cycleID)
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
			rec, ok := cycleByID(t, manager, cycleID)
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
	rec, ok := cycleByID(t, manager, running)
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
	// Live counters: the batch has started but cannot have finished. Every
	// counter is asserted, not just one -- a resolver that forgot to load
	// Discovered would still satisfy a Started-only check, and would then
	// report a NEGATIVE unstarted_batches.
	if rec.Started != 1 {
		t.Errorf("started_batches = %d, want 1 (live counters must be readable)", rec.Started)
	}
	if rec.Discovered != 1 {
		t.Errorf("discovered_batches = %d, want 1 while running", rec.Discovered)
	}
	if rec.Unstarted < 0 {
		t.Errorf("unstarted_batches = %d, must never be negative", rec.Unstarted)
	}
	if rec.Succeeded != 0 {
		t.Errorf("succeeded_batches = %d, want 0 while the batch is blocked", rec.Succeeded)
	}

	close(release)
	finished := <-done
	if finished != running {
		t.Fatalf("finished cycle %d != running cycle %d", finished, running)
	}

	rec, ok = cycleByID(t, manager, finished)
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
// append branch and write TWO records per cycle; a lookup returns one of them,
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
	second := make(chan struct{})
	go func() {
		defer close(second)
		_, _ = manager.RunCompactionCycleForDatabase(context.Background(), "db1", []string{"hourly"})
	}()

	awaitBatch(t, entered)

	outcome := cycleOutcomeIssue915(t, manager)
	if status := outcome["status"]; status == "running" {
		t.Error("last_cycle reports running; the new status must stay in the cycle ring")
	}
	if got := outcome["cycle_id"]; got != first {
		t.Errorf("last_cycle.cycle_id = %v, want %d (the previous finished cycle)", got, first)
	}

	// Waited for, not abandoned: historyRig closes the storage backend in
	// t.Cleanup, and a cycle still running past the test would outlive it.
	close(release)
	<-second
}

// TestCycleRecordNamesFailedPartitionsIssue1162 covers the follow-up question
// after failed_batches: WHICH partitions. Deduplicated by partition, so this
// is the retry list; job history is the attempt-level detail beneath it.
func TestCycleRecordNamesFailedPartitionsIssue1162(t *testing.T) {
	manager := historyRig(t, func(context.Context, Candidate) error {
		return errors.New("compaction exploded")
	})

	cycleID, err := manager.RunCompactionCycleForDatabase(context.Background(), "db1", []string{"hourly"})
	if err == nil {
		t.Fatal("cycle reported success despite a failing batch")
	}

	rec, ok := cycleByID(t, manager, cycleID)
	if !ok {
		t.Fatalf("cycle %d not retained", cycleID)
	}
	if rec.Status != "failed" {
		t.Errorf("status = %q, want failed", rec.Status)
	}
	if rec.Failed != 1 {
		t.Errorf("failed_batches = %d, want 1", rec.Failed)
	}
	if got := rec.FailedPartitions["db1/cpu/2026/10/08/09"]; got != 1 {
		t.Errorf("failed_partitions[the failing partition] = %d, want 1; map = %v", got, rec.FailedPartitions)
	}
	if len(rec.FailedPartitions) != 1 {
		t.Errorf("failed_partitions holds %d partitions, want 1: %v", len(rec.FailedPartitions), rec.FailedPartitions)
	}
	if rec.FailedPartitionsTruncated {
		t.Error("failed_partitions reported truncated with one partition")
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

// TestCycleLookupReportsAbsenceIssue1162 pins that an empty history is
// distinguishable from a real range. Ids start at 1, so a 0/0 range would make
// every id look newer than the newest retained cycle.
func TestCycleLookupReportsAbsenceIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)

	if _, _, ok := retainedRange(t, manager); ok {
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
	if rec, ok := cycleByID(t, manager, 1); ok {
		t.Errorf("a lookup on an empty history returned %+v", rec)
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

	oldest, newest, ok := retainedRange(t, manager)
	if !ok {
		t.Fatal("no retained range after four cycles")
	}
	if oldest != ids[0] || newest != ids[3] {
		t.Errorf("range = %d..%d, want %d..%d", oldest, newest, ids[0], ids[3])
	}
}

// TestCycleLookupIsRaceFreeIssue1162 reads a running cycle while its workers
// mutate BOTH the atomic counters and failedSample.
//
// The first version of this test asserted the right thing about the wrong
// state. With one candidate and MaxConcurrent 1 there is a single worker: it
// increments started once and then blocks, and succeeded/failed fire only
// after release is closed -- which happened AFTER the readers finished. So no
// counter was written during any read, recordFailure was never called at all,
// and removing the mutex from recordFailure and snapshotFailedSample left the
// whole package passing under -race. failedSample is the only part of
// cycleProgress that is not an atomic, so it was the one thing that needed
// this test and the one thing it did not touch.
//
// Now: many candidates, several workers, every batch FAILS (so recordFailure
// writes), each sleeping briefly to widen the window, and readers spin for the
// whole cycle rather than a fixed count.
func TestCycleLookupIsRaceFreeIssue1162(t *testing.T) {
	const candidates = 60

	manager := historyRig(t, func(context.Context, Candidate) error {
		// Widen the window deliberately: without a pause every failure lands
		// within microseconds of the others and the readers can miss the
		// writes entirely. All 60 write, since 60 is well under the
		// distinct-partition cap.
		time.Sleep(time.Millisecond)
		return errors.New("deliberate failure")
	})
	manager.MaxConcurrent = 4
	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(context.Context, string, string) ([]Candidate, error) {
			out := make([]Candidate, 0, candidates)
			for i := 0; i < candidates; i++ {
				out = append(out, Candidate{
					Database:      "db1",
					Measurement:   "cpu",
					PartitionPath: fmt.Sprintf("db1/cpu/2026/10/08/%02d", i),
					Files:         []string{"a.parquet", "b.parquet"},
					FileCount:     2,
					Tier:          "hourly",
				})
			}
			return out, nil
		},
	}}

	// Readers start BEFORE the cycle and spin until it ends, so overlap does
	// not depend on timing.
	done := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				if p := manager.CyclePage(10); p.HasRunning {
					cycleByID(t2NoFatal{}, manager, p.RunningCycleID)
				}
				manager.CyclePage(0)
			}
		}()
	}

	cycleID, err := manager.RunCompactionCycleForDatabase(context.Background(), "db1", []string{"hourly"})
	close(done)
	readers.Wait()

	if err == nil {
		t.Fatal("cycle reported success despite every batch failing")
	}
	rec, ok := cycleByID(t, manager, cycleID)
	if !ok {
		t.Fatalf("cycle %d not retained", cycleID)
	}
	// Precondition: recordFailure genuinely ran, and filled the sample to its
	// cap. Without this the test could pass having raced nothing.
	if rec.Failed != candidates {
		t.Errorf("failed_batches = %d, want %d", rec.Failed, candidates)
	}
	// Precondition: recordFailure genuinely ran for every distinct partition.
	// 60 candidates are 60 distinct paths, well under the cap, so a complete
	// map is the expected result and a short one means the readers raced
	// nothing.
	if len(rec.FailedPartitions) != candidates {
		t.Fatalf("failed_partitions holds %d partitions, want all %d -- recordFailure did not race the readers",
			len(rec.FailedPartitions), candidates)
	}
	if rec.FailedPartitionsTruncated {
		t.Errorf("reported truncated at %d distinct partitions, under the cap of %d", len(rec.FailedPartitions), cycleFailedPartitionLimit)
	}
	// The exact reconciliation rule, pinned on a real cycle.
	sum := 0
	for _, n := range rec.FailedPartitions {
		sum += n
	}
	if int64(sum) != rec.Failed {
		t.Errorf("sum(failed_partitions) = %d, want failed_batches = %d", sum, rec.Failed)
	}
	if rec.Unstarted < 0 {
		t.Errorf("unstarted_batches = %d, must never be negative", rec.Unstarted)
	}
}

// t2NoFatal lets the reader goroutines call cycleByID, whose signature takes a
// *testing.T only for t.Helper(). Calling t.Fatal off the test goroutine is
// invalid, and these readers assert nothing.
type t2NoFatal struct{}

func (t2NoFatal) Helper() {}

// TestFinalisedRecordDropsItsProgressPointerIssue1162 pins the retention half
// of the progress contract.
//
// CycleRecord.progress documents that the finalizer "clears it, so a finished
// outcome never follows the pointer". Nothing enforced that: leaving the
// pointer on the finalised outcome kept both packages green, because by then
// the live atomics equal the stored values and counters() recomputes the same
// numbers. The cost is invisible and purely retention -- up to
// CycleHistoryLimit cycleProgress structs, each with its failedSample backing
// array, pinned for the life of the process.
func TestFinalisedRecordDropsItsProgressPointerIssue1162(t *testing.T) {
	manager := historyRig(t, func(context.Context, Candidate) error { return nil })

	if _, err := manager.RunCompactionCycleForDatabase(context.Background(), "db1", []string{"hourly"}); err != nil {
		t.Fatalf("cycle failed: %v", err)
	}

	// Inspected in the retained history, not through a lookup: counters()
	// nils the pointer on the COPY it returns, so a lookup can never see this.
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.cycleHistory) != 1 {
		t.Fatalf("history holds %d records, want 1", len(manager.cycleHistory))
	}
	rec := manager.cycleHistory[0]
	if rec.FinishedAt.IsZero() {
		t.Fatal("record is not finalised; this test asserts nothing")
	}
	if rec.progress != nil {
		t.Error("a finalised record still holds its cycleProgress pointer, pinning it for the life of the process")
	}
}
