package compaction

// The cluster-wide compaction pause (#1087) as the compaction package sees it:
// a cycle stops at its batch boundaries and does not start while paused, the
// scheduler still arms cron during a pause and skips ticks, TriggerNow is
// refused, and the watcher reports its pending commits.

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

	"github.com/rs/zerolog"
)

type pausedCycleRig struct {
	manager *Manager
	paused  atomic.Bool
	starts  atomic.Int32
	started chan struct{}
	release chan struct{}
}

// newPausedCycleRig builds a manager whose first batch blocks until release
// is closed, so a test can flip the pause while a batch is running.
func newPausedCycleRig(t *testing.T, candidates []Candidate) *pausedCycleRig {
	t.Helper()
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)
	manager.ManifestManager = nil
	manager.MaxConcurrent = 1
	rig := &pausedCycleRig{manager: manager, started: make(chan struct{}), release: make(chan struct{})}
	manager.SetPauseGate(rig.paused.Load)
	manager.compactBatchForTest = func(ctx context.Context, _ Candidate) error {
		if rig.starts.Add(1) == 1 {
			close(rig.started)
			select {
			case <-rig.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(context.Context, string, string) ([]Candidate, error) { return candidates, nil },
	}}
	return rig
}

func (rig *pausedCycleRig) runCycle(t *testing.T) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := rig.manager.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"})
		done <- err
	}()
	select {
	case <-rig.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the first batch never started")
	}
	return done
}

func awaitCycle(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the cycle did not finish")
		return nil
	}
}

func assertPausedOutcome(t *testing.T, manager *Manager, err error, wantStarts int32, starts *atomic.Int32) {
	t.Helper()
	if !errors.Is(err, ErrCompactionPaused) {
		t.Fatalf("cycle error = %v, want ErrCompactionPaused", err)
	}
	if got := starts.Load(); got != wantStarts {
		t.Fatalf("batches started = %d, want %d (the running batch finishes, no new one starts)", got, wantStarts)
	}
	outcome := cycleOutcomeIssue915(t, manager)
	if outcome["status"] != "paused" {
		t.Fatalf("status = %v, want paused", outcome["status"])
	}
	for key, want := range map[string]int64{"discovered_batches": 2, "started_batches": 1, "succeeded_batches": 1, "unstarted_batches": 1, "failed_batches": 0} {
		if got, _ := outcome[key].(int64); got != want {
			t.Errorf("%s = %v, want %d", key, outcome[key], want)
		}
	}
	if manager.IsCycleRunning() {
		t.Fatal("cycle still marked running")
	}
	if paused, _ := manager.Stats()["paused"].(bool); !paused {
		t.Fatal("Stats()[paused] is not true while the gate says paused")
	}
}

// Three partitions, one worker: the pause lands while partition one runs and
// the dispatcher is already waiting for capacity behind it, so the check that
// matters is the one made with capacity in hand: partition two must not be
// launched, and partition three must never be discovered, because the node
// acks the pause only once the cycle has ended and discovery must not hold
// that up. The sync-eligibility hook fires as the dispatcher finishes
// discovering partition two, right before it blocks on the semaphore; the
// test then flips the pause and lets batch one end.
func TestCyclePauseStopsBeforeTheNextWorkerIssue1087(t *testing.T) {
	rig := newPausedCycleRig(t, []Candidate{candidateIssue915("partition-one"), candidateIssue915("partition-two"), candidateIssue915("partition-three")})
	secondDiscovered := make(chan struct{})
	var filterCalls atomic.Int32
	rig.manager.SetSyncEligibility(func(_ context.Context, paths []string) (map[string]bool, error) {
		if filterCalls.Add(1) == 2 {
			close(secondDiscovered)
		}
		eligible := make(map[string]bool, len(paths))
		for _, p := range paths {
			eligible[p] = true
		}
		return eligible, nil
	})
	done := rig.runCycle(t)
	select {
	case <-secondDiscovered:
	case <-time.After(10 * time.Second):
		t.Fatal("the dispatcher never discovered partition two")
	}
	// Let the dispatcher reach the semaphore and block behind batch one.
	time.Sleep(200 * time.Millisecond)
	rig.paused.Store(true)
	close(rig.release)
	assertPausedOutcome(t, rig.manager, awaitCycle(t, done), 1, &rig.starts)

	// After the resume the next cycle runs everything.
	rig.paused.Store(false)
	rig.starts.Store(0)
	rig.manager.compactBatchForTest = func(context.Context, Candidate) error { rig.starts.Add(1); return nil }
	if _, err := rig.manager.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"}); err != nil {
		t.Fatalf("cycle after resume: %v", err)
	}
	if rig.starts.Load() != 3 {
		t.Fatalf("batches after resume = %d, want 3", rig.starts.Load())
	}
	if paused, _ := rig.manager.Stats()["paused"].(bool); paused {
		t.Fatal("Stats()[paused] still true after the resume")
	}
}

// One partition split in two batches: the pause lands while batch one runs,
// and the partition goroutine must not start batch two. The dispatcher is
// already waiting at the end of the tier, so the goroutine's skip has to
// surface as the cycle result.
func TestCyclePauseStopsBetweenTheBatchesOfOnePartitionIssue1087(t *testing.T) {
	big := candidateIssue915("partition-one")
	big.Files = []string{"one.parquet", "two.parquet", "three.parquet", "four.parquet"}
	big.FileCount = 4
	rig := newPausedCycleRig(t, []Candidate{big})
	rig.manager.MaxFilesPerBatch = 2
	done := rig.runCycle(t)
	rig.paused.Store(true)
	close(rig.release)
	assertPausedOutcome(t, rig.manager, awaitCycle(t, done), 1, &rig.starts)
}

// A pause that lands while the dispatcher is still discovering (no batch
// running) ends the cycle at the next measurement instead of listing the rest
// of the store first: the node acks the pause only once the cycle has ended.
// Two measurements exist in storage; discovery of the first flips the pause,
// and the second must never be visited.
func TestCyclePauseStopsDiscoveryBetweenMeasurementsIssue1087(t *testing.T) {
	manager, backend, cleanup := setupTestManager(t)
	defer cleanup()
	manager.ManifestManager = nil
	ctx := context.Background()
	for _, p := range []string{"db/cpu/2026/01/01/00/a.parquet", "db/mem/2026/01/01/00/a.parquet"} {
		if err := backend.Write(ctx, p, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	var paused atomic.Bool
	manager.SetPauseGate(paused.Load)
	var finds atomic.Int32
	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(_ context.Context, _, measurement string) ([]Candidate, error) {
			finds.Add(1)
			paused.Store(true) // the pause lands during discovery
			return nil, nil
		},
	}}
	manager.compactBatchForTest = func(context.Context, Candidate) error { t.Error("a batch ran"); return nil }

	_, err := manager.RunCompactionCycleForDatabase(ctx, "db", []string{"hourly"})
	if !errors.Is(err, ErrCompactionPaused) {
		t.Fatalf("err = %v, want ErrCompactionPaused", err)
	}
	if finds.Load() != 1 {
		t.Fatalf("measurements discovered under the pause = %d, want 1 (discovery must stop at the next measurement)", finds.Load())
	}
	if outcome := cycleOutcomeIssue915(t, manager); outcome["status"] != "paused" {
		t.Fatalf("status = %v, want paused", outcome["status"])
	}
}

// A cycle that would start under a pause does not start at all, and is
// recorded as a paused cycle with nothing discovered.
func TestCycleDoesNotStartWhilePausedIssue1087(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()
	manager.ManifestManager = nil
	var paused atomic.Bool
	paused.Store(true)
	manager.SetPauseGate(paused.Load)
	var finds atomic.Int32
	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(context.Context, string, string) ([]Candidate, error) {
			finds.Add(1)
			return []Candidate{candidateIssue915("partition-one")}, nil
		},
	}}
	manager.compactBatchForTest = func(context.Context, Candidate) error { t.Error("a batch ran under the pause"); return nil }

	_, err := manager.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"})
	if !errors.Is(err, ErrCompactionPaused) {
		t.Fatalf("err = %v, want ErrCompactionPaused", err)
	}
	if finds.Load() != 0 {
		t.Fatalf("discovery ran %d times under the pause", finds.Load())
	}
	outcome := cycleOutcomeIssue915(t, manager)
	if outcome["status"] != "paused" {
		t.Fatalf("status = %v, want paused", outcome["status"])
	}
	if got, _ := outcome["discovered_batches"].(int64); got != 0 {
		t.Fatalf("discovered = %d, want 0", got)
	}
	// No gate at all (OSS): never paused.
	manager.SetPauseGate(nil)
	if manager.Paused() {
		t.Fatal("Paused() with no gate")
	}
}

// pausableGate is a ClusterGate that also reports the pause, as main.go's
// compactionClusterGate does.
type pausableGate struct {
	stubGate
	reason string
}

func (g *pausableGate) CompactionPauseReason() string { return g.reason }

// Start during a pause arms cron (the pause is not role gating), a tick
// under the pause is skipped, and the first tick after the resume runs. The
// manager is nil, so a tick that reaches it panics: skipped means no panic,
// ran means a recovered panic.
func TestScheduler_StartDuringPauseArmsCronAndTicksRunAfterResumeIssue1087(t *testing.T) {
	gate := &pausableGate{stubGate: stubGate{canCompact: true, role: "compactor"}, reason: "restore b1 (requested by writer-A, generation 1, expires 2026-10-06T10:02:00Z)"}
	sched, err := NewScheduler(&SchedulerConfig{Manager: nil, Schedule: "5 * * * *", Enabled: true, ClusterGate: gate, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sched.Start(); err != nil {
		t.Fatalf("Start under a pause: %v", err)
	}
	t.Cleanup(sched.Stop)
	if !sched.IsRunning() {
		t.Fatal("Start under a pause did not arm cron; the node would stay idle after the resume")
	}
	if gated, _ := sched.Status()["role_gated"].(bool); gated {
		t.Fatal("a pause was recorded as role gating")
	}

	ran := func() (reached bool) {
		defer func() {
			if r := recover(); r != nil {
				reached = true
			}
		}()
		sched.runCompaction()
		return false
	}
	if ran() {
		t.Fatal("a tick under the pause ran compaction")
	}
	gate.reason = ""
	if !ran() {
		t.Fatal("the first tick after the resume did not run compaction")
	}
}

func TestScheduler_TriggerNowRefusedWhilePausedIssue1087(t *testing.T) {
	gate := &pausableGate{stubGate: stubGate{canCompact: true, role: "compactor"}, reason: "restore b1 (requested by writer-A)"}
	sched, err := NewScheduler(&SchedulerConfig{Manager: nil, Schedule: "5 * * * *", Enabled: true, ClusterGate: gate, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = sched.TriggerNow(context.Background())
	if !errors.Is(err, ErrCompactionPaused) {
		t.Fatalf("TriggerNow under a pause: err = %v, want ErrCompactionPaused", err)
	}
	if err == nil || !strings.Contains(err.Error(), "restore b1") {
		t.Fatalf("err = %v, want it to carry the pause reason", err)
	}
}

type pauseTestBridge struct {
	mu        sync.Mutex
	registers []string
	deletes   []string
}

func (b *pauseTestBridge) RegisterCompactedFile(context.Context, CompactedFile) error { return nil }
func (b *pauseTestBridge) DeleteCompactedSource(context.Context, string, string) error {
	return nil
}
func (b *pauseTestBridge) BatchFileOps(_ context.Context, registers []CompactedFile, deletes []DeleteSourceOp) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range registers {
		b.registers = append(b.registers, r.Path)
	}
	for _, d := range deletes {
		b.deletes = append(b.deletes, d.Path)
	}
	return nil
}

func (b *pauseTestBridge) calls() (registers, deletes []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.registers...), append([]string(nil), b.deletes...)
}

// Under the pause the watcher leaves output_written manifests on disk (a
// fresh watcher instance would otherwise re-issue their phase-1 register,
// for an output a restore may just have removed) and still applies
// sources_deleted ones (that is the drain the pause ack waits for). Once the
// pause ends the output_written manifest proceeds as usual.
func TestWatcherLeavesOutputWrittenManifestsAloneWhilePausedIssue1087(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pending")
	var paused atomic.Bool
	paused.Store(true)
	bridge := &pauseTestBridge{}
	w, err := NewCompletionWatcher(CompletionWatcherConfig{Dir: dir, Bridge: bridge, PauseGate: paused.Load, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	output := func(path string) CompactedOutput {
		return CompactedOutput{Path: path, SHA256: strings.Repeat("a", 64), SizeBytes: 1, Database: "db", Measurement: "cpu", Tier: "hot", PartitionTime: time.Now(), CreatedAt: time.Now()}
	}
	const (
		stuckOutput   = "db/cpu/2026/01/01/00/stuck_compacted.parquet"
		drainedOutput = "db/cpu/2026/01/01/01/done_compacted.parquet"
		drainedInput  = "db/cpu/2026/01/01/01/i1.parquet"
	)
	for _, m := range []*CompletionManifest{
		{JobID: "j_output", State: CompletionStateOutputWritten, Outputs: []CompactedOutput{output(stuckOutput)}},
		{JobID: "j_sources", State: CompletionStateSourcesDeleted, Outputs: []CompactedOutput{output(drainedOutput)}, DeletedSources: []string{drainedInput}},
	} {
		if err := writeCompletionManifest(dir, m); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	w.poll(ctx)
	w.poll(ctx)
	registers, deletes := bridge.calls()
	if strings.Join(registers, ",") != drainedOutput || strings.Join(deletes, ",") != drainedInput {
		t.Fatalf("under the pause: registers=%v deletes=%v, want only the sources_deleted manifest applied (%s, %s)", registers, deletes, drainedOutput, drainedInput)
	}
	if _, err := os.Stat(filepath.Join(dir, "j_sources.json")); !os.IsNotExist(err) {
		t.Fatalf("the applied sources_deleted manifest is still on disk: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "j_output.json")); err != nil {
		t.Fatalf("the output_written manifest must stay on disk under the pause: %v", err)
	}
	sd, ow, err := w.PendingCommits()
	if err != nil || sd != 0 || len(ow) != 1 {
		t.Fatalf("PendingCommits after the drain = %d %v %v, want 0 [j_output]", sd, ow, err)
	}

	paused.Store(false)
	w.poll(ctx)
	registers, _ = bridge.calls()
	if strings.Join(registers, ",") != drainedOutput+","+stuckOutput {
		t.Fatalf("after the resume: registers=%v, want the output_written manifest registered too", registers)
	}
	if _, err := os.Stat(filepath.Join(dir, "j_output.json")); err != nil {
		t.Fatalf("an output_written manifest is kept until sources_deleted (normal two-phase progression): %v", err)
	}
}

// PendingCommits counts sources_deleted manifests (the phase-2 commits the
// pause waits for), lists output_written job IDs (stuck once no cycle runs),
// and ignores writing_output, unreadable files, and a missing directory.
func TestWatcherPendingCommitsCountsStatesIssue1087(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pending")
	w, err := NewCompletionWatcher(CompletionWatcherConfig{Dir: dir, Bridge: &pauseTestBridge{}, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	sd, ow, err := w.PendingCommits()
	if err != nil || sd != 0 || len(ow) != 0 {
		t.Fatalf("missing dir: %d %v %v, want 0 none nil", sd, ow, err)
	}
	for _, m := range []*CompletionManifest{
		{JobID: "j1_writing", State: CompletionStateWritingOutput},
		{JobID: "j2_output", State: CompletionStateOutputWritten},
		{JobID: "j3_sources", State: CompletionStateSourcesDeleted},
		{JobID: "j4_sources", State: CompletionStateSourcesDeleted},
	} {
		if err := writeCompletionManifest(dir, m); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "corrupt.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "inflight.json.tmp"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	sd, ow, err = w.PendingCommits()
	if err != nil {
		t.Fatal(err)
	}
	if sd != 2 || len(ow) != 1 || ow[0] != "j2_output" {
		t.Fatalf("PendingCommits = %d, %v; want 2 sources_deleted and [j2_output]", sd, ow)
	}
}
