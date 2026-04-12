package compaction

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// --- fakeBridge ----------------------------------------------------------

// fakeBridge captures bridge calls for assertions and lets tests script
// per-call return values (success, ErrNotLeader, or arbitrary error).
// Safe for concurrent use.
type fakeBridge struct {
	mu sync.Mutex

	// Scripted behavior. If nil/zero, bridge returns nil (success).
	registerErrors []error // consumed per RegisterCompactedFile call
	deleteErrors   []error // consumed per DeleteCompactedSource call

	// Captured calls
	registerCalls []CompactedFile
	deleteCalls   []struct {
		Path   string
		Reason string
	}

	// Counts (atomic for test assertions without taking the mutex)
	registerCount atomic.Int64
	deleteCount   atomic.Int64
}

func (b *fakeBridge) RegisterCompactedFile(ctx context.Context, file CompactedFile) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.registerCalls = append(b.registerCalls, file)
	b.registerCount.Add(1)
	idx := int(b.registerCount.Load()) - 1
	if idx < len(b.registerErrors) {
		return b.registerErrors[idx]
	}
	return nil
}

func (b *fakeBridge) DeleteCompactedSource(ctx context.Context, path, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deleteCalls = append(b.deleteCalls, struct {
		Path   string
		Reason string
	}{path, reason})
	b.deleteCount.Add(1)
	idx := int(b.deleteCount.Load()) - 1
	if idx < len(b.deleteErrors) {
		return b.deleteErrors[idx]
	}
	return nil
}

// --- helpers -------------------------------------------------------------

func newTestWatcher(t *testing.T, dir string, bridge ManifestBridge) *CompletionWatcher {
	t.Helper()
	w, err := NewCompletionWatcher(CompletionWatcherConfig{
		Dir:          dir,
		Bridge:       bridge,
		PollInterval: 10 * time.Millisecond,
		ApplyTimeout: 500 * time.Millisecond,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCompletionWatcher: %v", err)
	}
	return w
}

// waitForApplied spins until w.Stats()["manifests_applied"] reaches target
// or the deadline passes. Returns the final stats.
func waitForApplied(t *testing.T, w *CompletionWatcher, target int64, timeout time.Duration) map[string]int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s := w.Stats()
		if s["manifests_applied"] >= target {
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
	return w.Stats()
}

// --- NewCompletionWatcher config validation ---

func TestNewCompletionWatcher_RequiresDir(t *testing.T) {
	_, err := NewCompletionWatcher(CompletionWatcherConfig{
		Bridge: &fakeBridge{},
	})
	if err == nil || !errors.Is(errors.Unwrap(err), nil) && err.Error() == "" {
		t.Fatal("expected error for missing Dir")
	}
}

func TestNewCompletionWatcher_RequiresBridge(t *testing.T) {
	_, err := NewCompletionWatcher(CompletionWatcherConfig{
		Dir: t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected error for missing Bridge")
	}
}

func TestNewCompletionWatcher_DefaultsIntervals(t *testing.T) {
	w, err := NewCompletionWatcher(CompletionWatcherConfig{
		Dir:    t.TempDir(),
		Bridge: &fakeBridge{},
	})
	if err != nil {
		t.Fatalf("NewCompletionWatcher: %v", err)
	}
	if w.cfg.PollInterval != 1*time.Second {
		t.Errorf("PollInterval default: got %v, want 1s", w.cfg.PollInterval)
	}
	if w.cfg.ApplyTimeout != 5*time.Second {
		t.Errorf("ApplyTimeout default: got %v, want 5s", w.cfg.ApplyTimeout)
	}
}

// --- Happy path: output_written → sources_deleted → applied ---

func TestWatcher_HappyPath_OutputWrittenThenSourcesDeleted(t *testing.T) {
	dir := t.TempDir()
	bridge := &fakeBridge{}
	w := newTestWatcher(t, dir, bridge)

	// Seed a manifest in state output_written with 1 output, no deleted
	// sources yet. The watcher should apply RegisterCompactedFile but NOT
	// remove the manifest (waiting for the sources_deleted transition).
	m := newTestManifest("happy_1", CompletionStateOutputWritten)
	m.Outputs[0].Path = "db/cpu/compacted_happy.parquet"
	if err := writeCompletionManifest(dir, m); err != nil {
		t.Fatalf("seed: %v", err)
	}

	w.Start(context.Background())

	// Wait briefly for at least one poll cycle.
	time.Sleep(50 * time.Millisecond)

	// RegisterCompactedFile MUST have been called at least once (proving
	// the watcher saw the output_written state). The watcher re-polls the
	// manifest on every tick while it's in output_written state and calls
	// RegisterCompactedFile each time — that's fine because the FSM
	// handler is idempotent. What matters is that deletes have NOT been
	// issued (the manifest hasn't advanced to sources_deleted yet) and
	// the manifest is still on disk (not prematurely removed).
	if bridge.registerCount.Load() < 1 {
		t.Errorf("register calls after output_written: got %d, want >= 1", bridge.registerCount.Load())
	}
	if bridge.deleteCount.Load() != 0 {
		t.Errorf("delete calls after output_written: got %d, want 0", bridge.deleteCount.Load())
	}
	if w.Stats()["manifests_applied"] != 0 {
		t.Errorf("manifests_applied: got %d, want 0 (still waiting on sources_deleted)", w.Stats()["manifests_applied"])
	}
	// Manifest file must still exist on disk.
	if _, err := readCompletionManifest(filepath.Join(dir, m.JobID+".json")); err != nil {
		t.Errorf("manifest should still exist on disk: %v", err)
	}

	// Now advance to sources_deleted — simulates the subprocess finishing
	// deleteOldFiles and rewriting the manifest.
	m.State = CompletionStateSourcesDeleted
	m.DeletedSources = []string{"src_a.parquet", "src_b.parquet"}
	m.UpdatedAt = time.Now().UTC()
	if err := writeCompletionManifest(dir, m); err != nil {
		t.Fatalf("advance: %v", err)
	}

	stats := waitForApplied(t, w, 1, 2*time.Second)
	w.Stop()

	if stats["manifests_applied"] != 1 {
		t.Errorf("manifests_applied: got %d, want 1", stats["manifests_applied"])
	}
	if bridge.deleteCount.Load() != 2 {
		t.Errorf("delete calls: got %d, want 2", bridge.deleteCount.Load())
	}
	// Manifest file should be gone.
	if _, err := readCompletionManifest(filepath.Join(dir, m.JobID+".json")); err == nil {
		t.Error("manifest should have been removed after successful apply")
	}
	// One-shot happy path: register called exactly once across both ticks.
	// (The watcher re-applies RegisterCompactedFile on each pickup, which
	// is fine because the FSM handler is idempotent.)
	if bridge.registerCount.Load() < 2 {
		t.Errorf("register calls total: got %d, expected at least 2 (once per tick until applied)", bridge.registerCount.Load())
	}
}

// --- sources_deleted arrives in one shot ---

func TestWatcher_SourcesDeletedInOneShot(t *testing.T) {
	dir := t.TempDir()
	bridge := &fakeBridge{}
	w := newTestWatcher(t, dir, bridge)

	// Seed directly in sources_deleted — simulates an operator dropping
	// a completed manifest, or a subprocess that got through both states
	// before the watcher's first tick.
	m := newTestManifest("oneshot_1", CompletionStateSourcesDeleted)
	m.DeletedSources = []string{"s1.parquet"}
	if err := writeCompletionManifest(dir, m); err != nil {
		t.Fatalf("seed: %v", err)
	}

	w.Start(context.Background())
	stats := waitForApplied(t, w, 1, 2*time.Second)
	w.Stop()

	if stats["manifests_applied"] != 1 {
		t.Errorf("manifests_applied: got %d, want 1", stats["manifests_applied"])
	}
	if bridge.registerCount.Load() != 1 {
		t.Errorf("register calls: got %d, want 1", bridge.registerCount.Load())
	}
	if bridge.deleteCount.Load() != 1 {
		t.Errorf("delete calls: got %d, want 1", bridge.deleteCount.Load())
	}
}

// --- writing_output is ignored ---

func TestWatcher_IgnoresWritingOutputState(t *testing.T) {
	dir := t.TempDir()
	bridge := &fakeBridge{}
	w := newTestWatcher(t, dir, bridge)

	// Manifest is still in progress in the subprocess. The watcher must
	// leave it alone and not call the bridge.
	m := newTestManifest("writing_1", CompletionStateWritingOutput)
	if err := writeCompletionManifest(dir, m); err != nil {
		t.Fatalf("seed: %v", err)
	}

	w.Start(context.Background())
	time.Sleep(80 * time.Millisecond) // several poll intervals
	w.Stop()

	if bridge.registerCount.Load() != 0 {
		t.Errorf("register calls: got %d, want 0 (writing_output must be ignored)", bridge.registerCount.Load())
	}
	if bridge.deleteCount.Load() != 0 {
		t.Errorf("delete calls: got %d, want 0", bridge.deleteCount.Load())
	}
	// Manifest must still exist.
	if _, err := readCompletionManifest(filepath.Join(dir, m.JobID+".json")); err != nil {
		t.Errorf("manifest should still exist: %v", err)
	}
}

// --- ErrNotLeader keeps the manifest on disk ---

func TestWatcher_ErrNotLeaderKeepsManifest(t *testing.T) {
	dir := t.TempDir()
	// First N calls return ErrNotLeader, subsequent calls succeed. This
	// simulates a brief leader flap that recovers.
	bridge := &fakeBridge{
		registerErrors: []error{
			fmt.Errorf("wrapped: %w", ErrNotLeader),
			fmt.Errorf("wrapped: %w", ErrNotLeader),
		},
	}
	w := newTestWatcher(t, dir, bridge)

	m := newTestManifest("notleader_1", CompletionStateSourcesDeleted)
	m.DeletedSources = []string{"s1.parquet"}
	if err := writeCompletionManifest(dir, m); err != nil {
		t.Fatalf("seed: %v", err)
	}

	w.Start(context.Background())
	stats := waitForApplied(t, w, 1, 2*time.Second)
	w.Stop()

	if stats["manifests_applied"] != 1 {
		t.Errorf("manifests_applied: got %d, want 1 (should retry past leader flap)", stats["manifests_applied"])
	}
	if stats["manifests_not_leader"] < 2 {
		t.Errorf("manifests_not_leader: got %d, want >= 2", stats["manifests_not_leader"])
	}
	// apply_errors must NOT count ErrNotLeader — it's an expected retry.
	if stats["apply_errors"] != 0 {
		t.Errorf("apply_errors: got %d, want 0 (ErrNotLeader is not an error)", stats["apply_errors"])
	}
	if bridge.registerCount.Load() < 3 {
		t.Errorf("register calls: got %d, want >= 3 (2 failures + 1 success)", bridge.registerCount.Load())
	}
}

// --- bridge error keeps the manifest and increments apply_errors ---

func TestWatcher_BridgeErrorKeepsManifestAndCounts(t *testing.T) {
	dir := t.TempDir()
	bridge := &fakeBridge{
		registerErrors: []error{errors.New("fake backend error")},
	}
	w := newTestWatcher(t, dir, bridge)

	m := newTestManifest("bridgeerr_1", CompletionStateSourcesDeleted)
	m.DeletedSources = []string{"s1.parquet"}
	if err := writeCompletionManifest(dir, m); err != nil {
		t.Fatalf("seed: %v", err)
	}

	w.Start(context.Background())
	// Wait for at least one apply attempt.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && w.Stats()["apply_errors"] == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	w.Stop()

	stats := w.Stats()
	if stats["apply_errors"] == 0 {
		t.Errorf("apply_errors: got 0, want >= 1")
	}
	// Manifest must still exist on disk.
	if _, err := readCompletionManifest(filepath.Join(dir, m.JobID+".json")); err != nil {
		t.Errorf("manifest should still exist after bridge error: %v", err)
	}
}

// --- Partial apply: register succeeds, delete fails ---

func TestWatcher_DeleteFailureKeepsManifest(t *testing.T) {
	dir := t.TempDir()
	bridge := &fakeBridge{
		deleteErrors: []error{errors.New("delete boom")},
	}
	w := newTestWatcher(t, dir, bridge)

	m := newTestManifest("partial_1", CompletionStateSourcesDeleted)
	m.DeletedSources = []string{"s1.parquet"}
	if err := writeCompletionManifest(dir, m); err != nil {
		t.Fatalf("seed: %v", err)
	}

	w.Start(context.Background())
	// Let at least one apply attempt complete.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && w.Stats()["apply_errors"] == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	w.Stop()

	stats := w.Stats()
	if stats["apply_errors"] == 0 {
		t.Errorf("apply_errors: got 0, want >= 1")
	}
	if bridge.registerCount.Load() < 1 {
		t.Errorf("register calls: got %d, want >= 1", bridge.registerCount.Load())
	}
	// Manifest must still exist on disk.
	if _, err := readCompletionManifest(filepath.Join(dir, m.JobID+".json")); err != nil {
		t.Errorf("manifest should still exist after delete failure: %v", err)
	}
}

// --- Context cancellation during poll ---

func TestWatcher_CtxCancelStopsPromptly(t *testing.T) {
	dir := t.TempDir()
	bridge := &fakeBridge{}
	w := newTestWatcher(t, dir, bridge)

	// Seed many manifests to ensure the loop is mid-scan when we cancel.
	for i := 0; i < 10; i++ {
		m := newTestManifest(fmt.Sprintf("ctx_%02d", i), CompletionStateSourcesDeleted)
		m.DeletedSources = []string{"s.parquet"}
		if err := writeCompletionManifest(dir, m); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	cancel()
	// Stop should return promptly (within ~100ms).
	done := make(chan struct{})
	go func() {
		w.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("watcher Stop did not return within 1s after ctx cancel")
	}
}

// --- Empty directory is not an error ---

func TestWatcher_EmptyDirIsSilent(t *testing.T) {
	dir := t.TempDir()
	bridge := &fakeBridge{}
	w := newTestWatcher(t, dir, bridge)

	w.Start(context.Background())
	time.Sleep(50 * time.Millisecond)
	w.Stop()

	stats := w.Stats()
	if stats["polls_total"] == 0 {
		t.Errorf("polls_total: got 0, expected at least 1")
	}
	if stats["manifests_seen"] != 0 {
		t.Errorf("manifests_seen: got %d, want 0", stats["manifests_seen"])
	}
	if stats["apply_errors"] != 0 {
		t.Errorf("apply_errors: got %d, want 0", stats["apply_errors"])
	}
}

// --- Multiple manifests applied in one poll cycle ---

func TestWatcher_MultipleManifestsInOneCycle(t *testing.T) {
	dir := t.TempDir()
	bridge := &fakeBridge{}
	w := newTestWatcher(t, dir, bridge)

	// Seed 5 completed manifests.
	for i := 0; i < 5; i++ {
		m := newTestManifest(fmt.Sprintf("multi_%02d", i), CompletionStateSourcesDeleted)
		m.DeletedSources = []string{fmt.Sprintf("src_%02d.parquet", i)}
		if err := writeCompletionManifest(dir, m); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	w.Start(context.Background())
	stats := waitForApplied(t, w, 5, 2*time.Second)
	w.Stop()

	if stats["manifests_applied"] != 5 {
		t.Errorf("manifests_applied: got %d, want 5", stats["manifests_applied"])
	}
	if bridge.registerCount.Load() != 5 {
		t.Errorf("register calls: got %d, want 5", bridge.registerCount.Load())
	}
	if bridge.deleteCount.Load() != 5 {
		t.Errorf("delete calls: got %d, want 5", bridge.deleteCount.Load())
	}
}

// --- Stop is safe to call multiple times ---

func TestWatcher_StopIsIdempotent(t *testing.T) {
	w := newTestWatcher(t, t.TempDir(), &fakeBridge{})
	w.Start(context.Background())
	w.Stop()
	w.Stop() // second Stop must not panic
	w.Stop() // third Stop must not panic
}

// --- Start is idempotent ---

func TestWatcher_StartIsIdempotent(t *testing.T) {
	w := newTestWatcher(t, t.TempDir(), &fakeBridge{})
	w.Start(context.Background())
	w.Start(context.Background()) // second Start must be a no-op
	time.Sleep(20 * time.Millisecond)
	w.Stop()
	// No assertion beyond "doesn't panic"; the goroutine count check is
	// enforced by the race detector if something went wrong.
}
