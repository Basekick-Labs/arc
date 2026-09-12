package reconciliation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/metrics"
)

// invalidManifestKey is accepted by raft.ValidateManifestPath and refused by
// storage.ValidateKey, which is what lets it into the cluster manifest and then
// makes every storage call with it fail permanently. The full reachable set is
// pinned in internal/cluster/raft/manifest_path_reachability_test.go.
const invalidManifestKey = `db\m/2026/04/11/14/bad.parquet`

// flakyBackend fails one key with an ordinary, retryable error. Everything
// else, including key validation, behaves like fakeBackend.
type flakyBackend struct {
	*fakeBackend
	flakyKey string
}

func (b *flakyBackend) Exists(ctx context.Context, path string) (bool, error) {
	if path == b.flakyKey {
		return false, errors.New("RequestTimeout: your socket connection to the server was not read from")
	}
	return b.fakeBackend.Exists(ctx, path)
}

func (b *flakyBackend) Delete(ctx context.Context, path string) error {
	if path == b.flakyKey {
		return errors.New("RequestTimeout: your socket connection to the server was not read from")
	}
	return b.fakeBackend.Delete(ctx, path)
}

func deletedPaths(t *testing.T, coord *fakeCoordinator) []string {
	t.Helper()
	var out []string
	for _, batch := range coord.batchCalls {
		for _, op := range batch {
			var payload raft.DeleteFilePayload
			if err := json.Unmarshal(op.Payload, &payload); err != nil {
				t.Fatalf("unmarshal op payload: %v", err)
			}
			out = append(out, payload.Path)
		}
	}
	return out
}

// TestManifestSweepSeparatesPermanentFromTransient is the discriminating test
// for the manifest sweep. Before #747 both failure kinds landed in one bucket
// whose log line promises "next run retries", which is true of a timeout and
// false of a key no backend can address: that entry was re-listed, re-checked
// and re-skipped on every run forever, with nothing in the run report saying so.
//
// The two failing keys are asserted together on purpose. Testing the permanent
// one alone cannot fail for a reason that matters, because skipping it was
// already the behaviour; what changed is that the two are now told apart.
func TestManifestSweepSeparatesPermanentFromTransient(t *testing.T) {
	const goodOrphan = "db/m/2026/04/11/14/gone.parquet"
	const flakyOrphan = "db/m/2026/04/11/14/timeout.parquet"

	coord := newFakeCoordinator()
	store := &flakyBackend{fakeBackend: newFakeBackend(), flakyKey: flakyOrphan}
	r := newReconciler(t, Config{
		Enabled:          true,
		BackendKind:      BackendShared,
		BatchSize:        100,
		MaxDeletesPerRun: 100,
		SamplePathsCap:   10,
	}, coord, store, &fakeGate{scan: true, sweep: true, role: "leader"})

	before := quarantineMetric()
	run := &Run{ID: "run-1"}
	// Invalid key at index 1 of 3: a quarantine that aborted its loop instead
	// of continuing would still pass with it first or last.
	paths := []string{goodOrphan, invalidManifestKey, flakyOrphan}
	if err := r.sweepOrphanManifest(context.Background(), run, paths, false); err != nil {
		t.Fatalf("sweepOrphanManifest: %v", err)
	}

	if run.SkippedInvalidPath != 1 {
		t.Errorf("SkippedInvalidPath = %d, want 1", run.SkippedInvalidPath)
	}
	if run.SkippedTransient != 1 {
		t.Errorf("SkippedTransient = %d, want 1; a timeout must stay in the retryable bucket", run.SkippedTransient)
	}
	// The counter must move for the permanent entry and only for it, or the
	// one aggregated signal this change ships disagrees with the run report.
	if got := quarantineMetric() - before; got != 1 {
		t.Errorf("quarantine counter moved by %d, want 1", got)
	}

	deleted := deletedPaths(t, coord)
	if len(deleted) != 1 || deleted[0] != goodOrphan {
		t.Fatalf("manifest deletes = %v, want exactly [%s]: neither failure kind may poison the batch, and neither may be deleted", deleted, goodOrphan)
	}

	// The permanent entry must be reported in a way an operator can act on,
	// and must not be dropped from the manifest: this sweep issues Raft
	// deletes, and an object can sit under the literal key on an object store
	// where the listing filter hides it, so absence is unobservable here
	// rather than established.
	// The path is %q-quoted in the entry, so match on the quoted form rather
	// than the raw key: a backslash appears escaped there.
	var sawPermanent bool
	for _, e := range run.Errors {
		if strings.Contains(e, fmt.Sprintf("%q", invalidManifestKey)) && strings.Contains(e, "permanently unusable") {
			sawPermanent = true
		}
	}
	if !sawPermanent {
		t.Errorf("run.Errors does not name the permanently unusable key: %v", run.Errors)
	}
}

// TestStorageSweepSeparatesPermanentFromTransient is the same split one file
// over, in the sweep that deletes bytes rather than manifest entries. Its log
// line carried the same false promise.
func TestStorageSweepSeparatesPermanentFromTransient(t *testing.T) {
	const goodOrphan = "db/m/2026/04/11/14/orphan.parquet"
	const flakyOrphan = "db/m/2026/04/11/14/timeout.parquet"

	store := &flakyBackend{fakeBackend: newFakeBackend(), flakyKey: flakyOrphan}
	store.put(goodOrphan, time.Now().UTC().Add(-24*time.Hour))
	store.put(flakyOrphan, time.Now().UTC().Add(-24*time.Hour))

	r := newReconciler(t, Config{
		Enabled:          true,
		BackendKind:      BackendShared,
		BatchSize:        100,
		MaxDeletesPerRun: 100,
	}, newFakeCoordinator(), store, &fakeGate{scan: true, sweep: true, role: "leader"})

	run := &Run{ID: "run-2"}
	// hasBatchDelete is false so the per-file loop runs. Classification lives
	// there on purpose: DeleteBatch joins per-key failures into one error, so a
	// batch error matches ErrInvalidPath when a single member is bad, and
	// quarantining on that would discard every valid delete in the batch.
	applied := r.applyStorageDeletes(context.Background(), run, []string{goodOrphan, invalidManifestKey, flakyOrphan}, nil, false)

	if applied != 1 {
		t.Errorf("applied = %d, want 1", applied)
	}
	if run.SkippedInvalidPath != 1 {
		t.Errorf("SkippedInvalidPath = %d, want 1", run.SkippedInvalidPath)
	}
	if run.SkippedTransient != 1 {
		t.Errorf("SkippedTransient = %d, want 1", run.SkippedTransient)
	}
	if ok, _ := store.fakeBackend.Exists(context.Background(), goodOrphan); ok {
		t.Error("the deletable orphan survived; one unusable key poisoned the batch")
	}
}

// TestRunReportDistinguishesUnconvergedRuns pins why the two counters are
// separate fields rather than one "skipped" number. An operator watching a run
// report needs to know whether waiting helps.
func TestRunReportDistinguishesUnconvergedRuns(t *testing.T) {
	run := &Run{SkippedTransient: 3, SkippedInvalidPath: 2}
	blob, err := json.Marshal(run)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(blob, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"skipped_transient", "skipped_invalid_path"} {
		if _, ok := decoded[key]; !ok {
			t.Errorf("run report is missing %q, so the API surface cannot show it", key)
		}
	}
}

func quarantineMetric() int64 {
	return metrics.Get().Snapshot()["storage_invalid_path_quarantined_total"].(int64)
}

// TestDryRunDoesNotMoveTheGlobalCounter pins that a dry run reports what it
// would have done without touching a process-wide counter operators alert on.
// The manifest sweep runs its re-check even in dry-run mode, to produce
// accurate counts, so this is the one site where the distinction is live.
func TestDryRunDoesNotMoveTheGlobalCounter(t *testing.T) {
	coord := newFakeCoordinator()
	store := newFakeBackend()
	r := newReconciler(t, Config{
		Enabled:          true,
		BackendKind:      BackendShared,
		BatchSize:        100,
		MaxDeletesPerRun: 100,
	}, coord, store, &fakeGate{scan: true, sweep: true, role: "leader"})

	before := quarantineMetric()
	run := &Run{ID: "run-dry"}
	if err := r.sweepOrphanManifest(context.Background(), run, []string{invalidManifestKey}, true); err != nil {
		t.Fatalf("sweepOrphanManifest: %v", err)
	}
	if run.SkippedInvalidPath != 1 {
		t.Errorf("SkippedInvalidPath = %d, want 1: a dry run still reports what it found", run.SkippedInvalidPath)
	}
	if got := quarantineMetric() - before; got != 0 {
		t.Errorf("dry run moved the global counter by %d, want 0", got)
	}
}
