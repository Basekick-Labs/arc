package compaction

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// invalidOutputKey is the one spelling usable at every site in #747: raft
// ValidateManifestPath accepts it, storage.ValidateKey refuses it, and unlike
// the other five it also survives coordinator sanitizeFetchPath. Pinned in
// internal/cluster/raft/manifest_path_reachability_test.go.
const invalidOutputKey = `testdb\cpu/2026/04/11/14/out.parquet`

func newManifestFixture(t *testing.T) (*ManifestManager, storage.Backend, context.Context) {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	return NewManifestManager(backend, zerolog.Nop()), backend, context.Background()
}

func seedInput(t *testing.T, backend storage.Backend, ctx context.Context, key string) {
	t.Helper()
	if err := backend.Write(ctx, key, []byte("parquet")); err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
}

// TestRecoverManifestParksUnusableOutput covers the loop the issue opens with:
// an output key no backend can address makes Exists, Delete and Read all fail
// permanently, so recovery reprocesses the same manifest every cycle and
// GetFilesInManifests keeps its inputs out of compaction forever.
//
// Before the fix this left the manifest in place indefinitely. The load-bearing
// assertion is that it leaves the recovery work set, not that the call returns
// nil: RecoverOrphanedManifests swallows per-manifest errors with a continue,
// so the returned error was already nil.
func TestRecoverManifestParksUnusableOutput(t *testing.T) {
	mm, backend, ctx := newManifestFixture(t)
	const input = "testdb/cpu/2026/04/11/14/in.parquet"
	seedInput(t, backend, ctx, input)

	manifestPath, err := mm.WriteManifest(ctx, &Manifest{
		OutputPath:    invalidOutputKey,
		OutputSize:    7,
		InputFiles:    []string{input},
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionPath: "testdb/cpu/2026/04/11/14",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "job_invalid_output",
	})
	if err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	if _, err := mm.RecoverOrphanedManifests(ctx, nil, nil); err != nil {
		t.Fatalf("RecoverOrphanedManifests: %v", err)
	}

	remaining, err := mm.ListManifests(ctx)
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("manifest is still in the recovery work set after quarantine: %v", remaining)
	}

	parked := manifestPath + ManifestQuarantineSuffix
	exists, err := backend.Exists(ctx, parked)
	if err != nil {
		t.Fatalf("Exists(%s): %v", parked, err)
	}
	if !exists {
		t.Fatal("manifest was removed without being parked; the record of which output and inputs were involved is lost")
	}

	// The inputs must survive. Parking is not a completion: nothing here
	// established that the output was written, so deleting the inputs would
	// destroy the only copy of the data.
	if ok, err := backend.Exists(ctx, input); err != nil || !ok {
		t.Fatalf("input file was removed by a quarantine that proved nothing about the output (exists=%v err=%v)", ok, err)
	}
}

// TestRecoverManifestParkedCopyKeepsBytes pins that parking preserves the
// manifest verbatim, including fields this binary does not know about. The
// quarantine re-reads raw bytes for exactly this reason: a manifest written by
// a different version must survive a round trip through a parking operation
// that never parses it.
func TestRecoverManifestParkedCopyKeepsBytes(t *testing.T) {
	mm, backend, ctx := newManifestFixture(t)

	manifestPath, err := mm.WriteManifest(ctx, &Manifest{
		OutputPath:    invalidOutputKey,
		InputFiles:    []string{"testdb/cpu/2026/04/11/14/in.parquet"},
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionPath: "testdb/cpu/2026/04/11/14",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "job_bytes",
	})
	if err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	original, err := backend.Read(ctx, manifestPath)
	if err != nil {
		t.Fatalf("read original: %v", err)
	}

	if _, err := mm.RecoverOrphanedManifests(ctx, nil, nil); err != nil {
		t.Fatalf("RecoverOrphanedManifests: %v", err)
	}

	parkedBytes, err := backend.Read(ctx, manifestPath+ManifestQuarantineSuffix)
	if err != nil {
		t.Fatalf("read parked: %v", err)
	}
	if string(parkedBytes) != string(original) {
		t.Fatalf("parked copy differs from the original:\n got: %s\nwant: %s", parkedBytes, original)
	}
}

// TestRecoverManifestSkipsUnusableInput covers the second site: one input whose
// Delete fails permanently used to count as a delete error, which kept the
// whole manifest for a retry that could only fail the same way, so the valid
// siblings were never deleted and the consumed-inputs marks never fired.
func TestRecoverManifestSkipsUnusableInput(t *testing.T) {
	mm, backend, ctx := newManifestFixture(t)
	const output = "testdb/cpu/2026/04/11/14/out.parquet"
	const goodInput = "testdb/cpu/2026/04/11/14/in-good.parquet"
	const badInput = `testdb\cpu/2026/04/11/14/in-bad.parquet`

	seedInput(t, backend, ctx, output)
	seedInput(t, backend, ctx, goodInput)

	// Bad input at index 1 of 3, not first and not last: a quarantine that
	// aborted its loop rather than continuing would still pass with it first.
	if _, err := mm.WriteManifest(ctx, &Manifest{
		OutputPath:    output,
		OutputSize:    int64(len("parquet")),
		InputFiles:    []string{goodInput, badInput, goodInput},
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionPath: "testdb/cpu/2026/04/11/14",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "job_invalid_input",
	}); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	var consumed []string
	recovered, err := mm.RecoverOrphanedManifests(ctx, nil, func(inputs []string) error {
		consumed = append(consumed, inputs...)
		return nil
	})
	if err != nil {
		t.Fatalf("RecoverOrphanedManifests: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1: the manifest must complete rather than be held for a retry that cannot succeed", recovered)
	}

	if ok, _ := backend.Exists(ctx, goodInput); ok {
		t.Fatal("the deletable input was not deleted; one unusable sibling poisoned the whole recovery")
	}
	if len(consumed) != 3 {
		t.Fatalf("consumed inputs = %v, want all three: the content is in the output either way, and an unmarked receipt makes the spoke re-upload the raw beside it", consumed)
	}
	remaining, err := mm.ListManifests(ctx)
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("manifest kept for retry: %v", remaining)
	}
}

// transientBackend fails one key with an ordinary error. Everything else
// delegates to a real LocalBackend, so the contract under test is the real one.
type transientBackend struct {
	storage.Backend
	failKey string
	failErr error
}

func (b *transientBackend) Exists(ctx context.Context, path string) (bool, error) {
	if path == b.failKey {
		return false, b.failErr
	}
	return b.Backend.Exists(ctx, path)
}

func (b *transientBackend) Delete(ctx context.Context, path string) error {
	if path == b.failKey {
		return b.failErr
	}
	return b.Backend.Delete(ctx, path)
}

// TestRecoverManifestKeepsManifestOnTransientError is the over-correction
// guard, and it is the dangerous direction.
//
// A widened quarantine that parked or dropped the manifest on ANY Exists
// failure would re-queue an already-compacted partition on a throttle from S3,
// producing a duplicate compaction and an orphaned output. The quarantine must
// fire only for the permanent error.
func TestRecoverManifestKeepsManifestOnTransientError(t *testing.T) {
	base, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	defer base.Close()
	ctx := context.Background()

	const output = "testdb/cpu/2026/04/11/14/out.parquet"
	backend := &transientBackend{Backend: base, failKey: output, failErr: errors.New("SlowDown: please reduce your request rate")}
	mm := NewManifestManager(backend, zerolog.Nop())

	manifestPath, err := mm.WriteManifest(ctx, &Manifest{
		OutputPath:    output,
		OutputSize:    7,
		InputFiles:    []string{"testdb/cpu/2026/04/11/14/in.parquet"},
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionPath: "testdb/cpu/2026/04/11/14",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "job_transient",
	})
	if err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	if _, err := mm.RecoverOrphanedManifests(ctx, nil, nil); err != nil {
		t.Fatalf("RecoverOrphanedManifests: %v", err)
	}

	remaining, err := mm.ListManifests(ctx)
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}
	if len(remaining) != 1 || remaining[0] != manifestPath {
		t.Fatalf("a transient Exists failure must keep the manifest for the next cycle; work set is now %v", remaining)
	}
	if ok, _ := backend.Exists(ctx, manifestPath+ManifestQuarantineSuffix); ok {
		t.Fatal("a transient failure was treated as permanent and parked the manifest")
	}
}

// TestRecoverManifestCountsTransientInputDeleteFailure is the same guard on the
// input-deletion loop: a transient Delete failure must still hold the manifest
// for a retry, or the marks fire and the manifest is dropped while files that
// could have been deleted are left behind with nothing tracking them.
func TestRecoverManifestCountsTransientInputDeleteFailure(t *testing.T) {
	base, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	defer base.Close()
	ctx := context.Background()

	const output = "testdb/cpu/2026/04/11/14/out.parquet"
	const input = "testdb/cpu/2026/04/11/14/in.parquet"
	if err := base.Write(ctx, output, []byte("parquet")); err != nil {
		t.Fatalf("seed output: %v", err)
	}
	if err := base.Write(ctx, input, []byte("parquet")); err != nil {
		t.Fatalf("seed input: %v", err)
	}

	backend := &transientBackend{Backend: base, failKey: input, failErr: fmt.Errorf("connection reset by peer")}
	mm := NewManifestManager(backend, zerolog.Nop())

	if _, err := mm.WriteManifest(ctx, &Manifest{
		OutputPath:    output,
		OutputSize:    int64(len("parquet")),
		InputFiles:    []string{input},
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionPath: "testdb/cpu/2026/04/11/14",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "job_transient_input",
	}); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	marked := false
	recovered, err := mm.RecoverOrphanedManifests(ctx, nil, func([]string) error {
		marked = true
		return nil
	})
	if err != nil {
		t.Fatalf("RecoverOrphanedManifests: %v", err)
	}
	if recovered != 0 {
		t.Fatalf("recovered = %d, want 0 for a transient input delete failure", recovered)
	}
	if marked {
		t.Fatal("consumed-inputs marks fired even though an input deletion failed transiently")
	}
	remaining, _ := mm.ListManifests(ctx)
	if len(remaining) != 1 {
		t.Fatalf("manifest must be kept for the retry; work set is %v", remaining)
	}
}

// TestQuarantinedManifestLeavesRecoveryWorkSet pins the mechanism the parking
// name depends on: ListManifests selects on a ".json" suffix, so the parked
// object is invisible to both recovery and GetFilesInManifests. If that filter
// ever changes, the quarantine turns back into an endless retry and this test
// is what says so.
func TestQuarantinedManifestLeavesRecoveryWorkSet(t *testing.T) {
	mm, backend, ctx := newManifestFixture(t)
	const input = "testdb/cpu/2026/04/11/14/in.parquet"
	seedInput(t, backend, ctx, input)

	if _, err := mm.WriteManifest(ctx, &Manifest{
		OutputPath:    invalidOutputKey,
		InputFiles:    []string{input},
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionPath: "testdb/cpu/2026/04/11/14",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "job_worklist",
	}); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	if _, err := mm.RecoverOrphanedManifests(ctx, nil, nil); err != nil {
		t.Fatalf("first recovery: %v", err)
	}

	// The input must no longer be held back from compaction: that exclusion
	// lasting forever is the operator-visible harm the issue describes.
	held, err := mm.GetFilesInManifests(ctx)
	if err != nil {
		t.Fatalf("GetFilesInManifests: %v", err)
	}
	if _, stillHeld := held[input]; stillHeld {
		t.Fatal("a parked manifest is still excluding its inputs from compaction")
	}

	// And a second pass must find nothing to do, rather than re-parking.
	recovered, err := mm.RecoverOrphanedManifests(ctx, nil, nil)
	if err != nil {
		t.Fatalf("second recovery: %v", err)
	}
	if recovered != 0 {
		t.Fatalf("second recovery processed %d manifests; the quarantine did not converge", recovered)
	}
}

// TestQuarantinePathFitsWithinTheKeyLimit covers the case that used to make
// parking impossible and fall back to deleting the record.
//
// GenerateManifestPath allows a filename right up to the 255-byte segment
// limit, so appending the suffix to the longest ones overflowed. Reachable in
// production: the job id is built from the database and the folded partition
// path, both operator- and ingest-controlled.
func TestQuarantinePathFitsWithinTheKeyLimit(t *testing.T) {
	mm, _, _ := newManifestFixture(t)

	for _, tc := range []struct {
		name  string
		jobID string
	}{
		{"ordinary", "job_20260411_140000_testdb_cpu"},
		// 250 bytes + ".json" = 255, exactly at the limit, so the suffix
		// overflows by the full length of the suffix.
		{"filename at the segment limit", strings.Repeat("j", 250)},
		{"filename one under the limit", strings.Repeat("j", 249)},
		// Past the limit GenerateManifestPath already hashes, so the parked
		// name is short again; included so the boundary is covered on both
		// sides.
		{"filename past the limit", strings.Repeat("j", 400)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifestPath := mm.GenerateManifestPath("hourly", "testdb", "testdb/cpu/2026/04/11/14", tc.jobID)
			if err := storage.ValidateKey(manifestPath); err != nil {
				t.Fatalf("GenerateManifestPath produced an unusable key: %v", err)
			}
			parked, err := quarantinePathFor(manifestPath)
			if err != nil {
				t.Fatalf("no parked name could be derived, so the record would be deleted instead: %v", err)
			}
			if err := storage.ValidateKey(parked); err != nil {
				t.Fatalf("parked name %q is not a usable key: %v", parked, err)
			}
			if parked == manifestPath {
				t.Fatal("parked name equals the manifest path, so parking would overwrite the original")
			}
			if strings.HasSuffix(parked, ".json") {
				t.Fatalf("parked name %q still ends in .json, so ListManifests would keep returning it", parked)
			}
		})
	}
}

// TestQuarantinePathIsDeterministic pins that a repeated recovery pass
// addresses the same parked object rather than accumulating copies, including
// on the hashed branch.
func TestQuarantinePathIsDeterministic(t *testing.T) {
	mm, _, _ := newManifestFixture(t)
	for _, jobID := range []string{"job_short", strings.Repeat("j", 250)} {
		manifestPath := mm.GenerateManifestPath("hourly", "testdb", "p", jobID)
		first, err := quarantinePathFor(manifestPath)
		if err != nil {
			t.Fatalf("quarantinePathFor: %v", err)
		}
		second, err := quarantinePathFor(manifestPath)
		if err != nil {
			t.Fatalf("quarantinePathFor (second call): %v", err)
		}
		if first != second {
			t.Fatalf("parked name is not deterministic: %q then %q", first, second)
		}
	}
}

// TestRecoverManifestParkingCountsTheMetricOnce pins that the counter tracks
// entries that actually left the work set. It used to be incremented on entry,
// so a transient backend failure during parking counted a drop that did not
// happen, once per recovery cycle, for a manifest still being retried.
func TestRecoverManifestParkingCountsTheMetricOnce(t *testing.T) {
	mm, backend, ctx := newManifestFixture(t)
	const input = "testdb/cpu/2026/04/11/14/in.parquet"
	seedInput(t, backend, ctx, input)

	if _, err := mm.WriteManifest(ctx, &Manifest{
		OutputPath:    invalidOutputKey,
		InputFiles:    []string{input},
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionPath: "testdb/cpu/2026/04/11/14",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "job_metric",
	}); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	before := quarantineCount()
	if _, err := mm.RecoverOrphanedManifests(ctx, nil, nil); err != nil {
		t.Fatalf("RecoverOrphanedManifests: %v", err)
	}
	if got := quarantineCount() - before; got != 1 {
		t.Fatalf("counter moved by %d, want 1", got)
	}

	// A second pass has nothing left to do, so the counter must not move again.
	if _, err := mm.RecoverOrphanedManifests(ctx, nil, nil); err != nil {
		t.Fatalf("second recovery: %v", err)
	}
	if got := quarantineCount() - before; got != 1 {
		t.Fatalf("counter moved by %d across two passes, want 1", got)
	}
}

func quarantineCount() int64 {
	return metrics.Get().Snapshot()["storage_invalid_path_quarantined_total"].(int64)
}

// TestGenerateManifestPathAlwaysProducesAUsableKey closes the window this PR
// found while testing the parked name.
//
// The generator hashed only past MaxKeySegmentLen (255), but ValidateKey
// subtracts PartSuffix and refuses anything over 250, so a job id of 246 to 250
// characters produced a filename the generator considered fine and every write
// refused. That is the same failure #744 removed, in the last five bytes of the
// range, and it is silent: WriteManifest fails, compaction for the partition
// fails, and it repeats every cycle.
func TestGenerateManifestPathAlwaysProducesAUsableKey(t *testing.T) {
	mm, backend, ctx := newManifestFixture(t)

	for n := 240; n <= 260; n++ {
		jobID := strings.Repeat("j", n)
		path := mm.GenerateManifestPath("hourly", "testdb", "testdb/cpu/2026/04/11/14", jobID)
		if err := storage.ValidateKey(path); err != nil {
			t.Fatalf("job id of %d bytes produced a key the backend refuses: %v", n, err)
		}
		// And prove it round-trips through a real backend rather than only
		// through the validator.
		if err := backend.Write(ctx, path, []byte("{}")); err != nil {
			t.Fatalf("job id of %d bytes: write failed: %v", n, err)
		}
	}
}
