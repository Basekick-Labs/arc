package compaction

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// quarantinedManifests lists the parked manifests beside manifestPath.
func quarantinedManifests(t *testing.T, root, manifestPath string) []string {
	t.Helper()
	dir := filepath.Join(root, filepath.Dir(manifestPath))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read manifest dir %s: %v", dir, err)
	}
	var parked []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ManifestQuarantineSuffix) {
			parked = append(parked, e.Name())
		}
	}
	return parked
}

// TestRecoveryParksAManifestWhosePermanentFailureCannotClear covers the
// condition recovery cannot ever satisfy: a completion manifest that belongs to
// a different job. Returning a plain error would retain the manifest, so every
// cycle would re-read and re-hash the whole compacted output and fail the same
// way -- the #747 shape this file already guards against for an unusable output
// key. It parks instead, leaving the record for an operator.
func TestRecoveryParksAManifestWhosePermanentFailureCannotClear(t *testing.T) {
	root := t.TempDir()
	completionDir := filepath.Join(root, ".completion", "pending")
	manager, manifestPath, inputPath, outputPath, _ := newIssue1155Recovery(t, root, completionDir)

	// A completion manifest under this job's ID that names a different job.
	// Nothing on a later cycle can change either file, so the failure is final.
	if err := writeCompletionManifest(completionDir, &CompletionManifest{
		JobID:     "issue1155",
		State:     CompletionStateOutputWritten,
		CreatedAt: time.Date(2026, 1, 2, 4, 0, 0, 0, time.UTC),
		Outputs:   []CompactedOutput{{Path: "db/cpu/2026/01/02/03/somebody-elses.parquet", SizeBytes: 1}},
	}); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	recovered, err := manager.recoverOrphanedManifests(ctx, recoveryScope{})
	if err != nil {
		t.Fatalf("a parked manifest must not surface as a cycle error: %v", err)
	}
	// "recovered" counts manifests that LEFT the work set, which is the
	// existing semantics for the #747 park too -- not jobs completed.
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1 (the manifest left the work set)", recovered)
	}

	// Parked, not retained: the next cycle cannot see it, so the re-hash loop
	// is broken.
	if parked := quarantinedManifests(t, root, manifestPath); len(parked) != 1 {
		t.Fatalf("parked manifests = %v, want exactly one %s file", parked, ManifestQuarantineSuffix)
	}
	if _, err := manager.ManifestManager.ReadManifest(ctx, manifestPath); err == nil {
		t.Fatal("the original manifest is still in the work set, so recovery will retry it forever")
	}

	// Parking returns before the deletion loop, so both objects survive for an
	// operator to inspect.
	if exists, err := manager.StorageBackend.Exists(ctx, inputPath); err != nil || !exists {
		t.Errorf("input exists = %t, err = %v; want present", exists, err)
	}
	if exists, err := manager.StorageBackend.Exists(ctx, outputPath); err != nil || !exists {
		t.Errorf("output exists = %t, err = %v; want present", exists, err)
	}

	// And a second cycle is a no-op rather than another full re-hash.
	if recovered, err := manager.recoverOrphanedManifests(ctx, recoveryScope{}); err != nil || recovered != 0 {
		t.Fatalf("second cycle = (%d, %v), want (0, nil) -- a parked manifest must be invisible to later passes", recovered, err)
	}
}

// sizeBlindBackend embeds the Backend INTERFACE, so only Backend's methods are
// in its static method set and ObjectLister is not promoted -- the established
// shape for a fake that hides an optional interface. That skips the size check
// in recoverLoadedManifest, which is the gap the hook's own byte count covers.
// Arc's three real backends all implement ObjectLister; this proves the
// handling does not depend on that.
type sizeBlindBackend struct {
	storage.Backend
}

// TestRecoveryDiscardsAShortOutputWithoutObjectLister: the stored output is
// shorter than the manifest records, and the backend cannot be asked for sizes,
// so the hook's byte count is the only check that sees it. It must reach the
// same partial-upload handling the ObjectLister path uses -- delete the output
// and the manifest so the next cycle redoes the job -- rather than parking or
// looping.
func TestRecoveryDiscardsAShortOutputWithoutObjectLister(t *testing.T) {
	root := t.TempDir()
	completionDir := filepath.Join(root, ".completion", "pending")

	local, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })
	backend := &sizeBlindBackend{Backend: local}
	if _, isLister := interface{}(backend).(storage.ObjectLister); isLister {
		t.Fatal("precondition: the backend must NOT implement ObjectLister, or the hook is not the check under test")
	}

	ctx := context.Background()
	outputPath := "db/cpu/2026/01/02/03/compacted.parquet"
	inputPath := "db/cpu/2026/01/02/03/input.parquet"
	if err := backend.Write(ctx, outputPath, []byte("short")); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, inputPath, []byte("source bytes")); err != nil {
		t.Fatal(err)
	}

	manifestManager := NewManifestManager(backend, zerolog.Nop())
	manifestPath, err := manifestManager.WriteManifest(ctx, &Manifest{
		OutputPath:    outputPath,
		OutputSize:    99999, // disagrees with the five bytes actually stored
		InputFiles:    []string{inputPath},
		Database:      "db",
		Measurement:   "cpu",
		PartitionPath: filepath.ToSlash(filepath.Dir(outputPath)),
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Date(2026, 1, 2, 4, 0, 0, 0, time.UTC),
		JobID:         "issue1155short",
	})
	if err != nil {
		t.Fatal(err)
	}
	manager := &Manager{
		StorageBackend:  backend,
		ManifestManager: manifestManager,
		CompletionDir:   completionDir,
		logger:          zerolog.Nop(),
	}

	recovered, err := manager.recoverOrphanedManifests(ctx, recoveryScope{})
	if err != nil {
		t.Fatalf("a partial upload must not surface as a cycle error: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1 (the manifest left the work set)", recovered)
	}

	// The partial output and its manifest both go, so the next cycle
	// rediscovers the input and redoes the job.
	if exists, err := backend.Exists(ctx, outputPath); err != nil || exists {
		t.Errorf("partial output exists = %t, err = %v; want deleted", exists, err)
	}
	if _, err := manifestManager.ReadManifest(ctx, manifestPath); err == nil {
		t.Error("manifest survived a partial upload; the next cycle cannot redo the job cleanly")
	}
	if parked := quarantinedManifests(t, root, manifestPath); len(parked) != 0 {
		t.Errorf("a partial upload must not be parked, it is retryable: %v", parked)
	}
	// The input is what the retry needs.
	if exists, err := backend.Exists(ctx, inputPath); err != nil || !exists {
		t.Errorf("input exists = %t, err = %v; want present for the retry", exists, err)
	}
	// No completion manifest: nothing was registered, so nothing to undo.
	if _, err := readCompletionManifest(filepath.Join(completionDir, "issue1155short.json")); err == nil {
		t.Error("a completion manifest was written for an output that was discarded")
	}
}
