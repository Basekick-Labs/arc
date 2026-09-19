package compaction

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// The cold lookup and both cached lookup paths must report the same
// protected input AND output files.
func TestManifestCacheRetainsOutputIssue750(t *testing.T) {
	ctx := context.Background()

	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	manager := NewManifestManager(backend, zerolog.Nop())

	manifest := &Manifest{
		OutputPath: "db/cpu/2026/09/19/02/output.parquet",
		OutputSize: 1024,
		InputFiles: []string{
			"db/cpu/2026/09/19/02/input-a.parquet",
			"db/cpu/2026/09/19/02/input-b.parquet",
		},
		Database:      "db",
		Measurement:   "cpu",
		PartitionPath: "db/cpu/2026/09/19/02",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "issue750-cache-output",
	}

	if _, err := manager.WriteManifest(ctx, manifest); err != nil {
		t.Fatal(err)
	}

	// Pass 1 rebuilds the cache. Passes 2 and 3 read the warm cache.
	for pass := 1; pass <= 3; pass++ {
		files, err := manager.GetFilesInManifests(ctx)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}

		for _, input := range manifest.InputFiles {
			if _, ok := files[input]; !ok {
				t.Fatalf("pass %d missing input %q", pass, input)
			}
		}

		if _, ok := files[manifest.OutputPath]; !ok {
			t.Fatalf(
				"cache pass %d missing output %q",
				pass, manifest.OutputPath,
			)
		}

		if want := len(manifest.InputFiles) + 1; len(files) != want {
			t.Fatalf("pass %d: got %d files, want %d",
				pass, len(files), want)
		}
	}

	// The lookup used by other callers must agree with the set above.
	found, err := manager.IsFileInManifest(ctx, manifest.OutputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("IsFileInManifest forgot the cached output")
	}
}
