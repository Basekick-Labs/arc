package compaction

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestStaleManifestStillRecoversInputsIssue750(t *testing.T) {
	ctx := context.Background()

	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	input := "db/cpu/2026/09/12/14/raw.parquet"
	output := "db/cpu/2026/09/12/14/compacted.parquet"
	outputBody := []byte("compacted-output")

	if err := backend.Write(ctx, input, []byte("raw-input")); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, output, outputBody); err != nil {
		t.Fatal(err)
	}

	manager := NewManifestManager(backend, zerolog.Nop())
	manifest := &Manifest{
		OutputPath:    output,
		OutputSize:    int64(len(outputBody)),
		InputFiles:    []string{input},
		Database:      "db",
		Measurement:   "cpu",
		PartitionPath: "db/cpu/2026/09/12/14",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().Add(-ManifestMaxAge - 24*time.Hour),
		JobID:         "issue750-stale-age",
	}

	manifestPath, err := manager.WriteManifest(ctx, manifest)
	if err != nil {
		t.Fatal(err)
	}

	var keptOutput string
	var consumed []string

	recovered, err := manager.RecoverOrphanedManifests(
		ctx,
		func(path string) { keptOutput = path },
		func(paths []string) error {
			consumed = append(consumed, paths...)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("recovered %d manifests, want 1", recovered)
	}
	if keptOutput != output {
		t.Fatalf("kept output = %q, want %q", keptOutput, output)
	}
	if len(consumed) != 1 || consumed[0] != input {
		t.Fatalf("consumed = %v, want [%q]", consumed, input)
	}

	for _, tc := range []struct {
		key  string
		want bool
	}{
		{output, true},
		{input, false},
		{manifestPath, false},
	} {
		exists, err := backend.Exists(ctx, tc.key)
		if err != nil {
			t.Fatal(err)
		}
		if exists != tc.want {
			t.Errorf("Exists(%q) = %v, want %v",
				tc.key, exists, tc.want)
		}
	}
}
