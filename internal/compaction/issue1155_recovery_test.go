package compaction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestRecoveryWritesCompletionManifestBeforeDeletingInputs(t *testing.T) {
	root := t.TempDir()
	completionDir := filepath.Join(root, ".completion", "pending")
	manager, manifestPath, inputPath, outputPath, output := newIssue1155Recovery(t, root, completionDir)
	ctx := context.Background()
	partitionTime := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)

	manager.SetOnCompactedOutput(func(storageKey string) {
		if storageKey != outputPath {
			t.Errorf("compacted output callback path = %q, want %q", storageKey, outputPath)
		}
		completion, err := readCompletionManifest(filepath.Join(completionDir, "issue1155.json"))
		if err != nil {
			t.Errorf("read completion manifest before input deletion: %v", err)
			return
		}
		if completion.State != CompletionStateOutputWritten {
			t.Errorf("completion state before input deletion = %q, want %q", completion.State, CompletionStateOutputWritten)
		}
		exists, err := manager.StorageBackend.Exists(ctx, inputPath)
		if err != nil {
			t.Errorf("check input before deletion: %v", err)
		} else if !exists {
			t.Error("input was deleted before the output completion manifest was written")
		}
	})

	recovered, err := manager.recoverOrphanedManifests(ctx, recoveryScope{})
	if err != nil {
		t.Fatalf("recover orphaned manifest: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1", recovered)
	}
	if exists, err := manager.StorageBackend.Exists(ctx, inputPath); err != nil || exists {
		t.Fatalf("input exists after recovery = %t, err = %v; want absent", exists, err)
	}
	if _, err := manager.ManifestManager.ReadManifest(ctx, manifestPath); err == nil {
		t.Fatal("recovery manifest remains after successful recovery")
	}

	completion, err := readCompletionManifest(filepath.Join(completionDir, "issue1155.json"))
	if err != nil {
		t.Fatalf("read final completion manifest: %v", err)
	}
	if completion.State != CompletionStateSourcesDeleted {
		t.Fatalf("final completion state = %q, want %q", completion.State, CompletionStateSourcesDeleted)
	}
	if len(completion.Outputs) != 1 {
		t.Fatalf("completion outputs = %d, want 1", len(completion.Outputs))
	}
	wantHash := sha256.Sum256(output)
	gotOutput := completion.Outputs[0]
	if gotOutput.Path != outputPath {
		t.Errorf("output path = %q, want %q", gotOutput.Path, outputPath)
	}
	if gotOutput.SHA256 != hex.EncodeToString(wantHash[:]) {
		t.Errorf("output hash = %q, want %q", gotOutput.SHA256, hex.EncodeToString(wantHash[:]))
	}
	if gotOutput.SizeBytes != int64(len(output)) {
		t.Errorf("output size = %d, want %d", gotOutput.SizeBytes, len(output))
	}
	if !gotOutput.PartitionTime.Equal(partitionTime) {
		t.Errorf("partition time = %s, want %s", gotOutput.PartitionTime, partitionTime)
	}
	if len(completion.DeletedSources) != 1 || completion.DeletedSources[0] != inputPath {
		t.Errorf("deleted sources = %v, want [%s]", completion.DeletedSources, inputPath)
	}
}

func TestRecoveryKeepsInputsWhenCompletionManifestCannotBeWritten(t *testing.T) {
	root := t.TempDir()
	completionDir := filepath.Join(root, ".completion", "pending")
	if err := os.MkdirAll(completionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(completionDir, "issue1155.json.tmp"), 0o700); err != nil {
		t.Fatal(err)
	}
	manager, manifestPath, inputPath, _, _ := newIssue1155Recovery(t, root, completionDir)

	recovered, err := manager.recoverOrphanedManifests(context.Background(), recoveryScope{})
	if err == nil {
		t.Fatal("expected completion manifest write to fail")
	}
	if recovered != 0 {
		t.Fatalf("recovered = %d, want 0 after completion write failure", recovered)
	}
	if exists, err := manager.StorageBackend.Exists(context.Background(), inputPath); err != nil || !exists {
		t.Fatalf("input exists after failed completion write = %t, err = %v; want present", exists, err)
	}
	if _, err := manager.ManifestManager.ReadManifest(context.Background(), manifestPath); err != nil {
		t.Fatalf("recovery manifest was removed after completion write failure: %v", err)
	}
}

func TestRecoveryAdvancesWritingOutputCompletionManifest(t *testing.T) {
	root := t.TempDir()
	completionDir := filepath.Join(root, ".completion", "pending")
	manager, _, _, _, _ := newIssue1155Recovery(t, root, completionDir)
	if err := writeCompletionManifest(completionDir, &CompletionManifest{
		JobID:     "issue1155",
		State:     CompletionStateWritingOutput,
		CreatedAt: time.Date(2026, 1, 2, 4, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatal(err)
	}

	if recovered, err := manager.recoverOrphanedManifests(context.Background(), recoveryScope{}); err != nil || recovered != 1 {
		t.Fatalf("recovery = (%d, %v), want (1, nil)", recovered, err)
	}
	completion, err := readCompletionManifest(filepath.Join(completionDir, "issue1155.json"))
	if err != nil {
		t.Fatal(err)
	}
	if completion.State != CompletionStateSourcesDeleted {
		t.Fatalf("completion state = %q, want %q", completion.State, CompletionStateSourcesDeleted)
	}
}

func TestRecoveryPartitionTimeSupportsExistingTiers(t *testing.T) {
	tests := []struct {
		name     string
		manifest Manifest
		want     time.Time
	}{
		{
			name:     "hourly legacy manifest",
			manifest: Manifest{PartitionPath: "db/cpu/2026/01/02/03", Tier: "hourly"},
			want:     time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC),
		},
		{
			name:     "daily legacy manifest",
			manifest: Manifest{PartitionPath: "db/cpu/2026/01/02", Tier: "daily"},
			want:     time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := recoveryPartitionTime(&tt.manifest)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("partition time = %s, want %s", got, tt.want)
			}
		})
	}
}

func newIssue1155Recovery(t *testing.T, root, completionDir string) (*Manager, string, string, string, []byte) {
	t.Helper()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	outputPath := "db/cpu/2026/01/02/03/compacted.parquet"
	inputPath := "db/cpu/2026/01/02/03/input.parquet"
	output := []byte("compacted bytes")
	if err := backend.Write(context.Background(), outputPath, output); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(context.Background(), inputPath, []byte("source bytes")); err != nil {
		t.Fatal(err)
	}

	manifestManager := NewManifestManager(backend, zerolog.Nop())
	manifestPath, err := manifestManager.WriteManifest(context.Background(), &Manifest{
		OutputPath:    outputPath,
		OutputSize:    int64(len(output)),
		InputFiles:    []string{inputPath},
		Database:      "db",
		Measurement:   "cpu",
		PartitionPath: filepath.ToSlash(filepath.Dir(outputPath)),
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Date(2026, 1, 2, 4, 0, 0, 0, time.UTC),
		JobID:         "issue1155",
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
	return manager, manifestPath, inputPath, outputPath, output
}
