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

func TestManifestManager_WriteAndRead(t *testing.T) {
	// Create temp directory for test
	tempDir, err := os.MkdirTemp("", "manifest_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create local storage backend
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	backend, err := storage.NewLocalBackend(tempDir, logger)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	// Create manifest manager
	mm := NewManifestManager(backend, logger)

	// Create a test manifest
	manifest := &Manifest{
		OutputPath:    "testdb/cpu/2025/01/15/10/cpu_20250115_100000_compacted.parquet",
		OutputSize:    1024 * 1024,
		InputFiles:    []string{"testdb/cpu/2025/01/15/10/file1.parquet", "testdb/cpu/2025/01/15/10/file2.parquet"},
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionPath: "testdb/cpu/2025/01/15/10",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "test_job_123",
	}

	ctx := context.Background()

	// Write manifest
	manifestPath, err := mm.WriteManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("Failed to write manifest: %v", err)
	}

	// Verify path format
	expectedPathPrefix := filepath.Join(ManifestBasePath, "hourly", "testdb")
	if !manifestContains(manifestPath, expectedPathPrefix) {
		t.Errorf("Manifest path %s doesn't contain expected prefix %s", manifestPath, expectedPathPrefix)
	}

	// Read manifest back
	readManifest, err := mm.ReadManifest(ctx, manifestPath)
	if err != nil {
		t.Fatalf("Failed to read manifest: %v", err)
	}

	// Verify fields
	if readManifest.OutputPath != manifest.OutputPath {
		t.Errorf("OutputPath mismatch: got %s, want %s", readManifest.OutputPath, manifest.OutputPath)
	}
	if readManifest.OutputSize != manifest.OutputSize {
		t.Errorf("OutputSize mismatch: got %d, want %d", readManifest.OutputSize, manifest.OutputSize)
	}
	if len(readManifest.InputFiles) != len(manifest.InputFiles) {
		t.Errorf("InputFiles length mismatch: got %d, want %d", len(readManifest.InputFiles), len(manifest.InputFiles))
	}
	if readManifest.Database != manifest.Database {
		t.Errorf("Database mismatch: got %s, want %s", readManifest.Database, manifest.Database)
	}
	if readManifest.Tier != manifest.Tier {
		t.Errorf("Tier mismatch: got %s, want %s", readManifest.Tier, manifest.Tier)
	}
	if readManifest.Status != manifest.Status {
		t.Errorf("Status mismatch: got %s, want %s", readManifest.Status, manifest.Status)
	}

	// Delete manifest
	err = mm.DeleteManifest(ctx, manifestPath)
	if err != nil {
		t.Fatalf("Failed to delete manifest: %v", err)
	}

	// Verify deleted
	_, err = mm.ReadManifest(ctx, manifestPath)
	if err == nil {
		t.Error("Expected error reading deleted manifest, got nil")
	}
}

func TestManifestManager_ListManifests(t *testing.T) {
	// Create temp directory for test
	tempDir, err := os.MkdirTemp("", "manifest_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create local storage backend
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	backend, err := storage.NewLocalBackend(tempDir, logger)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	mm := NewManifestManager(backend, logger)
	ctx := context.Background()

	// Initially empty
	manifests, err := mm.ListManifests(ctx)
	if err != nil {
		t.Fatalf("Failed to list manifests: %v", err)
	}
	if len(manifests) != 0 {
		t.Errorf("Expected 0 manifests, got %d", len(manifests))
	}

	// Create multiple manifests
	for i := 0; i < 3; i++ {
		manifest := &Manifest{
			OutputPath:    "testdb/cpu/2025/01/15/10/output.parquet",
			OutputSize:    1024,
			InputFiles:    []string{"input.parquet"},
			Database:      "testdb",
			Measurement:   "cpu",
			PartitionPath: "testdb/cpu/2025/01/15/10",
			Tier:          "hourly",
			Status:        ManifestStatusPending,
			CreatedAt:     time.Now().UTC(),
			JobID:         "job_" + string(rune('0'+i)),
		}
		_, err := mm.WriteManifest(ctx, manifest)
		if err != nil {
			t.Fatalf("Failed to write manifest %d: %v", i, err)
		}
	}

	// List again
	manifests, err = mm.ListManifests(ctx)
	if err != nil {
		t.Fatalf("Failed to list manifests: %v", err)
	}
	if len(manifests) != 3 {
		t.Errorf("Expected 3 manifests, got %d", len(manifests))
	}
}

func TestManifestManager_GetFilesInManifests(t *testing.T) {
	// Create temp directory for test
	tempDir, err := os.MkdirTemp("", "manifest_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create local storage backend
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	backend, err := storage.NewLocalBackend(tempDir, logger)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	mm := NewManifestManager(backend, logger)
	ctx := context.Background()

	// Create manifest with input files
	manifest := &Manifest{
		OutputPath:    "testdb/cpu/output.parquet",
		OutputSize:    1024,
		InputFiles:    []string{"testdb/cpu/file1.parquet", "testdb/cpu/file2.parquet", "testdb/cpu/file3.parquet"},
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionPath: "testdb/cpu",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "test_job",
	}
	_, err = mm.WriteManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("Failed to write manifest: %v", err)
	}

	// Get files in manifests
	files, err := mm.GetFilesInManifests(ctx)
	if err != nil {
		t.Fatalf("Failed to get files in manifests: %v", err)
	}

	// Should contain all input files
	for _, f := range manifest.InputFiles {
		if _, exists := files[f]; !exists {
			t.Errorf("Expected file %s to be in manifest files", f)
		}
	}

	// Should also contain output file
	if _, exists := files[manifest.OutputPath]; !exists {
		t.Errorf("Expected output file %s to be in manifest files", manifest.OutputPath)
	}

	// Total should be inputs + output
	expectedCount := len(manifest.InputFiles) + 1
	if len(files) != expectedCount {
		t.Errorf("Expected %d files, got %d", expectedCount, len(files))
	}
}

func TestManifestManager_RecoverOrphanedManifests_OutputMissing(t *testing.T) {
	// Create temp directory for test
	tempDir, err := os.MkdirTemp("", "manifest_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create local storage backend
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	backend, err := storage.NewLocalBackend(tempDir, logger)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	mm := NewManifestManager(backend, logger)
	ctx := context.Background()

	// Create manifest (output file doesn't exist)
	manifest := &Manifest{
		OutputPath:    "testdb/cpu/output.parquet", // This file doesn't exist
		OutputSize:    1024,
		InputFiles:    []string{"testdb/cpu/file1.parquet"},
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionPath: "testdb/cpu",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "orphan_job",
	}
	manifestPath, err := mm.WriteManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("Failed to write manifest: %v", err)
	}

	// Verify manifest exists
	manifests, _ := mm.ListManifests(ctx)
	if len(manifests) != 1 {
		t.Fatalf("Expected 1 manifest before recovery, got %d", len(manifests))
	}

	// Run recovery - should delete manifest because output doesn't exist
	recovered, err := mm.RecoverOrphanedManifests(ctx)
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	if recovered != 1 {
		t.Errorf("Expected 1 recovered, got %d", recovered)
	}

	// Verify manifest was deleted
	_, err = mm.ReadManifest(ctx, manifestPath)
	if err == nil {
		t.Error("Expected manifest to be deleted after recovery")
	}
}

func TestManifestManager_RecoverOrphanedManifests_OutputExists(t *testing.T) {
	// Create temp directory for test
	tempDir, err := os.MkdirTemp("", "manifest_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create local storage backend
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	backend, err := storage.NewLocalBackend(tempDir, logger)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	mm := NewManifestManager(backend, logger)
	ctx := context.Background()

	// Create output file (simulating successful upload)
	outputPath := "testdb/cpu/output.parquet"
	outputContent := []byte("fake parquet data")
	err = backend.Write(ctx, outputPath, outputContent)
	if err != nil {
		t.Fatalf("Failed to write output file: %v", err)
	}

	// Create input files (these should be deleted by recovery)
	inputFile := "testdb/cpu/input1.parquet"
	err = backend.Write(ctx, inputFile, []byte("input data"))
	if err != nil {
		t.Fatalf("Failed to write input file: %v", err)
	}

	// Create manifest
	manifest := &Manifest{
		OutputPath:    outputPath,
		OutputSize:    int64(len(outputContent)),
		InputFiles:    []string{inputFile},
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionPath: "testdb/cpu",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "orphan_job",
	}
	manifestPath, err := mm.WriteManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("Failed to write manifest: %v", err)
	}

	// Run recovery - should delete input files and manifest
	recovered, err := mm.RecoverOrphanedManifests(ctx)
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	if recovered != 1 {
		t.Errorf("Expected 1 recovered, got %d", recovered)
	}

	// Verify manifest was deleted
	_, err = mm.ReadManifest(ctx, manifestPath)
	if err == nil {
		t.Error("Expected manifest to be deleted after recovery")
	}

	// Verify input file was deleted
	exists, _ := backend.Exists(ctx, inputFile)
	if exists {
		t.Error("Expected input file to be deleted after recovery")
	}

	// Verify output file still exists
	exists, _ = backend.Exists(ctx, outputPath)
	if !exists {
		t.Error("Expected output file to still exist after recovery")
	}
}

func TestManifestManager_IsFileInManifest(t *testing.T) {
	// Create temp directory for test
	tempDir, err := os.MkdirTemp("", "manifest_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create local storage backend
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	backend, err := storage.NewLocalBackend(tempDir, logger)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	mm := NewManifestManager(backend, logger)
	ctx := context.Background()

	// Create manifest
	manifest := &Manifest{
		OutputPath:    "testdb/cpu/output.parquet",
		OutputSize:    1024,
		InputFiles:    []string{"testdb/cpu/tracked.parquet"},
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionPath: "testdb/cpu",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "test_job",
	}
	_, err = mm.WriteManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("Failed to write manifest: %v", err)
	}

	// Check tracked file
	inManifest, err := mm.IsFileInManifest(ctx, "testdb/cpu/tracked.parquet")
	if err != nil {
		t.Fatalf("Failed to check file: %v", err)
	}
	if !inManifest {
		t.Error("Expected tracked file to be in manifest")
	}

	// Check untracked file
	inManifest, err = mm.IsFileInManifest(ctx, "testdb/cpu/untracked.parquet")
	if err != nil {
		t.Fatalf("Failed to check file: %v", err)
	}
	if inManifest {
		t.Error("Expected untracked file to not be in manifest")
	}
}

func manifestContains(s, substr string) bool {
	return strings.Contains(s, substr)
}
