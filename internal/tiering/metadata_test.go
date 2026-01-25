package tiering

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

func setupTestMetadataStore(t *testing.T) (*MetadataStore, func()) {
	t.Helper()

	// Create temp file for SQLite
	tmpFile, err := os.CreateTemp("", "tiering_metadata_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		os.Remove(tmpFile.Name())
		t.Fatalf("Failed to open SQLite: %v", err)
	}

	logger := zerolog.Nop()
	store, err := NewMetadataStore(db, logger)
	if err != nil {
		db.Close()
		os.Remove(tmpFile.Name())
		t.Fatalf("Failed to create metadata store: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(tmpFile.Name())
	}

	return store, cleanup
}

func TestMetadataStore_RecordFile(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()

	file := &FileMetadata{
		Path:          "test/db/measurement/2025/01/01/00/file.parquet",
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Tier:          TierHot,
		SizeBytes:     1024,
		CreatedAt:     time.Now().UTC(),
	}

	// Record file
	err := store.RecordFile(ctx, file)
	if err != nil {
		t.Fatalf("RecordFile() error = %v", err)
	}

	// Retrieve file
	retrieved, err := store.GetFile(ctx, file.Path)
	if err != nil {
		t.Fatalf("GetFile() error = %v", err)
	}

	if retrieved == nil {
		t.Fatal("GetFile() returned nil")
	}

	if retrieved.Database != file.Database {
		t.Errorf("Database = %v, want %v", retrieved.Database, file.Database)
	}
	if retrieved.Measurement != file.Measurement {
		t.Errorf("Measurement = %v, want %v", retrieved.Measurement, file.Measurement)
	}
	if retrieved.Tier != file.Tier {
		t.Errorf("Tier = %v, want %v", retrieved.Tier, file.Tier)
	}
	if retrieved.SizeBytes != file.SizeBytes {
		t.Errorf("SizeBytes = %v, want %v", retrieved.SizeBytes, file.SizeBytes)
	}
}

func TestMetadataStore_UpdateTier(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()

	file := &FileMetadata{
		Path:          "test/db/measurement/2025/01/01/00/file.parquet",
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Tier:          TierHot,
		SizeBytes:     1024,
	}

	// Record file
	err := store.RecordFile(ctx, file)
	if err != nil {
		t.Fatalf("RecordFile() error = %v", err)
	}

	// Update tier
	err = store.UpdateTier(ctx, file.Path, TierCold)
	if err != nil {
		t.Fatalf("UpdateTier() error = %v", err)
	}

	// Verify tier changed
	retrieved, err := store.GetFile(ctx, file.Path)
	if err != nil {
		t.Fatalf("GetFile() error = %v", err)
	}

	if retrieved.Tier != TierCold {
		t.Errorf("Tier = %v, want %v", retrieved.Tier, TierCold)
	}

	if retrieved.MigratedAt == nil {
		t.Error("MigratedAt should not be nil after tier update")
	}
}

func TestMetadataStore_GetFilesInTier(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()

	// Add files in different tiers (2-tier system: hot and cold)
	files := []FileMetadata{
		{Path: "hot1.parquet", Database: "db1", Measurement: "m1", PartitionTime: time.Now().UTC(), Tier: TierHot, SizeBytes: 100},
		{Path: "hot2.parquet", Database: "db1", Measurement: "m1", PartitionTime: time.Now().UTC(), Tier: TierHot, SizeBytes: 200},
		{Path: "cold1.parquet", Database: "db1", Measurement: "m1", PartitionTime: time.Now().UTC(), Tier: TierCold, SizeBytes: 300},
	}

	for _, f := range files {
		file := f
		err := store.RecordFile(ctx, &file)
		if err != nil {
			t.Fatalf("RecordFile() error = %v", err)
		}
	}

	// Get hot tier files
	hotFiles, err := store.GetFilesInTier(ctx, TierHot)
	if err != nil {
		t.Fatalf("GetFilesInTier() error = %v", err)
	}

	if len(hotFiles) != 2 {
		t.Errorf("GetFilesInTier(hot) returned %d files, want 2", len(hotFiles))
	}

	// Get cold tier files
	coldFiles, err := store.GetFilesInTier(ctx, TierCold)
	if err != nil {
		t.Fatalf("GetFilesInTier() error = %v", err)
	}

	if len(coldFiles) != 1 {
		t.Errorf("GetFilesInTier(cold) returned %d files, want 1", len(coldFiles))
	}
}

func TestMetadataStore_GetFilesOlderThan(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()

	now := time.Now().UTC()

	// Add files with different ages
	files := []FileMetadata{
		{Path: "recent.parquet", Database: "db1", Measurement: "m1", PartitionTime: now.Add(-1 * time.Hour), Tier: TierHot, SizeBytes: 100},
		{Path: "old.parquet", Database: "db1", Measurement: "m1", PartitionTime: now.Add(-10 * 24 * time.Hour), Tier: TierHot, SizeBytes: 200},
		{Path: "very_old.parquet", Database: "db1", Measurement: "m1", PartitionTime: now.Add(-30 * 24 * time.Hour), Tier: TierHot, SizeBytes: 300},
	}

	for _, f := range files {
		file := f
		err := store.RecordFile(ctx, &file)
		if err != nil {
			t.Fatalf("RecordFile() error = %v", err)
		}
	}

	// Get files older than 7 days
	oldFiles, err := store.GetFilesOlderThan(ctx, TierHot, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("GetFilesOlderThan() error = %v", err)
	}

	if len(oldFiles) != 2 {
		t.Errorf("GetFilesOlderThan(7d) returned %d files, want 2", len(oldFiles))
	}
}

func TestMetadataStore_DeleteFile(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()

	file := &FileMetadata{
		Path:          "test.parquet",
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionTime: time.Now().UTC(),
		Tier:          TierHot,
		SizeBytes:     1024,
	}

	// Record file
	err := store.RecordFile(ctx, file)
	if err != nil {
		t.Fatalf("RecordFile() error = %v", err)
	}

	// Delete file
	err = store.DeleteFile(ctx, file.Path)
	if err != nil {
		t.Fatalf("DeleteFile() error = %v", err)
	}

	// Verify file is gone
	retrieved, err := store.GetFile(ctx, file.Path)
	if err != nil {
		t.Fatalf("GetFile() error = %v", err)
	}

	if retrieved != nil {
		t.Error("GetFile() should return nil after delete")
	}
}

func TestMetadataStore_GetTierStats(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()

	// Add files (2-tier system: hot and cold)
	files := []FileMetadata{
		{Path: "hot1.parquet", Database: "db1", Measurement: "m1", PartitionTime: time.Now().UTC(), Tier: TierHot, SizeBytes: 1024 * 1024},       // 1MB
		{Path: "hot2.parquet", Database: "db1", Measurement: "m1", PartitionTime: time.Now().UTC(), Tier: TierHot, SizeBytes: 2 * 1024 * 1024},   // 2MB
		{Path: "cold1.parquet", Database: "db1", Measurement: "m1", PartitionTime: time.Now().UTC(), Tier: TierCold, SizeBytes: 5 * 1024 * 1024}, // 5MB
	}

	for _, f := range files {
		file := f
		err := store.RecordFile(ctx, &file)
		if err != nil {
			t.Fatalf("RecordFile() error = %v", err)
		}
	}

	stats, err := store.GetTierStats(ctx)
	if err != nil {
		t.Fatalf("GetTierStats() error = %v", err)
	}

	if stats[TierHot].FileCount != 2 {
		t.Errorf("Hot tier file count = %d, want 2", stats[TierHot].FileCount)
	}
	if stats[TierHot].TotalSizeMB != 3 {
		t.Errorf("Hot tier size = %d MB, want 3 MB", stats[TierHot].TotalSizeMB)
	}

	if stats[TierCold].FileCount != 1 {
		t.Errorf("Cold tier file count = %d, want 1", stats[TierCold].FileCount)
	}
	if stats[TierCold].TotalSizeMB != 5 {
		t.Errorf("Cold tier size = %d MB, want 5 MB", stats[TierCold].TotalSizeMB)
	}
}
