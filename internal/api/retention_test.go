package api

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// setupTestRetentionHandler creates a test retention handler with local storage
func setupTestRetentionHandler(t *testing.T) (*RetentionHandler, string) {
	t.Helper()

	// Create temporary directory for tests
	tmpDir, err := os.MkdirTemp("", "arc-retention-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create LocalBackend: %v", err)
	}

	// Create a DuckDB instance for tests
	duckdb, err := database.New(&database.Config{
		MemoryLimit:    "256MB",
		ThreadCount:    2,
		MaxConnections: 2,
	}, logger)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create DuckDB: %v", err)
	}

	retentionCfg := &config.RetentionConfig{
		Enabled: true,
		DBPath:  filepath.Join(tmpDir, "retention.db"),
	}

	handler, err := NewRetentionHandler(backend, duckdb, retentionCfg, logger)
	if err != nil {
		duckdb.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create RetentionHandler: %v", err)
	}

	t.Cleanup(func() {
		handler.Close()
		duckdb.Close()
		os.RemoveAll(tmpDir)
	})

	return handler, tmpDir
}

func TestBuildParquetPath_LocalBackend(t *testing.T) {
	handler, tmpDir := setupTestRetentionHandler(t)

	path := handler.buildParquetPath("testdb/measurements/2024/01/01/00/data.parquet")
	expected := filepath.Join(tmpDir, "testdb/measurements/2024/01/01/00/data.parquet")

	if path != expected {
		t.Errorf("buildParquetPath() = %q, want %q", path, expected)
	}
}

func TestBuildParquetPath_S3Backend(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)

	// Create handler with S3 backend
	tmpDir, err := os.MkdirTemp("", "arc-retention-s3-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	retentionCfg := &config.RetentionConfig{
		Enabled: true,
		DBPath:  filepath.Join(tmpDir, "retention.db"),
	}

	duckdb, err := database.New(&database.Config{
		MemoryLimit:    "256MB",
		ThreadCount:    2,
		MaxConnections: 2,
	}, logger)
	if err != nil {
		t.Fatalf("failed to create DuckDB: %v", err)
	}
	defer duckdb.Close()

	// Create a real S3 backend for testing path generation
	s3Cfg := &storage.S3Config{
		Bucket:    "test-bucket",
		Region:    "us-east-1",
		Endpoint:  "localhost:9000",
		UseSSL:    false,
		PathStyle: true,
		AccessKey: "test",
		SecretKey: "test",
	}
	s3Backend, err := storage.NewS3Backend(s3Cfg, logger)
	if err != nil {
		// Skip test if we can't create S3 backend (no MinIO running)
		t.Skipf("Skipping S3 test - could not create S3 backend: %v", err)
	}

	handler := &RetentionHandler{
		storage: s3Backend,
		config:  retentionCfg,
		duckdb:  duckdb,
		logger:  logger,
	}

	path := handler.buildParquetPath("testdb/measurements/2024/01/01/00/data.parquet")
	expected := "s3://test-bucket/testdb/measurements/2024/01/01/00/data.parquet"

	if path != expected {
		t.Errorf("buildParquetPath() = %q, want %q", path, expected)
	}
}

func TestGetMeasurementsToProcess_SpecificMeasurement(t *testing.T) {
	handler, _ := setupTestRetentionHandler(t)

	measurement := "temperature"
	policy := &RetentionPolicy{
		Database:    "testdb",
		Measurement: &measurement,
	}

	measurements, err := handler.getMeasurementsToProcess(context.Background(), policy)
	if err != nil {
		t.Fatalf("getMeasurementsToProcess() error = %v", err)
	}

	if len(measurements) != 1 || measurements[0] != "temperature" {
		t.Errorf("getMeasurementsToProcess() = %v, want [temperature]", measurements)
	}
}

func TestGetMeasurementsToProcess_AllMeasurements(t *testing.T) {
	handler, tmpDir := setupTestRetentionHandler(t)

	// Create some test measurement directories with parquet files
	testFiles := []string{
		"testdb/temperature/2024/01/01/00/data.parquet",
		"testdb/humidity/2024/01/01/00/data.parquet",
		"testdb/pressure/2024/01/01/00/data.parquet",
	}

	for _, f := range testFiles {
		fullPath := filepath.Join(tmpDir, f)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}
		if err := os.WriteFile(fullPath, []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
	}

	policy := &RetentionPolicy{
		Database:    "testdb",
		Measurement: nil, // nil means all measurements
	}

	measurements, err := handler.getMeasurementsToProcess(context.Background(), policy)
	if err != nil {
		t.Fatalf("getMeasurementsToProcess() error = %v", err)
	}

	if len(measurements) != 3 {
		t.Errorf("getMeasurementsToProcess() returned %d measurements, want 3", len(measurements))
	}

	// Check all expected measurements are present
	measurementSet := make(map[string]bool)
	for _, m := range measurements {
		measurementSet[m] = true
	}

	for _, expected := range []string{"temperature", "humidity", "pressure"} {
		if !measurementSet[expected] {
			t.Errorf("getMeasurementsToProcess() missing measurement %q", expected)
		}
	}
}

func TestDeleteOldFiles_NoFiles(t *testing.T) {
	handler, _ := setupTestRetentionHandler(t)

	cutoff := time.Now().Add(-24 * time.Hour)
	deletedRows, deletedFiles, err := handler.deleteOldFiles(context.Background(), "testdb", "nonexistent", cutoff, false)

	if err != nil {
		t.Fatalf("deleteOldFiles() error = %v", err)
	}

	if deletedRows != 0 || deletedFiles != 0 {
		t.Errorf("deleteOldFiles() = (%d, %d), want (0, 0)", deletedRows, deletedFiles)
	}
}

func TestDeleteOldFiles_DryRun(t *testing.T) {
	handler, tmpDir := setupTestRetentionHandler(t)

	// Create a test parquet file with DuckDB
	db := handler.duckdb.DB()

	measurementDir := filepath.Join(tmpDir, "testdb", "logs", "2020", "01", "01", "00")
	if err := os.MkdirAll(measurementDir, 0755); err != nil {
		t.Fatalf("failed to create measurement dir: %v", err)
	}

	parquetPath := filepath.Join(measurementDir, "test.parquet")

	// Create a parquet file with old timestamps (2020)
	createSQL := `COPY (
		SELECT
			TIMESTAMP '2020-01-01 00:00:00' as time,
			'test' as message
		FROM range(10)
	) TO '` + parquetPath + `' (FORMAT PARQUET)`

	if _, err := db.Exec(createSQL); err != nil {
		t.Fatalf("failed to create test parquet file: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(parquetPath); os.IsNotExist(err) {
		t.Fatalf("test parquet file was not created")
	}

	// Run dry-run deletion with a cutoff date after the data
	cutoff := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	deletedRows, deletedFiles, err := handler.deleteOldFiles(context.Background(), "testdb", "logs", cutoff, true)

	if err != nil {
		t.Fatalf("deleteOldFiles() error = %v", err)
	}

	// Should report files eligible for deletion
	if deletedFiles != 1 {
		t.Errorf("deleteOldFiles(dry_run=true) deletedFiles = %d, want 1", deletedFiles)
	}

	if deletedRows != 10 {
		t.Errorf("deleteOldFiles(dry_run=true) deletedRows = %d, want 10", deletedRows)
	}

	// File should still exist (dry run)
	if _, err := os.Stat(parquetPath); os.IsNotExist(err) {
		t.Error("deleteOldFiles(dry_run=true) should not delete the file")
	}
}

func TestDeleteOldFiles_ActualDelete(t *testing.T) {
	handler, tmpDir := setupTestRetentionHandler(t)

	// Create a test parquet file with DuckDB
	db := handler.duckdb.DB()

	measurementDir := filepath.Join(tmpDir, "testdb", "logs", "2020", "01", "01", "00")
	if err := os.MkdirAll(measurementDir, 0755); err != nil {
		t.Fatalf("failed to create measurement dir: %v", err)
	}

	parquetPath := filepath.Join(measurementDir, "test.parquet")

	// Create a parquet file with old timestamps (2020)
	createSQL := `COPY (
		SELECT
			TIMESTAMP '2020-01-01 00:00:00' as time,
			'test' as message
		FROM range(10)
	) TO '` + parquetPath + `' (FORMAT PARQUET)`

	if _, err := db.Exec(createSQL); err != nil {
		t.Fatalf("failed to create test parquet file: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(parquetPath); os.IsNotExist(err) {
		t.Fatalf("test parquet file was not created")
	}

	// Run actual deletion with a cutoff date after the data
	cutoff := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	deletedRows, deletedFiles, err := handler.deleteOldFiles(context.Background(), "testdb", "logs", cutoff, false)

	if err != nil {
		t.Fatalf("deleteOldFiles() error = %v", err)
	}

	// Should report files deleted
	if deletedFiles != 1 {
		t.Errorf("deleteOldFiles() deletedFiles = %d, want 1", deletedFiles)
	}

	if deletedRows != 10 {
		t.Errorf("deleteOldFiles() deletedRows = %d, want 10", deletedRows)
	}

	// File should be deleted
	if _, err := os.Stat(parquetPath); !os.IsNotExist(err) {
		t.Error("deleteOldFiles() should have deleted the file")
	}
}

func TestDeleteOldFiles_KeepsRecentFiles(t *testing.T) {
	handler, tmpDir := setupTestRetentionHandler(t)

	// Create a test parquet file with DuckDB
	db := handler.duckdb.DB()

	measurementDir := filepath.Join(tmpDir, "testdb", "logs", "2025", "01", "01", "00")
	if err := os.MkdirAll(measurementDir, 0755); err != nil {
		t.Fatalf("failed to create measurement dir: %v", err)
	}

	parquetPath := filepath.Join(measurementDir, "test.parquet")

	// Create a parquet file with recent timestamps (2025)
	createSQL := `COPY (
		SELECT
			TIMESTAMP '2025-01-01 00:00:00' as time,
			'test' as message
		FROM range(10)
	) TO '` + parquetPath + `' (FORMAT PARQUET)`

	if _, err := db.Exec(createSQL); err != nil {
		t.Fatalf("failed to create test parquet file: %v", err)
	}

	// Run deletion with a cutoff date before the data
	cutoff := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	deletedRows, deletedFiles, err := handler.deleteOldFiles(context.Background(), "testdb", "logs", cutoff, false)

	if err != nil {
		t.Fatalf("deleteOldFiles() error = %v", err)
	}

	// Should not delete any files
	if deletedFiles != 0 {
		t.Errorf("deleteOldFiles() deletedFiles = %d, want 0 (file is recent)", deletedFiles)
	}

	if deletedRows != 0 {
		t.Errorf("deleteOldFiles() deletedRows = %d, want 0 (file is recent)", deletedRows)
	}

	// File should still exist
	if _, err := os.Stat(parquetPath); os.IsNotExist(err) {
		t.Error("deleteOldFiles() should not delete recent files")
	}
}
