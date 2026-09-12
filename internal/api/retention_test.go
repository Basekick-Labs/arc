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

	// Create a DuckDB instance for tests. LocalStorageRoot is needed since
	// the sandbox active in New() restricts file reads/writes to allowlisted
	// directories — the test fixtures live under tmpDir.
	duckdb, err := database.New(&database.Config{
		MemoryLimit:      "256MB",
		ThreadCount:      2,
		MaxConnections:   2,
		LocalStorageRoot: tmpDir,
	}, logger)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create DuckDB: %v", err)
	}

	retentionCfg := &config.RetentionConfig{
		Enabled: true,
		DBPath:  filepath.Join(tmpDir, "retention.db"),
	}

	handler, err := NewRetentionHandler(backend, duckdb, retentionCfg, nil, nil, logger)
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

// Retention resolves a listed key to the location the backend actually wrote
// it to. Before #746 it built "s3://{bucket}/{key}" with no prefix, so on a
// deployment with storage.s3_prefix set every file 404'd, the per-file read
// errored, and deleteOldFiles skipped it: retention deleted nothing at all,
// while still recording the run as "completed".
//
// The prefixed case is the one that matters and is exactly the one the old
// tests here did not cover.
func TestRetentionResolvesKeysToWrittenLocation(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	const key = "testdb/measurements/2024/01/01/00/data.parquet"

	t.Run("local", func(t *testing.T) {
		handler, tmpDir := setupTestRetentionHandler(t)
		got, err := storage.ObjectURI(handler.storage, key)
		if err != nil {
			t.Fatalf("ObjectURI: %v", err)
		}
		if want := filepath.Join(tmpDir, key); got != want {
			t.Errorf("ObjectURI() = %q, want %q", got, want)
		}
	})

	for _, tc := range []struct{ name, prefix, want string }{
		{"s3 without prefix", "", "s3://test-bucket/" + key},
		{"s3 with prefix", "tenant", "s3://test-bucket/tenant/" + key},
		{"s3 with nested prefix", "a/b", "s3://test-bucket/a/b/" + key},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend, err := storage.NewS3Backend(&storage.S3Config{
				Bucket: "test-bucket", Region: "us-east-1", Endpoint: "localhost:9000",
				UseSSL: false, PathStyle: true, AccessKey: "test", SecretKey: "test",
				Prefix: tc.prefix,
			}, logger)
			if err != nil {
				t.Skipf("could not create S3 backend: %v", err)
			}
			got, err := storage.ObjectURI(backend, key)
			if err != nil {
				t.Fatalf("ObjectURI: %v", err)
			}
			if got != tc.want {
				t.Errorf("ObjectURI() = %q, want %q", got, tc.want)
			}
		})
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
	deletedRows, deletedFiles, skipped, err := handler.deleteOldFiles(context.Background(), "testdb", "nonexistent", cutoff, false, "retention:test")

	if err != nil {
		t.Fatalf("deleteOldFiles() error = %v", err)
	}
	if skipped != 0 {
		t.Errorf("deleteOldFiles() skipped = %d, want 0", skipped)
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
	deletedRows, deletedFiles, skipped, err := handler.deleteOldFiles(context.Background(), "testdb", "logs", cutoff, true, "retention:test")

	if err != nil {
		t.Fatalf("deleteOldFiles() error = %v", err)
	}
	if skipped != 0 {
		t.Errorf("deleteOldFiles() skipped = %d, want 0", skipped)
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
	deletedRows, deletedFiles, skipped, err := handler.deleteOldFiles(context.Background(), "testdb", "logs", cutoff, false, "retention:test")

	if err != nil {
		t.Fatalf("deleteOldFiles() error = %v", err)
	}
	if skipped != 0 {
		t.Errorf("deleteOldFiles() skipped = %d, want 0", skipped)
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
	deletedRows, deletedFiles, skipped, err := handler.deleteOldFiles(context.Background(), "testdb", "logs", cutoff, false, "retention:test")

	if err != nil {
		t.Fatalf("deleteOldFiles() error = %v", err)
	}
	if skipped != 0 {
		t.Errorf("deleteOldFiles() skipped = %d, want 0", skipped)
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

func TestDeleteOldFiles_CleansUpEmptyDirectories(t *testing.T) {
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

	// Verify directory structure exists
	hourDir := filepath.Join(tmpDir, "testdb", "logs", "2020", "01", "01", "00")
	if _, err := os.Stat(hourDir); os.IsNotExist(err) {
		t.Fatalf("hour directory was not created")
	}

	// Run actual deletion with a cutoff date after the data
	cutoff := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	_, _, skipped, err := handler.deleteOldFiles(context.Background(), "testdb", "logs", cutoff, false, "retention:test")

	if err != nil {
		t.Fatalf("deleteOldFiles() error = %v", err)
	}
	if skipped != 0 {
		t.Errorf("deleteOldFiles() skipped = %d, want 0", skipped)
	}

	// Hour directory should be deleted (empty after file deletion)
	if _, err := os.Stat(hourDir); !os.IsNotExist(err) {
		t.Error("deleteOldFiles() should have deleted empty hour directory")
	}

	// Day directory should be deleted (empty after hour deletion)
	dayDir := filepath.Join(tmpDir, "testdb", "logs", "2020", "01", "01")
	if _, err := os.Stat(dayDir); !os.IsNotExist(err) {
		t.Error("deleteOldFiles() should have deleted empty day directory")
	}

	// Month directory should be deleted (empty after day deletion)
	monthDir := filepath.Join(tmpDir, "testdb", "logs", "2020", "01")
	if _, err := os.Stat(monthDir); !os.IsNotExist(err) {
		t.Error("deleteOldFiles() should have deleted empty month directory")
	}

	// Year directory should be deleted (empty after month deletion)
	yearDir := filepath.Join(tmpDir, "testdb", "logs", "2020")
	if _, err := os.Stat(yearDir); !os.IsNotExist(err) {
		t.Error("deleteOldFiles() should have deleted empty year directory")
	}

	// Measurement directory should still exist (we don't delete it)
	measurementBaseDir := filepath.Join(tmpDir, "testdb", "logs")
	if _, err := os.Stat(measurementBaseDir); os.IsNotExist(err) {
		t.Error("deleteOldFiles() should NOT delete measurement directory")
	}
}

// TestReadParquetPathRejectsGlobMetacharacters.
//
// The read_parquet sink needs a rule the key contract deliberately lacks. A
// key containing "*" names exactly one object to a write and to any literal
// reader, so storage.ObjectURI accepts it; interpolated into read_parquet it is
// a pattern, and one file's key would silently expand to many.
//
// Reachable: edgesync.validateSpokeID has no character allowlist, and a spoke
// ID is the first path segment of everything that spoke writes into the hub's
// storage root.
func TestReadParquetPathRejectsGlobMetacharacters(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: "test-bucket", Region: "us-east-1", Endpoint: "localhost:9000",
		UseSSL: false, PathStyle: true, AccessKey: "test", SecretKey: "test",
		Prefix: "tenant",
	}, logger)
	if err != nil {
		t.Skipf("could not create S3 backend: %v", err)
	}

	for _, key := range []string{
		"rocket*01/cpu/2026/09/12/13/f.parquet",
		"db/cpu/2026/09/12/13/f?.parquet",
		"db/cpu/2026/09/12/13/f[0].parquet",
		"db/cpu/2026/09/12/13/f{1,2}.parquet",
	} {
		// ObjectURI itself must keep accepting these: the same key handed to
		// iceberg-go or os.Open reads exactly one file.
		if _, err := storage.ObjectURI(backend, key); err != nil {
			t.Errorf("ObjectURI(%q) must accept a literal location: %v", key, err)
		}
		if _, err := readParquetPath(backend, key); err == nil {
			t.Errorf("readParquetPath(%q) must reject a key that would glob", key)
		}
	}

	// And an ordinary key still resolves, with the prefix.
	const ok = "db/cpu/2026/09/12/13/f.parquet"
	got, err := readParquetPath(backend, ok)
	if err != nil {
		t.Fatalf("readParquetPath(%q): %v", ok, err)
	}
	if want := "s3://test-bucket/tenant/" + ok; got != want {
		t.Errorf("readParquetPath() = %q, want %q", got, want)
	}
}
