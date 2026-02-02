package tiering

import (
	"context"
	"database/sql"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

// mockBackend implements storage.Backend for testing
type mockBackend struct {
	files map[string][]byte
	typ   string
	mu    sync.RWMutex
}

func newMockBackend(typ string) *mockBackend {
	return &mockBackend{
		files: make(map[string][]byte),
		typ:   typ,
	}
}

func (m *mockBackend) Type() string       { return m.typ }
func (m *mockBackend) ConfigJSON() string { return "{}" }
func (m *mockBackend) Read(_ context.Context, path string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if data, ok := m.files[path]; ok {
		return data, nil
	}
	return nil, os.ErrNotExist
}
func (m *mockBackend) ReadTo(_ context.Context, path string, w io.Writer) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if data, ok := m.files[path]; ok {
		_, err := w.Write(data)
		return err
	}
	return os.ErrNotExist
}
func (m *mockBackend) Write(_ context.Context, path string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[path] = data
	return nil
}
func (m *mockBackend) WriteReader(_ context.Context, path string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[path] = data
	return nil
}
func (m *mockBackend) Delete(_ context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, path)
	return nil
}
func (m *mockBackend) Exists(_ context.Context, path string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.files[path]
	return ok, nil
}
func (m *mockBackend) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var paths []string
	for p := range m.files {
		if len(p) >= len(prefix) && p[:len(prefix)] == prefix {
			paths = append(paths, p)
		}
	}
	return paths, nil
}
func (m *mockBackend) Close() error { return nil }

// mockLicenseClient implements enough of license.Client for testing
type mockLicenseClient struct {
	canUseTieredStorage bool
}

func (m *mockLicenseClient) CanUseTieredStorage() bool {
	return m.canUseTieredStorage
}

// setupIntegrationTest creates a full tiering setup for integration testing
// 2-tier system: Hot (local) -> Cold (S3/Azure)
func setupIntegrationTest(t *testing.T, withColdBackend bool) (*Manager, *mockBackend, *mockBackend, func()) {
	t.Helper()

	// Create temp file for SQLite
	tmpFile, err := os.CreateTemp("", "tiering_integration_test_*.db")
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

	hotBackend := newMockBackend("local")
	var coldBackend *mockBackend
	if withColdBackend {
		coldBackend = newMockBackend("s3")
	}

	cfg := &config.TieredStorageConfig{
		Enabled:                true,
		MigrationSchedule:      "0 2 * * *",
		MigrationMaxConcurrent: 2,
		MigrationBatchSize:     10,
		DefaultHotMaxAgeDays:   7,
		Cold: config.ColdTierConfig{
			Enabled:        withColdBackend,
			Backend:        "s3",
			S3Bucket:       "arc-archive-test",
			S3Region:       "us-east-1",
			S3StorageClass: "GLACIER",
		},
	}

	// Create a wrapper that implements what Manager needs
	type licenseWrapper interface {
		CanUseTieredStorage() bool
	}

	mockLicense := &mockLicenseClient{canUseTieredStorage: true}

	// Since we can't easily mock the full license.Client, we'll test the components
	// directly without the manager for integration tests

	// Create metadata store directly
	metadata, err := NewMetadataStore(db, logger)
	if err != nil {
		db.Close()
		os.Remove(tmpFile.Name())
		t.Fatalf("Failed to create metadata store: %v", err)
	}

	// Create policy store directly
	policies, err := NewPolicyStore(db, cfg, logger)
	if err != nil {
		db.Close()
		os.Remove(tmpFile.Name())
		t.Fatalf("Failed to create policy store: %v", err)
	}

	// Create a minimal manager-like struct for testing
	// We can't use NewManager because it requires a real license.Client
	m := &Manager{
		hotBackend:  hotBackend,
		coldBackend: coldBackend,
		metadata:    metadata,
		policies:    policies,
		config:      cfg,
		logger:      logger,
		stopCh:      make(chan struct{}),
	}

	// Create migrator
	m.migrator = NewMigrator(&MigratorConfig{
		Manager:       m,
		MaxConcurrent: cfg.MigrationMaxConcurrent,
		BatchSize:     cfg.MigrationBatchSize,
		Logger:        logger,
	})

	cleanup := func() {
		db.Close()
		os.Remove(tmpFile.Name())
	}

	// Need to assign mockLicense to a variable to avoid unused variable error
	_ = mockLicense

	return m, hotBackend, coldBackend, cleanup
}

func TestIntegration_RecordAndRetrieveFiles(t *testing.T) {
	m, hotBackend, _, cleanup := setupIntegrationTest(t, false)
	defer cleanup()

	ctx := context.Background()

	// Write some test data to hot backend
	testData := []byte("test parquet data")
	testPath := "testdb/cpu/2025/01/15/data.parquet"
	if err := hotBackend.Write(ctx, testPath, testData); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	// Record file in metadata
	file := &FileMetadata{
		Path:          testPath,
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionTime: time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC),
		SizeBytes:     int64(len(testData)),
	}

	if err := m.RecordNewFile(ctx, file); err != nil {
		t.Fatalf("RecordNewFile failed: %v", err)
	}

	// Verify file is recorded in hot tier
	retrieved, err := m.metadata.GetFile(ctx, testPath)
	if err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}

	if retrieved.Tier != TierHot {
		t.Errorf("Expected tier hot, got %s", retrieved.Tier)
	}
	if retrieved.Database != "testdb" {
		t.Errorf("Expected database testdb, got %s", retrieved.Database)
	}
}

func TestIntegration_PolicyBasedMigration(t *testing.T) {
	m, hotBackend, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()

	// Create test data for two databases
	db1Data := []byte("database 1 data")
	db2Data := []byte("database 2 data")

	db1Path := "db1/cpu/2025/01/01/cpu_20250101_000000_daily.parquet"
	db2Path := "db2/cpu/2025/01/01/cpu_20250101_000000_daily.parquet"

	if err := hotBackend.Write(ctx, db1Path, db1Data); err != nil {
		t.Fatalf("Failed to write db1 data: %v", err)
	}
	if err := hotBackend.Write(ctx, db2Path, db2Data); err != nil {
		t.Fatalf("Failed to write db2 data: %v", err)
	}

	// Record files - both with old partition time (should be eligible for migration)
	oldTime := time.Now().UTC().AddDate(0, 0, -30) // 30 days old

	file1 := &FileMetadata{
		Path:          db1Path,
		Database:      "db1",
		Measurement:   "cpu",
		PartitionTime: oldTime,
		SizeBytes:     int64(len(db1Data)),
	}
	file2 := &FileMetadata{
		Path:          db2Path,
		Database:      "db2",
		Measurement:   "cpu",
		PartitionTime: oldTime,
		SizeBytes:     int64(len(db2Data)),
	}

	if err := m.RecordNewFile(ctx, file1); err != nil {
		t.Fatalf("RecordNewFile failed: %v", err)
	}
	if err := m.RecordNewFile(ctx, file2); err != nil {
		t.Fatalf("RecordNewFile failed: %v", err)
	}

	// Set db2 as hot_only (should not migrate)
	policy := &DatabasePolicy{
		Database: "db2",
		HotOnly:  true,
	}
	if err := m.policies.Set(ctx, policy); err != nil {
		t.Fatalf("Set policy failed: %v", err)
	}

	// Find migration candidates (hot -> cold in 2-tier system)
	candidates, err := m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if err != nil {
		t.Fatalf("FindCandidates failed: %v", err)
	}

	// Only db1 should be a candidate (db2 is hot_only)
	if len(candidates) != 1 {
		t.Fatalf("Expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].Database != "db1" {
		t.Errorf("Expected candidate from db1, got %s", candidates[0].Database)
	}

	// Verify db2 is excluded
	for _, c := range candidates {
		if c.Database == "db2" {
			t.Error("db2 should be excluded (hot_only)")
		}
	}
}

func TestIntegration_CustomPolicyOverridesDefaults(t *testing.T) {
	m, _, coldBackend, cleanup := setupIntegrationTest(t, false)
	_ = coldBackend // unused in this test
	defer cleanup()

	ctx := context.Background()

	// Get effective policy for database without custom policy
	effective := m.policies.GetEffective(ctx, "default_db")
	if effective.Source != "global" {
		t.Errorf("Expected source 'global', got '%s'", effective.Source)
	}
	if effective.HotMaxAgeDays != 7 {
		t.Errorf("Expected HotMaxAgeDays 7, got %d", effective.HotMaxAgeDays)
	}

	// Set custom policy
	customDays := 3
	policy := &DatabasePolicy{
		Database:      "custom_db",
		HotMaxAgeDays: &customDays,
	}
	if err := m.policies.Set(ctx, policy); err != nil {
		t.Fatalf("Set policy failed: %v", err)
	}

	// Verify effective policy uses custom value
	effective = m.policies.GetEffective(ctx, "custom_db")
	if effective.Source != "custom" {
		t.Errorf("Expected source 'custom', got '%s'", effective.Source)
	}
	if effective.HotMaxAgeDays != 3 {
		t.Errorf("Expected HotMaxAgeDays 3 (custom), got %d", effective.HotMaxAgeDays)
	}
}

func TestIntegration_TierStats(t *testing.T) {
	m, hotBackend, _, cleanup := setupIntegrationTest(t, false)
	defer cleanup()

	ctx := context.Background()

	// Record multiple files
	for i := 0; i < 5; i++ {
		path := "testdb/cpu/2025/01/01/data_" + string(rune('a'+i)) + ".parquet"
		data := make([]byte, 1000*(i+1)) // Varying sizes

		if err := hotBackend.Write(ctx, path, data); err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}

		file := &FileMetadata{
			Path:          path,
			Database:      "testdb",
			Measurement:   "cpu",
			PartitionTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			SizeBytes:     int64(len(data)),
		}
		if err := m.RecordNewFile(ctx, file); err != nil {
			t.Fatalf("RecordNewFile failed: %v", err)
		}
	}

	// Get tier stats
	stats, err := m.metadata.GetTierStats(ctx)
	if err != nil {
		t.Fatalf("GetTierStats failed: %v", err)
	}

	hotStats := stats[TierHot]
	if hotStats.FileCount != 5 {
		t.Errorf("Expected 5 files in hot tier, got %d", hotStats.FileCount)
	}

	// TotalSizeMB is bytes / (1024*1024), so small files will show 0 MB
	// Just verify the field is accessible and non-negative
	if hotStats.TotalSizeMB < 0 {
		t.Errorf("TotalSizeMB should be non-negative, got %d", hotStats.TotalSizeMB)
	}
}

func TestIntegration_FileDeletion(t *testing.T) {
	m, hotBackend, _, cleanup := setupIntegrationTest(t, false)
	defer cleanup()

	ctx := context.Background()

	testPath := "testdb/cpu/2025/01/15/data.parquet"
	testData := []byte("test data")

	if err := hotBackend.Write(ctx, testPath, testData); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	file := &FileMetadata{
		Path:          testPath,
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionTime: time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC),
		SizeBytes:     int64(len(testData)),
	}

	if err := m.RecordNewFile(ctx, file); err != nil {
		t.Fatalf("RecordNewFile failed: %v", err)
	}

	// Verify file exists
	retrieved, err := m.metadata.GetFile(ctx, testPath)
	if err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}
	if retrieved == nil {
		t.Fatal("File should exist")
	}

	// Delete file
	if err := m.DeleteFile(ctx, testPath); err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}

	// Verify file is gone from metadata
	retrieved, err = m.metadata.GetFile(ctx, testPath)
	if err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}
	if retrieved != nil {
		t.Error("File should not exist after deletion")
	}
}

func TestIntegration_MigrateBatch(t *testing.T) {
	m, hotBackend, coldBackend, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()

	// Create test files
	oldTime := time.Now().UTC().AddDate(0, 0, -30)
	var files []MigrationCandidate

	for i := 0; i < 3; i++ {
		path := "testdb/cpu/2025/01/01/data_" + string(rune('a'+i)) + ".parquet"
		data := []byte("test data for file " + string(rune('a'+i)))

		if err := hotBackend.Write(ctx, path, data); err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}

		file := &FileMetadata{
			Path:          path,
			Database:      "testdb",
			Measurement:   "cpu",
			PartitionTime: oldTime,
			SizeBytes:     int64(len(data)),
		}

		if err := m.RecordNewFile(ctx, file); err != nil {
			t.Fatalf("RecordNewFile failed: %v", err)
		}

		files = append(files, MigrationCandidate{
			Path:          path,
			Database:      "testdb",
			Measurement:   "cpu",
			PartitionTime: oldTime,
			SizeBytes:     int64(len(data)),
			CurrentTier:   TierHot,
			TargetTier:    TierCold,
		})
	}

	// Migrate batch
	migrated, errors := m.migrator.MigrateBatch(ctx, files)

	if errors > 0 {
		t.Errorf("Expected 0 errors, got %d", errors)
	}
	if migrated != 3 {
		t.Errorf("Expected 3 migrated, got %d", migrated)
	}

	// Verify files are now in cold backend
	for _, f := range files {
		exists, err := coldBackend.Exists(ctx, f.Path)
		if err != nil {
			t.Fatalf("Exists check failed: %v", err)
		}
		if !exists {
			t.Errorf("File %s should exist in cold backend", f.Path)
		}

		// File should be removed from hot backend
		exists, _ = hotBackend.Exists(ctx, f.Path)
		if exists {
			t.Errorf("File %s should be removed from hot backend", f.Path)
		}

		// Metadata should show cold tier
		retrieved, err := m.metadata.GetFile(ctx, f.Path)
		if err != nil {
			t.Fatalf("GetFile failed: %v", err)
		}
		if retrieved.Tier != TierCold {
			t.Errorf("File %s tier should be cold, got %s", f.Path, retrieved.Tier)
		}
	}
}

func TestIntegration_OnlyDailyCompactedFilesMigrate(t *testing.T) {
	m, hotBackend, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	oldTime := time.Now().UTC().AddDate(0, 0, -30)

	// Create three file types: raw, hourly-compacted, daily-compacted
	files := []struct {
		path     string
		eligible bool
	}{
		{"testdb/cpu/2025/01/01/10/cpu_20250101_100000_123456789.parquet", false}, // raw
		{"testdb/cpu/2025/01/01/10/cpu_20250101_100000_compacted.parquet", false}, // hourly
		{"testdb/cpu/2025/01/01/10/cpu_20250101_100000_daily.parquet", true},      // daily
	}

	for _, f := range files {
		data := []byte("test data")
		if err := hotBackend.Write(ctx, f.path, data); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
		if err := m.RecordNewFile(ctx, &FileMetadata{
			Path:          f.path,
			Database:      "testdb",
			Measurement:   "cpu",
			PartitionTime: oldTime,
			SizeBytes:     int64(len(data)),
		}); err != nil {
			t.Fatalf("RecordNewFile failed: %v", err)
		}
	}

	candidates, err := m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if err != nil {
		t.Fatalf("FindCandidates failed: %v", err)
	}

	if len(candidates) != 1 {
		t.Fatalf("Expected 1 candidate (daily only), got %d", len(candidates))
	}
	if candidates[0].Path != files[2].path {
		t.Errorf("Expected daily file, got %s", candidates[0].Path)
	}
}
