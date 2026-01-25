package compaction

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// setupTestManager creates a test compaction manager with a temporary storage backend
func setupTestManager(t *testing.T) (*Manager, storage.Backend, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "arc-compaction-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create storage backend: %v", err)
	}

	lockManager := NewLockManager()

	cfg := &ManagerConfig{
		StorageBackend: backend,
		LockManager:    lockManager,
		MinAgeHours:    1,
		MinFiles:       5,
		TargetSizeMB:   256,
		MaxConcurrent:  2,
		TempDirectory:  tmpDir + "/temp",
		Logger:         logger,
	}

	manager := NewManager(cfg)

	cleanup := func() {
		backend.Close()
		os.RemoveAll(tmpDir)
	}

	return manager, backend, cleanup
}

// TestNewManager tests manager creation
func TestNewManager(t *testing.T) {
	t.Run("with defaults", func(t *testing.T) {
		tmpDir, _ := os.MkdirTemp("", "arc-test-*")
		defer os.RemoveAll(tmpDir)

		logger := zerolog.Nop()
		backend, _ := storage.NewLocalBackend(tmpDir, logger)
		defer backend.Close()

		cfg := &ManagerConfig{
			StorageBackend: backend,
			LockManager:    NewLockManager(),
			Logger:         logger,
		}

		manager := NewManager(cfg)

		// Check defaults
		if manager.MinAgeHours != 1 {
			t.Errorf("MinAgeHours = %d, want 1", manager.MinAgeHours)
		}
		if manager.MinFiles != 10 {
			t.Errorf("MinFiles = %d, want 10", manager.MinFiles)
		}
		if manager.TargetSizeMB != 512 {
			t.Errorf("TargetSizeMB = %d, want 512", manager.TargetSizeMB)
		}
		if manager.MaxConcurrent != 2 {
			t.Errorf("MaxConcurrent = %d, want 2", manager.MaxConcurrent)
		}
	})

	t.Run("with custom config", func(t *testing.T) {
		tmpDir, _ := os.MkdirTemp("", "arc-test-*")
		defer os.RemoveAll(tmpDir)

		logger := zerolog.Nop()
		backend, _ := storage.NewLocalBackend(tmpDir, logger)
		defer backend.Close()

		cfg := &ManagerConfig{
			StorageBackend: backend,
			LockManager:    NewLockManager(),
			MinAgeHours:    24,
			MinFiles:       20,
			TargetSizeMB:   1024,
			MaxConcurrent:  4,
			TempDirectory:  "/custom/temp",
			Logger:         logger,
		}

		manager := NewManager(cfg)

		if manager.MinAgeHours != 24 {
			t.Errorf("MinAgeHours = %d, want 24", manager.MinAgeHours)
		}
		if manager.MinFiles != 20 {
			t.Errorf("MinFiles = %d, want 20", manager.MinFiles)
		}
		if manager.TargetSizeMB != 1024 {
			t.Errorf("TargetSizeMB = %d, want 1024", manager.TargetSizeMB)
		}
		if manager.MaxConcurrent != 4 {
			t.Errorf("MaxConcurrent = %d, want 4", manager.MaxConcurrent)
		}
		if manager.TempDirectory != "/custom/temp" {
			t.Errorf("TempDirectory = %s, want /custom/temp", manager.TempDirectory)
		}
	})
}

// TestManager_Stats tests statistics reporting
func TestManager_Stats(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	stats := manager.Stats()

	// Check expected fields
	if _, ok := stats["total_jobs_completed"]; !ok {
		t.Error("stats should contain total_jobs_completed")
	}
	if _, ok := stats["total_jobs_failed"]; !ok {
		t.Error("stats should contain total_jobs_failed")
	}
	if _, ok := stats["total_files_compacted"]; !ok {
		t.Error("stats should contain total_files_compacted")
	}
	if _, ok := stats["total_bytes_saved"]; !ok {
		t.Error("stats should contain total_bytes_saved")
	}
	if _, ok := stats["cycle_running"]; !ok {
		t.Error("stats should contain cycle_running")
	}
	if _, ok := stats["current_cycle_id"]; !ok {
		t.Error("stats should contain current_cycle_id")
	}
}

// TestManager_CycleRunning tests cycle running status
func TestManager_CycleRunning(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Initially not running
	if manager.IsCycleRunning() {
		t.Error("IsCycleRunning should be false initially")
	}

	// Initial cycle ID should be 0
	if manager.GetCurrentCycleID() != 0 {
		t.Errorf("GetCurrentCycleID = %d, want 0", manager.GetCurrentCycleID())
	}
}

// TestManager_FindCandidates_Empty tests finding candidates in empty storage
func TestManager_FindCandidates_Empty(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	candidates, err := manager.FindCandidates(ctx)
	if err != nil {
		t.Fatalf("FindCandidates failed: %v", err)
	}

	if len(candidates) != 0 {
		t.Errorf("Expected 0 candidates in empty storage, got %d", len(candidates))
	}
}

// TestManager_listDatabases tests database discovery
func TestManager_listDatabases(t *testing.T) {
	manager, backend, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create some test files to simulate databases
	backend.Write(ctx, "db1/cpu/2025/01/01/00/file1.parquet", []byte("data"))
	backend.Write(ctx, "db1/mem/2025/01/01/00/file1.parquet", []byte("data"))
	backend.Write(ctx, "db2/disk/2025/01/01/00/file1.parquet", []byte("data"))
	backend.Write(ctx, ".hidden/table/2025/01/01/00/file1.parquet", []byte("data"))

	databases, err := manager.listDatabases(ctx)
	if err != nil {
		t.Fatalf("listDatabases failed: %v", err)
	}

	// Should find db1 and db2, not .hidden
	if len(databases) != 2 {
		t.Errorf("Expected 2 databases, got %d: %v", len(databases), databases)
	}

	hasDB1 := false
	hasDB2 := false
	for _, db := range databases {
		if db == "db1" {
			hasDB1 = true
		}
		if db == "db2" {
			hasDB2 = true
		}
	}

	if !hasDB1 {
		t.Error("Expected to find db1")
	}
	if !hasDB2 {
		t.Error("Expected to find db2")
	}
}

// TestManager_listMeasurements tests measurement discovery
func TestManager_listMeasurements(t *testing.T) {
	manager, backend, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create test files
	backend.Write(ctx, "mydb/cpu/2025/01/01/00/file1.parquet", []byte("data"))
	backend.Write(ctx, "mydb/memory/2025/01/01/00/file1.parquet", []byte("data"))
	backend.Write(ctx, "mydb/disk/2025/01/01/00/file1.parquet", []byte("data"))

	measurements, err := manager.listMeasurements(ctx, "mydb")
	if err != nil {
		t.Fatalf("listMeasurements failed: %v", err)
	}

	if len(measurements) != 3 {
		t.Errorf("Expected 3 measurements, got %d: %v", len(measurements), measurements)
	}

	expected := map[string]bool{"cpu": false, "memory": false, "disk": false}
	for _, m := range measurements {
		if _, ok := expected[m]; ok {
			expected[m] = true
		}
	}

	for name, found := range expected {
		if !found {
			t.Errorf("Expected to find measurement %s", name)
		}
	}
}

// TestManager_ConcurrentCycles tests that concurrent cycles are prevented
func TestManager_ConcurrentCycles(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Simulate a cycle is running by setting the flag
	manager.cycleRunning.Store(true)

	ctx := context.Background()
	_, err := manager.RunCompactionCycle(ctx)

	if err != ErrCycleAlreadyRunning {
		t.Errorf("Expected ErrCycleAlreadyRunning, got %v", err)
	}

	// Reset
	manager.cycleRunning.Store(false)
}

// TestManager_RunCompactionCycle_NoCandidates tests running a cycle with no candidates
func TestManager_RunCompactionCycle_NoCandidates(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	cycleID, err := manager.RunCompactionCycle(ctx)
	if err != nil {
		t.Fatalf("RunCompactionCycle failed: %v", err)
	}

	if cycleID != 1 {
		t.Errorf("First cycle ID should be 1, got %d", cycleID)
	}

	// Verify cycle ID incremented
	if manager.GetCurrentCycleID() != 1 {
		t.Errorf("GetCurrentCycleID = %d, want 1", manager.GetCurrentCycleID())
	}
}

// TestManager_ContextCancellation tests that cycles respect context cancellation
func TestManager_ContextCancellation(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := manager.RunCompactionCycle(ctx)

	// Should return context error or complete quickly (no candidates)
	// In this case with no candidates, it should complete before checking context
	if err != nil && err != context.Canceled {
		t.Errorf("Unexpected error: %v", err)
	}
}

// TestLockManager tests the lock manager
func TestLockManager(t *testing.T) {
	lm := NewLockManager()

	t.Run("acquire and release", func(t *testing.T) {
		key := "test/partition"

		// Should acquire successfully
		if !lm.AcquireLock(key) {
			t.Error("First AcquireLock should succeed")
		}

		// Should be locked
		if !lm.IsLocked(key) {
			t.Error("Key should be locked")
		}

		// Second acquire should fail
		if lm.AcquireLock(key) {
			t.Error("Second AcquireLock should fail")
		}

		// Release
		lm.ReleaseLock(key)

		// Should no longer be locked
		if lm.IsLocked(key) {
			t.Error("Key should not be locked after release")
		}

		// Should be able to acquire again
		if !lm.AcquireLock(key) {
			t.Error("Should be able to acquire after release")
		}
		lm.ReleaseLock(key)
	})

	t.Run("multiple keys", func(t *testing.T) {
		key1 := "db1/partition1"
		key2 := "db1/partition2"

		lm.AcquireLock(key1)

		// Should be able to acquire different key
		if !lm.AcquireLock(key2) {
			t.Error("Should be able to acquire different key")
		}

		if !lm.IsLocked(key1) {
			t.Error("key1 should still be locked")
		}
		if !lm.IsLocked(key2) {
			t.Error("key2 should be locked")
		}

		lm.ReleaseLock(key1)
		lm.ReleaseLock(key2)
	})

	t.Run("concurrent access", func(t *testing.T) {
		key := "concurrent/test"
		successCount := 0
		var mu sync.Mutex
		var wg sync.WaitGroup

		// Try to acquire same lock from multiple goroutines
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if lm.AcquireLock(key) {
					mu.Lock()
					successCount++
					mu.Unlock()
					time.Sleep(10 * time.Millisecond)
					lm.ReleaseLock(key)
				}
			}()
		}

		wg.Wait()

		// Only one should have succeeded at any given time
		// but multiple might have acquired over time after releases
		if successCount == 0 {
			t.Error("At least one goroutine should have acquired the lock")
		}
	})
}

// TestManager_JobHistory tests job history tracking
func TestManager_JobHistory(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Initially empty
	stats := manager.Stats()
	recentJobs, ok := stats["recent_jobs"].([]map[string]interface{})
	if !ok {
		t.Fatal("recent_jobs should be a slice of maps")
	}
	if len(recentJobs) != 0 {
		t.Errorf("Expected 0 recent jobs initially, got %d", len(recentJobs))
	}
}

// TestCandidate tests the Candidate struct
func TestCandidate(t *testing.T) {
	candidate := Candidate{
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionPath: "testdb/cpu/2025/01/01/00",
		Files:         []string{"file1.parquet", "file2.parquet"},
		Tier:          "hourly",
	}

	if candidate.Database != "testdb" {
		t.Errorf("Database = %s, want testdb", candidate.Database)
	}
	if candidate.Measurement != "cpu" {
		t.Errorf("Measurement = %s, want cpu", candidate.Measurement)
	}
	if len(candidate.Files) != 2 {
		t.Errorf("Files count = %d, want 2", len(candidate.Files))
	}
	if candidate.Tier != "hourly" {
		t.Errorf("Tier = %s, want hourly", candidate.Tier)
	}
}

// TestManager_CleanupOrphanedTempDirs tests orphaned temp directory cleanup
func TestManager_CleanupOrphanedTempDirs(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create orphaned temp directories (simulating crash leftovers)
	orphan1 := manager.TempDirectory + "/orphan_job_123"
	orphan2 := manager.TempDirectory + "/orphan_job_456"
	if err := os.MkdirAll(orphan1, 0755); err != nil {
		t.Fatalf("failed to create orphan1: %v", err)
	}
	if err := os.MkdirAll(orphan2, 0755); err != nil {
		t.Fatalf("failed to create orphan2: %v", err)
	}

	// Create a file inside one of them (simulating downloaded parquet)
	testFile := orphan1 + "/test.parquet"
	if err := os.WriteFile(testFile, []byte("test data"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Verify orphans exist
	entries, err := os.ReadDir(manager.TempDirectory)
	if err != nil {
		t.Fatalf("failed to read temp directory: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 orphan directories, got %d", len(entries))
	}

	// Run cleanup
	if err := manager.CleanupOrphanedTempDirs(); err != nil {
		t.Fatalf("CleanupOrphanedTempDirs failed: %v", err)
	}

	// Verify orphans are removed
	entries, err = os.ReadDir(manager.TempDirectory)
	if err != nil {
		t.Fatalf("failed to read temp directory after cleanup: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 directories after cleanup, got %d", len(entries))
	}
}

// TestManager_CleanupOrphanedTempDirs_NonExistentDir tests cleanup when temp dir doesn't exist
func TestManager_CleanupOrphanedTempDirs_NonExistentDir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-compaction-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatalf("failed to create storage backend: %v", err)
	}
	defer backend.Close()

	// Use a non-existent temp directory
	manager := NewManager(&ManagerConfig{
		StorageBackend: backend,
		LockManager:    NewLockManager(),
		TempDirectory:  tmpDir + "/nonexistent/path",
		Logger:         logger,
	})

	// Should not error when directory doesn't exist
	if err := manager.CleanupOrphanedTempDirs(); err != nil {
		t.Errorf("CleanupOrphanedTempDirs should not error on non-existent dir, got: %v", err)
	}
}
