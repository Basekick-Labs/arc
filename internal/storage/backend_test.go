package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestLocalBackend_BasicOperations tests the LocalBackend implementation
func TestLocalBackend_BasicOperations(t *testing.T) {
	// Create temporary directory for tests
	tmpDir, err := os.MkdirTemp("", "arc-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatalf("failed to create LocalBackend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	t.Run("Write and Read", func(t *testing.T) {
		testPath := "test/data.txt"
		testData := []byte("hello world")

		// Write
		if err := backend.Write(ctx, testPath, testData); err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// Read
		data, err := backend.Read(ctx, testPath)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}

		if string(data) != string(testData) {
			t.Errorf("Read data = %q, want %q", string(data), string(testData))
		}
	})

	t.Run("Exists", func(t *testing.T) {
		testPath := "test/exists.txt"

		// Should not exist initially
		exists, err := backend.Exists(ctx, testPath)
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("Expected file to not exist")
		}

		// Create file
		if err := backend.Write(ctx, testPath, []byte("data")); err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// Should exist now
		exists, err = backend.Exists(ctx, testPath)
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("Expected file to exist")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		testPath := "test/delete.txt"

		// Create file
		if err := backend.Write(ctx, testPath, []byte("data")); err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// Delete
		if err := backend.Delete(ctx, testPath); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Should not exist
		exists, err := backend.Exists(ctx, testPath)
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("Expected file to be deleted")
		}
	})

	t.Run("List", func(t *testing.T) {
		// Create some files
		files := []string{
			"list/file1.parquet",
			"list/file2.parquet",
			"list/subdir/file3.parquet",
		}
		for _, f := range files {
			if err := backend.Write(ctx, f, []byte("data")); err != nil {
				t.Fatalf("Write failed: %v", err)
			}
		}

		// List directory
		listed, err := backend.List(ctx, "list/")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(listed) < 2 {
			t.Errorf("Expected at least 2 files, got %d", len(listed))
		}
	})
}

// TestLocalBackend_DirectoryLister tests the DirectoryLister interface
func TestLocalBackend_DirectoryLister(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatalf("failed to create LocalBackend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Create directory structure
	dirs := []string{
		"databases/db1/measurement1",
		"databases/db1/measurement2",
		"databases/db2/measurement1",
	}
	for _, dir := range dirs {
		path := filepath.Join(tmpDir, dir, "data.parquet")
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatalf("failed to create dir: %v", err)
		}
		if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
			t.Fatalf("failed to write file: %v", err)
		}
	}

	t.Run("ListDirectories", func(t *testing.T) {
		// List databases
		databases, err := backend.ListDirectories(ctx, "databases/")
		if err != nil {
			t.Fatalf("ListDirectories failed: %v", err)
		}

		if len(databases) != 2 {
			t.Errorf("Expected 2 databases, got %d: %v", len(databases), databases)
		}

		// List measurements in db1
		measurements, err := backend.ListDirectories(ctx, "databases/db1/")
		if err != nil {
			t.Fatalf("ListDirectories failed: %v", err)
		}

		if len(measurements) != 2 {
			t.Errorf("Expected 2 measurements, got %d: %v", len(measurements), measurements)
		}
	})
}

// TestLocalBackend_ObjectLister tests the ObjectLister interface
func TestLocalBackend_ObjectLister(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatalf("failed to create LocalBackend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Create test files
	files := []struct {
		path string
		data []byte
	}{
		{"objects/file1.parquet", []byte("small data")},
		{"objects/file2.parquet", []byte("larger data content here")},
		{"objects/subdir/file3.parquet", []byte("nested file")},
	}
	for _, f := range files {
		if err := backend.Write(ctx, f.path, f.data); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	t.Run("ListObjects", func(t *testing.T) {
		objects, err := backend.ListObjects(ctx, "objects/")
		if err != nil {
			t.Fatalf("ListObjects failed: %v", err)
		}

		if len(objects) != 3 {
			t.Errorf("Expected 3 objects, got %d", len(objects))
		}

		// Verify object info
		for _, obj := range objects {
			if obj.Size <= 0 {
				t.Errorf("Expected positive size for %s, got %d", obj.Path, obj.Size)
			}
			if obj.LastModified.IsZero() {
				t.Errorf("Expected non-zero LastModified for %s", obj.Path)
			}
		}
	})
}

// TestLocalBackend_BatchDeleter tests the BatchDeleter interface
func TestLocalBackend_BatchDeleter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatalf("failed to create LocalBackend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Create test files
	files := []string{
		"batch/file1.txt",
		"batch/file2.txt",
		"batch/file3.txt",
	}
	for _, f := range files {
		if err := backend.Write(ctx, f, []byte("data")); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	t.Run("DeleteBatch", func(t *testing.T) {
		// Delete first two files
		if err := backend.DeleteBatch(ctx, files[:2]); err != nil {
			t.Fatalf("DeleteBatch failed: %v", err)
		}

		// Verify deleted
		for _, f := range files[:2] {
			exists, _ := backend.Exists(ctx, f)
			if exists {
				t.Errorf("Expected %s to be deleted", f)
			}
		}

		// Verify third file still exists
		exists, _ := backend.Exists(ctx, files[2])
		if !exists {
			t.Errorf("Expected %s to still exist", files[2])
		}
	})

	t.Run("DeleteBatch_Empty", func(t *testing.T) {
		// Empty batch should not error
		if err := backend.DeleteBatch(ctx, []string{}); err != nil {
			t.Errorf("DeleteBatch with empty list should not error: %v", err)
		}
	})
}

// TestObjectInfo tests the ObjectInfo struct
func TestObjectInfo(t *testing.T) {
	info := ObjectInfo{
		Path:         "test/path.parquet",
		Size:         1024,
		LastModified: time.Now(),
	}

	if info.Path != "test/path.parquet" {
		t.Errorf("Expected path test/path.parquet, got %s", info.Path)
	}
	if info.Size != 1024 {
		t.Errorf("Expected size 1024, got %d", info.Size)
	}
	if info.LastModified.IsZero() {
		t.Error("Expected non-zero LastModified")
	}
}

// BenchmarkLocalBackend_Write benchmarks write performance
func BenchmarkLocalBackend_Write(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "arc-storage-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := NewLocalBackend(tmpDir, logger)
	if err != nil {
		b.Fatalf("failed to create LocalBackend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()
	data := make([]byte, 1024) // 1KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := filepath.Join("bench", "file"+string(rune(i%1000))+".txt")
		_ = backend.Write(ctx, path, data)
	}
}

// BenchmarkLocalBackend_Read benchmarks read performance
func BenchmarkLocalBackend_Read(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "arc-storage-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := NewLocalBackend(tmpDir, logger)
	if err != nil {
		b.Fatalf("failed to create LocalBackend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()
	testPath := "bench/read.txt"
	data := make([]byte, 1024) // 1KB
	_ = backend.Write(ctx, testPath, data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = backend.Read(ctx, testPath)
	}
}
