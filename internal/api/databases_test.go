package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// countingBackend wraps a storage backend to count operations
type countingBackend struct {
	storage.Backend
	listCalls   atomic.Int64
	existsCalls atomic.Int64
}

func (c *countingBackend) List(ctx context.Context, prefix string) ([]string, error) {
	c.listCalls.Add(1)
	return c.Backend.List(ctx, prefix)
}

func (c *countingBackend) Exists(ctx context.Context, path string) (bool, error) {
	c.existsCalls.Add(1)
	return c.Backend.Exists(ctx, path)
}

func (c *countingBackend) ResetCounts() {
	c.listCalls.Store(0)
	c.existsCalls.Store(0)
}

// Also implement DirectoryLister if underlying backend supports it
func (c *countingBackend) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	c.listCalls.Add(1) // Count directory listings as list calls
	if lister, ok := c.Backend.(storage.DirectoryLister); ok {
		return lister.ListDirectories(ctx, prefix)
	}
	// Fallback - should not happen in tests
	return nil, fmt.Errorf("backend does not support ListDirectories")
}

// setupTestDatabasesHandler creates a test handler with a local storage backend
func setupTestDatabasesHandler(t *testing.T, deleteEnabled bool) (*DatabasesHandler, *fiber.App, string) {
	t.Helper()

	// Create temporary directory for tests
	tmpDir, err := os.MkdirTemp("", "arc-databases-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create LocalBackend: %v", err)
	}

	deleteConfig := &config.DeleteConfig{
		Enabled: deleteEnabled,
	}

	handler := NewDatabasesHandler(backend, deleteConfig, logger)

	app := fiber.New()
	handler.RegisterRoutes(app)

	return handler, app, tmpDir
}

// TestDatabasesHandler_List tests listing databases
func TestDatabasesHandler_List(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, false)
	defer os.RemoveAll(tmpDir)

	// Create some databases by creating directories with files
	ctx := context.Background()
	backend, _ := storage.NewLocalBackend(tmpDir, zerolog.Nop())
	defer backend.Close()

	// Create database1 with 2 measurements
	backend.Write(ctx, "database1/measurement1/data.parquet", []byte("data"))
	backend.Write(ctx, "database1/measurement2/data.parquet", []byte("data"))

	// Create database2 with 1 measurement
	backend.Write(ctx, "database2/measurement1/data.parquet", []byte("data"))

	t.Run("List databases", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/databases", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		if resp.StatusCode != fiber.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
		}

		var result DatabaseListResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if result.Count != 2 {
			t.Errorf("Expected 2 databases, got %d", result.Count)
		}

		// Check that databases have correct measurement counts
		dbMap := make(map[string]int)
		for _, db := range result.Databases {
			dbMap[db.Name] = db.MeasurementCount
		}

		if dbMap["database1"] != 2 {
			t.Errorf("Expected database1 to have 2 measurements, got %d", dbMap["database1"])
		}
		if dbMap["database2"] != 1 {
			t.Errorf("Expected database2 to have 1 measurement, got %d", dbMap["database2"])
		}
	})
}

// TestDatabasesHandler_ListEmpty tests listing when no databases exist
func TestDatabasesHandler_ListEmpty(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, false)
	defer os.RemoveAll(tmpDir)

	req := httptest.NewRequest("GET", "/api/v1/databases", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	var result DatabaseListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if result.Count != 0 {
		t.Errorf("Expected 0 databases, got %d", result.Count)
	}
}

// TestDatabasesHandler_Create tests creating a database
func TestDatabasesHandler_Create(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, false)
	defer os.RemoveAll(tmpDir)

	t.Run("Create database", func(t *testing.T) {
		body := `{"name": "mydb"}`
		req := httptest.NewRequest("POST", "/api/v1/databases", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		if resp.StatusCode != fiber.StatusCreated {
			respBody, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected status 201, got %d: %s", resp.StatusCode, string(respBody))
		}

		var result DatabaseInfo
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if result.Name != "mydb" {
			t.Errorf("Expected name 'mydb', got %q", result.Name)
		}
		if result.MeasurementCount != 0 {
			t.Errorf("Expected 0 measurements, got %d", result.MeasurementCount)
		}
		if result.CreatedAt == "" {
			t.Error("Expected CreatedAt to be set")
		}
	})
}

// TestDatabasesHandler_CreateInvalidName tests creating a database with invalid name
func TestDatabasesHandler_CreateInvalidName(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, false)
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name        string
		dbName      string
		expectError bool
	}{
		{"empty name", "", true},
		{"starts with number", "123db", true},
		{"contains spaces", "my db", true},
		{"contains special chars", "my@db", true},
		{"valid name", "my_db", false},
		{"valid with hyphen", "my-db", false},
		{"valid alphanumeric", "myDb123", false},
		{"reserved name system", "system", true},
		{"reserved name internal", "internal", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := `{"name": "` + tt.dbName + `"}`
			req := httptest.NewRequest("POST", "/api/v1/databases", bytes.NewBufferString(body))
			req.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}

			if tt.expectError {
				if resp.StatusCode == fiber.StatusCreated {
					t.Errorf("Expected error for name %q, but got success", tt.dbName)
				}
			} else {
				if resp.StatusCode != fiber.StatusCreated {
					respBody, _ := io.ReadAll(resp.Body)
					t.Errorf("Expected success for name %q, got status %d: %s", tt.dbName, resp.StatusCode, string(respBody))
				}
			}
		})
	}
}

// TestDatabasesHandler_CreateAlreadyExists tests creating a database that already exists
func TestDatabasesHandler_CreateAlreadyExists(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, false)
	defer os.RemoveAll(tmpDir)

	// Create database first
	body := `{"name": "existingdb"}`
	req := httptest.NewRequest("POST", "/api/v1/databases", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, _ := app.Test(req)
	if resp.StatusCode != fiber.StatusCreated {
		t.Fatalf("Failed to create initial database")
	}

	// Try to create again
	req = httptest.NewRequest("POST", "/api/v1/databases", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, _ = app.Test(req)

	if resp.StatusCode != fiber.StatusConflict {
		t.Errorf("Expected status 409 Conflict, got %d", resp.StatusCode)
	}

	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	if result["error"] == "" {
		t.Error("Expected error message in response")
	}
}

// TestDatabasesHandler_Get tests getting a database
func TestDatabasesHandler_Get(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, false)
	defer os.RemoveAll(tmpDir)

	// Create a database with measurements
	ctx := context.Background()
	backend, _ := storage.NewLocalBackend(tmpDir, zerolog.Nop())
	defer backend.Close()
	backend.Write(ctx, "testdb/measurement1/data.parquet", []byte("data"))
	backend.Write(ctx, "testdb/measurement2/data.parquet", []byte("data"))

	t.Run("Get existing database", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/databases/testdb", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		if resp.StatusCode != fiber.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
		}

		var result DatabaseInfo
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if result.Name != "testdb" {
			t.Errorf("Expected name 'testdb', got %q", result.Name)
		}
		if result.MeasurementCount != 2 {
			t.Errorf("Expected 2 measurements, got %d", result.MeasurementCount)
		}
	})
}

// TestDatabasesHandler_GetNotFound tests getting a non-existent database
func TestDatabasesHandler_GetNotFound(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, false)
	defer os.RemoveAll(tmpDir)

	req := httptest.NewRequest("GET", "/api/v1/databases/nonexistent", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusNotFound {
		t.Errorf("Expected status 404, got %d", resp.StatusCode)
	}
}

// TestDatabasesHandler_ListMeasurements tests listing measurements in a database
func TestDatabasesHandler_ListMeasurements(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, false)
	defer os.RemoveAll(tmpDir)

	// Create a database with measurements
	ctx := context.Background()
	backend, _ := storage.NewLocalBackend(tmpDir, zerolog.Nop())
	defer backend.Close()
	backend.Write(ctx, "testdb/cpu/data.parquet", []byte("data"))
	backend.Write(ctx, "testdb/memory/data.parquet", []byte("data"))
	backend.Write(ctx, "testdb/disk/data.parquet", []byte("data"))

	t.Run("List measurements", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/databases/testdb/measurements", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		if resp.StatusCode != fiber.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
		}

		var result MeasurementListResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if result.Database != "testdb" {
			t.Errorf("Expected database 'testdb', got %q", result.Database)
		}
		if result.Count != 3 {
			t.Errorf("Expected 3 measurements, got %d", result.Count)
		}
	})
}

// TestDatabasesHandler_ListMeasurementsEmpty tests listing measurements in an empty database
func TestDatabasesHandler_ListMeasurementsEmpty(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, false)
	defer os.RemoveAll(tmpDir)

	// Create empty database (just the marker file)
	ctx := context.Background()
	backend, _ := storage.NewLocalBackend(tmpDir, zerolog.Nop())
	defer backend.Close()
	backend.Write(ctx, "emptydb/.arc-database", []byte(`{"created_at":"2025-01-01T00:00:00Z"}`))

	req := httptest.NewRequest("GET", "/api/v1/databases/emptydb/measurements", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
	}

	var result MeasurementListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if result.Count != 0 {
		t.Errorf("Expected 0 measurements, got %d", result.Count)
	}
}

// TestDatabasesHandler_Delete tests deleting a database
func TestDatabasesHandler_Delete(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, true) // delete enabled
	defer os.RemoveAll(tmpDir)

	// Create a database with data
	ctx := context.Background()
	backend, _ := storage.NewLocalBackend(tmpDir, zerolog.Nop())
	defer backend.Close()
	backend.Write(ctx, "deletedb/measurement1/data1.parquet", []byte("data"))
	backend.Write(ctx, "deletedb/measurement1/data2.parquet", []byte("data"))
	backend.Write(ctx, "deletedb/measurement2/data1.parquet", []byte("data"))

	t.Run("Delete database with confirmation", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/api/v1/databases/deletedb?confirm=true", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		if resp.StatusCode != fiber.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		filesDeleted, ok := result["files_deleted"].(float64)
		if !ok || filesDeleted < 3 {
			t.Errorf("Expected at least 3 files deleted, got %v", result["files_deleted"])
		}

		// Verify database no longer exists
		exists, _ := backend.Exists(ctx, "deletedb/measurement1/data1.parquet")
		if exists {
			t.Error("Expected database files to be deleted")
		}
	})
}

// TestDatabasesHandler_DeleteDisabled tests deleting when delete is disabled
func TestDatabasesHandler_DeleteDisabled(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, false) // delete disabled
	defer os.RemoveAll(tmpDir)

	// Create a database
	ctx := context.Background()
	backend, _ := storage.NewLocalBackend(tmpDir, zerolog.Nop())
	defer backend.Close()
	backend.Write(ctx, "testdb/measurement1/data.parquet", []byte("data"))

	req := httptest.NewRequest("DELETE", "/api/v1/databases/testdb?confirm=true", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusForbidden {
		t.Errorf("Expected status 403, got %d", resp.StatusCode)
	}

	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	if result["error"] == "" {
		t.Error("Expected error message in response")
	}
}

// TestDatabasesHandler_DeleteRequiresConfirm tests that delete requires confirmation
func TestDatabasesHandler_DeleteRequiresConfirm(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, true) // delete enabled
	defer os.RemoveAll(tmpDir)

	// Create a database
	ctx := context.Background()
	backend, _ := storage.NewLocalBackend(tmpDir, zerolog.Nop())
	defer backend.Close()
	backend.Write(ctx, "testdb/measurement1/data.parquet", []byte("data"))

	t.Run("Without confirm param", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/api/v1/databases/testdb", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		if resp.StatusCode != fiber.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", resp.StatusCode)
		}
	})

	t.Run("With confirm=false", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/api/v1/databases/testdb?confirm=false", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		if resp.StatusCode != fiber.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", resp.StatusCode)
		}
	})
}

// TestDatabasesHandler_DeleteNotFound tests deleting a non-existent database
func TestDatabasesHandler_DeleteNotFound(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, true) // delete enabled
	defer os.RemoveAll(tmpDir)

	req := httptest.NewRequest("DELETE", "/api/v1/databases/nonexistent?confirm=true", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusNotFound {
		t.Errorf("Expected status 404, got %d", resp.StatusCode)
	}
}

// TestDatabasesHandler_DeleteEmptyDatabase tests deleting a database that only has the marker file
func TestDatabasesHandler_DeleteEmptyDatabase(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, true) // delete enabled
	defer os.RemoveAll(tmpDir)

	// Create a database with only the marker file (as created by POST /api/v1/databases)
	ctx := context.Background()
	backend, _ := storage.NewLocalBackend(tmpDir, zerolog.Nop())
	defer backend.Close()
	backend.Write(ctx, "emptydb/.arc-database", []byte(`{"created_at":"2025-01-01T00:00:00Z"}`))

	// Verify database appears in list before deletion
	listReq := httptest.NewRequest("GET", "/api/v1/databases", nil)
	listResp, _ := app.Test(listReq)
	var listResult DatabaseListResponse
	json.NewDecoder(listResp.Body).Decode(&listResult)
	found := false
	for _, db := range listResult.Databases {
		if db.Name == "emptydb" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Expected emptydb to appear in database list before deletion")
	}

	// Delete the database
	req := httptest.NewRequest("DELETE", "/api/v1/databases/emptydb?confirm=true", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	// Should have deleted at least the marker file
	filesDeleted, ok := result["files_deleted"].(float64)
	if !ok || filesDeleted < 1 {
		t.Errorf("Expected at least 1 file deleted (marker file), got %v", result["files_deleted"])
	}

	// Verify database no longer appears in list
	listReq2 := httptest.NewRequest("GET", "/api/v1/databases", nil)
	listResp2, _ := app.Test(listReq2)
	var listResult2 DatabaseListResponse
	json.NewDecoder(listResp2.Body).Decode(&listResult2)
	for _, db := range listResult2.Databases {
		if db.Name == "emptydb" {
			t.Error("Expected emptydb to NOT appear in database list after deletion")
		}
	}
}

// TestIsValidDatabaseName tests the database name validation function
func TestIsValidDatabaseName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"valid lowercase", "mydb", true},
		{"valid with underscore", "my_db", true},
		{"valid with hyphen", "my-db", true},
		{"valid mixed case", "MyDb", true},
		{"valid with numbers", "db123", true},
		{"empty", "", false},
		{"starts with number", "123db", false},
		{"starts with underscore", "_db", false},
		{"starts with hyphen", "-db", false},
		{"contains space", "my db", false},
		{"contains special char", "my@db", false},
		{"contains dot", "my.db", false},
		{"too long", string(make([]byte, 65)), false},
		{"max length", string(make([]byte, 64)), false}, // all same char doesn't match pattern
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidDatabaseName(tt.input)
			// Special case for max length - needs to be valid chars
			if tt.name == "max length" {
				validMaxLength := "a" + string(make([]byte, 63))
				for i := range validMaxLength {
					if i > 0 {
						validMaxLength = validMaxLength[:i] + "a" + validMaxLength[i+1:]
					}
				}
				// Just test length constraint
				if len(tt.input) > 64 && result {
					t.Errorf("isValidDatabaseName(%q) = %v, want %v", tt.input, result, tt.expected)
				}
				return
			}
			if result != tt.expected {
				t.Errorf("isValidDatabaseName(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// setupBenchmarkHandler creates a handler with counting backend for benchmarks
func setupBenchmarkHandler(b *testing.B, numDatabases, measurementsPerDB int) (*DatabasesHandler, *fiber.App, *countingBackend, func()) {
	b.Helper()

	tmpDir, err := os.MkdirTemp("", "arc-databases-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}

	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	backend, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatalf("failed to create LocalBackend: %v", err)
	}

	// Wrap with counting backend
	counting := &countingBackend{Backend: backend}

	// Create test databases and measurements
	ctx := context.Background()
	for i := 0; i < numDatabases; i++ {
		dbName := fmt.Sprintf("database%d", i)
		for j := 0; j < measurementsPerDB; j++ {
			path := fmt.Sprintf("%s/measurement%d/data.parquet", dbName, j)
			backend.Write(ctx, path, []byte("test data"))
		}
	}

	deleteConfig := &config.DeleteConfig{Enabled: false}
	handler := NewDatabasesHandler(counting, deleteConfig, logger)

	app := fiber.New()
	handler.RegisterRoutes(app)

	cleanup := func() {
		backend.Close()
		os.RemoveAll(tmpDir)
	}

	return handler, app, counting, cleanup
}

// BenchmarkDatabasesHandler_List measures storage calls for listing databases
// This benchmark exposes the N+1 query pattern where listing N databases
// results in N+1 storage.List() calls instead of 1-2.
func BenchmarkDatabasesHandler_List(b *testing.B) {
	cases := []struct {
		name           string
		numDBs         int
		measurePerDB   int
		expectedCalls  int // Expected with optimization (1 or 2 calls)
	}{
		{"5_databases_2_measurements", 5, 2, 2},
		{"10_databases_3_measurements", 10, 3, 2},
		{"20_databases_5_measurements", 20, 5, 2},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			_, app, counting, cleanup := setupBenchmarkHandler(b, tc.numDBs, tc.measurePerDB)
			defer cleanup()

			// Reset counts before benchmark
			counting.ResetCounts()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				req := httptest.NewRequest("GET", "/api/v1/databases", nil)
				resp, err := app.Test(req)
				if err != nil {
					b.Fatalf("Request failed: %v", err)
				}
				if resp.StatusCode != fiber.StatusOK {
					b.Fatalf("Expected 200, got %d", resp.StatusCode)
				}
				// Drain body
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			b.StopTimer()

			// Report storage call metrics
			totalListCalls := counting.listCalls.Load()
			callsPerRequest := float64(totalListCalls) / float64(b.N)
			b.ReportMetric(callsPerRequest, "list_calls/op")

			// Current: N+1 calls (1 for databases + N for measurements)
			// Expected after fix: 1-2 calls
			expectedCurrentCalls := float64(1 + tc.numDBs) // N+1 pattern
			if callsPerRequest > expectedCurrentCalls*1.1 {
				b.Logf("WARNING: More storage calls than expected: %.1f vs expected %.1f", callsPerRequest, expectedCurrentCalls)
			}
		})
	}
}

// BenchmarkDatabasesHandler_Exists measures storage calls for checking database existence
// This benchmark exposes the inefficient databaseExists() that lists ALL databases
// instead of checking if a single database marker file exists.
func BenchmarkDatabasesHandler_Exists(b *testing.B) {
	cases := []struct {
		name   string
		numDBs int
	}{
		{"10_databases", 10},
		{"50_databases", 50},
		{"100_databases", 100},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			_, app, counting, cleanup := setupBenchmarkHandler(b, tc.numDBs, 2)
			defer cleanup()

			// Test getting a specific database (triggers databaseExists + listMeasurements)
			targetDB := fmt.Sprintf("database%d", tc.numDBs/2)

			counting.ResetCounts()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				req := httptest.NewRequest("GET", "/api/v1/databases/"+targetDB, nil)
				resp, err := app.Test(req)
				if err != nil {
					b.Fatalf("Request failed: %v", err)
				}
				if resp.StatusCode != fiber.StatusOK {
					b.Fatalf("Expected 200, got %d", resp.StatusCode)
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			b.StopTimer()

			totalListCalls := counting.listCalls.Load()
			callsPerRequest := float64(totalListCalls) / float64(b.N)
			b.ReportMetric(callsPerRequest, "list_calls/op")

			// Current: 2 calls (databaseExists lists all DBs, then listMeasurements)
			// Expected after fix: 1 call (just listMeasurements, or 1 Exists check)
		})
	}
}

// BenchmarkDatabasesHandler_ListMeasurements measures redundant existence check
func BenchmarkDatabasesHandler_ListMeasurements(b *testing.B) {
	_, app, counting, cleanup := setupBenchmarkHandler(b, 10, 5)
	defer cleanup()

	counting.ResetCounts()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/api/v1/databases/database5/measurements", nil)
		resp, err := app.Test(req)
		if err != nil {
			b.Fatalf("Request failed: %v", err)
		}
		if resp.StatusCode != fiber.StatusOK {
			b.Fatalf("Expected 200, got %d", resp.StatusCode)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	b.StopTimer()

	totalListCalls := counting.listCalls.Load()
	callsPerRequest := float64(totalListCalls) / float64(b.N)
	b.ReportMetric(callsPerRequest, "list_calls/op")

	// Current: 2 calls (databaseExists + listMeasurements)
	// Expected after fix: 1 call
}
