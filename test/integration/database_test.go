package integration

import (
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/logger"
)

func TestDuckDBConnection(t *testing.T) {
	// Setup logger
	logger.Setup("info", "json")
	log := logger.Get("test")

	// Create database config
	cfg := &database.Config{
		MaxConnections: 5,
		MemoryLimit:    "1GB",
		ThreadCount:    2,
		EnableWAL:      true,
	}

	// Initialize database
	db, err := database.New(cfg, log)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Test simple query
	rows, err := db.Query("SELECT 1 as num, 'test' as str")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	var num int
	var str string
	if rows.Next() {
		if err := rows.Scan(&num, &str); err != nil {
			t.Fatalf("Failed to scan result: %v", err)
		}

		if num != 1 {
			t.Errorf("Expected num=1, got %d", num)
		}
		if str != "test" {
			t.Errorf("Expected str='test', got '%s'", str)
		}
	} else {
		t.Fatal("No rows returned")
	}
}

func TestDuckDBGeneratedData(t *testing.T) {
	logger.Setup("info", "json")
	log := logger.Get("test")

	cfg := &database.Config{
		MaxConnections: 5,
		MemoryLimit:    "1GB",
		ThreadCount:    2,
		EnableWAL:      true,
	}

	db, err := database.New(cfg, log)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Test data generation (similar to Python test)
	query := `
		SELECT
			unnest(range(1, 101)) as id,
			'test_' || unnest(range(1, 101))::VARCHAR as name,
			random() as value
	`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		var id int
		var name string
		var value float64

		if err := rows.Scan(&id, &name, &value); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		rowCount++
	}

	if rowCount != 100 {
		t.Errorf("Expected 100 rows, got %d", rowCount)
	}
}

func TestDuckDBConnectionPool(t *testing.T) {
	logger.Setup("info", "json")
	log := logger.Get("test")

	cfg := &database.Config{
		MaxConnections: 5,
		MemoryLimit:    "1GB",
		ThreadCount:    2,
		EnableWAL:      true,
	}

	db, err := database.New(cfg, log)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Check pool stats
	stats := db.Stats()
	if stats.MaxOpenConnections != 5 {
		t.Errorf("Expected max open connections=5, got %d", stats.MaxOpenConnections)
	}

	t.Logf("Pool stats: Open=%d, InUse=%d, Idle=%d",
		stats.OpenConnections, stats.InUse, stats.Idle)
}
