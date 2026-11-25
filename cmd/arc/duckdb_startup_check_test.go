package main

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/rs/zerolog"
)

// TestCheckDuckDBReleasesConnection pins #1782: the startup test query must give
// its pooled connection back. On a one-connection pool a held connection leaves
// nothing for queries, so the follow-up query below would wait out its context.
func TestCheckDuckDBReleasesConnection(t *testing.T) {
	db, err := database.New(&database.Config{
		MaxConnections:   1,
		MemoryLimit:      "256MB",
		ThreadCount:      1,
		LocalStorageRoot: t.TempDir(),
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("database.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	message, version, found, err := checkDuckDB(db)
	if err != nil {
		t.Fatalf("checkDuckDB: %v", err)
	}
	if !found || message != "Hello from Arc in Go!" || version == "" {
		t.Fatalf("checkDuckDB = (%q, %q, %v), want the test row and a DuckDB version", message, version, found)
	}

	if inUse := db.Stats().InUse; inUse != 0 {
		t.Fatalf("connections in use after the startup check = %d, want 0", inUse)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var one int
	if err := db.DB().QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("query on the one-connection pool after the startup check: %v", err)
	}
}
