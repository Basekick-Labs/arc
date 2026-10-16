package database

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
)

func TestConfiguredDuckDBExtensionsLoadBeforeSandboxLockdown(t *testing.T) {
	// Keep DuckDB's downloaded extension cache inside the test's temporary
	// directory rather than the developer's home directory.
	t.Setenv("HOME", t.TempDir())

	root := t.TempDir()
	db, err := New(&Config{
		MaxConnections:   1,
		MemoryLimit:      "256MB",
		ThreadCount:      1,
		TempDirectory:    root,
		LocalStorageRoot: root,
		Extensions:       []string{"spatial"},
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("New with spatial extension: %v", err)
	}
	defer db.Close()

	var point bool
	if err := db.db.QueryRowContext(context.Background(),
		"SELECT ST_X(ST_Point(1, 2)) = 1 AND ST_Y(ST_Point(1, 2)) = 2").Scan(&point); err != nil {
		t.Fatalf("query spatial functions after sandbox lockdown: %v", err)
	}
	if !point {
		t.Fatal("spatial functions returned unexpected coordinates")
	}
}
