package database

import (
	"os"
	"testing"

	"github.com/rs/zerolog"
)

// TestBuildDSN_ArcxDisabled confirms that when no arcx path is configured
// the DSN stays empty and DuckDB opens with default (signed-only) settings.
func TestBuildDSN_ArcxDisabled(t *testing.T) {
	t.Parallel()
	cfg := &Config{}
	if dsn := buildDSN(cfg); dsn != "" {
		t.Errorf("buildDSN with no arcx path = %q, want empty", dsn)
	}
}

// TestBuildDSN_ArcxEnabled confirms that ArcxExtensionPath flips the DSN
// to allow_unsigned_extensions, which is required for LOAD on a private,
// unsigned arcx binary.
func TestBuildDSN_ArcxEnabled(t *testing.T) {
	t.Parallel()
	cfg := &Config{ArcxExtensionPath: "/opt/arcx/arcx.duckdb_extension"}
	want := "?allow_unsigned_extensions=true"
	if dsn := buildDSN(cfg); dsn != want {
		t.Errorf("buildDSN with arcx path = %q, want %q", dsn, want)
	}
}

// TestArcxLoadsAndReportsVersion is an opt-in end-to-end test that
// requires the arcx extension to be built locally. Skips when
// ARCX_TEST_PATH is unset; CI does NOT set it. Local devs run after
// `make` in the arcx repo.
//
// Exercises the real wiring: openDuckDB (which registers the connInitFn),
// configureDatabase (which runs verifyArcxLoaded), and the
// connection-pool path so we get evidence that LOAD took effect on a
// fresh connection rather than the implicit first one.
func TestArcxLoadsAndReportsVersion(t *testing.T) {
	path := os.Getenv("ARCX_TEST_PATH")
	if path == "" {
		t.Skip("set ARCX_TEST_PATH to the path of arcx.duckdb_extension to run this test")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("ARCX_TEST_PATH=%q: %v", path, err)
	}

	cfg := &Config{
		ArcxExtensionPath: path,
		MaxConnections:    4, // > 1 so we exercise the pool, not just one connection
		MemoryLimit:       "1GB",
		ThreadCount:       2,
	}
	db, err := New(cfg, zerolog.Nop())
	if err != nil {
		t.Fatalf("database.New: %v", err)
	}
	defer db.Close()

	// Force at least a few distinct pool connections to exercise the
	// connInitFn — if LOAD only ran on the first one, the second
	// arcx_version() call would fail with "function does not exist".
	for i := 0; i < 4; i++ {
		var ver string
		if err := db.DB().QueryRow("SELECT arcx_version()").Scan(&ver); err != nil {
			t.Fatalf("iter %d: SELECT arcx_version(): %v", i, err)
		}
		if ver == "" {
			t.Errorf("iter %d: arcx_version returned empty string", i)
		}
		t.Logf("iter %d: %s", i, ver)
	}
}
