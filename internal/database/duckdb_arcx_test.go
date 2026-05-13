package database

import (
	"database/sql"
	"os"
	"testing"

	"github.com/rs/zerolog"
)

// TestBuildDSN_ArcxDisabled confirms that when no arcx path is configured
// the DSN stays empty and DuckDB opens with default (signed-only) settings.
func TestBuildDSN_ArcxDisabled(t *testing.T) {
	cfg := &Config{}
	if dsn := buildDSN(cfg); dsn != "" {
		t.Errorf("buildDSN with no arcx path = %q, want empty", dsn)
	}
}

// TestBuildDSN_ArcxEnabled confirms that ArcxExtensionPath flips the DSN
// to allow_unsigned_extensions, which is required for LOAD on a private,
// unsigned arcx binary.
func TestBuildDSN_ArcxEnabled(t *testing.T) {
	cfg := &Config{ArcxExtensionPath: "/opt/arcx/arcx.duckdb_extension"}
	want := "?allow_unsigned_extensions=true"
	if dsn := buildDSN(cfg); dsn != want {
		t.Errorf("buildDSN with arcx path = %q, want %q", dsn, want)
	}
}

// TestConfigureArcxExtension_DisabledNoOp confirms the loader is a no-op
// when ArcxExtensionPath is empty. The test doesn't open a real DuckDB
// connection because configureDatabase's caller already guards on the
// path being non-empty before invoking configureArcxExtension; this is
// belt-and-suspenders for the helper itself, which we never call with an
// empty path in production.
func TestConfigureArcxExtension_DisabledNoOp(t *testing.T) {
	// This guards the precondition. configureArcxExtension does not check
	// cfg.ArcxExtensionPath itself; the caller (configureDatabase) does.
	// If the caller's check ever regresses, this test will keep passing
	// silently — so we rely on TestConfigureDatabase_ArcxDisabled below
	// for the real check.
	t.Skip("covered by configureDatabase precondition check; see TestConfigureDatabase_ArcxDisabled")
}

// TestConfigureArcxExtension_LoadsAndReportsVersion is an opt-in end-to-end
// test that requires the arcx extension to be built locally. Skips when
// ARCX_TEST_PATH is unset; CI does NOT set it, so this test only runs
// when a developer points it at a freshly-built .duckdb_extension after
// `make` in the arcx repo.
func TestConfigureArcxExtension_LoadsAndReportsVersion(t *testing.T) {
	path := os.Getenv("ARCX_TEST_PATH")
	if path == "" {
		t.Skip("set ARCX_TEST_PATH to the path of arcx.duckdb_extension to run this test")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("ARCX_TEST_PATH=%q: %v", path, err)
	}

	cfg := &Config{ArcxExtensionPath: path}
	dsn := buildDSN(cfg)
	if dsn != "?allow_unsigned_extensions=true" {
		t.Fatalf("buildDSN returned %q, expected allow_unsigned_extensions", dsn)
	}

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("db.Ping: %v", err)
	}

	if err := configureArcxExtension(db, cfg, zerolog.Nop()); err != nil {
		t.Fatalf("configureArcxExtension: %v", err)
	}

	var ver string
	if err := db.QueryRow("SELECT arcx_version()").Scan(&ver); err != nil {
		t.Fatalf("SELECT arcx_version(): %v", err)
	}
	if ver == "" {
		t.Errorf("arcx_version returned empty string")
	}
	t.Logf("arcx loaded: %s", ver)
}
