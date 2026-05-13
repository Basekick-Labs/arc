package database

import (
	"context"
	"database/sql"
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
// configureDatabase (which runs verifyArcxLoaded), and most importantly
// **distinct concurrent pool connections** — sequential QueryRow calls
// share an idle connection, so they would all pass even if connInitFn
// only fired on the first connection. Holding 4 concurrent *sql.Conn
// forces database/sql to open 4 distinct connections; if the
// per-connection LOAD ever regresses, iter 2+ fails with
// "function arcx_version does not exist".
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
		MaxConnections:    4,
		MemoryLimit:       "1GB",
		ThreadCount:       2,
	}
	db, err := New(cfg, zerolog.Nop())
	if err != nil {
		t.Fatalf("database.New: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	const n = 4
	conns := make([]*sql.Conn, 0, n)
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	// Hold n concurrent connections so the pool is forced to open n
	// distinct ones (database/sql will not reuse an idle conn while
	// another caller still holds it).
	for i := 0; i < n; i++ {
		c, err := db.DB().Conn(ctx)
		if err != nil {
			t.Fatalf("iter %d: acquire conn: %v", i, err)
		}
		conns = append(conns, c)
	}

	// Each pinned connection must have arcx_version() available — proves
	// connInitFn fired on every pool member, not just the first.
	for i, c := range conns {
		var ver string
		if err := c.QueryRowContext(ctx, "SELECT arcx_version()").Scan(&ver); err != nil {
			t.Fatalf("conn %d: SELECT arcx_version(): %v", i, err)
		}
		if ver == "" {
			t.Errorf("conn %d: arcx_version returned empty string", i)
		}
		t.Logf("conn %d: %s", i, ver)
	}
}

// TestArcxStorageRootIsSetOnEveryConn confirms the SET arcx.storage_root
// statement runs alongside LOAD in connInitFn so every pool connection has
// the setting available — without it, arc_partition_agg errors at Bind.
func TestArcxStorageRootIsSetOnEveryConn(t *testing.T) {
	path := os.Getenv("ARCX_TEST_PATH")
	if path == "" {
		t.Skip("set ARCX_TEST_PATH to the path of arcx.duckdb_extension to run this test")
	}
	const want = "/tmp/arc-test-storage"

	cfg := &Config{
		ArcxExtensionPath: path,
		ArcxStorageRoot:   want,
		MaxConnections:    3,
		MemoryLimit:       "1GB",
		ThreadCount:       2,
	}
	db, err := New(cfg, zerolog.Nop())
	if err != nil {
		t.Fatalf("database.New: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	const n = 3
	conns := make([]*sql.Conn, 0, n)
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	for i := 0; i < n; i++ {
		c, err := db.DB().Conn(ctx)
		if err != nil {
			t.Fatalf("iter %d: acquire conn: %v", i, err)
		}
		conns = append(conns, c)
	}
	for i, c := range conns {
		var got string
		if err := c.QueryRowContext(ctx, "SELECT current_setting('arcx.storage_root')").Scan(&got); err != nil {
			t.Fatalf("conn %d: SELECT current_setting: %v", i, err)
		}
		if got != want {
			t.Errorf("conn %d: arcx.storage_root = %q, want %q", i, got, want)
		}
	}
}
