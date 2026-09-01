package database

// Regression tests for #682: DuckDB's session time zone must be pinned to
// UTC so naive timestamp literals mean the same instant to the engine as
// they do to the partition pruner (which parses them as UTC).

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// naiveLiteralIsUTC reports whether a naive literal comparison against a
// TIMESTAMPTZ value behaves with UTC semantics on the given handle: the
// instant 14:30Z compared against the naive literal '14:00:00' is only >=
// when the literal is interpreted as 14:00 UTC.
func naiveLiteralIsUTC(t *testing.T, q interface {
	QueryRow(query string, args ...interface{}) *sql.Row
}) bool {
	t.Helper()
	var match bool
	err := q.QueryRow(`SELECT TIMESTAMPTZ '2024-03-15 14:30:00+00' >= '2024-03-15 14:00:00'
		AND TIMESTAMPTZ '2024-03-15 14:30:00+00' < '2024-03-15 16:00:00'`).Scan(&match)
	if err != nil {
		t.Fatalf("literal comparison probe: %v", err)
	}
	return match
}

func TestNew_PinsTimeZoneToUTC(t *testing.T) {
	// Force a non-UTC host zone so this test guards the pin even on UTC CI
	// runners: without it, deleting the SET still passes under TZ=UTC.
	// DuckDB reads TZ at instance open, so Setenv before New is effective.
	t.Setenv("TZ", "America/Costa_Rica")
	tmp := t.TempDir()
	storageRoot := filepath.Join(tmp, "data")
	if err := os.MkdirAll(storageRoot, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	db, err := New(&Config{
		MaxConnections:   2,
		MemoryLimit:      "256MB",
		LocalStorageRoot: storageRoot,
		TempDirectory:    tmp,
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer db.Close()

	var tz string
	if err := db.db.QueryRow("SELECT current_setting('TimeZone')").Scan(&tz); err != nil {
		t.Fatalf("read TimeZone: %v", err)
	}
	if tz != "UTC" {
		t.Fatalf("TimeZone = %q, want UTC — the #682 pin was dropped", tz)
	}
	if !naiveLiteralIsUTC(t, db.db) {
		t.Fatal("naive literal not interpreted as UTC on an Arc handle — pruner and engine disagree (#682)")
	}
}

// Documents the mechanism the pin closes: on a raw handle with a non-UTC
// session zone, the same comparison flips, which is exactly the state a
// non-UTC host was in before the pin.
func TestNaiveLiteral_MismatchWithoutPin(t *testing.T) {
	raw, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer raw.Close()
	// SET TimeZone is session-scoped; force a single pooled connection so
	// the probe below runs on the session that executed the SET.
	raw.SetMaxOpenConns(1)
	if _, err := raw.Exec("SET TimeZone='America/Costa_Rica'"); err != nil {
		t.Skipf("cannot set named zone (ICU unavailable?): %v", err)
	}
	if naiveLiteralIsUTC(t, raw) {
		t.Fatal("expected the naive literal to shift under a non-UTC session zone; the #682 hazard no longer reproduces — re-evaluate whether the pin is still needed")
	}
}
