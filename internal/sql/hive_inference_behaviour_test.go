package sql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver registration
)

// TestHiveInferenceOverwritesStoredColumn is the bug, against the real engine:
// a key=value DIRECTORY component makes DuckDB synthesise a column, and on a
// name collision the path's value replaces the stored value AND its type, with
// no error. The second half asserts that what ReadParquet emits stops it.
//
// Fails pre-fix: without hive_partitioning=false the first read returns the
// directory's value.
func TestHiveInferenceOverwritesStoredColumn(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "host=hub01")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "f.parquet")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"COPY (SELECT 'realhost' AS host, 7 AS v) TO %s (FORMAT PARQUET)",
		QuoteStringLiteral(file))); err != nil {
		t.Fatal(err)
	}

	// What Arc builds now.
	var host string
	if err := db.QueryRowContext(ctx,
		"SELECT host FROM "+ReadParquet(QuoteStringLiteral(file))).Scan(&host); err != nil {
		t.Fatal(err)
	}
	if host != "realhost" {
		t.Errorf("stored value was overwritten by the path: got %q, want %q", host, "realhost")
	}

	// The unguarded spelling, asserted rather than logged: if this ever stops
	// returning the path's value the engine has changed and the flag may no
	// longer be load-bearing, which is something a maintainer must be told
	// rather than left to notice.
	var unguarded string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT host FROM read_parquet(%s)", QuoteStringLiteral(file))).Scan(&unguarded); err != nil {
		t.Fatal(err)
	}
	if unguarded != "hub01" {
		t.Errorf("an unguarded read returned %q, not the directory's value %q — Hive inference appears to have changed; re-confirm that hive_partitioning=false is still required before relaxing anything (#1005)", unguarded, "hub01")
	}
}

// TestHiveInferenceChangesPredicateResults is why the delete API is the worst
// case: the predicate is evaluated against the phantom, so a WHERE on the
// DIRECTORY value matches every row (which drives delete's "all rows deleted,
// remove the file" branch) and a WHERE on the STORED value matches none.
func TestHiveInferenceChangesPredicateResults(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "host=hub01")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "f.parquet")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"COPY (SELECT 'realhost' AS host, 1 AS v UNION ALL SELECT 'realhost', 2) TO %s (FORMAT PARQUET)",
		QuoteStringLiteral(file))); err != nil {
		t.Fatal(err)
	}

	count := func(where string) int64 {
		var n int64
		q := fmt.Sprintf("SELECT COUNT(*) FILTER (WHERE %s) FROM %s",
			where, ReadParquet(QuoteStringLiteral(file)))
		if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if got := count("host = 'hub01'"); got != 0 {
		t.Errorf("a predicate on the DIRECTORY value matched %d rows; want 0 — this is what deletes the wrong file", got)
	}
	if got := count("host = 'realhost'"); got != 2 {
		t.Errorf("a predicate on the STORED value matched %d rows; want 2 — this is the delete that silently no-ops", got)
	}
}
