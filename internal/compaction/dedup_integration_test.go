//go:build duckdb_arrow

package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver
)

// TestBuildCompactionQuery_DedupMixedTimeType is a DuckDB-backed regression test
// for the dedup compaction query. String assertions cannot catch the binder
// behavior this code depends on, so this exercises the real generated SQL
// against real parquet fixtures.
//
// It guards two distinct DuckDB pitfalls in the dedup path:
//   - the subquery `SELECT *, ROW_NUMBER() ... ) WHERE rn=1` form mis-binds time
//     as VARCHAR under union_by_name (loud plan error), and
//   - a top-level `SELECT * REPLACE(time...) ... QUALIFY ROW_NUMBER() OVER(... time)`
//     runs the window over the RAW time (QUALIFY precedes projection), silently
//     under-deduping a mixed-type partition.
//
// The fixtures put the SAME (host, time) key in two files — one with time as a
// proper TIMESTAMPTZ, one with time as a VARCHAR epoch string (a pre-fix writer).
// Correct dedup collapses them to exactly one row.
func TestBuildCompactionQuery_DedupMixedTimeType(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// Worst case for tz handling: a non-UTC session timezone. Use the fixed
	// POSIX offset zone Etc/GMT+3 (= UTC-03) rather than a geographic name like
	// America/Argentina/Buenos_Aires: the Etc/* offsets are built into DuckDB's
	// ICU and don't depend on the host tzdata, so this won't be flaky on minimal
	// CI runners (e.g. alpine) that lack the timezone database. (DuckDB rejects a
	// bare numeric offset like '-03:00'.)
	if _, err := db.ExecContext(ctx, "SET TimeZone='Etc/GMT+3'"); err != nil {
		t.Fatalf("set tz: %v", err)
	}

	// ToSlash: these paths are interpolated into DuckDB SQL (COPY TO / read_parquet);
	// on Windows filepath.Join yields backslashes that would break the SQL.
	fileTZ := filepath.ToSlash(filepath.Join(dir, "a_tz.parquet"))
	fileStr := filepath.ToSlash(filepath.Join(dir, "b_str.parquet"))
	out := filepath.ToSlash(filepath.Join(dir, "out.parquet"))

	// 2021-01-01T00:00:00Z = 1609459200000000 microseconds. Same (host,time) in both.
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`COPY (SELECT 'h1' AS host, make_timestamptz(1609459200000000) AS "time", 1.0 AS v) TO '%s' (FORMAT PARQUET)`, escapeSQLPath(fileTZ))); err != nil {
		t.Fatalf("write tz fixture: %v", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`COPY (SELECT 'h1' AS host, '1609459200000000' AS "time", 2.0 AS v) TO '%s' (FORMAT PARQUET)`, escapeSQLPath(fileStr))); err != nil {
		t.Fatalf("write varchar fixture: %v", err)
	}

	fileList := fmt.Sprintf("['%s', '%s']", escapeSQLPath(fileTZ), escapeSQLPath(fileStr))
	query := buildCompactionQuery(fileList, `ORDER BY "time"`, out, []string{"host"})

	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatalf("compaction query failed (bind error regression?): %v\nquery:\n%s", err, query)
	}

	// Correct dedup → exactly one row.
	var rows int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s')`, escapeSQLPath(out))).Scan(&rows); err != nil {
		t.Fatalf("count output: %v", err)
	}
	if rows != 1 {
		t.Errorf("dedup produced %d rows, want 1 (mixed-type (host,time) must collapse to one)", rows)
	}

	// Output time must be TIMESTAMP WITH TIME ZONE (matches Arc's ingest schema; UTC-anchored).
	// Use typeof() rather than a DESCRIBE-subquery: the shipped DuckDB version
	// rejects the DESCRIBE-as-subquery form in some contexts (see the 26.06.2
	// CSV/Parquet-import fix), so avoid relying on it here. typeof() is simpler
	// and equivalent for asserting the column's type.
	var colType string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT typeof("time") FROM read_parquet('%s') LIMIT 1`, escapeSQLPath(out))).Scan(&colType); err != nil {
		t.Fatalf("get type of time: %v", err)
	}
	if colType != "TIMESTAMP WITH TIME ZONE" {
		t.Errorf("output time type = %q, want TIMESTAMP WITH TIME ZONE", colType)
	}

	// The instant must be exactly 1609459200000000 µs regardless of the non-UTC session tz.
	var epochUS int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT epoch_us("time") FROM read_parquet('%s')`, escapeSQLPath(out))).Scan(&epochUS); err != nil {
		t.Fatalf("epoch_us: %v", err)
	}
	if epochUS != 1609459200000000 {
		t.Errorf("stored instant = %d µs, want 1609459200000000 (UTC must be preserved)", epochUS)
	}
}

// TestBuildCompactionQuery_StandardMixedTimeType is the tagless-branch counterpart
// of the dedup test above. A measurement whose Parquet files carry NO "arc:tags"
// metadata (pre-dedup files, msgpack-columnar) takes the standard branch in
// buildCompactionQuery, which the dedup-branch comment claims mis-binds "time"
// as VARCHAR under union_by_name even with a top-level SELECT * REPLACE. This
// test reproduces a mixed-type tagless partition (the live wedge on
// production/cpu/2026/06/18/03) against Arc's real linked DuckDB.
func TestBuildCompactionQuery_StandardMixedTimeType(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, "SET TimeZone='Etc/GMT+3'"); err != nil {
		t.Fatalf("set tz: %v", err)
	}

	fileTZ := filepath.ToSlash(filepath.Join(dir, "a_tz.parquet"))
	fileStr := filepath.ToSlash(filepath.Join(dir, "b_str.parquet"))
	out := filepath.ToSlash(filepath.Join(dir, "out.parquet"))

	// Distinct (host, time) rows — no dedup expected, just a type-mixed partition.
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`COPY (SELECT 'h1' AS host, make_timestamptz(1609459200000000) AS "time", 1.0 AS v) TO '%s' (FORMAT PARQUET)`, escapeSQLPath(fileTZ))); err != nil {
		t.Fatalf("write tz fixture: %v", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`COPY (SELECT 'h2' AS host, '1609462800000000' AS "time", 2.0 AS v) TO '%s' (FORMAT PARQUET)`, escapeSQLPath(fileStr))); err != nil {
		t.Fatalf("write varchar fixture: %v", err)
	}

	fileList := fmt.Sprintf("['%s', '%s']", escapeSQLPath(fileTZ), escapeSQLPath(fileStr))
	// nil tagColumns → standard branch (the wedged path).
	query := buildCompactionQuery(fileList, `ORDER BY "time"`, out, nil)

	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatalf("standard compaction query failed (bind error regression?): %v\nquery:\n%s", err, query)
	}

	var rows int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s')`, escapeSQLPath(out))).Scan(&rows); err != nil {
		t.Fatalf("count output: %v", err)
	}
	if rows != 2 {
		t.Errorf("standard compaction produced %d rows, want 2 (no dedup on distinct keys)", rows)
	}

	var colType string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT typeof("time") FROM read_parquet('%s') LIMIT 1`, escapeSQLPath(out))).Scan(&colType); err != nil {
		t.Fatalf("get type of time: %v", err)
	}
	if colType != "TIMESTAMP WITH TIME ZONE" {
		t.Errorf("output time type = %q, want TIMESTAMP WITH TIME ZONE", colType)
	}
}
