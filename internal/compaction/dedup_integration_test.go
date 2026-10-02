//go:build duckdb_arrow

package compaction

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver
)

// execCompaction runs the statement(s) buildCompactionQuery returns, in order,
// on a single pinned connection — mirroring the production caller in job.go. The
// pin is required, not cosmetic: the dedup path's TEMP table is connection-local
// and database/sql does not guarantee two ExecContext calls share a connection.
func execCompaction(ctx context.Context, db *sql.DB, fileListSQL, orderByClause, outputFile string, tagColumns []string, dedupTime bool) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if len(tagColumns) > 0 || dedupTime {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = conn.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+dedupStagingTable)
		}
		conn.Close()
	}()
	for _, stmt := range buildCompactionQuery(fileListSQL, orderByClause, outputFile, tagColumns, dedupTime) {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

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
	if err := execCompaction(ctx, db, fileList, `ORDER BY "time"`, out, []string{"host"}, false); err != nil {
		t.Fatalf("compaction query failed (bind error regression?): %v", err)
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

func TestCountParquetRows(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	file := filepath.ToSlash(filepath.Join(dir, "rows.parquet"))
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`COPY (SELECT * FROM range(3)) TO '%s' (FORMAT PARQUET)`, escapeSQLPath(file))); err != nil {
		t.Fatalf("write parquet fixture: %v", err)
	}

	count, err := countParquetRows(ctx, db, fmt.Sprintf("['%s']", escapeSQLPath(file)))
	if err != nil {
		t.Fatalf("count parquet rows: %v", err)
	}
	if count != 3 {
		t.Fatalf("countParquetRows() = %d, want 3", count)
	}

	// Job.compactFiles passes a BARE quoted path, not a list, whenever a batch
	// holds one file — a different parquet_file_metadata overload, and the one
	// shape nothing else covers. Which overload it hit did not matter pre-fix
	// (the function always errored); it does now.
	bare, err := countParquetRows(ctx, db, fmt.Sprintf("'%s'", escapeSQLPath(file)))
	if err != nil || bare != 3 {
		t.Fatalf("single-path form: countParquetRows() = %d, err = %v, want 3", bare, err)
	}
}

// TestDedupRatioLogFires is #1015's first acceptance criterion, and the reason
// the issue was filed: the dedup-ratio log is the ONLY output Arc produces for
// how many rows de-duplication removed, and it had never fired since auto-dedup
// shipped, because countParquetRows always returned a Binder Error and the
// error was discarded.
//
// Counting rows correctly is necessary but not sufficient — the log sits behind
// `dedupBranch && rowsBefore > 0` and then `rowsAfter > 0 && rowsAfter <
// rowsBefore`. A unit test on countParquetRows alone would still pass if any of
// those gates went wrong, which is exactly how the metric went dark unnoticed
// the first time. So drive the real caller, Job.compactFiles, and assert on the
// log line an operator would actually read.
//
// Fails pre-fix: with the parquet_metadata spelling the count errors, rowsBefore
// stays 0, and the log is skipped.
func TestDedupRatioLogFires(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// Two files carrying the "arc:tags" footer entry ingest writes, with
	// identical (host, time) pairs — so dedup must collapse 4 rows to 2.
	rows := `SELECT 'h1' AS host, TIMESTAMPTZ '2026-01-01 00:00:00' AS "time", 1 AS v
	         UNION ALL SELECT 'h2', TIMESTAMPTZ '2026-01-01 00:00:01', 2`
	write := func(name string) string {
		path := filepath.ToSlash(filepath.Join(dir, name))
		if _, err := db.ExecContext(ctx, fmt.Sprintf(
			`COPY (%s) TO '%s' (FORMAT PARQUET, KV_METADATA {'arc:tags': 'host'})`,
			rows, escapeSQLPath(path))); err != nil {
			t.Fatalf("write fixture %s: %v", name, err)
		}
		return path
	}

	var logs bytes.Buffer
	job := &Job{
		Measurement:   "cpu",
		Tier:          "hourly",
		TempDirectory: dir,
		JobID:         "dedup-ratio-log",
		logger:        zerolog.New(&logs),
		db:            db,
	}
	output, err := job.compactFiles(ctx, []downloadedFile{
		{localPath: write("a.parquet"), storageKey: "db/cpu/a.parquet"},
		{localPath: write("b.parquet"), storageKey: "db/cpu/b.parquet"},
	}, dir)
	if err != nil {
		t.Fatalf("compactFiles: %v\nlog:\n%s", err, logs.String())
	}

	var got int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*) FROM read_parquet('%s')`, escapeSQLPath(output))).Scan(&got); err != nil {
		t.Fatalf("count output rows: %v", err)
	}
	if got != 2 {
		t.Fatalf("compacted output has %d rows, want 2 — the fixture no longer exercises dedup, so the log assertion below proves nothing", got)
	}

	line := logs.String()
	if !strings.Contains(line, "Deduplication removed duplicate rows") {
		t.Fatalf("the dedup-ratio log did not fire on a partition where dedup removed half the rows (#1015); log:\n%s", line)
	}
	for _, want := range []string{`"rows_before":4`, `"rows_after":2`, `"rows_deduped":2`, `"dedup_ratio":50`} {
		if !strings.Contains(line, want) {
			t.Errorf("dedup-ratio log is missing %s; log:\n%s", want, line)
		}
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
	// nil tagColumns → standard branch.
	if err := execCompaction(ctx, db, fileList, `ORDER BY "time"`, out, nil, false); err != nil {
		t.Fatalf("standard compaction query failed (bind error regression?): %v", err)
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

// TestBuildCompactionQuery_DedupTimeOnly is the #521 no-group-by CQ path against
// real DuckDB: NO tag columns, dedupTime=true. Two files carry the SAME "time"
// (a window re-emitted twice by a crash-retry); dedup on time alone must collapse
// them to exactly one row. Without the dedupTime flag these would both survive
// (the standard branch keeps distinct rows), so this proves the marker is what
// makes a tagless CQ idempotent.
func TestBuildCompactionQuery_DedupTimeOnlyDuckDB(t *testing.T) {
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

	fileA := filepath.ToSlash(filepath.Join(dir, "a.parquet"))
	fileB := filepath.ToSlash(filepath.Join(dir, "b.parquet"))
	out := filepath.ToSlash(filepath.Join(dir, "out.parquet"))

	// Same window (same "time"), no tag column — two emissions of a no-group-by
	// aggregate. avg differs (2.0 vs 2.5) but they are the same window; dedup
	// keeps the newest by "time" DESC (a tie here, so exactly one survives).
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`COPY (SELECT make_timestamptz(1609459200000000) AS "time", 2.0 AS avg_v) TO '%s' (FORMAT PARQUET)`, escapeSQLPath(fileA))); err != nil {
		t.Fatalf("write fixture a: %v", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`COPY (SELECT make_timestamptz(1609459200000000) AS "time", 2.5 AS avg_v) TO '%s' (FORMAT PARQUET)`, escapeSQLPath(fileB))); err != nil {
		t.Fatalf("write fixture b: %v", err)
	}

	fileList := fmt.Sprintf("['%s', '%s']", escapeSQLPath(fileA), escapeSQLPath(fileB))
	// nil tagColumns, dedupTime=true → PARTITION BY "time" alone.
	if err := execCompaction(ctx, db, fileList, `ORDER BY "time"`, out, nil, true); err != nil {
		t.Fatalf("dedup-time compaction failed: %v", err)
	}

	var rows int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s')`, escapeSQLPath(out))).Scan(&rows); err != nil {
		t.Fatalf("count output: %v", err)
	}
	if rows != 1 {
		t.Errorf("dedup-time produced %d rows, want 1 (same-time rows must collapse)", rows)
	}
}
