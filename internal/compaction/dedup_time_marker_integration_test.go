//go:build duckdb_arrow

package compaction

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// markerFixture is one Parquet input: a SELECT producing its rows, and the
// footer key-value metadata ingest (or an older build) wrote on it.
type markerFixture struct {
	rows string
	kv   string // DuckDB KV_METADATA map literal, or "" for none
}

const (
	marked     = `{'arc:dedup_exact': 'true'}`
	t1         = `TIMESTAMPTZ '2026-01-01 00:00:00+00'`
	t2         = `TIMESTAMPTZ '2026-01-01 00:01:00+00'`
	t3         = `TIMESTAMPTZ '2026-01-01 00:02:00+00'`
	markerTest = "dedup-time-marker"
)

type markerEnv struct {
	t   *testing.T
	db  *sql.DB
	dir string
	n   int
	raw bool
}

func newMarkerEnv(t *testing.T) *markerEnv {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return &markerEnv{t: t, db: db, dir: t.TempDir()}
}

func (e *markerEnv) write(f markerFixture) string {
	e.t.Helper()
	e.n++
	path := filepath.ToSlash(filepath.Join(e.dir, fmt.Sprintf("in%d.parquet", e.n)))
	opts := "FORMAT PARQUET"
	if f.kv != "" {
		opts += ", KV_METADATA " + f.kv
	}
	if _, err := e.db.Exec(fmt.Sprintf(`COPY (%s) TO '%s' (%s)`, f.rows, escapeSQLPath(path), opts)); err != nil {
		e.t.Fatalf("write fixture %d: %v", e.n, err)
	}
	return path
}

// compact runs the production compactFiles over the given input paths and
// returns the output path, its rows as sorted "series@minute=value" strings,
// whether it carries the arc:dedup_time marker, and the job log.
func (e *markerEnv) compact(paths ...string) (string, []string, bool, string) {
	e.t.Helper()
	var logs bytes.Buffer
	job := &Job{Measurement: "cq_out", Tier: "hourly", TempDirectory: e.dir, JobID: markerTest, logger: zerolog.New(&logs), db: e.db}
	files := make([]downloadedFile, len(paths))
	for i, p := range paths {
		files[i] = downloadedFile{localPath: p, storageKey: fmt.Sprintf("db/cq_out/%d.parquet", i)}
	}
	out, err := job.compactFiles(context.Background(), files, e.dir)
	if err != nil {
		e.t.Fatalf("compactFiles: %v\nlog:\n%s", err, logs.String())
	}
	if e.raw {
		return out, nil, false, logs.String()
	}
	rows, err := e.db.Query(fmt.Sprintf(
		`SELECT coalesce(host, '-'), minute("time"), v FROM read_parquet('%s', union_by_name=true)`, escapeSQLPath(out)))
	if err != nil {
		e.t.Fatalf("read output: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var host string
		var minute int
		var v float64
		if err := rows.Scan(&host, &minute, &v); err != nil {
			e.t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%s@%d=%g", host, minute, v))
	}
	sort.Strings(got)
	var marker string
	err = e.db.QueryRow(fmt.Sprintf(
		`SELECT value FROM parquet_kv_metadata('%s') WHERE key = 'arc:dedup_exact'`, escapeSQLPath(out))).Scan(&marker)
	if err != nil && err != sql.ErrNoRows {
		e.t.Fatalf("read output marker: %v", err)
	}
	return out, got, marker == "true", logs.String()
}

// compactRaw compacts without reading the output as host/minute/v rows, for
// fixtures with other columns.
func (e *markerEnv) compactRaw(paths ...string) (string, []string, bool, string) {
	e.raw = true
	defer func() { e.raw = false }()
	return e.compact(paths...)
}

func row(host, at string, v float64) string {
	return fmt.Sprintf(`SELECT '%s' AS host, %s AS "time", %g::DOUBLE AS v`, host, at, v)
}

func union(rows ...string) string { return strings.Join(rows, " UNION ALL ") }

func expectRows(t *testing.T, got []string, want ...string) {
	t.Helper()
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("rows = %v, want %v", got, want)
	}
}

const tagged = `{'arc:tags': 'host'}`

// #1232: rows that share tags and time but differ in a field are different
// data. Before, compaction kept one row per (host, time) and deleted the rest —
// a correction, or a re-sent point with a new value, was lost. Only a row
// identical in every column collapses.
func TestCompactFiles_TaggedKeepsRowsThatDifferInAField(t *testing.T) {
	e := newMarkerEnv(t)
	a := e.write(markerFixture{rows: union(row("a", t1, 1), row("b", t1, 5)), kv: tagged})
	b := e.write(markerFixture{rows: union(row("a", t1, 2), row("b", t1, 5)), kv: tagged})
	_, got, outMarked, _ := e.compact(a, b)
	expectRows(t, got, "a@0=1", "a@0=2", "b@0=5")
	if outMarked {
		t.Error("tagged output carries arc:dedup_time")
	}
}

// Two writes of the same series and time carrying different fields are both
// kept: one row per key would drop one write's fields.
func TestCompactFiles_PartialWritesAreBothKept(t *testing.T) {
	e := newMarkerEnv(t)
	a := e.write(markerFixture{rows: `SELECT 'a' AS host, ` + t1 + ` AS "time", 1::DOUBLE AS usage`, kv: tagged})
	b := e.write(markerFixture{rows: `SELECT 'a' AS host, ` + t1 + ` AS "time", 5::DOUBLE AS temp`, kv: tagged})
	out, _, _, _ := e.compactRaw(a, b)
	var n, usage, temp int
	if err := e.db.QueryRow(fmt.Sprintf(`SELECT count(*), count(usage), count(temp) FROM read_parquet('%s', union_by_name=true)`,
		escapeSQLPath(out))).Scan(&n, &usage, &temp); err != nil {
		t.Fatal(err)
	}
	if n != 2 || usage != 1 || temp != 1 {
		t.Errorf("rows=%d usage=%d temp=%d, want both partial writes kept", n, usage, temp)
	}
}

// A tagless file shares a tagged partition: its rows have NULL in the tag
// column. Key matching must treat NULLs as equal (IS NOT DISTINCT FROM), or
// those rows match no key and drop out of the output entirely.
func TestCompactFiles_NullTagKeysAreNeitherLostNorDuplicated(t *testing.T) {
	e := newMarkerEnv(t)
	a := e.write(markerFixture{rows: row("a", t1, 1), kv: tagged})
	b := e.write(markerFixture{rows: `SELECT ` + t1 + ` AS "time", 7::DOUBLE AS v`})
	c := e.write(markerFixture{rows: `SELECT ` + t1 + ` AS "time", 7::DOUBLE AS v UNION ALL SELECT ` + t2 + `, 8::DOUBLE`})
	_, got, _, _ := e.compact(a, b, c)
	expectRows(t, got, "a@0=1", "-@0=7", "-@1=8")
}

// The #1227 case: a grouped continuous query with no tag_columns, one window
// with one series and one with two. Every distinct row survives, and the
// output stays marked so a later pass checks it again.
func TestCompactFiles_GroupedTaglessContinuousQueryKeepsEverySeries(t *testing.T) {
	e := newMarkerEnv(t)
	a := e.write(markerFixture{rows: row("a", t1, 1), kv: marked})
	b := e.write(markerFixture{rows: union(row("a", t2, 2), row("b", t2, 3)), kv: marked})
	_, got, outMarked, _ := e.compact(a, b)
	expectRows(t, got, "a@0=1", "a@1=2", "b@1=3")
	if !outMarked {
		t.Error("tagless exact-row output lost the marker")
	}
}

// Continuous-query output: an identical copy of a window (WAL replay, a retry
// over unchanged data) collapses even when the copy is unmarked; a re-run whose
// value changed is kept beside the original. A further copy against the
// compacted output still collapses: the output is marked.
func TestCompactFiles_ContinuousQueryCopiesCollapseReRunsAreKept(t *testing.T) {
	e := newMarkerEnv(t)
	original := e.write(markerFixture{rows: union(row("-", t1, 1), row("-", t2, 2)), kv: marked})
	replayed := e.write(markerFixture{rows: row("-", t1, 1)})
	rerun := e.write(markerFixture{rows: row("-", t2, 2.5), kv: marked})
	out, got, outMarked, _ := e.compact(original, replayed, rerun)
	expectRows(t, got, "-@0=1", "-@1=2", "-@1=2.5")
	if !outMarked {
		t.Fatal("output not marked")
	}
	again := e.write(markerFixture{rows: row("-", t1, 1)})
	_, got, _, _ = e.compact(out, again)
	expectRows(t, got, "-@0=1", "-@1=2", "-@1=2.5")
}

// Raw ingest without tags or a marker is untouched, identical rows included.
func TestCompactFiles_UnmarkedTaglessBatchKeepsEveryRow(t *testing.T) {
	e := newMarkerEnv(t)
	a := e.write(markerFixture{rows: row("a", t1, 1)})
	b := e.write(markerFixture{rows: row("a", t1, 1)})
	_, got, outMarked, _ := e.compact(a, b)
	expectRows(t, got, "a@0=1", "a@0=1")
	if outMarked {
		t.Error("undeduplicated output is marked")
	}
}

// Only the value "true" marks a file.
func TestCompactFiles_MarkerMustBeTrue(t *testing.T) {
	e := newMarkerEnv(t)
	a := e.write(markerFixture{rows: row("a", t1, 1), kv: `{'arc:dedup_time': 'false'}`})
	b := e.write(markerFixture{rows: row("a", t1, 1), kv: `{'arc:dedup_time': 'false'}`})
	_, got, _, _ := e.compact(a, b)
	expectRows(t, got, "a@0=1", "a@0=1")
}

// A tag-read failure skips dedup for the batch rather than guessing a key.
func TestCompactFiles_TagReadErrorSkipsDedup(t *testing.T) {
	e := newMarkerEnv(t)
	bad := `{'arc:dedup_time': 'true', 'arc:tags': 'host;drop'}`
	a := e.write(markerFixture{rows: row("a", t1, 1), kv: bad})
	b := e.write(markerFixture{rows: row("a", t1, 1), kv: marked})
	_, got, _, _ := e.compact(a, b)
	expectRows(t, got, "a@0=1", "a@0=1")
}

// Files written before #1232 carry the older arc:dedup_time key; it still opts
// the batch in. Compacted output is marked with the new key only, so a
// rolled-back build — which reads arc:dedup_time as "dedup on time alone" —
// never sees it.
func TestCompactFiles_LegacyMarkerStillOptsIn(t *testing.T) {
	e := newMarkerEnv(t)
	legacy := `{'arc:dedup_time': 'true'}`
	a := e.write(markerFixture{rows: union(row("a", t1, 1), row("b", t1, 2)), kv: legacy})
	b := e.write(markerFixture{rows: row("a", t1, 1)})
	out, got, outMarked, _ := e.compact(a, b)
	expectRows(t, got, "a@0=1", "b@0=2")
	if !outMarked {
		t.Error("output not marked with arc:dedup_exact")
	}
	var n int
	if err := e.db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM parquet_kv_metadata('%s') WHERE key = 'arc:dedup_time'`,
		escapeSQLPath(out))).Scan(&n); err != nil || n != 0 {
		t.Errorf("output carries the legacy arc:dedup_time key (count %d, err %v)", n, err)
	}
}
