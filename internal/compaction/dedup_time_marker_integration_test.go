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
	marked     = `{'arc:dedup_time': 'true'}`
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
		`SELECT value FROM parquet_kv_metadata('%s') WHERE key = 'arc:dedup_time'`, escapeSQLPath(out))).Scan(&marker)
	if err != nil && err != sql.ErrNoRows {
		e.t.Fatalf("read output marker: %v", err)
	}
	return out, got, marker == "true", logs.String()
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

// The #1227 bug: a grouped continuous query with no tag_columns marks a window
// that held one series and not one that held two. Before the fix one marked
// file made the batch dedup on time, and host b's row at minute 1 was deleted.
func TestCompactFiles_OneMarkedFileNoLongerCollapsesTheBatch(t *testing.T) {
	e := newMarkerEnv(t)
	a := e.write(markerFixture{rows: row("a", t1, 1), kv: marked})
	b := e.write(markerFixture{rows: union(row("a", t2, 2), row("b", t2, 3))})
	_, got, outMarked, _ := e.compact(a, b)
	expectRows(t, got, "a@0=1", "a@1=2", "b@1=3")
	if outMarked {
		t.Error("output of a mixed batch carries arc:dedup_time")
	}
}

// What #521 built the marker for still works: two emissions of the same window
// by a no-group-by continuous query collapse, and the output keeps the marker
// so a later compaction can dedup a further re-emission against it. Which of
// the two equal-time rows survives is not asserted: the dedup window orders by
// time alone, so the tie-break is arbitrary (pre-existing).
func TestCompactFiles_AllMarkedReEmissionCollapsesAndStaysMarked(t *testing.T) {
	e := newMarkerEnv(t)
	a := e.write(markerFixture{rows: union(row("-", t1, 1), row("-", t2, 2)), kv: marked})
	b := e.write(markerFixture{rows: row("-", t1, 1.5), kv: marked})
	out, got, outMarked, _ := e.compact(a, b)
	if len(got) != 2 {
		t.Fatalf("rows = %v, want one per minute", got)
	}
	if !outMarked {
		t.Fatal("output of time-only dedup does not carry arc:dedup_time")
	}

	// The next hourly pass, or the daily tier, sees the compacted output plus a
	// further re-emission: still one row per time.
	c := e.write(markerFixture{rows: row("-", t2, 9), kv: marked})
	_, got, outMarked, _ = e.compact(out, c)
	if len(got) != 2 {
		t.Errorf("re-emission against a compacted output: rows = %v, want one per minute", got)
	}
	if !outMarked {
		t.Error("second-pass output lost the marker")
	}

	// A compacted output plus an unmarked file is a mixed batch.
	d := e.write(markerFixture{rows: row("-", t3, 4)})
	_, _, outMarked, _ = e.compact(out, d)
	if outMarked {
		t.Error("a compaction with an unmarked input produced a marked output")
	}
}

// A file marked by a build before #1227 can hold several rows per time (the
// buffer ORed the marker across the batches of a flush). The marker is not
// trusted on its own: such a batch keeps every distinct row.
func TestCompactFiles_MarkedFileNotUniqueOnTimeIsNotTimeDeduped(t *testing.T) {
	e := newMarkerEnv(t)
	poisoned := e.write(markerFixture{rows: union(row("a", t1, 1), row("b", t1, 2)), kv: marked})
	clean := e.write(markerFixture{rows: row("c", t2, 3), kv: marked})
	_, got, outMarked, log := e.compact(poisoned, clean)
	expectRows(t, got, "a@0=1", "b@0=2", "c@1=3")
	if outMarked {
		t.Error("output carries arc:dedup_time though an input was not unique on time")
	}
	if !strings.Contains(log, "holds several rows per time") {
		t.Errorf("no warning naming the cause; log:\n%s", log)
	}
}

// The uniqueness probe compares time the way compaction does — normalized — so
// a file holding the same instant spelled twice (a VARCHAR epoch and an ISO
// string) is not unique. Comparing the raw values would call it unique and
// time-dedup two series into one.
func TestCompactFiles_UniquenessProbeNormalizesTime(t *testing.T) {
	e := newMarkerEnv(t)
	spelled := e.write(markerFixture{
		rows: `SELECT 'a' AS host, '1767225600000000' AS "time", 1::DOUBLE AS v
		       UNION ALL SELECT 'b', '2026-01-01 00:00:00+00', 2::DOUBLE`,
		kv: marked})
	other := e.write(markerFixture{rows: `SELECT 'c' AS host, '2026-01-01 00:01:00+00' AS "time", 3::DOUBLE AS v`, kv: marked})
	_, got, _, _ := e.compact(spelled, other)
	expectRows(t, got, "a@0=1", "b@0=2", "c@1=3")
}

// A batch mixing marked and unmarked files drops rows identical in every
// column — a WAL-replayed or replicated copy — and keeps everything else,
// including a re-emission whose values changed.
func TestCompactFiles_MixedBatchDropsOnlyExactCopies(t *testing.T) {
	e := newMarkerEnv(t)
	original := e.write(markerFixture{rows: union(row("-", t1, 1), row("-", t2, 2)), kv: marked})
	replayed := e.write(markerFixture{rows: union(row("-", t1, 1), row("-", t2, 2.5))})
	_, got, outMarked, _ := e.compact(original, replayed)
	expectRows(t, got, "-@0=1", "-@1=2", "-@1=2.5")
	if outMarked {
		t.Error("output of exact-row dedup carries arc:dedup_time")
	}
}

// Raw ingest without tags or markers is untouched, identical rows included.
func TestCompactFiles_UnmarkedTaglessBatchKeepsEveryRow(t *testing.T) {
	e := newMarkerEnv(t)
	a := e.write(markerFixture{rows: row("a", t1, 1)})
	b := e.write(markerFixture{rows: row("a", t1, 1)})
	_, got, _, _ := e.compact(a, b)
	expectRows(t, got, "a@0=1", "a@0=1")
}

// A tag-read failure used to fall through to the marker, so a batch of marked
// files carrying tags could be deduped on time alone. It now skips dedup.
func TestCompactFiles_TagReadErrorSkipsDedup(t *testing.T) {
	e := newMarkerEnv(t)
	bad := `{'arc:dedup_time': 'true', 'arc:tags': 'host;drop'}`
	a := e.write(markerFixture{rows: row("a", t1, 1), kv: bad})
	b := e.write(markerFixture{rows: row("b", t1, 2), kv: marked})
	_, got, outMarked, _ := e.compact(a, b)
	expectRows(t, got, "a@0=1", "b@0=2")
	if outMarked {
		t.Error("output marked though no time-only dedup ran")
	}
}

// A legacy file carrying both arc:tags and the marker dedups on (tags, time)
// and its output is NOT marked: it holds several rows per time, and compacted
// output carries no arc:tags to say which. Marking it would let the daily tier
// dedup it on time alone.
func TestCompactFiles_TaggedOutputIsNeverMarked(t *testing.T) {
	e := newMarkerEnv(t)
	both := `{'arc:dedup_time': 'true', 'arc:tags': 'host'}`
	a := e.write(markerFixture{rows: union(row("a", t1, 1), row("b", t1, 2)), kv: both})
	b := e.write(markerFixture{rows: row("a", t1, 1), kv: both})
	out1, got, outMarked, _ := e.compact(a, b)
	expectRows(t, got, "a@0=1", "b@0=2")
	if outMarked {
		t.Fatal("tagged dedup produced a marked output")
	}
	c := e.write(markerFixture{rows: union(row("c", t1, 3), row("d", t1, 4)), kv: both})
	out2, _, _, _ := e.compact(c)
	_, got, _, _ = e.compact(out1, out2)
	expectRows(t, got, "a@0=1", "b@0=2", "c@0=3", "d@0=4")
}

// Only the value "true" marks a file; anything else is unmarked.
func TestCompactFiles_MarkerMustBeTrue(t *testing.T) {
	e := newMarkerEnv(t)
	a := e.write(markerFixture{rows: row("a", t1, 1), kv: `{'arc:dedup_time': 'false'}`})
	b := e.write(markerFixture{rows: row("b", t1, 2), kv: marked})
	_, got, _, _ := e.compact(a, b)
	expectRows(t, got, "a@0=1", "b@0=2")
}
