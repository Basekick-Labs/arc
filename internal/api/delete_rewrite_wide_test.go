package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

const wideKey = "db/wide/2026/10/25/10/f.parquet"

type wideRig struct {
	t       *testing.T
	root    string
	tempDir string
	db      *database.DuckDB
	h       *DeleteHandler
	opened  int // private rewrite instances opened
}

// newWideRig builds a handler on local storage. single picks the rewrite
// path; nil leaves the real gate.
func newWideRig(t *testing.T, single *bool) *wideRig {
	t.Helper()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	tempDir := t.TempDir()
	db, err := database.New(&database.Config{MemoryLimit: "512MB", ThreadCount: 2, MaxConnections: 2, LocalStorageRoot: root, UploadDir: tempDir}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	r := &wideRig{t: t, root: root, tempDir: tempDir, db: db}
	r.h = NewDeleteHandler(db, backend, nil, nil, tempDir, zerolog.Nop())
	if single != nil {
		prev := needsSingleThread
		needsSingleThread = func(rewriteShape, int64, int) bool { return *single }
		t.Cleanup(func() { needsSingleThread = prev })
	}
	prevOpen := openRewriteDB
	openRewriteDB = func(ctx context.Context, o database.RewriteDBOptions, l zerolog.Logger) (*sql.DB, error) {
		r.opened++
		return prevOpen(ctx, o, l)
	}
	t.Cleanup(func() { openRewriteDB = prevOpen })
	return r
}

// file writes wideKey: rows of (time, host, cols extra DOUBLE columns) in
// time order, with Arc's metadata plus an ARROW:schema a rewrite must drop.
func (r *wideRig) file(rows, cols int) string { return r.fileRG(rows, cols, 4096) }

// fileRG is file with source row groups of rg rows.
func (r *wideRig) fileRG(rows, cols, rg int) string {
	r.t.Helper()
	full := filepath.Join(r.root, wideKey)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		r.t.Fatal(err)
	}
	var sel strings.Builder
	sel.WriteString("TIMESTAMP '2026-10-25 10:00:00' + to_microseconds(i) AS time, CASE WHEN i % 2 = 0 THEN 'a' ELSE 'b' END AS host")
	for c := 0; c < cols; c++ {
		fmt.Fprintf(&sel, ", CASE WHEN i %% %d = 0 THEN i::DOUBLE END AS f%d", c+1, c)
	}
	q := fmt.Sprintf(`COPY (SELECT %s FROM range(%d) t(i)) TO '%s' (FORMAT PARQUET, ROW_GROUP_SIZE %d,
		KV_METADATA {'arc:tags': 'host,o''brien', 'arc:dedup_exact': 'true', 'ARROW:schema': 'not-this-file'})`, sel.String(), rows, full, rg)
	if _, err := r.db.DB().Exec(q); err != nil {
		r.t.Fatal(err)
	}
	return full
}

// rewrite deletes host = 'b' from wideKey and returns the new file's path.
func (r *wideRig) rewrite(full string, rows int64) (string, error) {
	r.t.Helper()
	_, rel, err := r.h.rewriteLocalFile(context.Background(), full, wideKey, "host = 'b'", rows, rows/2)
	if err != nil {
		return "", err
	}
	return filepath.Join(r.root, rel), nil
}

func (r *wideRig) kv(path string) map[string]string {
	r.t.Helper()
	rows, err := r.db.DB().Query(fmt.Sprintf("SELECT key, value FROM parquet_kv_metadata('%s')", path))
	if err != nil {
		r.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v []byte
		if err := rows.Scan(&k, &v); err != nil {
			r.t.Fatal(err)
		}
		out[string(k)] = string(v)
	}
	return out
}

func (r *wideRig) scalar(q string, dst ...any) {
	r.t.Helper()
	if err := r.db.DB().QueryRow(q).Scan(dst...); err != nil {
		r.t.Fatalf("%s: %v", q, err)
	}
}

func assertRewritten(t *testing.T, r *wideRig, out string, rows int64) {
	t.Helper()
	var n, b, unordered int64
	r.scalar(fmt.Sprintf("SELECT count(*), count(*) FILTER (WHERE host = 'b') FROM read_parquet('%s')", out), &n, &b)
	if n != rows/2 || b != 0 {
		t.Fatalf("rewritten file has %d rows, %d of host b; want %d and 0", n, b, rows/2)
	}
	r.scalar(fmt.Sprintf(`SELECT count(*) FROM (SELECT time < lag(time) OVER (ORDER BY file_row_number) AS back
		FROM read_parquet('%s', file_row_number=true)) WHERE back`, out), &unordered)
	if unordered != 0 {
		t.Fatalf("rewritten file is out of time order in %d places", unordered)
	}
	kv := r.kv(out)
	if kv["arc:tags"] != "host,o'brien" || kv["arc:dedup_exact"] != "true" {
		t.Fatalf("Arc's metadata not carried over: %q", kv)
	}
	if _, ok := kv["ARROW:schema"]; ok {
		t.Fatalf("ARROW:schema carried into a file DuckDB wrote: %q", kv)
	}
}

func TestDeleteRewriteOfAWideFileRunsOnASingleThreadInstance(t *testing.T) {
	single := true
	r := newWideRig(t, &single)
	const rows = 20000
	out, err := r.rewrite(r.file(rows, 40), rows)
	if err != nil {
		t.Fatal(err)
	}
	if r.opened != 1 {
		t.Fatalf("private rewrite instances opened: %d, want 1", r.opened)
	}
	assertRewritten(t, r, out, rows)
	left, _ := filepath.Glob(filepath.Join(r.tempDir, rewriteScratchPrefix+"*"))
	if len(left) != 0 {
		t.Fatalf("scratch directories left behind: %v", left)
	}
}

func TestDeleteRewriteOfANarrowFileStaysOnTheMainDatabase(t *testing.T) {
	single := false
	r := newWideRig(t, &single)
	const rows = 20000
	out, err := r.rewrite(r.file(rows, 4), rows)
	if err != nil {
		t.Fatal(err)
	}
	if r.opened != 0 {
		t.Fatalf("a narrow file opened %d private instances", r.opened)
	}
	assertRewritten(t, r, out, rows)
}

func TestDeleteRewriteOfAWideFileFailsWhenTheSingleThreadInstanceCannotOpen(t *testing.T) {
	single := true
	r := newWideRig(t, &single)
	openRewriteDB = func(context.Context, database.RewriteDBOptions, zerolog.Logger) (*sql.DB, error) {
		return nil, errors.New("no instance")
	}
	full := r.file(1000, 4)
	if _, err := r.rewrite(full, 1000); err == nil {
		t.Fatal("rewrite fell back to the main database")
	}
	matches, _ := filepath.Glob(filepath.Join(filepath.Dir(full), "*.parquet"))
	if len(matches) != 1 {
		t.Fatalf("partition holds %v, want only the source", matches)
	}
}

func TestDeleteRewriteOfAFileThatCannotBeSizedFails(t *testing.T) {
	r := newWideRig(t, nil)
	full := filepath.Join(r.root, wideKey)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("not parquet"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.rewrite(full, 10); err == nil || !strings.Contains(err.Error(), "size its rewrite") {
		t.Fatalf("rewrite of an unreadable footer: %v", err)
	}
	if r.opened != 0 {
		t.Fatalf("opened %d private instances", r.opened)
	}
}

func TestDeleteRewriteGateUsesTheRealWidthByDefault(t *testing.T) {
	r := newWideRig(t, nil)
	// 2,000 columns in one 16,384-row group: ~295 MB decoded + 300 MB of
	// writer state, against half of 512 MB.
	const rows = 16384
	out, err := r.rewrite(r.fileRG(rows, 2000, rows), rows)
	if err != nil {
		t.Fatal(err)
	}
	if r.opened != 1 {
		t.Fatalf("a 2,000-column file was rewritten on the main database")
	}
	assertRewritten(t, r, out, rows)
	// Row groups sized from the width and the private instance's 256 MB
	// limit (half of 512 MB): 64 MB / (9 + 16 + 2,000 x 9 B) = 3,723 rows,
	// in whole 2,048-row vectors.
	var maxRG int64
	r.scalar(fmt.Sprintf("SELECT max(row_group_num_rows) FROM parquet_metadata('%s')", out), &maxRG)
	if want := int64(2048); maxRG != want {
		t.Fatalf("largest row group %d rows, want %d", maxRG, want)
	}
}

func TestProbeRewriteShapeReadsTheFooter(t *testing.T) {
	r := newWideRig(t, nil)
	full := r.fileRG(10000, 30, 4096)
	s, err := probeRewriteShape(context.Background(), r.db.DB(), full, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	// time (INT64) + host (BYTE_ARRAY) + 30 DOUBLE columns.
	if s.leafColumns != 32 || s.bytesPerRow != 9+16+30*9 || s.rowGroups != 3 || s.maxRowGroupRows != 4096 || s.totalRows != 10000 {
		t.Fatalf("shape %+v", s)
	}
	// The main instance writes the whole 10,000 rows as one row group.
	if want := int64(9+16+30*9)*10000 + 32*writerStatePerColumn; s.perThreadBytes() != want {
		t.Fatalf("per-thread bytes %d, want %d", s.perThreadBytes(), want)
	}
	if len(s.kv) != 2 || s.kv[0] != [2]string{"arc:dedup_exact", "true"} || s.kv[1] != [2]string{"arc:tags", "host,o'brien"} {
		t.Fatalf("metadata %q", s.kv)
	}
}

func TestNeedsSingleThread(t *testing.T) {
	const gb = int64(1) << 30
	wide := rewriteShape{leafColumns: 3100, bytesPerRow: 3100 * 9, maxRowGroupRows: 122880, rowGroups: 8}
	narrow := rewriteShape{leafColumns: 64, bytesPerRow: 64 * 9, maxRowGroupRows: 122880, rowGroups: 2}
	many := rewriteShape{leafColumns: 64, bytesPerRow: 64 * 9, maxRowGroupRows: 122880, rowGroups: 40}
	for _, c := range []struct {
		name    string
		s       rewriteShape
		mem     int64
		threads int
		want    bool
	}{
		{"3,100 columns, 46 threads, 4 GB", wide, 4 * gb, 46, true},
		{"64 columns, 2 row groups, 46 threads, 4 GB", narrow, 4 * gb, 46, false},
		{"64 columns, 40 row groups, 46 threads, 4 GB", many, 4 * gb, 46, true},
		{"64 columns, 40 row groups, 4 threads, 4 GB", many, 4 * gb, 4, false},
		{"no row groups", rewriteShape{leafColumns: 1, bytesPerRow: 9}, 4 * gb, 46, false},
		// A file in small row groups (one this path rewrote) is priced at the
		// 122,880-row group the main instance would write.
		{"1,000 columns in 120 x 4,096-row groups, 2 threads, 1 GB",
			rewriteShape{leafColumns: 1000, bytesPerRow: 1000 * 9, maxRowGroupRows: 4096, totalRows: 491520, rowGroups: 120}, gb, 2, true},
		{"3,100 columns in 4,096-row groups, 2 threads, 4 GB",
			rewriteShape{leafColumns: 3100, bytesPerRow: 3100 * 9, maxRowGroupRows: 4096, totalRows: 983040, rowGroups: 240}, 4 * gb, 2, true},
		// Half the limit is the line: 600 MB in flight against 1 GB.
		{"600 MB of a 1 GB limit", rewriteShape{bytesPerRow: 1, maxRowGroupRows: 600 << 20, rowGroups: 1}, gb, 8, true},
		{"400 MB of a 1 GB limit", rewriteShape{bytesPerRow: 1, maxRowGroupRows: 400 << 20, rowGroups: 1}, gb, 8, false},
	} {
		if got := needsSingleThread(c.s, c.mem, c.threads); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestSingleThreadRowGroupRows(t *testing.T) {
	const gb = int64(1) << 30
	for _, c := range []struct {
		perRow, limit, want int64
	}{
		{3100 * 9, 2 * gb, 4096},
		{10000 * 9, 2 * gb, 2048},
		{200000 * 9, 2 * gb, 2048},
		{8, 2 * gb, parquetRowGroupSize},
		{0, 2 * gb, parquetRowGroupSize},
		// A 256 MB instance: a quarter of its limit per row group.
		{1000 * 9, 256 << 20, 6144},
	} {
		if got := (rewriteShape{bytesPerRow: c.perRow}).singleThreadRowGroupRows(c.limit); got != c.want {
			t.Errorf("%d B/row: %d rows, want %d", c.perRow, got, c.want)
		}
	}
}

func TestNewDeleteHandlerRemovesRewriteScratchLeftBehind(t *testing.T) {
	dir := t.TempDir()
	left := filepath.Join(dir, rewriteScratchPrefix+"42", "spill")
	if err := os.MkdirAll(left, 0o700); err != nil {
		t.Fatal(err)
	}
	NewDeleteHandler(nil, nil, nil, nil, dir, zerolog.Nop())
	if _, err := os.Stat(filepath.Dir(left)); !os.IsNotExist(err) {
		t.Fatalf("scratch left by a stopped process still there: %v", err)
	}
}

func TestRemoveRewriteScratchKeepsEverythingElse(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{rewriteScratchPrefix + "123", "upload-x"} {
		if err := os.MkdirAll(filepath.Join(dir, d, "spill"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, rewriteScratchPrefix+"file"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	removeRewriteScratch(dir, zerolog.Nop())
	entries, _ := os.ReadDir(dir)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if strings.Join(names, ",") != rewriteScratchPrefix+"file,upload-x" {
		t.Fatalf("left %v", names)
	}
}
