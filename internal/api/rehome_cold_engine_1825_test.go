//go:build duckdb_arrow

package api

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
)

// The real engine over two tiers (two local directories): a read of both,
// with the exclusions of each state of a job on a cold file, returns every
// row once. And the name the engine reports for the cold file, asked the way
// a job asks, is the literal the exclusion uses.
//
// The old file is in cold storage (rows 1, 2, 4); its two replacements are on
// the hot tier (rows 1, 2 and row 4). Each value is a distinct power of two,
// so a sum names the rows read.
func TestColdSourceThroughTheEngine1825(t *testing.T) {
	ctx := context.Background()
	hotRoot, coldRoot := t.TempDir(), t.TempDir()
	hot, err := storage.NewLocalBackend(hotRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	cold, err := storage.NewLocalBackend(coldRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hot.Close(); _ = cold.Close() })
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	const (
		oldKey = "db/m/2026/01/05/m_20260105_daily.parquet"
		out1   = "db/m/2026/01/05/m_new_a_daily.parquet"
		out2   = "db/m/2026/01/06/m_new_b_daily.parquet"
	)
	write := func(root, key, values string) {
		t.Helper()
		full := filepath.Join(root, key)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT * FROM (VALUES %s) v(n)) TO '%s' (FORMAT PARQUET)`, values, full)); err != nil {
			t.Fatal(err)
		}
	}
	write(coldRoot, oldKey, "(1), (2), (4)")
	write(hotRoot, out1, "(1), (2)")
	write(hotRoot, out2, "(4)")

	uri := func(b storage.Backend, key string) string {
		u, err := storage.ObjectURI(b, key)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	hotGlob := strings.TrimSuffix(uri(hot, "db/m/x"), "x") + "**/*.parquet"
	coldGlob := strings.TrimSuffix(uri(cold, "db/m/x"), "x") + "**/*.parquet"
	read := sqlutil.ReadParquetList([]string{sqlutil.QuoteStringLiteral(hotGlob), sqlutil.QuoteStringLiteral(coldGlob)}, "union_by_name=true")
	sum := func(excl readExclusions, coldBackend storage.Backend) int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, "SELECT coalesce(sum(n), 0) FROM "+excl.apply(hot, coldBackend, read)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	// With no job: the old file and its replacements, every row twice. The
	// rig is the case the exclusions exist for.
	if got := sum(readExclusions{}, cold); got != 14 {
		t.Fatalf("with nothing left out: %d, want 14 (every row twice)", got)
	}
	// committed: the replacements are left out, the old file is read.
	if got := sum(readExclusions{keys: []string{out1, out2}}, cold); got != 7 {
		t.Fatalf("committed: %d, want 7", got)
	}
	// latched: the old file is left out, by its address in cold storage.
	latched := readExclusions{keys: []string{oldKey}, coldKeys: []string{oldKey}}
	if got := sum(latched, cold); got != 7 {
		t.Fatalf("latched: %d, want 7", got)
	}
	// The same exclusions without the cold address read the old file too.
	if got := sum(readExclusions{keys: []string{oldKey}}, cold); got != 14 {
		t.Fatalf("latched, left out by its hot address only: %d; the test would not notice a missing cold address", got)
	}

	// What the engine calls the file, asked the way a job asks.
	names := ColdFileNamesFromEngine(db)
	want := uri(cold, oldKey)
	dir := want[:strings.LastIndex(want, "/")]
	for _, glob := range []string{dir + "/*.parquet", dir + "/**/*.parquet"} {
		got, err := names(ctx, glob)
		if err != nil {
			t.Fatalf("%s: %v", glob, err)
		}
		if len(got) != 1 || got[0] != want {
			t.Fatalf("%s: the engine reports %v, the exclusion uses %q", glob, got, want)
		}
	}
	// A glob that matches nothing is an error, not an empty answer: the job
	// refuses on it.
	if _, err := names(ctx, dir+"/nothing-*.parquet"); err == nil {
		t.Fatal("a glob matching nothing returned no error")
	}
}
