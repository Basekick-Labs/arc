package database

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// writeGeoParquetPair writes, under dir, a plain BLOB file and a GeoParquet
// file (a "geo" key naming geom) of the same column, with the given engine.
func writeGeoParquetPair(t *testing.T, exec func(string) error, dir string) []string {
	t.Helper()
	wkb := "from_hex('0101000000000000000000F03F0000000000000040')"
	geo := `'{"version":"1.1.0","primary_column":"geom","columns":{"geom":{"encoding":"WKB","geometry_types":[]}}}'`
	plain, gp := filepath.Join(dir, "plain.parquet"), filepath.Join(dir, "geo.parquet")
	for _, q := range []string{
		fmt.Sprintf("COPY (SELECT 1 AS t, %s AS geom) TO '%s' (FORMAT PARQUET)", wkb, plain),
		fmt.Sprintf("COPY (SELECT 2 AS t, %s AS geom) TO '%s' (FORMAT PARQUET, KV_METADATA {'geo': %s})", wkb, gp, geo),
	} {
		if err := exec(q); err != nil {
			t.Fatal(err)
		}
	}
	return []string{plain, gp}
}

// The main engine reads a GeoParquet column as BLOB, with the setting off
// after a pool reconnect too, so it unions with Arc's BLOB files (#440).
func TestMainEngineReadsGeoParquetAsBlob(t *testing.T) {
	root := t.TempDir()
	db, err := New(&Config{MaxConnections: 4, MemoryLimit: "256MB", ThreadCount: 1, TempDirectory: root, LocalStorageRoot: root}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	files := writeGeoParquetPair(t, func(q string) error { _, err := db.db.Exec(q); return err }, root)
	ctx := context.Background()
	// Connections held at once, so each is a different one: a session-scoped
	// SET would leave all but the first with conversion on.
	q := fmt.Sprintf("SELECT any_value(typeof(geom)), count(DISTINCT geom) FROM read_parquet(['%s','%s'], union_by_name=true)", files[0], files[1])
	for i := 0; i < 4; i++ {
		conn, err := db.db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		var typ string
		var n int
		if err := conn.QueryRowContext(ctx, q).Scan(&typ, &n); err != nil {
			t.Fatalf("connection %d: query over a BLOB and a GeoParquet file: %v", i, err)
		}
		if typ != "BLOB" || n != 1 {
			t.Fatalf("connection %d: geom read as %s (%d distinct), want BLOB (1)", i, typ, n)
		}
	}
}

// So does the single-thread engine of wide-file DELETE rewrites.
func TestRewriteEngineReadsGeoParquetAsBlob(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenRewriteDB(context.Background(), RewriteDBOptions{MemoryLimitBytes: 256 << 20, SpillDir: dir, AllowedDirs: []string{dir}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	files := writeGeoParquetPair(t, func(q string) error { _, err := db.Exec(q); return err }, dir)
	var typ string
	q := fmt.Sprintf("SELECT any_value(typeof(geom)) FROM read_parquet(['%s','%s'], union_by_name=true)", files[0], files[1])
	if err := db.QueryRow(q).Scan(&typ); err != nil || typ != "BLOB" {
		t.Fatalf("geom read as %q, err %v; want BLOB", typ, err)
	}
}
