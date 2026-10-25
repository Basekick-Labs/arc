//go:build duckdb_arrow

package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// A partition holding a GeoParquet file (a "geo" key naming geom) next to
// Arc's own BLOB files of the same column compacts (#440). With the engine's
// GeoParquet conversion on, the GeoParquet file reads geom as GEOMETRY, the
// BLOB file cannot be cast to it, and the compaction fails on every cycle.
func TestCompactionOfAGeoParquetFileNextToBlobFiles(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	baseDir := t.TempDir()
	backend, err := storage.NewLocalBackend(baseDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	const partition = "testdb/places/2026/04/11/14"
	wkb := "from_hex('0101000000000000000000F03F0000000000000040')"
	geo := `'{"version":"1.1.0","primary_column":"geom","columns":{"geom":{"encoding":"WKB","geometry_types":[]}}}'`
	inputs := map[string]string{
		partition + "/a.parquet": fmt.Sprintf(`COPY (SELECT TIMESTAMPTZ '2026-04-11 14:00:01Z' AS time, %s AS geom) TO '%%s' (FORMAT PARQUET)`, wkb),
		partition + "/b.parquet": fmt.Sprintf(`COPY (SELECT TIMESTAMPTZ '2026-04-11 14:00:02Z' AS time, %s AS geom) TO '%%s' (FORMAT PARQUET, KV_METADATA {'geo': %s})`, wkb, geo),
	}
	var keys []string
	for key, q := range inputs {
		fixture := filepath.Join(t.TempDir(), "f.parquet")
		if _, err := db.ExecContext(ctx, fmt.Sprintf(q, escapeSQLPath(filepath.ToSlash(fixture)))); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.Write(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	storageConfig := backend.ConfigJSON()
	res, err := RunSubprocessJob(&SubprocessJobConfig{
		Database: "testdb", Measurement: "places", PartitionPath: partition, Files: keys,
		Tier: "hourly", TempDirectory: t.TempDir(), StorageType: backend.Type(), StorageConfig: storageConfig,
		JobID: "geoparquet-440",
	})
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}
	if !res.Success || res.FilesCompacted != 2 {
		t.Fatalf("result %+v, want both files compacted", res)
	}
}
