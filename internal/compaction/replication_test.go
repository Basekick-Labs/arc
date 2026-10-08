package compaction

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/basekick-labs/arc/internal/replicaview"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestCompactionPreservesReplicationCoverageAcrossHours(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root := t.TempDir()
	var paths []string
	for i, hour := range []int64{1, 2} {
		path := filepath.Join(root, []string{"a.parquet", "b.parquet"}[i])
		metadata := replicaview.FileMetadata{Database: "db", Measurement: "cpu", Hour: hour, Coverage: replicaview.Coverage{{Instance: 1, First: 1, Last: 2}}}
		query := "COPY (SELECT TIMESTAMP '2026-10-01 01:00:00' AS time, 7::BIGINT AS v) TO " + sqlutil.QuoteStringLiteral(path) + " (FORMAT PARQUET, KV_METADATA {'" + replicaview.FileMetadataKey + "': " + sqlutil.QuoteStringLiteral(metadata.Encode()) + "})"
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	metadata, err := compactionReplicationMetadata(paths, []string{"db/cpu/a.parquet", "db/cpu/b.parquet"})
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata.Partitions) != 2 || len(metadata.Replaces) != 2 {
		t.Fatalf("hour coverage lost: %+v", metadata)
	}
	output := filepath.Join(root, "out.parquet")
	list := "[" + sqlutil.QuoteStringLiteral(paths[0]) + "," + sqlutil.QuoteStringLiteral(paths[1]) + "]"
	// CQ dedup deliberately removes a row; its original WAL identity must
	// remain covered or a stale replica would bring that row back.
	statements := buildCompactionQuery(list, "ORDER BY time", output, nil, true, map[string]string{replicaview.FileMetadataKey: metadata.Encode(), replicaview.MetadataKey: metadata.Coverage.Encode()})
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, stmt := range statements {
		if _, err := conn.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("compaction: %v\n%s", err, stmt)
		}
	}
	f, err := os.Open(output)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	actual, err := replicaview.ReadFileMetadata(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, metadata) {
		t.Fatalf("compacted metadata changed: %+v != %+v", actual, metadata)
	}
	var count int
	if err := db.QueryRow("SELECT count(*) FROM read_parquet(?)", output).Scan(&count); err != nil || count != 1 {
		t.Fatalf("dedup rows=%d err=%v", count, err)
	}
}
