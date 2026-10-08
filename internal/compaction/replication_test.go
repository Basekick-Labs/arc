package compaction

import (
	"context"
	"database/sql"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

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

// Simulate losing the child after upload but before its completion record.
// Recovery must publish the same coverage before withdrawing source files.
func TestCompactionRecoveryPreservesReplicationCoverage(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	partition := "db/cpu/2026/10/01/01"
	key := partition + "/input.parquet"
	fixture := filepath.Join(t.TempDir(), "input.parquet")
	hour := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).Unix() / 3600
	metadata := replicaview.FileMetadata{Database: "db", Measurement: "cpu", Hour: hour, Coverage: replicaview.Coverage{{Instance: 1, First: 1, Last: 2}}}
	query := "COPY (SELECT TIMESTAMP '2026-10-01 01:00:00' AS time, 7::BIGINT AS v) TO " + sqlutil.QuoteStringLiteral(fixture) + " (FORMAT PARQUET, KV_METADATA {'" + replicaview.FileMetadataKey + "': " + sqlutil.QuoteStringLiteral(metadata.Encode()) + "})"
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	completionDir := filepath.Join(t.TempDir(), "completion")
	// A regular file prevents both completion writes, while storage upload works.
	if err := os.WriteFile(completionDir, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	manifests := NewManifestManager(backend, zerolog.Nop())
	job := NewJob(&JobConfig{Database: "db", Measurement: "cpu", PartitionPath: partition,
		PartitionTime: time.Unix(hour*3600, 0), Files: []string{key}, Tier: "hourly",
		StorageBackend: backend, TempDirectory: t.TempDir(), DB: db, Logger: zerolog.Nop(),
		ManifestManager: manifests, JobID: "coverage-recovery", CompletionDir: completionDir})
	if err := job.Run(ctx); err == nil || !strings.Contains(err.Error(), "failed to write completion manifest") {
		t.Fatalf("expected post-upload interruption, got %v", err)
	}
	if exists, err := backend.Exists(ctx, key); err != nil || !exists {
		t.Fatalf("source was not retained: %v", err)
	}
	if err := os.Remove(completionDir); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{StorageBackend: backend, ManifestManager: manifests, CompletionDir: completionDir, logger: zerolog.Nop()}
	if n, err := manager.recoverOrphanedManifests(ctx, recoveryScope{}); err != nil || n != 1 {
		t.Fatalf("recovery=%d: %v", n, err)
	}
	completion, err := readCompletionManifest(filepath.Join(completionDir, job.JobID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(completion.Outputs) != 1 {
		t.Fatalf("outputs=%v", completion.Outputs)
	}
	output := completion.Outputs[0]
	if !reflect.DeepEqual(output.WALCoverage, metadata.PartitionCoverages()) || !reflect.DeepEqual(output.Replaces, []string{key}) {
		t.Fatalf("recovery lost handoff metadata: %+v", output)
	}
	if completion.State != CompletionStateSourcesDeleted {
		t.Fatalf("state=%s", completion.State)
	}
}
