package ingest

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/replicaview"
	"github.com/basekick-labs/arc/internal/storage"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"
)

func TestReplicaQueryHandoffRealParquet(t *testing.T) {
	for _, tc := range []struct {
		name                                             string
		primaryEvery, replicaEvery, identical, multiHour bool
	}{
		{name: "same_flush_boundaries"},
		{name: "separate_primary_flushes", primaryEvery: true},
		{name: "separate_replica_flushes", replicaEvery: true},
		{name: "identical_legitimate_writes", primaryEvery: true, identical: true},
		{name: "multiple_hour_partitions", multiHour: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := &config.IngestConfig{MaxBufferSize: 1000000, MaxBufferAgeMS: 60000, FlushWorkers: 1, FlushQueueSize: 10, ShardCount: 1, Compression: "snappy"}
			makeBuffer := func() (*ArrowBuffer, *storage.LocalBackend, string, *replicaview.View) {
				root := t.TempDir()
				backend, err := storage.NewLocalBackend(root, zerolog.Nop())
				if err != nil {
					t.Fatal(err)
				}
				b := NewArrowBuffer(cfg, backend, zerolog.Nop())
				view := replicaview.NewView()
				b.SetReplicationPublisher(view)
				t.Cleanup(func() { _ = b.Close(); _ = backend.Close() })
				return b, backend, root, view
			}
			primary, pstore, _, _ := makeBuffer()
			replica, rstore, root, view := makeBuffer()
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			query := func(wantRows, wantDistinct int) {
				t.Helper()
				snapshot := view.Snapshot("db", "cpu")
				defer snapshot.Close()
				relation, err := snapshot.SQL(func(path string) string { return filepath.Join(root, path) }, "", "hive_partitioning=false")
				if err != nil {
					t.Fatal(err)
				}
				var count, distinct int
				if err := db.QueryRow("SELECT count(*),count(DISTINCT v) FROM "+relation).Scan(&count, &distinct); err != nil {
					t.Fatalf("query: %v\n%s", err, relation)
				}
				if count != wantRows || distinct != wantDistinct {
					t.Fatalf("rows=%d distinct=%d; want %d/%d", count, distinct, wantRows, wantDistinct)
				}
				// Internal row positions must not shadow user columns or leak into SELECT *.
				var a, b, c int
				if err := db.QueryRow("SELECT count(DISTINCT ordinality),count(DISTINCT file_row_number),count(DISTINCT entry_row) FROM "+relation).Scan(&a, &b, &c); err != nil {
					t.Fatal(err)
				}
				if a != 1 || b != 1 || c != 1 {
					t.Fatalf("user columns changed: %d/%d/%d", a, b, c)
				}
			}
			expectedRows := 0
			for i := 0; i < 2; i++ {
				identity := fmt.Sprintf("%016x%016x", uint64(123), uint64(i+1))
				value := i + 1
				if tc.identical {
					value = 1
				}
				timestamps := []interface{}{int64(1700000000000000)}
				values := []interface{}{int64(value)}
				if tc.multiHour {
					timestamps = append(timestamps, int64(1700000000000000)+int64(time.Hour/time.Microsecond))
					values = append(values, int64(value))
				}
				columns := map[string][]interface{}{"time": timestamps, "v": values}
				for _, name := range []string{"ordinality", "file_row_number", "entry_row"} {
					columns[name] = make([]interface{}, len(values))
					for j := range values {
						columns[name][j] = int64(99)
					}
				}
				if err := primary.WriteColumnarDirectReplay(ctx, "db", "cpu", columns, identity); err != nil {
					t.Fatal(err)
				}
				if err := replica.WriteReplicatedColumnar(ctx, "db", "cpu", columns, identity); err != nil {
					t.Fatal(err)
				}
				// A concurrent replay of an entry already buffered is idempotent.
				if err := replica.WriteReplicatedColumnar(ctx, "db", "cpu", columns, identity); err != nil {
					t.Fatal(err)
				}
				expectedRows += len(values)
				if tc.primaryEvery {
					if err := primary.FlushAll(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if tc.replicaEvery {
					if err := replica.FlushAll(ctx); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := primary.FlushAll(ctx); err != nil {
				t.Fatal(err)
			}
			if err := replica.FlushAll(ctx); err != nil {
				t.Fatal(err)
			}
			wantDistinct := 2
			if tc.identical {
				wantDistinct = 1
			}
			query(expectedRows, wantDistinct)
			// Replica materializations cannot accidentally appear in normal canonical
			// globs or masquerade as measurement subdirectories.
			if files, err := rstore.List(ctx, "db/cpu"); err != nil || len(files) != 0 {
				t.Fatalf("replica published canonical files: %v %v", files, err)
			}
			paths, err := pstore.List(ctx, "db/cpu")
			if err != nil || len(paths) == 0 {
				t.Fatalf("primary files: %v %v", paths, err)
			}
			for _, path := range paths {
				data, err := pstore.Read(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				metadata, err := replicaview.ReadFileMetadata(bytes.NewReader(data), int64(len(data)))
				if err != nil || metadata == nil {
					t.Fatalf("metadata: %v %v", metadata, err)
				}
				if err := rstore.Write(ctx, path, data); err != nil {
					t.Fatal(err)
				}
				// Publication stands for a checksum-verified completed pull, not a Raft
				// advertisement. Query after each arrival, including mismatched boundaries.
				if err := view.PublishReplicationFile(ctx, path, *metadata, int64(len(data)), "verified-"+path); err != nil {
					t.Fatal(err)
				}
				query(expectedRows, wantDistinct)
			}
		})
	}
}
