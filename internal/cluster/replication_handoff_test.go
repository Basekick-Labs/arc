package cluster

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/replicaview"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"
)

type handoffRegistrar struct{ calls atomic.Int64 }

func (r *handoffRegistrar) RegisterFile(_, _, _ string, _ time.Time, _ int64, _ string) {
	r.calls.Add(1)
}

// This exercises real ingestion, Parquet encoding, storage and SQL. The copy
// below models a completed peer pull; it does not exercise its network protocol.
// In particular, matching whole-file fingerprints cannot satisfy both cases
// with different flush boundaries, and content deduplication cannot satisfy
// the identical accepted writes case.
func TestReplicationHandoffKeepsExactlyOneCopy(t *testing.T) {
	for _, tc := range []struct {
		name         string
		primaryEvery bool
		replicaEvery bool
		identical    bool
	}{
		{name: "same_boundaries"},
		{name: "primary_flushes_separately", primaryEvery: true},
		{name: "replica_flushes_separately", replicaEvery: true},
		{name: "identical_distinct_writes", primaryEvery: true, identical: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := &config.IngestConfig{MaxBufferSize: 1000000, MaxBufferAgeMS: 60000, FlushWorkers: 1, FlushQueueSize: 10, ShardCount: 1, Compression: "snappy"}
			newBackend := func() (*storage.LocalBackend, string) {
				root := t.TempDir()
				backend, err := storage.NewLocalBackend(root, zerolog.Nop())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = backend.Close() })
				return backend, root
			}
			primary, _ := newBackend()
			replica, root := newBackend()
			primaryBuffer := ingest.NewArrowBuffer(cfg, primary, zerolog.Nop())
			replicaBuffer := ingest.NewArrowBuffer(cfg, replica, zerolog.Nop())
			view := replicaview.NewView()
			primaryBuffer.SetReplicationPublisher(replicaview.NewView())
			replicaBuffer.SetReplicationPublisher(view)
			t.Cleanup(func() { _ = primaryBuffer.Close() })
			t.Cleanup(func() { _ = replicaBuffer.Close() })
			primaryRegistrar, replicaRegistrar := &handoffRegistrar{}, &handoffRegistrar{}
			primaryBuffer.SetFileRegistrar(primaryRegistrar)
			replicaBuffer.SetFileRegistrar(replicaRegistrar)
			coordinator := &Coordinator{ingestBuffer: replicaBuffer, localNode: NewNode("reader", "reader", RoleReader, "handoff"), logger: zerolog.Nop()}
			handler := coordinator.buildReplicationIngestHandler()
			for i := int64(0); i < 2; i++ {
				value := i + 1
				if tc.identical {
					value = 1
				}
				columns := map[string][]interface{}{"time": {int64(1700000000000000)}, "v": {value}}
				identity := fmt.Sprintf("%016x%016x", uint64(123), uint64(i+1))
				if err := primaryBuffer.WriteColumnarDirectReplay(ctx, "default", "live", columns, identity); err != nil {
					t.Fatal(err)
				}
				payload, err := msgpack.Marshal(map[string]interface{}{"m": "live", "columns": columns})
				if err != nil {
					t.Fatal(err)
				}
				idBytes, _ := hex.DecodeString(identity)
				tracked := append([]byte{wal.WALTrackedMarker}, idBytes...)
				tracked = append(tracked, payload...)
				if err := handler.ApplyReplicatedEntry(ctx, tracked); err != nil {
					t.Fatal(err)
				}
				if tc.primaryEvery {
					if err := primaryBuffer.FlushAll(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if tc.replicaEvery {
					if err := replicaBuffer.FlushAll(ctx); err != nil {
						t.Fatal(err)
					}
				}
			}
			for _, buffer := range []*ingest.ArrowBuffer{primaryBuffer, replicaBuffer} {
				if err := buffer.FlushAll(ctx); err != nil {
					t.Fatal(err)
				}
			}
			paths, err := primary.List(ctx, "default/live")
			if err != nil || len(paths) == 0 {
				t.Fatalf("primary paths=%v error=%v", paths, err)
			}
			for _, path := range paths {
				data, err := primary.Read(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				if err := replica.Write(ctx, path, data); err != nil {
					t.Fatal(err)
				}
				metadata, err := replicaview.ReadFileMetadata(bytes.NewReader(data), int64(len(data)))
				if err != nil || metadata == nil {
					t.Fatalf("primary metadata: %v", err)
				}
				if err := view.PublishReplicationFile(ctx, path, *metadata, int64(len(data)), "verified-"+path); err != nil {
					t.Fatal(err)
				}
			}
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var total, distinct int
			snapshot := view.Snapshot("default", "live")
			defer snapshot.Close()
			relation, err := snapshot.SQL(func(path string) string { return filepath.Join(root, path) }, "", "hive_partitioning=false")
			if err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT count(*), count(DISTINCT v) FROM "+relation).Scan(&total, &distinct); err != nil {
				t.Fatal(err)
			}
			wantDistinct := 2
			if tc.identical {
				wantDistinct = 1
			}
			if total != 2 || distinct != wantDistinct {
				t.Errorf("query returned %d rows, %d distinct; want 2 rows, %d distinct", total, distinct, wantDistinct)
			}
			if calls := replicaRegistrar.calls.Load(); calls != 0 {
				t.Errorf("replica announced %d independent files", calls)
			}
			if calls := primaryRegistrar.calls.Load(); calls != int64(len(paths)) {
				t.Errorf("primary announced %d files, want %d", calls, len(paths))
			}
		})
	}
}
