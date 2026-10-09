package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Exercise the production callback, Arrow buffer, real Parquet backend and WAL
// checkpoints. Reopen both writer and buffer after only the first range commits.
// This is a controlled restart, not a power-loss simulation.
func testTrackedRowRecoveryParquetRestart(t *testing.T, count, firstLimit, retryLimit int, source []string, dataRoot string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	if dataRoot == "" {
		dataRoot = filepath.Join(dir, "data")
	}
	backend, err := storage.NewLocalBackend(dataRoot, zerolog.Nop())
	require.NoError(t, err)
	walDir := filepath.Join(dir, "wal")
	newPair := func() (*wal.Writer, *ingest.ArrowBuffer, func()) {
		w, err := wal.NewWriter(&wal.WriterConfig{WALDir: walDir, SyncMode: wal.SyncModeFsync, Logger: zerolog.Nop()})
		require.NoError(t, err)
		b := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000,
			Compression: "snappy", ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 8, FlushTimeoutSeconds: 5}, backend, zerolog.Nop())
		b.SetWAL(w)
		var once sync.Once
		closePair := func() { once.Do(func() { require.NoError(t, b.Close()); require.NoError(t, w.Close()) }) }
		t.Cleanup(closePair)
		return w, b, closePair
	}
	w, b, closePair := newPair()
	rows := make([]map[string]interface{}, count)
	for i := range rows {
		rows[i] = map[string]interface{}{"_database": "test", "_measurement": "bounded_rows", "time": int64(1700000000000000 + i), "index": int64(i)}
		if len(source) > 0 {
			rows[i]["source_line"] = source[i]
		}
	}
	ids, err := w.AppendTracked(rows)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	path := w.CurrentFile()
	require.NoError(t, w.Rotate())
	callback := createTrackedWALRecoveryCallback(b, zerolog.Nop())
	calls := 0
	stats, err := wal.NewRecovery(walDir, zerolog.Nop()).RecoverWithOptions(ctx, nil, &wal.RecoveryOptions{
		SkipActiveFile: w.CurrentFile(), BatchSize: firstLimit,
		ValidateTrackedRows: validateTrackedWALRecoveryRows,
		TrackedRowCallback: func(ctx context.Context, batch []map[string]interface{}, id string) error {
			calls++
			if calls > 1 {
				return errors.New("interrupt replay before next batch")
			}
			require.Len(t, batch, firstLimit)
			if err := callback(ctx, batch, id); err != nil {
				return err
			}
			return b.FlushAllAndWait(ctx)
		},
		BeforeDelete:        b.NewRecoveryFlushBarrier(),
		CheckpointRecovered: w.MarkFlushedContext,
	})
	require.NoError(t, err)
	require.Positive(t, stats.KeptFiles)
	require.FileExists(t, path)
	require.Equal(t, 1, w.PendingUnflushedCount())
	proof, err := w.CurrentCheckpointHashes()
	require.NoError(t, err)
	require.NotContains(t, proof, ids[0], "partial flush checkpointed the whole entry")
	require.Len(t, proof, 1)
	parent, start, end, ok := wal.ParseRecoveryRowIdentity(proof[0])
	require.True(t, ok)
	require.Equal(t, ids[0], parent)
	require.Zero(t, start)
	require.Equal(t, firstLimit, end)
	closePair()
	w, b, _ = newPair()
	callback = createTrackedWALRecoveryCallback(b, zerolog.Nop())
	stats, err = wal.NewRecovery(walDir, zerolog.Nop()).RecoverWithOptions(ctx, nil, &wal.RecoveryOptions{
		SkipActiveFile: w.CurrentFile(), BatchSize: retryLimit,
		ValidateTrackedRows: validateTrackedWALRecoveryRows,
		TrackedRowCallback: func(ctx context.Context, batch []map[string]interface{}, id string) error {
			require.LessOrEqual(t, len(batch), retryLimit)
			return callback(ctx, batch, id)
		},
		BeforeDelete:        b.NewRecoveryFlushBarrier(),
		CheckpointRecovered: w.MarkFlushedContext,
	})
	require.NoError(t, err)
	require.Zero(t, stats.KeptFiles)
	require.Equal(t, count-firstLimit, stats.RecoveredEntries)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
	proof, err = w.CurrentCheckpointHashes()
	require.NoError(t, err)
	require.Contains(t, proof, ids[0])
	keys, err := backend.List(ctx, "test/bounded_rows/")
	require.NoError(t, err)
	seen := make(map[int64]int)
	for _, key := range keys {
		if !strings.HasSuffix(key, ".parquet") {
			continue
		}
		data, err := backend.Read(ctx, key)
		require.NoError(t, err)
		table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		require.NoError(t, err)
		func() {
			defer table.Release()
			indices := table.Schema().FieldIndices("index")
			require.Len(t, indices, 1)
			for _, chunk := range table.Column(indices[0]).Data().Chunks() {
				require.Zero(t, chunk.NullN())
				switch values := chunk.(type) {
				case *array.Int64:
					for _, v := range values.Int64Values() {
						seen[v]++
					}
				case *array.Float64:
					for _, v := range values.Float64Values() {
						require.Equal(t, float64(int64(v)), v)
						seen[int64(v)]++
					}
				default:
					t.Fatalf("unexpected index type %T", chunk)
				}
			}
		}()
	}
	require.Len(t, seen, count)
	for i := 0; i < count; i++ {
		require.Equal(t, 1, seen[int64(i)], "Parquet index %d", i)
	}
}

func TestTrackedRowRecoveryParquetRestart(t *testing.T) {
	testTrackedRowRecoveryParquetRestart(t, 7, 3, 2, nil, "")
}

func TestTrackedRowRecoveryValidatesBeforeBatching(t *testing.T) {
	for _, invalid := range []map[string]interface{}{{"m": "other"}, {"m": "events", "database": "other"}, {"value": 1}} {
		dir := t.TempDir()
		w, err := wal.NewWriter(&wal.WriterConfig{WALDir: dir, SyncMode: wal.SyncModeFsync, Logger: zerolog.Nop()})
		require.NoError(t, err)
		_, err = w.AppendTracked([]map[string]interface{}{{"m": "events"}, invalid})
		require.NoError(t, err)
		require.NoError(t, w.Close())
		stats, err := wal.NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &wal.RecoveryOptions{
			BatchSize: 1, ValidateTrackedRows: validateTrackedWALRecoveryRows,
			TrackedRowCallback: func(context.Context, []map[string]interface{}, string) error {
				t.Fatal("invalid entry partially submitted")
				return nil
			},
		})
		require.NoError(t, err)
		require.Equal(t, 1, stats.KeptFiles)
	}
}
