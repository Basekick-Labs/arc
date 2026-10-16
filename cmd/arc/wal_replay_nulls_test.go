package main

// A NULL cell survives a WAL replay as NULL: typed writes (CSV/Parquet/TLE
// imports) wrote it into the WAL row as 0 / "" / false before, and a sparse
// column NULL across a whole replay batch came back as an all-NULL string
// column. User columns named like the WAL row's legacy target keys survive
// replay.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// replayed writes through write into a buffer with a WAL, never flushing it,
// then replays the WAL through the production tracked callback with
// recovery batches of batchSize into a second buffer (raw columnar entries
// through the production columnar callback), and returns the Parquet files
// that replay wrote.
func replayed(t *testing.T, batchSize int, write func(b *ingest.ArrowBuffer)) []arrow.Table {
	t.Helper()
	tables, _ := replayedWithTags(t, batchSize, write)
	return tables
}

// replayedWithTags is replayed, plus each replayed file's arc:tags metadata
// (the tag columns the replayed write carried).
func replayedWithTags(t *testing.T, batchSize int, write func(b *ingest.ArrowBuffer)) ([]arrow.Table, []string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	cfg := &config.IngestConfig{MaxBufferSize: 1_000_000, MaxBufferAgeMS: 600000,
		Compression: "snappy", ShardCount: 2, FlushWorkers: 1, FlushQueueSize: 8, FlushTimeoutSeconds: 5}

	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: walDir, SyncMode: wal.SyncModeFsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	discard, err := storage.NewLocalBackend(filepath.Join(dir, "never-flushed"), zerolog.Nop())
	require.NoError(t, err)
	src := ingest.NewArrowBuffer(cfg, discard, zerolog.Nop())
	src.SetWAL(w)
	write(src)
	require.NoError(t, w.Rotate()) // the written entries are now in a closed file

	replayRoot := filepath.Join(dir, "replayed")
	backend, err := storage.NewLocalBackend(replayRoot, zerolog.Nop())
	require.NoError(t, err)
	dst := ingest.NewArrowBuffer(cfg, backend, zerolog.Nop())
	t.Cleanup(func() { _ = dst.Close(); _ = w.Close() })
	callback := createTrackedWALRecoveryCallback(dst, zerolog.Nop())
	_, err = wal.NewRecovery(walDir, zerolog.Nop()).RecoverWithOptions(ctx, nil, &wal.RecoveryOptions{
		SkipActiveFile:                 w.CurrentFile(),
		BatchSize:                      batchSize,
		ValidateTrackedRows:            validateTrackedWALRecoveryRows,
		TrackedRowCallback:             callback,
		ColumnarCallbackWithTagColumns: createColumnarRecoveryCallback(dst, zerolog.Nop()),
	})
	require.NoError(t, err)
	require.NoError(t, dst.FlushAllAndWait(ctx))

	var tables []arrow.Table
	var tags []string
	require.NoError(t, filepath.Walk(replayRoot, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		f, err := os.Open(p)
		require.NoError(t, err)
		defer f.Close()
		pf, err := file.NewParquetReader(f)
		require.NoError(t, err)
		defer pf.Close()
		if v := pf.MetaData().KeyValueMetadata().FindValue("arc:tags"); v != nil {
			tags = append(tags, *v)
		} else {
			tags = append(tags, "")
		}
		fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		require.NoError(t, err)
		tbl, err := fr.ReadTable(ctx)
		require.NoError(t, err)
		t.Cleanup(tbl.Release)
		tables = append(tables, tbl)
		return nil
	}))
	require.NotEmpty(t, tables)
	return tables, tags
}

// column returns the replayed values of a column across the files, keyed by
// the row's "i" value, nil for NULL.
func column(t *testing.T, tables []arrow.Table, name string) map[int64]interface{} {
	t.Helper()
	out := map[int64]interface{}{}
	for _, tbl := range tables {
		idx := tbl.Schema().FieldIndices("i")
		require.Len(t, idx, 1)
		vidx := tbl.Schema().FieldIndices(name)
		tr := array.NewTableReader(tbl, -1)
		for tr.Next() {
			rec := tr.RecordBatch()
			ids := rec.Column(idx[0]).(*array.Int64)
			for r := 0; r < int(rec.NumRows()); r++ {
				if len(vidx) == 0 || rec.Column(vidx[0]).IsNull(r) {
					out[ids.Value(r)] = nil
					continue
				}
				switch c := rec.Column(vidx[0]).(type) {
				case *array.Int64:
					out[ids.Value(r)] = c.Value(r)
				case *array.Float64:
					out[ids.Value(r)] = c.Value(r)
				case *array.String:
					out[ids.Value(r)] = c.Value(r)
				case *array.Boolean:
					out[ids.Value(r)] = c.Value(r)
				}
			}
		}
		tr.Release()
	}
	return out
}

func TestTypedWriteNullsSurviveWALReplay(t *testing.T) {
	tables := replayed(t, 10000, func(b *ingest.ArrowBuffer) {
		batch := &ingest.TypedColumnBatch{
			Data: map[string]interface{}{
				"time": []int64{1700000000000000, 1700000001000000, 1700000002000000},
				"i":    []int64{0, 1, 2},
				"n":    []int64{10, 0, 30},
				"f":    []float64{1.5, 0, 3.5},
				"s":    []string{"a", "", "c"},
				"b":    []bool{true, false, true},
				"z":    []int64{0, 0, 0},
			},
			Validity: map[string][]bool{
				"z": {false, false, false},
				"n": {true, false, true},
				"f": {true, false, true},
				"s": {true, false, true},
				"b": {true, false, true},
			},
		}
		require.NoError(t, b.WriteTypedColumnarDirect(context.Background(), "test", "m", batch, 3))
	})
	for _, name := range []string{"n", "f", "s", "b"} {
		got := column(t, tables, name)
		require.Nil(t, got[1], "%s: a NULL came back from the WAL as a value", name)
		require.NotNil(t, got[0], name)
	}
	// The all-NULL typed column is not replayed as a string column.
	for _, tbl := range tables {
		for _, f := range tbl.Schema().Fields() {
			if f.Name == "z" {
				require.NotEqual(t, arrow.BinaryTypes.String.ID(), f.Type.ID(), "z replayed as a string column")
			}
		}
	}
}

// A typed column NULL in a whole entry and valued in the next (a sensor
// offline for one import chunk): after replay the column is numeric in every
// file, so sum() binds.
func TestTypedAllNullEntryThenValuesReplay(t *testing.T) {
	tables := replayed(t, 10000, func(b *ingest.ArrowBuffer) {
		for chunk := 0; chunk < 2; chunk++ {
			tm := []int64{1700000000000000 + int64(chunk)*10, 1700000000000001 + int64(chunk)*10}
			batch := &ingest.TypedColumnBatch{
				Data: map[string]interface{}{"time": tm, "i": []int64{int64(chunk * 2), int64(chunk*2 + 1)}, "z": []int64{7, 8}},
			}
			if chunk == 0 {
				batch.Validity = map[string][]bool{"z": {false, false}}
			}
			require.NoError(t, b.WriteTypedColumnarDirect(context.Background(), "test", "m", batch, 2))
		}
	})
	for _, tbl := range tables {
		for _, f := range tbl.Schema().Fields() {
			if f.Name == "z" {
				require.Equal(t, arrow.PrimitiveTypes.Int64.ID(), f.Type.ID(), "z is %s in a replayed file", f.Type)
			}
		}
	}
	got := column(t, tables, "z")
	require.Nil(t, got[0])
	require.EqualValues(t, 7, got[2])
}

// A column NULL across a whole recovery batch is absent from that batch's
// file, not an all-NULL string column next to files where it is a number
// (union_by_name would type it VARCHAR and sum() would not bind).
func TestSparseColumnAcrossReplayBatches(t *testing.T) {
	tables := replayed(t, 5, func(b *ingest.ArrowBuffer) {
		cols := map[string][]interface{}{"time": {}, "i": {}, "v": {}}
		for i := 0; i < 10; i++ {
			cols["time"] = append(cols["time"], int64(1700000000000000+i*1000000))
			cols["i"] = append(cols["i"], int64(i))
			if i < 5 {
				cols["v"] = append(cols["v"], int64(i))
			} else {
				cols["v"] = append(cols["v"], nil) // the second recovery batch has no v
			}
		}
		require.NoError(t, b.WriteColumnarRecord(context.Background(), "test", &models.ColumnarRecord{Measurement: "m", Columnar: true, Columns: cols}))
	})
	for _, tbl := range tables {
		for _, f := range tbl.Schema().Fields() {
			if f.Name == "v" {
				require.NotEqual(t, arrow.BinaryTypes.String.ID(), f.Type.ID(), "v replayed as a string column")
			}
		}
	}
	got := column(t, tables, "v")
	require.EqualValues(t, 3, got[3])
	require.Nil(t, got[7])
}

// Columns named like the WAL row's legacy target keys stay columns.
func TestWALReplayKeepsColumnsNamedLikeTargetKeys(t *testing.T) {
	tables := replayed(t, 10000, func(b *ingest.ArrowBuffer) {
		batch := &ingest.TypedColumnBatch{Data: map[string]interface{}{
			"time":        []int64{1700000000000000},
			"i":           []int64{0},
			"measurement": []string{"x"},
			"m":           []string{"y"},
			"database":    []string{"prod"},
		}}
		require.NoError(t, b.WriteTypedColumnarDirect(context.Background(), "test", "cpu", batch, 1))
	})
	require.Equal(t, "x", column(t, tables, "measurement")[0])
	require.Equal(t, "y", column(t, tables, "m")[0])
	require.Equal(t, "prod", column(t, tables, "database")[0])
}

// A line-protocol write (typed parse, typed WAL entry) replays with its
// values, types and NULL cells.
func TestLPWriteReplaysFromTypedWALEntry(t *testing.T) {
	tables := replayed(t, 10000, func(b *ingest.ArrowBuffer) {
		body := "m,host=a i=0i,f=1.5,s=\"x\",b=t 1700000000000000000\n" +
			"m,host=b i=1i,f=2 1700000001000000000\n" +
			"m i=2i,s=\"z\" 1700000002000000000\n"
		batches, rows, drops, err := ingest.NewLineProtocolParser().ParseLPWrite([]byte(body), "ns")
		require.NoError(t, err)
		require.Empty(t, drops)
		require.NoError(t, b.WriteTypedColumnarDirect(context.Background(), "test", "m", batches["m"], rows["m"]))
	})
	require.Equal(t, map[int64]interface{}{0: 1.5, 1: 2.0, 2: nil}, column(t, tables, "f"))
	require.Equal(t, map[int64]interface{}{0: "x", 1: nil, 2: "z"}, column(t, tables, "s"))
	require.Equal(t, map[int64]interface{}{0: true, 1: nil, 2: nil}, column(t, tables, "b"))
	require.Equal(t, map[int64]interface{}{0: "a", 1: "b", 2: nil}, column(t, tables, "host"))
}

// A Telegraf-style array of columnar maps is written item by item, each item's
// WAL entry its own client bytes; the WAL copies them, so the request buffer
// being reused after the write (fasthttp does) changes nothing, and replay
// gives the values, NULLs and tag columns back. A payload that takes the
// generic path (a row item in the array) replays too.
func TestMsgPackArrayReplaysFromItemEntries(t *testing.T) {
	item := func(m string, ids ...int64) map[string]interface{} {
		var tm, ii, host, v []interface{}
		for _, id := range ids {
			tm = append(tm, int64(1700000000000)+id) // ms, as Telegraf sends
			ii = append(ii, id)
			host = append(host, "h"+string(rune('a'+id)))
			if id%2 == 0 {
				v = append(v, float64(id)+0.5)
			} else {
				v = append(v, nil)
			}
		}
		return map[string]interface{}{"m": m, "tag_keys": []interface{}{"host"},
			"columns": map[string]interface{}{"time": tm, "i": ii, "host": host, "v": v}}
	}
	for _, tc := range []struct {
		name  string
		items []interface{}
		typed bool
	}{
		{"typed items", []interface{}{item("m", 0, 1), item("m", 2, 3)}, true},
		{"generic fallback", []interface{}{item("m", 0, 1), item("m", 2, 3), withoutTime(item("m", 4))}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tables, tags := replayedWithTags(t, 10000, func(b *ingest.ArrowBuffer) {
				payload, err := msgpack.Marshal(tc.items)
				require.NoError(t, err)
				dec := ingest.NewMessagePackDecoder(zerolog.Nop())
				dec.SetTypedDecodeEnabled(true)
				recs, err := dec.Decode(payload)
				require.NoError(t, err)
				_, isTyped := recs.([]interface{})[0].(*ingest.TypedColumnarRecord)
				require.Equal(t, tc.typed, isTyped)
				require.NoError(t, b.Write(context.Background(), "test", recs))
				for i := range payload {
					payload[i] = 0 // the request buffer is reused
				}
			})
			got := map[int64]interface{}{}
			for k, v := range column(t, tables, "v") {
				got[k] = v
			}
			require.Equal(t, 0.5, got[0])
			require.Nil(t, got[1])
			require.Equal(t, 2.5, got[2])
			require.Nil(t, got[3])
			// Raw columnar entries keep tag_keys; row entries (the generic
			// path's form for array items) do not.
			if tc.typed {
				for _, tg := range tags {
					require.Contains(t, tg, "host", "a replayed file lost its tag columns: %q", tags)
				}
			}
			hosts := column(t, tables, "host")
			for id, want := range map[int64]string{0: "ha", 1: "hb", 2: "hc", 3: "hd"} {
				require.Equal(t, want, hosts[id])
			}
		})
	}
}

// withoutTime drops an item's time column: the typed path refuses it (its
// generated time must reach the WAL), so the whole payload is generic.
func withoutTime(item map[string]interface{}) map[string]interface{} {
	cols := item["columns"].(map[string]interface{})
	delete(cols, "time")
	return item
}
