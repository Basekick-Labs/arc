package ingest

// #1284: a flush whose string or binary column holds more than Arrow's int32
// offsets can address is written as several records (row groups) of one
// file, not one record that overflows them. The limit is lowered so no test
// allocates gigabytes. The sync and async flush tests follow the approach of
// Efe Gökdemir's #1321.

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func lowerParquetChunkLimit(t *testing.T, n int64) {
	t.Helper()
	prev := maxParquetChunkColumnBytes
	maxParquetChunkColumnBytes = n
	t.Cleanup(func() { maxParquetChunkColumnBytes = prev })
}

func TestParquetChunksCutWhereAColumnWouldPassTheLimit(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "b", Type: arrow.BinaryTypes.Binary},
		{Name: "v", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	cols := func(s []string, b [][]byte) map[string]interface{} {
		return map[string]interface{}{"s": s, "b": b, "v": make([]float64, len(s))}
	}
	cases := []struct {
		name string
		s    []string
		b    [][]byte
		want []int
	}{
		{"fits", []string{"ab", "cd"}, [][]byte{{1}, {2}}, []int{0, 2}},
		{"exactly the limit fits", []string{"abc", "de"}, [][]byte{{1}, {2}}, []int{0, 2}},
		{"string column", []string{"abc", "de", "klmn", "o", "pqrst"}, make([][]byte, 5), []int{0, 2, 4, 5}},
		{"binary column", make([]string, 4), [][]byte{[]byte("abc"), []byte("def"), []byte("g"), []byte("hi")}, []int{0, 1, 3, 4}},
		{"either column cuts", []string{"abcd", "e", "f"}, [][]byte{[]byte("a"), []byte("bcdef"), []byte("g")}, []int{0, 1, 2, 3}},
		{"a value over the limit is alone", []string{"a", "bcdefgh", "i"}, make([][]byte, 3), []int{0, 1, 2, 3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parquetChunks(schema, cols(tc.s, tc.b), len(tc.s), 5)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

type parquetFileContents struct {
	rowGroups int
	metadata  arrow.Metadata
	rows      []map[string]interface{}
}

func readParquetContents(t *testing.T, data []byte) parquetFileContents {
	t.Helper()
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer pf.Close()
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	tbl, err := fr.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()
	out := parquetFileContents{rowGroups: pf.NumRowGroups(), metadata: tbl.Schema().Metadata()}
	out.rows = make([]map[string]interface{}, tbl.NumRows())
	for i := range out.rows {
		out.rows[i] = map[string]interface{}{}
	}
	for i, fld := range tbl.Schema().Fields() {
		row := 0
		for _, chunk := range tbl.Column(i).Data().Chunks() {
			for r := 0; r < chunk.Len(); r++ {
				var v interface{}
				if !chunk.IsNull(r) {
					switch c := chunk.(type) {
					case *array.String:
						v = c.Value(r)
					case *array.Binary:
						v = string(c.Value(r))
					case *array.Float64:
						v = c.Value(r)
					case *array.Boolean:
						v = c.Value(r)
					case *array.Timestamp:
						v = int64(c.Value(r))
					default:
						v = chunk.ValueStr(r)
					}
				}
				out.rows[row][fld.Name] = v
				row++
			}
		}
	}
	return out
}

// Every row, NULL and type survives the chunked write, NULL bitmaps are cut
// with their columns, and the file's metadata (tags, markers) is the same as
// a write of one record.
func TestChunkedParquetWriteRoundTrips(t *testing.T) {
	base := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC).UnixMicro()
	columns := func() map[string]interface{} {
		return map[string]interface{}{
			"time": []int64{base, base + 1, base + 2, base + 3, base + 4},
			"host": []string{"abc", "de", "klmn", "o", "pqrst"},
			"blob": [][]byte{[]byte("x"), nil, []byte("yz"), []byte("w"), []byte("v")},
			"v":    []float64{1, 2, 3, 4, 5},
			"ok":   []bool{true, false, true, false, true},
			"price": []decimal128.Num{
				decimal128.FromI64(100), decimal128.FromI64(250), decimal128.FromI64(-75),
				decimal128.FromI64(0), decimal128.FromI64(999),
			},
		}
	}
	// The cuts fall before rows 2 and 4. NULLs on both sides of the first.
	validity := map[string][]bool{
		"host":  {true, false, false, true, true},
		"blob":  {true, false, true, true, true},
		"v":     {true, true, false, true, true},
		"price": {true, true, false, true, true},
	}
	decimals := map[string]config.DecimalSpec{"price": {Precision: 10, Scale: 2}}
	write := func() []byte {
		w := NewArrowWriter(&config.IngestConfig{Compression: "snappy"}, zerolog.Nop())
		data, err := w.WriteParquetColumnar(context.Background(), "m", columns(), validity, []string{"host"}, true, decimals)
		require.NoError(t, err)
		return data
	}

	whole := readParquetContents(t, write())
	require.Equal(t, 1, whole.rowGroups, "a write within the limit is one row group, as before")

	lowerParquetChunkLimit(t, 5)
	chunked := readParquetContents(t, write())
	// host bytes (NULL counted as written): abc|de, klmn|o, pqrst.
	require.Equal(t, 3, chunked.rowGroups)
	require.Equal(t, whole.rows, chunked.rows)
	require.Equal(t, whole.metadata, chunked.metadata)
	require.Nil(t, chunked.rows[1]["host"])
	require.Nil(t, chunked.rows[2]["host"])
	require.Nil(t, chunked.rows[2]["v"])
	require.Nil(t, chunked.rows[2]["price"])
	require.Equal(t, "pqrst", chunked.rows[4]["host"])
	require.Equal(t, "w", chunked.rows[3]["blob"])
	require.Equal(t, "9.99", chunked.rows[4]["price"])
}

// flushedRowGroups flushes the rig and returns each file's row-group count
// and every row's host value by time.
func flushedRowGroups(t *testing.T, r *flushRig) ([]int, map[int64]string) {
	t.Helper()
	require.NoError(t, r.buf.FlushAllAndWait(context.Background()))
	var groups []int
	hosts := map[int64]string{}
	require.NoError(t, filepath.WalkDir(r.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		data, err := os.ReadFile(p)
		require.NoError(t, err)
		c := readParquetContents(t, data)
		groups = append(groups, c.rowGroups)
		for _, row := range c.rows {
			hosts[row["time"].(int64)] = row["host"].(string)
			// The binary column holds the same bytes as host.
			require.Equal(t, row["host"], row["blob"])
		}
		return nil
	}))
	return groups, hosts
}

var chunkTestHosts = []string{"abc", "de", "klmn", "o", "pqrst"}

func writeChunkTestRows(t *testing.T, b *ArrowBuffer, base int64) {
	t.Helper()
	for i, host := range chunkTestHosts {
		require.NoError(t, b.WriteColumnarRecord(context.Background(), "db", &models.ColumnarRecord{
			Measurement: "m",
			Columnar:    true,
			Columns: map[string][]interface{}{
				"time": {base + int64(i)},
				"v":    {float64(i)},
				"host": {host},
				"blob": {[]byte(host)},
			},
		}))
	}
}

func requireChunkTestRows(t *testing.T, base int64, groups []int, hosts map[int64]string) {
	t.Helper()
	require.Equal(t, []int{3}, groups, "one file, written as three row groups")
	require.Len(t, hosts, len(chunkTestHosts))
	for i, host := range chunkTestHosts {
		require.Equal(t, host, hosts[base+int64(i)])
	}
}

func TestSyncFlushWritesAnOversizedColumnAsRowGroups(t *testing.T) {
	lowerParquetChunkLimit(t, 5)
	r := newFlushRig(t, nil)
	base := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC).UnixMicro()
	writeChunkTestRows(t, r.buf, base)

	shard := r.buf.shards[0]
	shard.mu.Lock()
	err := r.buf.flushBufferLocked(context.Background(), shard, "db/m", "db", "m")
	shard.mu.Unlock()
	require.NoError(t, err)

	groups, hosts := flushedRowGroups(t, r)
	requireChunkTestRows(t, base, groups, hosts)
}

func TestAsyncFlushWritesAnOversizedColumnAsRowGroups(t *testing.T) {
	lowerParquetChunkLimit(t, 5)
	r := newFlushRig(t, nil)
	base := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC).UnixMicro()
	writeChunkTestRows(t, r.buf, base)

	shard := r.buf.shards[0]
	shard.mu.Lock()
	records := append([]interface{}(nil), shard.buffers["db/m"]...)
	count := shard.bufferRecordCounts["db/m"]
	delete(shard.buffers, "db/m")
	delete(shard.bufferRecordCounts, "db/m")
	delete(shard.bufferStartTimes, "db/m")
	delete(shard.bufferSchemas, "db/m")
	shard.mu.Unlock()
	require.NoError(t, r.buf.flushRecordsAsync(context.Background(), "db/m", "db", "m", records, count, collectWALHashes(records), nil))

	groups, hosts := flushedRowGroups(t, r)
	requireChunkTestRows(t, base, groups, hosts)
}

// A covered flush stays one file with one coverage: chunking happens inside
// the file, so the manifest sees exactly what it saw before.
func TestChunkedFlushKeepsOneCoveredFile(t *testing.T) {
	lowerParquetChunkLimit(t, 5)
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000, reg)
	store := &recordingBackend{}
	b.storage = store
	base := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC).UnixMicro()
	records := make([]interface{}, 0, len(chunkTestHosts))
	for i, host := range chunkTestHosts {
		records = append(records, &TypedColumnBatch{
			Data: map[string]interface{}{
				"time": []int64{base + int64(i)},
				"v":    []float64{float64(i)},
				"host": []string{host},
			},
			Coverage: []CoveragePiece{{Instance: 7, CSeq: uint64(i + 1)}},
		})
	}
	group := b.newFlushGroup("db/cpu", records, nil)
	require.NoError(t, b.flushRecordsAsync(context.Background(), "db/cpu", "db", "cpu", records, len(chunkTestHosts), nil, group))

	files := reg.snapshot()
	require.Len(t, files, 1)
	ranges, partial, exact := files[0].cov.FilePieces()
	require.True(t, exact)
	require.Empty(t, partial)
	require.Len(t, ranges, 1)
	require.Equal(t, uint64(1), ranges[0].Lo)
	require.Equal(t, uint64(5), ranges[0].Hi)
	require.Len(t, store.files, 1)
	for _, data := range store.files {
		require.Equal(t, 3, readParquetContents(t, data).rowGroups)
	}
}

// recordingBackend keeps the bytes of every file written to it.
type recordingBackend struct {
	mockStorageBackend
	mu    sync.Mutex
	files map[string][]byte
}

func (r *recordingBackend) Write(_ context.Context, path string, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.files == nil {
		r.files = map[string][]byte{}
	}
	r.files[path] = append([]byte(nil), data...)
	return nil
}

// A file written as several records sizes its buffer from the learned bytes
// per row: its arrays are built one record at a time, so they cannot clamp
// the hint (they clamped it to 64 KiB).
func TestParquetSizeHintWithoutArraysIsNotClamped(t *testing.T) {
	w := NewArrowWriter(&config.IngestConfig{Compression: "snappy"}, zerolog.Nop())
	w.fileBytesPerRow.Store("m", int64(100))
	require.Equal(t, 100*10_000+100*10_000/8+4096, w.parquetSizeHint("m", 10_000, nil))
	require.Equal(t, maxParquetSizeHint, w.parquetSizeHint("m", 10_000_000, nil))
}
