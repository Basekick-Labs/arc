package ingest

// What a flush writes: columns never stored (empty name, "_" prefix) are
// skipped safely, and the flush path's copies (merge, sort, Arrow, Parquet)
// write the rows a buffer holds, in sort order, with their types and NULLs.

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// flushRig is a buffer over a local directory.
type flushRig struct {
	buf *ArrowBuffer
	dir string
}

func newFlushRig(t *testing.T, cfg *config.IngestConfig) *flushRig {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	require.NoError(t, err)
	if cfg == nil {
		cfg = &config.IngestConfig{}
	}
	cfg.MaxBufferSize, cfg.MaxBufferAgeMS, cfg.Compression = 1_000_000, 600000, "snappy"
	cfg.FlushWorkers, cfg.FlushQueueSize, cfg.ShardCount, cfg.DataPageVersion = 1, 8, 1, "2.0"
	b := NewArrowBuffer(cfg, backend, zerolog.Nop())
	t.Cleanup(func() { b.Close() })
	return &flushRig{buf: b, dir: dir}
}

// flushedFile is one Parquet file: its schema's column types and its rows
// (column -> values in file order, nil for NULL).
type flushedFile struct {
	path  string
	types map[string]string
	cols  map[string][]interface{}
}

// files flushes and reads every Parquet file under the rig, sorted by path.
func (r *flushRig) files(t *testing.T) []flushedFile {
	t.Helper()
	require.NoError(t, r.buf.FlushAllAndWait(context.Background()))
	var out []flushedFile
	require.NoError(t, filepath.WalkDir(r.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		f, err := os.Open(p)
		require.NoError(t, err)
		defer f.Close()
		pf, err := file.NewParquetReader(f)
		require.NoError(t, err)
		defer pf.Close()
		fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		require.NoError(t, err)
		tbl, err := fr.ReadTable(context.Background())
		require.NoError(t, err)
		defer tbl.Release()
		ff := flushedFile{path: p, types: map[string]string{}, cols: map[string][]interface{}{}}
		for i, fld := range tbl.Schema().Fields() {
			ff.types[fld.Name] = fld.Type.String()
			for _, chunk := range tbl.Column(i).Data().Chunks() {
				for r := 0; r < chunk.Len(); r++ {
					if chunk.IsNull(r) {
						ff.cols[fld.Name] = append(ff.cols[fld.Name], nil)
						continue
					}
					var v interface{}
					switch c := chunk.(type) {
					case *array.Int64:
						v = c.Value(r)
					case *array.Float64:
						v = c.Value(r)
					case *array.String:
						v = c.Value(r)
					case *array.Boolean:
						v = c.Value(r)
					case *array.Timestamp:
						v = int64(c.Value(r))
					case *array.Decimal128:
						v = c.ValueStr(r)
					default:
						v = chunk.ValueStr(r)
					}
					ff.cols[fld.Name] = append(ff.cols[fld.Name], v)
				}
			}
		}
		out = append(out, ff)
		return nil
	}))
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out
}

// A column never stored (an empty name, or "_"-prefixed) whose type differs
// between two writes of one buffer used to crash the flush (the merge typed
// it by the first batch); an empty name crashed every flush (the schema read
// its first byte). Both are skipped; the stored columns are written.
func TestFlushSkipsUnstoredColumns(t *testing.T) {
	for _, name := range []string{"_x", ""} {
		for _, second := range []interface{}{int64(2), 2.5} {
			r := newFlushRig(t, nil)
			ctx := context.Background()
			require.NoError(t, r.buf.WriteColumnarRecord(ctx, "d", &models.ColumnarRecord{Measurement: "m", Columnar: true,
				Columns: map[string][]interface{}{"time": {int64(1700000000000000)}, "v": {1.0}, name: {int64(1)}}}))
			require.NoError(t, r.buf.WriteColumnarRecord(ctx, "d", &models.ColumnarRecord{Measurement: "m", Columnar: true,
				Columns: map[string][]interface{}{"time": {int64(1700000000000001)}, "v": {2.0}, name: {second}}}))
			files := r.files(t)
			require.Len(t, files, 1, "name %q", name)
			require.Equal(t, []interface{}{1.0, 2.0}, files[0].cols["v"], "name %q", name)
			_, has := files[0].types[name]
			require.False(t, has, "name %q was stored", name)
		}
	}
}

type flushRow map[string]interface{}

// flushInput writes batches of typed rows and returns the rows written.
// Times interleave across batches (and, with shuffle, within one), so the
// flush has to sort; every fourth "n" and every third "s" is NULL.
func flushInput(t *testing.T, r *flushRig, batches, rows int, hours int, shuffle bool) []flushRow {
	t.Helper()
	base := int64(1700000000000000) // µs
	base -= base % microPerHour
	var all []flushRow
	for k := 0; k < batches; k++ {
		times := make([]int64, rows)
		hosts := make([]string, rows)
		vs := make([]float64, rows)
		ns := make([]int64, rows)
		ss := make([]string, rows)
		oks := make([]bool, rows)
		nValid := make([]bool, rows)
		sValid := make([]bool, rows)
		for j := 0; j < rows; j++ {
			jj := j
			if shuffle {
				jj = (j*7 + 3) % rows
			}
			seq := int64(k + batches*jj)
			times[j] = base + (seq%int64(hours))*microPerHour + seq*1000
			hosts[j] = []string{"b", "a", "c"}[(k+j)%3]
			vs[j] = float64(seq) + 0.5
			ns[j] = seq
			ss[j] = "s" + string(rune('a'+seq%26))
			oks[j] = seq%2 == 0
			nValid[j] = seq%4 != 0
			sValid[j] = seq%3 != 0
			row := flushRow{"time": times[j], "host": hosts[j], "v": vs[j], "n": ns[j], "s": ss[j], "ok": oks[j]}
			if !nValid[j] {
				row["n"] = nil
			}
			if !sValid[j] {
				row["s"] = nil
			}
			all = append(all, row)
		}
		batch := &TypedColumnBatch{
			Data:       map[string]interface{}{"time": times, "host": hosts, "v": vs, "n": ns, "s": ss, "ok": oks},
			Validity:   map[string][]bool{"n": nValid, "s": sValid},
			TagColumns: []string{"host"},
		}
		require.NoError(t, r.buf.WriteTypedColumnarDirect(context.Background(), "d", "m", batch, rows))
	}
	return all
}

// expectFiles is what the flush must write: one file per hour, its rows
// sorted by keys.
func expectFiles(rows []flushRow, keys []string) map[int64][]flushRow {
	out := map[int64][]flushRow{}
	for _, row := range rows {
		h := HourBucketID(row["time"].(int64))
		out[h] = append(out[h], row)
	}
	for _, rs := range out {
		sort.SliceStable(rs, func(i, j int) bool {
			for _, k := range keys {
				a, b := rs[i][k], rs[j][k]
				switch av := a.(type) {
				case int64:
					if av != b.(int64) {
						return av < b.(int64)
					}
				case string:
					if av != b.(string) {
						return av < b.(string)
					}
				}
			}
			return false
		})
	}
	return out
}

func checkFlushedFiles(t *testing.T, files []flushedFile, want map[int64][]flushRow) {
	t.Helper()
	require.Len(t, files, len(want))
	for _, f := range files {
		times := f.cols["time"]
		require.NotEmpty(t, times)
		rows := want[HourBucketID(times[0].(int64))]
		require.Len(t, times, len(rows), f.path)
		for col := range rows[0] {
			got := f.cols[col]
			exp := make([]interface{}, len(rows))
			for i, row := range rows {
				exp[i] = row[col]
			}
			require.Equal(t, exp, got, "%s column %s", f.path, col)
		}
	}
	for _, f := range files {
		require.Equal(t, "float64", f.types["v"])
		require.Equal(t, "int64", f.types["n"])
		require.Equal(t, "utf8", f.types["s"])
		require.Equal(t, "bool", f.types["ok"])
		require.Contains(t, f.types["time"], "timestamp")
	}
}

func TestFlushWritesBufferedRowsInOrder(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		batches, rows, hours int
		shuffle              bool
		sortKeys             []string
	}{
		{"one batch sorted", 1, 200, 1, false, nil},
		{"one batch unsorted", 1, 200, 1, true, nil},
		{"many batches interleaved", 8, 50, 1, false, nil},
		{"many batches shuffled", 8, 50, 1, true, nil},
		{"many batches three hours", 8, 50, 3, true, nil},
		{"one batch three hours", 1, 300, 3, true, nil},
		{"sort by host then time", 8, 50, 1, true, []string{"m:host,time"}},
		{"sort by host then time three hours", 8, 50, 3, true, []string{"m:host,time"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newFlushRig(t, &config.IngestConfig{SortKeys: tc.sortKeys})
			rows := flushInput(t, r, tc.batches, tc.rows, tc.hours, tc.shuffle)
			keys := []string{"time"}
			if tc.sortKeys != nil {
				keys = []string{"host", "time"}
			}
			checkFlushedFiles(t, r.files(t), expectFiles(rows, keys))
			// Several batches of one hour, sorted by time alone: the flush
			// merged them in sorted order.
			sorted := tc.batches > 1 && tc.hours == 1 && tc.sortKeys == nil
			require.Equal(t, sorted, r.buf.sortedMerges.Load() > 0, "sorted merge used")
		})
	}
}

// One batch keeps its columns as written, unstored ones included: the
// schema skips them.
func TestFlushOneBatchSkipsUnstoredColumn(t *testing.T) {
	r := newFlushRig(t, nil)
	require.NoError(t, r.buf.WriteColumnarRecord(context.Background(), "d", &models.ColumnarRecord{Measurement: "m", Columnar: true,
		Columns: map[string][]interface{}{"time": {int64(1700000000000000)}, "v": {1.0}, "_x": {int64(1)}}}))
	files := r.files(t)
	require.Len(t, files, 1)
	_, has := files[0].types["_x"]
	require.False(t, has, "_x was stored")
	require.Equal(t, []interface{}{1.0}, files[0].cols["v"])
}

func TestSyncFlushSplitsStringColumnsAcrossFiles(t *testing.T) {
	r := newFlushRig(t, nil)
	ctx := context.Background()
	base := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC).UnixMicro()
	values := []string{"abc", "de", "klmn", "o", "pqrst"}
	for i, value := range values {
		record := &models.ColumnarRecord{
			Measurement: "m",
			Columnar:    true,
			Columns: map[string][]interface{}{
				"time":  {base + int64(i)},
				"value": {int64(i)},
				"text":  {value},
			},
		}
		require.NoError(t, r.buf.WriteColumnarRecord(ctx, "db", record))
	}

	shard := r.buf.shards[0]
	shard.mu.Lock()
	err := r.buf.flushBufferLockedWithStringLimit(ctx, shard, "db/m", "db", "m", 5)
	shard.mu.Unlock()
	require.NoError(t, err)

	files := r.files(t)
	require.Len(t, files, 3, "the five rows should be grouped into three bounded string batches")
	got := make(map[int64]string)
	for _, file := range files {
		textValues := file.cols["text"]
		var textBytes int
		for _, value := range textValues {
			textBytes += len(value.(string))
		}
		require.LessOrEqual(t, textBytes, 5)
		times := file.cols["time"]
		for i, value := range textValues {
			got[times[i].(int64)] = value.(string)
		}
	}
	require.Len(t, got, len(values))
	for i, value := range values {
		require.Equal(t, value, got[base+int64(i)])
	}
}

func writerColumns(rows int) map[string]interface{} {
	times, vs := make([]int64, rows), make([]float64, rows)
	for i := range times {
		times[i], vs[i] = 1700000000000000+int64(i), float64(i)*1.37
	}
	return map[string]interface{}{"time": times, "v": vs}
}

// The output buffer is sized from the measurement's last large file, and
// never past the write's own column bytes: a 1-row file (mostly footer) once
// sized the next 200,000-row file at 184 times its size.
func TestParquetSizeHintBounded(t *testing.T) {
	w := NewArrowWriter(&config.IngestConfig{Compression: "snappy", DataPageVersion: "2.0"}, zerolog.Nop())
	ctx := context.Background()
	write := func(rows int) []byte {
		data, _, err := w.writeParquetColumnarWithSchema(ctx, "m", writerColumns(rows), nil, nil, false, nil)
		require.NoError(t, err)
		return data
	}
	write(1)
	_, learned := w.fileBytesPerRow.Load("m")
	require.False(t, learned, "a 1-row file sized the next one")
	raw := 200000 * 16
	big := write(200000)
	require.LessOrEqual(t, cap(big), 2*len(big)+raw, "first large file")
	next := write(200000)
	// The hint: the last file's bytes per row (rounded up), times rows,
	// plus 1/8 and 4 KiB; plus allocator rounding.
	perRow := len(big)/200000 + 1
	hint := perRow*200000 + perRow*200000/8 + 4096
	require.LessOrEqual(t, cap(next), hint+64<<10, "sized from the last large file")
	require.GreaterOrEqual(t, cap(next), len(next))
	require.LessOrEqual(t, cap(next), raw+64<<10)

	// A hint learned from wide rows never reserves past a narrow write's own
	// bytes.
	wide := writerColumns(2000)
	long := make([]string, 2000)
	for i := range long {
		long[i] = strings.Repeat(string(rune('a'+i%26)), 4000) + fmt.Sprint(i)
	}
	wide["s"] = long
	_, _, err := w.writeParquetColumnarWithSchema(ctx, "m", wide, nil, nil, false, nil)
	require.NoError(t, err)
	narrow := write(200000)
	require.LessOrEqual(t, cap(narrow), raw+64<<10+16<<10, "narrow write after wide rows")
}

// A NULL bitmap of a fixed-width column: empty means all valid (as for the
// builders), another length is an error, not NULLs past its end.
func TestFixedWidthNullBitmapContract(t *testing.T) {
	w := NewArrowWriter(&config.IngestConfig{Compression: "snappy", DataPageVersion: "2.0"}, zerolog.Nop())
	ctx := context.Background()
	cols := writerColumns(3)
	_, _, err := w.writeParquetColumnarWithSchema(ctx, "m", cols, map[string][]bool{"v": {}}, nil, false, nil)
	require.NoError(t, err)
	_, _, err = w.writeParquetColumnarWithSchema(ctx, "m", cols, map[string][]bool{"v": {true}}, nil, false, nil)
	require.ErrorContains(t, err, "NULL bitmap has 1 entries for 3 values")

	data, err := fixedWidthArrayData(arrow.PrimitiveTypes.Float64, arrow.Float64Traits.CastToBytes([]float64{1, 2, 3}), 3, []bool{})
	require.NoError(t, err)
	require.Zero(t, data.NullN())
	data, err = fixedWidthArrayData(arrow.PrimitiveTypes.Float64, arrow.Float64Traits.CastToBytes([]float64{1, 2, 3}), 3, []bool{true, false, true})
	require.NoError(t, err)
	require.Equal(t, 1, data.NullN())
}
