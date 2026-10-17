package api

// Hour grouping (#1070 follow-up): an unsorted file used to make one file per
// hour per chunk; grouped, it makes about one per hour per window, and stores
// the same rows.

import (
	"context"
	"fmt"
	"io/fs"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
)

const groupRows, groupHours = 20000, 50

// shuffledHours returns, per row, an hour offset in a shuffled order.
func shuffledHours() []int {
	r := rand.New(rand.NewSource(7))
	hours := make([]int, groupRows)
	for i := range hours {
		hours[i] = i % groupHours
	}
	r.Shuffle(len(hours), func(i, j int) { hours[i], hours[j] = hours[j], hours[i] })
	return hours
}

func shuffledLP() []byte {
	var b strings.Builder
	for i, h := range shuffledHours() {
		fmt.Fprintf(&b, "cpu,host=h%d v=%di %d\n", i%3, i, (int64(1700006400)+int64(h)*3600+int64(i%3000))*1e9)
	}
	return []byte(b.String())
}

func shuffledCSV() []byte {
	var b strings.Builder
	b.WriteString("time,host,v\n")
	for i, h := range shuffledHours() {
		fmt.Fprintf(&b, "%d,h%d,%d\n", int64(1700006400)+int64(h)*3600+int64(i%3000), i%3, i)
	}
	return []byte(b.String())
}

func shuffledParquet(t *testing.T) []byte {
	sc := arrow.NewSchema([]arrow.Field{{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_us}, {Name: "host", Type: arrow.BinaryTypes.String}, {Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	bld := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer bld.Release()
	for i, h := range shuffledHours() {
		bld.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp((int64(1700006400) + int64(h)*3600 + int64(i%3000)) * 1e6))
		bld.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("h%d", i%3))
		bld.Field(2).(*array.Int64Builder).Append(int64(i))
	}
	rec := bld.NewRecord()
	defer rec.Release()
	var out strings.Builder
	w, err := pqarrow.NewFileWriter(sc, &out, nil, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())
	return []byte(out.String())
}

// stored returns the number of Parquet files and, per hour directory, the
// row count and the sum of v.
func storedByHour(t *testing.T, ci *chunkedImport) (int, map[string][2]int64) {
	t.Helper()
	require.NoError(t, ci.buf.FlushAllAndWait(context.Background()))
	files := 0
	per := map[string][2]int64{}
	require.NoError(t, filepath.WalkDir(ci.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		files++
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
		hour, err := filepath.Rel(ci.dir, filepath.Dir(p))
		require.NoError(t, err)
		agg := per[hour]
		tr := array.NewTableReader(tbl, -1)
		defer tr.Release()
		vi := tbl.Schema().FieldIndices("v")[0]
		for tr.Next() {
			rec := tr.RecordBatch()
			col := rec.Column(vi).(*array.Int64)
			for i := 0; i < col.Len(); i++ {
				agg[0]++
				agg[1] += col.Value(i)
			}
		}
		per[hour] = agg
		return nil
	}))
	return files, per
}

func TestImportGroupsUnsortedFileByHour(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		content    func(t *testing.T) []byte
	}{
		{"lp", "/api/v1/import/lp?db=d", func(*testing.T) []byte { return shuffledLP() }},
		{"csv", "/api/v1/import/csv?db=d&measurement=cpu&time_format=epoch_s", func(*testing.T) []byte { return shuffledCSV() }},
		{"parquet", "/api/v1/import/parquet?db=d&measurement=cpu", shuffledParquet},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setVar(t, &lpChunkBytes, 16<<10)
			setVar(t, &csvChunkRows, 400)
			setVar(t, &parquetBatchRows, 400)
			content := tc.content(t)

			chunked := newChunkedImport(t, 200) // budget 0: each ~400-row chunk flushes, one file per hour it touches
			status, body := chunked.post(t, tc.path, content)
			require.Equal(t, http.StatusOK, status, body)
			chunkedFiles, chunkedRows := storedByHour(t, chunked)

			grouped := newChunkedImport(t, 200)
			setVar(t, &importGroupBudgetOverride, 64<<20)
			status, body = grouped.post(t, tc.path, content)
			require.Equal(t, http.StatusOK, status, body)
			groupedFiles, groupedRows := storedByHour(t, grouped)

			t.Logf("%s: %d hours, files per chunk %d, grouped %d", tc.name, groupHours, chunkedFiles, groupedFiles)
			require.Greater(t, chunkedFiles, 5*groupHours, "the ungrouped import should write about one file per hour per chunk")
			require.LessOrEqual(t, groupedFiles, groupHours+2)
			require.Equal(t, chunkedRows, groupedRows)
		})
	}
}

// The grouping budget is carved out of the import's allowance: the pacing
// limit leaves room for the rows the grouper holds.
func TestImportWriterCarvesGroupBudgetFromAllowance(t *testing.T) {
	ci := newChunkedImport(t, 1000)
	setVar(t, &importAllowanceOverride, 100<<20)
	setVar(t, &importGroupBudgetOverride, -1)
	w, err := newImportWriter(context.Background(), ci.buf, "d")
	require.NoError(t, err)
	defer w.close()
	require.Equal(t, ci.buf.BufferedBytes()+50<<20, w.limit)
}

// Several windows: a budget of about three chunks fills, writes, keeps
// going, and the last window is written at the end; the rows stored are
// those of the ungrouped import, hour by hour.
func TestImportGroupingAcrossWindows(t *testing.T) {
	setVar(t, &csvChunkRows, 400)
	content := shuffledCSV()
	path := "/api/v1/import/csv?db=d&measurement=cpu&time_format=epoch_s"

	whole := newChunkedImport(t, 1) // every group write flushes as one file
	status, body := whole.post(t, path, content)
	require.Equal(t, http.StatusOK, status, body)
	_, want := storedByHour(t, whole)

	ci := newChunkedImport(t, 1)
	setVar(t, &importGroupBudgetOverride, 40<<10) // ~3 chunks of 400 rows
	status, body = ci.post(t, path, content)
	require.Equal(t, http.StatusOK, status, body)
	files, got := storedByHour(t, ci)
	windows := (groupRows/400 + 2) / 3
	t.Logf("%d writes, %d files, about %d windows of %d hours", len(ci.writes), files, windows, groupHours)
	require.Greater(t, len(ci.writes), 3*groupHours, "fewer writes than several windows make")
	require.LessOrEqual(t, files, (windows+2)*groupHours)
	require.Equal(t, want, got)
}

// A failure in a later window answers rows_written = exactly the rows the
// earlier windows wrote, and nothing else is stored.
func TestImportGroupingPartialCountsWrittenRows(t *testing.T) {
	_, _ = captureRefusals(t)
	setVar(t, &csvChunkRows, 400)
	ci := newChunkedImport(t, 1)
	setVar(t, &importGroupBudgetOverride, 40<<10)
	var calls int
	ci.buf.SetIngestGate(func() error {
		calls++
		if calls > 120 { // admission, then the first two windows' group writes, roughly
			return fencedErr(t)
		}
		return nil
	})
	status, body := ci.post(t, "/api/v1/import/csv?db=d&measurement=cpu&time_format=epoch_s", shuffledCSV())
	require.Equal(t, http.StatusInternalServerError, status, body)
	var written int64
	for _, n := range ci.writes {
		written += n
	}
	require.Positive(t, written)
	require.Less(t, written, int64(groupRows))
	require.EqualValues(t, written, body["rows_written"])
	_, stored := storedByHour(t, ci)
	var total int64
	for _, agg := range stored {
		total += agg[0]
	}
	require.Equal(t, written, total, "rows were stored that rows_written does not count, or the reverse")
}
