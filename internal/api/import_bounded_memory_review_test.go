package api

// Review findings on the chunked imports (#1070 follow-up).

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// oneByteReader returns one byte per Read: the worst case for a scan that
// re-reads what it already searched.
type oneByteReader struct{ r io.Reader }

func (o oneByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return o.r.Read(p)
}

// A long line is searched once: with a rescan per read, a 2 MB line read a
// byte at a time takes about 2*10^12 steps and never finishes here.
func TestForEachLineChunkLongLineIsLinear(t *testing.T) {
	line := append(bytes.Repeat([]byte("x"), 2<<20), '\n')
	done := make(chan error, 1)
	go func() {
		done <- forEachLineChunk(oneByteReader{bytes.NewReader(line)}, 1024, func([]byte) error { return nil })
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("a long line took quadratic time")
	}
}

func TestForEachLineChunkLineCap(t *testing.T) {
	setVar(t, &maxImportLineBytes, 1<<20)
	for _, tail := range []string{"", "\n"} { // at the end of the file, and before a newline
		data := append(bytes.Repeat([]byte("x"), 3<<20), tail...)
		err := forEachLineChunk(bytes.NewReader(data), 1024, func([]byte) error { return nil })
		require.ErrorIs(t, err, errImportLineTooLong, "tail %q", tail)
	}
}

// A decimal column is converted from any value, as before chunking: the
// first value in the file does not make it an integer column.
func TestLPImportDecimalColumnNotCoerced(t *testing.T) {
	setVar(t, &lpChunkBytes, 64)
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
		DecimalColumns: []string{"orders:price=10,2"},
	}, backend, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	h := NewImportHandler(zerolog.Nop())
	h.SetArrowBuffer(buf)
	ci := &chunkedImport{app: importApp(t, h), buf: buf, dir: dir}
	var lp strings.Builder
	lp.WriteString("orders price=10i 1700000000000000000\n")
	for i := 1; i < 10; i++ {
		fmt.Fprintf(&lp, "orders price=%d.55 %d\n", i, 1700000000000000000+int64(i)*1e9)
	}
	status, body := ci.post(t, "/api/v1/import/lp?db=d", []byte(lp.String()))
	require.Equal(t, http.StatusOK, status, body)
	require.NoError(t, buf.FlushAllAndWait(context.Background()))
	var sum float64
	walkParquet(t, dir, func(rec arrow.RecordBatch) {
		col := rec.Column(int(rec.Schema().FieldIndices("price")[0])).(*array.Decimal128)
		for i := 0; i < col.Len(); i++ {
			sum += col.Value(i).ToFloat64(2)
		}
	})
	require.InDelta(t, 10+45+9*0.55, sum, 0.001, "decimal values were truncated")
}

func walkParquet(t *testing.T, dir string, fn func(arrow.RecordBatch)) {
	t.Helper()
	require.NoError(t, filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(p, ".parquet") {
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
		tr := array.NewTableReader(tbl, -1)
		defer tr.Release()
		for tr.Next() {
			fn(tr.RecordBatch())
		}
		return nil
	}))
}

// A field named like a tag of other records shares the tag's column: a file
// that mixes them is refused before anything is written, as its write failed
// before chunking.
func TestLPImportTagFieldCollisionRefused(t *testing.T) {
	setVar(t, &lpChunkBytes, 64)
	ci := newChunkedImport(t, 1)
	var lp strings.Builder
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&lp, "cpu host=%di %d\n", i, 1700000000000000000+int64(i)*1e9)
	}
	lp.WriteString("cpu,host=a v=1i 1700000100000000000\n")
	status, body := ci.post(t, "/api/v1/import/lp?db=d", []byte(lp.String()))
	require.Equal(t, http.StatusBadRequest, status, body)
	require.Empty(t, ci.writes)
}

// A record with a field named like its own tag writes the field as
// <name>_value, chunk or not.
func TestLPImportFieldNamedLikeItsTag(t *testing.T) {
	setVar(t, &lpChunkBytes, 64)
	ci := newChunkedImport(t, 1)
	var lp strings.Builder
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&lp, "cpu,host=a host=%di %d\n", i, 1700000000000000000+int64(i)*1e9)
	}
	status, body := ci.post(t, "/api/v1/import/lp?db=d", []byte(lp.String()))
	require.Equal(t, http.StatusOK, status, body)
	_, types := ci.stored(t)
	require.Equal(t, map[string]bool{"int64": true}, types["host_value"])
	require.Equal(t, map[string]bool{"utf8": true}, types["host"])
}

// An LP import refused part-way across chunks names what it wrote.
func TestLPImportRefusedPartWayAcrossChunks(t *testing.T) {
	_, _ = captureRefusals(t)
	setVar(t, &lpChunkBytes, 64)
	ci := newChunkedImport(t, 1000)
	var calls int
	ci.buf.SetIngestGate(func() error {
		calls++
		if calls > 3 { // admission, then two writes
			return fencedErr(t)
		}
		return nil
	})
	var lp strings.Builder
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&lp, "cpu value=%d %d\nmem value=%d %d\n", i, 1700000000000000000+int64(i)*1e9, i, 1700000000000000000+int64(i)*1e9)
	}
	status, body := ci.post(t, "/api/v1/import/lp?db=d", []byte(lp.String()))
	require.Equal(t, http.StatusInternalServerError, status, body)
	require.EqualValues(t, ci.writes[0]+ci.writes[1], body["rows_written"])
	require.NotEmpty(t, body["written_measurements"])
	require.NotEmpty(t, body["stopped_at_measurement"])
}

// A decode error the validation pass cannot see (it reads the time column
// only) after rows were written is a failure part-way, not a rejected file.
func TestParquetDecodeErrorAfterWritesIsPartial(t *testing.T) {
	setVar(t, &parquetBatchRows, 5)
	sc := arrow.NewSchema([]arrow.Field{{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_us}, {Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	for i := 0; i < 10; i++ {
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1700000000000000 + int64(i)*1e6))
		b.Field(1).(*array.Int64Builder).Append(int64(i))
	}
	rec := b.NewRecord()
	var out bytes.Buffer
	w, err := pqarrow.NewFileWriter(sc, &out, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(5)), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())
	data := out.Bytes()
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, 2, pf.NumRowGroups())
	cc, err := pf.MetaData().RowGroup(1).ColumnChunk(1)
	require.NoError(t, err)
	off := cc.DataPageOffset()
	for i := int64(0); i < 8; i++ {
		data[off+i] = 0xFF // the second row group's v page header
	}
	ci := newChunkedImport(t, 1000)
	status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=m", data)
	require.Equal(t, http.StatusInternalServerError, status, body)
	require.EqualValues(t, 5, body["rows_written"])
}

// A wide CSV file is chunked by cells, not only rows.
func TestCSVImportChunksByCells(t *testing.T) {
	setVar(t, &csvChunkCells, 10) // 5 columns -> 2 rows
	ci := newChunkedImport(t, 1000)
	csv := "time,a,b,c,d\n"
	for i := 0; i < 6; i++ {
		csv += fmt.Sprintf("%d,1,2,3,4\n", 1700000000+i)
	}
	status, body := ci.post(t, "/api/v1/import/csv?db=d&measurement=m&time_format=epoch_s", []byte(csv))
	require.Equal(t, http.StatusOK, status, body)
	require.Equal(t, []int64{2, 2, 2}, ci.writes)
}

// A "file" field without a filename is a form value, not the upload.
func TestImportFileFieldWithoutFilename(t *testing.T) {
	ci := newChunkedImport(t, 10)
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	require.NoError(t, mw.WriteField("file", "cpu value=1 1700000000000000000\n"))
	require.NoError(t, mw.Close())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/import/lp?db=d", &b)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := ci.app.Test(req, -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestImportContentEncodingIdentityAccepted(t *testing.T) {
	ci := newChunkedImport(t, 10)
	body, ct := multipartUpload(t, []byte("cpu value=1 1700000000000000000\n"))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/import/lp?db=d", bytes.NewReader(body))
	req.Header.Set("Content-Type", ct)
	req.Header.Set("Content-Encoding", "identity")
	resp, err := ci.app.Test(req, -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// The in-place file slice in other body layouts: a part after the file, a
// preamble, bare LF line endings (verified, else copied: same bytes).
func TestImportFileBytesLayouts(t *testing.T) {
	build := func(preamble string, after bool, lf bool) ([]byte, string) {
		content := []byte("PAR1 file\r\nbytes")
		if lf {
			content = []byte("PAR1 file\nbytes")
		}
		var b bytes.Buffer
		mw := multipart.NewWriter(&b)
		fw, err := mw.CreateFormFile("file", "f.parquet")
		require.NoError(t, err)
		_, _ = fw.Write(content)
		if after {
			require.NoError(t, mw.WriteField("note", "after"))
		}
		require.NoError(t, mw.Close())
		raw := preamble + b.String()
		if lf {
			// Every framing line ends in a bare LF (the content holds no CR).
			raw = strings.ReplaceAll(raw, "\r\n", "\n")
		}
		return []byte(raw), mw.FormDataContentType()
	}
	for _, tc := range []struct {
		name      string
		preamble  string
		after, lf bool
	}{
		{"file then field", "", true, false},
		{"preamble", "ignored preamble\r\n", false, false},
		{"bare LF delimiters", "", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, ct := build(tc.preamble, tc.after, tc.lf)
			app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
			app.Post("/", func(c *fiber.Ctx) error {
				u, ierr := importUploadFrom(c)
				require.Nil(t, ierr)
				got, err := u.fileBytes()
				require.NoError(t, err)
				want := "PAR1 file\r\nbytes"
				if tc.lf {
					want = "PAR1 file\nbytes"
				}
				require.Equal(t, want, string(got))
				return nil
			})
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			req.Header.Set("Content-Type", ct)
			_, err := app.Test(req, -1)
			require.NoError(t, err)
		})
	}
}

// One import writes at a time on a node: a second waits for the first's
// write pass to end.
func TestImportsWriteOneAtATime(t *testing.T) {
	setVar(t, &csvChunkRows, 2)
	ci := newChunkedImport(t, 1000)
	release := make(chan struct{})
	var releaseOnce sync.Once
	// A failed assertion must not leave the first import holding the gate.
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var firstWrite, secondWrite atomic.Bool
	importWriteHook = func(rows int64) {
		if firstWrite.CompareAndSwap(false, true) {
			<-release // the first import holds its write pass open
			return
		}
		secondWrite.Store(true)
	}
	post := func(db string) chan int {
		done := make(chan int, 1)
		go func() {
			body, ct := multipartUpload(t, []byte("time,v\n1700000000,1\n1700000001,2\n"))
			req := httptest.NewRequest(http.MethodPost, "/api/v1/import/csv?db="+db+"&measurement=m&time_format=epoch_s", bytes.NewReader(body))
			req.Header.Set("Content-Type", ct)
			resp, err := ci.app.Test(req, -1)
			if err != nil {
				done <- 0
				return
			}
			done <- resp.StatusCode
		}()
		return done
	}
	a := post("a")
	require.Eventually(t, firstWrite.Load, 5*time.Second, time.Millisecond)
	b := post("b")
	time.Sleep(200 * time.Millisecond)
	require.False(t, secondWrite.Load(), "a second import wrote while the first was writing")
	releaseOnce.Do(func() { close(release) })
	require.Equal(t, http.StatusOK, <-a)
	require.Equal(t, http.StatusOK, <-b)
	require.True(t, secondWrite.Load())
}

// A body cut short is a bad request, whatever the format.
func TestImportBodyCutShort(t *testing.T) {
	for _, tc := range []struct{ path, content string }{
		{"/api/v1/import/csv?db=d&measurement=m&time_format=epoch_s", "time,v\n1700000000,1\n1700000001,2\n"},
		{"/api/v1/import/lp?db=d", "cpu value=1 1700000000000000000\n"},
	} {
		ci := newChunkedImport(t, 10)
		body, ct := multipartUpload(t, []byte(tc.content))
		cut := body[:bytes.LastIndex(body, []byte("\r\n--"))] // no closing delimiter
		req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(cut))
		req.Header.Set("Content-Type", ct)
		resp, err := ci.app.Test(req, -1)
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, tc.path)
		require.Empty(t, ci.writes)
	}
}
