package api

// Bulk imports are read and written in chunks so their memory does not grow
// with the file (#1070 follow-up): a validation pass refuses a bad file before
// anything is written, the write pass types every chunk as the whole file,
// and the writes are paced to the flushes.

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// chunkedImport is an import handler over a buffer that writes to dir.
type chunkedImport struct {
	app    *fiber.App
	buf    *ingest.ArrowBuffer
	dir    string
	writes []int64 // rows per write
}

func newChunkedImport(t *testing.T, maxBufferSize int) *chunkedImport {
	t.Helper()
	// Every chunk written as it comes (no hour grouping), so these tests
	// see the chunking itself; grouping tests set their own budget.
	setVar(t, &importGroupBudgetOverride, 0)
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: maxBufferSize, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 2, FlushQueueSize: 8, ShardCount: 2, DataPageVersion: "2.0",
	}, backend, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	h := NewImportHandler(zerolog.Nop())
	h.SetArrowBuffer(buf)
	ci := &chunkedImport{app: importApp(t, h), buf: buf, dir: dir}
	importWriteHook = func(rows int64) { ci.writes = append(ci.writes, rows) }
	t.Cleanup(func() { importWriteHook = nil })
	return ci
}

func setVar[T any](t *testing.T, p *T, v T) {
	old := *p
	*p = v
	t.Cleanup(func() { *p = old })
}

func (ci *chunkedImport) post(t *testing.T, path string, content []byte) (int, map[string]any) {
	t.Helper()
	body, ct := multipartUpload(t, content)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", ct)
	resp, err := ci.app.Test(req, -1)
	require.NoError(t, err)
	out, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(out, &m)
	return resp.StatusCode, m
}

// stored flushes and reads back every Parquet file: total rows, and per
// column the set of Arrow types it was written with.
func (ci *chunkedImport) stored(t *testing.T) (int64, map[string]map[string]bool) {
	t.Helper()
	require.NoError(t, ci.buf.FlushAllAndWait(context.Background()))
	var rows int64
	types := map[string]map[string]bool{}
	require.NoError(t, filepath.WalkDir(ci.dir, func(p string, d fs.DirEntry, err error) error {
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
		sc, err := fr.Schema()
		require.NoError(t, err)
		for _, fld := range sc.Fields() {
			if types[fld.Name] == nil {
				types[fld.Name] = map[string]bool{}
			}
			types[fld.Name][fld.Type.String()] = true
		}
		rows += pf.NumRows()
		return nil
	}))
	return rows, types
}

func maxOf(xs []int64) int64 {
	var m int64
	for _, x := range xs {
		m = max(m, x)
	}
	return m
}

func TestForEachLineChunk(t *testing.T) {
	data := []byte("a\nbb\nccc\n" + strings.Repeat("x", 40) + "\nlast")
	var got bytes.Buffer
	var chunks []string
	require.NoError(t, forEachLineChunk(bytes.NewReader(data), 4, func(c []byte) error {
		chunks = append(chunks, string(c))
		got.Write(c)
		return nil
	}))
	require.Equal(t, string(data), got.String())
	for _, c := range chunks[:len(chunks)-1] {
		require.True(t, strings.HasSuffix(c, "\n"), "chunk %q does not end a line", c)
	}
	require.Contains(t, chunks, strings.Repeat("x", 40)+"\n", "a line longer than the target is one chunk")
	require.Equal(t, "last", chunks[len(chunks)-1])
}

// An LP file is written in chunks, and a field's type is the one its first
// value in the FILE gives it: an integer field stays an integer in every file
// though a later chunk starts with a float.
func TestLPImportChunksKeepFileTypes(t *testing.T) {
	setVar(t, &lpChunkBytes, 128)
	ci := newChunkedImport(t, 5)
	var lp strings.Builder
	for i := 0; i < 60; i++ {
		v := fmt.Sprintf("%di", i)
		if i%7 == 3 {
			v = fmt.Sprintf("%d.5", i)
		}
		fmt.Fprintf(&lp, "cpu,host=h%d value=%s %d\n", i%3, v, 1700000000000000000+int64(i)*1e9)
	}
	status, body := ci.post(t, "/api/v1/import/lp?db=d", []byte(lp.String()))
	require.Equal(t, http.StatusOK, status, body)
	require.Greater(t, len(ci.writes), 3, "not written in chunks")
	require.Less(t, maxOf(ci.writes), int64(60))
	rows, types := ci.stored(t)
	require.EqualValues(t, 60, rows)
	require.Equal(t, map[string]bool{"int64": true}, types["value"], "the field was typed per chunk")
}

// A CSV column is typed over the whole file: a column whose last cell is not
// a number is a string in every file, and a column empty in one chunk is
// NULLs of its type there, not strings.
func TestCSVImportChunksKeepFileTypes(t *testing.T) {
	setVar(t, &csvChunkRows, 2)
	ci := newChunkedImport(t, 1)
	csv := "time,a,b\n1700000000,1,1\n1700000001,2,2\n1700000002,3,\n1700000003,0123A,\n1700000004,5,5\n"
	status, body := ci.post(t, "/api/v1/import/csv?db=d&measurement=m&time_format=epoch_s", []byte(csv))
	require.Equal(t, http.StatusOK, status, body)
	require.LessOrEqual(t, maxOf(ci.writes), int64(2))
	require.Len(t, ci.writes, 3)
	rows, types := ci.stored(t)
	require.EqualValues(t, 5, rows)
	require.Equal(t, map[string]bool{"utf8": true}, types["a"])
	require.Equal(t, map[string]bool{"int64": true}, types["b"])
	result := body["result"].(map[string]any)
	require.EqualValues(t, 5, result["rows_imported"])
	require.EqualValues(t, 1, result["partitions_created"])
}

// The answer of a chunked import is the answer of the whole file at once.
func TestCSVImportResultIndependentOfChunking(t *testing.T) {
	var csv strings.Builder
	csv.WriteString("time,v\n")
	for i := 0; i < 50; i++ {
		fmt.Fprintf(&csv, "%d,%d\n", 1700000000+i*1800, i) // 25 hours
	}
	answer := func(rows int) map[string]any {
		setVar(t, &csvChunkRows, rows)
		ci := newChunkedImport(t, 1000)
		status, body := ci.post(t, "/api/v1/import/csv?db=d&measurement=m&time_format=epoch_s", []byte(csv.String()))
		require.Equal(t, http.StatusOK, status, body)
		r := body["result"].(map[string]any)
		delete(r, "duration_ms")
		return r
	}
	require.Equal(t, answer(1_000_000), answer(7))
}

func parquetFile(t *testing.T, rows int, nullTimeAt int) []byte {
	t.Helper()
	sc := arrow.NewSchema([]arrow.Field{{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true}, {Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	for i := 0; i < rows; i++ {
		if i == nullTimeAt {
			b.Field(0).(*array.TimestampBuilder).AppendNull()
		} else {
			b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1700000000000000 + int64(i)*1e6))
		}
		b.Field(1).(*array.Int64Builder).Append(int64(i))
	}
	rec := b.NewRecord()
	defer rec.Release()
	var out bytes.Buffer
	w, err := pqarrow.NewFileWriter(sc, &out, nil, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, w.Write(rec)) // one row group
	require.NoError(t, w.Close())
	return out.Bytes()
}

func TestParquetImportReadsInBatches(t *testing.T) {
	setVar(t, &parquetBatchRows, 4)
	ci := newChunkedImport(t, 1)
	status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=m", parquetFile(t, 10, -1))
	require.Equal(t, http.StatusOK, status, body)
	require.LessOrEqual(t, maxOf(ci.writes), int64(4))
	require.Len(t, ci.writes, 3)
	rows, _ := ci.stored(t)
	require.EqualValues(t, 10, rows)
}

// What the write pass would fail on in its LAST chunk is refused before
// anything is written.
func TestImportRefusesBadLastChunkBeforeWriting(t *testing.T) {
	setVar(t, &lpChunkBytes, 64)
	setVar(t, &csvChunkRows, 2)
	setVar(t, &parquetBatchRows, 4)
	var lp strings.Builder
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&lp, "cpu value=%di %d\n", i, 1700000000000000000+int64(i)*1e9)
	}
	lp.WriteString("cpu value=\"text\" 1700000100000000000\n")
	cases := []struct {
		name, path string
		content    []byte
	}{
		{"lp mixed field types", "/api/v1/import/lp?db=d", []byte(lp.String())},
		{"csv bad time", "/api/v1/import/csv?db=d&measurement=m&time_format=epoch_s", []byte("time,v\n1700000000,1\n1700000001,2\n1700000002,3\nnot-a-time,4\n")},
		{"parquet null time", "/api/v1/import/parquet?db=d&measurement=m", parquetFile(t, 10, 9)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ci := newChunkedImport(t, 1)
			status, body := ci.post(t, tc.path, tc.content)
			require.Equal(t, http.StatusBadRequest, status, body)
			require.Empty(t, ci.writes, "rows were written before the bad chunk was found")
			require.Zero(t, ci.buf.BufferedBytes())
		})
	}
}

// Fenced after the first chunk: 500 with what was written, no refusal headers.
func TestChunkedImportRefusedPartWay(t *testing.T) {
	_, _ = captureRefusals(t)
	setVar(t, &csvChunkRows, 2)
	setVar(t, &parquetBatchRows, 4)
	for _, tc := range []struct {
		name, path string
		content    []byte
	}{
		{"csv", "/api/v1/import/csv?db=d&measurement=m&time_format=epoch_s", []byte("time,v\n1700000000,1\n1700000001,2\n1700000002,3\n1700000003,4\n")},
		{"parquet", "/api/v1/import/parquet?db=d&measurement=m", parquetFile(t, 10, -1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ci := newChunkedImport(t, 1000)
			var calls atomic.Int32
			ci.buf.SetIngestGate(func() error {
				if calls.Add(1) > 2 { // admission, then the first write
					return fencedErr(t)
				}
				return nil
			})
			body, ct := multipartUpload(t, tc.content)
			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(body))
			req.Header.Set("Content-Type", ct)
			resp, err := ci.app.Test(req, -1)
			require.NoError(t, err)
			out, _ := io.ReadAll(resp.Body)
			require.Equal(t, http.StatusInternalServerError, resp.StatusCode, string(out))
			require.Empty(t, resp.Header.Get(IngestFencedHeader))
			require.Empty(t, resp.Header.Get(fiber.HeaderRetryAfter))
			var m map[string]any
			require.NoError(t, json.Unmarshal(out, &m))
			require.EqualValues(t, ci.writes[0], m["rows_written"])
		})
	}
}

// Writes wait for the flushes: over its allowance an import flushes its
// database and waits, and stops when that cannot succeed (here the storage
// refuses writes) instead of buffering the whole file.
func TestChunkedImportWaitsForFlushes(t *testing.T) {
	setVar(t, &lpChunkBytes, 64)
	setVar(t, &csvChunkRows, 2)
	setVar(t, &parquetBatchRows, 4)
	setVar(t, &importAllowanceOverride, 1)
	setVar(t, &importFlushWait, 50*time.Millisecond)
	var lp strings.Builder
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&lp, "cpu value=%d %d\n", i, 1700000000000000000+int64(i)*1e9)
	}
	for _, tc := range []struct {
		name, path string
		content    []byte
	}{
		{"lp", "/api/v1/import/lp?db=d", []byte(lp.String())},
		{"csv", "/api/v1/import/csv?db=d&measurement=m&time_format=epoch_s", []byte("time,v\n1700000000,1\n1700000001,2\n1700000002,3\n1700000003,4\n")},
		{"parquet", "/api/v1/import/parquet?db=d&measurement=m", parquetFile(t, 10, -1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ci := newChunkedImport(t, 1_000_000) // only the import's own flush
			require.NoError(t, os.Chmod(ci.dir, 0o555))
			t.Cleanup(func() { _ = os.Chmod(ci.dir, 0o755) })
			status, body := ci.post(t, tc.path, tc.content)
			require.Equal(t, http.StatusInternalServerError, status, body)
			require.NotNil(t, body["rows_written"], body)
			require.Len(t, ci.writes, 1, "kept writing while nothing flushed")
		})
	}
}

// Over its allowance an import flushes its own rows: rows below the buffer's
// size threshold do not wait for its age threshold.
func TestChunkedImportFlushesItsOwnRows(t *testing.T) {
	setVar(t, &csvChunkRows, 2)
	setVar(t, &importAllowanceOverride, 1)
	setVar(t, &importFlushWait, 50*time.Millisecond)
	ci := newChunkedImport(t, 1_000_000) // size threshold never reached
	status, body := ci.post(t, "/api/v1/import/csv?db=d&measurement=m&time_format=epoch_s", []byte("time,v\n1700000000,1\n1700000001,2\n1700000002,3\n1700000003,4\n"))
	require.Equal(t, http.StatusOK, status, body)
	require.Len(t, ci.writes, 2)
}

// The allowance counts from what was buffered when the import started: other
// writers' rows waiting for their own flush do not stall it.
func TestChunkedImportAllowanceIsAboveOtherWrites(t *testing.T) {
	setVar(t, &csvChunkRows, 2)
	setVar(t, &importAllowanceOverride, 1)
	setVar(t, &importFlushWait, 2*time.Second)
	ci := newChunkedImport(t, 2) // a 2-row chunk flushes; 1 row waits
	other := &ingest.TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1}, "v": []int64{1}}}
	require.NoError(t, ci.buf.WriteTypedColumnarDirect(context.Background(), "other", "m", other, 1))
	require.Positive(t, ci.buf.BufferedBytes())
	status, body := ci.post(t, "/api/v1/import/csv?db=d&measurement=m&time_format=epoch_s", []byte("time,v\n1700000000,1\n1700000001,2\n1700000002,3\n1700000003,4\n"))
	require.Equal(t, http.StatusOK, status, body)
}

func TestLPImportGzipChunked(t *testing.T) {
	setVar(t, &lpChunkBytes, 256)
	ci := newChunkedImport(t, 7)
	var lp bytes.Buffer
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&lp, "cpu,host=h%d value=%d %d\n", i%4, i, 1700000000000000000+int64(i)*1e9)
	}
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write(lp.Bytes())
	require.NoError(t, zw.Close())
	status, body := ci.post(t, "/api/v1/import/lp?db=d", gz.Bytes())
	require.Equal(t, http.StatusOK, status, body)
	rows, _ := ci.stored(t)
	require.EqualValues(t, 200, rows)
}

// The validation pass sees every measurement, so RBAC and name checks cover
// one that appears only in the last chunk.
func TestLPScanSeesLastChunkMeasurement(t *testing.T) {
	setVar(t, &lpChunkBytes, 32)
	var lp strings.Builder
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&lp, "cpu value=%d %d\n", i, 1700000000000000000+int64(i)*1e9)
	}
	lp.WriteString("secret value=1 1700000100000000000\n")
	body, ct := multipartUpload(t, []byte(lp.String()))
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	var scan *lpScan
	app.Post("/", func(c *fiber.Ctx) error {
		u, ierr := importUploadFrom(c)
		require.Nil(t, ierr)
		var serr *importError
		scan, serr = scanLPImport(u, "ns", "", nil)
		require.Nil(t, serr)
		return nil
	})
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("Content-Type", ct)
	_, err := app.Test(req, -1)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"cpu", "secret"}, scan.measurements)
}

func TestImportRefusesContentEncoding(t *testing.T) {
	ci := newChunkedImport(t, 10)
	body, ct := multipartUpload(t, []byte("cpu value=1\n"))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/import/lp?db=d", bytes.NewReader(body))
	req.Header.Set("Content-Type", ct)
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := ci.app.Test(req, -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)
}

// The Parquet file is a slice of the request body, not a copy; a part the
// slice cannot be trusted for falls back to a copy with the same bytes.
func TestImportFileBytesInPlace(t *testing.T) {
	content := []byte("PAR1\r\n--not-a-boundary\r\nbytes\r\n")
	check := func(body []byte, ct string, wantInPlace bool) {
		app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
		app.Post("/", func(c *fiber.Ctx) error {
			u, ierr := importUploadFrom(c)
			require.Nil(t, ierr)
			got, err := u.fileBytes()
			require.NoError(t, err)
			require.Equal(t, content, got)
			raw := c.Request().Body()
			start := uintptr(unsafe.Pointer(unsafe.SliceData(raw)))
			p := uintptr(unsafe.Pointer(unsafe.SliceData(got)))
			require.Equal(t, wantInPlace, p >= start && p < start+uintptr(len(raw)))
			return nil
		})
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		req.Header.Set("Content-Type", ct)
		_, err := app.Test(req, -1)
		require.NoError(t, err)
	}

	// A field before the file part, as browsers send.
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	require.NoError(t, mw.WriteField("note", "x"))
	fw, err := mw.CreateFormFile("file", "f.parquet")
	require.NoError(t, err)
	_, _ = fw.Write(content)
	require.NoError(t, mw.Close())
	check(b.Bytes(), mw.FormDataContentType(), true)

	// Quoted-printable: the bytes on the wire are not the file.
	b.Reset()
	mw = multipart.NewWriter(&b)
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", `form-data; name="file"; filename="f.parquet"`)
	h.Set("Content-Transfer-Encoding", "quoted-printable")
	pw, err := mw.CreatePart(h)
	require.NoError(t, err)
	_, _ = pw.Write([]byte("PAR1=0D=0A--not-a-boundary=0D=0Abytes=0D=0A"))
	require.NoError(t, mw.Close())
	check(b.Bytes(), mw.FormDataContentType(), false)
}
