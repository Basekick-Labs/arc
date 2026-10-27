package api

// Peak live heap of a bulk import, measured from the handler's entry (the
// request body is already held there) against a bound below the file's size,
// so one more copy of the file fails it. A real listener and a client that
// streams the upload from disk: the server's copy of the body is the only
// large thing in the heap besides the import. Opt-in:
// ARC_IMPORT_MEMORY_TEST=1, not under -race. Before chunking, a 164 MB LP
// file took over 3 GB (#1070 follow-up).

import (
	"bufio"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func writeTestFile(t *testing.T, name string, fill func(w *bufio.Writer)) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	f, err := os.Create(p)
	require.NoError(t, err)
	w := bufio.NewWriterSize(f, 1<<20)
	fill(w)
	require.NoError(t, w.Flush())
	require.NoError(t, f.Close())
	return p
}

func writeParquetTestFile(t *testing.T, rows int) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "big.parquet")
	f, err := os.Create(p)
	require.NoError(t, err)
	sc := arrow.NewSchema([]arrow.Field{{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_us}, {Name: "host", Type: arrow.BinaryTypes.String}, {Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	w, err := pqarrow.NewFileWriter(sc, f, nil, pqarrow.DefaultWriterProps()) // one row group
	require.NoError(t, err)
	for done := 0; done < rows; done += 500000 {
		b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
		for i := done; i < min(done+500000, rows); i++ {
			b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1750000000000000 + int64(i)*1e6))
			b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("host-%d", i%5000))
			b.Field(2).(*array.Int64Builder).Append(int64(i))
		}
		rec := b.NewRecord()
		require.NoError(t, w.WriteBuffered(rec)) // Write would start a row group per call
		rec.Release()
		b.Release()
	}
	require.NoError(t, w.Close()) // closes f too
	return p
}

func TestImportPeakHeap(t *testing.T) {
	if os.Getenv("ARC_IMPORT_MEMORY_TEST") == "" {
		t.Skip("set ARC_IMPORT_MEMORY_TEST=1 (not under -race)")
	}
	setVar(t, &importAllowanceOverride, 32<<20)

	lp := writeTestFile(t, "big.lp", func(w *bufio.Writer) {
		for i := 0; i < 1800000; i++ {
			fmt.Fprintf(w, "cpu,host=h%d value=%d,v2=%d %d\n", i%50, i, i*2, 1750000000000000000+int64(i)*1e9)
			fmt.Fprintf(w, "mem,host=h%d used=%d,free=%d %d\n", i%50, i, i*3, 1750000000000000000+int64(i)*1e9)
		}
	})
	csv := writeTestFile(t, "big.csv", func(w *bufio.Writer) {
		w.WriteString("time,host,value,v2\n")
		for i := 0; i < 6000000; i++ {
			fmt.Fprintf(w, "%d,h%d,%d,%d\n", 1750000000+i, i%50, i, i*2)
		}
	})
	pq := writeParquetTestFile(t, 10_000_000)

	// Bound: a fixed amount, below each file's size, so one more copy of the
	// file fails it. Measured: LP +71 MB, CSV +27 MB, Parquet +77 MB (its one
	// row group read without buffered streams: +343 MB).
	cases := []struct {
		path, file string
		bound      int64
	}{
		{"/api/v1/import/lp?db=z", lp, 128 << 20},
		{"/api/v1/import/csv?db=z&measurement=c&time_format=epoch_s", csv, 128 << 20},
		{"/api/v1/import/parquet?db=z&measurement=p", pq, 128 << 20},
	}
	for _, tc := range cases {
		ci := newChunkedImport(t, 50000)
		var entry, peak atomic.Int64
		app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
		app.Use(func(c *fiber.Ctx) error {
			runtime.GC()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			entry.Store(int64(m.HeapAlloc))
			return c.Next()
		})
		h := NewImportHandler(zerolog.Nop())
		h.SetArrowBuffer(ci.buf)
		h.RegisterRoutes(app)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		go func() { _ = app.Listener(ln) }()

		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			var m runtime.MemStats
			for {
				select {
				case <-stop:
					return
				case <-time.After(20 * time.Millisecond):
				}
				if entry.Load() == 0 {
					continue
				}
				runtime.GC() // live heap, not garbage awaiting a collection
				runtime.ReadMemStats(&m)
				if int64(m.HeapAlloc) > peak.Load() {
					peak.Store(int64(m.HeapAlloc))
				}
			}
		}()

		pr, pw := io.Pipe()
		mw := multipart.NewWriter(pw)
		go func() {
			fw, err := mw.CreateFormFile("file", filepath.Base(tc.file))
			if err == nil {
				var f *os.File
				if f, err = os.Open(tc.file); err == nil {
					_, err = io.Copy(fw, f)
					f.Close()
				}
			}
			if err == nil {
				err = mw.Close()
			}
			pw.CloseWithError(err)
		}()
		resp, err := http.Post("http://"+ln.Addr().String()+tc.path, mw.FormDataContentType(), pr)
		close(stop)
		<-done
		require.NoError(t, err)
		resp.Body.Close()
		require.NoError(t, app.Shutdown())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		st, _ := os.Stat(tc.file)
		grew := peak.Load() - entry.Load()
		t.Logf("%s: file %d MB, handler peak +%d MB", tc.path, st.Size()>>20, grew>>20)
		require.Less(t, grew, tc.bound, "%s: the import's memory grows with the file", tc.path)
	}
}
