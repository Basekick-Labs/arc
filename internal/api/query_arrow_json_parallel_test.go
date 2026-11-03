//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// poisoningAllocator scribbles over a buffer when it is freed, so a read
// after release sees garbage instead of the stale (still correct) bytes the
// Go allocator would leave behind.
type poisoningAllocator struct{ memory.Allocator }

func (a poisoningAllocator) Free(b []byte) {
	for i := range b {
		b[i] = 0xA5
	}
	a.Allocator.Free(b)
}

// releasingReader releases each record when the next one is read, as
// duckdb-go's and arcx's readers do: a consumer that keeps a record past Next
// must hold its own reference.
type releasingReader struct {
	schema  *arrow.Schema
	records []arrow.Record
	idx     int
	onNext  func(i int) // called before record i is returned
	nexts   int
}

func (r *releasingReader) Schema() *arrow.Schema { return r.schema }
func (r *releasingReader) Next() bool {
	r.nexts++
	if r.idx >= 0 && r.idx < len(r.records) {
		r.records[r.idx].Release()
		r.records[r.idx] = nil
	}
	r.idx++
	if r.idx < len(r.records) && r.onNext != nil {
		r.onNext(r.idx)
	}
	return r.idx < len(r.records)
}
func (r *releasingReader) Record() arrow.Record           { return r.records[r.idx] }
func (r *releasingReader) RecordBatch() arrow.RecordBatch { return r.records[r.idx] }
func (r *releasingReader) Err() error                     { return nil }
func (r *releasingReader) Retain()                        {}
func (r *releasingReader) Release() {
	for i, rec := range r.records {
		if rec != nil {
			rec.Release()
			r.records[i] = nil
		}
	}
}

var parallelTestSchema = arrow.NewSchema([]arrow.Field{
	{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "v", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "ok", Type: arrow.FixedWidthTypes.Boolean},
}, nil)

// skewedSizes makes later batches finish before earlier ones: a large batch
// followed by tiny ones.
var skewedSizes = []int{1500, 900, 4000, 3, 1, 2500, 7, 2048, 0, 1200, 64, 3000}

func buildParallelRecords(t *testing.T, alloc memory.Allocator, sizes []int) []arrow.Record {
	t.Helper()
	recs := make([]arrow.Record, len(sizes))
	row := 0
	for b, n := range sizes {
		rows := make([][]interface{}, n)
		for i := range rows {
			var host interface{} = "server" + string(rune('a'+row%26)) + `"q"`
			var v interface{} = float64(row) / 7
			if row%11 == 0 {
				host, v = nil, nil
			}
			rows[i] = []interface{}{time.Unix(1760000000+int64(row/3), int64(row%3)*250000000).UTC(), host, v, int64(row * 31), row%2 == 0}
			row++
		}
		recs[b] = buildArrowBatch(alloc, parallelTestSchema, rows)
	}
	return recs
}

var executionTimeRE = regexp.MustCompile(`"execution_time_ms":[0-9.e+-]+`)

// encodeWith runs streamArrowJSON over records with the given encoder limit
// (0 keeps every batch on the stream goroutine).
func encodeWith(t *testing.T, limit int64, reader array.RecordReader, maxRows int, w *bufio.Writer, ctx context.Context) (int, error) {
	t.Helper()
	orig := encoderLimit
	encoderLimit = func() int64 { return limit }
	defer func() { encoderLimit = orig }()
	return streamArrowJSON(ctx, w, reader, maxRows, nil, time.Now(), "2026-10-19T12:00:00Z")
}

type parallelRun struct {
	body     string
	rows     int
	err      error
	handoffs int64
}

func runParallelCase(t *testing.T, limit int64, sizes []int, maxRows int, w func(*bytes.Buffer) *bufio.Writer, setup func(*releasingReader)) parallelRun {
	t.Helper()
	checked := memory.NewCheckedAllocator(poisoningAllocator{memory.NewGoAllocator()})
	recs := buildParallelRecords(t, checked, sizes)
	reader := &releasingReader{schema: parallelTestSchema, records: recs, idx: -1}
	if setup != nil {
		setup(reader)
	}
	var out bytes.Buffer
	bw := bufio.NewWriterSize(&out, 4096)
	if w != nil {
		bw = w(&out)
	}
	goroutines := runtime.NumGoroutine()
	before := encodeHandoffs.Load()
	rows, err := encodeWith(t, limit, reader, maxRows, bw, context.Background())
	_ = bw.Flush()
	handoffs := encodeHandoffs.Load() - before
	reader.Release()
	checked.AssertSize(t, 0)
	if n := encodersInUse.Load(); n != 0 {
		t.Fatalf("%d encoder slots still taken after the stream returned", n)
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > goroutines && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if g := runtime.NumGoroutine(); g > goroutines {
		t.Fatalf("goroutines %d after the stream, %d before", g, goroutines)
	}
	// Valid JSON on every path: the "serial" baseline also goes through the
	// pipeline once past parallelEncodeAfterBatches (encoding inline), so
	// comparing the two alone cannot see a rule both get wrong.
	if w == nil && !json.Valid(out.Bytes()) {
		t.Fatalf("not valid JSON: %q", out.String()[:min(out.Len(), 300)])
	}
	return parallelRun{body: executionTimeRE.ReplaceAllString(out.String(), ""), rows: rows, err: err, handoffs: handoffs}
}

// Batches encoded on other goroutines give exactly the bytes of the serial
// encoder, in order, with every record released and nothing left running,
// even when later batches finish first.
func TestStreamArrowJSONParallelMatchesSerial(t *testing.T) {
	serial := runParallelCase(t, 0, skewedSizes, 0, nil, nil)
	if serial.err != nil || serial.handoffs != 0 {
		t.Fatalf("serial run: %v, %d hand-offs", serial.err, serial.handoffs)
	}
	for _, limit := range []int64{1, 3, 16} {
		par := runParallelCase(t, limit, skewedSizes, 0, nil, nil)
		// Empty batches before the parallel phase: the comma must come from
		// the rows handed out, not the rows written so far.
		emptyFirst := []int{0, 0, 700, 700, 700, 700, 700}
		if a, b := runParallelCase(t, 0, emptyFirst, 0, nil, nil), runParallelCase(t, limit, emptyFirst, 0, nil, nil); a.body != b.body {
			t.Fatalf("limit %d: output differs after empty leading batches", limit)
		}
		if par.err != nil {
			t.Fatal(par.err)
		}
		if par.handoffs == 0 {
			t.Fatalf("limit %d: no batch was encoded off the stream goroutine", limit)
		}
		if par.body != serial.body || par.rows != serial.rows {
			t.Fatalf("limit %d: parallel output differs from serial (rows %d vs %d)", limit, par.rows, serial.rows)
		}
	}
	total := 0
	for _, n := range skewedSizes {
		total += n
	}
	if serial.rows != total || !strings.Contains(serial.body, `"row_count":`+itoa(total)) {
		t.Fatalf("rows %d, want %d", serial.rows, total)
	}
}

// The governance cap in the parallel phase: mid-batch, and exactly at a batch
// boundary (one more Next, as before), matches the serial result.
func TestStreamArrowJSONParallelCap(t *testing.T) {
	boundary := skewedSizes[0] + skewedSizes[1] + skewedSizes[2] + skewedSizes[3]
	for _, maxRows := range []int{boundary + 500, boundary, 2400} {
		var serialNexts, parNexts int
		serial := runParallelCase(t, 0, skewedSizes, maxRows, nil, func(r *releasingReader) { r.onNext = func(int) { serialNexts++ } })
		par := runParallelCase(t, 8, skewedSizes, maxRows, nil, func(r *releasingReader) { r.onNext = func(int) { parNexts++ } })
		if serial.body != par.body || serial.rows != maxRows || par.rows != maxRows {
			t.Fatalf("cap %d: rows serial %d parallel %d; bodies equal %v", maxRows, serial.rows, par.rows, serial.body == par.body)
		}
		if !strings.Contains(par.body, `"rows_capped":true`) {
			t.Fatalf("cap %d: not marked capped", maxRows)
		}
		if serialNexts != parNexts {
			t.Fatalf("cap %d: Next called %d times serially, %d in parallel", maxRows, serialNexts, parNexts)
		}
	}
}

// A context cancelled while batches are out ends the stream truncated at a
// batch boundary; nothing leaks and the rows written are the rows counted.
func TestStreamArrowJSONParallelCancel(t *testing.T) {
	checked := memory.NewCheckedAllocator(poisoningAllocator{memory.NewGoAllocator()})
	recs := buildParallelRecords(t, checked, skewedSizes)
	ctx, cancel := context.WithCancel(context.Background())
	reader := &releasingReader{schema: parallelTestSchema, records: recs, idx: -1, onNext: func(i int) {
		if i == 7 {
			cancel()
		}
	}}
	var out bytes.Buffer
	bw := bufio.NewWriter(&out)
	rows, err := encodeWith(t, 8, reader, 0, bw, ctx)
	_ = bw.Flush()
	reader.Release()
	checked.AssertSize(t, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want context.Canceled", err)
	}
	body := out.String()
	if !strings.Contains(body, `"truncated":true`) || !strings.Contains(body, `"row_count":`+itoa(rows)) {
		t.Fatalf("rows %d, body tail %q", rows, body[max(0, len(body)-200):])
	}
	if got := strings.Count(body[strings.Index(body, `"data":[`):], "],["); got+1 != rows {
		t.Fatalf("%d rows in data, row_count %d", got+1, rows)
	}
}

// A client that goes away while batches are out: the flush error stops the
// stream, the error says why, and every handed-out batch is still waited for
// and released.
func TestStreamArrowJSONParallelFlushError(t *testing.T) {
	sentinel := errors.New("client disconnected")
	run := runParallelCase(t, 8, skewedSizes, 0, func(*bytes.Buffer) *bufio.Writer {
		return bufio.NewWriterSize(&errAfterNBytes{limit: 400 << 10, err: sentinel}, 4096)
	}, nil)
	if !errors.Is(run.err, sentinel) || !errors.Is(run.err, errClientDisconnected) {
		t.Fatalf("err %v", run.err)
	}
}

// panicValueArray has no case of its own in the encoder, so it takes the
// ValueStr fallback, which panics.
type panicValueArray struct{ *array.Int64 }

func (panicValueArray) ValueStr(int) string { panic("encoder test panic") }

// A panic while encoding a handed-out batch ends the stream truncated instead
// of the process; the rows before it are written, nothing after it (later
// batches are already encoded and waiting when the failed one is taken back).
func TestStreamArrowJSONParallelEncoderPanic(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
	var recs []arrow.Record
	for b := 0; b < 14; b++ {
		bld := array.NewInt64Builder(alloc)
		for i := 0; i < 100; i++ {
			bld.Append(int64(b*100 + i))
		}
		col := bld.NewInt64Array()
		bld.Release()
		var arr arrow.Array = col
		if b == 4 {
			arr = panicValueArray{col}
		}
		recs = append(recs, array.NewRecord(schema, []arrow.Array{arr}, 100))
		col.Release()
	}
	reader := &releasingReader{schema: schema, records: recs, idx: -1}
	var out bytes.Buffer
	bw := bufio.NewWriter(&out)
	rows, err := encodeWith(t, 8, reader, 0, bw, context.Background())
	_ = bw.Flush()
	reader.Release()
	alloc.AssertSize(t, 0)
	if err == nil || !strings.Contains(err.Error(), "encoder panicked") {
		t.Fatalf("err %v", err)
	}
	if rows != 400 || !strings.Contains(out.String(), `"row_count":400`) || !strings.Contains(out.String(), `"truncated":true`) {
		t.Fatalf("rows %d, body tail %q", rows, out.String()[max(0, out.Len()-160):])
	}
}

func itoa(n int) string { return string(appendInt(nil, n)) }

func appendInt(b []byte, n int) []byte {
	if n >= 10 {
		b = appendInt(b, n/10)
	}
	return append(b, byte('0'+n%10))
}

// A result of up to parallelEncodeAfterBatches batches is encoded on the stream
// goroutine alone: small results never pay for a hand-off.
func TestStreamArrowJSONSmallResultStaysSerial(t *testing.T) {
	run := runParallelCase(t, 8, []int{2048, 2048}, 0, nil, nil)
	if run.err != nil || run.handoffs != 0 {
		t.Fatalf("err %v, %d hand-offs for a two-batch result", run.err, run.handoffs)
	}
	run = runParallelCase(t, 8, []int{2048, 2048, 10}, 0, nil, nil)
	if run.handoffs != 1 {
		t.Fatalf("%d hand-offs for a three-batch result, want 1", run.handoffs)
	}
}

// decimalParallelRun encodes decimal (cast to float before encoding) and
// string batches; the job owns the cast record, which holds the columns it
// shares with the batch.
func decimalParallelRun(t *testing.T, limit int64, maxRows int) (string, int) {
	t.Helper()
	checked := memory.NewCheckedAllocator(poisoningAllocator{memory.NewGoAllocator()})
	decType := &arrow.Decimal128Type{Precision: 10, Scale: 2}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "d", Type: decType},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)
	var recs []arrow.Record
	row := 0
	for _, sz := range []int{1500, 900, 4000, 3, 0, 2500, 7, 2048, 1200, 3000} {
		db := array.NewDecimal128Builder(checked, decType)
		nb := array.NewInt64Builder(checked)
		sb := array.NewStringBuilder(checked)
		for i := 0; i < sz; i++ {
			db.Append(decimal128.FromI64(int64(row)))
			nb.Append(int64(row * 3))
			sb.Append("host-" + itoa(row))
			row++
		}
		a, b, c := db.NewArray(), nb.NewArray(), sb.NewArray()
		recs = append(recs, array.NewRecord(schema, []arrow.Array{a, b, c}, int64(sz)))
		a.Release()
		b.Release()
		c.Release()
		db.Release()
		nb.Release()
		sb.Release()
	}
	reader := &releasingReader{schema: schema, records: recs, idx: -1}
	var out bytes.Buffer
	bw := bufio.NewWriter(&out)
	rows, err := encodeWith(t, limit, reader, maxRows, bw, context.Background())
	_ = bw.Flush()
	reader.Release()
	checked.AssertSize(t, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(out.Bytes()) {
		t.Fatal("invalid JSON")
	}
	return executionTimeRE.ReplaceAllString(out.String(), ""), rows
}

// Decimal results (SUM/AVG) in the parallel phase: same bytes as serial,
// every record released, with and without a cap.
func TestStreamArrowJSONParallelDecimals(t *testing.T) {
	for _, maxRows := range []int{0, 7000, 6403} {
		want, wantRows := decimalParallelRun(t, 0, maxRows)
		for i := 0; i < 5; i++ {
			got, rows := decimalParallelRun(t, 8, maxRows)
			if got != want || rows != wantRows {
				t.Fatalf("cap %d: parallel differs from serial (%d vs %d rows)", maxRows, rows, wantRows)
			}
		}
	}
}

// A cap reached exactly at a batch boundary, then an empty batch, then a
// non-empty one: the empty batch does not trip the cap (no row reached it),
// the next one does. Five reads in all, as before the parallel path.
func TestStreamArrowJSONCapThenEmptyBatch(t *testing.T) {
	sizes := []int{1500, 900, 4000, 0, 3000, 500}
	for _, limit := range []int64{0, 8} {
		nexts := 0
		run := runParallelCase(t, limit, sizes, 1500+900+4000, nil, func(r *releasingReader) { r.onNext = func(int) { nexts++ } })
		if run.rows != 6400 || !strings.Contains(run.body, `"rows_capped":true`) {
			t.Fatalf("limit %d: rows %d", limit, run.rows)
		}
		if nexts != 5 {
			t.Fatalf("limit %d: %d batches read, want 5", limit, nexts)
		}
	}
}

// Encoded batches are written as soon as they are ready, not when several
// have piled up: when a later batch is read, the earlier one is already out.
func TestStreamArrowJSONWritesReadyBatchesEarly(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.NewGoAllocator())
	recs := buildParallelRecords(t, checked, []int{100, 100, 100, 100, 100, 100})
	var out bytes.Buffer
	bw := bufio.NewWriterSize(&out, 4096)
	var seen []int
	reader := &releasingReader{schema: parallelTestSchema, records: recs, idx: -1, onNext: func(i int) {
		if i == 4 {
			seen = append(seen, strings.Count(out.String(), "],["))
		}
	}}
	// Limit 0: every batch is encoded inline, so it is ready at once.
	if _, err := encodeWith(t, 0, reader, 0, bw, context.Background()); err != nil {
		t.Fatal(err)
	}
	reader.Release()
	if len(seen) != 1 || seen[0] < 299 {
		t.Fatalf("rows on the wire when batch 5 was read: %v, want the first three batches (300)", seen)
	}
}

// A query keeps at most encodeMaxInFlight batches handed out.
func TestJSONEncodePipelineBound(t *testing.T) {
	orig := encoderLimit
	encoderLimit = func() int64 { return 0 }
	defer func() { encoderLimit = orig }()
	p := &encodePipeline{}
	for i := 0; i < encodeMaxInFlight; i++ {
		if p.full() {
			t.Fatalf("full after %d jobs", i)
		}
		p.submit(&encodeJob{fill: func(b []byte) ([]byte, error) { return b, nil }})
	}
	if !p.full() || encodeMaxInFlight > 8 {
		t.Fatalf("not full after %d jobs (bound %d)", encodeMaxInFlight, encodeMaxInFlight)
	}
	p.drain()
}

// blockingValueArray takes the ValueStr fallback, which waits for release:
// a batch still being encoded when the stream is cancelled.
type blockingValueArray struct {
	*array.Int64
	release <-chan struct{}
}

func (b blockingValueArray) ValueStr(i int) string {
	<-b.release
	return b.Int64.ValueStr(i)
}

// A cancellation that arrives while a handed-out batch is still being encoded
// waits for it and writes it: the client gets every row read before the
// cancellation, as when each row was written as it was encoded.
func TestStreamArrowJSONCancelWritesBatchesInFlight(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
	release := make(chan struct{})
	var recs []arrow.Record
	for b := 0; b < 8; b++ {
		bld := array.NewInt64Builder(alloc)
		for i := 0; i < 100; i++ {
			bld.Append(int64(b*100 + i))
		}
		col := bld.NewInt64Array()
		bld.Release()
		var arr arrow.Array = col
		if b == 3 {
			arr = blockingValueArray{col, release}
		}
		recs = append(recs, array.NewRecord(schema, []arrow.Array{arr}, 100))
		col.Release()
	}
	ctx, cancel := context.WithCancel(context.Background())
	reader := &releasingReader{schema: schema, records: recs, idx: -1, onNext: func(i int) {
		if i == 5 {
			cancel()
			time.AfterFunc(50*time.Millisecond, func() { close(release) })
		}
	}}
	var out bytes.Buffer
	bw := bufio.NewWriter(&out)
	rows, err := encodeWith(t, 8, reader, 0, bw, ctx)
	_ = bw.Flush()
	reader.Release()
	alloc.AssertSize(t, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
	// Batches 0-4 were read before the cancellation; batch 3 was still being
	// encoded when it arrived.
	if rows != 500 || !strings.Contains(out.String(), `"row_count":500`) {
		t.Fatalf("rows %d, want 500", rows)
	}
}
