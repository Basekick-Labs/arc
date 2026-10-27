//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// msgpackParallelSchema covers every typed column encoder plus the ValueStr
// fallback (Duration), with NULLs.
var msgpackParallelSchema = arrow.NewSchema([]arrow.Field{
	{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
	{Name: "i16", Type: arrow.PrimitiveTypes.Int16},
	{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
	{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
	{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
	{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
	{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
	{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
	{Name: "ok", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	{Name: "day", Type: arrow.FixedWidthTypes.Date32},
	{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "big", Type: arrow.BinaryTypes.LargeString},
	{Name: "raw", Type: arrow.BinaryTypes.Binary},
	{Name: "dur", Type: &arrow.DurationType{Unit: arrow.Millisecond}},
}, nil)

// msgpackRecord builds rows [start, start+n) of the test result; a row's
// values depend only on its index, so any split into batches holds the same
// result.
func msgpackRecord(t *testing.T, alloc memory.Allocator, start, n int) arrow.Record {
	t.Helper()
	b := array.NewRecordBuilder(alloc, msgpackParallelSchema)
	defer b.Release()
	for i := start; i < start+n; i++ {
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1760000000000000 + int64(i)*1500))
		if i%13 == 0 {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.Int64Builder).Append(int64(i) * -977)
		}
		b.Field(2).(*array.Int32Builder).Append(int32(i))
		b.Field(3).(*array.Int16Builder).Append(int16(i))
		b.Field(4).(*array.Int8Builder).Append(int8(i))
		b.Field(5).(*array.Uint64Builder).Append(uint64(i) << 40)
		b.Field(6).(*array.Uint32Builder).Append(uint32(i))
		b.Field(7).(*array.Uint16Builder).Append(uint16(i))
		b.Field(8).(*array.Uint8Builder).Append(uint8(i))
		if i%7 == 0 {
			b.Field(9).AppendNull()
		} else {
			b.Field(9).(*array.Float64Builder).Append(float64(i) / 3)
		}
		b.Field(10).(*array.Float32Builder).Append(float32(i) / 7)
		if i%5 == 0 {
			b.Field(11).AppendNull()
		} else {
			b.Field(11).(*array.BooleanBuilder).Append(i%2 == 0)
		}
		b.Field(12).(*array.Date32Builder).Append(arrow.Date32(20000 + i%400))
		if i%11 == 0 {
			b.Field(13).AppendNull()
		} else {
			b.Field(13).(*array.StringBuilder).Append("server-" + strings.Repeat("x", i%9))
		}
		b.Field(14).(*array.LargeStringBuilder).Append("large")
		b.Field(15).(*array.BinaryBuilder).Append([]byte{byte(i), byte(i >> 8)})
		b.Field(16).(*array.DurationBuilder).Append(arrow.Duration(i))
	}
	return b.NewRecord()
}

// msgpackBody is the response up to the "row_count" key: everything after it
// carries the execution time.
func msgpackBody(t *testing.T, out []byte) []byte {
	t.Helper()
	i := bytes.Index(out, append([]byte{0xa9}, "row_count"...))
	if i < 0 {
		t.Fatalf("no row_count key in %d bytes", len(out))
	}
	return out[:i]
}

func runMsgPack(t *testing.T, limit int64, batches []arrow.Record, rows int, w *bufio.Writer, ctx context.Context) (int, error) {
	t.Helper()
	orig := encoderLimit
	encoderLimit = func() int64 { return limit }
	defer func() { encoderLimit = orig }()
	return streamMsgPackFromBatches(ctx, w, msgpackParallelSchema, batches, rows, 0, nil, time.Now(), "2026-10-19T12:00:00Z")
}

// Many batches encoded on encoder goroutines give exactly the bytes of the
// whole result in one batch (the serial loop), for every column type, with
// every batch still owned by the caller and nothing left running.
func TestStreamMsgPackParallelMatchesSerial(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	sizes := []int{3000, 17, 2048, 0, 9000, 1, 40000, 2048, 5}
	total := 0
	for _, n := range sizes {
		total += n
	}

	whole := msgpackRecord(t, alloc, 0, total)
	var want bytes.Buffer
	bw := bufio.NewWriter(&want)
	if _, err := runMsgPack(t, 0, []arrow.Record{whole}, total, bw, context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = bw.Flush()
	whole.Release()

	for _, limit := range []int64{0, 1, 3, 16} {
		var batches []arrow.Record
		start := 0
		for _, n := range sizes {
			batches = append(batches, msgpackRecord(t, alloc, start, n))
			start += n
		}
		goroutines := runtime.NumGoroutine()
		before := encodeHandoffs.Load()
		var got bytes.Buffer
		bw := bufio.NewWriter(&got)
		rows, err := runMsgPack(t, limit, batches, total, bw, context.Background())
		_ = bw.Flush()
		handoffs := encodeHandoffs.Load() - before
		for _, b := range batches {
			b.Release()
		}
		if err != nil || rows != total {
			t.Fatalf("limit %d: rows %d err %v", limit, rows, err)
		}
		if limit > 0 && handoffs == 0 {
			t.Fatalf("limit %d: nothing was encoded off the stream goroutine", limit)
		}
		if !bytes.Equal(msgpackBody(t, got.Bytes()), msgpackBody(t, want.Bytes())) {
			t.Fatalf("limit %d: bytes differ from the serial encoding", limit)
		}
		var decoded map[string]interface{}
		if err := msgpack.Unmarshal(got.Bytes(), &decoded); err != nil {
			t.Fatalf("limit %d: response does not decode: %v", limit, err)
		}
		if n := encodersInUse.Load(); n != 0 {
			t.Fatalf("%d encoder slots still taken", n)
		}
		deadline := time.Now().Add(2 * time.Second)
		for runtime.NumGoroutine() > goroutines && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if g := runtime.NumGoroutine(); g > goroutines {
			t.Fatalf("goroutines %d after, %d before", g, goroutines)
		}
	}
	alloc.AssertSize(t, 0)
}

// manyBatches builds k batches of n rows.
func manyBatches(t *testing.T, alloc memory.Allocator, k, n int) []arrow.Record {
	var out []arrow.Record
	for i := 0; i < k; i++ {
		out = append(out, msgpackRecord(t, alloc, i*n, n))
	}
	return out
}

// A context cancelled while columns are out ends the stream with the context
// error (not relabelled a client disconnect), every job waited for.
func TestStreamMsgPackParallelCancel(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	batches := manyBatches(t, alloc, 40, 2048)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	before := encodeHandoffs.Load()
	_, err := runMsgPack(t, 8, batches, 40*2048, bufio.NewWriter(&out), ctx)
	// Checked at each column before its runs are handed out: an already
	// cancelled stream starts no work.
	if n := encodeHandoffs.Load() - before; n != 0 {
		t.Fatalf("%d jobs handed out for a cancelled stream", n)
	}
	for _, b := range batches {
		b.Release()
	}
	alloc.AssertSize(t, 0)
	if !errors.Is(err, context.Canceled) || errors.Is(err, errClientDisconnected) {
		t.Fatalf("err %v", err)
	}
	if n := encodersInUse.Load(); n != 0 {
		t.Fatalf("%d encoder slots still taken", n)
	}
}

// A writer that fails while columns are out is a client disconnect.
func TestStreamMsgPackParallelWriterError(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	batches := manyBatches(t, alloc, 40, 2048)
	fw := &failingWriter{budget: 300 << 10, err: io.ErrClosedPipe}
	before := encodeHandoffs.Load()
	_, err := runMsgPack(t, 64, batches, 40*2048, bufio.NewWriterSize(fw, 4096), context.Background())
	// The failed write stops the stream: most of the 17 columns x 5 runs are
	// never encoded.
	if n := encodeHandoffs.Load() - before; n > 20 {
		t.Fatalf("%d jobs encoded after the client went away", n)
	}
	for _, b := range batches {
		b.Release()
	}
	alloc.AssertSize(t, 0)
	if !errors.Is(err, errClientDisconnected) || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("err %v", err)
	}
}

// panicDurationArray takes the encoder's ValueStr fallback, which panics.
type panicDurationArray struct{ *array.Duration }

func (panicDurationArray) ValueStr(int) string { panic("msgpack encoder test panic") }

// A panic while encoding a column run ends the stream with an error, not the
// process; the column runs after it are not written.
func TestStreamMsgPackParallelEncoderPanic(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	batches := manyBatches(t, alloc, 20, 2048)
	// Swap batch 12's duration column (the last column) for one that panics.
	rec := batches[12]
	cols := make([]arrow.Array, rec.NumCols())
	for i := range cols {
		cols[i] = rec.Column(i)
	}
	cols[16] = panicDurationArray{rec.Column(16).(*array.Duration)}
	batches[12] = array.NewRecord(msgpackParallelSchema, cols, rec.NumRows())
	rec.Release()
	var out bytes.Buffer
	_, err := runMsgPack(t, 8, batches, 20*2048, bufio.NewWriter(&out), context.Background())
	for _, b := range batches {
		b.Release()
	}
	alloc.AssertSize(t, 0)
	if err == nil || !strings.Contains(err.Error(), "encoder panicked") {
		t.Fatalf("err %v", err)
	}
	// A server fault, not relabelled a client disconnect (logged at Warn).
	if errors.Is(err, errClientDisconnected) || isClientError(err) {
		t.Fatalf("encoder panic reported as a client error: %v", err)
	}
	if n := encodersInUse.Load(); n != 0 {
		t.Fatalf("%d encoder slots still taken", n)
	}
}

// singleColumnRun encodes one column of k batches of n rows, built by mk,
// against the same rows in one batch (the serial loop), with every job handed
// out; returns the jobs handed out.
func singleColumnRun(t *testing.T, field arrow.Field, k, n int, mk func(alloc memory.Allocator, start, n int) arrow.Array) int64 {
	t.Helper()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	encode := func(batches []arrow.Record, limit int64) ([]byte, int64) {
		orig := encoderLimit
		encoderLimit = func() int64 { return limit }
		defer func() { encoderLimit = orig }()
		before := encodeHandoffs.Load()
		var out bytes.Buffer
		bw := bufio.NewWriter(&out)
		if _, err := streamMsgPackFromBatches(context.Background(), bw, schema, batches, k*n, 0, nil, time.Now(), "t"); err != nil {
			t.Fatal(err)
		}
		_ = bw.Flush()
		return msgpackBody(t, out.Bytes()), encodeHandoffs.Load() - before
	}
	whole := mk(alloc, 0, k*n)
	wholeRec := array.NewRecord(schema, []arrow.Array{whole}, int64(k*n))
	whole.Release()
	want, _ := encode([]arrow.Record{wholeRec}, 0)
	wholeRec.Release()
	var batches []arrow.Record
	for i := 0; i < k; i++ {
		a := mk(alloc, i*n, n)
		batches = append(batches, array.NewRecord(schema, []arrow.Array{a}, int64(n)))
		a.Release()
	}
	got, handoffs := encode(batches, 64)
	for _, b := range batches {
		b.Release()
	}
	alloc.AssertSize(t, 0)
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: bytes differ from the serial encoding", field.Name)
	}
	return handoffs
}

// A dictionary (ENUM) column takes the ValueStr fallback, which builds the
// dictionary lazily: each batch's array is read by one job only, so -race
// stays quiet and the bytes match.
func TestStreamMsgPackParallelDictionaryColumn(t *testing.T) {
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	singleColumnRun(t, arrow.Field{Name: "level", Type: dictType}, 12, 2048, func(alloc memory.Allocator, start, n int) arrow.Array {
		b := array.NewDictionaryBuilder(alloc, dictType).(*array.BinaryDictionaryBuilder)
		defer b.Release()
		levels := []string{"debug", "info", "warn", "error"}
		for i := start; i < start+n; i++ {
			if err := b.AppendString(levels[i%4]); err != nil {
				t.Fatal(err)
			}
		}
		return b.NewArray()
	})
}

// Long values end a column run before msgpackChunkRows: ten batches of 2048
// one-kilobyte strings (2 MB each) are ten jobs, not two.
func TestStreamMsgPackParallelChunksBySize(t *testing.T) {
	jobs := singleColumnRun(t, arrow.Field{Name: "msg", Type: arrow.BinaryTypes.String}, 10, 2048, func(alloc memory.Allocator, start, n int) arrow.Array {
		b := array.NewStringBuilder(alloc)
		defer b.Release()
		for i := start; i < start+n; i++ {
			b.Append(strings.Repeat(string(rune('a'+i%26)), 1024))
		}
		return b.NewArray()
	})
	if jobs != 10 {
		t.Fatalf("%d jobs for ten 2 MB batches, want 10", jobs)
	}
}

// blockingDurationArray takes the ValueStr fallback, which waits for release:
// a column run still being encoded when the stream fails.
type blockingDurationArray struct {
	*array.Duration
	release <-chan struct{}
}

func (b blockingDurationArray) ValueStr(i int) string {
	<-b.release
	return b.Duration.ValueStr(i)
}

// The stream returns only after every job it handed out has finished, also
// when it fails early: the caller releases the batches as soon as it returns.
func TestStreamMsgPackParallelWaitsForJobsOnError(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	durType := &arrow.DurationType{Unit: arrow.Millisecond}
	schema := arrow.NewSchema([]arrow.Field{{Name: "dur", Type: durType}}, nil)
	release := make(chan struct{})
	var batches []arrow.Record
	for i := 0; i < 24; i++ {
		b := array.NewDurationBuilder(alloc, durType)
		for r := 0; r < 2048; r++ {
			b.Append(arrow.Duration(r))
		}
		col := b.NewDurationArray()
		b.Release()
		var arr arrow.Array = col
		if i == 8 { // in the second run of the column
			arr = blockingDurationArray{col, release}
		}
		batches = append(batches, array.NewRecord(schema, []arrow.Array{arr}, 2048))
		col.Release()
	}
	// The header goes through; writing the first run fails while the second
	// is still blocked.
	fw := &failingWriter{budget: 200, err: io.ErrClosedPipe}
	orig := encoderLimit
	encoderLimit = func() int64 { return 64 }
	defer func() { encoderLimit = orig }()
	time.AfterFunc(200*time.Millisecond, func() { close(release) })
	start := time.Now()
	_, err := streamMsgPackFromBatches(context.Background(), bufio.NewWriterSize(fw, 16), schema, batches, 24*2048, 0, nil, time.Now(), "t")
	if n := encodersInUse.Load(); n != 0 {
		t.Fatalf("stream returned with %d jobs still encoding", n)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Fatalf("stream returned after %v, before the blocked job finished", time.Since(start))
	}
	for _, b := range batches {
		b.Release()
	}
	alloc.AssertSize(t, 0)
	if !errors.Is(err, errClientDisconnected) {
		t.Fatalf("err %v", err)
	}
}

// Results of up to parallelEncodeAfterBatches batches keep the serial loop.
func TestStreamMsgPackSmallResultStaysSerial(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	batches := manyBatches(t, alloc, 2, 2048)
	before := encodeHandoffs.Load()
	var out bytes.Buffer
	if _, err := runMsgPack(t, 64, batches, 2*2048, bufio.NewWriter(&out), context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, b := range batches {
		b.Release()
	}
	alloc.AssertSize(t, 0)
	if n := encodeHandoffs.Load() - before; n != 0 {
		t.Fatalf("%d jobs handed out for a two-batch result", n)
	}
}

// A deadline that passes while a column run is being encoded: the job's
// context error ends the stream as a timeout (not a client disconnect), and
// the short column is never passed off as complete.
func TestStreamMsgPackParallelDeadlineMidRun(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	durType := &arrow.DurationType{Unit: arrow.Millisecond}
	schema := arrow.NewSchema([]arrow.Field{{Name: "dur", Type: durType}}, nil)
	release := make(chan struct{})
	var batches []arrow.Record
	for i := 0; i < 24; i++ {
		b := array.NewDurationBuilder(alloc, durType)
		for r := 0; r < 2048; r++ {
			b.Append(arrow.Duration(r))
		}
		col := b.NewDurationArray()
		b.Release()
		var arr arrow.Array = col
		if i == 20 { // in the column's last run
			arr = blockingDurationArray{col, release}
		}
		batches = append(batches, array.NewRecord(schema, []arrow.Array{arr}, 2048))
		col.Release()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	time.AfterFunc(150*time.Millisecond, func() { close(release) })
	orig := encoderLimit
	encoderLimit = func() int64 { return 64 }
	defer func() { encoderLimit = orig }()
	var out bytes.Buffer
	_, err := streamMsgPackFromBatches(ctx, bufio.NewWriter(&out), schema, batches, 24*2048, 0, nil, time.Now(), "t")
	for _, b := range batches {
		b.Release()
	}
	alloc.AssertSize(t, 0)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errClientDisconnected) {
		t.Fatalf("err %v, want a deadline", err)
	}
}
