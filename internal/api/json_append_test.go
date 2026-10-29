package api

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// goldenColumns builds one array of every type the encoder handles (and one
// it does not), with NULLs and edge values.
func goldenColumns(t *testing.T) []arrow.Array {
	t.Helper()
	mem := memory.NewGoAllocator()
	var cols []arrow.Array
	add := func(b array.Builder) { cols = append(cols, b.NewArray()); b.Release() }

	i64 := array.NewInt64Builder(mem)
	i64.AppendValues([]int64{0, -1, math.MaxInt64, math.MinInt64}, nil)
	i64.AppendNull()
	add(i64)
	i32 := array.NewInt32Builder(mem)
	i32.AppendValues([]int32{math.MinInt32, 7}, nil)
	add(i32)
	i16 := array.NewInt16Builder(mem)
	i16.AppendValues([]int16{-300, 300}, nil)
	add(i16)
	i8 := array.NewInt8Builder(mem)
	i8.AppendValues([]int8{-128, 127}, nil)
	add(i8)
	u64 := array.NewUint64Builder(mem)
	u64.AppendValues([]uint64{math.MaxUint64, 0}, nil)
	add(u64)
	u32 := array.NewUint32Builder(mem)
	u32.AppendValues([]uint32{math.MaxUint32}, nil)
	add(u32)
	u16 := array.NewUint16Builder(mem)
	u16.AppendValues([]uint16{65535}, nil)
	add(u16)
	u8 := array.NewUint8Builder(mem)
	u8.AppendValues([]uint8{255}, nil)
	add(u8)
	f64 := array.NewFloat64Builder(mem)
	f64.AppendValues([]float64{0, -0.0, 1.5, 1e21, 1e-7, 123456789.123456789, math.NaN(), math.Inf(1), math.Inf(-1), math.MaxFloat64, math.SmallestNonzeroFloat64}, nil)
	f64.AppendNull()
	add(f64)
	f32 := array.NewFloat32Builder(mem)
	f32.AppendValues([]float32{0.1, float32(math.NaN()), float32(math.Inf(-1)), 3.4e38}, nil)
	add(f32)
	bl := array.NewBooleanBuilder(mem)
	bl.AppendValues([]bool{true, false}, nil)
	bl.AppendNull()
	add(bl)
	for _, unit := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond} {
		ts := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: unit, TimeZone: "UTC"})
		ts.AppendValues([]arrow.Timestamp{0, 1, -1, 1760000000, 1760000000123, 1760000000123456, 1760000000123456789, 1760000000100000000, -62135596800 * 1000, 253402300799}, nil)
		ts.AppendNull()
		add(ts)
	}
	d32 := array.NewDate32Builder(mem)
	d32.AppendValues([]arrow.Date32{0, -1, 20000, -719162}, nil)
	add(d32)
	str := array.NewStringBuilder(mem)
	str.AppendValues([]string{"", "plain", `quote " back \ slash`, "nl\ncr\rtab\tbs\bff\f", "\x00\x01\x1f", "héllo ✓", "\xff\xfe invalid utf-8", "</script>"}, nil)
	str.AppendNull()
	add(str)
	ls := array.NewLargeStringBuilder(mem)
	ls.AppendValues([]string{"large", "\"x\""}, nil)
	add(ls)
	bin := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	bin.AppendValues([][]byte{{0x00, 0x22, 0x5c}, []byte("bytes"), {0xff, 0x80, 0xfe}, {}}, nil)
	bin.AppendNull()
	add(bin)
	// No case of its own: takes the ValueStr fallback.
	dur := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Millisecond})
	dur.AppendValues([]arrow.Duration{1500, -2}, nil)
	add(dur)
	return cols
}

// The append encoder produces exactly what writeArrowValue (the encoder it
// replaced, oracleWriteArrowValue) wrote, for every type, NULL and edge value.
func TestJSONValueEncoderMatchesWriteArrowValue(t *testing.T) {
	cols := goldenColumns(t)
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	enc := &jsonValueEncoder{}
	for ci, col := range cols {
		for row := 0; row < col.Len(); row++ {
			var want bytes.Buffer
			bw := bufio.NewWriter(&want)
			oracleWriteArrowValue(bw, make([]byte, 0, 128), col, row)
			_ = bw.Flush()
			got := enc.appendValue(nil, col, row)
			if !bytes.Equal(got, want.Bytes()) {
				t.Errorf("column %d (%s) row %d: got %q, want %q", ci, col.DataType(), row, got, want.Bytes())
			}
		}
	}
}

// appendTime matches t.UTC().AppendFormat(time.RFC3339Nano) for random
// instants across every unit's range, with the per-second cache warm and
// cold, including pre-1970 sub-second values and years outside 0-9999.
func TestJSONValueEncoderTimeMatchesAppendFormat(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	enc := &jsonValueEncoder{}
	check := func(tm time.Time) {
		want := tm.UTC().AppendFormat(nil, time.RFC3339Nano)
		if got := enc.appendTime(nil, tm); !bytes.Equal(got, want) {
			t.Fatalf("%v: got %q, want %q", tm, got, want)
		}
	}
	for i := 0; i < 200000; i++ {
		var tm time.Time
		switch i % 5 {
		case 0: // any nanosecond instant representable in int64 ns
			tm = time.Unix(0, r.Int63()-r.Int63())
		case 1: // seconds far outside 1970 +- 292 years, years < 0 and > 9999
			tm = time.Unix(r.Int63n(1<<40)-1<<39, int64(r.Intn(1e9)))
		case 2: // consecutive instants within and across seconds (cache hits)
			tm = time.Unix(1760000000+int64(i/7), int64(i%7)*125000000)
		case 3: // round fractions
			tm = time.Unix(r.Int63n(4e9)-2e9, int64(r.Intn(10))*1e8)
		default: // pre-1970 sub-second
			tm = time.Unix(-int64(r.Intn(1e9)), -int64(r.Intn(1e9)))
		}
		check(tm)
	}
	for _, tm := range []time.Time{time.Time{}, time.Unix(-62135596800, 0), time.Unix(253402300799, 999999999), time.Unix(253402300800, 0), time.Unix(-62135596801, 1)} {
		check(tm)
	}
}

func FuzzJSONValueEncoderTime(f *testing.F) {
	f.Add(int64(0), int64(0))
	f.Add(int64(-1), int64(999999999))
	f.Add(int64(1760000000), int64(123456789))
	f.Fuzz(func(t *testing.T, sec, nsec int64) {
		tm := time.Unix(sec, nsec)
		want := tm.UTC().AppendFormat(nil, time.RFC3339Nano)
		enc := &jsonValueEncoder{}
		for i := 0; i < 2; i++ { // cold, then cached
			if got := enc.appendTime(nil, tm); !bytes.Equal(got, want) {
				t.Fatalf("%d/%d: got %q, want %q", sec, nsec, got, want)
			}
		}
	})
}

// oracleWriteArrowValue is the writer the append encoder replaced, kept as
// the reference its bytes are checked against.
func oracleWriteArrowValue(w *bufio.Writer, scratch []byte, col arrow.Array, row int) []byte {
	if col.IsNull(row) {
		w.WriteString("null")
		return scratch
	}

	switch c := col.(type) {
	case *array.Int64:
		scratch = strconv.AppendInt(scratch[:0], c.Value(row), 10)
		w.Write(scratch)
	case *array.Int32:
		scratch = strconv.AppendInt(scratch[:0], int64(c.Value(row)), 10)
		w.Write(scratch)
	case *array.Int16:
		scratch = strconv.AppendInt(scratch[:0], int64(c.Value(row)), 10)
		w.Write(scratch)
	case *array.Int8:
		scratch = strconv.AppendInt(scratch[:0], int64(c.Value(row)), 10)
		w.Write(scratch)
	case *array.Uint64:
		scratch = strconv.AppendUint(scratch[:0], c.Value(row), 10)
		w.Write(scratch)
	case *array.Uint32:
		scratch = strconv.AppendUint(scratch[:0], uint64(c.Value(row)), 10)
		w.Write(scratch)
	case *array.Uint16:
		scratch = strconv.AppendUint(scratch[:0], uint64(c.Value(row)), 10)
		w.Write(scratch)
	case *array.Uint8:
		scratch = strconv.AppendUint(scratch[:0], uint64(c.Value(row)), 10)
		w.Write(scratch)
	case *array.Float64:
		v := c.Value(row)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			w.WriteString("null")
		} else {
			scratch = strconv.AppendFloat(scratch[:0], v, 'f', -1, 64)
			w.Write(scratch)
		}
	case *array.Float32:
		v := float64(c.Value(row))
		if math.IsNaN(v) || math.IsInf(v, 0) {
			w.WriteString("null")
		} else {
			scratch = strconv.AppendFloat(scratch[:0], v, 'f', -1, 64)
			w.Write(scratch)
		}
	case *array.Boolean:
		if c.Value(row) {
			w.WriteString("true")
		} else {
			w.WriteString("false")
		}
	case *array.Timestamp:
		ts := c.Value(row)
		unit := c.DataType().(*arrow.TimestampType).Unit
		t := ts.ToTime(unit)
		w.WriteByte('"')
		scratch = t.UTC().AppendFormat(scratch[:0], time.RFC3339Nano)
		w.Write(scratch)
		w.WriteByte('"')
	case *array.Date32:
		d := c.Value(row)
		t := d.ToTime()
		w.WriteByte('"')
		scratch = t.UTC().AppendFormat(scratch[:0], time.RFC3339Nano)
		w.Write(scratch)
		w.WriteByte('"')
	case *array.String:
		writeJSONString(w, scratch, c.Value(row))
	case *array.LargeString:
		writeJSONString(w, scratch, c.Value(row))
	case *array.Binary:
		// #440: base64, not the raw bytes the replaced encoder wrote.
		w.WriteByte('"')
		w.WriteString(base64.StdEncoding.EncodeToString(c.Value(row)))
		w.WriteByte('"')
	default:
		// Fallback: use ValueStr for any unhandled Arrow types
		writeJSONString(w, scratch, col.ValueStr(row))
	}

	return scratch
}

// A BLOB or GEOMETRY value with bytes that are not UTF-8 is valid JSON, and
// decodes back to the stored bytes (#440).
func TestJSONValueEncoderBinaryIsBase64(t *testing.T) {
	mem := memory.NewGoAllocator()
	b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	want := [][]byte{{0x00, 0xff, 0x80}, {}, []byte("POINT")}
	b.AppendValues(want, nil)
	b.AppendNull()
	col := b.NewArray()
	defer col.Release()
	enc := &jsonValueEncoder{}
	for row := 0; row < col.Len(); row++ {
		got := enc.appendValue(nil, col, row)
		var s *string
		if err := json.Unmarshal(got, &s); err != nil {
			t.Fatalf("row %d: %q is not valid JSON: %v", row, got, err)
		}
		if row == len(want) {
			if s != nil {
				t.Fatalf("NULL row = %q, want null", *s)
			}
			continue
		}
		dec, err := base64.StdEncoding.DecodeString(*s)
		if err != nil || !bytes.Equal(dec, want[row]) {
			t.Fatalf("row %d: %q decodes to %x (%v), want %x", row, *s, dec, err, want[row])
		}
	}
}
