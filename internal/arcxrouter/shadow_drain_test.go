// UNTAGGED on purpose — see shadow_drain.go. If this file were behind
// `arcx_engine` it would not run in CI, which is exactly how the truncation bug
// below reached production.
package arcxrouter

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// multiBatchReader builds a deterministic MULTI-batch reader with the column types
// arcx actually exports on a scan: Utf8, Float64, Int64 and Timestamp(µs, UTC).
// `sizes` gives the row count of each batch.
func multiBatchReader(t *testing.T, sizes []int) (array.RecordReader, int) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "t", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	}, nil)

	var recs []arrow.Record
	total := 0
	row := 0
	for _, n := range sizes {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		hb := b.Field(0).(*array.StringBuilder)
		vb := b.Field(1).(*array.Float64Builder)
		nb := b.Field(2).(*array.Int64Builder)
		tb := b.Field(3).(*array.TimestampBuilder)
		for i := 0; i < n; i++ {
			// Every 7th row NULL in each column: the drain must preserve nulls, and
			// a comparison that silently dropped them would still "match" on counts.
			if row%7 == 0 {
				hb.AppendNull()
				vb.AppendNull()
				nb.AppendNull()
				tb.AppendNull()
			} else {
				hb.Append("host-" + string(rune('a'+row%26)))
				vb.Append(float64(row) + 0.5)
				nb.Append(int64(row))
				tb.Append(arrow.Timestamp(int64(row) * 1_000_000))
			}
			row++
		}
		rec := b.NewRecord()
		b.Release()
		recs = append(recs, rec)
		total += n
	}
	rr, err := array.NewRecordReader(schema, recs)
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	for _, r := range recs {
		r.Release()
	}
	return rr, total
}

// THE regression test. Before the fix, drainReaderToRecord built an array.Table and
// took ONE record from array.NewTableReader — which clamps to the CHUNK boundary, so
// an N-batch result was silently truncated to its FIRST batch. On the real corpus a
// 50-batch / 60,875-row scan was compared as 765 rows and shadow reported a MISMATCH
// for a result arcx had computed correctly.
//
// The single-batch case always worked (an early return), which is why every existing
// fixture passed. A multi-batch fixture is the whole point.
func TestDrainReaderToRecordKeepsEveryBatch(t *testing.T) {
	for _, sizes := range [][]int{
		{765, 1200, 1100, 1200},  // the shape of the real failure: first batch smallest
		{1, 1, 1, 1, 1, 1, 1, 1}, // many tiny batches
		{3, 0, 5, 0, 7},          // zero-row batches interleaved
		{10},                     // single batch (the path that always worked)
		{1000, 1},                // uneven tail
	} {
		rr, want := multiBatchReader(t, sizes)
		rec, err := drainReaderToRecord(rr, ShadowMaxRows)
		if err != nil {
			t.Fatalf("sizes=%v: unexpected error: %v", sizes, err)
		}
		if got := int(rec.NumRows()); got != want {
			t.Fatalf("sizes=%v: drained %d rows, want %d (truncation regression)", sizes, got, want)
		}
		if tz := rec.Schema().Field(3).Type.(*arrow.TimestampType).TimeZone; tz != "UTC" {
			t.Fatalf("sizes=%v: timestamp timezone lost: %q", sizes, tz)
		}
		if rec.Schema().NumFields() != 4 {
			t.Fatalf("sizes=%v: schema lost columns: %v", sizes, rec.Schema())
		}
		// Values must survive concatenation in order, nulls included.
		host := rec.Column(0).(*array.String)
		val := rec.Column(1).(*array.Float64)
		n := rec.Column(2).(*array.Int64)
		ts := rec.Column(3).(*array.Timestamp)
		for i := 0; i < int(rec.NumRows()); i++ {
			if i%7 == 0 {
				if host.IsValid(i) || n.IsValid(i) {
					t.Fatalf("sizes=%v: row %d should be NULL", sizes, i)
				}
				continue
			}
			if !n.IsValid(i) || n.Value(i) != int64(i) {
				t.Fatalf("sizes=%v: row %d = %v, want %d (order or value lost)", sizes, i, n.Value(i), i)
			}
			// float64 and timestamp-with-tz must survive concatenation too — the
			// column types arcx actually exports, and the ones a Table round-trip
			// was most likely to mangle.
			if !val.IsValid(i) || val.Value(i) != float64(i)+0.5 {
				t.Fatalf("sizes=%v: row %d float = %v, want %v", sizes, i, val.Value(i), float64(i)+0.5)
			}
			if !ts.IsValid(i) || int64(ts.Value(i)) != int64(i)*1_000_000 {
				t.Fatalf("sizes=%v: row %d ts = %v, want %v", sizes, i, ts.Value(i), int64(i)*1_000_000)
			}
		}
		rec.Release()
		rr.Release()
	}
}

// The cap is enforced inside the drain loop, before any concatenation, and reports a
// SKIP sentinel — never a mismatch (gotcha #4).
func TestDrainReaderToRecordHonoursTheCap(t *testing.T) {
	rr, _ := multiBatchReader(t, []int{100, 100, 100})
	_, err := drainReaderToRecord(rr, 150)
	if !errors.Is(err, errShadowTruncated) {
		t.Fatalf("expected errShadowTruncated, got %v", err)
	}
	rr.Release()

	// Exactly at the cap must still succeed.
	rr2, want := multiBatchReader(t, []int{100, 100})
	rec, err := drainReaderToRecord(rr2, want)
	if err != nil {
		t.Fatalf("at-cap drain failed: %v", err)
	}
	if int(rec.NumRows()) != want {
		t.Fatalf("at-cap drained %d, want %d", rec.NumRows(), want)
	}
	rec.Release()
	rr2.Release()
}

// A zero-batch reader yields an empty record carrying the schema, not a nil.
func TestDrainReaderToRecordEmpty(t *testing.T) {
	rr, _ := multiBatchReader(t, []int{})
	rec, err := drainReaderToRecord(rr, ShadowMaxRows)
	if err != nil {
		t.Fatalf("empty drain: %v", err)
	}
	if rec.NumRows() != 0 || rec.Schema().NumFields() != 4 {
		t.Fatalf("empty drain gave rows=%d fields=%d", rec.NumRows(), rec.Schema().NumFields())
	}
	rec.Release()
	rr.Release()
}
