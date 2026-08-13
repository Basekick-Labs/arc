//go:build duckdb_arrow

package api

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// buildTestRecord creates a record with a low-cardinality string column, a
// high-cardinality string column, an int column, and scattered nulls.
func buildTestRecord(t *testing.T, rows int, batchIdx int) arrow.Record {
	t.Helper()
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "symbol", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "url", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	sb := array.NewStringBuilder(mem)
	ub := array.NewStringBuilder(mem)
	vb := array.NewInt64Builder(mem)
	defer sb.Release()
	defer ub.Release()
	defer vb.Release()

	symbols := []string{"AAPL", "MSFT", "GOOG", "AMZN", "NVDA"}
	for i := 0; i < rows; i++ {
		if i%97 == 0 {
			sb.AppendNull()
		} else {
			// batchIdx shifts the distribution so later batches introduce
			// dictionary values unseen in the first batch (delta path).
			sb.Append(symbols[(i+batchIdx)%len(symbols)])
		}
		ub.Append(fmt.Sprintf("https://example.com/%d/%d", batchIdx, i)) // all unique
		vb.Append(int64(i))
	}

	sArr := sb.NewArray()
	uArr := ub.NewArray()
	vArr := vb.NewArray()
	defer sArr.Release()
	defer uArr.Release()
	defer vArr.Release()
	return array.NewRecord(schema, []arrow.Array{sArr, uArr, vArr}, int64(rows))
}

// TestArrowDictTransformerRoundTrip encodes multiple batches through the
// transformer + IPC writer with deltas and reads them back, asserting the
// decoded values are identical to the originals, the low-cardinality column
// became dictionary-typed, and the high-cardinality column stayed plain.
func TestArrowDictTransformerRoundTrip(t *testing.T) {
	const rows = 1000
	const batches = 3

	originals := make([]arrow.Record, batches)
	for b := 0; b < batches; b++ {
		originals[b] = buildTestRecord(t, rows, b)
		defer originals[b].Release()
	}

	xform := newArrowDictTransformer(originals[0], nil)
	if xform == nil {
		t.Fatal("expected transformer (symbol column qualifies)")
	}
	defer xform.release()

	// Schema expectations: symbol dictionary-encoded, url and v untouched.
	if _, ok := xform.schema.Field(0).Type.(*arrow.DictionaryType); !ok {
		t.Fatalf("symbol should be dictionary-typed, got %s", xform.schema.Field(0).Type)
	}
	if xform.schema.Field(1).Type.ID() != arrow.STRING {
		t.Fatalf("url (all-unique) should stay plain, got %s", xform.schema.Field(1).Type)
	}
	if xform.schema.Field(2).Type.ID() != arrow.INT64 {
		t.Fatalf("v should stay int64, got %s", xform.schema.Field(2).Type)
	}

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(xform.schema))
	for b := 0; b < batches; b++ {
		enc, err := xform.transform(originals[b])
		if err != nil {
			t.Fatalf("transform batch %d: %v", b, err)
		}
		if err := w.Write(enc); err != nil {
			enc.Release()
			t.Fatalf("ipc write batch %d: %v", b, err)
		}
		enc.Release()
	}
	if err := w.Close(); err != nil {
		t.Fatalf("ipc close: %v", err)
	}

	// Read back and compare values.
	r, err := ipc.NewReader(&buf)
	if err != nil {
		t.Fatalf("ipc reader: %v", err)
	}
	defer r.Release()

	for b := 0; b < batches; b++ {
		if !r.Next() {
			t.Fatalf("missing batch %d on read-back: %v", b, r.Err())
		}
		got := r.Record()
		orig := originals[b]
		if got.NumRows() != orig.NumRows() {
			t.Fatalf("batch %d rows %d != %d", b, got.NumRows(), orig.NumRows())
		}

		dict, ok := got.Column(0).(*array.Dictionary)
		if !ok {
			t.Fatalf("batch %d symbol not a dictionary array: %T", b, got.Column(0))
		}
		dictVals := dict.Dictionary().(*array.String)
		origSym := orig.Column(0).(*array.String)
		for i := 0; i < int(got.NumRows()); i++ {
			if origSym.IsNull(i) != dict.IsNull(i) {
				t.Fatalf("batch %d row %d null mismatch", b, i)
			}
			if origSym.IsNull(i) {
				continue
			}
			if gotV := dictVals.Value(dict.GetValueIndex(i)); gotV != origSym.Value(i) {
				t.Fatalf("batch %d row %d symbol %q != %q", b, i, gotV, origSym.Value(i))
			}
		}

		gotURL := got.Column(1).(*array.String)
		origURL := orig.Column(1).(*array.String)
		for i := 0; i < int(got.NumRows()); i++ {
			if gotURL.Value(i) != origURL.Value(i) {
				t.Fatalf("batch %d row %d url mismatch", b, i)
			}
		}
	}
	if r.Next() {
		t.Fatal("unexpected extra batch")
	}
}

// TestArrowDictTransformerSkipsSmallAndHighCardinality pins the adaptive
// gates: tiny first batches and high-cardinality-only schemas produce no
// transformer.
func TestArrowDictTransformerSkips(t *testing.T) {
	small := buildTestRecord(t, dictMinRows-1, 0)
	defer small.Release()
	if newArrowDictTransformer(small, nil) != nil {
		t.Fatal("tiny first batch must not enable dictionary encoding")
	}

	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "url", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	ub := array.NewStringBuilder(mem)
	defer ub.Release()
	for i := 0; i < 1000; i++ {
		ub.Append(fmt.Sprintf("unique-%d", i))
	}
	uArr := ub.NewArray()
	defer uArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{uArr}, 1000)
	defer rec.Release()
	if newArrowDictTransformer(rec, nil) != nil {
		t.Fatal("all-unique string column must not enable dictionary encoding")
	}
}

// TestArrowDictTransformerIgnoresLargeString is the B1 regression guard:
// arrow-go v18 has NO LargeString dictionary builder — constructing one
// panics, and a panic in the fasthttp stream goroutine kills the process.
// A low-cardinality LargeString column must therefore be left plain.
func TestArrowDictTransformerIgnoresLargeString(t *testing.T) {
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "sym", Type: arrow.BinaryTypes.LargeString, Nullable: true},
	}, nil)
	lb := array.NewLargeStringBuilder(mem)
	defer lb.Release()
	for i := 0; i < 1000; i++ {
		lb.Append([]string{"a", "b", "c"}[i%3]) // low cardinality — would qualify if supported
	}
	lArr := lb.NewArray()
	defer lArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{lArr}, 1000)
	defer rec.Release()

	if newArrowDictTransformer(rec, nil) != nil {
		t.Fatal("LargeString columns must not be dictionary-encoded (no builder support in arrow-go)")
	}
}

// TestArrowDictTransformerAllNullFirstBatch covers the empty-first-dictionary
// edge: a qualifying column whose first batch is entirely null produces an
// empty dictionary, and later batches grow it — the dictionary-emission path
// the main round-trip doesn't touch.
func TestArrowDictTransformerAllNullFirstBatch(t *testing.T) {
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "sym", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	buildBatch := func(vals []string, nulls bool) arrow.Record {
		sb := array.NewStringBuilder(mem)
		defer sb.Release()
		for i := 0; i < 500; i++ {
			if nulls {
				sb.AppendNull()
			} else {
				sb.Append(vals[i%len(vals)])
			}
		}
		arr := sb.NewArray()
		defer arr.Release()
		return array.NewRecord(schema, []arrow.Array{arr}, 500)
	}

	first := buildBatch(nil, true)
	defer first.Release()
	second := buildBatch([]string{"x", "y"}, false)
	defer second.Release()

	xform := newArrowDictTransformer(first, nil)
	if xform == nil {
		t.Fatal("all-null column should qualify (0 uniques)")
	}
	defer xform.release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(xform.schema))
	for _, rec := range []arrow.Record{first, second} {
		enc, err := xform.transform(rec)
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		if err := w.Write(enc); err != nil {
			enc.Release()
			t.Fatalf("write: %v", err)
		}
		enc.Release()
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := ipc.NewReader(&buf)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer r.Release()

	if !r.Next() {
		t.Fatalf("missing batch 1: %v", r.Err())
	}
	b1 := r.Record().Column(0).(*array.Dictionary)
	for i := 0; i < 500; i++ {
		if !b1.IsNull(i) {
			t.Fatalf("batch 1 row %d should be null", i)
		}
	}
	if !r.Next() {
		t.Fatalf("missing batch 2: %v", r.Err())
	}
	b2 := r.Record().Column(0).(*array.Dictionary)
	vals := b2.Dictionary().(*array.String)
	want := []string{"x", "y"}
	for i := 0; i < 500; i++ {
		if got := vals.Value(b2.GetValueIndex(i)); got != want[i%2] {
			t.Fatalf("batch 2 row %d: %q != %q", i, got, want[i%2])
		}
	}
}
