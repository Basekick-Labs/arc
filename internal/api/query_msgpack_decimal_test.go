//go:build duckdb_arrow

package api

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestMsgPackDecimal_TypesMatchValues is the regression test for the
// contract bug found in plan review: the msgpack "types" array advertised
// decimal(38, 0) while the encoder — which has no *array.Decimal128 case —
// sent the column through encodeFallbackColumn as msgpack *strings*.
//
// DuckDB returns SUM(integer) as decimal(38,0) and AVG as decimal(x,y), so
// this hit ordinary analytical queries, not exotic ones. The Arrow IPC path
// already normalized decimals (normalizeDecimalSchema); the msgpack path
// did not, so the two binary formats also disagreed with each other for the
// same SQL.
//
// Reverting the normalizeDecimalSchema call in executeQueryArrowMsgPack (or
// in msgpackStreamToBytes, which mirrors it) fails this test: types come
// back as "decimal(38, 0)"/"decimal(10, 2)" and the values decode as
// strings rather than numbers.
func TestMsgPackDecimal_TypesMatchValues(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		// SUM(int) shape: scale 0 → int64
		{Name: "total", Type: &arrow.Decimal128Type{Precision: 38, Scale: 0}},
		// AVG / DECIMAL(10,2) shape: scale > 0 → float64
		{Name: "price", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}},
	}, nil)

	totalB := array.NewDecimal128Builder(alloc, &arrow.Decimal128Type{Precision: 38, Scale: 0})
	defer totalB.Release()
	totalB.Append(decimal128.FromI64(3))
	totalB.Append(decimal128.FromI64(-7))

	priceB := array.NewDecimal128Builder(alloc, &arrow.Decimal128Type{Precision: 10, Scale: 2})
	defer priceB.Release()
	// Unscaled 123 at scale 2 == 1.23
	priceB.Append(decimal128.FromI64(123))
	priceB.Append(decimal128.FromI64(4550))

	totalArr := totalB.NewArray()
	defer totalArr.Release()
	priceArr := priceB.NewArray()
	defer priceArr.Release()

	batch := array.NewRecord(schema, []arrow.Array{totalArr, priceArr}, 2)
	defer batch.Release()

	reader := newSimpleRecordReader(schema, []arrow.Record{batch})
	data, rowCount := msgpackStreamToBytes(reader, 0)
	if rowCount != 2 {
		t.Fatalf("rowCount = %d, want 2", rowCount)
	}

	result := decodeMsgpack(t, data)

	// 1. The advertised types must be the normalized ones, not decimal.
	types, ok := result["types"].([]interface{})
	if !ok {
		t.Fatalf("expected 'types' array, got %T", result["types"])
	}
	if len(types) != 2 {
		t.Fatalf("len(types) = %d, want 2", len(types))
	}
	if got := types[0]; got != "int64" {
		t.Errorf("types[0] = %v, want \"int64\" (scale-0 decimal must normalize to int64)", got)
	}
	if got := types[1]; got != "float64" {
		t.Errorf("types[1] = %v, want \"float64\" (scaled decimal must normalize to float64)", got)
	}

	// 2. The values must actually BE those types on the wire — this is
	//    the half that was broken. Strings here mean the contract lies.
	cols := colsOf(t, result)

	for i, v := range cols[0] {
		if _, isStr := v.(string); isStr {
			t.Fatalf("total[%d] decoded as string %q — decimal values must be numeric on the wire", i, v)
		}
	}
	if got := toInt64(t, cols[0][0]); got != 3 {
		t.Errorf("total[0] = %d, want 3", got)
	}
	if got := toInt64(t, cols[0][1]); got != -7 {
		t.Errorf("total[1] = %d, want -7", got)
	}

	for i, v := range cols[1] {
		if _, isStr := v.(string); isStr {
			t.Fatalf("price[%d] decoded as string %q — decimal values must be numeric on the wire", i, v)
		}
	}
	if got := toFloat64(t, cols[1][0]); got < 1.2299 || got > 1.2301 {
		t.Errorf("price[0] = %v, want ~1.23 (unscaled 123 at scale 2)", got)
	}
	if got := toFloat64(t, cols[1][1]); got < 45.4999 || got > 45.5001 {
		t.Errorf("price[1] = %v, want ~45.50 (unscaled 4550 at scale 2)", got)
	}
}

// TestMsgPackDecimal_NoDecimalIsZeroOverhead guards the fast path: a schema
// with no decimal columns must produce a nil castInfo so the drain skips
// casting entirely.
func TestMsgPackDecimal_NoDecimalIsZeroOverhead(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "host", Type: arrow.BinaryTypes.String},
	}, nil)
	if info := normalizeDecimalSchema(schema); info != nil {
		t.Errorf("normalizeDecimalSchema returned non-nil for a decimal-free schema; the drain would cast needlessly")
	}
}

func toInt64(t *testing.T, v interface{}) int64 {
	t.Helper()
	switch n := v.(type) {
	case int64:
		return n
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case uint64:
		return int64(n)
	case uint8:
		return int64(n)
	case uint16:
		return int64(n)
	case uint32:
		return int64(n)
	default:
		t.Fatalf("expected an integer, got %T (%v)", v, v)
		return 0
	}
}

func toFloat64(t *testing.T, v interface{}) float64 {
	t.Helper()
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	default:
		t.Fatalf("expected a float, got %T (%v)", v, v)
		return 0
	}
}
