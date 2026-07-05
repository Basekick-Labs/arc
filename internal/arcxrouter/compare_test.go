// Compare-logic tests. Tagged (they build arcx-shaped arrow.Records). Verify the
// multiset comparison: shuffled bucket order still matches; a differing bucket,
// count, or row count is a mismatch; timestamp unit is normalized.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// aggRecord builds an arcx-shaped agg result: Timestamp(µs,UTC) + Int64
// count_star(). buckets are µs epochs.
func aggRecord(t *testing.T, buckets, counts []int64) arrow.Record {
	t.Helper()
	if len(buckets) != len(counts) {
		t.Fatal("buckets/counts length mismatch")
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "date_trunc('day', time)", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "count_star()", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	tsB := b.Field(0).(*array.TimestampBuilder)
	cB := b.Field(1).(*array.Int64Builder)
	for i := range buckets {
		tsB.Append(arrow.Timestamp(buckets[i]))
		cB.Append(counts[i])
	}
	return b.NewRecord()
}

// countRecord builds an arcx-shaped count(*) result: single Int64 row.
func countRecord(t *testing.T, total int64) arrow.Record {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "count_star()", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(total)
	return b.NewRecord()
}

// oracleAgg builds the DuckDB-side canonical result for an agg (no *sql.Rows in a
// unit test — canonicalResult is the comparison currency).
func oracleAgg(buckets, counts []int64) canonicalResult {
	rows := make([]canonicalRow, len(buckets))
	for i := range buckets {
		rows[i] = canonicalRow{bucketMicros: buckets[i], count: counts[i]}
	}
	return canonicalResult{rows: rows, numCols: 2}
}

func oracleCount(total int64) canonicalResult {
	return canonicalResult{rows: []canonicalRow{{bucketMicros: noBucket, count: total}}, isScalar: true, numCols: 1}
}

func TestCompare_AggMatchesShuffled(t *testing.T) {
	rec := aggRecord(t, []int64{100, 200, 300}, []int64{5, 6, 7})
	defer rec.Release()
	// Oracle has the same rows in a DIFFERENT order — multiset must still match.
	oracle := oracleAgg([]int64{300, 100, 200}, []int64{7, 5, 6})
	if diff := compareResults(rec, oracle); diff != "" {
		t.Fatalf("expected match, got diff: %s", diff)
	}
}

func TestCompare_AggBucketDiffers(t *testing.T) {
	rec := aggRecord(t, []int64{100, 200}, []int64{5, 6})
	defer rec.Release()
	oracle := oracleAgg([]int64{100, 999}, []int64{5, 6})
	if diff := compareResults(rec, oracle); diff == "" {
		t.Fatal("expected mismatch on differing bucket, got match")
	}
}

func TestCompare_AggCountDiffers(t *testing.T) {
	rec := aggRecord(t, []int64{100, 200}, []int64{5, 6})
	defer rec.Release()
	oracle := oracleAgg([]int64{100, 200}, []int64{5, 999})
	if diff := compareResults(rec, oracle); diff == "" {
		t.Fatal("expected mismatch on differing count, got match")
	}
}

func TestCompare_RowCountDiffers(t *testing.T) {
	rec := aggRecord(t, []int64{100, 200}, []int64{5, 6})
	defer rec.Release()
	oracle := oracleAgg([]int64{100}, []int64{5})
	if diff := compareResults(rec, oracle); diff == "" {
		t.Fatal("expected mismatch on differing row count, got match")
	}
}

func TestCompare_CountStarMatches(t *testing.T) {
	rec := countRecord(t, 42)
	defer rec.Release()
	if diff := compareResults(rec, oracleCount(42)); diff != "" {
		t.Fatalf("expected match, got diff: %s", diff)
	}
}

func TestCompare_CountStarDiffers(t *testing.T) {
	rec := countRecord(t, 42)
	defer rec.Release()
	if diff := compareResults(rec, oracleCount(43)); diff == "" {
		t.Fatal("expected mismatch on differing count, got match")
	}
}

func TestCompare_ShapeMismatch(t *testing.T) {
	// arcx returned a scalar count but the oracle is a 2-col agg → column-count
	// mismatch, caught before value comparison.
	rec := countRecord(t, 42)
	defer rec.Release()
	if diff := compareResults(rec, oracleAgg([]int64{100}, []int64{42})); diff == "" {
		t.Fatal("expected column-count mismatch, got match")
	}
}
