// agg-6b companion, tagged: re-validation, the tolerance set, the regr_count
// UInt32 decode, and the oracle-unstable WARN class live in the tagged files.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"math/big"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestIsAggItemBinaryMoments(t *testing.T) {
	for _, item := range []string{"corr(y, x)", "covar_pop(a, b)", "regr_count(y, x)", "regr_r2(Y, X)"} {
		if !isAggItem(item) {
			t.Fatalf("isAggItem(%q) = false", item)
		}
	}
	for _, item := range []string{"corr(y)", "corr(y, x, z)", "corr(y,x)", "corr(y, x) FROM t"} {
		if isAggItem(item) {
			t.Fatalf("isAggItem(%q) = true", item)
		}
	}
}

func TestBinaryMomentTolerance(t *testing.T) {
	for _, item := range []string{"corr(y, x)", "covar_samp(y, x)", "regr_slope(y, x)", "regr_sxy(y, x)"} {
		if !aggItemTolerant(item) {
			t.Fatalf("aggItemTolerant(%q) = false", item)
		}
	}
	if aggItemTolerant("regr_count(y, x)") {
		t.Fatal("regr_count must compare exact")
	}
	for _, item := range []string{"covar_pop(y, x)", "covar_samp(y, x)", "regr_sxy(y, x)", "regr_intercept(y, x)"} {
		if !isOracleUnstableItem(item) {
			t.Fatalf("isOracleUnstableItem(%q) = false", item)
		}
	}
	for _, item := range []string{"corr(y, x)", "regr_slope(y, x)", "regr_r2(y, x)", "sum(x)"} {
		if isOracleUnstableItem(item) {
			t.Fatalf("isOracleUnstableItem(%q) = true", item)
		}
	}
}

func oneRowRecord(t *testing.T, fields []arrow.Field, build func(b *array.RecordBuilder)) arrow.Record {
	t.Helper()
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(fields, nil))
	defer b.Release()
	build(b)
	return b.NewRecord()
}

func TestCompareAggBinaryMoments(t *testing.T) {
	fields := []arrow.Field{
		{Name: "regr_count(y, x)", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "covar_pop(y, x)", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "corr(y, x)", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}
	items := []string{"regr_count(y, x)", "covar_pop(y, x)", "corr(y, x)"}
	rec := func(n uint32, cov, corr float64) arrow.Record {
		return oneRowRecord(t, fields, func(b *array.RecordBuilder) {
			b.Field(0).(*array.Uint32Builder).Append(n)
			b.Field(1).(*array.Float64Builder).Append(cov)
			b.Field(2).(*array.Float64Builder).Append(corr)
		})
	}
	oracle := func(n int64, cov, corr float64) []aggCell {
		return []aggCell{{isInt: true, i: big.NewInt(n)}, {f: cov}, {f: corr}}
	}
	// exact match, UInt32 decoded as an integer cell
	if d := compareAgg(rec(3, 1.5, 0.9), oracle(3, 1.5, 0.9), items); d != "" {
		t.Fatalf("expected match, got %q", d)
	}
	// covar_pop beyond tolerance: oracle-unstable → WARN class, not the alarm
	if d := compareAgg(rec(3, 1.5, 0.9), oracle(3, 1.5001, 0.9), items); !strings.HasPrefix(d, "oraclediff:") {
		t.Fatalf("covar_pop value drift must be oraclediff, got %q", d)
	}
	// corr beyond tolerance: a real mismatch
	if d := compareAgg(rec(3, 1.5, 0.9), oracle(3, 1.5, 0.9001), items); d == "" || strings.HasPrefix(d, "oraclediff:") {
		t.Fatalf("corr drift must alarm, got %q", d)
	}
	// regr_count differs: alarm
	if d := compareAgg(rec(4, 1.5, 0.9), oracle(3, 1.5, 0.9), items); d == "" || strings.HasPrefix(d, "oraclediff:") {
		t.Fatalf("regr_count difference must alarm, got %q", d)
	}
	// covar_pop drift AND a corr drift: the alarm wins
	if d := compareAgg(rec(3, 1.5, 0.9), oracle(3, 1.6, 0.95), items); d == "" || strings.HasPrefix(d, "oraclediff:") {
		t.Fatalf("a stable-cell difference must alarm even with an unstable-cell drift, got %q", d)
	}
}
