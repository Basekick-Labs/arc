// arcX moments-nd companion (docs/2026-11-12-moments-no-decline.md, M3): arcx pins
// covar_pop / covar_samp / regr_sxy to NaN on non-finite input because DuckDB's own
// answer there (±Inf or NaN) depends on its partitioning. arcx NaN vs DuckDB ±Inf on
// those three is the known divergence: oraclediff (WARN), never the mismatch alarm.
// Everything else about NaN / Inf stays strict.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestCompareAggPinnedNaNVersusInf(t *testing.T) {
	fields := []arrow.Field{
		{Name: "covar_pop(y, x)", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "corr(y, x)", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}
	items := []string{"covar_pop(y, x)", "corr(y, x)"}
	rec := func(cov, corr float64) arrow.Record {
		return oneRowRecord(t, fields, func(b *array.RecordBuilder) {
			b.Field(0).(*array.Float64Builder).Append(cov)
			b.Field(1).(*array.Float64Builder).Append(corr)
		})
	}
	oracle := func(cov, corr float64) []aggCell { return []aggCell{{f: cov}, {f: corr}} }
	nan, inf := math.NaN(), math.Inf(1)

	// The pinned divergence: arcx NaN, DuckDB ±Inf on covar_pop.
	for _, d := range []float64{inf, math.Inf(-1)} {
		if diff := compareAgg(rec(nan, nan), oracle(d, nan), items); !strings.HasPrefix(diff, "oraclediff:") {
			t.Fatalf("covar_pop NaN vs %v must be oraclediff, got %q", d, diff)
		}
	}
	// Both NaN: a match.
	if diff := compareAgg(rec(nan, nan), oracle(nan, nan), items); diff != "" {
		t.Fatalf("NaN vs NaN must match, got %q", diff)
	}
	// Narrow: the reverse direction (arcx Inf, DuckDB NaN) still alarms.
	if diff := compareAgg(rec(inf, nan), oracle(nan, nan), items); diff == "" || strings.HasPrefix(diff, "oraclediff:") {
		t.Fatalf("arcx Inf vs DuckDB NaN must alarm, got %q", diff)
	}
	// Narrow: arcx NaN vs a FINITE DuckDB value still alarms.
	if diff := compareAgg(rec(nan, nan), oracle(1.5, nan), items); diff == "" || strings.HasPrefix(diff, "oraclediff:") {
		t.Fatalf("arcx NaN vs finite must alarm, got %q", diff)
	}
	// Narrow: an unpinned function (corr) NaN vs Inf still alarms.
	if diff := compareAgg(rec(1.5, nan), oracle(1.5, inf), items); diff == "" || strings.HasPrefix(diff, "oraclediff:") {
		t.Fatalf("corr NaN vs Inf must alarm, got %q", diff)
	}
	// The pinned WARN never masks a real alarm on another cell.
	if diff := compareAgg(rec(nan, 0.5), oracle(inf, 0.9), items); diff == "" || strings.HasPrefix(diff, "oraclediff:") {
		t.Fatalf("a stable-cell difference must alarm alongside the pinned WARN, got %q", diff)
	}
}

func TestCompareGroupedPinnedNaNVersusInf(t *testing.T) {
	items := []string{"host", "regr_sxy(y, x)", "count(*)"}
	row := func(h string, v float64, n int64) groupedRow {
		return groupedRow{keys: []keyPart{{s: h}}, cells: []aggCell{{f: v}, {isInt: true, i: big.NewInt(n)}}}
	}
	nan, inf := math.NaN(), math.Inf(1)
	got := []groupedRow{row("a", nan, 3), row("b", 2.5, 4)}
	if diff := compareGroupedRows(got, []groupedRow{row("a", inf, 3), row("b", 2.5, 4)}, items, []int{0}, ""); !strings.HasPrefix(diff, "oraclediff:") {
		t.Fatalf("regr_sxy NaN vs Inf must be oraclediff, got %q", diff)
	}
	if diff := compareGroupedRows(got, []groupedRow{row("a", nan, 3), row("b", 2.5, 4)}, items, []int{0}, ""); diff != "" {
		t.Fatalf("NaN vs NaN must match, got %q", diff)
	}
	items2 := []string{"host", "regr_slope(y, x)", "count(*)"}
	if diff := compareGroupedRows(got, []groupedRow{row("a", inf, 3), row("b", 2.5, 4)}, items2, []int{0}, ""); diff == "" || strings.HasPrefix(diff, "oraclediff:") {
		t.Fatalf("regr_slope NaN vs Inf must alarm, got %q", diff)
	}
}
