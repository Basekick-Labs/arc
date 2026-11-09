// agg-8 companion: any difference on a shape-statistic cell is an oracle defect
// (DuckDB 1.5.6 raw sums), filed as oraclediff — never a mismatch alarm.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestCompareAggShapeStatsAreOracleDiff(t *testing.T) {
	items := []string{"skewness(v)", "kurtosis(v)", "count(*)"}
	// Constant column: arcx NULL, DuckDB 1.5.6 garbage / NaN — WARN, not alarm.
	rec := aggRec(t, nil, nil, int64(5))
	for _, oracle := range [][]aggCell{
		{{f: -1.5e8}, {f: -4.5e16}, {isInt: true, i: big.NewInt(5)}},
		{{f: math.NaN()}, {isNull: true}, {isInt: true, i: big.NewInt(5)}},
	} {
		diff := compareAgg(rec, oracle, items)
		if !strings.HasPrefix(diff, "oraclediff:") {
			t.Fatalf("shape diff must be oraclediff, got %q", diff)
		}
	}
	// Equal within tolerance: a match.
	if diff := compareAgg(aggRec(t, 0.5, -1.2, int64(5)), []aggCell{{f: 0.5 + 1e-12}, {f: -1.2}, {isInt: true, i: big.NewInt(5)}}, items); diff != "" {
		t.Fatalf("unexpected diff %q", diff)
	}
	// A non-shape cell still alarms even next to a shape diff.
	diff := compareAgg(aggRec(t, 0.5, -1.2, int64(5)), []aggCell{{f: 9}, {f: -1.2}, {isInt: true, i: big.NewInt(6)}}, items)
	if diff == "" || strings.HasPrefix(diff, "oraclediff:") {
		t.Fatalf("a count mismatch must alarm, got %q", diff)
	}
}

// groupedShapeRec: rows of (host Utf8, skewness Float64, count Int64); a nil
// skewness is NULL.
func groupedShapeRec(t *testing.T, hosts []string, skew []*float64, counts []int64) arrow.Record {
	t.Helper()
	pool := memory.NewGoAllocator()
	hb := array.NewStringBuilder(pool)
	fb := array.NewFloat64Builder(pool)
	ib := array.NewInt64Builder(pool)
	for i := range hosts {
		hb.Append(hosts[i])
		if skew[i] == nil {
			fb.AppendNull()
		} else {
			fb.Append(*skew[i])
		}
		ib.Append(counts[i])
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "skewness(v)", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "count_star()", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	return array.NewRecord(schema, []arrow.Array{hb.NewArray(), fb.NewArray(), ib.NewArray()}, int64(len(hosts)))
}

// Review M2: the grouped comparator's shape exemption — a stuck-sensor group
// (arcx NULL vs DuckDB garbage) is oraclediff; a count mismatch in the same row
// still alarms; a kind difference on the shape cell alarms (review M1).
func TestCompareGroupedShapeStats(t *testing.T) {
	items := []string{"host", "skewness(v)", "count(*)"}
	half := 0.5
	rec := groupedShapeRec(t, []string{"a", "b"}, []*float64{nil, &half}, []int64{3, 4})
	row := func(h string, c aggCell, n int64) groupedRow {
		return groupedRow{keys: []keyPart{{s: h}}, cells: []aggCell{c, {isInt: true, i: big.NewInt(n)}}}
	}
	oracle := []groupedRow{row("a", aggCell{f: -1.5e8}, 3), row("b", aggCell{f: 0.5}, 4)}
	if diff := compareGrouped(rec, oracle, items, []int{0}, ""); !strings.HasPrefix(diff, "oraclediff:") {
		t.Fatalf("stuck-sensor shape diff must be oraclediff, got %q", diff)
	}
	bad := []groupedRow{row("a", aggCell{f: -1.5e8}, 3), row("b", aggCell{f: 0.5}, 5)}
	if diff := compareGrouped(rec, bad, items, []int{0}, ""); diff == "" || strings.HasPrefix(diff, "oraclediff:") {
		t.Fatalf("a count mismatch beside a shape diff must alarm, got %q", diff)
	}
	kind := []groupedRow{row("a", aggCell{isNull: true}, 3), row("b", aggCell{isInt: true, i: big.NewInt(0)}, 4)}
	if diff := compareGrouped(rec, kind, items, []int{0}, ""); diff == "" || strings.HasPrefix(diff, "oraclediff:") {
		t.Fatalf("a kind difference on a shape cell must alarm, got %q", diff)
	}
}
