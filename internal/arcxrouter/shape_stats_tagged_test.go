// agg-8 companion: any difference on a shape-statistic cell is an oracle defect
// (DuckDB 1.5.6 raw sums), filed as oraclediff — never a mismatch alarm.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"math"
	"math/big"
	"strings"
	"testing"
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
