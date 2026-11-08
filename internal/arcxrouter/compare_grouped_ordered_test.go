// res-2 companion: the ordered, tie-aware grouped comparison.

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

type topRow struct {
	host string
	n    int64
	avg  float64
}

func topRec(rows ...topRow) arrow.Record {
	hb := array.NewStringBuilder(memory.DefaultAllocator)
	ib := array.NewInt64Builder(memory.DefaultAllocator)
	fb := array.NewFloat64Builder(memory.DefaultAllocator)
	for _, r := range rows {
		hb.Append(r.host)
		ib.Append(r.n)
		fb.Append(r.avg)
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "count_star()", Type: arrow.PrimitiveTypes.Int64},
		{Name: "avg(x)", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	return array.NewRecord(schema, []arrow.Array{hb.NewArray(), ib.NewArray(), fb.NewArray()}, int64(len(rows)))
}

func topOracle(rows ...topRow) []groupedRow {
	out := make([]groupedRow, len(rows))
	for i, r := range rows {
		out[i] = groupedRow{keys: []keyPart{{s: r.host}},
			cells: []aggCell{{isInt: true, i: big.NewInt(r.n)}, {f: r.avg}}}
	}
	return out
}

func TestCompareGroupedOrdered(t *testing.T) {
	items := []string{"host", "count(*)", "avg(x)"}
	byCountDesc := []OrderKey{{Pos: 2, Desc: true}}
	byAvg := []OrderKey{{Pos: 3}}
	byAvgDesc := []OrderKey{{Pos: 3, Desc: true}}
	nan := math.NaN()
	for _, c := range []struct {
		name     string
		arcx     []topRow
		duck     []topRow
		keys     []OrderKey
		hasLimit bool
		limit    int
		want     string // "" match, "tiediff:", or "alarm"
	}{
		{"identical", []topRow{{"a", 5, 1}, {"b", 3, 2}}, []topRow{{"a", 5, 1}, {"b", 3, 2}}, byCountDesc, true, 2, ""},
		{"tied rows in another order", []topRow{{"a", 5, 1}, {"b", 5, 2}, {"c", 3, 3}},
			[]topRow{{"b", 5, 2}, {"a", 5, 1}, {"c", 3, 3}}, byCountDesc, false, 0, ""},
		{"arcx out of order", []topRow{{"a", 3, 1}, {"b", 5, 2}}, []topRow{{"b", 5, 2}, {"a", 3, 1}}, byCountDesc, false, 0, "alarm"},
		{"different group tied at the cut", []topRow{{"a", 6, 1}, {"b", 5, 2}}, []topRow{{"a", 6, 1}, {"c", 5, 3}},
			byCountDesc, true, 2, "tiediff:"},
		{"cut rows not tied with each other", []topRow{{"a", 6, 1}, {"b", 5, 2}}, []topRow{{"a", 6, 1}, {"c", 4, 3}},
			byCountDesc, true, 2, "alarm"},
		{"one-sided tie, no LIMIT", []topRow{{"a", 6, 1}, {"b", 5, 2}}, []topRow{{"a", 6, 1}, {"c", 5, 3}},
			byCountDesc, false, 0, "alarm"},
		{"one-sided tie, result shorter than LIMIT", []topRow{{"a", 6, 1}, {"b", 5, 2}}, []topRow{{"a", 6, 1}, {"c", 5, 3}},
			byCountDesc, true, 3, "alarm"},
		{"one-sided row before the cut class", []topRow{{"a", 6, 1}, {"b", 5, 2}}, []topRow{{"z", 6, 1}, {"b", 5, 2}},
			byCountDesc, true, 2, "alarm"},
		{"cell differs inside a class", []topRow{{"a", 5, 1}, {"b", 5, 2}}, []topRow{{"b", 5, 2}, {"a", 5, 7}},
			byCountDesc, false, 0, "alarm"},
		{"tolerant last key: near-tie in another order", []topRow{{"a", 1, 1.0}, {"b", 1, 1.0 + 1e-12}},
			[]topRow{{"b", 1, 1.0}, {"a", 1, 1.0 + 5e-13}}, byAvg, false, 0, ""},
		{"tolerant key: clear swap alarms", []topRow{{"a", 1, 1.0}, {"b", 1, 2.0}},
			[]topRow{{"b", 1, 1.0}, {"a", 1, 2.0}}, byAvg, false, 0, "alarm"},
		{"NaN sorts largest", []topRow{{"a", 1, nan}, {"b", 1, 2}, {"c", 1, 1}},
			[]topRow{{"a", 1, nan}, {"b", 1, 2}, {"c", 1, 1}}, byAvgDesc, false, 0, ""},
		{"NaN misplaced on arcx", []topRow{{"b", 1, 2}, {"a", 1, nan}, {"c", 1, 1}},
			[]topRow{{"a", 1, nan}, {"b", 1, 2}, {"c", 1, 1}}, byAvgDesc, false, 0, "alarm"},
		{"-0 ties +0", []topRow{{"a", 1, math.Copysign(0, -1)}, {"b", 1, 0}},
			[]topRow{{"b", 1, 0}, {"a", 1, math.Copysign(0, -1)}}, byAvg, false, 0, ""},
		{"row count", []topRow{{"a", 1, 1}}, []topRow{{"a", 1, 1}, {"b", 1, 1}}, byAvg, false, 0, "alarm"},
	} {
		diff := compareGroupedOrdered(topRec(c.arcx...), topOracle(c.duck...), items, []int{0}, c.keys, c.hasLimit, c.limit)
		switch c.want {
		case "":
			if diff != "" {
				t.Errorf("%s: want match, got %q", c.name, diff)
			}
		case "tiediff:":
			if !strings.HasPrefix(diff, "tiediff:") {
				t.Errorf("%s: want tiediff, got %q", c.name, diff)
			}
		default:
			if diff == "" || strings.Contains(diff, "diff:") {
				t.Errorf("%s: want an alarm, got %q", c.name, diff)
			}
		}
	}
}

// ORDER BY a key column: the order is read from the key part, and the cell
// index of a later aggregate skips the key correctly.
func TestCompareGroupedOrderedByKey(t *testing.T) {
	items := []string{"host", "count(*)", "avg(x)"}
	keys := []OrderKey{{Pos: 1, Desc: true}}
	rows := []topRow{{"c", 1, 1}, {"b", 2, 2}, {"a", 3, 3}}
	if diff := compareGroupedOrdered(topRec(rows...), topOracle(rows...), items, []int{0}, keys, false, 0); diff != "" {
		t.Fatalf("want match, got %q", diff)
	}
	asc := []topRow{{"a", 3, 3}, {"b", 2, 2}, {"c", 1, 1}}
	if diff := compareGroupedOrdered(topRec(asc...), topOracle(rows...), items, []int{0}, keys, false, 0); diff == "" {
		t.Fatal("ascending arcx rows under ORDER BY host DESC must alarm")
	}
}
