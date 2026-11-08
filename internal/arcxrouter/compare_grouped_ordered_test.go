// res-2 companion: the ordered, tie-aware grouped comparison.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"context"
	"database/sql"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/basekick-labs/arc/internal/arcxengine"
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
		// Review M1: the ORACLE's rows tie at the boundary, arcx's do not; arcx's
		// one-sided row is not tied with its own last row, so it alarms.
		{"one-sided row not tied with its side's cut", []topRow{{"A", 6, 1}, {"B", 5, 2}},
			[]topRow{{"Z", 5, 1}, {"C", 5, 2}}, byCountDesc, true, 2, "alarm"},
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

func nullableAvgRec(hosts []string, avgs []*float64) arrow.Record {
	hb := array.NewStringBuilder(memory.DefaultAllocator)
	ib := array.NewInt64Builder(memory.DefaultAllocator)
	fb := array.NewFloat64Builder(memory.DefaultAllocator)
	for i, h := range hosts {
		hb.Append(h)
		ib.Append(1)
		if avgs[i] == nil {
			fb.AppendNull()
		} else {
			fb.Append(*avgs[i])
		}
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "count_star()", Type: arrow.PrimitiveTypes.Int64},
		{Name: "avg(x)", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	return array.NewRecord(schema, []arrow.Array{hb.NewArray(), ib.NewArray(), fb.NewArray()}, int64(len(hosts)))
}

// Review M1: NULL sort values, both null orders, in both directions.
func TestCompareGroupedOrderedNullSortValues(t *testing.T) {
	items := []string{"host", "count(*)", "avg(x)"}
	one, two := 1.0, 2.0
	oracleRows := func(hosts []string, avgs []*float64) []groupedRow {
		out := make([]groupedRow, len(hosts))
		for i, h := range hosts {
			c := aggCell{isNull: true}
			if avgs[i] != nil {
				c = aggCell{f: *avgs[i]}
			}
			out[i] = groupedRow{keys: []keyPart{{s: h}}, cells: []aggCell{{isInt: true, i: big.NewInt(1)}, c}}
		}
		return out
	}
	last := []*float64{&one, &two, nil}
	first := []*float64{nil, &one, &two}
	for _, c := range []struct {
		name       string
		keys       []OrderKey
		arcx, duck []*float64
		arcxH      []string
		duckH      []string
		alarm      bool
	}{
		{"ASC NULLS LAST", []OrderKey{{Pos: 3}}, last, last, []string{"a", "b", "c"}, []string{"a", "b", "c"}, false},
		{"ASC NULLS FIRST", []OrderKey{{Pos: 3, NullsFirst: true}}, first, first, []string{"c", "a", "b"}, []string{"c", "a", "b"}, false},
		{"DESC NULLS LAST", []OrderKey{{Pos: 3, Desc: true}}, []*float64{&two, &one, nil}, []*float64{&two, &one, nil},
			[]string{"b", "a", "c"}, []string{"b", "a", "c"}, false},
		{"NULL first on arcx under NULLS LAST", []OrderKey{{Pos: 3}}, first, last,
			[]string{"c", "a", "b"}, []string{"a", "b", "c"}, true},
	} {
		diff := compareGroupedOrdered(nullableAvgRec(c.arcxH, c.arcx), oracleRows(c.duckH, c.duck), items, []int{0}, c.keys, false, 0)
		if c.alarm != (diff != "" && !strings.Contains(diff, "diff:") && !strings.HasPrefix(diff, skipPrefix)) {
			t.Errorf("%s: alarm=%t, got %q", c.name, c.alarm, diff)
		}
	}
}

// Review H2: an arcx record with the wrong column count alarms, never panics.
func TestCompareGroupedOrderedArity(t *testing.T) {
	items := []string{"host", "count(*)", "avg(x)", "max(x)"}
	rows := []topRow{{"a", 1, 1}}
	oracle := []groupedRow{{keys: []keyPart{{s: "a"}}, cells: []aggCell{{isInt: true, i: big.NewInt(1)}, {f: 1}, {f: 1}}}}
	diff := compareGroupedOrdered(topRec(rows...), oracle, items, []int{0}, []OrderKey{{Pos: 4}}, false, 0)
	if diff == "" || strings.Contains(diff, "diff:") || strings.HasPrefix(diff, skipPrefix) {
		t.Fatalf("arity difference must alarm, got %q", diff)
	}
}

// Review H3: one tie class of 20k rows (every row tied) compares in well under a
// second — the per-class match is sort+merge, not O(k^2).
func TestCompareGroupedOrderedLargeTieClass(t *testing.T) {
	items := []string{"host", "count(*)", "avg(x)"}
	const n = 20000
	rows := make([]topRow, n)
	rev := make([]topRow, n)
	for i := range rows {
		rows[i] = topRow{host: strings.Repeat("h", 1) + string(rune('a'+i%26)) + itoa(i), n: 7, avg: 1}
	}
	for i := range rows {
		rev[i] = rows[n-1-i]
	}
	if diff := compareGroupedOrdered(topRec(rows...), topOracle(rev...), items, []int{0}, []OrderKey{{Pos: 2}}, false, 0); diff != "" {
		t.Fatalf("want match, got %q", diff)
	}
}

func itoa(i int) string { return big.NewInt(int64(i)).String() }

// Review H1: a tie at the LIMIT cut is filed as tiediff only after a
// deterministic DuckDB query (engine SQL + the engine's tie-break) agrees with
// arcx row for row. A wrong arcx row at the cut — the top-N heap that keeps the
// evicted value — is a mismatch.
func TestVerifyTopNTie(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	base := t.TempDir()
	dir := filepath.Join(base, "prod", "cpu", "2026", "02", "03", "10")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A×10, B×9, C×9, D×5.
	if _, err := db.Exec(`COPY (SELECT h AS host, 1.0 AS x, TIMESTAMPTZ '2026-02-03 10:00:00+00' AS time
		FROM (SELECT unnest(['A','A','A','A','A','A','A','A','A','A','B','B','B','B','B','B','B','B','B',
		'C','C','C','C','C','C','C','C','C','D','D','D','D','D']) AS h)) TO '` + filepath.Join(dir, "a.parquet") + `' (FORMAT parquet)`); err != nil {
		t.Fatal(err)
	}
	deps := newLocalDeps(t, base)
	deps.DB = db
	d := Decision{
		Eligible:  true,
		Shape:     ShapeScanAggGrouped,
		Ctx:       arcxengine.Context{Database: "prod", Measurement: "cpu", TimeColumn: "time"},
		AggItems:  []string{"host", "count(*)"},
		GroupKey:  "host",
		OrderKeys: []OrderKey{{Pos: 2, Desc: true}},
		HasLimit:  true,
		Limit:     2,
	}
	items := d.AggItems
	rec := func(hosts []string, counts []int64) arrow.Record {
		hb := array.NewStringBuilder(memory.DefaultAllocator)
		ib := array.NewInt64Builder(memory.DefaultAllocator)
		for i := range hosts {
			hb.Append(hosts[i])
			ib.Append(counts[i])
		}
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "host", Type: arrow.BinaryTypes.String},
			{Name: "count_star()", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		return array.NewRecord(schema, []arrow.Array{hb.NewArray(), ib.NewArray()}, int64(len(hosts)))
	}
	ctx := context.Background()
	// The engine's answer (B wins the B/C tie by key) — DuckDB's user query kept C.
	got, err := deps.verifyTopNTie(ctx, d, rec([]string{"A", "B"}, []int64{10, 9}), []int{0}, "tiediff:x")
	if err != nil || got != "tiediff:x" {
		t.Fatalf("legitimate tie: got (%q, %v)", got, err)
	}
	// Wrong row at the cut: D carrying the evicted value 9.
	got, err = deps.verifyTopNTie(ctx, d, rec([]string{"A", "D"}, []int64{10, 9}), []int{0}, "tiediff:x")
	if err != nil || got == "" || strings.Contains(got, "diff:") {
		t.Fatalf("wrong row at the cut must be a mismatch: got (%q, %v)", got, err)
	}
	// The other tie member: not what the engine's tie-break picks — a mismatch.
	got, err = deps.verifyTopNTie(ctx, d, rec([]string{"A", "C"}, []int64{10, 9}), []int{0}, "tiediff:x")
	if err != nil || got == "" || strings.Contains(got, "diff:") {
		t.Fatalf("a tie member the engine would not pick must be a mismatch: got (%q, %v)", got, err)
	}
	_ = items

	// Wired through compareToOracle with the user's own query as the oracle:
	// whichever B/C member DuckDB keeps, D-with-9 at the cut must alarm, and the
	// engine's answer is a match or a verified tiediff.
	deps.ConvertedSQL = "SELECT host, count(*) FROM read_parquet('" + filepath.Join(dir, "a.parquet") +
		"') GROUP BY host ORDER BY 2 DESC LIMIT 2"
	got, err = deps.compareToOracle(ctx, d, rec([]string{"A", "D"}, []int64{10, 9}))
	if err != nil || got == "" || strings.Contains(got, "diff:") {
		t.Fatalf("compareToOracle: wrong row at the cut must be a mismatch: got (%q, %v)", got, err)
	}
	got, err = deps.compareToOracle(ctx, d, rec([]string{"A", "B"}, []int64{10, 9}))
	if err != nil || !(got == "" || strings.HasPrefix(got, "tiediff:")) {
		t.Fatalf("compareToOracle: engine answer must match or be a verified tie: got (%q, %v)", got, err)
	}
}
