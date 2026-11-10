// res-3 companion: under a HAVING over an unstable item (Decision.HavingUnstable),
// a group on one side only is a WARN; everything else still compares strictly.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"math/big"
	"strings"
	"testing"
)

func TestCompareGroupedUnstableHavingOneSided(t *testing.T) {
	items := []string{"host", "skewness(v)", "count(*)"}
	half, one := 0.5, 1.0
	row := func(h string, s float64, n int64) groupedRow {
		return groupedRow{keys: []keyPart{{s: h}}, cells: []aggCell{{f: s}, {isInt: true, i: big.NewInt(n)}}}
	}
	// arcx {a, c}; DuckDB {a, b, c}: b's DuckDB skewness crossed the literal.
	arcx := groupedShapeRec(t, []string{"a", "c"}, []*float64{&half, &one}, []int64{3, 5})
	oracle := []groupedRow{row("a", 0.5, 3), row("b", 7, 4), row("c", 1, 5)}

	// Strict (no HAVING over an unstable item): a row-count alarm.
	if diff := compareGrouped(arcx, oracle, items, []int{0}, ""); !strings.Contains(diff, "row count differs") {
		t.Fatalf("strict mode must alarm on a one-sided group, got %q", diff)
	}
	for _, c := range []struct{ class, prefix string }{{"oracle", "oraclediff:"}, {"arg", "argdiff:"}} {
		if diff := compareGrouped(arcx, append([]groupedRow(nil), oracle...), items, []int{0}, c.class); !strings.HasPrefix(diff, c.prefix) {
			t.Fatalf("%s: one-sided group must be %s, got %q", c.class, c.prefix, diff)
		}
	}
	// One-sided on the ARCX side too.
	arcx3 := groupedShapeRec(t, []string{"a", "b", "c"}, []*float64{&half, &half, &one}, []int64{3, 4, 5})
	if diff := compareGrouped(arcx3, []groupedRow{row("a", 0.5, 3), row("c", 1, 5)}, items, []int{0}, "oracle"); !strings.HasPrefix(diff, "oraclediff:") {
		t.Fatalf("arcx-only group must be oraclediff, got %q", diff)
	}
	// A matched group whose STABLE cell differs still alarms (review H3).
	bad := []groupedRow{row("a", 0.5, 3), row("b", 7, 4), row("c", 1, 6)}
	if diff := compareGrouped(arcx, bad, items, []int{0}, "oracle"); diff == "" || strings.Contains(diff, "diff:") {
		t.Fatalf("a count mismatch on a matched group must alarm, got %q", diff)
	}
	// A duplicate group key on arcx alarms instead of reading as one-sided.
	dup := groupedShapeRec(t, []string{"a", "a"}, []*float64{&half, &half}, []int64{3, 3})
	if diff := compareGrouped(dup, []groupedRow{row("a", 0.5, 3)}, items, []int{0}, "oracle"); diff == "" || strings.Contains(diff, "diff:") {
		t.Fatalf("a duplicate group key must alarm, got %q", diff)
	}
	// Review M1: no group in common alarms (one side empty, or disjoint keys).
	if diff := compareGrouped(arcx, nil, items, []int{0}, "oracle"); diff == "" || strings.Contains(diff, "diff:") {
		t.Fatalf("an empty DuckDB side against groups must alarm, got %q", diff)
	}
	if diff := compareGrouped(arcx, []groupedRow{row("x", 0.5, 3), row("y", 1, 5)}, items, []int{0}, "oracle"); diff == "" || strings.Contains(diff, "diff:") {
		t.Fatalf("disjoint group keys must alarm, got %q", diff)
	}
	// Equal row sets: a match.
	if diff := compareGrouped(arcx, []groupedRow{row("a", 0.5, 3), row("c", 1, 5)}, items, []int{0}, "oracle"); diff != "" {
		t.Fatalf("unexpected diff %q", diff)
	}
}

func TestCompareGroupedKeyOrderedUnstableHaving(t *testing.T) {
	items := []string{"host", "skewness(v)", "count(*)"}
	half, one := 0.5, 1.0
	row := func(h string, s float64, n int64) groupedRow {
		return groupedRow{keys: []keyPart{{s: h}}, cells: []aggCell{{f: s}, {isInt: true, i: big.NewInt(n)}}}
	}
	desc := []OrderKey{{Pos: 1, Desc: true}}
	oracle := []groupedRow{row("c", 1, 5), row("b", 7, 4), row("a", 0.5, 3)}
	inOrder := groupedShapeRec(t, []string{"c", "a"}, []*float64{&one, &half}, []int64{5, 3})
	if diff := compareGroupedKeyOrdered(inOrder, append([]groupedRow(nil), oracle...), items, []int{0}, desc, "oracle"); !strings.HasPrefix(diff, "oraclediff:") {
		t.Fatalf("key-ordered one-sided group must be oraclediff, got %q", diff)
	}
	outOfOrder := groupedShapeRec(t, []string{"a", "c"}, []*float64{&half, &one}, []int64{3, 5})
	if diff := compareGroupedKeyOrdered(outOfOrder, append([]groupedRow(nil), oracle...), items, []int{0}, desc, "oracle"); !strings.Contains(diff, "not in ORDER BY order") {
		t.Fatalf("arcx rows out of key order must alarm, got %q", diff)
	}
	if diff := compareGroupedKeyOrdered(inOrder, oracle, items, []int{0}, []OrderKey{{Pos: 3}}, "oracle"); !strings.HasPrefix(diff, skipPrefix) {
		t.Fatalf("an aggregate ORDER BY target is a recognizer invariant breach (skip), got %q", diff)
	}
}

// res-3b: a select list without an aggregate builds only when the HAVING carries
// one (a hidden item) — never a bare DISTINCT, even from a hand-made Decision.
func TestBuildGroupedSQLHiddenOnly(t *testing.T) {
	paths := "['/p.parquet']"
	if got, ok := buildGroupedSQL(Decision{AggItems: []string{"host"}, GroupKey: "host"}, paths); ok {
		t.Fatalf("bare DISTINCT must not build: %q", got)
	}
	if got, ok := buildGroupedSQL(Decision{AggItems: []string{"host"}, GroupKey: "host", HavingHidden: 1}, paths); ok {
		t.Fatalf("a hidden count without HAVING text must not build: %q", got)
	}
	got, ok := buildGroupedSQL(Decision{
		AggItems: []string{"host"}, GroupKey: "host", HavingHidden: 1, HavingText: "count(*) > 100",
	}, paths)
	want := "SELECT host FROM read_parquet(['/p.parquet']) GROUP BY 1 HAVING count(*) > 100"
	if !ok || got != want {
		t.Fatalf("got (%q, %t), want %q", got, ok, want)
	}
}
