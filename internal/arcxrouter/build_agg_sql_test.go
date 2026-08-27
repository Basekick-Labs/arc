// Engine-SQL construction tests for the agg-1 shape. Tagged: buildScanAggSQL
// lives in the engine-linked router.

//go:build cgo && arcx_engine

package arcxrouter

import "testing"

func TestBuildScanAggSQL(t *testing.T) {
	paths := "['/base/db/cpu/2026/01/01/a.parquet']"
	cases := []struct {
		d    Decision
		want string
	}{
		{
			Decision{AggItems: []string{"count(*)"}, WhereText: "host = 'a'"},
			"SELECT count(*) FROM read_parquet(['/base/db/cpu/2026/01/01/a.parquet']) WHERE host = 'a'",
		},
		{
			Decision{AggItems: []string{"count(*)", "avg(usage_user)"}, WhereText: ""},
			"SELECT count(*), avg(usage_user) FROM read_parquet(['/base/db/cpu/2026/01/01/a.parquet'])",
		},
		{
			// Argument spelling preserved end-to-end (derived name parity).
			Decision{AggItems: []string{"sum(X)"}, WhereText: "X > 1"},
			"SELECT sum(X) FROM read_parquet(['/base/db/cpu/2026/01/01/a.parquet']) WHERE X > 1",
		},
	}
	for _, c := range cases {
		got, ok := buildScanAggSQL(c.d, paths)
		if !ok || got != c.want {
			t.Fatalf("buildScanAggSQL = (%q, %t), want %q", got, ok, c.want)
		}
	}
}

func TestBuildScanAggSQLDeclinesUnsafeItems(t *testing.T) {
	paths := "['/p.parquet']"
	for _, items := range [][]string{
		{},                              // empty list
		{"count(*) --"},                 // trailing junk
		{"sum(x); DROP TABLE t"},        // injection attempt
		{"sum(x')"},                     // non-ident arg
		{"median(x)"},                   // unknown fn
		{"sum(a, b)"},                   // arity
		{"SUM(x)"},                      // fn not lowercased = not our serialization
		{"sum(x)", "count(*) OR 1 = 1"}, // second item unsafe
	} {
		if got, ok := buildScanAggSQL(Decision{AggItems: items}, paths); ok {
			t.Fatalf("must decline unsafe items %v, got %q", items, got)
		}
	}
}

func TestBuildGroupedCountSQL(t *testing.T) {
	paths := "['/p.parquet']"
	got, ok := buildGroupedCountSQL(Decision{
		AggItems: []string{"count(*)", "host"},
		GroupKey: "host",
	}, paths)
	want := "SELECT count(*), host FROM read_parquet(['/p.parquet']) GROUP BY host"
	if !ok || got != want {
		t.Fatalf("got (%q, %t), want %q", got, ok, want)
	}
	for _, d := range []Decision{
		{AggItems: []string{"count(*)"}, GroupKey: "host"},              // no key item
		{AggItems: []string{"host", "count(*)"}, GroupKey: "h; DROP"},   // unsafe key
		{AggItems: []string{"host", "sum(x)"}, GroupKey: "host"},        // non-count item
		{AggItems: []string{"host", "host", "count(*)"}, GroupKey: "host"}, // repeated key
	} {
		if got, ok := buildGroupedCountSQL(d, paths); ok {
			t.Fatalf("must decline unsafe decision %v, got %q", d, got)
		}
	}
}
