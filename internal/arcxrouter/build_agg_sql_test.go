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

func TestBuildGroupedSQL(t *testing.T) {
	paths := "['/p.parquet']"
	got, ok := buildGroupedSQL(Decision{
		AggItems: []string{"count(*)", "host"},
		GroupKey: "host",
	}, paths)
	// agg-3: GROUP BY (and ORDER BY) emit by POSITION — valid for bare keys and
	// bucket keys alike, and no identifier text reaches the emitted clause.
	want := "SELECT count(*), host FROM read_parquet(['/p.parquet']) GROUP BY 2"
	if !ok || got != want {
		t.Fatalf("got (%q, %t), want %q", got, ok, want)
	}
	// WHERE-bearing (mimalloc slice): the reserialized tree lands between the
	// path array and GROUP BY, mirroring buildScanAggSQL.
	got, ok = buildGroupedSQL(Decision{
		AggItems:  []string{"host", "count(*)", "avg(cpu_user)"},
		GroupKey:  "host",
		WhereText: "cpu_user > 90",
	}, paths)
	want = "SELECT host, count(*), avg(cpu_user) FROM read_parquet(['/p.parquet']) WHERE cpu_user > 90 GROUP BY 1"
	if !ok || got != want {
		t.Fatalf("got (%q, %t), want %q", got, ok, want)
	}
	// agg-3 bucket key + ORDER BY: the bucket text is REBUILT from the validated
	// parts (BucketUnit/BucketCol), never taken from GroupKey.
	got, ok = buildGroupedSQL(Decision{
		AggItems:    []string{"date_trunc('minute', time)", "count(*)", "avg(cpu_user)"},
		BucketUnit:  "minute",
		BucketCol:   "time",
		WhereText:   "time >= '2026-01-01T00:00:00Z'",
		OrderByItem: 1,
	}, paths)
	want = "SELECT date_trunc('minute', time), count(*), avg(cpu_user) FROM read_parquet(['/p.parquet']) WHERE time >= '2026-01-01T00:00:00Z' GROUP BY 1 ORDER BY 1"
	if !ok || got != want {
		t.Fatalf("got (%q, %t), want %q", got, ok, want)
	}
	// agg-3b epoch-math bucket: rebuilt from validated parts, alias included.
	got, ok = buildGroupedSQL(Decision{
		AggItems:       []string{"to_timestamp((epoch_ns(time) // 1000000000 // 300) * 300) AS time", "avg(cpu_user)"},
		EpochWidthSecs: 300,
		BucketCol:      "time",
		BucketAlias:    "time",
		OrderByItem:    1,
	}, paths)
	want = "SELECT to_timestamp((epoch_ns(time) // 1000000000 // 300) * 300) AS time, avg(cpu_user) FROM read_parquet(['/p.parquet']) GROUP BY 1 ORDER BY 1"
	if !ok || got != want {
		t.Fatalf("got (%q, %t), want %q", got, ok, want)
	}
	for _, d := range []Decision{
		{ // epoch-math with unsafe parts must never be emitted
			AggItems:       []string{"to_timestamp((epoch_ns(t; DROP) // 1000000000 // 300) * 300) AS time", "count(*)"},
			EpochWidthSecs: 300, BucketCol: "t; DROP", BucketAlias: "time",
		},
		{
			AggItems:       []string{"to_timestamp((epoch_ns(time) // 1000000000 // 300) * 300) AS x; DROP", "count(*)"},
			EpochWidthSecs: 300, BucketCol: "time", BucketAlias: "x; DROP",
		},
		{ // unsafe bucket parts must never be emitted
			AggItems:   []string{"date_trunc('week', time)", "count(*)"},
			BucketUnit: "week", BucketCol: "time",
		},
		{
			AggItems:   []string{"date_trunc('minute', x)", "count(*)"},
			BucketUnit: "minute", BucketCol: "x; DROP",
		},
		{AggItems: []string{"count(*)"}, GroupKey: "host"},                 // no key item
		{AggItems: []string{"host", "count(*)"}, GroupKey: "h; DROP"},      // unsafe key
		{AggItems: []string{"host", "median(x)"}, GroupKey: "host"},        // unknown fn item
		{AggItems: []string{"host", "host", "count(*)"}, GroupKey: "host"}, // repeated key
	} {
		if got, ok := buildGroupedSQL(d, paths); ok {
			t.Fatalf("must decline unsafe decision %v, got %q", d, got)
		}
	}
}

func TestBuildGroupedSQLComposite(t *testing.T) {
	paths := "['/p.parquet']"
	// Bucket + tag: positional GROUP BY list; ORDER BY validated as a key pos.
	got, ok := buildGroupedSQL(Decision{
		AggItems: []string{
			"to_timestamp((epoch_ns(time) // 1000000000 // 300) * 300) AS time",
			"host", "avg(cpu_user)",
		},
		GroupKey:       "host",
		EpochWidthSecs: 300,
		BucketCol:      "time",
		BucketAlias:    "time",
		OrderByItem:    1,
	}, paths)
	want := "SELECT to_timestamp((epoch_ns(time) // 1000000000 // 300) * 300) AS time, host, avg(cpu_user) FROM read_parquet(['/p.parquet']) GROUP BY 1, 2 ORDER BY 1"
	if !ok || got != want {
		t.Fatalf("got (%q, %t), want %q", got, ok, want)
	}
	// ORDER BY pointing at a NON-key position must decline.
	if _, ok := buildGroupedSQL(Decision{
		AggItems:    []string{"date_trunc('hour', time)", "host", "count(*)"},
		GroupKey:    "host",
		BucketUnit:  "hour",
		BucketCol:   "time",
		OrderByItem: 3,
	}, paths); ok {
		t.Fatal("ORDER BY on an aggregate position must decline")
	}
}

func TestIsAggItemTwoArg(t *testing.T) {
	for item, want := range map[string]bool{
		"arg_max(v, time)":    true,
		"min_by(Value, Time)": true,
		"arg_max(v, time, 2)": false, // 3-arg re-serialization can't exist, but the validator must still reject
		"arg_max(v)":          false,
		"argmax(v, time)":     false,
		"arg_max(a b, time)":  false,
		"arg_max(v; DROP, t)": false,
	} {
		if got := isAggItem(item); got != want {
			t.Fatalf("isAggItem(%q) = %t, want %t", item, got, want)
		}
	}
}
