// buildScanSQL tests: the engine-SQL string emitted from a scan Decision. Tagged
// (Decision lives in the tagged router.go). Covers projection + WHERE emission,
// string-literal escaping, and the defense-in-depth declines on unsafe parts.

//go:build cgo && arcx_engine

package arcxrouter

import "testing"

func TestBuildScanSQL(t *testing.T) {
	// buildScanSQL emits the engine SQL from the parsed parts. Verify the exact
	// string for representative shapes (the path array is supplied directly here).
	cases := []struct {
		name string
		d    Decision
		arr  string
		want string
	}{
		{
			"proj only",
			Decision{Shape: ShapeScan, Cols: []string{"host", "code"}},
			"['/a.parquet']",
			"SELECT host, code FROM read_parquet(['/a.parquet'])",
		},
		{
			"int predicate",
			Decision{Shape: ShapeScan, Cols: []string{"code"}, Preds: []scanPred{{col: "code", op: ">=", num: "5"}}},
			"['/a.parquet']",
			"SELECT code FROM read_parquet(['/a.parquet']) WHERE code >= 5",
		},
		{
			"string predicate escapes quote",
			Decision{Shape: ShapeScan, Cols: []string{"code"}, Preds: []scanPred{{col: "host", op: "=", str: "we'b", isStr: true}}},
			"['/a.parquet']",
			"SELECT code FROM read_parquet(['/a.parquet']) WHERE host = 'we''b'",
		},
		{
			"and conjunction",
			Decision{Shape: ShapeScan, Cols: []string{"code"}, Preds: []scanPred{
				{col: "code", op: ">=", num: "5"},
				{col: "host", op: "=", str: "web", isStr: true},
			}},
			"['/a.parquet']",
			"SELECT code FROM read_parquet(['/a.parquet']) WHERE code >= 5 AND host = 'web'",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := buildScanSQL(tc.d, tc.arr)
			if !ok {
				t.Fatalf("buildScanSQL declined unexpectedly")
			}
			if got != tc.want {
				t.Fatalf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}

func TestBuildScanSQL_DeclinesUnsafe(t *testing.T) {
	// Defense-in-depth: non-bare columns / bad literals / bad ops decline rather
	// than emit unsafe SQL (the tokenizer wouldn't produce these, but the guard is
	// the last line before SQL-string interpolation).
	bad := []Decision{
		{Shape: ShapeScan, Cols: nil},                                                            // empty projection
		{Shape: ShapeScan, Cols: []string{"a b"}},                                                // space in col
		{Shape: ShapeScan, Cols: []string{"code"}, Preds: []scanPred{{col: "a;drop", op: "=", num: "1"}}}, // injection-y col
		{Shape: ShapeScan, Cols: []string{"code"}, Preds: []scanPred{{col: "a", op: "LIKE", num: "1"}}},   // bad op
		{Shape: ShapeScan, Cols: []string{"code"}, Preds: []scanPred{{col: "a", op: "=", num: "1x"}}},     // bad int literal
	}
	for i, d := range bad {
		if _, ok := buildScanSQL(d, "['/a.parquet']"); ok {
			t.Fatalf("case %d: expected decline, got SQL", i)
		}
	}
}

func TestBuildScanSQL_OrderByLimit(t *testing.T) {
	cases := []struct {
		name string
		d    Decision
		want string
	}{
		{
			"order desc limit",
			Decision{Shape: ShapeScan, Cols: []string{"code"}, OrderBy: []scanOrderKey{{"code", true}}, Limit: 100},
			"SELECT code FROM read_parquet(['/a.parquet']) ORDER BY code DESC LIMIT 100",
		},
		{
			"multi key asc",
			Decision{Shape: ShapeScan, Cols: []string{"code"}, OrderBy: []scanOrderKey{{"host", false}, {"code", true}}},
			"SELECT code FROM read_parquet(['/a.parquet']) ORDER BY host ASC, code DESC",
		},
		{
			"where order limit",
			Decision{Shape: ShapeScan, Cols: []string{"code"}, Preds: []scanPred{{col: "code", op: ">=", num: "5"}}, OrderBy: []scanOrderKey{{"code", true}}, Limit: 10},
			"SELECT code FROM read_parquet(['/a.parquet']) WHERE code >= 5 ORDER BY code DESC LIMIT 10",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := buildScanSQL(tc.d, "['/a.parquet']")
			if !ok {
				t.Fatalf("declined unexpectedly")
			}
			if got != tc.want {
				t.Fatalf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}
