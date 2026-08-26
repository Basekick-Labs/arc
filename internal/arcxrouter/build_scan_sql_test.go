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
		{
			"float equality (2b-1b)",
			Decision{Shape: ShapeScan, Cols: []string{"value"}, Preds: []scanPred{{col: "value", op: "=", num: "99.5", isFloat: true}}},
			"['/a.parquet']",
			"SELECT value FROM read_parquet(['/a.parquet']) WHERE value = 99.5",
		},
		{
			"float inequality-of-equals (!=) with negative literal",
			Decision{Shape: ShapeScan, Cols: []string{"value"}, Preds: []scanPred{{col: "value", op: "!=", num: "-2.25", isFloat: true}}},
			"['/a.parquet']",
			"SELECT value FROM read_parquet(['/a.parquet']) WHERE value != -2.25",
		},
		{
			"float inequality < (2b-4)",
			Decision{Shape: ShapeScan, Cols: []string{"value"}, Preds: []scanPred{{col: "value", op: "<", num: "1.5", isFloat: true}}},
			"['/a.parquet']",
			"SELECT value FROM read_parquet(['/a.parquet']) WHERE value < 1.5",
		},
		{
			"float BETWEEN desugared to two float preds (2b-4)",
			Decision{Shape: ShapeScan, Cols: []string{"value"}, Preds: []scanPred{
				{col: "value", op: ">=", num: "1.5", isFloat: true},
				{col: "value", op: "<=", num: "3.5", isFloat: true},
			}},
			"['/a.parquet']",
			"SELECT value FROM read_parquet(['/a.parquet']) WHERE value >= 1.5 AND value <= 3.5",
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
		{Shape: ShapeScan, Cols: nil},             // empty projection
		{Shape: ShapeScan, Cols: []string{"a b"}}, // space in col
		{Shape: ShapeScan, Cols: []string{"code"}, Preds: []scanPred{{col: "a;drop", op: "=", num: "1"}}},              // injection-y col
		{Shape: ShapeScan, Cols: []string{"code"}, Preds: []scanPred{{col: "a", op: "LIKE", num: "1"}}},                // bad op
		{Shape: ShapeScan, Cols: []string{"code"}, Preds: []scanPred{{col: "a", op: "=", num: "1x"}}},                  // bad int literal
		{Shape: ShapeScan, Cols: []string{"code"}, Preds: []scanPred{{col: "a", op: "=", num: "1.2x", isFloat: true}}}, // bad float literal
		{Shape: ShapeScan, Cols: []string{"code"}, Preds: []scanPred{{col: "a", op: "=", num: "0.0", isFloat: true}}},  // ±0.0 float (declines defense-in-depth)
		{Shape: ShapeScan, Cols: []string{"code"}, Preds: []scanPred{{col: "a", op: "<", num: "0.0", isFloat: true}}},  // ±0.0 inequality still declines (2b-4)
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

func TestBuildScanSQL_IsNull(t *testing.T) {
	cases := []struct {
		name string
		d    Decision
		want string
	}{
		{
			"is null",
			Decision{Shape: ShapeScan, Cols: []string{"a"}, Preds: []scanPred{{col: "b", isNull: true}}},
			"SELECT a FROM read_parquet(['/a.parquet']) WHERE b IS NULL",
		},
		{
			"is not null",
			Decision{Shape: ShapeScan, Cols: []string{"a"}, Preds: []scanPred{{col: "b", isNull: true, negated: true}}},
			"SELECT a FROM read_parquet(['/a.parquet']) WHERE b IS NOT NULL",
		},
		{
			"cmp and is null",
			Decision{Shape: ShapeScan, Cols: []string{"a"}, Preds: []scanPred{
				{col: "code", op: ">=", num: "5"},
				{col: "host", isNull: true, negated: true},
			}},
			"SELECT a FROM read_parquet(['/a.parquet']) WHERE code >= 5 AND host IS NOT NULL",
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

func TestBuildScanSQL_WhereTextTree(t *testing.T) {
	// A boolean-tree Decision emits `WHERE <WhereText>` verbatim (the text was already
	// re-serialized + re-escaped by reserializeWhere).
	d := Decision{
		Shape:     ShapeScan,
		Cols:      []string{"a"},
		WhereText: "a = 1 OR (b = 2 AND host = 'we''b')",
	}
	got, ok := buildScanSQL(d, "['/a.parquet']")
	if !ok {
		t.Fatal("declined unexpectedly")
	}
	want := "SELECT a FROM read_parquet(['/a.parquet']) WHERE a = 1 OR (b = 2 AND host = 'we''b')"
	if got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

func TestReserializeWhere_RoundTripFidelity(t *testing.T) {
	// The router's re-serialized WHERE must lex+recognize back to the SAME whereText when
	// fed through the recognizer again (idempotent), and a nasty string literal (quotes,
	// parens, AND, --) must survive as a single escaped literal — no injection across the
	// round-trip. This is the cross-serializer divergence guard (router emits → re-parse).
	inputs := []string{
		"SELECT a FROM cpu WHERE a = 1 OR (b = 2 AND c = 3)",
		"SELECT a FROM cpu WHERE host = 'a'') OR 1=1 --' OR host = 'ok'",
		"SELECT a FROM cpu WHERE (a = 1 OR b = 2) AND (c = 3 OR d = 4)",
	}
	for _, sql := range inputs {
		m1, ok := eligibleShape(sql)
		if !ok || m1.shape != ShapeScan || m1.whereText == "" {
			t.Fatalf("expected boolean-tree scan for %q", sql)
		}
		// Reconstruct a user-form query from the re-serialized whereText and recognize it
		// again: the re-serializer must be IDEMPOTENT (its output re-lexes to the same
		// text), which is exactly what the engine relies on when it re-parses whereText.
		reconstructed := "SELECT a FROM cpu WHERE " + m1.whereText
		m2, ok := eligibleShape(reconstructed)
		if !ok || m2.whereText != m1.whereText {
			t.Fatalf("round-trip not idempotent:\n in:  %q\n out: %q", m1.whereText, m2.whereText)
		}
	}
}

func TestReserializeWhere_InRoundTrip(t *testing.T) {
	// IN / NOT IN re-serialize idempotently, and a nasty string element survives as one
	// escaped literal (no injection). Same round-trip discipline as the 2b-2 test.
	inputs := []string{
		"SELECT a FROM cpu WHERE x IN (1, 2, 3)",
		"SELECT a FROM cpu WHERE host IN ('a'') OR 1=1 --', 'ok')",
		"SELECT a FROM cpu WHERE x IN (1,2) AND y NOT IN (3,4)",
	}
	for _, sql := range inputs {
		m1, ok := eligibleShape(sql)
		if !ok || m1.shape != ShapeScan || m1.whereText == "" {
			t.Fatalf("expected IN scan-eligible with whereText for %q", sql)
		}
		reconstructed := "SELECT a FROM cpu WHERE " + m1.whereText
		m2, ok := eligibleShape(reconstructed)
		if !ok || m2.whereText != m1.whereText {
			t.Fatalf("IN round-trip not idempotent:\n in:  %q\n out: %q", m1.whereText, m2.whereText)
		}
	}
}
