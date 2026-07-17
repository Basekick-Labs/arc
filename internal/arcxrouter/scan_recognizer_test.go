// Scan-shape recognizer + buildScanSQL tests (Phase 2a). Cgo-free. Every accept,
// every decline reason, and the whole-token mis-accepts — the router must not
// re-introduce a mis-accept the engine already rejects, and must decline anything
// the engine's 2a scan doesn't answer (SELECT *, OR, functions, ORDER BY/LIMIT).

package arcxrouter

import "testing"

func TestMatchScan_Accepts(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		wantCols []string
		wantMeas string
		wantN    int // predicate count
	}{
		{"single col no where", "SELECT host FROM cpu", []string{"host"}, "cpu", 0},
		{"multi col", "SELECT host, region FROM cpu", []string{"host", "region"}, "cpu", 0},
		{"dotted measurement", "SELECT host FROM mydb.cpu", []string{"host"}, "mydb.cpu", 0},
		{"int predicate", "SELECT code FROM cpu WHERE code >= 5", []string{"code"}, "cpu", 1},
		{"string predicate", "SELECT code FROM cpu WHERE host = 'web'", []string{"code"}, "cpu", 1},
		{"and conjunction", "SELECT code FROM cpu WHERE code >= 5 AND host = 'web'", []string{"code"}, "cpu", 2},
		{"negative literal", "SELECT ts FROM cpu WHERE ts >= -5", []string{"ts"}, "cpu", 1},
		{"ne op", "SELECT code FROM cpu WHERE code != 2", []string{"code"}, "cpu", 1},
		{"lowercase", "select host from cpu", []string{"host"}, "cpu", 0},
		{"case preserved cols", "SELECT Host, UsageIdle FROM cpu", []string{"Host", "UsageIdle"}, "cpu", 0},
		// 2f-0 computed projection: length(<col>) re-serialized as an item string, arg
		// spelling preserved (DuckDB echoes it as the output column name).
		{"length sole", "SELECT length(host) FROM cpu", []string{"length(host)"}, "cpu", 0},
		{"length caps arg", "SELECT length(Host) FROM cpu", []string{"length(Host)"}, "cpu", 0},
		{"length with passthrough", "SELECT length(host), code FROM cpu", []string{"length(host)", "code"}, "cpu", 0},
		{"two length", "SELECT length(host), length(region) FROM cpu", []string{"length(host)", "length(region)"}, "cpu", 0},
		{"length under filter", "SELECT length(host) FROM cpu WHERE code > 5", []string{"length(host)"}, "cpu", 1},
		// 2f-1 substr: re-serialized `substr(col, N[, M])` with `, ` separators, arg spelling
		// and int-literal text preserved (both engines normalize identically).
		{"substr 3-arg", "SELECT substr(host, 1, 3) FROM cpu", []string{"substr(host, 1, 3)"}, "cpu", 0},
		{"substr 2-arg", "SELECT substr(host, 3) FROM cpu", []string{"substr(host, 3)"}, "cpu", 0},
		{"substr negative", "SELECT substr(host, -2, 2) FROM cpu", []string{"substr(host, -2, 2)"}, "cpu", 0},
		{"substr + passthrough", "SELECT substr(host, 1, 2), code FROM cpu", []string{"substr(host, 1, 2)", "code"}, "cpu", 0},
		// 2f-2 string predicates: `<fn>(col, '<str>')`, needle re-escaped.
		{"starts_with", "SELECT starts_with(host, 'web') FROM cpu", []string{"starts_with(host, 'web')"}, "cpu", 0},
		{"ends_with", "SELECT ends_with(host, 'lo') FROM cpu", []string{"ends_with(host, 'lo')"}, "cpu", 0},
		{"contains", "SELECT contains(host, 'ell') FROM cpu", []string{"contains(host, 'ell')"}, "cpu", 0},
		{"contains reescaped quote", "SELECT contains(host, 'a''b') FROM cpu", []string{"contains(host, 'a''b')"}, "cpu", 0},
		{"starts_with + passthrough", "SELECT starts_with(host, 'w'), code FROM cpu", []string{"starts_with(host, 'w')", "code"}, "cpu", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := eligibleShape(tc.sql)
			if !ok {
				t.Fatalf("expected eligible, got decline for %q", tc.sql)
			}
			if m.shape != ShapeScan {
				t.Fatalf("shape = %q, want scan", m.shape)
			}
			if m.measurement != tc.wantMeas {
				t.Fatalf("meas = %q, want %q", m.measurement, tc.wantMeas)
			}
			if len(m.cols) != len(tc.wantCols) {
				t.Fatalf("cols = %v, want %v", m.cols, tc.wantCols)
			}
			for i := range tc.wantCols {
				if m.cols[i] != tc.wantCols[i] {
					t.Fatalf("cols[%d] = %q, want %q", i, m.cols[i], tc.wantCols[i])
				}
			}
			if len(m.preds) != tc.wantN {
				t.Fatalf("preds = %v, want %d", m.preds, tc.wantN)
			}
		})
	}
}

func TestMatchScan_Between(t *testing.T) {
	// BETWEEN desugars to two preds (col >= lo, col <= hi), matching arcx's binder.
	m, ok := eligibleShape("SELECT host FROM cpu WHERE code BETWEEN 5 AND 20")
	if !ok || m.shape != ShapeScan {
		t.Fatalf("BETWEEN should be scan-eligible")
	}
	if len(m.preds) != 2 {
		t.Fatalf("BETWEEN -> %d preds, want 2: %+v", len(m.preds), m.preds)
	}
	if m.preds[0].op != ">=" || m.preds[0].num != "5" || m.preds[1].op != "<=" || m.preds[1].num != "20" {
		t.Fatalf("BETWEEN desugar wrong: %+v", m.preds)
	}
	// Inner AND is not confused with the conjunction AND: `BETWEEN 1 AND 2 AND host='x'`
	// -> 3 preds (>=1, <=2, host='x').
	m2, ok := eligibleShape("SELECT a FROM cpu WHERE code BETWEEN 1 AND 2 AND host = 'web'")
	if !ok || len(m2.preds) != 3 {
		t.Fatalf("chained BETWEEN+AND -> want 3 preds, got %+v", m2.preds)
	}
	// NOT BETWEEN declines (a leading NOT isn't a bare column → not scan-eligible).
	if m3, ok := eligibleShape("SELECT a FROM cpu WHERE code NOT BETWEEN 1 AND 2"); ok && m3.shape == ShapeScan {
		t.Fatalf("NOT BETWEEN should decline")
	}
}

func TestMatchScan_BooleanTreeWhere(t *testing.T) {
	// A WHERE with OR / parens is recognized as a scan and RE-SERIALIZED into whereText
	// (not the flat preds path). The engine re-lexes whereText and owns the tree.
	cases := []struct {
		name string
		sql  string
		want string // expected whereText
	}{
		{"simple or", "SELECT a FROM cpu WHERE a = 1 OR b = 2", "a = 1 OR b = 2"},
		{"precedence", "SELECT a FROM cpu WHERE a = 1 OR b = 2 AND c = 3", "a = 1 OR b = 2 AND c = 3"},
		{"parens", "SELECT a FROM cpu WHERE (a = 1 OR b = 2) AND c = 3", "(a = 1 OR b = 2) AND c = 3"},
		{"string reescaped", "SELECT a FROM cpu WHERE host = 'we''b' OR host = 'x'", "host = 'we''b' OR host = 'x'"},
		{"is null in or", "SELECT a FROM cpu WHERE a IS NULL OR b = 2", "a IS NULL OR b = 2"},
		{"between in or", "SELECT a FROM cpu WHERE x BETWEEN 1 AND 5 OR y = 9", "x BETWEEN 1 AND 5 OR y = 9"},
		{"wide or", "SELECT a FROM cpu WHERE h = 'a' OR h = 'b' OR h = 'c'", "h = 'a' OR h = 'b' OR h = 'c'"},
		// LIKE (2d): a LIKE token forces the tree path even without OR/paren. The pattern is
		// re-escaped (doubled `''`) and multibyte survives — the re-serialization round-trip
		// fidelity that matters (a mangled served pattern would be an engine-invisible wrong
		// answer). Backslash / ESCAPE / non-literal LIKE decline (TestMatchScan_*Declines).
		{"bare like", "SELECT a FROM cpu WHERE host LIKE 'web%'", "host LIKE 'web%'"},
		{"not like", "SELECT a FROM cpu WHERE host NOT LIKE 'web%'", "host NOT LIKE 'web%'"},
		{"like in and", "SELECT a FROM cpu WHERE host LIKE 'web%' AND code = 5", "host LIKE 'web%' AND code = 5"},
		{"like in or", "SELECT a FROM cpu WHERE host LIKE 'a%' OR host LIKE 'b%'", "host LIKE 'a%' OR host LIKE 'b%'"},
		{"like reescaped quote", "SELECT a FROM cpu WHERE host LIKE 'a''b%'", "host LIKE 'a''b%'"},
		{"like multibyte", "SELECT a FROM cpu WHERE host LIKE 'café%'", "host LIKE 'café%'"},
		// 2e DOUBLE arith in WHERE — re-serialized verbatim (engine owns type-gate + normalize).
		{"arith mul", "SELECT a FROM cpu WHERE value * 100 > 120", "value * 100 > 120"},
		{"arith add float", "SELECT a FROM cpu WHERE value + 0.5 > 1.5", "value + 0.5 > 1.5"},
		{"arith sub spaced", "SELECT a FROM cpu WHERE value - 5.0 > 0.0", "value - 5.0 > 0.0"},
		{"arith in and", "SELECT a FROM cpu WHERE value * 2.0 > 4.0 AND host = 'x'", "value * 2.0 > 4.0 AND host = 'x'"},
		{"arith neg cmp lit", "SELECT a FROM cpu WHERE value * -1.0 >= -0.0", "value * -1.0 >= -0.0"},
		// 2e-division: `/` now serves (any divisor incl. zero / -0.0; engine folds -0.0→+0.0).
		{"div float", "SELECT a FROM cpu WHERE value / 2.0 > 3.0", "value / 2.0 > 3.0"},
		{"div int lit", "SELECT a FROM cpu WHERE value / 2 = 2", "value / 2 = 2"},
		{"div by zero", "SELECT a FROM cpu WHERE value / 0.0 > 2.5", "value / 0.0 > 2.5"},
		{"div neg zero divisor", "SELECT a FROM cpu WHERE value / -0.0 > 0.0", "value / -0.0 > 0.0"},
		{"div in and", "SELECT a FROM cpu WHERE value / 2.0 > 2.0 AND host = 'x'", "value / 2.0 > 2.0 AND host = 'x'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := eligibleShape(tc.sql)
			if !ok || m.shape != ShapeScan {
				t.Fatalf("expected scan-eligible: %q", tc.sql)
			}
			if len(m.preds) != 0 {
				t.Fatalf("boolean-tree WHERE should set whereText, not preds: %+v", m.preds)
			}
			if m.whereText != tc.want {
				t.Fatalf("whereText = %q, want %q", m.whereText, tc.want)
			}
		})
	}
}

func TestMatchScan_BooleanTreeDeclines(t *testing.T) {
	// The re-serializer is a strict allowlist: anything outside the boolean-atom
	// vocabulary declines AT THE ROUTER (never route-then-engine-decline / shadow mismatch).
	decline := []string{
		"SELECT a FROM cpu WHERE a = 1 OR NOT b = 2",         // NOT prefix
		"SELECT a FROM cpu WHERE a = 1 OR b LIKE 'x\\%'",     // LIKE w/ backslash (2d decline)
		"SELECT a FROM cpu WHERE a = 1 OR b LIKE 'x' ESCAPE '!'", // LIKE ESCAPE (2d decline)
		"SELECT a FROM cpu WHERE a = 1 OR b LIKE c",          // LIKE non-literal pattern
		"SELECT a FROM cpu WHERE a = 1 OR lower(b) = 'x'",    // function call
		"SELECT a FROM cpu WHERE a = 1 OR b = 1 + 1",      // arith on the RHS (not col-arith)
		// 2e arith declines (mirror the engine):
		"SELECT a FROM cpu WHERE value // 2.0 > 3.0",      // floor-div (2nd `/` fails isNumericTok)
		"SELECT a FROM cpu WHERE value / 2.0 / 3.0 > 1.0", // second div op
		"SELECT a FROM cpu WHERE value / 2 * 3 > 5.0",     // div then mul (second arith op)
		"SELECT a FROM cpu WHERE value--5.0 > 0.0",        // `--` line comment (CRITICAL)
		"SELECT a FROM cpu WHERE value * 2.0 + 1.0 > 5.0", // multi-term
		"SELECT a FROM cpu WHERE value * host > 5.0",      // column in arith
		"SELECT a FROM cpu WHERE value * 2.0 > host",      // column cmp RHS
		"SELECT a FROM cpu WHERE 2.0 * value > 10.0",      // literal-left orientation
		"SELECT a FROM cpu WHERE (a = 1 OR b = 2",         // unbalanced (
		"SELECT a FROM cpu WHERE a = 1 OR b = 2)",         // stray )
		"SELECT a FROM cpu WHERE ()",                      // empty parens
		"SELECT a FROM cpu WHERE a = 1 OR",                // trailing OR
		"SELECT a FROM cpu WHERE OR a = 1",                // leading OR
	}
	for _, sql := range decline {
		if m, ok := eligibleShape(sql); ok && m.shape == ShapeScan {
			t.Fatalf("expected NOT scan-eligible: %q (whereText=%q)", sql, m.whereText)
		}
	}
}

func TestMatchScan_InList(t *testing.T) {
	// IN / NOT IN (2b-3) route through the boolean-tree re-serializer (whereText); the
	// engine re-lexes and desugars to Or-of-equals / And-of-not-equals.
	cases := []struct {
		name string
		sql  string
		want string
	}{
		{"in ints", "SELECT a FROM cpu WHERE x IN (1, 2, 3)", "x IN (1, 2, 3)"},
		{"in strings", "SELECT a FROM cpu WHERE host IN ('a', 'b')", "host IN ('a', 'b')"},
		{"not in", "SELECT a FROM cpu WHERE x NOT IN (1, 2)", "x NOT IN (1, 2)"},
		{"in single", "SELECT a FROM cpu WHERE x IN (5)", "x IN (5)"},
		{"in composed", "SELECT a FROM cpu WHERE x IN (1,2) AND y NOT IN (3,4)", "x IN (1, 2) AND y NOT IN (3, 4)"},
		{"in string reescaped", "SELECT a FROM cpu WHERE h IN ('we''b', 'x')", "h IN ('we''b', 'x')"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := eligibleShape(tc.sql)
			if !ok || m.shape != ShapeScan {
				t.Fatalf("expected scan-eligible: %q", tc.sql)
			}
			if m.whereText != tc.want {
				t.Fatalf("whereText = %q, want %q", m.whereText, tc.want)
			}
		})
	}
}

func TestMatchScan_InDeclines(t *testing.T) {
	decline := []string{
		"SELECT a FROM cpu WHERE x IN ()",                // empty
		"SELECT a FROM cpu WHERE x NOT IN ()",            // empty NOT IN
		"SELECT a FROM cpu WHERE x IN (1,2",              // unbalanced
		"SELECT a FROM cpu WHERE x IN (1 2)",             // missing comma
		"SELECT a FROM cpu WHERE x IN (,1)",              // leading comma
		"SELECT a FROM cpu WHERE x IN 1",                 // no paren
		"SELECT a FROM cpu WHERE x IN (SELECT a FROM t)", // subquery
		"SELECT a FROM cpu WHERE NOT x IN (1,2)",         // NOT before col (NOT-node)
		"SELECT a FROM cpu WHERE x NOT BETWEEN 1 AND 2",  // NOT BETWEEN
	}
	for _, sql := range decline {
		if m, ok := eligibleShape(sql); ok && m.shape == ShapeScan {
			t.Fatalf("expected NOT scan-eligible: %q (whereText=%q)", sql, m.whereText)
		}
	}
}

func TestWhereHasOrOrParen_RoutesIN(t *testing.T) {
	// LOAD-BEARING: a bare `x IN (1,2)` (no boolean OR/paren) must still route to the
	// tree re-serializer (whereText), because the []scanPred flat path can't represent
	// IN. It routes because the IN list's `(` is a tokPunct. Pin it so nobody narrows
	// whereHasOrOrParen to boolean-parens-only and silently stops routing IN.
	toks, ok := tokenize("SELECT a FROM cpu WHERE x IN (1, 2)")
	if !ok {
		t.Fatal("tokenize failed")
	}
	// advance a cursor to just after WHERE
	c := &cursor{toks: toks}
	for c.i < len(c.toks) && c.toks[c.i].lower != "where" {
		c.i++
	}
	c.i++ // past `where`
	if !whereHasOrOrParen(c) {
		t.Fatal("whereHasOrOrParen must return true for `x IN (1,2)` so it routes to whereText")
	}
	// And end-to-end: it recognizes as a scan with whereText set (not flat preds).
	m, ok := eligibleShape("SELECT a FROM cpu WHERE x IN (1, 2)")
	if !ok || m.shape != ShapeScan || m.whereText == "" || len(m.preds) != 0 {
		t.Fatalf("bare IN should route to whereText: ok=%v whereText=%q preds=%+v", ok, m.whereText, m.preds)
	}
}

func TestMatchScan_DoubleEquality(t *testing.T) {
	// DOUBLE eq (2b-1b): `= f` / `!= f` with a `digit.digit` float literal is accepted
	// as a scan pred (isFloat), mirroring the engine binder. The router recognizes the
	// SHAPE; the engine is the type authority (a float literal on a non-DOUBLE column
	// declines at bind time → DuckDB).
	m, ok := eligibleShape("SELECT host FROM cpu WHERE value = 99.5")
	if !ok || m.shape != ShapeScan || len(m.preds) != 1 {
		t.Fatalf("float eq should be scan-eligible with 1 pred: ok=%v %+v", ok, m.preds)
	}
	p := m.preds[0]
	if p.op != "=" || p.num != "99.5" || !p.isFloat || p.isStr {
		t.Fatalf("float eq pred wrong: %+v", p)
	}
	// `!=`, negative literal, and mixed with another AND pred.
	if m2, ok := eligibleShape("SELECT a FROM cpu WHERE value != -2.25 AND host = 'web'"); !ok ||
		len(m2.preds) != 2 || !m2.preds[0].isFloat || m2.preds[0].num != "-2.25" {
		t.Fatalf("float ne + and wrong: ok=%v %+v", ok, m2.preds)
	}

	// 2b-4: DOUBLE inequality now ROUTES on a finite (non-±0.0) float literal — arrow
	// total_cmp == DuckDB ordering. All four ops + float BETWEEN (desugared Ge/Le).
	accept := []struct {
		sql string
		op  string
	}{
		{"SELECT a FROM cpu WHERE value < 1.5", "<"},
		{"SELECT a FROM cpu WHERE value > 1.5", ">"},
		{"SELECT a FROM cpu WHERE value <= 1.5", "<="},
		{"SELECT a FROM cpu WHERE value >= 1.5", ">="},
	}
	for _, tc := range accept {
		m, ok := eligibleShape(tc.sql)
		if !ok || m.shape != ShapeScan || len(m.preds) != 1 || !m.preds[0].isFloat || m.preds[0].op != tc.op {
			t.Fatalf("float inequality should route (op=%s): ok=%v %+v", tc.op, ok, m.preds)
		}
	}
	// Float BETWEEN desugars in the flat path to two isFloat preds (`>= lo`, `<= hi`).
	if m, ok := eligibleShape("SELECT a FROM cpu WHERE value BETWEEN 1.0 AND 2.0"); !ok ||
		m.shape != ShapeScan || len(m.preds) != 2 ||
		!m.preds[0].isFloat || m.preds[0].op != ">=" || m.preds[0].num != "1.0" ||
		!m.preds[1].isFloat || m.preds[1].op != "<=" || m.preds[1].num != "2.0" {
		t.Fatalf("float BETWEEN should desugar to 2 float preds (2b-4): ok=%v %+v", ok, m.preds)
	}
	// A ±0.0 float BETWEEN bound declines (the desugared `>= 0.0` fires the guard).
	if m, ok := eligibleShape("SELECT a FROM cpu WHERE value BETWEEN 0.0 AND 2.0"); ok && m.shape == ShapeScan {
		t.Fatalf("±0.0 float BETWEEN bound should decline: %+v", m.preds)
	}

	// Declines the router must STILL make (mirror the engine so it never routes a decline):
	decline := []string{
		"SELECT a FROM cpu WHERE value = 0.0",    // ±0.0 (signed-zero divergence) — all ops
		"SELECT a FROM cpu WHERE value = -0.0",   // "
		"SELECT a FROM cpu WHERE value != 0.0",   // "
		"SELECT a FROM cpu WHERE value < 0.0",    // ±0.0 inequality (2b-4 widened the guard)
		"SELECT a FROM cpu WHERE value >= -0.0",  // "
		"SELECT a FROM cpu WHERE value = 00.000", // ±0.0 alt spelling
		"SELECT a FROM cpu WHERE value = 5.",     // `digit.` not a float token → junk
		"SELECT a FROM cpu WHERE value = .5",     // `.digit` not a float token → junk
	}
	for _, sql := range decline {
		if m, ok := eligibleShape(sql); ok && m.shape == ShapeScan {
			t.Fatalf("expected NOT scan-eligible: %q (preds=%+v)", sql, m.preds)
		}
	}
}

func TestMatchScan_StringInequality(t *testing.T) {
	// 2b-4: string `<`/`>`/`<=`/`>=` route (the flat `tokStr` arm has no op restriction;
	// arcx compares byte-wise == DuckDB default BINARY collation). The collation guard in
	// eligibleShape defends the in-query COLLATE case (see TestEligibleShape_Declines).
	for _, op := range []string{"<", ">", "<=", ">="} {
		sql := "SELECT code FROM cpu WHERE host " + op + " 'm'"
		m, ok := eligibleShape(sql)
		if !ok || m.shape != ShapeScan || len(m.preds) != 1 {
			t.Fatalf("string inequality should route (op=%s): ok=%v %+v", op, ok, m.preds)
		}
		p := m.preds[0]
		if p.op != op || !p.isStr || p.str != "m" {
			t.Fatalf("string inequality pred wrong (op=%s): %+v", op, p)
		}
	}
	// String BETWEEN desugars in the flat path to two isStr preds (`>= lo`, `<= hi`).
	if m, ok := eligibleShape("SELECT code FROM cpu WHERE host BETWEEN 'a' AND 'm'"); !ok ||
		m.shape != ShapeScan || len(m.preds) != 2 ||
		!m.preds[0].isStr || m.preds[0].op != ">=" || m.preds[1].op != "<=" {
		t.Fatalf("string BETWEEN should desugar to 2 str preds (2b-4): ok=%v %+v", ok, m.preds)
	}
}

func TestMatchScan_OrderByLimit(t *testing.T) {
	cases := []struct {
		name      string
		sql       string
		wantOrder []scanOrderKey
		wantLimit int
	}{
		{"order asc default", "SELECT code FROM cpu ORDER BY code", []scanOrderKey{{"code", false}}, 0},
		{"order desc", "SELECT code FROM cpu ORDER BY code DESC", []scanOrderKey{{"code", true}}, 0},
		{"order asc explicit", "SELECT code FROM cpu ORDER BY code ASC", []scanOrderKey{{"code", false}}, 0},
		{"order desc limit", "SELECT code FROM cpu ORDER BY code DESC LIMIT 100", []scanOrderKey{{"code", true}}, 100},
		{"multi key", "SELECT code FROM cpu ORDER BY host ASC, code DESC", []scanOrderKey{{"host", false}, {"code", true}}, 0},
		{"where order limit", "SELECT code FROM cpu WHERE code >= 5 ORDER BY code DESC LIMIT 10", []scanOrderKey{{"code", true}}, 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := eligibleShape(tc.sql)
			if !ok || m.shape != ShapeScan {
				t.Fatalf("expected scan-eligible for %q", tc.sql)
			}
			if len(m.orderBy) != len(tc.wantOrder) {
				t.Fatalf("orderBy = %v, want %v", m.orderBy, tc.wantOrder)
			}
			for i := range tc.wantOrder {
				if m.orderBy[i] != tc.wantOrder[i] {
					t.Fatalf("orderBy[%d] = %v, want %v", i, m.orderBy[i], tc.wantOrder[i])
				}
			}
			if m.limit != tc.wantLimit {
				t.Fatalf("limit = %d, want %d", m.limit, tc.wantLimit)
			}
		})
	}
}

func TestMatchScan_Declines(t *testing.T) {
	// Each must NOT be recognized as a scan (either declines entirely or matches a
	// different, more-specific shape). The engine's 2a scan doesn't answer these.
	decline := []string{
		"SELECT * FROM cpu",                          // star not routed (drift-unprovable)
		"SELECT *, host FROM cpu",                    // star mixed
		"SELECT host FROM cpu WHERE a LIKE 'x\\%'",   // LIKE backslash pattern (2d decline)
		"SELECT host FROM cpu WHERE NOT a = 1",       // NOT (2b-2b)
		"SELECT host FROM cpu WHERE (a = 1",          // unbalanced paren
		"SELECT host FROM cpu WHERE a = 1 OR",        // trailing OR
		"SELECT lower(host) FROM cpu",                // non-length function (later sub-phase)
		"SELECT upper(host) FROM cpu",                // non-length function
		"SELECT length(host, 2) FROM cpu",            // length wrong arity
		"SELECT length() FROM cpu",                   // length no args
		"SELECT length('lit') FROM cpu",              // length literal arg (2f-1)
		"SELECT length(upper(host)) FROM cpu",        // length nested function
		"SELECT length(*) FROM cpu",                  // length on star
		"SELECT substr(host) FROM cpu",               // substr missing start arg
		"SELECT substr(host, 1, 2, 3) FROM cpu",      // substr arity 4
		"SELECT substr(host, 'x', 2) FROM cpu",       // substr non-int start
		"SELECT substr(host, +1, 3) FROM cpu",        // substr unary-plus (not tokNum)
		"SELECT substr(host, 1, code) FROM cpu",      // substr column len
		"SELECT substr(upper(host), 1, 2) FROM cpu",  // substr nested function
		"SELECT upper(host) FROM cpu",                // non-PROJ_FUNCS (ICU) — decline
		"SELECT starts_with(host, 1) FROM cpu",       // non-string needle
		"SELECT starts_with(host, code) FROM cpu",    // column needle
		"SELECT starts_with(host) FROM cpu",          // wrong arity
		"SELECT starts_with(host, 'a', 'b') FROM cpu", // wrong arity
		"SELECT contains(host, [1,2,3]) FROM cpu",    // LIST contains overload
		"SELECT starts_with(upper(host), 'a') FROM cpu", // nested function
		"SELECT host FROM cpu LIMIT 10",              // LIMIT without ORDER BY (nondeterministic)
		"SELECT host FROM cpu ORDER BY host LIMIT 0", // LIMIT 0 routed to DuckDB
		"SELECT host FROM cpu ORDER BY 1",            // positional ORDER BY (agg shape, not scan)
		"SELECT host AS h FROM cpu",                  // alias
		"SELECT host FROM cpu GROUP BY host",         // GROUP BY (Phase 3)
		"SELECT host FROM cpu WHERE code >",          // predicate missing literal
		"SELECT host FROM cpu WHERE code > 1 EXTRA",  // trailing junk
		"SELECT FROM cpu",                            // empty projection
		"SELECT from FROM cpu",                       // keyword-as-column
	}
	for _, sql := range decline {
		t.Run(sql, func(t *testing.T) {
			m, ok := eligibleShape(sql)
			if ok && m.shape == ShapeScan {
				t.Fatalf("expected NOT scan-eligible, but matched scan: %q (cols=%v preds=%v)", sql, m.cols, m.preds)
			}
		})
	}
}

func TestMatchScan_IsNull(t *testing.T) {
	cases := []struct {
		name       string
		sql        string
		wantNPreds int
		checkPred  func(*testing.T, scanPred) // on the last pred
	}{
		{"is null", "SELECT a FROM cpu WHERE b IS NULL", 1, func(t *testing.T, p scanPred) {
			if !p.isNull || p.negated || p.col != "b" {
				t.Fatalf("bad IS NULL pred: %+v", p)
			}
		}},
		{"is not null", "SELECT a FROM cpu WHERE b IS NOT NULL", 1, func(t *testing.T, p scanPred) {
			if !p.isNull || !p.negated || p.col != "b" {
				t.Fatalf("bad IS NOT NULL pred: %+v", p)
			}
		}},
		{"mixed with cmp", "SELECT a FROM cpu WHERE code >= 5 AND host IS NULL", 2, func(t *testing.T, p scanPred) {
			if !p.isNull || p.negated {
				t.Fatalf("bad trailing IS NULL pred: %+v", p)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := eligibleShape(tc.sql)
			if !ok || m.shape != ShapeScan {
				t.Fatalf("expected scan-eligible for %q", tc.sql)
			}
			if len(m.preds) != tc.wantNPreds {
				t.Fatalf("preds = %d, want %d", len(m.preds), tc.wantNPreds)
			}
			tc.checkPred(t, m.preds[len(m.preds)-1])
		})
	}
}

func TestMatchScan_MalformedIsDeclines(t *testing.T) {
	for _, sql := range []string{
		"SELECT a FROM cpu WHERE b IS 5",
		"SELECT a FROM cpu WHERE b IS NOT 5",
		"SELECT a FROM cpu WHERE b IS",
	} {
		t.Run(sql, func(t *testing.T) {
			if m, ok := eligibleShape(sql); ok && m.shape == ShapeScan {
				t.Fatalf("expected NOT scan-eligible: %q", sql)
			}
		})
	}
}
