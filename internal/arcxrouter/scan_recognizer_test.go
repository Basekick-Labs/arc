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

	// Declines the router must make (mirror the engine so it never routes a decline):
	decline := []string{
		"SELECT a FROM cpu WHERE value < 1.5",               // DOUBLE inequality (NaN-ordering, 2b-4)
		"SELECT a FROM cpu WHERE value > 1.5",               // "
		"SELECT a FROM cpu WHERE value <= 1.5",              // "
		"SELECT a FROM cpu WHERE value = 0.0",               // ±0.0 (signed-zero divergence)
		"SELECT a FROM cpu WHERE value = -0.0",              // "
		"SELECT a FROM cpu WHERE value != 0.0",              // "
		"SELECT a FROM cpu WHERE value = 00.000",            // ±0.0 alt spelling
		"SELECT a FROM cpu WHERE value BETWEEN 1.0 AND 2.0", // float BETWEEN (Ge/Le → decline)
		"SELECT a FROM cpu WHERE value = 5.",                // `digit.` not a float token → junk
		"SELECT a FROM cpu WHERE value = .5",                // `.digit` not a float token → junk
	}
	for _, sql := range decline {
		if m, ok := eligibleShape(sql); ok && m.shape == ShapeScan {
			t.Fatalf("expected NOT scan-eligible: %q (preds=%+v)", sql, m.preds)
		}
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
		"SELECT host FROM cpu WHERE a = 1 OR b = 2",  // OR (2b)
		"SELECT host FROM cpu WHERE a IN (1,2)",      // IN (2b) — `in` is an ident, IN(...) not our grammar
		"SELECT host FROM cpu WHERE a LIKE 'x'",      // LIKE (2b)
		"SELECT lower(host) FROM cpu",                // function in projection (2b)
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
