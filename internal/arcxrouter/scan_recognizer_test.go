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

func TestMatchScan_Declines(t *testing.T) {
	// Each must NOT be recognized as a scan (either declines entirely or matches a
	// different, more-specific shape). The engine's 2a scan doesn't answer these.
	decline := []string{
		"SELECT * FROM cpu",                                  // star not routed (drift-unprovable)
		"SELECT *, host FROM cpu",                            // star mixed
		"SELECT host FROM cpu WHERE a = 1 OR b = 2",          // OR (2b)
		"SELECT host FROM cpu WHERE a IN (1,2)",              // IN (2b) — `in` is an ident, IN(...) not our grammar
		"SELECT host FROM cpu WHERE a LIKE 'x'",              // LIKE (2b)
		"SELECT lower(host) FROM cpu",                        // function in projection (2b)
		"SELECT host FROM cpu ORDER BY host",                 // ORDER BY (engine declines)
		"SELECT host FROM cpu LIMIT 10",                      // LIMIT (engine declines)
		"SELECT host AS h FROM cpu",                          // alias
		"SELECT host FROM cpu GROUP BY host",                 // GROUP BY (Phase 3)
		"SELECT host FROM cpu WHERE code >",                  // predicate missing literal
		"SELECT host FROM cpu WHERE code > 1 EXTRA",          // trailing junk
		"SELECT FROM cpu",                                    // empty projection
		"SELECT from FROM cpu",                               // keyword-as-column
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
