package arcxrouter

import (
	"strings"
	"testing"
)

// map-1 companion: the recognizer accepts `col['key']` exactly where the engine
// serves it (select item, WHERE atom head in Scan and ScanAgg) and declines it
// everywhere the engine declines it (ORDER BY, aggregate argument, grouped shapes,
// arithmetic, projection-function argument, malformed keys).

func TestTokenize_Brackets(t *testing.T) {
	toks, ok := tokenize("SELECT a['k'] FROM m")
	if !ok {
		t.Fatal("brackets must lex")
	}
	var puncts []byte
	for _, tk := range toks {
		if tk.kind == tokPunct {
			puncts = append(puncts, tk.punct)
		}
	}
	if string(puncts) != "[]" {
		t.Fatalf("puncts = %q, want [ ]", puncts)
	}
}

func TestMatchScan_Subscript_Accepts(t *testing.T) {
	cases := []struct {
		name      string
		sql       string
		wantCols  []string
		wantPreds []string // flat preds as `col op` or `col IS [NOT] NULL`
		wantWhere string   // tree WHERE text, when the shape goes through the tree
	}{
		{"projection", "SELECT LogAttributes['userId'] FROM otel_logs", []string{"LogAttributes['userId']"}, nil, ""},
		{"projection with bare", "SELECT Timestamp, LogAttributes['userId'], Body FROM otel_logs",
			[]string{"Timestamp", "LogAttributes['userId']", "Body"}, nil, ""},
		{"dotted key", "SELECT ResourceAttributes['k8s.namespace.name'] FROM otel_logs",
			[]string{"ResourceAttributes['k8s.namespace.name']"}, nil, ""},
		{"empty key", "SELECT m[''] FROM t", []string{"m['']"}, nil, ""},
		{"two keys one map", "SELECT m['a'], m['b'] FROM t", []string{"m['a']", "m['b']"}, nil, ""},
		{"flat eq", "SELECT Body FROM otel_logs WHERE ResourceAttributes['service.name'] = 'payment'",
			[]string{"Body"}, []string{"ResourceAttributes['service.name'] ="}, ""},
		{"flat is not null", "SELECT Body FROM otel_logs WHERE LogAttributes['span_id'] IS NOT NULL",
			[]string{"Body"}, []string{"LogAttributes['span_id'] IS NOT NULL"}, ""},
		{"flat between", "SELECT id FROM t WHERE m['k'] BETWEEN 'a' AND 'b'",
			[]string{"id"}, []string{"m['k'] >=", "m['k'] <="}, ""},
		{"flat and mix", "SELECT id FROM t WHERE m['k'] = 'x' AND host = 'h1'",
			[]string{"id"}, []string{"m['k'] =", "host ="}, ""},
		{"tree or", "SELECT id FROM t WHERE m['k'] = 'x' OR host = 'h1'",
			[]string{"id"}, nil, "m['k'] = 'x' OR host = 'h1'"},
		{"tree like", "SELECT id FROM t WHERE m['k'] LIKE 'al%'",
			[]string{"id"}, nil, "m['k'] LIKE 'al%'"},
		{"tree in", "SELECT id FROM t WHERE m['k'] IN ('a', 'b')",
			[]string{"id"}, nil, "m['k'] IN ('a', 'b')"},
		{"tree not in", "SELECT id FROM t WHERE m['k'] NOT IN ('a')",
			[]string{"id"}, nil, "m['k'] NOT IN ('a')"},
		{"tree paren", "SELECT id FROM t WHERE (m['k'] = 'x' OR m['j'] = 'y') AND val > 1",
			[]string{"id"}, nil, "(m['k'] = 'x' OR m['j'] = 'y') AND val > 1"},
		{"order by bare after subscript pred", "SELECT Timestamp, m['k'] FROM t WHERE m['k'] = 'x' ORDER BY Timestamp DESC LIMIT 10",
			[]string{"Timestamp", "m['k']"}, []string{"m['k'] ="}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			toks, ok := tokenize(tc.sql)
			if !ok {
				t.Fatalf("tokenize failed")
			}
			cols, preds, whereText, _, _, _, ok := matchScan(toks)
			if !ok {
				t.Fatalf("matchScan declined: %s", tc.sql)
			}
			if strings.Join(cols, "|") != strings.Join(tc.wantCols, "|") {
				t.Fatalf("cols = %v, want %v", cols, tc.wantCols)
			}
			var got []string
			for _, p := range preds {
				switch {
				case p.isNull && p.negated:
					got = append(got, p.col+" IS NOT NULL")
				case p.isNull:
					got = append(got, p.col+" IS NULL")
				default:
					got = append(got, p.col+" "+p.op)
				}
			}
			if strings.Join(got, "|") != strings.Join(tc.wantPreds, "|") {
				t.Fatalf("preds = %v, want %v", got, tc.wantPreds)
			}
			if whereText != tc.wantWhere {
				t.Fatalf("whereText = %q, want %q", whereText, tc.wantWhere)
			}
		})
	}
}

func TestMatchScan_Subscript_Declines(t *testing.T) {
	cases := []struct{ name, sql string }{
		{"nested", "SELECT m['a']['b'] FROM t"},
		{"numeric key", "SELECT m[1] FROM t"},
		{"ident key", "SELECT m[k] FROM t"},
		{"quote in key", "SELECT m['it''s'] FROM t"},
		{"backslash in key", `SELECT m['a\b'] FROM t`},
		{"unterminated", "SELECT m['a' FROM t"},
		{"bare bracket", "SELECT m[ FROM t"},
		{"order by subscript", "SELECT id FROM t ORDER BY m['k']"},
		{"arith on subscript flat", "SELECT id FROM t WHERE m['k'] + 1 > 2"},
		{"arith on subscript tree", "SELECT id FROM t WHERE m['k'] * 2 > 2 OR id = 1"},
		{"subscript in proj func", "SELECT length(m['k']) FROM t"},
		{"subscript in between hi", "SELECT id FROM t WHERE id BETWEEN 1 AND m['k']"},
		{"subscript as literal", "SELECT id FROM t WHERE host = m['k']"},
		{"subscript on measurement", "SELECT id FROM t['x']"},
		{"dotted base", "SELECT a.b['k'] FROM t"},
		{"dotted base in where", "SELECT id FROM t WHERE a.b['k'] = 'x'"},
		{"dotted base in tree where", "SELECT id FROM t WHERE a.b['k'] = 'x' OR id = 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			toks, ok := tokenize(tc.sql)
			if !ok {
				return // unlexable is a decline too
			}
			if _, _, _, _, _, _, ok := matchScan(toks); ok {
				t.Fatalf("matchScan must decline: %s", tc.sql)
			}
		})
	}
}

func TestMatchScanAgg_Subscript(t *testing.T) {
	// Served: subscript in the WHERE of an ungrouped aggregate (engine ScanAgg).
	toks, _ := tokenize("SELECT count(*) FROM otel_logs WHERE ResourceAttributes['k8s.namespace.name'] = 'kube-system'")
	items, whereText, meas, ok := matchScanAgg(toks)
	if !ok || meas != "otel_logs" || len(items) != 1 || items[0] != "count(*)" {
		t.Fatalf("scan agg with subscript WHERE must match: ok=%v items=%v meas=%q", ok, items, meas)
	}
	if whereText != "ResourceAttributes['k8s.namespace.name'] = 'kube-system'" {
		t.Fatalf("whereText = %q", whereText)
	}
	// Declined: subscript as an aggregate argument (engine review B3).
	for _, sql := range []string{
		"SELECT count(m['k']) FROM t",
		"SELECT min(m['k']) FROM t WHERE host = 'h'",
		"SELECT arg_max(m['k'], time) FROM t",
	} {
		toks, ok := tokenize(sql)
		if !ok {
			continue
		}
		if _, _, _, ok := matchScanAgg(toks); ok {
			t.Fatalf("matchScanAgg must decline: %s", sql)
		}
	}
}

func TestMatchGroupedAgg_Subscript_Declines(t *testing.T) {
	for _, sql := range []string{
		"SELECT host, count(*) FROM t WHERE m['k'] = 'x' GROUP BY host",
		"SELECT m['k'], count(*) FROM t GROUP BY m['k']",
		"SELECT m['k'], count(*) FROM t GROUP BY 1",
		"SELECT host, count(*) FROM t WHERE m['k'] = 'x' OR host = 'a' GROUP BY host",
	} {
		toks, ok := tokenize(sql)
		if !ok {
			continue
		}
		if _, ok := matchGroupedAgg(toks); ok {
			t.Fatalf("matchGroupedAgg must decline: %s", sql)
		}
	}
}

func TestEligibleShape_Subscript(t *testing.T) {
	m, ok := eligibleShape("SELECT Timestamp, LogAttributes['userId'] FROM otel_logs WHERE ResourceAttributes['service.name'] = 'payment' ORDER BY Timestamp DESC LIMIT 100")
	if !ok || m.shape != ShapeScan {
		t.Fatalf("eligibleShape: ok=%v shape=%q", ok, m.shape)
	}
	if _, ok := eligibleShape("SELECT host, count(*) FROM t WHERE m['k'] = 'x' GROUP BY host"); ok {
		t.Fatal("grouped shape with a subscript must be ineligible")
	}
}

func TestIsSubscriptRef(t *testing.T) {
	for _, s := range []string{"m['k']", "LogAttributes['userId']", "m['']", "m['a.b-c d']"} {
		if !isSubscriptRef(s) {
			t.Fatalf("%q must be a subscript ref", s)
		}
	}
	for _, s := range []string{"m", "m['a']['b']", "m[1]", "m['it''s']", `m['a\b']`, "m.n['k']", "m['k'] = 'x'", "m['k']; DROP"} {
		if isSubscriptRef(s) {
			t.Fatalf("%q must NOT be a subscript ref", s)
		}
	}
}
