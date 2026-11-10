package api

import (
	"context"
	"strings"
	"testing"
)

// Regression tests for #1472: `IS [NOT] DISTINCT FROM` was read as a FROM
// clause. A literal operand was refused as a replacement scan, a column or
// NULL operand was rewritten into read_parquet(), and the RBAC extractor
// checked the operand as a table.

// TestValidateSQLAcceptsDistinctFromOperator: the issue's table, plus the
// sibling refusal of a literal after a function-form FROM.
func TestValidateSQLAcceptsDistinctFromOperator(t *testing.T) {
	for _, sql := range []string{
		"SELECT host FROM cpu WHERE host IS DISTINCT FROM 'a'",
		"SELECT host FROM cpu WHERE host IS NOT DISTINCT FROM 'a'",
		"SELECT 'a' IS DISTINCT FROM 'b' AS d FROM cpu",
		"SELECT host FROM cpu WHERE host IS DISTINCT FROM note",
		"SELECT host FROM cpu WHERE host IS DISTINCT FROM NULL",
		"SELECT host FROM cpu WHERE host is distinct /* c */ from 'a'",
		"SELECT host FROM cpu WHERE host IN (SELECT host FROM mem WHERE note IS DISTINCT FROM 'x')",
		"SELECT trim(BOTH FROM '  x  ') AS t FROM cpu",
		"SELECT trim(LEADING 'x' FROM 'xxa') AS t FROM cpu",
	} {
		if err := ValidateSQLRequest(sql); err != nil {
			t.Errorf("ValidateSQLRequest(%q) = %v, want nil", sql, err)
		}
	}
}

// TestValidateSQLStillRefusesReplacementScans: the masker now reaches the
// table-position guards, so every way it could hide a FROM that DuckDB reads
// a relation from must still be refused. The first two are the bypasses the
// #1472 plan review found on a built binary.
func TestValidateSQLStillRefusesReplacementScans(t *testing.T) {
	const victim = "data/arc/db2/secrets/**/*.parquet"
	for name, sql := range map[string]string{
		"comment opens a trim frame":          "SELECT * /* trim( */ FROM '" + victim + "' /* ) */",
		"line comment opens an extract frame": "SELECT * -- extract(\nFROM '" + victim + "' -- )\n",
		"operator words in a line comment":    "SELECT * -- a IS DISTINCT\nFROM '" + victim + "'",
		"U+0085 joins is and distinct":        "SELECT 'a' is\u0085distinct FROM '" + victim + "'",
		"U+2028 joins is and distinct":        "SELECT 'a' is distinct FROM '" + victim + "'",
		"U+200C joins is and distinct":        "SELECT 'a' is‌distinct FROM '" + victim + "'",
		"UNION DISTINCT FROM":                 "SELECT 1 UNION DISTINCT FROM '" + victim + "'",
		"INTERSECT DISTINCT FROM":             "SELECT 1 INTERSECT DISTINCT FROM '" + victim + "'",
		"EXCEPT DISTINCT FROM":                "SELECT 1 EXCEPT DISTINCT FROM '" + victim + "'",
		"set operation after the operator":    "SELECT 'a' IS DISTINCT FROM 'b' UNION DISTINCT FROM '" + victim + "'",
		"FROM-first subquery as the operand":  "SELECT * FROM cpu WHERE x IS DISTINCT FROM (FROM '" + victim + "')",
		"real FROM after the operator":        "SELECT a IS DISTINCT FROM b FROM '" + victim + "'",
		// The first fix's backward scan read these comment shapes wrongly
		// and hid the real FROM; validation was safe only because it strips
		// comments before masking, which nothing tested (#1472 review).
		"operator words in a block comment holding --": "SELECT v /*is distinct --*/ x FROM '" + victim + "'",
		"block delimiters inside line comments":        "SELECT a IS DISTINCT -- /*\nFROM cpu, 1 AS z -- */\nFROM '" + victim + "'",
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateSQLRequest(sql); err == nil {
				t.Errorf("ValidateSQLRequest(%q) accepted a path literal in table position", sql)
			}
		})
	}
}

// TestExtractTableReferencesIgnoresDistinctFromOperand: the operand is not a
// table, so RBAC must not demand a grant for it — and must not miss the real
// tables around it.
func TestExtractTableReferencesIgnoresDistinctFromOperand(t *testing.T) {
	cases := []struct {
		name, sql string
		want      []string
	}{
		{"column operand", "SELECT host FROM cpu WHERE host IS DISTINCT FROM mem", []string{"default/cpu"}},
		{"qualified column operand", "SELECT host FROM cpu WHERE host IS DISTINCT FROM cpu.host", []string{"default/cpu"}},
		{"db-qualified-looking operand", "SELECT host FROM cpu WHERE host IS DISTINCT FROM db2.secrets", []string{"default/cpu"}},
		{"operator before a comma", "SELECT a IS DISTINCT FROM b, c FROM cpu", []string{"default/cpu"}},
		{"real table in the operand's subquery", "SELECT * FROM cpu WHERE a IS DISTINCT FROM (SELECT b FROM mem)", []string{"default/cpu", "default/mem"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			normalised, identNames := normaliseForExtraction(tc.sql)
			got := refKeys(extractTableReferences(normalised, identNames))
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("extractTableReferences(%q) = %v, want %v", tc.sql, got, tc.want)
			}
		})
	}
}

// TestRewriteLeavesDistinctFromOperandAlone: the rewriters turn only real
// tables into read_parquet(), and never read another measurement through the
// operand. Covers the header-DB variant, whose single-table fast path
// rewrote whatever followed the first FROM.
func TestRewriteLeavesDistinctFromOperandAlone(t *testing.T) {
	h := newCommaJoinTestHandler()
	cases := []struct {
		name, sql string
		headerDB  string
		wantReads []string // rp(db, table) prefixes that must appear
		wantText  string   // operand text that must survive
	}{
		{"column operand", "SELECT host FROM cpu WHERE host IS DISTINCT FROM note", "", []string{rp("default", "cpu")}, "IS DISTINCT FROM note"},
		{"another measurement as operand", "SELECT host FROM cpu WHERE host IS DISTINCT FROM mem", "", []string{rp("default", "cpu")}, "IS DISTINCT FROM mem"},
		{"NULL operand", "SELECT host FROM cpu WHERE host IS NOT DISTINCT FROM NULL", "", []string{rp("default", "cpu")}, "IS NOT DISTINCT FROM NULL"},
		{"qualified operand", "SELECT host FROM cpu WHERE host IS DISTINCT FROM db2.secrets", "", []string{rp("default", "cpu")}, "IS DISTINCT FROM db2.secrets"},
		{"operator before a comma", "SELECT a IS DISTINCT FROM b, c FROM cpu", "", []string{rp("default", "cpu")}, "IS DISTINCT FROM b, c"},
		{"header DB, no table", "SELECT 1 IS DISTINCT FROM 2 AS d", "mydb", nil, "IS DISTINCT FROM 2"},
		{"header DB, newline before the table", "SELECT host IS DISTINCT FROM note AS d FROM\ncpu", "mydb", []string{rp("mydb", "cpu")}, "IS DISTINCT FROM note"},
		{"header DB, column operand", "SELECT host FROM cpu WHERE host IS DISTINCT FROM note", "mydb", []string{rp("mydb", "cpu")}, "IS DISTINCT FROM note"},
		{"comment before IS", "SELECT host FROM cpu WHERE host /* -- */ IS DISTINCT FROM note", "", []string{rp("default", "cpu")}, "IS DISTINCT FROM note"},
		{"block comment holding -- before a real FROM", "SELECT v /*is distinct --*/ x FROM cpu", "", []string{rp("default", "cpu")}, "x FROM read_parquet"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			if tc.headerDB == "" {
				got = h.convertSQLToStoragePaths(context.Background(), tc.sql)
			} else {
				got = h.convertSQLToStoragePathsWithHeaderDB(context.Background(), tc.sql, tc.headerDB)
			}
			if n := strings.Count(got, "read_parquet("); n != len(tc.wantReads) {
				t.Errorf("%d read_parquet calls, want %d: %s", n, len(tc.wantReads), got)
			}
			for _, r := range tc.wantReads {
				if !strings.Contains(got, r) {
					t.Errorf("missing %s in %s", r, got)
				}
			}
			if !strings.Contains(got, tc.wantText) {
				t.Errorf("operand rewritten; want %q in %s", tc.wantText, got)
			}
		})
	}
}

// TestDeleteWhereAcceptsDistinctFromOperator: the DELETE WHERE validator runs
// the same table-position guard.
func TestDeleteWhereAcceptsDistinctFromOperator(t *testing.T) {
	h := &DeleteHandler{}
	for _, where := range []string{
		"host IS DISTINCT FROM 'a'",
		"host IS NOT DISTINCT FROM 'a' AND time > '2026-01-01'",
	} {
		if _, err := h.validateWhereClause(where); err != nil {
			t.Errorf("validateWhereClause(%q) = %v, want nil", where, err)
		}
	}
	for _, where := range []string{
		"EXISTS (FROM '/data/db2/x.parquet') /* trim( */",
		"x IS DISTINCT FROM (FROM '/data/db2/x.parquet')",
		"1=1 /* trim( */ AND EXISTS (FROM '/data/db2/x.parquet') /* ) */",
	} {
		if _, err := h.validateWhereClause(where); err == nil {
			t.Errorf("validateWhereClause(%q) accepted a path literal in table position", where)
		}
	}
}

// TestQueryEntryFastPathBailsOnDistinctFrom: the /api/v1/query entry point has
// its own single-table fast path for header-DB queries, separate from the one
// inside convertSQLToStoragePathsWithHeaderDB, and both rewrote whatever
// followed the first FROM.
func TestQueryEntryFastPathBailsOnDistinctFrom(t *testing.T) {
	h := newParityTestHandler()
	for _, tc := range []struct {
		sql       string
		wantReads int
		wantText  string
	}{
		{"SELECT 1 IS DISTINCT FROM 2 AS d", 0, "IS DISTINCT FROM 2"},
		{"SELECT host IS DISTINCT FROM note AS d FROM\ncpu", 1, "IS DISTINCT FROM note"},
	} {
		got, _, err := h.getTransformedSQLForQuery(context.Background(), tc.sql, "mydb")
		if err != nil {
			t.Fatalf("getTransformedSQLForQuery(%q): %v", tc.sql, err)
		}
		if n := strings.Count(got, "read_parquet("); n != tc.wantReads {
			t.Errorf("%q: %d read_parquet calls, want %d: %s", tc.sql, n, tc.wantReads, got)
		}
		if !strings.Contains(got, tc.wantText) {
			t.Errorf("%q: operand rewritten; want %q in %s", tc.sql, tc.wantText, got)
		}
	}
}
