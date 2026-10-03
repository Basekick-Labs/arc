package api

import (
	"strings"
	"testing"
)

// DuckDB introduces a relation after FROM and JOIN — and also after TABLE,
// SUMMARIZE, DESCRIBE, PIVOT and UNPIVOT, each of which accepts a bare path
// there and reads the file:
//
//	TABLE '<glob>'                     -> the file's rows
//	SUMMARIZE '<glob>'                 -> min/max/count/approx_unique per column
//	PIVOT '<glob>' ON c USING count(*) -> the column's VALUES as column names
//
// walkTablePositions armed only on from/join, so the replacement-scan guard in
// ValidateSQLRequest never saw those positions. The statement passed
// validation, extractTableReferences emitted no reference for it (so
// checkQueryPermissions authorized an EMPTY set and allowed the query), and
// the rewriter left the literal untouched — DuckDB then read it, anywhere
// inside the sandbox allowlist, which includes the whole local storage root.
//
// This is the fourth round of the replacement-scan class (CVE-2026-47735,
// GHSA-93cm, GHSA-w8x2). The durable fix is to derive the relation set from a
// parse tree rather than by keyword scanning (#764, #491); this closes the
// keywords DuckDB has today.
func TestValidateSQLRequest_RejectsLiteralAfterRelationKeywords(t *testing.T) {
	const victim = "./data/db2/secrets/**/*.parquet"

	rejected := []struct{ name, sql string }{
		{"TABLE", "TABLE '" + victim + "'"},
		{"SUMMARIZE", "SUMMARIZE '" + victim + "'"},
		{"DESCRIBE", "DESCRIBE '" + victim + "'"},
		{"PIVOT", "PIVOT '" + victim + "' ON secret USING count(*)"},
		{"UNPIVOT", "UNPIVOT '" + victim + "' ON secret"},
		{"lowercase keyword", "table '" + victim + "'"},
		{"mixed case keyword", "TaBlE '" + victim + "'"},
		{"newline before the literal", "TABLE\n'" + victim + "'"},
		{"tab before the literal", "TABLE\t'" + victim + "'"},
		{"no space before the literal", "TABLE'" + victim + "'"},
		{"nested in a derived table", "SELECT * FROM (TABLE '" + victim + "')"},
		{"nested SUMMARIZE", "SELECT * FROM (SUMMARIZE '" + victim + "')"},
		{"comma cross-join with a real table", "SELECT v.secret FROM db1.cpu o, (TABLE '" + victim + "') v"},
		{"JOINed", "SELECT * FROM db1.cpu o JOIN (TABLE '" + victim + "') v ON true"},
		{"scalar subquery in the projection", "SELECT (TABLE '" + victim + "') AS s FROM db1.cpu"},
		{"EXISTS predicate oracle", "SELECT 1 FROM db1.cpu WHERE EXISTS (TABLE '" + victim + "')"},
		{"inside a CTE body", "WITH t AS (TABLE '" + victim + "') SELECT * FROM t"},
		{"after a comment", "TABLE /* x */ '" + victim + "'"},
	}
	for _, tt := range rejected {
		t.Run("rejected/"+tt.name, func(t *testing.T) {
			if err := ValidateSQLRequest(tt.sql); err == nil {
				t.Errorf("accepted a path literal in relation position: %s", tt.sql)
			}
		})
	}

	// The keywords stay usable in every form that does not put a literal in
	// relation position. fromArmed is deliberately not set for them, so a
	// comma or an IN-list after one is NOT a table position — these would be
	// refused if it were.
	accepted := []struct{ name, sql string }{
		{"plain query", "SELECT * FROM db1.cpu"},
		{"trailing PIVOT with a literal IN-list", "SELECT * FROM db1.cpu PIVOT (sum(usage) FOR host IN ('a','b'))"},
		{"trailing UNPIVOT", "SELECT * FROM db1.cpu UNPIVOT (v FOR k IN (usage))"},
		{"DESCRIBE a query", "DESCRIBE SELECT * FROM db1.cpu"},
		{"SUMMARIZE a query", "SUMMARIZE SELECT * FROM db1.cpu"},
		{"DESCRIBE a named table", "DESCRIBE db1.cpu"},
		{"the keyword inside a string literal", "SELECT * FROM db1.cpu WHERE msg = 'TABLE x'"},
		{"a literal IN-list", "SELECT * FROM db1.cpu WHERE host IN ('a','b')"},
		{"comma cross-join of real tables", "SELECT * FROM db1.cpu, db1.mem"},
		{"a column named like the keyword, quoted", `SELECT "table" FROM db1.cpu`},
		{"ORDER BY and LIMIT", "SELECT * FROM db1.cpu ORDER BY time DESC LIMIT 1"},
		{"PIVOT on a real table with ON list", "SELECT * FROM db1.cpu PIVOT (count(*) FOR host IN ('a'))"},
	}
	for _, tt := range accepted {
		t.Run("accepted/"+tt.name, func(t *testing.T) {
			if err := ValidateSQLRequest(tt.sql); err != nil {
				t.Errorf("false denial: %s -> %v", tt.sql, err)
			}
		})
	}
}

// The guard must name the offending construct, not leak the path back.
func TestValidateSQLRequest_RelationKeywordErrorDoesNotEchoPath(t *testing.T) {
	const victim = "./data/db2/secrets/**/*.parquet"
	err := ValidateSQLRequest("TABLE '" + victim + "'")
	if err == nil {
		t.Fatal("expected rejection")
	}
	if strings.Contains(err.Error(), victim) {
		t.Errorf("error echoes the probed path back to the caller: %v", err)
	}
}
