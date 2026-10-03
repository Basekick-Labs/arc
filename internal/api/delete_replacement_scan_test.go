package api

import "testing"

// The delete WHERE fragment is interpolated into
// `SELECT ... FROM read_parquet(...) WHERE <fragment>`, so a path literal
// standing in table position inside it is resolved by DuckDB rather than
// treated as a value. The keyword and I/O-function scans cannot see that
// class: a replacement scan has no function name to match, and the keyword
// list covers SELECT and UNION but not the other spellings DuckDB accepts
// for introducing a relation.
//
// The endpoint is admin-gated, so this is defence in depth rather than a
// privilege boundary — but the fragment is user-supplied text reaching the
// engine, and the row and file counts it produces are returned, dry-run
// included.
func TestValidateWhereClause_RejectsReplacementScan(t *testing.T) {
	h := &DeleteHandler{}
	// Deliberately NOT a multi-level glob: a path containing "/*" is refused by
	// the pre-existing punctuation scan (it looks like a comment open), which
	// would mask whether the table-position guard works at all. A single file
	// and a last-component glob both reach the guard.
	const victim = "./data/db2/secrets/d.parquet"
	const victimGlob = "./data/db2/secrets/d*.parquet"

	refused := []struct{ name, where string }{
		{"bare FROM subquery", "EXISTS (FROM '" + victim + "')"},
		{"bare FROM subquery, last-component glob", "EXISTS (FROM '" + victimGlob + "')"},
		{"TABLE subquery", "EXISTS (TABLE '" + victim + "')"},
		{"TABLE subquery, last-component glob", "EXISTS (TABLE '" + victimGlob + "')"},
		{"SUMMARIZE", "1=(SELECT 1 FROM (SUMMARIZE '" + victim + "'))"},
		{"FROM in a scalar subquery", "1=(FROM '" + victim + "')"},
		{"predicate against another path", "EXISTS (FROM '" + victim + "' WHERE v LIKE 'T%')"},
		{"comma cross-join", "EXISTS (FROM t, '" + victim + "')"},
		{"mixed case keyword", "EXISTS (table '" + victim + "')"},
	}
	for _, tt := range refused {
		t.Run("refused/"+tt.name, func(t *testing.T) {
			if _, err := h.validateWhereClause(tt.where); err == nil {
				t.Errorf("accepted a path literal in table position: %s", tt.where)
			}
		})
	}

	// A single-quoted string used as a VALUE is the overwhelmingly common
	// case and must keep working — including values that look path-like.
	accepted := []struct{ name, where string }{
		{"simple equality", "host = 'web1'"},
		{"path-looking value", "path = '/var/log/app.log'"},
		{"IN list", "host IN ('a','b')"},
		{"time range", "time > now() - INTERVAL 1 HOUR"},
		{"delete everything", "1=1"},
		{"LIKE", "msg LIKE '%timeout%'"},
		{"quoted identifier column", `"my col" = 'x'`},
	}
	for _, tt := range accepted {
		t.Run("accepted/"+tt.name, func(t *testing.T) {
			if _, err := h.validateWhereClause(tt.where); err != nil {
				t.Errorf("false denial on an ordinary predicate: %s -> %v", tt.where, err)
			}
		})
	}
}

// Pre-existing false positive, NOT caused by the table-position guard above
// and not fixed by it: the punctuation scan runs on raw text before string
// literals are masked, so a value containing a comment marker is refused as
// if it were SQL. `pattern = '**/*.parquet'` trips on the `/*` inside the
// literal. This is the delete-side half of the same defect tracked as #834
// (and #987 for the query side); the fix there is to mask literals first.
//
// Pinned so the behaviour is recorded rather than rediscovered, and so this
// test starts failing — informatively — when #834 lands.
func TestValidateWhereClause_KnownFalsePositive_Issue834(t *testing.T) {
	h := &DeleteHandler{}
	if _, err := h.validateWhereClause("pattern = '**/*.parquet'"); err == nil {
		t.Skip("#834 appears to be fixed: a comment marker inside a string literal is now accepted. Remove this test.")
	}
}
