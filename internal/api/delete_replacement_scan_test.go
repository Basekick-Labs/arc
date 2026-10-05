package api

import (
	"fmt"
	"testing"
)

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

// The scans run on the string-literal-MASKED clause (#834): a forbidden word,
// ';' or a comment marker inside a literal is data. The same syntax outside a
// literal, in every quoted form the masker knows, must still be refused.
func TestDeleteWhereIgnoresDangerousTokensInsideStringLiterals(t *testing.T) {
	h := &DeleteHandler{}
	keywords := []string{"drop", "delete", "insert", "update", "exec", "execute", "union", "select", "create", "alter", "copy", "attach", "detach", "load", "install", "pragma", "call", "set"}
	for _, keyword := range keywords {
		where := fmt.Sprintf("host = '%s'", keyword)
		if _, err := h.validateWhereClause(where); err != nil {
			t.Errorf("FALSE POSITIVE: DELETE rejected literal %q: %v", where, err)
		}
	}
	valid := []string{`host = E'drop'`, `host = $$drop$$`, `note = 'a;b--c'`, `msg = 'glob(/etc/*) failed'`, `msg = E'read_csv(x)'`, `"offset" = 'set'`}
	for _, where := range valid {
		if _, err := h.validateWhereClause(where); err != nil {
			t.Errorf("FALSE POSITIVE: DELETE rejected literal %q: %v", where, err)
		}
	}
	invalid := []string{`1=1); DROP TABLE x --`, `host = 'a' OR 1=1; DROP TABLE x`, `host = 'a'; -- comment`, `host = '\' OR 1=1; DROP TABLE x -- '`, "host = 'a' -- '\n; DROP TABLE x", `host = 'a' AS t$$$ ; DROP TABLE x`,
		// The file-I/O scan must still see a call outside a literal, in both its bare and its identifier-quoted spelling.
		`host IN (SELECT file FROM glob('/etc/*'))`, `host = "glob"('/etc/*')`, `host = ` + "`read_csv`" + `('/etc/passwd')`,
		// A quote inside a backtick identifier must not open a literal that swallows the call after it.
		"`a'` = 1 OR glob('/etc/passwd') IS NOT NULL OR `'` = 1"}
	for _, where := range invalid {
		if _, err := h.validateWhereClause(where); err == nil {
			t.Errorf("DELETE accepted dangerous syntax outside literal: %q", where)
		}
	}
}
