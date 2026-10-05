package api

import (
	"fmt"
	"testing"
)

func TestValidateWhereClauseAllowsDangerousTextInsideLiterals(t *testing.T) {
	h := &DeleteHandler{}

	for _, keyword := range []string{
		"DROP", "DELETE", "INSERT", "UPDATE", "EXEC", "EXECUTE", "UNION", "SELECT",
		"CREATE", "ALTER", "COPY", "ATTACH", "DETACH", "LOAD", "INSTALL", "PRAGMA",
		"CALL", "SET",
	} {
		for _, where := range []string{
			fmt.Sprintf("value = '%s'", keyword),
			fmt.Sprintf("value = E'%s'", keyword),
			fmt.Sprintf("value = $$%s$$", keyword),
		} {
			if _, err := h.validateWhereClause(where); err != nil {
				t.Errorf("validateWhereClause(%q) returned error: %v", where, err)
			}
		}
	}

	for _, where := range []string{
		`note = 'a;b--c'`,
		`note = E'a;b--c'`,
		`note = $$a;b--c$$`,
	} {
		if _, err := h.validateWhereClause(where); err != nil {
			t.Errorf("validateWhereClause(%q) returned error: %v", where, err)
		}
	}
}

func TestValidateWhereClauseRejectsDangerousTextOutsideLiterals(t *testing.T) {
	h := &DeleteHandler{}

	for _, where := range []string{
		`1=1); DROP TABLE x --`,
		`host = 'a' OR 1=1; DROP TABLE x`,
		`host = 'a' OR 1=1 --`,
		`host = 'a' OR glob('/data/**') IS NOT NULL`,
	} {
		if _, err := h.validateWhereClause(where); err == nil {
			t.Errorf("validateWhereClause(%q) accepted unsafe input", where)
		}
	}
}

// Edge cases the review added on top of the masking: literals that mention a
// file-I/O function or an identifier that is a keyword stay data; the escaped
// quote, trailing comment and dollar-tag shapes the masker was hardened
// against stay refused; and a quote inside a backtick identifier must not
// open a literal that swallows the call after it.
func TestValidateWhereClauseMaskingEdgeCases(t *testing.T) {
	h := &DeleteHandler{}
	for _, where := range []string{
		`msg = 'glob(/etc/*) failed'`,
		`msg = E'read_csv(x)'`,
		`"offset" = 'set'`,
	} {
		if _, err := h.validateWhereClause(where); err != nil {
			t.Errorf("validateWhereClause(%q) returned error: %v", where, err)
		}
	}
	for _, where := range []string{
		`host = '\' OR 1=1; DROP TABLE x -- '`,
		"host = 'a' -- '\n; DROP TABLE x",
		`host = 'a' AS t$$$ ; DROP TABLE x`,
		`host = "glob"('/etc/*')`,
		"host = `read_csv`('/etc/passwd')",
		"`a'` = 1 OR glob('/etc/passwd') IS NOT NULL OR `'` = 1",
	} {
		if _, err := h.validateWhereClause(where); err == nil {
			t.Errorf("validateWhereClause(%q) accepted unsafe input", where)
		}
	}
}
