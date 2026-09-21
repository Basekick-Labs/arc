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
