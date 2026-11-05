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

func TestValidateWhereClauseRejectsKeywordsGluedToNumbersOrLiterals(t *testing.T) {
	h := &DeleteHandler{}

	for _, where := range []string{
		`1=1UNION VALUES (1)`,
		`host='a'UNION VALUES (1)`,
		`1=1e0UNION VALUES (1)`,
		`1=1.5e3UNION VALUES (1)`,
		`1=.5UNION VALUES (1)`,
		`1=1_000.5e1_0UNION VALUES (1)`,
		`1=1.e1UNION VALUES (1)`,
		`1=1_0UNION VALUES (1)`,
	} {
		if _, err := h.validateWhereClause(where); err == nil {
			t.Errorf("validateWhereClause(%q) accepted a keyword without a word boundary", where)
		}
	}

	for _, where := range []string{
		`x1union = 1`,
		`host = 'aUNION'`,
		`host = 'web1'`,
		`value > 1e5`,
		`value < 1.5e-3 AND other = .25`,
	} {
		if _, err := h.validateWhereClause(where); err != nil {
			t.Errorf("validateWhereClause(%q) returned error: %v", where, err)
		}
	}
}

// Edge cases the review added on top of the masking: literals that mention a
// file-I/O function or an identifier that is a keyword stay data; the escaped
// quote, trailing comment and dollar-tag shapes the masker was hardened
// against stay refused; and a backtick, which is not a quote in DuckDB, is
// refused rather than guessed at (#1480).
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

// Column names containing sp_ or xp_ are accepted, and EXEC xp_cmdshell is
// still refused — by the keyword check, which is what was doing the work all
// along.
//
// #1077 removed the xp_/sp_ prefix scan as dead code: the patterns were
// lowercase and the clause was upper-cased before matching, so nothing was
// ever refused by it. This pins that the removal changed no answer, which
// nothing else did — the delete validator's "legitimate filters" list in
// query_measurement_security_test.go has no identifier containing either
// substring.
//
// It also pins why the other option #1077 offered — upper-casing the patterns
// so the check did what its comment claimed — would have been a bug rather
// than a fix. `resp_time` and `disp_name` are ordinary column names that
// contain sp_, and the query validator's own test fixture lists both as
// clauses that must pass. A working prefix scan would have refused every
// DELETE that mentioned one.
func TestValidateWhereClauseAcceptsIdentifiersContainingProcedurePrefixes(t *testing.T) {
	h := &DeleteHandler{}

	for _, where := range []string{
		`resp_time > 5`,
		`disp_name = 'x'`,
		`sp_count > 0`,
		`xp_total < 10`,
		`transport_mode = 'air'`,
		`value = 'sp_who'`,
	} {
		if _, err := h.validateWhereClause(where); err != nil {
			t.Errorf("validateWhereClause(%q) returned error: %v", where, err)
		}
	}

	// The keyword check, not the deleted prefix scan, is what refuses this.
	if _, err := h.validateWhereClause(`1=1; EXEC xp_cmdshell('dir')`); err == nil {
		t.Error("validateWhereClause accepted EXEC xp_cmdshell; the keyword check must still refuse it")
	}
}

// A WHERE fragment cannot call the functions that change or read state every
// user shares (read_statements.go).
func TestValidateWhereClauseRejectsSideEffectFunctions(t *testing.T) {
	h := &DeleteHandler{}
	for _, where := range []string{
		`EXISTS (FROM enable_logging(['QueryLog']))`,
		`host = 'a' OR EXISTS (FROM duckdb_logs)`,
		`v = nextval('s')`,
		`host = current_setting('threads')`,
		`host = "current_setting"('threads')`,
		"EXISTS (FROM enable_logging (['QueryLog']))",
	} {
		if _, err := h.validateWhereClause(where); err == nil {
			t.Errorf("validateWhereClause(%q) accepted unsafe input", where)
		}
	}
	if _, err := h.validateWhereClause(`host = 'enable_logging(x)'`); err != nil {
		t.Errorf("a function name inside a literal was refused: %v", err)
	}
}

// A DELETE predicate refuses every function the query validator refuses. It
// kept its own copy of the reader list, which missed the readers added after
// it was written and never checked dynamic SQL; under `EXISTS (FROM ...)`
// these ran with no SELECT for the keyword list to catch (GHSA-r7vm-9hcg-xgf7).
func TestValidateWhereClauseRefusesQueryDeniedFunctions(t *testing.T) {
	h := &DeleteHandler{}
	for _, where := range []string{
		"time > 0 AND EXISTS (FROM parquet_full_metadata('/x'))",
		"time > 0 AND EXISTS (FROM read_duckdb('/x'))",
		"time > 0 AND EXISTS (FROM query('SELECT 1'))",
		"time > 0 AND EXISTS (FROM query_table('t'))",
		"time > 0 AND EXISTS (FROM sniff_csv('/x'))",
		"time > 0 AND EXISTS (FROM read_json_objects('/x'))",
		"time > 0 AND EXISTS (FROM read_ndjson_objects('/x'))",
		"time > 0 AND EXISTS (FROM enable_profiling(format := 'json', save_location := '/x.json'))",
		"time > 0 AND EXISTS (FROM \"PARQUET_FULL_METADATA\"('/x'))",
		"time > 0 AND EXISTS (FROM duckdb_external_file_cache())",
		"time > 0 AND EXISTS (FROM histogram_values('/x', v) WHERE bin = 'S')",
		"time > 0 AND EXISTS (FROM histogram('/x', v))",
		"time > 0 AND json_serialize_plan('SELECT * FROM read_parquet(''/x'')') IS NOT NULL",
	} {
		if _, err := h.validateWhereClause(where); err == nil {
			t.Errorf("validateWhereClause accepted %q", where)
		}
	}
	// A column that shares a refused function's name is still a column.
	if _, err := h.validateWhereClause("enable_profiling = 1 AND query > 0 AND histogram > 0"); err != nil {
		t.Errorf("validateWhereClause refused plain columns: %v", err)
	}
}
