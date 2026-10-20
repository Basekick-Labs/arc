package api

import (
	"regexp"
	"strings"
	"unicode"
)

// The query API runs read statements only. DuckDB serves every user from one
// catalog and a pool of connections that are never reset, so a statement that
// changes the catalog (a macro, view, function, schema or table), the session
// (USE, a transaction) or the database (CHECKPOINT, VACUUM) changes what other
// users' queries see. The keyword denylist (dangerousSQLPattern) only refuses
// the statements it names; this allowlist refuses everything that is not a
// read, including statement types DuckDB adds later.

// readStatementKeywords are the statements that only read.
var readStatementKeywords = map[string]bool{
	"select": true, "from": true, "values": true, "pivot": true, "unpivot": true,
	"show": true, "describe": true, "summarize": true,
}

// cteBodyFollowers are the words that may follow a closing parenthesis at the
// top level of a WITH clause without starting the main statement: the column
// list of a CTE (`r(n) AS (`), DuckDB's `USING KEY (k)`, and the
// materialization hints.
var cteBodyFollowers = map[string]bool{
	"as": true, "using": true, "key": true, "not": true, "materialized": true,
}

// isReadStatement reports whether the normalised statement (literals masked,
// comments stripped, Unicode spaces folded, one statement) is a read.
func isReadStatement(normalised string) bool {
	return readStatementAt(strings.ToLower(normalised), 0, false)
}

func readStatementAt(s string, i int, explained bool) bool {
	tok, i := sqlToken(s, i)
	for tok == "(" {
		tok, i = sqlToken(s, i)
	}
	switch {
	case readStatementKeywords[tok]:
		return true
	case tok == "with":
		return withMainIsRead(s, i)
	case tok == "explain" && !explained:
		// EXPLAIN [ANALYZE] [(options)] <statement>: EXPLAIN ANALYZE runs the
		// statement, so the statement itself must be a read.
		next, j := sqlToken(s, i)
		if next == "analyze" || next == "analyse" {
			i = j
			next, j = sqlToken(s, i)
		}
		if next == "(" {
			end, ok := skipParens(s, j, 1)
			if !ok {
				return false
			}
			i = end
		}
		return readStatementAt(s, i, true)
	}
	return false
}

// withMainIsRead finds the statement a WITH clause introduces: the first
// top-level token after a CTE body's closing parenthesis that is not a comma
// or a CTE header word. That statement must be a read; a WITH that ends
// without one is refused.
func withMainIsRead(s string, i int) bool {
	depth := 0
	for {
		tok, j := sqlToken(s, i)
		switch tok {
		case "":
			return false
		case "(":
			depth++
		case ")":
			depth--
			if depth < 0 {
				return false
			}
			if depth == 0 {
				next, _ := sqlToken(s, j)
				switch {
				case next == "," || cteBodyFollowers[next]:
					// another CTE, or the header of this one
				case next == "(" || readStatementKeywords[next]:
					return readStatementAt(s, j, true)
				default:
					return false
				}
			}
		}
		i = j
	}
}

// skipParens returns the index after the parenthesis that closes depth.
func skipParens(s string, i, depth int) (int, bool) {
	for depth > 0 {
		tok, j := sqlToken(s, i)
		switch tok {
		case "":
			return 0, false
		case "(":
			depth++
		case ")":
			depth--
		}
		i = j
	}
	return i, true
}

// sqlToken returns the next token of lower-cased normalised SQL from i: a
// word ([a-z0-9_]+, which includes masked-literal placeholders), or a single
// other character. "" at the end.
func sqlToken(s string, i int) (string, int) {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r' || s[i] == '\f' || s[i] == '\v') {
		i++
	}
	if i >= len(s) {
		return "", i
	}
	start := i
	for i < len(s) && (s[i] >= 'a' && s[i] <= 'z' || s[i] >= '0' && s[i] <= '9' || s[i] == '_') {
		i++
	}
	if i == start {
		i++
	}
	return s[start:i], i
}

// foldUnicodeSpace turns every non-ASCII space (and the zero-width and BOM
// characters) into an ASCII space. DuckDB's lexer accepts them between
// tokens, while the `\s` and `\b` of Go's regexps do not see them as spaces:
// `DELETE<U+00A0>FROM` passed the keyword denylist and ran.
func foldUnicodeSpace(s string) string {
	ascii := true
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			ascii = false
			break
		}
	}
	if ascii {
		return s
	}
	return strings.Map(func(r rune) rune {
		if r >= 0x80 && (unicode.IsSpace(r) || unicode.In(r, unicode.Zs, unicode.Zl, unicode.Zp) ||
			r == 0xFEFF || r == 0x180E || r == 0x2060 || (r >= 0x200B && r <= 0x200D)) {
			return ' '
		}
		return r
	}, s)
}

// hasNestedBlockComment reports whether a block comment of the literal-masked
// SQL contains another `/*`. DuckDB nests block comments, Arc's stripper ends
// one at the first `*/`: in `/* /* */ SELECT */ CREATE ...` the stripper
// leaves `SELECT */ CREATE ...` while DuckDB runs `CREATE ...`. No query
// needs a nested comment, so they are refused rather than modelled.
func hasNestedBlockComment(masked string) bool {
	for i := 0; i+1 < len(masked); i++ {
		switch {
		case masked[i] == '-' && masked[i+1] == '-':
			for i < len(masked) && masked[i] != '\n' {
				i++
			}
		case masked[i] == '/' && masked[i+1] == '*':
			end := strings.Index(masked[i+2:], "*/")
			body := masked[i+2:]
			if end >= 0 {
				body = body[:end]
			}
			if strings.Contains(body, "/*") {
				return true
			}
			if end < 0 {
				return false
			}
			i += 2 + end + 1
		}
	}
	return false
}

// sideEffectFunctionPattern matches the DuckDB functions a SELECT can call to
// change or read state shared by every user: logging (enable_logging turns on
// a database-wide query log that duckdb_logs then shows to any connection),
// checkpoints, sequences, the random seed, secrets, settings and variables.
// Enumerated from duckdb_functions() of the linked DuckDB;
// TestSideEffectFunctionPattern_Family pins the list. The log views are also
// matched as bare names: `FROM duckdb_logs` needs no parentheses.
var sideEffectFunctionPattern = regexp.MustCompile(`(?i)\b(` + strings.Join([]string{
	"checkpoint", "force_checkpoint",
	"enable_logging", "disable_logging", "truncate_duckdb_logs", "write_log",
	"nextval", "currval", "setseed",
	"duckdb_secrets", "which_secret",
	"duckdb_settings", "duckdb_profiling_settings", "current_setting", "duckdb_variables", "getvariable",
}, "|") + `)\s*\(|\b(duckdb_logs|duckdb_logs_parsed|duckdb_log_contexts|pg_settings)\b`)

// sideEffectFunction returns the side-effect function or log view named in
// the identifier-unquoted, literal-masked SQL, or "".
func sideEffectFunction(ioNormalised string) string {
	m := sideEffectFunctionPattern.FindStringSubmatch(ioNormalised)
	if m == nil {
		return ""
	}
	if m[1] != "" {
		return m[1]
	}
	return m[2]
}
