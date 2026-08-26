package sql

import "strings"

// ForLog returns sql with every string-literal BODY replaced by `...`, for use
// in log fields. The statement's shape stays diagnosable (`WHERE host = '...'`);
// the values are gone. This is the log-side counterpart of the slow-query log's
// masking precedent, with one deliberate difference:
//
// It does NOT reuse MaskStringLiterals. That scanner treats backslash as an
// escape inside plain '...' strings; DuckDB does not (backslash is a literal
// character there — only E'...' strings honor it). Reusing it would mean a
// value legitimately ending in `\` (a Windows path, a regex) shifts every
// subsequent literal boundary and logs alternating literal CONTENTS in the
// clear — an under-mask leak on benign input. The scanner below follows
// DuckDB's actual rules. MaskStringLiterals itself is left untouched: it is
// hardened round-trip code on the query-rewrite path, and its escape rule is
// load-bearing there.
//
// Rules:
//   - '...'   standard string: only ” escapes a quote; backslash is literal.
//     Body → `...`.
//   - E'...'  escape string: backslash escapes (incl. \'), ” also escapes.
//     Body → `...`.
//   - $tag$...$tag$ dollar-quoted string: body → `...`, emitted as $$...$$.
//   - "..."   quoted IDENTIFIER: kept verbatim (operators need names to act on
//     a log line; identifiers are schema vocabulary, not row data) —
//     but scanned properly ("" escape) so a quote inside an identifier
//     cannot derail literal detection.
//   - -- and /* */ comment BODIES are masked too: they are arbitrary user text,
//     same class as a literal.
//   - Anything unterminated is masked to end of input (fail closed).
func ForLog(sql string) string {
	var b strings.Builder
	b.Grow(len(sql))
	i := 0
	n := len(sql)
	for i < n {
		c := sql[i]
		switch {
		case c == '\'':
			// Standard string. E-string is handled below before we get here.
			b.WriteString("'...'")
			i = skipStdString(sql, i+1)
		case (c == 'E' || c == 'e') && i+1 < n && sql[i+1] == '\'' && !identChar(prevByte(sql, i)):
			b.WriteString("E'...'")
			i = skipEscString(sql, i+2)
		case c == '$':
			if end, tagLen, ok := scanDollarQuote(sql, i); ok {
				b.WriteString("$$...$$")
				i = end
				_ = tagLen
			} else {
				b.WriteByte(c)
				i++
			}
		case c == '"':
			// Quoted identifier: copy verbatim, honoring "" escapes.
			j := skipQuotedIdent(sql, i+1)
			b.WriteString(sql[i:min(j, n)])
			i = j
		case c == '-' && i+1 < n && sql[i+1] == '-':
			b.WriteString("--...")
			for i < n && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && sql[i+1] == '*':
			b.WriteString("/*...*/")
			i += 2
			for i+1 < n && !(sql[i] == '*' && sql[i+1] == '/') {
				i++
			}
			if i+1 < n {
				i += 2
			} else {
				i = n
			}
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// skipStdString returns the index just past the closing quote of a standard
// '...' string whose opening quote is at i-1. Only ” escapes; backslash is a
// literal character (DuckDB semantics).
func skipStdString(sql string, i int) int {
	n := len(sql)
	for i < n {
		if sql[i] == '\'' {
			if i+1 < n && sql[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return n // unterminated: consume the rest (fail closed)
}

// skipEscString is skipStdString for E'...' strings, where backslash escapes.
func skipEscString(sql string, i int) int {
	n := len(sql)
	for i < n {
		switch sql[i] {
		case '\\':
			i += 2
			continue
		case '\'':
			if i+1 < n && sql[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return n
}

// skipQuotedIdent returns the index just past the closing double quote, with
// "" as the escape.
func skipQuotedIdent(sql string, i int) int {
	n := len(sql)
	for i < n {
		if sql[i] == '"' {
			if i+1 < n && sql[i+1] == '"' {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return n
}

// scanDollarQuote matches $tag$...$tag$ starting at i (sql[i] == '$'). Returns
// (index past the closing delimiter, tag length, true) on a match.
func scanDollarQuote(sql string, i int) (int, int, bool) {
	n := len(sql)
	j := i + 1
	for j < n && identChar(sql[j]) {
		j++
	}
	if j >= n || sql[j] != '$' {
		return 0, 0, false
	}
	delim := sql[i : j+1] // "$tag$" (or "$$")
	body := j + 1
	end := strings.Index(sql[body:], delim)
	if end < 0 {
		return n, len(delim) - 2, true // unterminated: consume the rest
	}
	return body + end + len(delim), len(delim) - 2, true
}

func identChar(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// prevByte is shared with mask.go.

// SanitizeErrText prepares an ERROR STRING for logging or storage: cut the
// engine's "LINE 1: <query>" echo block (verbatim user query text), keep only
// the first line (drops candidate-binding and raw-value echo lines), then mask
// quoted values. Used by the global zerolog Err() hook AND by call sites that
// PERSIST error strings (the query registry), which the log hook cannot reach.
func SanitizeErrText(s string) string {
	if i := strings.Index(s, "\nLINE "); i >= 0 {
		s = s[:i]
	}
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return MaskErrText(s)
}

// MaskErrText masks quoted spans in ERROR TEXT (not SQL): both '...' and "..."
// spans are masked, because engine error messages quote offending VALUES with
// either style ("Could not convert string 'x' …", `invalid timestamp "y"`).
// This deliberately differs from ForLog, which keeps double-quoted identifiers
// — an error message's double-quoted span is usually a value or a name copied
// from user input, and the message class survives without it.
func MaskErrText(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	n := len(s)
	for i < n {
		c := s[i]
		if c == '\'' || c == '"' {
			b.WriteByte(c)
			b.WriteString("...")
			b.WriteByte(c)
			i++
			for i < n && s[i] != c {
				i++
			}
			if i < n {
				i++
			}
			continue
		}
		b.WriteByte(c)
		i++
	}
	return b.String()
}
