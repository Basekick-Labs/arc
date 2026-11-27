package sql

import (
	"regexp"
	"strings"
)

// CutStatementContext removes the "LINE n: <statement>" block DuckDB appends
// to many error messages, and everything after it. For a query that block
// quotes the statement DuckDB ran: Arc's rewritten SQL, with a read_parquet
// over the storage path of every partition read. SanitizeErrText (logs) and
// ClientErrText (responses) both cut here.
func CutStatementContext(s string) string {
	if i := strings.Index(s, "\nLINE "); i >= 0 {
		s = s[:i]
	}
	return s
}

// TrimErrStatement is ClientErrText without the path redaction: the message
// and its hint lines, without DuckDB's statement context. For routes whose
// errors name files the caller is entitled to see (DELETE reports the
// database-relative file it could not rewrite).
func TrimErrStatement(s string) string {
	return keepMessageLines(CutStatementContext(strings.ReplaceAll(strings.TrimSpace(s), "\r\n", "\n")))
}

// ClientErrText is the text of a DuckDB execution error as a client should
// see it: DuckDB's message and its hint lines (candidate columns, "Did you
// mean", GROUP BY advice, cast hints), without the statement context and
// without anything that names storage. paths are what was removed, for the
// server log: the global log sanitiser (SanitizeErrText) drops them too, and
// support needs to know which file a schema-drift or corrupt-file error was
// about.
//
// SanitizeErrText is the wrong tool for a response: it keeps one line and
// masks every quoted span, removing the column and function names a client
// needs to fix its query.
func ClientErrText(s string) (text string, paths []string) {
	s = TrimErrStatement(s)
	// Redact before capping: a cut inside a quoted path would leave a span
	// with no closing quote.
	s, paths = redactStorage(s)
	if r := []rune(s); len(r) > 1000 {
		s = string(r[:1000]) + "…"
	}
	return s, paths
}

// keepMessageLines keeps at most five lines and drops the trailing blank
// lines DuckDB puts before its statement context.
func keepMessageLines(s string) string {
	lines := strings.Split(s, "\n")
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) > 5 {
		lines = lines[:5]
	}
	return strings.Join(lines, "\n")
}

var (
	clientIPv4        = regexp.MustCompile(`^\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?$`)
	clientHostPort    = regexp.MustCompile(`^[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+:\d+$`)
	clientDrive       = regexp.MustCompile(`^[A-Za-z]:[\\/]`)
	clientLeadingSep  = regexp.MustCompile(`^(?:\.{0,2}[/\\])[A-Za-z0-9_.-]`)
	clientNumericPath = regexp.MustCompile(`^[0-9]+(?:[/\\-][0-9]+)+$`)
)

// storagePathLike reports whether a token, or the inside of a quoted span,
// names a file or a storage location. Judged by content, not by a list of
// prefixes. A lone operator ('/(VARCHAR, INTEGER_LITERAL)'), an all-numeric
// date ('2026/11/26') and a single-slash name ("usage/idle") are kept: they
// are the client's own SQL or data.
func storagePathLike(tok string) bool {
	t := strings.Trim(tok, "()[]{},;:'\"`")
	lower := strings.ToLower(t)
	switch {
	case t == "", clientNumericPath.MatchString(t):
		return false
	case strings.Contains(t, "://"), strings.HasPrefix(lower, "file:"),
		strings.HasPrefix(t, `\\`), strings.Contains(lower, "%2f"),
		strings.HasSuffix(lower, ".parquet"), clientDrive.MatchString(t),
		clientIPv4.MatchString(t), clientHostPort.MatchString(t):
		return true
	}
	seps := strings.Count(t, "/") + strings.Count(t, `\`)
	return seps >= 2 || seps >= 1 && (clientLeadingSep.MatchString(t) || strings.Contains(t, "="))
}

// redactStorage replaces what names storage with <path>: read_parquet(...)
// calls, quoted spans whose content is path-like (the quote characters are
// kept: In file "<path>" the column ...), and unquoted path-like tokens.
// Quoted spans are found with the same quote rules the SQL masker uses (”
// and "" escapes), so a quote inside a path does not end the span early.
func redactStorage(s string) (string, []string) {
	var b strings.Builder
	var paths []string
	b.Grow(len(s))
	word := strings.Builder{}
	flushWord := func() {
		w := word.String()
		word.Reset()
		if w != "" && storagePathLike(w) {
			paths = append(paths, w)
			b.WriteString("<path>")
			return
		}
		b.WriteString(w)
	}
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case hasPrefixFold(s[i:], "read_parquet(") && word.Len() == 0:
			end := skipCall(s, i+len("read_parquet("))
			paths = append(paths, s[i:end])
			b.WriteString("read_parquet(<path>)")
			i = end
		case c == '\'' || c == '"' || c == '`':
			flushWord()
			var end int
			switch c {
			case '\'':
				end = skipStdString(s, i+1)
			case '"':
				end = skipQuotedIdent(s, i+1)
			default:
				end = i + 1
				for end < len(s) && s[end] != '`' {
					end++
				}
				if end < len(s) {
					end++
				}
			}
			span := s[i:end]
			inner := strings.TrimSuffix(span[1:], string(c))
			redact := storagePathLike(inner)
			for _, part := range strings.Fields(inner) {
				redact = redact || storagePathLike(part)
			}
			if redact {
				paths = append(paths, inner)
				b.WriteByte(c)
				b.WriteString("<path>")
				if strings.HasSuffix(span, string(c)) && len(span) > 1 {
					b.WriteByte(c)
				}
			} else {
				b.WriteString(span)
			}
			i = end
		case c == ' ' || c == '\n' || c == '\t':
			flushWord()
			b.WriteByte(c)
			i++
		default:
			word.WriteByte(c)
			i++
		}
	}
	flushWord()
	return b.String(), paths
}

// skipCall returns the index just past the parenthesis that closes a call
// whose opening parenthesis ends just before i, skipping quoted spans.
func skipCall(s string, i int) int {
	depth := 1
	for i < len(s) {
		switch s[i] {
		case '\'':
			i = skipStdString(s, i+1)
			continue
		case '"':
			i = skipQuotedIdent(s, i+1)
			continue
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
		i++
	}
	return len(s)
}

func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}
