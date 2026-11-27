package sql

import "strings"

// StripUnicodeSpaces returns sql as DuckDB's parser will read it: every
// Unicode space character DuckDB accepts between tokens, replaced by one
// ASCII space.
//
// DuckDB does not lex those characters. Before parsing, Parser::ParseQuery
// runs Parser::StripUnicodeSpaces (src/parser/parser.cpp; ported from
// v1.5.0, identical in v1.4.3, and checked byte for byte against that C++
// compiled on its own; Arc links v1.5.6) over the statement and parses the result. That pass is
// a small state machine of its own: it skips '…' and "…" spans (a doubled
// quote escapes), $tag$…$tag$ spans (opened at any $ followed by $ or a tag
// byte, wherever it stands) and -- comments ending at CR or LF, and replaces
// U+00A0, U+2000–U+200B, U+202F, U+205F, U+2060, U+3000 and U+FEFF
// everywhere else. It knows nothing of /* */ comments, of E'…' backslash
// escapes or of identifiers, so it can disagree with the lexer about what is
// inside a literal.
//
// Every reading Arc makes of a statement — the validator, the permission
// checks, the rewrites — must start from this text, or it reads a different
// statement from the one DuckDB runs: to Arc's masker a Unicode space is an
// identifier byte, to DuckDB it is a separator. This is a control-flow-exact
// port, including the pass's quirks (see the comments below); keep it that
// way when DuckDB is upgraded, and re-check it against parser.cpp.
func StripUnicodeSpaces(sql string) string {
	for {
		stripped, changed := stripUnicodeSpacesOnce(sql)
		if !changed {
			return sql
		}
		// ParseQuery re-runs the pass on its own output until it finds
		// nothing more; so does this.
		sql = stripped
	}
}

// hasUnicodeSpaceLeadByte reports whether s contains the first byte of any
// UTF-8 sequence the pass replaces. A byte loop, not strings.ContainsAny: that decodes its set
// as runes and would never match a lone lead byte.
func hasUnicodeSpaceLeadByte(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case 0xC2, 0xE2, 0xE3, 0xEF:
			return true
		}
	}
	return false
}

// dollarTagFirst and dollarTagNext are DuckDB's
// IsValidDollarQuotedStringTagFirstChar / …SubsequentChar.
func dollarTagFirst(c byte) bool {
	return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c == '_' || c >= 0x80
}

func dollarTagNext(c byte) bool {
	return dollarTagFirst(c) || c >= '0' && c <= '9'
}

// stripUnicodeSpacesOnce is one run of DuckDB's pass. The states are the
// labels of the C++ (regular, in_quotes, in_dollar_quotes, in_comment); a
// transition sets the state and continues the outer loop WITHOUT advancing
// pos, exactly where the C++ uses goto into a for loop with an empty init.
func stripUnicodeSpacesOnce(sql string) (string, bool) {
	if !hasUnicodeSpaceLeadByte(sql) {
		return sql, false
	}
	type span struct{ pos, n int }
	var spaces []span
	q := sql
	n := len(q)
	pos := 0

	const (
		regular = iota
		inQuotes
		inDollarQuotes
		inComment
	)
	state := regular
	var quote byte
	var tag string

scan:
	for {
		switch state {
		case regular:
			for ; pos+2 < n; pos++ {
				c := q[pos]
				if c == 0xC2 && q[pos+1] == 0xA0 {
					spaces = append(spaces, span{pos, 2}) // U+00A0
				}
				switch {
				case c == 0xE2:
					switch {
					case q[pos+1] == 0x80 && q[pos+2] >= 0x80 && q[pos+2] <= 0x8B:
						spaces = append(spaces, span{pos, 3}) // U+2000–U+200B
					case q[pos+1] == 0x80 && q[pos+2] == 0xAF:
						spaces = append(spaces, span{pos, 3}) // U+202F
					case q[pos+1] == 0x81 && (q[pos+2] == 0x9F || q[pos+2] == 0xA0):
						spaces = append(spaces, span{pos, 3}) // U+205F, U+2060
					}
				case c == 0xE3:
					if q[pos+1] == 0x80 && q[pos+2] == 0x80 {
						spaces = append(spaces, span{pos, 3}) // U+3000
					}
				case c == 0xEF:
					if q[pos+1] == 0xBB && q[pos+2] == 0xBF {
						spaces = append(spaces, span{pos, 3}) // U+FEFF
					}
				case c == '"' || c == '\'':
					quote = c
					pos++
					state = inQuotes
					continue scan
				case c == '$' && (q[pos+1] == '$' || dollarTagFirst(q[pos+1])):
					pos++
					start := pos
					for ; pos+2 < n; pos++ {
						if q[pos] == '$' {
							// The tag ends here; the quoted span is searched
							// from this same $, so `$t$t$` closes at once.
							tag = q[start:pos]
							state = inDollarQuotes
							continue scan
						}
						if !dollarTagNext(q[pos]) {
							// Not a tag after all: this byte is read again
							// as ordinary text, without advancing.
							continue scan
						}
					}
					break scan
				case c == '-' && q[pos+1] == '-':
					state = inComment
					continue scan
				}
			}
			break scan

		case inQuotes:
			for ; pos+1 < n; pos++ {
				if q[pos] == quote {
					if q[pos+1] == quote {
						pos++ // escaped quote; the loop step skips its second half
						continue
					}
					pos++
					state = regular
					continue scan
				}
			}
			break scan

		case inDollarQuotes:
			for ; pos+2 < n; pos++ {
				if q[pos] == '$' && n-(pos+1) >= len(tag)+1 && q[pos+len(tag)+1] == '$' && q[pos+1:pos+1+len(tag)] == tag {
					// Resume on the closer's last $, which may open a new tag.
					pos += len(tag) + 1
					state = regular
					continue scan
				}
			}
			break scan

		case inComment:
			for ; pos < n; pos++ {
				if q[pos] == '\n' || q[pos] == '\r' {
					state = regular
					continue scan
				}
			}
			break scan
		}
	}

	if len(spaces) == 0 {
		return sql, false
	}
	var b strings.Builder
	b.Grow(len(sql))
	prev := 0
	for _, s := range spaces {
		b.WriteString(sql[prev:s.pos])
		b.WriteByte(' ')
		prev = s.pos + s.n
	}
	b.WriteString(sql[prev:])
	return b.String(), true
}

// HasUnicodeSpace reports whether s contains a character DuckDB's pre-pass
// replaces (see StripUnicodeSpaces), wherever it stands.
func HasUnicodeSpace(s string) bool {
	if !hasUnicodeSpaceLeadByte(s) {
		return false
	}
	for _, r := range s {
		switch {
		case r == 0x00A0, r >= 0x2000 && r <= 0x200B, r == 0x202F, r == 0x205F, r == 0x2060, r == 0x3000, r == 0xFEFF:
			return true
		}
	}
	return false
}

// BlockComment reads the /* comment that starts at s[i]. It walks byte by
// byte and looks for an opener before a closer at every position, the way
// DuckDB's scanner does, so in `/*/*/` it sees the second opener that shares
// the first one's '*'. end is the index just past the closing */, or len(s)
// when the comment is not closed; nested reports a second /* before the
// close, which DuckDB would nest and Arc refuses.
func BlockComment(s string, i int) (end int, closed, nested bool) {
	for k := i + 2; k+1 < len(s); k++ {
		switch {
		case s[k] == '/' && s[k+1] == '*':
			return len(s), false, true
		case s[k] == '*' && s[k+1] == '/':
			return k + 2, true, false
		}
	}
	return len(s), false, false
}
