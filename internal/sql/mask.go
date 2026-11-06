// Package sql provides SQL parsing and manipulation utilities.
package sql

import (
	"fmt"
	"strconv"
	"strings"
)

// StringMask holds a placeholder and the original string content it replaced.
type StringMask struct {
	Placeholder string
	Original    string

	// Identifier marks a double-quoted token. In DuckDB SQL `"…"` is an
	// IDENTIFIER, never a string — masking it identically to '…' literals is
	// what made quoted database/measurement names resolve to quote-polluted
	// storage paths (the quotes were spliced back inside the read_parquet
	// glob at unmask time). Identifier masks get a distinct placeholder class
	// so the table-reference rewriter can resolve them to their unquoted
	// names, while every other masking consumer (comment stripping,
	// validateSQL, the FROM-in-function-body scanner, the partition pruner)
	// still sees an inert placeholder.
	Identifier bool
}

// MaskStringLiterals replaces string literals with placeholders to prevent regex from matching inside them.
// Handles both single-quoted ('...') and double-quoted ("...") strings, including escaped quotes.
// Returns the masked SQL and a slice of masks for later restoration.
//
// The hasQuotes parameter is an optimization - if false, the function returns the original
// string without any processing (avoids allocation).
func MaskStringLiterals(sql string, hasQuotes bool) (string, []StringMask) {
	// Fast path: if no quotes, return original string (avoids allocation)
	if !hasQuotes {
		return sql, nil
	}

	var masks []StringMask
	var result strings.Builder
	result.Grow(len(sql))

	i := 0
	maskIndex := 0
	// Identical quoted identifiers share ONE placeholder. The CTE-name
	// registry and the table rewriter compare captured names textually, so
	// `WITH "x-y" AS (…) SELECT * FROM "x-y"` must present the same token at
	// both sites — per-occurrence placeholders would make the FROM site miss
	// the CTE and rewrite it to a nonexistent storage path.
	// Allocated lazily: the common single-quote-only query on this hot path
	// never carries a double-quoted identifier.
	var identPlaceholders map[string]string

	for i < len(sql) {
		ch := sql[i]

		// SECURITY: comments are copied through whole, so a quote inside one
		// cannot open a literal here. This scanner runs BEFORE the comment
		// stripper that the gates use, and a `'` in a comment used to open a
		// literal that ran to the next quote — swallowing the newline the
		// stripper needs to find a line comment's end. The stripper then
		// deleted from `--` to the end of the statement, so whatever followed
		// never reached the keyword, table-position or file-I/O gates, while
		// DuckDB, which ends the comment at the newline, parsed and ran it.
		//
		// A line comment ends at `\n` or `\r`, as in DuckDB.
		//
		// Block comments are scanned to the FIRST `*/`, which is what the
		// stripper does. DuckDB nests them, so its comment can only end later
		// than this one does: text this scanner treats as code and DuckDB
		// does not. A denylist only over-refuses on that, but an allowlist
		// can be fooled by it (`/* /* */ SELECT */ CREATE ...` shows the gate
		// a SELECT), so ValidateSQLRequest refuses nested block comments.
		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			j := i
			for j < len(sql) && sql[j] != '\n' && sql[j] != '\r' {
				j++
			}
			result.WriteString(sql[i:j])
			i = j
			continue
		}
		if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			j := i + 2
			for j+1 < len(sql) && !(sql[j] == '*' && sql[j+1] == '/') {
				j++
			}
			if j+1 < len(sql) {
				j += 2
			} else {
				j = len(sql)
			}
			result.WriteString(sql[i:j])
			i = j
			continue
		}

		// SECURITY: dollar-quoted strings ($tag$…$tag$). DuckDB accepts these
		// wherever a single-quoted literal is legal — including table position,
		// where the string triggers a replacement scan that reads the file
		// directly. Because the form carries NO quote character at all, an
		// unmasked $$…$$ payload also slips past every quote-parity check.
		// Left unmasked, `FROM $$/other-tenant/d.parquet$$` reached DuckDB and
		// returned cross-tenant rows (GHSA-wmjj-g8xc-6hwr review; verified on
		// DuckDB 1.4.3). Masked here so the table-position and denylist scanners
		// see an inert placeholder, exactly as they do for '…'.
		if ch == '$' {
			if tag, ok := dollarQuoteTag(sql, i); ok {
				closing := "$" + tag + "$"
				end := strings.Index(sql[i+len(closing):], closing)
				if end >= 0 {
					stop := i + len(closing) + end + len(closing)
					original := sql[i:stop]
					placeholder := fmt.Sprintf("__STR_%d__", maskIndex)
					maskIndex++
					masks = append(masks, StringMask{Placeholder: placeholder, Original: original})
					result.WriteString(placeholder)
					i = stop
					continue
				}
				// Unterminated dollar quote: mask the remainder so no scanner
				// treats the trailing content as executable SQL.
				original := sql[i:]
				placeholder := fmt.Sprintf("__STR_%d__", maskIndex)
				maskIndex++
				masks = append(masks, StringMask{Placeholder: placeholder, Original: original})
				result.WriteString(placeholder)
				i = len(sql)
				continue
			}
		}

		// SECURITY: escape-string literals (E'…' / e'…'). DuckDB treats the
		// E prefix as part of the literal, so `FROM e'/other/d.parquet'` is the
		// same replacement scan as the bare-quoted form. Masking the quoted body
		// alone would leave a stray `e` bareword sitting in table position, which
		// reads as a plain identifier and hides the replacement scan from
		// stringLiteralInTablePosition. Consume the prefix WITH the literal so the
		// whole construct collapses to one placeholder (GHSA-wmjj-g8xc-6hwr).
		//
		// The prefix must start a token: in `a$e'…'` the `e` continues the
		// identifier `a$e` (DuckDB accepts `$` inside one), and the quote opens a
		// plain string, whose escape rule differs.
		if (ch == 'e' || ch == 'E') && i+1 < len(sql) && sql[i+1] == '\'' && !continuesIdentifier(sql, i) {
			start := i
			i += 2 // move past the E and the opening quote
			i = skipEscString(sql, i)
			original := sql[start:i]
			placeholder := fmt.Sprintf("__STR_%d__", maskIndex)
			maskIndex++
			masks = append(masks, StringMask{Placeholder: placeholder, Original: original})
			result.WriteString(placeholder)
			continue
		}

		// Check for string literal start (single or double quote)
		if ch == '\'' || ch == '"' {
			quote := ch
			start := i

			// SECURITY: scan to the closing quote by DuckDB's rules, which are
			// NOT the same for the three quoted forms. In a plain '…' string and
			// in a "…" identifier the ONLY escape is the doubled quote; a
			// backslash is an ordinary character. Only E'…' honours backslash,
			// and it is consumed by the branch above; the fourth form,
			// $tag$…$tag$, has no escape at all. Treating `\'` as an escape
			// here made the scan run past the real closing quote and swallow
			// everything up to the next one, so a value ending in a backslash
			// hid the rest of the statement from every consumer of the masked
			// form — including the keyword, table-position and file-I/O gates,
			// which then passed SQL that DuckDB parsed quite differently.
			if quote == '\'' {
				i = skipStdString(sql, i+1)
			} else {
				i = skipQuotedIdent(sql, i+1)
			}

			// Extract the full quoted token and create a placeholder.
			original := sql[start:i]
			if quote == '"' {
				// Identifier class: deduplicated, distinct prefix (see the
				// StringMask.Identifier comment).
				placeholder, seen := identPlaceholders[original]
				if !seen {
					if identPlaceholders == nil {
						identPlaceholders = make(map[string]string)
					}
					placeholder = fmt.Sprintf("__IDENT_%d__", maskIndex)
					identPlaceholders[original] = placeholder
					masks = append(masks, StringMask{Placeholder: placeholder, Original: original, Identifier: true})
					maskIndex++
				}
				result.WriteString(placeholder)
			} else {
				placeholder := fmt.Sprintf("__STR_%d__", maskIndex)
				masks = append(masks, StringMask{Placeholder: placeholder, Original: original})
				result.WriteString(placeholder)
				maskIndex++
			}
		} else {
			result.WriteByte(ch)
			i++
		}
	}

	return result.String(), masks
}

// UnmaskStringLiterals restores the original string literals from their placeholders.
//
// One pass over sql: each placeholder found is replaced by its mask's
// original text, and restored text is never scanned again. A string
// placeholder is restored at its first occurrence only; an identifier
// placeholder, deduplicated at mask time, at every occurrence. Replacing
// mask by mask (strings.Replace per mask) copied the whole statement once per
// literal, which is quadratic in the number of literals, and rescanned text
// it had already restored.
func UnmaskStringLiterals(sql string, masks []StringMask) string {
	if len(masks) == 0 {
		return sql
	}
	index := make(map[string]int, len(masks))
	for i, mask := range masks {
		index[mask.Placeholder] = i
	}
	restored := make([]bool, len(masks))
	var b strings.Builder
	b.Grow(len(sql) + len(sql)/4)
	last := 0
	for i := 0; i < len(sql); {
		j := strings.Index(sql[i:], "__")
		if j < 0 {
			break
		}
		j += i
		end := placeholderEnd(sql, j)
		if end < 0 {
			i = j + 1
			continue
		}
		k, ok := index[sql[j:end]]
		if !ok || (!masks[k].Identifier && restored[k]) {
			i = j + 1
			continue
		}
		restored[k] = true
		b.WriteString(sql[last:j])
		b.WriteString(masks[k].Original)
		last, i = end, end
	}
	if last == 0 {
		return sql
	}
	b.WriteString(sql[last:])
	return b.String()
}

// placeholderEnd returns the end of the `__STR_<n>__` or `__IDENT_<n>__`
// placeholder that starts at i in s, or -1 if none starts there. It is
// lenient about the digits: the caller looks the token up among the masks.
func placeholderEnd(s string, i int) int {
	rest := s[i:]
	var n int
	switch {
	case strings.HasPrefix(rest, "__STR_"):
		n = len("__STR_")
	case strings.HasPrefix(rest, "__IDENT_"):
		n = len("__IDENT_")
	default:
		return -1
	}
	d := n
	for d < len(rest) && rest[d] >= '0' && rest[d] <= '9' {
		d++
	}
	if !strings.HasPrefix(rest[d:], "__") {
		return -1
	}
	return i + d + 2
}

// IdentifierNames maps each identifier placeholder to its UNQUOTED name:
// surrounding double quotes stripped and the `""` escape collapsed. This is
// what the table-reference rewriter and the RBAC extractor resolve captured
// placeholders through, so both see the same clean name — the
// extraction-must-match-execution invariant.
func IdentifierNames(masks []StringMask) map[string]string {
	var out map[string]string
	for _, mask := range masks {
		if !mask.Identifier {
			continue
		}
		if out == nil {
			out = make(map[string]string)
		}
		name := mask.Original
		if len(name) >= 2 && name[0] == '"' && name[len(name)-1] == '"' {
			name = name[1 : len(name)-1]
		}
		out[mask.Placeholder] = strings.ReplaceAll(name, `""`, `"`)
	}
	return out
}

// HasQuotes returns true if the SQL string contains any quote characters.
// This is a fast check to avoid unnecessary processing in MaskStringLiterals.
// `$` counts as a quote character here: a dollar-quoted string ($tag$…$tag$)
// carries no ' or " at all, so gating masking on those alone let the whole
// construct through unmasked (GHSA-wmjj-g8xc-6hwr). The masker still verifies
// the `$` actually opens a valid dollar quote, so a stray `$` only costs one
// scan pass, never a false mask.
func HasQuotes(sql string) bool {
	return strings.ContainsAny(sql, "'\"$")
}

// fromKeywordFunctions lists SQL builtins whose argument list contains a bare
// FROM keyword that the table-rewriter regex would otherwise mis-match as a
// `FROM <table>` reference:
//
//	EXTRACT(field FROM source)
//	SUBSTRING(string FROM start FOR length)
//	TRIM([LEADING|TRAILING|BOTH] chars FROM string)
//	OVERLAY(string PLACING new FROM start [FOR length])
//
// POSITION(substring IN string) is intentionally NOT in this list — the IN
// keyword does not collide with the FROM/JOIN rewrite regexes.
var fromKeywordFunctions = [...]string{"extract", "substring", "trim", "overlay"}

// FromMask records a FROM keyword that MaskFromKeywordsInFunctionBodies
// replaced with a unique placeholder so the table-rewriter regex skips it.
// Original preserves the source casing (FROM vs from vs From) so unmask
// restores the user's SQL verbatim — DuckDB accepts any case, but tests
// and downstream tooling pass through the rewriter expect bytewise fidelity.
type FromMask struct {
	Placeholder string
	Original    string
}

// ContainsFromKeywordFunction reports whether sql calls any of the SQL
// builtins whose argument list contains a bare FROM keyword (EXTRACT,
// SUBSTRING, TRIM, OVERLAY). Word-boundary aware so a column literally
// named `extracted` next to a real `EXTRACT(...)` does not hide the real
// call from later passes. Case-insensitive via per-byte fold.
func ContainsFromKeywordFunction(sql string) bool {
	for _, name := range fromKeywordFunctions {
		if findKeywordFunctionCall(sql, name) >= 0 {
			return true
		}
	}
	return false
}

// findKeywordFunctionCall returns the byte offset of the first whole-word
// case-insensitive occurrence of `name` in sql that is followed (modulo
// whitespace and inline SQL comments) by `(`, or -1. The follow-by-paren
// check distinguishes the builtin call site from a same-spelled column
// reference. Comment handling is required because the masker runs before
// stripSQLComments, and `EXTRACT /* c */ (YEAR FROM x)` must still match.
func findKeywordFunctionCall(sql, name string) int {
	n := len(name)
	for i := 0; i+n <= len(sql); i++ {
		if !equalFoldASCII(sql[i:i+n], name) {
			continue
		}
		if i > 0 && isIdentByte(sql[i-1]) {
			continue
		}
		j := i + n
		if j < len(sql) && isIdentByte(sql[j]) {
			continue
		}
		j = skipWhitespaceAndCommentsForward(sql, j)
		if j < len(sql) && sql[j] == '(' {
			return i
		}
	}
	return -1
}

// skipWhitespaceAndCommentsForward walks forward from sql[i] over ASCII
// whitespace and SQL comments (`/* ... */` block and `-- ...\n` line),
// returning the index of the first non-skip byte (or len(sql) if the
// scan reaches the end).
func skipWhitespaceAndCommentsForward(sql string, i int) int {
	for i < len(sql) {
		c := sql[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		if c == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			i += 2
			for i+1 < len(sql) && !(sql[i] == '*' && sql[i+1] == '/') {
				i++
			}
			if i+1 < len(sql) {
				i += 2 // step past `*/`
			} else {
				i = len(sql) // unterminated comment; treat as end-of-input
			}
			continue
		}
		if c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			for i < len(sql) && sql[i] != '\n' && sql[i] != '\r' {
				i++
			}
			continue
		}
		break
	}
	return i
}

// ContainsMaskableFromKeyword reports whether sql holds a FROM keyword that
// MaskFromKeywordsInFunctionBodies masks: one inside EXTRACT, SUBSTRING, TRIM
// or OVERLAY, or the FROM of an `IS [NOT] DISTINCT FROM` comparison (#1472).
// Callers that rewrite the token after FROM without running the masker (the
// single-table fast paths) must bail out when this is true. It is the
// masker's own scan, stopped at the first hit, so the two cannot disagree;
// linear in len(sql) and allocation-free unless a trigger function's
// parenthesis is open.
func ContainsMaskableFromKeyword(sql string) bool {
	found := false
	scanMaskableFroms(sql, func(int) bool {
		found = true
		return false
	})
	return found
}

// maskToken is one token of the scan's look-behind window: a word, or a single
// byte of anything else (punctuation, a quote, a byte of a multi-byte UTF-8
// sequence). Whitespace and comments are not tokens.
type maskToken struct {
	word string // the word, or "" for a non-word byte
	b    byte   // the byte, when word == ""
}

func (t maskToken) is(word string) bool { return t.word != "" && equalFoldASCII(t.word, word) }

// scanMaskableFroms is the one pass behind MaskFromKeywordsInFunctionBodies and
// ContainsMaskableFromKeyword. It walks sql forward, skipping whitespace and
// SQL comments the way DuckDB's lexer does, and calls visit with the byte
// offset of every FROM keyword to mask; visit returns false to stop.
//
// A FROM is masked when it is
//   - inside the argument list of EXTRACT, SUBSTRING, TRIM or OVERLAY, at
//     that call's own paren depth (a subquery in the arguments keeps its FROM);
//   - the FROM of `IS [NOT] DISTINCT FROM`: the three or four tokens before it
//     are the words IS, optional NOT, DISTINCT, and the token before IS is not
//     a '.' (a qualified `t.is` is a column).
//
// Adjacency of words is the safety argument, not keyword reservation: IS is
// not reserved in DuckDB, but `IS [NOT] DISTINCT` is never followed by a FROM
// that introduces a relation, while `UNION DISTINCT FROM '…'` (FROM-first
// syntax) does read one and fails the IS test.
//
// Words are split the way DuckDB splits identifiers: only ASCII space, tab,
// CR, LF and form feed separate them (DuckDB rejects a vertical tab), and a
// non-ASCII byte is part of the word, so `is<U+0085>distinct` and `éis`, which
// DuckDB reads as single identifiers, are never IS. A caller must therefore
// run this before folding Unicode spaces.
//
// Comments are skipped, never read: a word, a paren or a FROM inside one is
// not code. Before #1472 the masker's main loop read comments as code and its
// look-behind scanned back across them, so `/* trim( */` could open a masking
// frame and `/*is distinct --*/ x FROM t` could hide a real FROM.
//
// Linear: one forward pass with a four-token window. The earlier backward
// scans rescanned each line from its start per keyword.
func scanMaskableFroms(sql string, visit func(fromIdx int) bool) {
	var (
		stack []int // paren depths at which a trigger function's args open
		depth int
		win   [4]maskToken // win[3] is the most recent token
	)
	push := func(t maskToken) {
		win[0], win[1], win[2], win[3] = win[1], win[2], win[3], t
	}
	operatorBefore := func() bool {
		if !win[3].is("distinct") {
			return false
		}
		if win[2].is("is") {
			return !(win[1].word == "" && win[1].b == '.')
		}
		return win[2].is("not") && win[1].is("is") && !(win[0].word == "" && win[0].b == '.')
	}
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case (c == '/' && i+1 < len(sql) && sql[i+1] == '*') || (c == '-' && i+1 < len(sql) && sql[i+1] == '-'):
			i = skipWhitespaceAndCommentsForward(sql, i)
		case isScanWordByte(c):
			start := i
			for i < len(sql) && isScanWordByte(sql[i]) {
				i++
			}
			word := sql[start:i]
			if len(word) == 4 && equalFoldASCII(word, "from") {
				inFrame := len(stack) > 0 && depth == stack[len(stack)-1]
				if (inFrame || operatorBefore()) && !visit(start) {
					return
				}
			}
			push(maskToken{word: word})
		case c == '(':
			if w := win[3].word; w != "" && isFromKeywordFunction(w) {
				stack = append(stack, depth+1)
			}
			depth++
			push(maskToken{b: c})
			i++
		case c == ')':
			depth--
			if len(stack) > 0 && stack[len(stack)-1] > depth {
				stack = stack[:len(stack)-1]
			}
			push(maskToken{b: c})
			i++
		default:
			push(maskToken{b: c})
			i++
		}
	}
}

// isScanWordByte reports whether c continues a word for scanMaskableFroms: an
// ASCII identifier byte, or any byte of a multi-byte UTF-8 sequence, since
// DuckDB accepts non-ASCII letters (and the code points Arc folds to spaces)
// inside an unquoted identifier.
func isScanWordByte(c byte) bool { return isIdentByte(c) || c >= 0x80 }

// isFromKeywordFunction reports whether word names EXTRACT, SUBSTRING, TRIM or
// OVERLAY, case-insensitively.
func isFromKeywordFunction(word string) bool {
	for _, name := range fromKeywordFunctions {
		if equalFoldASCII(word, name) {
			return true
		}
	}
	return false
}

// fromMaskPrefix opens every MaskFromKeywordsInFunctionBodies placeholder.
const fromMaskPrefix = "__FROM_MASK_"

// ContainsReservedPlaceholder reports whether s holds the opening of a
// placeholder the maskers in this package generate: `__STR_`, `__IDENT_` or
// `__FROM_MASK_`. The restorers match placeholders by their exact text,
// wherever it appears, so a caller-written copy is restored with the real
// ones; names and other text spliced into masked SQL must not carry it.
func ContainsReservedPlaceholder(s string) bool {
	return strings.Contains(s, "__STR_") || strings.Contains(s, "__IDENT_") || strings.Contains(s, fromMaskPrefix)
}

// ContainsFromMaskPlaceholder reports whether s holds the opening of a
// MaskFromKeywordsInFunctionBodies placeholder. UnmaskFromKeywordsInFunctionBodies
// restores every occurrence of a placeholder's text, so caller SQL that
// spells one gains a FROM after the checks that ran on the masked form.
// Run it on the SQL as written and again after MaskStringLiterals: a
// literal's placeholder ends in `__`, so text written straight after a
// literal can complete one that the raw SQL lacks.
func ContainsFromMaskPlaceholder(s string) bool {
	return strings.Contains(s, fromMaskPrefix)
}

// MaskFromKeywordsInFunctionBodies replaces the FROM keywords that do not
// introduce a relation with unique placeholders, so the caller's
// table-rewriter regex (which would otherwise match `FROM <ident>` greedily)
// skips them: the FROM inside the argument list of EXTRACT, SUBSTRING, TRIM or
// OVERLAY, and the FROM of `IS [NOT] DISTINCT FROM` (#1472). See
// scanMaskableFroms for the exact rules. The placeholders contain only
// identifier characters and never satisfy `\bFROM\b`.
//
// The mask is content-addressed via the placeholders, not offset-based —
// callers may run arbitrary length-changing transformations between the
// mask and unmask calls (e.g. the FROM/JOIN rewrites replace short
// measurement names with `read_parquet('long/path/**/*.parquet')`).
// UnmaskFromKeywordsInFunctionBodies restores the originals by simple
// string replacement. The trailing `__` in `__FROM_MASK_N__` isolates
// counters so unmask order is irrelevant (e.g. mask 1 cannot be a
// substring of mask 10).
//
// PRECONDITION: string literals must already be masked by the caller via
// MaskStringLiterals, so a quoted "is" or a literal containing FROM is a
// placeholder. Comments may still be present; they are skipped.
//
// Returns the original sql (no allocation) when there is nothing to mask.
func MaskFromKeywordsInFunctionBodies(sql string) (string, []FromMask) {
	if len(sql) == 0 {
		return sql, nil
	}
	var at []int
	scanMaskableFroms(sql, func(i int) bool {
		at = append(at, i)
		return true
	})
	if len(at) == 0 {
		return sql, nil
	}

	var b strings.Builder
	b.Grow(len(sql) + len(at)*16)
	masks := make([]FromMask, 0, len(at))
	// Reusable scratch buffer for placeholder generation — strconv.AppendInt
	// writes into this byte slice so we avoid fmt.Sprintf's allocation per
	// mask. "__FROM_MASK_" + max-19-digit int + "__" fits in 32 bytes.
	var phBuf [32]byte
	last := 0
	for n, i := range at {
		ph := strconv.AppendInt(append(phBuf[:0], fromMaskPrefix...), int64(n), 10)
		ph = append(ph, '_', '_')
		placeholder := string(ph)
		masks = append(masks, FromMask{Placeholder: placeholder, Original: sql[i : i+4]})
		b.WriteString(sql[last:i])
		b.WriteString(placeholder)
		last = i + 4
	}
	b.WriteString(sql[last:])
	return b.String(), masks
}

// UnmaskFromKeywordsInFunctionBodies restores FROM tokens that
// MaskFromKeywordsInFunctionBodies replaced with placeholders. Safe to run
// after arbitrary length-changing transformations on the masked SQL —
// placeholders are unique strings, restored by string replace.
//
// Uses strings.NewReplacer for a single-pass walk over sql; the old
// loop-of-Replace allocated one intermediate string per mask
// (gemini-code-assist round 2).
func UnmaskFromKeywordsInFunctionBodies(sql string, masks []FromMask) string {
	if len(masks) == 0 {
		return sql
	}
	pairs := make([]string, 0, len(masks)*2)
	for _, m := range masks {
		pairs = append(pairs, m.Placeholder, m.Original)
	}
	return strings.NewReplacer(pairs...).Replace(sql)
}

// isIdentByte returns true if c is a valid SQL identifier byte (a-z, A-Z,
// 0-9, _). Mirrors internal/api.isIdentChar; redeclared here to keep this
// package self-contained.
func isIdentByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_'
}

// equalFoldASCII compares a and b case-insensitively over ASCII. Faster than
// strings.EqualFold for the all-ASCII function-name compare we need.
func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// EscapeStringLiteral escapes single quotes for safe use as a DuckDB
// single-quoted string literal: 'foo' → 'foo', 'it's' → 'it”s'.
// Use this for every interpolation into a DuckDB SQL fragment that
// cannot be parameterised — most importantly `read_parquet('PATH')`,
// where the path comes from user-controlled inputs (database header,
// measurement name, manifest entries).
//
// SECURITY: This is the single source of truth for DuckDB string-literal
// escaping. Sites that interpolate paths must call this helper or
// QuoteStringLiteral.
func EscapeStringLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// QuoteStringLiteral escapes a string and wraps it in DuckDB single quotes.
func QuoteStringLiteral(s string) string {
	return "'" + EscapeStringLiteral(s) + "'"
}

// dollarQuoteTag reports whether a dollar-quote opener starts at sql[i] and, if
// so, returns its tag (empty for the anonymous `$$` form).
//
// DuckDB/Postgres dollar quoting is `$tag$body$tag$`, where tag is empty or a
// valid identifier that must not start with a digit. As in an unquoted
// identifier, every byte of a multi-byte UTF-8 character is a tag byte:
// DuckDB reads `$é$…$é$` as a string, so the masker must too, to keep the same
// code/literal split the checks rely on. Bounding the tag this way keeps `a$1`
// (a legal identifier) and `$1` (a positional parameter) from being misread as
// string openers, which would fold the rest of the query into one placeholder.
func dollarQuoteTag(sql string, i int) (string, bool) {
	if i >= len(sql) || sql[i] != '$' {
		return "", false
	}
	// SECURITY: a `$` that continues an identifier is part of that identifier
	// (or a positional parameter), not a quote opener — and DuckDB accepts `$`
	// itself inside an unquoted identifier, so the run has to be walked back,
	// not just the single preceding byte. `t$$$` is one name; reading its
	// second and third `$` as an opener found no closing tag and masked the
	// rest of the statement into one placeholder, hiding it from every gate
	// while DuckDB parsed the name and ran what followed.
	if continuesIdentifier(sql, i) {
		return "", false
	}
	k := i + 1
	for k < len(sql) && sql[k] != '$' {
		c := sql[k]
		isAlpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c >= 0x80
		isDigit := c >= '0' && c <= '9'
		if !isAlpha && !(isDigit && k > i+1) {
			return "", false
		}
		k++
	}
	if k >= len(sql) {
		return "", false
	}
	return sql[i+1 : k], true
}

// isIdentifierByte reports whether c can appear inside an unquoted SQL identifier.
//
// SECURITY: every byte of a multi-byte UTF-8 character counts. DuckDB accepts
// Unicode letters in unquoted identifiers, so in `é$$` the `$$` continues the
// identifier `é$$`; judging by the continuation byte alone read it as a
// dollar-quote opener that masked everything up to the next `$$` (another
// statement, a file read) from every gate while DuckDB ran it. The same holds
// for the `E'` escape-string prefix.
func isIdentifierByte(c byte) bool {
	return c >= 0x80 || c == '_' ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}

// continuesIdentifier reports whether the byte at i would continue an unquoted
// identifier that ends just before it. DuckDB accepts `$` inside an identifier
// (not as its first byte), so a run of `$` is walked back and the byte before
// the run decides: `t$$$` and `a$e` are each one name, while a lone `$` is not.
func continuesIdentifier(sql string, i int) bool {
	j := i - 1
	for j >= 0 && sql[j] == '$' {
		j--
	}
	return j >= 0 && isIdentifierByte(sql[j])
}

// Span marks a byte range [Start, End) of sql that is inside a string literal
// (any of the four quoted forms) or a comment — the regions a text rewrite
// must not match into, because changing their bytes changes what the SQL
// means or where a literal/comment ends. Code is every byte outside a span.
type Span struct {
	Start int
	End   int
}

// scanSQLSpans walks sql once and calls emit for each maximal literal or
// comment span, in order, using the SAME leaf scanners and the SAME branch
// order as MaskStringLiterals (single/double quotes, E'…', $tag$…$tag$) and as
// stripSQLComments (line comment to \n or \r, block comment to the first */).
// It is the single place those rules live for span consumers;
// TestProtectedSpansPropertyVsMasker pins it against MaskStringLiterals so the
// two cannot drift.
func scanSQLSpans(sql string, emit func(start, end int)) {
	i := 0
	n := len(sql)
	for i < n {
		ch := sql[i]

		// Line comment: ends at \n or \r (DuckDB), like stripSQLComments.
		if ch == '-' && i+1 < n && sql[i+1] == '-' {
			j := i
			for j < n && sql[j] != '\n' && sql[j] != '\r' {
				j++
			}
			emit(i, j)
			i = j
			continue
		}
		// Block comment: to the first */ (not nested), like stripSQLComments.
		if ch == '/' && i+1 < n && sql[i+1] == '*' {
			j := i + 2
			for j+1 < n && !(sql[j] == '*' && sql[j+1] == '/') {
				j++
			}
			if j+1 < n {
				j += 2
			} else {
				j = n
			}
			emit(i, j)
			i = j
			continue
		}
		// Dollar-quoted string $tag$…$tag$ (no escape; may be unterminated).
		if ch == '$' {
			if tag, ok := dollarQuoteTag(sql, i); ok {
				closing := "$" + tag + "$"
				rel := strings.Index(sql[i+len(closing):], closing)
				var stop int
				if rel >= 0 {
					stop = i + len(closing) + rel + len(closing)
				} else {
					stop = n // unterminated: to end
				}
				emit(i, stop)
				i = stop
				continue
			}
		}
		// Escape string E'…' / e'…' (backslash escapes); the prefix is part of
		// the span. Same guard as the masker (continuesIdentifier), and
		// skipEscString folds quote-continuations identically.
		if (ch == 'e' || ch == 'E') && i+1 < n && sql[i+1] == '\'' && !continuesIdentifier(sql, i) {
			start := i
			i = skipEscString(sql, i+2)
			emit(start, i)
			continue
		}
		// Plain '…' string or "…" identifier (only the doubled quote escapes).
		if ch == '\'' {
			start := i
			i = skipStdString(sql, i+1)
			emit(start, i)
			continue
		}
		if ch == '"' {
			start := i
			i = skipQuotedIdent(sql, i+1)
			emit(start, i)
			continue
		}
		i++
	}
}

// ProtectedSpans returns the literal and comment spans of sql, in order. A
// text rewrite that matches outside every span, and whose own output does not
// alter span structure, keeps the SQL's meaning unchanged.
func ProtectedSpans(sql string) []Span {
	var spans []Span
	scanSQLSpans(sql, func(start, end int) {
		spans = append(spans, Span{Start: start, End: end})
	})
	return spans
}

// InProtectedSpan reports whether byte offset pos falls inside one of spans
// (which must be in ascending, non-overlapping order, as ProtectedSpans
// returns them).
func InProtectedSpan(spans []Span, pos int) bool {
	for _, s := range spans {
		if pos < s.Start {
			return false
		}
		if pos < s.End {
			return true
		}
	}
	return false
}

// IsMaskerClean reports whether s contains no string literal and no comment:
// MaskStringLiterals leaves it byte-for-byte unchanged and it has no comment
// marker. A fragment that is masker-clean can be duplicated or moved by a
// rewrite without changing the surrounding SQL's literal/comment structure.
// s must begin in code (a caller checks the match start is not in a span
// first), so a fragment that opens a literal is detected by the masker here.
func IsMaskerClean(s string) bool {
	if strings.Contains(s, "--") || strings.Contains(s, "/*") || strings.Contains(s, "*/") {
		return false
	}
	if !HasQuotes(s) {
		return true
	}
	// A `'`, `"` or `$` that does not open a literal (a dollar-quoted body
	// needs a closing tag; `ts$a$` is one identifier) leaves the masker output
	// identical, and is clean.
	masked, _ := MaskStringLiterals(s, true)
	return masked == s
}
