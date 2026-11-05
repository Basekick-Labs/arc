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
		if (ch == 'e' || ch == 'E') && i+1 < len(sql) && sql[i+1] == '\'' && !isIdentifierByte(prevByte(sql, i)) {
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
// MaskFromKeywordsInFunctionBodies would mask: one inside EXTRACT, SUBSTRING,
// TRIM or OVERLAY, or the FROM of an `IS [NOT] DISTINCT FROM` comparison
// (#1472). Callers that rewrite the token after FROM without running the
// masker (the single-table fast path) must bail out when this is true.
// Allocation-free; a false positive only costs the slower path.
func ContainsMaskableFromKeyword(sql string) bool {
	return ContainsFromKeywordFunction(sql) || containsDistinctFromOperator(sql)
}

// containsDistinctFromOperator reports whether some whole-word FROM in sql is
// the FROM of an `IS [NOT] DISTINCT FROM` comparison. Allocation-free.
func containsDistinctFromOperator(sql string) bool {
	for i := 0; i+4 <= len(sql); i++ {
		if (sql[i] == 'f' || sql[i] == 'F') && isFromKeywordAt(sql, i) && isDistinctFromOperatorFrom(sql, i) {
			return true
		}
	}
	return false
}

// isDistinctFromOperatorFrom reports whether the FROM keyword at sql[fromIdx]
// is the FROM of an `IS [NOT] DISTINCT FROM` comparison (#1472): walking
// backwards over ASCII whitespace and SQL comments, the preceding words are
// bare DISTINCT, an optional bare NOT, and bare IS, each directly adjacent.
//
// Adjacency is the safety argument, not keyword reservation: IS is not
// reserved in DuckDB, but `IS [NOT] DISTINCT` is never followed by a FROM that
// introduces a relation. `UNION DISTINCT FROM '…'` (FROM-first syntax) DOES
// read a relation, and fails the IS test, as does any word other than IS.
// Only ASCII whitespace counts as a separator: DuckDB reads `is<U+0085>distinct`
// as one identifier, so a caller must run this before folding Unicode spaces.
// A word inside a comment is skipped as comment, so `-- is distinct⏎FROM t`
// leaves that FROM alone. Quoted words are placeholders by the masker's
// precondition, so `"is" DISTINCT FROM` never matches.
func isDistinctFromOperatorFrom(sql string, fromIdx int) bool {
	start, ok := bareWordBefore(sql, fromIdx)
	if !ok || !equalFoldASCII(sql[start:start+wordLen(sql, start)], "distinct") {
		return false
	}
	start, ok = bareWordBefore(sql, start)
	if !ok {
		return false
	}
	if equalFoldASCII(sql[start:start+wordLen(sql, start)], "not") {
		if start, ok = bareWordBefore(sql, start); !ok {
			return false
		}
	}
	// A qualified `t.is` is a column, not the keyword: DuckDB rejects
	// `t.is DISTINCT FROM`, but nothing here depends on that.
	if start > 0 && sql[start-1] == '.' {
		return false
	}
	return equalFoldASCII(sql[start:start+wordLen(sql, start)], "is")
}

// bareWordBefore returns the start of the identifier-character run that ends
// at the last non-whitespace, non-comment byte before sql[i], or false when
// that byte is not an identifier character.
func bareWordBefore(sql string, i int) (int, bool) {
	j := skipWhitespaceAndCommentsBack(sql, i-1)
	if j < 0 || !isIdentByte(sql[j]) {
		return 0, false
	}
	for j >= 0 && isIdentByte(sql[j]) {
		j--
	}
	return j + 1, true
}

// wordLen returns the length of the identifier-character run at sql[i].
func wordLen(sql string, i int) int {
	n := 0
	for i+n < len(sql) && isIdentByte(sql[i+n]) {
		n++
	}
	return n
}

// MaskFromKeywordsInFunctionBodies replaces whole-word FROM tokens that appear
// inside the argument list of EXTRACT, SUBSTRING, TRIM, or OVERLAY with unique
// placeholders so the caller's table-rewriter regex (which would otherwise
// match `FROM <ident>` greedily inside these expressions) skips them. It masks
// the FROM of an `IS [NOT] DISTINCT FROM` comparison the same way, at any
// paren depth (#1472; see isDistinctFromOperatorFrom). The placeholders
// contain only identifier characters and never satisfy `\bFROM\b`.
//
// SQL comments are copied through untouched: a `(` or a FROM inside one is
// not code. Before #1472 a comment could open a masking frame
// (`/* trim( */ FROM '…' /* ) */`) and hide a real FROM.
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
// MaskStringLiterals. The paren-depth scanner does not re-detect quotes,
// so a literal `'... FROM ...'` inside a trigger-function arg would have
// its FROM masked too if not pre-masked.
//
// Returns the original sql (no allocation) when no trigger function is
// present.
func MaskFromKeywordsInFunctionBodies(sql string) (string, []FromMask) {
	if len(sql) == 0 {
		return sql, nil
	}
	// Cheap pre-check skips the scanner allocation when no trigger
	// function call is present. Round-2 reviewers asked if the
	// pre-check is redundant; it is NOT — without it we'd build a
	// strings.Builder and walk the entire SQL byte-by-byte for every
	// uncached query, even the 99% that have no EXTRACT/SUBSTRING/
	// TRIM/OVERLAY. The miss path is 0 allocs.
	if !ContainsMaskableFromKeyword(sql) {
		return sql, nil
	}

	// stack holds the paren depths at which active mask frames were entered.
	// A new frame is pushed when a `(` is preceded by a trigger function
	// name; popped when paren depth drops back through that level. Nested
	// non-trigger calls (e.g. EXTRACT(YEAR FROM CAST(t AS DATE))) inherit
	// the outer mask because the outer frame stays on the stack until its
	// matching `)`.
	var stack []int
	depth := 0

	var b strings.Builder
	b.Grow(len(sql))

	var masks []FromMask
	maskIndex := 0
	// Reusable scratch buffer for placeholder generation — strconv.AppendInt
	// writes into this byte slice so we avoid fmt.Sprintf's allocation per
	// mask. "__FROM_MASK_" + max-19-digit int + "__" fits in 32 bytes.
	var phBuf [32]byte
	i := 0
	for i < len(sql) {
		c := sql[i]
		// A comment is copied verbatim: neither a paren nor a FROM in it is
		// code. skipWhitespaceAndCommentsForward stops at the first byte that
		// is neither, so the comment ends where it says it does.
		if (c == '/' && i+1 < len(sql) && sql[i+1] == '*') || (c == '-' && i+1 < len(sql) && sql[i+1] == '-') {
			end := skipWhitespaceAndCommentsForward(sql, i)
			b.WriteString(sql[i:end])
			i = end
			continue
		}
		switch c {
		case '(':
			if isFromKeywordFuncBeforeParen(sql, i) {
				stack = append(stack, depth+1)
			}
			depth++
			b.WriteByte(c)
			i++
		case ')':
			depth--
			if len(stack) > 0 && stack[len(stack)-1] > depth {
				stack = stack[:len(stack)-1]
			}
			b.WriteByte(c)
			i++
		default:
			// Only mask FROM at the trigger frame's own paren depth.
			// `EXTRACT(YEAR FROM (SELECT t FROM cpu))` — the inner
			// subquery's `FROM cpu` must be left alone so the regex
			// rewriter can convert `cpu` to read_parquet(...). Without
			// the depth-equality check, DuckDB fails with "Table cpu
			// does not exist". (gemini-code-assist round 2.)
			inFuncFrame := len(stack) > 0 && depth == stack[len(stack)-1]
			if (c == 'f' || c == 'F') && isFromKeywordAt(sql, i) && (inFuncFrame || isDistinctFromOperatorFrom(sql, i)) {
				ph := strconv.AppendInt(append(phBuf[:0], "__FROM_MASK_"...), int64(maskIndex), 10)
				ph = append(ph, '_', '_')
				placeholder := string(ph)
				masks = append(masks, FromMask{Placeholder: placeholder, Original: sql[i : i+4]})
				b.WriteString(placeholder)
				maskIndex++
				i += 4
				continue
			}
			b.WriteByte(c)
			i++
		}
	}
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

// isFromKeywordFuncBeforeParen reports whether the identifier immediately
// preceding the `(` at sql[parenIdx] is one of the trigger SQL builtins
// (EXTRACT, SUBSTRING, TRIM, OVERLAY). Walks backwards over whitespace and
// inline SQL comments (`/* ... */` and `--`-to-EOL) — required because the
// masker runs before stripSQLComments, so `EXTRACT /* c */ (YEAR FROM x)`
// must still be recognised.
//
// Rejects quoted identifiers — `"EXTRACT"(...)` or “ `EXTRACT`(...) “ is
// a user column, not the builtin. DuckDB does not accept backticks for
// identifiers in its default parser, but the check costs nothing and
// matches gemini-code-assist's defensive guidance.
//
// Allocation-free: compares the byte range against fromKeywordFunctions
// in place via equalFoldASCII.
func isFromKeywordFuncBeforeParen(sql string, parenIdx int) bool {
	j := skipWhitespaceAndCommentsBack(sql, parenIdx-1)
	if j < 0 || sql[j] == '"' || sql[j] == '`' {
		return false
	}
	end := j + 1
	for j >= 0 && isIdentByte(sql[j]) {
		j--
	}
	start := j + 1
	if start >= end {
		return false
	}
	ident := sql[start:end]
	for _, name := range fromKeywordFunctions {
		if equalFoldASCII(ident, name) {
			return true
		}
	}
	return false
}

// skipWhitespaceAndCommentsBack walks backwards from sql[i] over ASCII
// whitespace and SQL comments, returning the index of the first non-skip
// byte (or -1 if the scan reaches before the start of the string).
//
// Handles:
//   - whitespace: space, tab, CR, LF.
//   - block comments `/* ... */` — the cursor is on `/` of the closing
//     `*/`; we walk back to the matching `/*` (no nesting in standard SQL).
//   - line comments `-- ... \n` — the closing newline is consumed as
//     whitespace; once we land on the trailing char of a `-- ...` segment
//     we must walk back to the `--` start. Detected by scanning the
//     current line backwards to either start-of-string or `\n`, and
//     checking if a `--` precedes the cursor position on that line.
func skipWhitespaceAndCommentsBack(sql string, i int) int {
	for i >= 0 {
		c := sql[i]
		// Skip whitespace.
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i--
			continue
		}
		// Closing of a block comment `*/`: walk back to the matching `/*`.
		if c == '/' && i > 0 && sql[i-1] == '*' {
			i -= 2 // step past the `/` and `*` of `*/`
			for i > 0 && !(sql[i-1] == '/' && sql[i] == '*') {
				i--
			}
			// i now points at the `*` of `/*`; step past both bytes.
			i -= 2
			continue
		}
		// Possible end of a `-- ...` line comment: walk to start-of-line
		// and check whether a `--` opens a comment that covers position i.
		if dashStart, ok := findDashCommentStartBackward(sql, i); ok {
			i = dashStart - 1
			continue
		}
		return i
	}
	return -1
}

// findDashCommentStartBackward checks whether sql[i] is inside a
// `-- ...`-style line comment that has not yet been terminated by `\n`.
// If so, returns the index of the first `-` in the `--` opener (so the
// caller can resume scanning at that-1). Otherwise returns (_, false).
//
// SQL `--` line comments run from `--` to the next `\n`. The caller has
// already established sql[i] is non-whitespace; we walk back to either
// start-of-string or `\n` and then forward looking for `--` on that line.
func findDashCommentStartBackward(sql string, i int) (int, bool) {
	lineStart := i
	for lineStart > 0 && sql[lineStart-1] != '\n' && sql[lineStart-1] != '\r' {
		lineStart--
	}
	for k := lineStart; k < i; k++ {
		if sql[k] == '-' && k+1 < len(sql) && sql[k+1] == '-' {
			return k, true
		}
	}
	return 0, false
}

// isFromKeywordAt reports whether sql[i:i+4] is a whole-word case-insensitive
// FROM keyword.
func isFromKeywordAt(sql string, i int) bool {
	if i+4 > len(sql) {
		return false
	}
	c0, c1, c2, c3 := sql[i], sql[i+1], sql[i+2], sql[i+3]
	if (c0 != 'f' && c0 != 'F') ||
		(c1 != 'r' && c1 != 'R') ||
		(c2 != 'o' && c2 != 'O') ||
		(c3 != 'm' && c3 != 'M') {
		return false
	}
	if i > 0 && isIdentByte(sql[i-1]) {
		return false
	}
	if i+4 < len(sql) && isIdentByte(sql[i+4]) {
		return false
	}
	return true
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
// valid identifier that must not start with a digit. Bounding the tag this way
// keeps `a$1` (a legal identifier) and `$1` (a positional parameter) from being
// misread as string openers — misreading either would swallow the rest of the
// query into a placeholder and hide real SQL from every scanner.
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
	j := i - 1
	for j >= 0 && sql[j] == '$' {
		j--
	}
	if j >= 0 && isIdentifierByte(sql[j]) {
		return "", false
	}
	k := i + 1
	for k < len(sql) && sql[k] != '$' {
		c := sql[k]
		isAlpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
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

// prevByte returns the byte before index i, or 0 at the start of the string.
func prevByte(s string, i int) byte {
	if i == 0 {
		return 0
	}
	return s[i-1]
}
