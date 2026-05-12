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

	for i < len(sql) {
		ch := sql[i]

		// Check for string literal start (single or double quote)
		if ch == '\'' || ch == '"' {
			quote := ch
			start := i
			i++ // Move past opening quote

			// Find the closing quote, handling escaped quotes
			for i < len(sql) {
				if sql[i] == quote {
					// Check if it's an escaped quote ('' or "")
					if i+1 < len(sql) && sql[i+1] == quote {
						i += 2 // Skip escaped quote
						continue
					}
					// Also handle backslash escaping (\' or \")
					if i > 0 && sql[i-1] == '\\' {
						i++
						continue
					}
					break // Found closing quote
				}
				i++
			}

			// Include the closing quote if found
			if i < len(sql) {
				i++
			}

			// Extract the full string literal and create a placeholder
			original := sql[start:i]
			placeholder := fmt.Sprintf("__STR_%d__", maskIndex)
			masks = append(masks, StringMask{Placeholder: placeholder, Original: original})
			result.WriteString(placeholder)
			maskIndex++
		} else {
			result.WriteByte(ch)
			i++
		}
	}

	return result.String(), masks
}

// UnmaskStringLiterals restores the original string literals from their placeholders.
func UnmaskStringLiterals(sql string, masks []StringMask) string {
	result := sql
	for _, mask := range masks {
		result = strings.Replace(result, mask.Placeholder, mask.Original, 1)
	}
	return result
}

// HasQuotes returns true if the SQL string contains any quote characters.
// This is a fast check to avoid unnecessary processing in MaskStringLiterals.
func HasQuotes(sql string) bool {
	return strings.ContainsAny(sql, "'\"")
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
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		break
	}
	return i
}

// MaskFromKeywordsInFunctionBodies replaces whole-word FROM tokens that appear
// inside the argument list of EXTRACT, SUBSTRING, TRIM, or OVERLAY with unique
// placeholders so the caller's table-rewriter regex (which would otherwise
// match `FROM <ident>` greedily inside these expressions) skips them. The
// placeholders contain only identifier characters and never satisfy
// `\bFROM\b`.
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
	if !ContainsFromKeywordFunction(sql) {
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
			if len(stack) > 0 && depth == stack[len(stack)-1] && (c == 'f' || c == 'F') && isFromKeywordAt(sql, i) {
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
// Rejects quoted identifiers — `"EXTRACT"(...)` or `` `EXTRACT`(...) `` is
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
	for lineStart > 0 && sql[lineStart-1] != '\n' {
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
// single-quoted string literal: 'foo' → 'foo', 'it's' → 'it''s'.
// Use this for every interpolation into a DuckDB SQL fragment that
// cannot be parameterised — most importantly `read_parquet('PATH')`,
// where the path comes from user-controlled inputs (database header,
// measurement name, manifest entries).
//
// SECURITY: This is the single source of truth for DuckDB string-
// literal escaping in the API layer. Sites that interpolate paths
// must call this helper. See internal/api/query.go convertSQL* and
// buildReadParquetExpr for the call sites.
func EscapeStringLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
