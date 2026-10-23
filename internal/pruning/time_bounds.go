package pruning

import (
	"regexp"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/sql"
)

// The time-range scanner behind ExtractTimeRange and the exact-shape checks
// (IsExactTimeRange, IsStartOnlyTimeRange).
//
// A range is used to leave partitions (and, with file-time pruning, files)
// out of a query, so it must hold for every row the query can return. The
// regexes it replaces read the first matching "time >= …" anywhere in the
// first WHERE clause and applied it to every table, which returned too few
// rows for, among others: a bound under OR or NOT, a column merely ending in
// "time", arithmetic after the literal, an inclusive end on an hour boundary,
// a commented-out bound, a bound in a subquery, JOIN, UNION or CTE, and a
// column named "timestamp" (the partition column is always "time").
//
// The rule now: a bound is taken only from a top-level AND term of the one
// WHERE clause of a single-table query, and only when that whole term is a
// bare comparison of the time column with a literal or with now() plus or
// minus one interval. Every other term is ignored, which can only widen the
// range (each term of an AND narrows the result), so ignoring is always
// safe. A top-level OR, or a query shape where the WHERE does not belong to
// exactly one table, yields no range at all.

// timeBounds is what the scanner found.
type timeBounds struct {
	start, end *time.Time
	// exact: every top-level AND term that mentions the time column was read
	// as a bound, so the bounds are the query's whole time predicate.
	exact bool
	// proofSafe: every bound is a literal or now() plus or minus an
	// interval, and no end is inclusive: the shapes the empty-range proof
	// and the open-ended shortcut were built and tested for.
	proofSafe bool
}

var (
	// structuralWord marks shapes whose WHERE need not belong to one table.
	structuralWord = regexp.MustCompile(`(?i)\b(join|union|intersect|except|with)\b`)
	fromWord       = regexp.MustCompile(`(?i)\bfrom\b`)
	whereWord      = regexp.MustCompile(`(?i)\bwhere\b`)
	subSelect      = regexp.MustCompile(`(?i)\(\s*select\b`)
	// whereEndWord ends the WHERE clause when found at paren depth 0.
	whereEndWord = regexp.MustCompile(`(?i)^(group\s+by|order\s+by|limit|offset|having|qualify|window|fetch)\b`)
	// mentionsTime finds the time column as a whole word (not downtime,
	// time_bucket or "time" inside another identifier).
	mentionsTime = regexp.MustCompile(`(?i)(^|[^a-z0-9_])time($|[^a-z0-9_])`)

	// A bare bound term: the time column (optionally qualified, optionally
	// parenthesised) compared with one operand, in either order.
	timeCol    = `(?:[a-z_][a-z0-9_]*\s*\.\s*)?(?:\(\s*time\s*\)|time)`
	operand    = `(__STR_\d+__(?:\s*::\s*timestamp(?:tz)?)?|timestamp\s+__STR_\d+__|(?:now\s*\(\s*\)|current_timestamp)(?:\s*([+-])\s*interval\s*(?:(__STR_\d+__)|(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months)))?)`
	colOpRHS   = regexp.MustCompile(`(?i)^` + timeCol + `\s*(>=|<=|>|<)\s*` + operand + `$`)
	lhsOpCol   = regexp.MustCompile(`(?i)^` + operand + `\s*(>=|<=|>|<)\s*` + timeCol + `$`)
	betweenRHS = regexp.MustCompile(`(?i)^` + timeCol + `\s+between\s+` + operand + `\s+and\s+` + operand + `$`)

	quotedInterval = regexp.MustCompile(`(?i)^\s*(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months)\s*$`)
)

// scanTimeBounds reads the time bounds of sqlStr. ok is false when the query
// has no usable range: no WHERE, a shape the WHERE may not belong to one
// table in, or a top-level OR.
func scanTimeBounds(sqlStr string) (timeBounds, bool) {
	masked, masks := sql.MaskStringLiterals(sqlStr, sql.HasQuotes(sqlStr))
	masked, ok := stripMaskedComments(masked)
	if !ok {
		return timeBounds{}, false
	}
	// One trailing statement terminator is allowed by the query API.
	masked = strings.TrimRight(strings.TrimSpace(masked), ";")
	// A quoted identifier spelling the time column reads as the column.
	for ph, name := range sql.IdentifierNames(masks) {
		if strings.EqualFold(name, "time") {
			masked = strings.ReplaceAll(masked, ph, "time")
		}
	}
	masked = separateTokens(masked)
	// Every position below is taken in this one string: the FROM inside
	// EXTRACT(… FROM …) and similar is masked here, and changes its length.
	shape, _ := sql.MaskFromKeywordsInFunctionBodies(masked)
	// An aggregate's FILTER (WHERE …) filters that aggregate's input, not
	// the query's rows, so its WHERE is not a second WHERE (#1360). It is
	// blanked only to count and find the query's WHERE: the FROM count and
	// the JOIN/UNION/WITH/subquery checks still read the whole query, so a
	// FILTER body holding a subquery or another table keeps the query
	// refused. A range read from the outer WHERE would be applied to that
	// table too and drop its rows.
	wheres := blankAggregateFilters(shape)
	if len(fromWord.FindAllStringIndex(shape, -1)) != 1 || structuralWord.MatchString(shape) ||
		subSelect.MatchString(shape) || len(whereWord.FindAllStringIndex(wheres, -1)) != 1 {
		return timeBounds{}, false
	}
	f := fromWord.FindStringIndex(shape)
	loc := whereWord.FindStringIndex(wheres)
	// The WHERE must follow the FROM (never slice on the assumption that the
	// one WHERE left after blanking aggregate FILTERs is the query's), and a
	// comma join (FROM a, b) is a second table without a JOIN keyword.
	if f == nil || loc == nil || f[1] > loc[0] || hasTopLevelComma(shape[f[1]:loc[0]]) {
		return timeBounds{}, false
	}
	terms, ok := topLevelConjuncts(shape[loc[1]:])
	if !ok {
		return timeBounds{}, false
	}
	literals := literalValues(masks)
	b := timeBounds{exact: true, proofSafe: true}
	b.addTerms(terms, literals)
	if b.start == nil && b.end == nil {
		return timeBounds{}, false
	}
	return b, true
}

// aggregateFilter finds the FILTER keyword of an aggregate's filter clause:
// the whole word followed by "(", right after the ")" that closes the
// aggregate call (or its WITHIN GROUP (…)). DuckDB's filter(list, lambda)
// function never follows a ")".
var aggregateFilter = regexp.MustCompile(`(?i)\)\s*(filter)\s*\(`)

// blankAggregateFilters returns s with every aggregate FILTER clause, keyword
// and parenthesised body, replaced by spaces of the same length, so every
// index into s still lines up. Literals are already masked, so the body's
// parens balance; an unbalanced body is left as it is.
func blankAggregateFilters(s string) string {
	locs := aggregateFilter.FindAllStringSubmatchIndex(s, -1)
	if len(locs) == 0 {
		return s
	}
	b := []byte(s)
	for _, l := range locs {
		kw, open := l[2], l[1]-1 // the keyword's start, the "(" after it
		depth, end := 0, -1
		for i := open; i < len(b); i++ {
			if b[i] == '(' {
				depth++
			} else if b[i] == ')' {
				depth--
				if depth == 0 {
					end = i
					break
				}
			}
		}
		if end < 0 {
			continue
		}
		for i := kw; i <= end; i++ {
			b[i] = ' '
		}
	}
	return string(b)
}

// addTerms reads each AND term; a parenthesised group that is itself an
// AND of terms (and nothing else) is read term by term.
func (b *timeBounds) addTerms(terms []string, literals map[string]string) {
	for _, term := range terms {
		term = trimOuterParens(term)
		lo, hi, isBound, safe := readBoundTerm(term, literals)
		if !isBound {
			if inner, ok := topLevelConjuncts(term); ok && len(inner) > 1 {
				b.addTerms(inner, literals)
				continue
			}
			if mentionsTime.MatchString(term) {
				b.exact = false
			}
			continue
		}
		if !safe {
			b.proofSafe = false
		}
		if lo != nil && (b.start == nil || lo.After(*b.start)) {
			b.start = lo
		}
		if hi != nil && (b.end == nil || hi.Before(*b.end)) {
			b.end = hi
		}
	}
}

// readBoundTerm parses one top-level AND term as a bare time bound. The end
// it returns is exclusive: an inclusive end (<=, BETWEEN) is moved one
// microsecond (Arc's timestamp precision) past its literal, so the hour it
// falls on is read.
func readBoundTerm(term string, literals map[string]string) (lo, hi *time.Time, ok, proofSafe bool) {
	if m := betweenRHS.FindStringSubmatch(term); m != nil {
		a, okA := operandTime(m[1:6], literals)
		z, okZ := operandTime(m[6:11], literals)
		if !okA || !okZ {
			return nil, nil, false, false
		}
		z = z.Add(time.Microsecond)
		return &a, &z, true, false
	}
	var op string
	var opnd []string
	if m := colOpRHS.FindStringSubmatch(term); m != nil {
		op, opnd = m[1], m[2:7]
	} else if m := lhsOpCol.FindStringSubmatch(term); m != nil {
		opnd, op = m[1:6], flipOp(m[6])
	} else {
		return nil, nil, false, false
	}
	t, okT := operandTime(opnd, literals)
	if !okT {
		return nil, nil, false, false
	}
	safe := operandProofSafe(opnd)
	switch op {
	case ">=", ">":
		return &t, nil, true, safe
	case "<=":
		t = t.Add(time.Microsecond)
		return nil, &t, true, false
	default: // "<"
		return nil, &t, true, safe
	}
}

// operandProofSafe: a literal, or now() with an interval (not
// CURRENT_TIMESTAMP, not a bare now()).
func operandProofSafe(m []string) bool {
	whole := strings.ToLower(strings.TrimSpace(m[0]))
	if strings.HasPrefix(whole, "__str_") || strings.HasPrefix(whole, "timestamp") {
		return true
	}
	return strings.HasPrefix(whole, "now") && m[1] != ""
}

func flipOp(op string) string {
	switch op {
	case ">=":
		return "<="
	case ">":
		return "<"
	case "<=":
		return ">="
	default:
		return ">"
	}
}

// operandTime evaluates one operand capture: [whole, sign, quotedInterval,
// amount, unit].
func operandTime(m []string, literals map[string]string) (time.Time, bool) {
	whole := strings.TrimSpace(m[0])
	if strings.HasPrefix(whole, "__STR_") || strings.HasPrefix(strings.ToLower(whole), "timestamp") {
		ph := placeholderIn(whole)
		lit, ok := literals[ph]
		if !ok || utcOffsetSuffix.MatchString(lit) {
			// An explicit offset: DuckDB's TIMESTAMP cast drops it where this
			// parser applies it, so the two would disagree by the offset.
			return time.Time{}, false
		}
		t, err := parseDateTime(lit)
		return t, err == nil
	}
	// now() or current_timestamp, with at most one interval.
	if m[1] == "" {
		return time.Now().UTC(), true
	}
	amount, unit := m[3], m[4]
	if m[2] != "" {
		lit, ok := literals[m[2]]
		if !ok {
			return time.Time{}, false
		}
		q := quotedInterval.FindStringSubmatch(lit)
		if q == nil {
			return time.Time{}, false
		}
		amount, unit = q[1], q[2]
	}
	t, err := evaluateRelativeTime(amount, unit, m[1] == "+")
	return t, err == nil
}

var placeholderPattern = regexp.MustCompile(`__STR_\d+__`)

var (
	// numberThenLetter: DuckDB ends a numeric literal at the first letter
	// that cannot continue it, so "1OR" is the number 1 then OR.
	numberThenLetter = regexp.MustCompile(`(^|[^A-Za-z0-9_.])(\d+(?:\.\d+)?)([A-Za-df-zA-DF-Z_])`)
	anyPlaceholder   = regexp.MustCompile(`__(?:STR|IDENT)_\d+__`)
)

// separateTokens puts a space around each masked literal and between a
// number and a letter that follows it, so keyword boundaries are where
// DuckDB's lexer puts them ('y'OR and 1OR are both followed by an OR).
func separateTokens(s string) string {
	s = anyPlaceholder.ReplaceAllString(s, " $0 ")
	return numberThenLetter.ReplaceAllString(s, "$1$2 $3")
}

// utcOffsetSuffix is a +hh, +hhmm or +hh:mm (or minus) offset after a clock
// time (never the day of a date-only literal).
var utcOffsetSuffix = regexp.MustCompile(`\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?\s*[+-]\d{2}(?::?\d{2})?$`)

func placeholderIn(s string) string { return placeholderPattern.FindString(s) }

// literalValues maps each string-literal placeholder to the literal's text,
// quotes removed and the ” escape collapsed.
func literalValues(masks []sql.StringMask) map[string]string {
	out := make(map[string]string, len(masks))
	for _, m := range masks {
		if m.Identifier {
			continue
		}
		v := m.Original
		if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
			v = strings.ReplaceAll(v[1:len(v)-1], "''", "'")
		} else {
			continue // E'', dollar quotes and the like are not read as times
		}
		out[m.Placeholder] = v
	}
	return out
}

// stripMaskedComments removes -- and /* */ comments from masked SQL (string
// literals are placeholders, so every comment marker left is real). A nested
// or unterminated block comment is refused.
func stripMaskedComments(s string) (string, bool) {
	if !strings.Contains(s, "--") && !strings.Contains(s, "/*") {
		return s, true
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		switch {
		case strings.HasPrefix(s[i:], "--"):
			j := strings.IndexAny(s[i:], "\n\r") // as DuckDB, and sql.MaskStringLiterals
			if j < 0 {
				return b.String(), true
			}
			b.WriteByte(' ')
			i += j
		case strings.HasPrefix(s[i:], "/*"):
			j := strings.Index(s[i+2:], "*/")
			if j < 0 || strings.Contains(s[i+2:i+2+j], "/*") {
				return "", false
			}
			b.WriteByte(' ')
			i += 2 + j + 2
		default:
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String(), true
}

// nesting tracks how deep a scan is inside brackets and CASE … END, the
// constructs whose AND, OR and commas are not top-level.
type nesting struct{ depth int }

// step advances over s[i] (and returns how many bytes it consumed: a whole
// word, so a keyword is seen once). lower is s with ASCII letters lowered.
func (n *nesting) step(lower string, i int) int {
	switch c := lower[i]; {
	case c == '(' || c == '[' || c == '{':
		n.depth++
	case c == ')' || c == ']' || c == '}':
		n.depth--
	case isWordByte(c) && (i == 0 || !isWordByte(lower[i-1])):
		e := i
		for e < len(lower) && isWordByte(lower[e]) {
			e++
		}
		switch lower[i:e] {
		case "case":
			n.depth++
		case "end":
			n.depth--
		}
		return e - i
	}
	return 1
}

// asciiLower lowers ASCII letters only, keeping every byte where it was, so
// indexes into the result index the input (strings.ToLower may change the
// length of non-ASCII text).
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}

// wordAt reports the whole word w at lower[i].
func wordAt(lower string, i int, w string) bool {
	if !strings.HasPrefix(lower[i:], w) || (i > 0 && isWordByte(lower[i-1])) {
		return false
	}
	e := i + len(w)
	return e >= len(lower) || !isWordByte(lower[e])
}

// topLevelConjuncts splits a WHERE clause (starting just after the keyword)
// into its top-level AND terms, stopping at the first clause keyword at
// depth 0. The AND of a BETWEEN stays inside its term. ok is false for a
// top-level OR, or unbalanced nesting.
func topLevelConjuncts(clause string) ([]string, bool) {
	var terms []string
	lower := asciiLower(clause)
	var n nesting
	start, end := 0, len(clause)
	betweenOpen := false
	for i := 0; i < len(clause); {
		top := n.depth == 0
		if top && (i == 0 || !isWordByte(lower[i-1])) {
			switch {
			case whereEndWord.MatchString(lower[i:]):
				end = i
				goto done
			case wordAt(lower, i, "or"):
				return nil, false
			case wordAt(lower, i, "between"):
				betweenOpen = true
			case wordAt(lower, i, "and"):
				if betweenOpen {
					betweenOpen = false
				} else {
					terms = append(terms, strings.TrimSpace(clause[start:i]))
					start = i + len("and")
				}
			}
		}
		i += n.step(lower, i)
		if n.depth < 0 {
			return nil, false
		}
	}
done:
	if n.depth != 0 {
		return nil, false
	}
	terms = append(terms, strings.TrimSpace(clause[start:end]))
	return terms, true
}

// hasTopLevelComma reports a comma outside brackets and CASE … END.
func hasTopLevelComma(s string) bool {
	lower := asciiLower(s)
	var n nesting
	for i := 0; i < len(s); {
		if n.depth == 0 && s[i] == ',' {
			return true
		}
		i += n.step(lower, i)
	}
	return false
}

func isWordByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// trimOuterParens removes parentheses that enclose the whole term.
func trimOuterParens(s string) string {
	for {
		s = strings.TrimSpace(s)
		if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
			return s
		}
		depth := 0
		for i := 0; i < len(s); i++ {
			switch s[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 && i != len(s)-1 {
					return s // the first paren closes before the end: (a) op (b)
				}
			}
		}
		if depth != 0 {
			return s
		}
		s = s[1 : len(s)-1]
	}
}
