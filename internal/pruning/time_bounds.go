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
	// A quoted identifier spelling the time column reads as the column.
	for ph, name := range sql.IdentifierNames(masks) {
		if strings.EqualFold(name, "time") {
			masked = strings.ReplaceAll(masked, ph, "time")
		}
	}
	shape, _ := sql.MaskFromKeywordsInFunctionBodies(masked)
	if len(fromWord.FindAllStringIndex(shape, -1)) != 1 || structuralWord.MatchString(shape) ||
		subSelect.MatchString(shape) || len(whereWord.FindAllStringIndex(shape, -1)) != 1 {
		return timeBounds{}, false
	}
	loc := whereWord.FindStringIndex(masked)
	if loc == nil {
		return timeBounds{}, false
	}
	// A comma join (FROM a, b) is a second table without a JOIN keyword.
	if f := fromWord.FindStringIndex(shape); f == nil || f[1] > loc[0] || hasTopLevelComma(shape[f[1]:loc[0]]) {
		return timeBounds{}, false
	}
	terms, ok := topLevelConjuncts(masked[loc[1]:])
	if !ok {
		return timeBounds{}, false
	}
	literals := literalValues(masks)
	b := timeBounds{exact: true}
	b.addTerms(terms, literals)
	if b.start == nil && b.end == nil {
		return timeBounds{}, false
	}
	return b, true
}

// addTerms reads each AND term; a parenthesised group that is itself an
// AND of terms (and nothing else) is read term by term.
func (b *timeBounds) addTerms(terms []string, literals map[string]string) {
	for _, term := range terms {
		term = trimOuterParens(term)
		lo, hi, isBound := readBoundTerm(term, literals)
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
func readBoundTerm(term string, literals map[string]string) (lo, hi *time.Time, ok bool) {
	if m := betweenRHS.FindStringSubmatch(term); m != nil {
		a, okA := operandTime(m[1:6], literals)
		z, okZ := operandTime(m[6:11], literals)
		if !okA || !okZ {
			return nil, nil, false
		}
		z = z.Add(time.Microsecond)
		return &a, &z, true
	}
	var op string
	var opnd []string
	if m := colOpRHS.FindStringSubmatch(term); m != nil {
		op, opnd = m[1], m[2:7]
	} else if m := lhsOpCol.FindStringSubmatch(term); m != nil {
		opnd, op = m[1:6], flipOp(m[6])
	} else {
		return nil, nil, false
	}
	t, okT := operandTime(opnd, literals)
	if !okT {
		return nil, nil, false
	}
	switch op {
	case ">=", ">":
		return &t, nil, true
	case "<=":
		t = t.Add(time.Microsecond)
		return nil, &t, true
	default: // "<"
		return nil, &t, true
	}
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
		if !ok {
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
			j := strings.IndexByte(s[i:], '\n')
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

// topLevelConjuncts splits a WHERE clause (starting just after the keyword)
// into its top-level AND terms, stopping at the first clause keyword at
// paren depth 0. The AND of a BETWEEN stays inside its term. ok is false for
// a top-level OR, or unbalanced parentheses.
func topLevelConjuncts(clause string) ([]string, bool) {
	var terms []string
	depth, start := 0, 0
	betweenOpen := false
	lower := strings.ToLower(clause)
	isWordAt := func(i int, w string) bool {
		if !strings.HasPrefix(lower[i:], w) {
			return false
		}
		if i > 0 && isWordByte(lower[i-1]) {
			return false
		}
		e := i + len(w)
		return e >= len(lower) || !isWordByte(lower[e])
	}
	end := len(clause)
	for i := 0; i < len(clause); i++ {
		c := clause[i]
		switch {
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth < 0 {
				end = i // the WHERE of an enclosing parenthesis ends here
				goto done
			}
		case depth == 0 && (i == 0 || !isWordByte(lower[i-1])) && whereEndWord.MatchString(lower[i:]):
			end = i
			goto done
		case depth == 0 && isWordAt(i, "or"):
			return nil, false
		case depth == 0 && isWordAt(i, "between"):
			betweenOpen = true
		case depth == 0 && isWordAt(i, "and"):
			if betweenOpen {
				betweenOpen = false
				continue
			}
			terms = append(terms, strings.TrimSpace(clause[start:i]))
			start = i + len("and")
		}
	}
done:
	if depth > 0 {
		return nil, false
	}
	terms = append(terms, strings.TrimSpace(clause[start:end]))
	return terms, true
}

// hasTopLevelComma reports a comma outside parentheses.
func hasTopLevelComma(s string) bool {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				return true
			}
		}
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
		s = s[1 : len(s)-1]
	}
}
