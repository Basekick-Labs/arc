package api

import (
	"strings"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
)

// unqualifyMeasurement drops the measurement-name qualifier from a delete's
// WHERE clause (#1451): `cpu.host = 'a'` on measurement cpu becomes
// `host = 'a'`, as does `"cpu".host`. The caller supplies only the predicate,
// so the measurement is the one relation it can mean.
//
// The statements a delete runs read the measurement's files with no alias,
// and they keep it that way. An alias would make the bare measurement name
// bind to the whole row in any file without a column of that name, and a
// measurement often has a field named like itself:
// `temperature IS NOT NULL` would then match every row of such a file.
// Rewriting the clause instead leaves those statements, the delete records a
// cluster stores and replays, and peers on older versions all seeing an
// unqualified clause.
//
// Only a qualifier is touched: a name directly followed by a dot and another
// name. A name preceded by a dot (`x.cpu.host`, a deeper path) and the bare
// name are left as written, and string literals are masked first. Matching is
// case-insensitive, as DuckDB's identifiers are.
func unqualifyMeasurement(where, measurement string) string {
	if measurement == "" || !strings.Contains(where, ".") {
		return where
	}
	masked, masks := sqlutil.MaskStringLiterals(where, sqlutil.HasQuotes(where))
	identNames := sqlutil.IdentifierNames(masks)

	var b strings.Builder
	last := 0
	for i := 0; i < len(masked); {
		if !isIdentChar(masked[i]) {
			i++
			continue
		}
		start := i
		for i < len(masked) && isIdentChar(masked[i]) {
			i++
		}
		name := masked[start:i]
		if orig, ok := identNames[name]; ok {
			name = orig
		}
		if !strings.EqualFold(name, measurement) || precededByDot(masked, start) {
			continue
		}
		dot := skipSQLSpace(masked, i)
		if dot >= len(masked) || masked[dot] != '.' {
			continue
		}
		next := skipSQLSpace(masked, dot+1)
		if next >= len(masked) || !isIdentChar(masked[next]) {
			continue
		}
		b.WriteString(masked[last:start])
		last = next
		i = next
	}
	if last == 0 {
		return where
	}
	b.WriteString(masked[last:])
	return sqlutil.UnmaskStringLiterals(b.String(), masks)
}

// precededByDot reports whether the token starting at i follows a dot, with
// optional blanks between.
func precededByDot(s string, i int) bool {
	for i > 0 {
		i--
		switch s[i] {
		case ' ', '\t', '\n', '\r':
			continue
		case '.':
			return true
		default:
			return false
		}
	}
	return false
}
