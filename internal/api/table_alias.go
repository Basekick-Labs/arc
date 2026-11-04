package api

import "strings"

// Table-name qualifiers (#1447).
//
// The storage-path rewrite replaces a table reference with a read_parquet()
// call or a subquery over one. DuckDB names an unaliased read_parquet()
// relation "read_parquet", so a column qualified by the table's own name, as
// Postgres users write it (`SELECT readings.value FROM readings`), failed to
// bind with "Referenced table readings not found". aliasTableRef gives the
// rewritten relation its table name back.
//
// It only does so when the query qualifies a column by that name somewhere
// and the reference has no alias of its own, so every other query is rewritten
// byte for byte as before.

// duckdbNonAliasKeywords holds DuckDB's reserved and type_function keywords:
// the words that cannot follow a table reference as an alias without AS. In
// the Postgres grammar DuckDB uses, a bare alias must be a ColId, which is an
// identifier or an unreserved or column_name keyword. Pinned against
// duckdb_keywords() by TestDuckDBNonAliasKeywordsMatchDuckDB.
var duckdbNonAliasKeywords = map[string]struct{}{
	"all":           {},
	"analyse":       {},
	"analyze":       {},
	"and":           {},
	"anti":          {},
	"any":           {},
	"array":         {},
	"as":            {},
	"asc":           {},
	"asof":          {},
	"asymmetric":    {},
	"at":            {},
	"authorization": {},
	"binary":        {},
	"both":          {},
	"by":            {},
	"case":          {},
	"cast":          {},
	"check":         {},
	"collate":       {},
	"collation":     {},
	"column":        {},
	"columns":       {},
	"concurrently":  {},
	"constraint":    {},
	"create":        {},
	"cross":         {},
	"default":       {},
	"deferrable":    {},
	"desc":          {},
	"describe":      {},
	"distinct":      {},
	"do":            {},
	"else":          {},
	"end":           {},
	"except":        {},
	"false":         {},
	"fetch":         {},
	"for":           {},
	"foreign":       {},
	"freeze":        {},
	"from":          {},
	"full":          {},
	"generated":     {},
	"glob":          {},
	"group":         {},
	"having":        {},
	"ilike":         {},
	"in":            {},
	"initially":     {},
	"inner":         {},
	"intersect":     {},
	"into":          {},
	"is":            {},
	"isnull":        {},
	"join":          {},
	"lambda":        {},
	"lateral":       {},
	"leading":       {},
	"left":          {},
	"like":          {},
	"limit":         {},
	"map":           {},
	"natural":       {},
	"not":           {},
	"notnull":       {},
	"null":          {},
	"offset":        {},
	"on":            {},
	"only":          {},
	"or":            {},
	"order":         {},
	"outer":         {},
	"overlaps":      {},
	"pivot":         {},
	"pivot_longer":  {},
	"pivot_wider":   {},
	"placing":       {},
	"positional":    {},
	"primary":       {},
	"qualify":       {},
	"references":    {},
	"returning":     {},
	"right":         {},
	"select":        {},
	"semi":          {},
	"show":          {},
	"similar":       {},
	"some":          {},
	"struct":        {},
	"summarize":     {},
	"symmetric":     {},
	"table":         {},
	"tablesample":   {},
	"then":          {},
	"to":            {},
	"trailing":      {},
	"true":          {},
	"try_cast":      {},
	"union":         {},
	"unique":        {},
	"unpack":        {},
	"unpivot":       {},
	"using":         {},
	"variadic":      {},
	"verbose":       {},
	"when":          {},
	"where":         {},
	"window":        {},
	"with":          {},
}

// qualifierNames returns the lower-cased names the query uses as a qualifier:
// an identifier followed by a dot and then a name or *, as in
// `readings.value` or `readings.*`. sql must be masked and comment-stripped
// and must not have been rewritten yet; emitted storage paths contain dots.
// resolve maps a quoted-identifier placeholder to its name.
func qualifierNames(sql string, resolve func(string) (string, bool)) map[string]bool {
	var names map[string]bool
	for i := 0; i < len(sql); {
		c := sql[i]
		if !isIdentChar(c) {
			i++
			continue
		}
		start := i
		for i < len(sql) && isIdentChar(sql[i]) {
			i++
		}
		// A number (1.5) can be collected too: no valid table name starts
		// with a digit, so it never matches one.
		j := skipSQLSpace(sql, i)
		if j >= len(sql) || sql[j] != '.' {
			continue
		}
		k := skipSQLSpace(sql, j+1)
		if k >= len(sql) || !(isIdentChar(sql[k]) || sql[k] == '*') {
			continue
		}
		name, ok := resolve(sql[start:i])
		if !ok {
			continue
		}
		if names == nil {
			names = make(map[string]bool)
		}
		names[strings.ToLower(name)] = true
	}
	return names
}

func skipSQLSpace(sql string, i int) int {
	for i < len(sql) && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r') {
		i++
	}
	return i
}

// keepsOwnName reports whether the table reference ending at end must not be
// given an alias: it already has one (`readings r`, `readings AS r`, a
// quoted alias), or it is followed by WITH ORDINALITY, which DuckDB accepts
// only before an alias.
func keepsOwnName(sql string, end int) bool {
	i := skipSQLSpace(sql, end)
	if i >= len(sql) || !isIdentChar(sql[i]) {
		return false
	}
	word := sqlWordAt(sql, i)
	lower := strings.ToLower(word)
	switch lower {
	case "as":
		return true
	case "with":
		next := skipSQLSpace(sql, i+len(word))
		return strings.EqualFold(sqlWordAt(sql, next), "ordinality")
	}
	_, keyword := duckdbNonAliasKeywords[lower]
	return !keyword
}

func sqlWordAt(sql string, i int) string {
	j := i
	for j < len(sql) && isIdentChar(sql[j]) {
		j++
	}
	return sql[i:j]
}

// aliasTableRef appends ` AS "<name>"` to expr, the rewritten form of the table
// reference that ends at end in sql, when the query qualifies by name and the
// reference keeps no name of its own. ok is false for a name the identifier
// resolver rejected; such a reference is never given an alias. It must wrap
// the finished expression: the open-compaction exclusion recognises an
// expression by its closing parenthesis.
func aliasTableRef(expr, sql string, end int, name string, ok bool, qualified map[string]bool) string {
	if expr == "" || !ok || !qualified[strings.ToLower(name)] {
		return expr
	}
	if containsMaskPlaceholder(name) {
		return expr
	}
	if keepsOwnName(sql, end) {
		return expr
	}
	return expr + ` AS "` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
