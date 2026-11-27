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
// It does so when the query qualifies a column by that name somewhere and the
// reference has no alias of its own. The storage-path rewrite also names a
// reference after its table when its FROM scope reads another rewritten
// table, which would otherwise leave both "read_parquet" (#1483; decided at
// expansion, see deferredRefs.expand). A query reading one table per scope is
// rewritten byte for byte as before.

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

// qualifierNames returns the lower-cased names the query uses as a qualifier,
// an identifier followed by a dot and then a name or * as in `readings.value`
// or `readings.*`, and does not also use as an alias. sql must be masked and
// comment-stripped and must not have been rewritten yet; emitted storage paths
// contain dots. resolve maps a quoted-identifier placeholder to its name.
//
// A name the query also gives another relation, as in
// `FROM cpu JOIN (SELECT ...) cpu USING (host)`, is left out: naming the table
// too would be a duplicate alias, and in a nested scope it would change which
// relation `cpu.x` reads. Such a table is rewritten as it was before #1447.
func qualifierNames(sql string, resolve func(string) (string, bool)) map[string]bool {
	names, _ := qualifierAndAliasNames(sql, resolve)
	return names
}

// qualifierAndAliasNames returns qualifierNames' result and the lower-cased
// names the query gives a relation as its alias, which the #1483 naming must
// not reuse.
func qualifierAndAliasNames(sql string, resolve func(string) (string, bool)) (map[string]bool, map[string]bool) {
	var names, aliases map[string]bool
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
		qualifier := j < len(sql) && sql[j] == '.'
		if qualifier {
			k := skipSQLSpace(sql, j+1)
			qualifier = k < len(sql) && (isIdentChar(sql[k]) || sql[k] == '*')
		}
		if qualifier && startsRelationRef(sql, start) {
			// `FROM db.readings`: the database part of a table reference,
			// not a column qualifier.
			continue
		}
		if !qualifier && !inAliasPosition(sql, start) {
			continue
		}
		name, ok := resolve(sql[start:i])
		if !ok {
			continue
		}
		set := &names
		if !qualifier {
			set = &aliases
		}
		if *set == nil {
			*set = make(map[string]bool)
		}
		(*set)[strings.ToLower(name)] = true
	}
	for name := range aliases {
		delete(names, name)
	}
	return names, aliases
}

// bareNameCounts counts, by lower-cased name, the words in sql that are
// neither a qualifier (`cpu.`) nor qualified (`.cpu`): a table written bare
// as a reference, a column, a SELECT-list alias, a function. sql is masked
// and comment-stripped and not yet rewritten; resolve maps a quoted
// identifier's placeholder to its name, since DuckDB binds `"disk"` as it
// binds `disk`.
//
// The #1483 naming compares this with the bare references it rewrote: a
// name the query also uses for anything else is never given to a table.
// DuckDB binds a table alias before a SELECT-list alias in WHERE and GROUP
// BY, and before an outer query's column, so `SELECT x AS mem … GROUP BY
// mem` or a correlated `disk` would start reading the table's whole row.
func bareNameCounts(sql string, resolve func(string) (string, bool)) map[string]int {
	var counts map[string]int
	for i := 0; i < len(sql); {
		if !isIdentChar(sql[i]) {
			i++
			continue
		}
		start := i
		for i < len(sql) && isIdentChar(sql[i]) {
			i++
		}
		if sql[start] >= '0' && sql[start] <= '9' {
			continue
		}
		if j := skipSQLSpace(sql, i); j < len(sql) && sql[j] == '.' {
			continue
		}
		k := start - 1
		for k >= 0 && (sql[k] == ' ' || sql[k] == '\t' || sql[k] == '\n' || sql[k] == '\r') {
			k--
		}
		if k >= 0 && sql[k] == '.' {
			continue
		}
		name, ok := resolve(sql[start:i])
		if !ok {
			continue
		}
		if counts == nil {
			counts = make(map[string]int)
		}
		counts[strings.ToLower(name)]++
	}
	return counts
}

// inAliasPosition reports whether the word starting at start is a relation's
// alias: it follows `)`, AS, or a word that is not a keyword, as in
// `(...) cpu`, `(...) AS cpu`, `limits cpu` or `"limits" cpu`, and it sits in
// a FROM or JOIN list (inRelationList). A word in the select list, a star
// modifier (`* EXCLUDE cpu`), `DISTINCT ON (...) cpu` or a function body is a
// column, however it is preceded.
func inAliasPosition(sql string, start int) bool {
	i := start - 1
	for i >= 0 && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r') {
		i--
	}
	if i < 0 {
		return false
	}
	if sql[i] != ')' {
		if !isIdentChar(sql[i]) {
			return false
		}
		end := i + 1
		for i >= 0 && isIdentChar(sql[i]) {
			i--
		}
		// After a dotted name (`db.limits cpu`) the word before is the
		// table's, whatever it is called.
		if i < 0 || sql[i] != '.' {
			prev := strings.ToLower(sql[i+1 : end])
			if _, keyword := duckdbNonAliasKeywords[prev]; keyword && prev != "as" {
				return false
			}
		}
	}
	return inRelationList(sql, start)
}

// clauseStartWords are the keywords that start a clause other than FROM: a
// word after one of them, at the same depth, is not in a FROM list. Other
// keywords (join types, ON and USING, the words of a join condition) can sit
// between FROM and a later relation's alias, as in
// `FROM a JOIN b ON true, (...) cpu`.
var clauseStartWords = map[string]bool{
	"select": true, "where": true, "group": true, "having": true,
	"qualify": true, "window": true, "order": true, "limit": true,
	"offset": true, "union": true, "intersect": true, "except": true,
	"returning": true, "with": true, "values": true, "set": true, "into": true,
}

// inRelationList reports whether the word starting at start is in a FROM or
// JOIN list: walking back at the same parenthesis depth, FROM or JOIN comes
// before a keyword that starts another clause (clauseStartWords) and before
// the parenthesis that opens the enclosing group. Other words, dots, commas
// and whole parenthesised groups are passed over.
func inRelationList(sql string, start int) bool {
	depth := 0
	for i := start - 1; i >= 0; {
		c := sql[i]
		switch {
		case c == ')':
			depth++
			i--
		case c == '(':
			if depth == 0 {
				return false
			}
			depth--
			i--
		case c == ';':
			if depth == 0 {
				return false
			}
			i--
		case isIdentChar(c):
			end := i + 1
			for i >= 0 && isIdentChar(sql[i]) {
				i--
			}
			// Inside a group, or after a dot (`db.group`), a word is a name.
			if depth > 0 || (i >= 0 && sql[i] == '.') {
				continue
			}
			word := strings.ToLower(sql[i+1 : end])
			if word == "from" || word == "join" {
				return true
			}
			if clauseStartWords[word] {
				return false
			}
		default:
			i--
		}
	}
	return false
}

// startsRelationRef reports whether the word starting at start begins a
// table reference: it directly follows FROM, JOIN, LATERAL or ONLY, or a
// comma in a FROM list.
func startsRelationRef(sql string, start int) bool {
	i := start - 1
	for i >= 0 && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r') {
		i--
	}
	if i < 0 {
		return false
	}
	if sql[i] == ',' {
		return inRelationList(sql, i)
	}
	end := i + 1
	for i >= 0 && isIdentChar(sql[i]) {
		i--
	}
	switch strings.ToLower(sql[i+1 : end]) {
	case "from", "join", "lateral", "only":
		return true
	}
	return false
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
