package sql

import (
	"strings"
	"testing"
)

// maskedFroms returns how many FROM keywords MaskFromKeywordsInFunctionBodies
// masked in sql, after checking that unmasking restores sql byte for byte.
func maskedFroms(t *testing.T, sql string) int {
	t.Helper()
	out, masks := MaskFromKeywordsInFunctionBodies(sql)
	if got := UnmaskFromKeywordsInFunctionBodies(out, masks); got != sql {
		t.Fatalf("unmask(mask(%q)) = %q", sql, got)
	}
	// The predicate gates the fast path, so it must never miss a FROM the
	// masker hides. It may over-report (a function name inside a comment):
	// that only sends the query down the slower path.
	if len(masks) > 0 && !ContainsMaskableFromKeyword(sql) {
		t.Errorf("ContainsMaskableFromKeyword(%q) = false, but the masker masked %d", sql, len(masks))
	}
	return len(masks)
}

// TestDistinctFromOperatorIsMasked (#1472): the FROM of `IS [NOT] DISTINCT
// FROM` is a comparison operator, not a table introducer.
func TestDistinctFromOperatorIsMasked(t *testing.T) {
	cases := []struct {
		name, sql string
		want      int
	}{
		{"literal operand", "SELECT host FROM cpu WHERE host IS DISTINCT FROM __STR_0__", 1},
		{"NOT form", "SELECT host FROM cpu WHERE host IS NOT DISTINCT FROM __STR_0__", 1},
		{"column operand", "SELECT host FROM cpu WHERE host IS DISTINCT FROM note", 1},
		{"NULL operand", "SELECT host FROM cpu WHERE host IS DISTINCT FROM NULL", 1},
		{"projection", "SELECT __STR_0__ IS DISTINCT FROM __STR_1__ AS d FROM cpu", 1},
		{"mixed case", "select a Is NoT dIsTiNcT fRoM b from cpu", 1},
		{"newlines and tabs", "SELECT a IS\n\tNOT\r\nDISTINCT\tFROM b FROM cpu", 1},
		{"comments between the words", "SELECT a IS /* x */ NOT -- y\nDISTINCT /* z */ FROM b FROM cpu", 1},
		{"inside parentheses", "SELECT (a IS DISTINCT FROM b) FROM cpu", 1},
		{"two operators", "SELECT a IS DISTINCT FROM b, c IS NOT DISTINCT FROM d FROM cpu", 2},
		// The operand's own subquery keeps its FROM: only the operator's is masked.
		{"subquery operand", "SELECT * FROM cpu WHERE a IS DISTINCT FROM (SELECT b FROM mem)", 1},
		{"with a function form", "SELECT extract(year FROM t) IS DISTINCT FROM 2024 FROM cpu", 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := maskedFroms(t, tc.sql); got != tc.want {
				t.Errorf("masked %d FROMs in %q, want %d", got, tc.sql, tc.want)
			}
		})
	}
}

// TestFromThatIntroducesARelationIsNotMasked: every FROM here is one DuckDB
// reads a relation from, or might, so masking it would hide it from the
// replacement-scan guard and the RBAC extractor.
func TestFromThatIntroducesARelationIsNotMasked(t *testing.T) {
	cases := []struct{ name, sql string }{
		// FROM-first syntax after a set operation reads the relation.
		{"UNION DISTINCT", "SELECT 1 UNION DISTINCT FROM __STR_0__"},
		{"INTERSECT DISTINCT", "SELECT 1 INTERSECT DISTINCT FROM __STR_0__"},
		{"EXCEPT DISTINCT", "SELECT 1 EXCEPT DISTINCT FROM __STR_0__"},
		{"SELECT DISTINCT", "SELECT DISTINCT FROM __STR_0__"},
		{"NOT DISTINCT without IS", "SELECT 1 UNION NOT DISTINCT FROM t"},
		// A quoted "is" is a masked identifier, not the keyword.
		{"quoted is", "SELECT 1 AS __IDENT_0__ DISTINCT FROM __STR_0__"},
		{"qualified is", "SELECT t.is DISTINCT FROM __STR_0__"},
		// Words inside a comment are comment.
		{"operator words in a line comment", "SELECT * -- a IS DISTINCT\nFROM __STR_0__"},
		{"operator words in a block comment", "SELECT * /* a IS DISTINCT */ FROM __STR_0__"},
		// A comment must not open a function masking frame (#1472 review B1).
		{"comment opens a trim frame", "SELECT * /* trim( */ FROM __STR_0__ /* ) */"},
		{"line comment opens an extract frame", "SELECT * -- extract(\nFROM __STR_0__ -- )\n"},
		// Only ASCII whitespace separates the words: DuckDB reads
		// `is<U+0085>distinct` as one identifier (#1472 review B2).
		{"U+0085 between IS and DISTINCT", "SELECT __STR_0__ is\u0085distinct FROM __STR_1__"},
		{"U+2028 between DISTINCT and FROM", "SELECT a IS distinct FROM __STR_0__"},
		{"identifier ending in is", "SELECT this DISTINCT FROM t"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := maskedFroms(t, tc.sql); got != 0 {
				t.Errorf("masked %d FROMs in %q, want 0", got, tc.sql)
			}
		})
	}
}

// TestOperatorThenSetOperation: the operator's FROM is masked and the set
// operation's FROM after it is not.
func TestOperatorThenSetOperation(t *testing.T) {
	sql := "SELECT __STR_0__ IS DISTINCT FROM __STR_1__ UNION DISTINCT FROM __STR_2__"
	out, masks := MaskFromKeywordsInFunctionBodies(sql)
	if len(masks) != 1 {
		t.Fatalf("masked %d FROMs, want 1: %q", len(masks), out)
	}
	if !strings.Contains(out, "UNION DISTINCT FROM __STR_2__") {
		t.Errorf("the set operation's FROM was masked: %q", out)
	}
}

// TestCommentedFunctionFormStillMasked: skipping comments in the main loop
// must not lose a comment between a function name and its paren, which
// isFromKeywordFuncBeforeParen already handles.
func TestCommentedFunctionFormStillMasked(t *testing.T) {
	if got := maskedFroms(t, "SELECT EXTRACT /* c */ (YEAR FROM t) FROM cpu"); got != 1 {
		t.Errorf("masked %d, want 1", got)
	}
}
