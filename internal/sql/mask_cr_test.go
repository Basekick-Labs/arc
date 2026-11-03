package sql

import (
	"strings"
	"testing"
)

// A line comment ends at a carriage return as well as a newline, as in
// DuckDB, in every scanner that skips comments.
func TestLineCommentEndsAtCarriageReturn(t *testing.T) {
	q := "EXTRACT --a\r(YEAR FROM t)"
	if got := findKeywordFunctionCall(q, "EXTRACT"); got != 0 {
		t.Errorf("findKeywordFunctionCall(%q) = %d, want 0", q, got)
	}
	q = "x --a\rEXTRACT (YEAR FROM t)"
	if !isFromKeywordFuncBeforeParen(q, strings.Index(q, "(")) {
		t.Errorf("isFromKeywordFuncBeforeParen(%q) = false: the comment ended at the carriage return", q)
	}
	masked, _ := MaskStringLiterals("SELECT 1 --x\r, 'a;b' AS s", true)
	if strings.Contains(masked, "a;b") {
		t.Errorf("the literal after a carriage-return-ended comment was not masked: %q", masked)
	}
}
