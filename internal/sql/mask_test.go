package sql

import (
	"strings"
	"testing"
)

func TestMaskFromKeywordsInFunctionBodies(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantMasks int
		// After mask runs, the table-rewriter regex should NOT see "FROM x"
		// where x is the inner column, but SHOULD still see the outer FROM.
		mustContainOuterFROM     []string
		mustNotContainInnerFROM  []string
	}{
		{
			name:                    "EXTRACT(YEAR FROM time) FROM table",
			input:                   "SELECT EXTRACT(YEAR FROM time) FROM cpu",
			wantMasks:               1,
			mustContainOuterFROM:    []string{"FROM cpu"},
			mustNotContainInnerFROM: []string{"FROM time"},
		},
		{
			name:                    "SUBSTRING(s FROM 1 FOR 3)",
			input:                   "SELECT SUBSTRING(s FROM 1 FOR 3) FROM t",
			wantMasks:               1,
			mustContainOuterFROM:    []string{"FROM t"},
			mustNotContainInnerFROM: []string{"FROM 1"},
		},
		{
			name:                    "TRIM(LEADING '0' FROM x)",
			input:                   "SELECT TRIM(LEADING '0' FROM x) FROM t",
			wantMasks:               1,
			mustContainOuterFROM:    []string{"FROM t"},
			mustNotContainInnerFROM: []string{"FROM x"},
		},
		{
			name:                    "OVERLAY(s PLACING 'x' FROM 2)",
			input:                   "SELECT OVERLAY(s PLACING 'x' FROM 2) FROM t",
			wantMasks:               1,
			mustContainOuterFROM:    []string{"FROM t"},
			mustNotContainInnerFROM: []string{"FROM 2"},
		},
		{
			name:                    "nested EXTRACT + CAST",
			input:                   "SELECT EXTRACT(YEAR FROM CAST(t AS DATE)) FROM trips",
			wantMasks:               1,
			mustContainOuterFROM:    []string{"FROM trips"},
			mustNotContainInnerFROM: []string{"FROM CAST"},
		},
		{
			name:                    "lowercase extract",
			input:                   "SELECT extract(year from time) FROM cpu",
			wantMasks:               1,
			mustContainOuterFROM:    []string{"FROM cpu"},
			mustNotContainInnerFROM: []string{"from time"},
		},
		{
			name:      "no trigger function",
			input:     "SELECT a, b FROM cpu WHERE x = 1",
			wantMasks: 0,
		},
		{
			name:                 "user column literally named extract",
			input:                "SELECT extract FROM cpu",
			wantMasks:            0,
			mustContainOuterFROM: []string{"extract FROM cpu"},
		},
		{
			name:      "MY_EXTRACT user function with bare FROM is not the builtin",
			input:     "SELECT MY_EXTRACT(YEAR FROM x) FROM cpu",
			wantMasks: 0,
		},
		{
			name:                    "two EXTRACT calls",
			input:                   "SELECT EXTRACT(YEAR FROM time), EXTRACT(MONTH FROM time) FROM cpu",
			wantMasks:               2,
			mustContainOuterFROM:    []string{"FROM cpu"},
			mustNotContainInnerFROM: []string{"FROM time"},
		},
		{
			// Correctness review finding: ContainsFromKeywordFunction
			// used strings.Index which returns only the first match. A
			// column literally named extract_col next to a real EXTRACT
			// call would hide the real call. Verify both detection AND
			// mask work here.
			name:                    "column prefixed extract_col next to real EXTRACT call",
			input:                   "SELECT extract_col FROM t WHERE EXTRACT(YEAR FROM time) = 2025",
			wantMasks:               1,
			mustContainOuterFROM:    []string{"FROM t"},
			mustNotContainInnerFROM: []string{"FROM time"},
		},
		{
			// Correctness review finding: EXTRACT appearing AFTER the
			// outer FROM/JOIN is the offset-shift corruption scenario
			// for offset-based unmask. The placeholder approach must
			// roundtrip cleanly here.
			name:                    "EXTRACT in WHERE after outer FROM",
			input:                   "SELECT a FROM cpu WHERE EXTRACT(YEAR FROM ts) = 2024",
			wantMasks:               1,
			mustContainOuterFROM:    []string{"FROM cpu"},
			mustNotContainInnerFROM: []string{"FROM ts"},
		},
		{
			// Gemini finding: comment between function name and paren
			// must not defeat the scanner. Verified to reproduce the
			// original Binder Error against the live clickbench dataset
			// before this fix.
			name:                    "block comment between EXTRACT and paren",
			input:                   "SELECT EXTRACT /* c */ (YEAR FROM ts) FROM cpu",
			wantMasks:               1,
			mustContainOuterFROM:    []string{"FROM cpu"},
			mustNotContainInnerFROM: []string{"FROM ts"},
		},
		{
			name:                    "block comment with newlines and trailing whitespace",
			input:                   "SELECT EXTRACT\n\t/* multi-\n   line */ \t (YEAR FROM ts) FROM cpu",
			wantMasks:               1,
			mustContainOuterFROM:    []string{"FROM cpu"},
			mustNotContainInnerFROM: []string{"FROM ts"},
		},
		{
			name:                    "line comment between SUBSTRING and paren",
			input:                   "SELECT SUBSTRING -- inline comment\n(s FROM 1 FOR 3) FROM t",
			wantMasks:               1,
			mustContainOuterFROM:    []string{"FROM t"},
			mustNotContainInnerFROM: []string{"FROM 1"},
		},
		{
			// Gemini finding: backtick-quoted identifier is a column,
			// not the EXTRACT builtin. DuckDB itself rejects backticks
			// today, but the defensive check costs nothing.
			name:      "backticked EXTRACT identifier is a column, not the builtin",
			input:     "SELECT `EXTRACT`(a, b) FROM t",
			wantMasks: 0,
		},
		{
			// Gemini finding (round 2): subquery inside the trigger's
			// argument list — the inner FROM must NOT be masked, or
			// the regex rewriter cannot convert `cpu` to read_parquet.
			// Verified to reproduce a "Table with name cpu does not
			// exist" Catalog Error against the live clickbench dataset
			// before this fix.
			name:                    "subquery FROM inside EXTRACT must not be masked",
			input:                   "SELECT EXTRACT(YEAR FROM (SELECT MIN(ts) FROM cpu)) FROM events",
			wantMasks:               1,
			mustContainOuterFROM:    []string{"FROM cpu", "FROM events"},
			mustNotContainInnerFROM: []string{"FROM (SELECT MIN(ts)"},
		},
		{
			name:                    "deeply nested subquery — both inner FROMs survive",
			input:                   "SELECT EXTRACT(YEAR FROM (SELECT a FROM (SELECT b FROM t))) FROM o",
			wantMasks:               1,
			mustContainOuterFROM:    []string{"FROM t", "FROM o", "FROM (SELECT b"},
			mustNotContainInnerFROM: []string{"FROM (SELECT a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			masked, masks := MaskFromKeywordsInFunctionBodies(tt.input)
			if len(masks) != tt.wantMasks {
				t.Errorf("masks count = %d, want %d (masked=%q)", len(masks), tt.wantMasks, masked)
			}
			for _, substr := range tt.mustContainOuterFROM {
				if !strings.Contains(masked, substr) {
					t.Errorf("masked SQL missing outer FROM substring %q.\nInput:  %s\nMasked: %s",
						substr, tt.input, masked)
				}
			}
			for _, substr := range tt.mustNotContainInnerFROM {
				if strings.Contains(masked, substr) {
					t.Errorf("masked SQL still contains inner FROM substring %q.\nInput:  %s\nMasked: %s",
						substr, tt.input, masked)
				}
			}

			restored := UnmaskFromKeywordsInFunctionBodies(masked, masks)
			if restored != tt.input {
				t.Errorf("roundtrip mismatch.\nGot:  %q\nWant: %q", restored, tt.input)
			}
		})
	}
}

// TestUnmaskAfterLengthChangingRewrite is the critical regression test for
// the offset-shift corruption bug found in post-implementation review. The
// caller masks, then runs a length-changing transformation (simulating
// patternSimpleTable rewriting `FROM cpu` → `FROM read_parquet('long/path')`),
// then unmasks. With offset-based unmask this corrupts the rewrite output;
// with placeholder-based unmask it must roundtrip cleanly.
func TestUnmaskAfterLengthChangingRewrite(t *testing.T) {
	input := "SELECT a FROM cpu WHERE EXTRACT(YEAR FROM time) = 2024"
	masked, masks := MaskFromKeywordsInFunctionBodies(input)
	if len(masks) != 1 {
		t.Fatalf("expected 1 mask, got %d (masked=%q)", len(masks), masked)
	}

	// Simulate the table rewriter expanding `FROM cpu` into a much longer
	// read_parquet call. This shifts every byte after the rewrite to the
	// right, which is exactly the failure mode the placeholder approach
	// must survive.
	rewritten := strings.Replace(masked,
		"FROM cpu",
		"FROM read_parquet('./data/default/cpu/**/*.parquet', union_by_name=true)",
		1)

	restored := UnmaskFromKeywordsInFunctionBodies(rewritten, masks)

	if !strings.Contains(restored, "EXTRACT(YEAR FROM time)") {
		t.Errorf("inner EXTRACT was not restored.\nGot: %s", restored)
	}
	if !strings.Contains(restored, "read_parquet('./data/default/cpu/**/*.parquet'") {
		t.Errorf("outer rewrite was corrupted.\nGot: %s", restored)
	}
	if strings.Contains(restored, "__FROM_MASK_") {
		t.Errorf("placeholder leaked into restored SQL.\nGot: %s", restored)
	}
	// Specifically: corruption signature from the offset-based version
	// was bytes like "deFROMt" or "cpFROM" landing inside the path.
	if strings.Contains(restored, "deFROMt") || strings.Contains(restored, "cpFROM") {
		t.Errorf("offset-based corruption detected.\nGot: %s", restored)
	}
}

func TestContainsFromKeywordFunction(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"SELECT EXTRACT(YEAR FROM time) FROM cpu", true},
		{"SELECT extract(year from time) FROM cpu", true},
		{"SELECT EXTRACT  (YEAR FROM time) FROM cpu", true}, // whitespace before paren
		{"SELECT * FROM cpu", false},
		{"SELECT extract FROM cpu", false}, // bare identifier, no `(`
		{"SELECT MY_EXTRACT(x) FROM cpu", false},
		{"SELECT extracted FROM cpu", false},   // prefix collision
		{"SELECT decomposition FROM x", false}, // substring containing position
		// Regression: first-match-only bug would skip the real EXTRACT
		// after a leading prefix collision.
		{"SELECT extract_col FROM t WHERE EXTRACT(YEAR FROM time) = 2025", true},
		{"SELECT SUBSTRING(s FROM 1 FOR 3) FROM t", true},
		{"SELECT TRIM(LEADING '0' FROM x) FROM t", true},
		{"SELECT OVERLAY(s PLACING 'x' FROM 2) FROM t", true},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ContainsFromKeywordFunction(tt.input); got != tt.want {
				t.Errorf("ContainsFromKeywordFunction(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func BenchmarkContainsFromKeywordFunction_Miss(b *testing.B) {
	sql := "SELECT a, b, c, d, e FROM cpu WHERE x = 1 AND y > 100 GROUP BY z ORDER BY a LIMIT 100"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ContainsFromKeywordFunction(sql)
	}
}

func BenchmarkContainsFromKeywordFunction_Hit(b *testing.B) {
	sql := "SELECT EXTRACT(YEAR FROM time), col FROM cpu WHERE x = 1 GROUP BY col ORDER BY a LIMIT 100"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ContainsFromKeywordFunction(sql)
	}
}

func BenchmarkMaskFromKeywordsInFunctionBodies_Miss(b *testing.B) {
	sql := "SELECT a, b, c, d, e FROM cpu WHERE x = 1 AND y > 100 GROUP BY z ORDER BY a LIMIT 100"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		MaskFromKeywordsInFunctionBodies(sql)
	}
}

func BenchmarkMaskFromKeywordsInFunctionBodies_Hit(b *testing.B) {
	sql := "SELECT EXTRACT(YEAR FROM time), col FROM cpu WHERE x = 1 GROUP BY col ORDER BY a LIMIT 100"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		MaskFromKeywordsInFunctionBodies(sql)
	}
}

// BenchmarkUnmaskFromKeywordsInFunctionBodies measures the unmask path on
// a post-rewrite SQL (after the FROM-table regex has expanded `FROM cpu`
// into the longer `read_parquet(...)` form). Three masks is a realistic
// upper bound for typical analytics SQL (one per EXTRACT clause).
func BenchmarkUnmaskFromKeywordsInFunctionBodies(b *testing.B) {
	input := "SELECT EXTRACT(YEAR FROM ts1), EXTRACT(MONTH FROM ts2), EXTRACT(DAY FROM ts3) FROM cpu WHERE host = 'a'"
	masked, masks := MaskFromKeywordsInFunctionBodies(input)
	// Simulate the table rewriter expanding the table reference.
	rewritten := strings.Replace(masked,
		"FROM cpu",
		"FROM read_parquet('./data/default/cpu/**/*.parquet', union_by_name=true)",
		1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		UnmaskFromKeywordsInFunctionBodies(rewritten, masks)
	}
}
