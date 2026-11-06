package sql

import "testing"

func TestContainsReservedPlaceholder(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"", false},
		{"SELECT 1", false},
		{"SELECT my_str_col, _ident_x, from_mask FROM cpu", false},
		{"SELECT a__b FROM cpu", false},
		{"__STR_", true},
		{"x __STR_0__ y", true},
		{"x__IDENT_3__", true},
		{"a __FROM_MASK_0__ b", true},
		// The restorers match exact bytes; other cases are ordinary names.
		{"a __from_mask_0__ b", false},
		{"x__str_0__", false},
		{"'__STR_1__'", true},
		{`"__IDENT_0__"`, true},
		{"___STR_", true},
		{"__ST", false},
		{"__FROM_MASK", false},
	} {
		if got := ContainsReservedPlaceholder(tc.in); got != tc.want {
			t.Errorf("ContainsReservedPlaceholder(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestHasCallerPlaceholder(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"SELECT 1", false},
		{"SELECT 'a', \"b\" FROM cpu", false},
		// Inside a literal or quoted identifier: hidden and restored as is.
		{"SELECT '__STR_0__', '__FROM_MASK_0__', \"x__IDENT_0__\" FROM cpu", false},
		{"SELECT v FROM cpu WHERE m LIKE '%__STR_%'", false},
		// The masker reuses an identifier's placeholder for each occurrence.
		{"SELECT \"x\" FROM cpu \"x\" WHERE \"x\".v = 'a'", false},
		// Outside them: restored with the real placeholders.
		{"SELECT __STR_0__ FROM cpu", true},
		{"SELECT __STR_x FROM cpu", true},
		{"SELECT 'a'IDENT_ FROM cpu", true},
		{"SELECT e__STR_0__, 'x' FROM cpu", true},
		{"SELECT __IDENT_0__, \"x\" FROM cpu", true},
		{"SELECT a __FROM_MASK_0__ cpu", true},
		{"SELECT v FROM cpu -- __STR_0__", true},
		// Completed only once the literal is masked.
		{"SELECT 'a'STR_1__, 'b' FROM cpu", true},
		{"SELECT 'a'FROM_MASK_0__ cpu", true},
		// Exact bytes: the restorers never match another case.
		{"SELECT __str_0__, __from_mask_0__ FROM cpu", false},
	} {
		masked, masks := MaskStringLiterals(tc.in, true)
		if got := HasCallerPlaceholder(masked, masks); got != tc.want {
			t.Errorf("HasCallerPlaceholder(%q) = %v, want %v (masked %q)", tc.in, got, tc.want, masked)
		}
	}
}
