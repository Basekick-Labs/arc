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

// A literal's placeholder ends in `__`, so caller text written straight
// after the literal can complete a FROM placeholder only in the masked form.
func TestContainsFromMaskPlaceholderAfterLiteralMask(t *testing.T) {
	raw := "SELECT 'a'FROM_MASK_0__ x"
	if ContainsReservedPlaceholder(raw) {
		t.Fatalf("raw form unexpectedly matched; the test no longer covers the masked-only shape")
	}
	masked, _ := MaskStringLiterals(raw, true)
	if !ContainsFromMaskPlaceholder(masked) {
		t.Fatalf("masked %q: FROM placeholder not detected", masked)
	}
	if ContainsFromMaskPlaceholder("SELECT __STR_0__, __IDENT_1__ FROM cpu") {
		t.Fatalf("literal and identifier placeholders must not match the FROM check")
	}
}
