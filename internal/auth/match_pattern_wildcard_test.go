package auth

import "testing"

// Every pattern patternValidationRegex accepts must have a matchPattern
// branch. A pattern that is accepted at creation but matches nothing is not
// harmless: since an RBAC denial became final, such a grant silently locks
// its token out of everything it was meant to authorize.
func TestMatchPattern_EveryAcceptedPatternCanMatch(t *testing.T) {
	// Patterns the validator accepts, each with a value it must match.
	mustMatch := []struct{ pattern, value string }{
		{"*", "anything"},
		{"*", ""},
		{"cpu", "cpu"},
		{"prod_*", "prod_us"},
		{"prod*", "production"},
		{"*_metrics", "cpu_metrics"},
		{"*metrics", "cpu_metrics"},   // the branch that was missing
		{"*metrics", "httpmetrics"},   // no separator needed
		{"*metrics", "metrics"},       // the bare suffix itself
		{"*-metrics", "node-metrics"}, // hyphen is accepted by the validator
	}
	for _, tc := range mustMatch {
		if !matchPattern(tc.pattern, tc.value) {
			t.Errorf("matchPattern(%q, %q) = false, want true", tc.pattern, tc.value)
		}
		if err := validatePattern(tc.pattern); err != nil {
			t.Errorf("validatePattern(%q) rejected a pattern matchPattern handles: %v", tc.pattern, err)
		}
	}

	// The fix's own edges: it must not widen anything else.
	mustNotMatch := []struct{ pattern, value string }{
		{"*metrics", "metrics_cpu"}, // suffix, not prefix
		{"*metrics", "cpu"},         // unrelated
		{"*_metrics", "cpumetrics"}, // the underscore form still needs one
		{"prod_*", "production"},    // still anchored on the underscore
		{"prod*", "staging_prod"},   // trailing form is a prefix, not a suffix
		{"cpu", "CPU"},              // case-sensitive, as before
		{"cpu", "cpu_total"},        // exact stays exact
		{"*metrics", ""},            // empty value matches no real suffix
	}
	for _, tc := range mustNotMatch {
		if matchPattern(tc.pattern, tc.value) {
			t.Errorf("matchPattern(%q, %q) = true, want false", tc.pattern, tc.value)
		}
	}
}
