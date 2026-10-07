package metrics

import "testing"

// The shadow counters' call sites live behind the `arcx_engine` build tag, which
// Arc's CI never compiles — so nothing else in CI can catch a mistyped label. This
// test is untagged on purpose: it pins every exported constant to a real slot and
// proves an unknown label is counted as `invalid` rather than minting a new one.
func TestArcxShadowOutcomeLabelsAreClosedAndComplete(t *testing.T) {
	m := &Metrics{}
	for _, label := range []string{
		ShadowMatch, ShadowMismatch, ShadowArgDiff, ShadowDeclined, ShadowError,
		ShadowOracleError, ShadowPanic, ShadowSkippedShed, ShadowSkippedCap, ShadowSkippedHost,
	} {
		before := m.arcxShadowSnapshot()
		m.IncArcxShadowOutcome(label)
		after := m.arcxShadowSnapshot()
		if _, ok := after[label]; !ok {
			t.Fatalf("label %q is not a slot in arcxShadowOutcomes", label)
		}
		if after[label] != before[label]+1 {
			t.Fatalf("label %q did not increment its own slot", label)
		}
		if after["invalid"] != before["invalid"] {
			t.Fatalf("label %q leaked into invalid", label)
		}
	}
	// Every declared slot must be reachable through an exported constant, so a new
	// outcome cannot be added to the table and then referenced by a bare literal.
	consts := map[string]bool{
		ShadowMatch: true, ShadowMismatch: true, ShadowArgDiff: true,
		ShadowDeclined: true, ShadowError: true, ShadowOracleError: true,
		ShadowPanic: true, ShadowSkippedShed: true, ShadowSkippedCap: true,
		ShadowSkippedHost: true,
	}
	for _, name := range ArcxShadowOutcomes() {
		if !consts[name] {
			t.Fatalf("outcome %q has no exported constant", name)
		}
	}
}

func TestArcxShadowUnknownOutcomeIsInvalid(t *testing.T) {
	m := &Metrics{}
	before := m.arcxShadowSnapshot()["invalid"]
	m.IncArcxShadowOutcome("mismatches") // a plausible typo
	if got := m.arcxShadowSnapshot()["invalid"]; got != before+1 {
		t.Fatalf("unknown label did not land in invalid (got %d, want %d)", got, before+1)
	}
}
