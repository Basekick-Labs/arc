package metrics

import "testing"

// The closed set is enforced at THIS boundary too: an unknown reason — e.g. a
// future caller passing an input-derived string — is counted as invalid and its
// text is never stored or emitted. This is the second half of the no-user-bytes
// guarantee (the first is the producer's constant-only table).
func TestArcxCensusRejectsUnknownReasons(t *testing.T) {
	m := Get()
	before := arcxCensusInvalid.Load()
	m.IncArcxShapeCensus("zzsentinelzz")
	if got := arcxCensusInvalid.Load(); got != before+1 {
		t.Fatalf("invalid counter: got %d, want %d", got, before+1)
	}
	snap := m.arcxCensusSnapshot()
	if _, leaked := snap["zzsentinelzz"]; leaked {
		t.Fatal("unknown reason string leaked into the snapshot as a key")
	}
	// And a valid reason lands in its slot.
	m.IncArcxShapeCensus("group_by")
	if snap := m.arcxCensusSnapshot(); snap["group_by"] < 1 {
		t.Fatal("valid reason not counted")
	}
}
