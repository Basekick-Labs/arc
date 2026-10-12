package security

import (
	"testing"
	"time"
)

// The capabilities MAC (#1207) leaves the original join MAC untouched — an
// older leader keeps validating it — and binds the capabilities themselves.
func TestJoinCapsHMAC(t *testing.T) {
	f := JoinAuthFields{NodeID: "w1", NodeName: "w1", Role: "writer", ClusterName: "c", Version: "v"}
	ts := time.Now().Unix()
	plain := ComputeJoinHMAC("s", "n", ts, f)
	if err := ValidateJoinHMAC("s", "n", ts, f, plain, time.Minute); err != nil {
		t.Fatalf("the original MAC no longer validates: %v", err)
	}
	caps := []string{"stream_claims_v1"}
	mac2 := ComputeJoinCapsHMAC("s", "n", ts, f, caps)
	if err := ValidateJoinCapsHMAC("s", "n", ts, f, caps, mac2, time.Minute); err != nil {
		t.Fatalf("caps MAC: %v", err)
	}
	if mac2 == plain {
		t.Fatal("the caps MAC equals the plain one: one could stand in for the other")
	}
	for name, other := range map[string][]string{"added": {"stream_claims_v1", "x"}, "removed": nil, "changed": {"stream_claims_v2"}} {
		if err := ValidateJoinCapsHMAC("s", "n", ts, f, other, mac2, time.Minute); err == nil {
			t.Errorf("capabilities %s in transit passed", name)
		}
	}
}
