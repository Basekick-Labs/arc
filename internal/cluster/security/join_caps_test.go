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

func TestJoinCapabilitiesAreBounded(t *testing.T) {
	many := make([]string, MaxJoinCapabilities+1)
	for i := range many {
		many[i] = "c"
	}
	long := []string{string(make([]byte, MaxJoinCapabilityLen+1))}
	for name, caps := range map[string][]string{"too many": many, "too long": long, "empty": {""}} {
		if CheckJoinCapabilities(caps) == nil {
			t.Errorf("%s capabilities accepted", name)
		}
		f := JoinAuthFields{NodeID: "w1"}
		ts := time.Now().Unix()
		if ValidateJoinCapsHMAC("s", "n", ts, f, caps, ComputeJoinCapsHMAC("s", "n", ts, f, caps), time.Minute) == nil {
			t.Errorf("%s capabilities validated", name)
		}
	}
	if CheckJoinCapabilities([]string{"stream_claims_v1"}) != nil || CheckJoinCapabilities(nil) != nil {
		t.Fatal("normal capabilities refused")
	}
}
