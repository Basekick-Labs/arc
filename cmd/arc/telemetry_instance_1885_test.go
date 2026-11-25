package main

import "testing"

// One configured UUID gives each cluster node its own, stable id.
func TestTelemetryInstanceIDPerNode1885(t *testing.T) {
	const base = "4f8a2c1e-9b3d-4e5f-8a7b-1c2d3e4f5a6b"
	if got := telemetryInstanceID(base, false, "reader-0"); got != base {
		t.Fatalf("standalone: %q, want the configured id", got)
	}
	if got := telemetryInstanceID("", true, "reader-0"); got != "" {
		t.Fatalf("unset: %q, want empty (the id file)", got)
	}
	r0, r1 := telemetryInstanceID(base, true, "reader-0"), telemetryInstanceID(base, true, "reader-1")
	if r0 == base || r1 == base || r0 == r1 {
		t.Fatalf("nodes got %q and %q from %q: want two distinct derived ids", r0, r1, base)
	}
	if again := telemetryInstanceID(base, true, "reader-0"); again != r0 {
		t.Fatalf("not stable: %q then %q", r0, again)
	}
	if got := telemetryInstanceID(base, true, " "); got != base {
		t.Fatalf("cluster without a node id: %q, want the configured id", got)
	}
}
