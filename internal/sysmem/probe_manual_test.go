package sysmem

import "testing"

// TestLimit_OnThisMachine is a live smoke check, not an assertion about any
// particular value: it reports what detection found so a reader of the test log
// can sanity-check it against the machine, and fails only if detection does not
// work at all on a platform Arc ships for.
func TestLimit_OnThisMachine(t *testing.T) {
	bytes, source, ok := Limit()
	t.Logf("detected: bytes=%d source=%s ok=%v (%.1f GiB)", bytes, source, ok, float64(bytes)/(1<<30))
	if !ok {
		t.Skipf("no limit detectable on this platform (source=%s)", source)
	}
	if bytes < 64<<20 {
		t.Fatalf("detected %d bytes, which is below any plausible machine or container limit", bytes)
	}
}
