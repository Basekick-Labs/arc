package mqtt

import (
	"strings"
	"testing"
)

// Grace days 7-14 of an expired license stop ingestion: subscriptions are
// suspended so the broker holds messages, and resume when that ends; the
// reason is independent of the others.
func TestLicenseWritesStoppedSuspends(t *testing.T) {
	m, s, _ := newSuspendableManager(t)
	m.SetLicenseWritesStopped(true)
	eventually(t, "not suspended", s.suspended.Load)
	m.SetIngestFenced(true)
	m.SetLicenseWritesStopped(false)
	settle(t, m)
	if !s.suspended.Load() {
		t.Fatal("resumed while still fenced")
	}
	m.SetIngestFenced(false)
	eventually(t, "never resumed", func() bool { return !s.suspended.Load() })
	if d := suspendLicense.describe(); !strings.Contains(d, "license") {
		t.Fatalf("%q lacks the license reason", d)
	}
}
