package syscpu

import (
	"runtime"
	"testing"
)

// TestEffectiveCores covers the helper every quota-derived default is built on.
//
// Table-driven over the pure form rather than driven through
// runtime.GOMAXPROCS(n): that is process-global, would slow every other test in
// the binary, and the values worth testing (0, 128 on a smaller box) are ones a
// test has no business installing process-wide.
func TestEffectiveCores(t *testing.T) {
	cases := []struct {
		name           string
		numCPU, gomaxp int
		want           int
	}{
		// The ordinary container case: NumCPU cannot see the CFS quota, GOMAXPROCS
		// can. This row is #1030.
		{"quota below machine", 64, 2, 2},
		{"no quota", 8, 8, 8},
		{"cpuset only", 2, 2, 2},
		// GOMAXPROCS env has no clamp in the runtime, so it can exceed the machine.
		// Verified: GOMAXPROCS=128 on an 8-CPU box reports 128.
		{"gomaxprocs raised above machine", 8, 128, 8},
		// An operator-raised GOMAXPROCS below the machine size is honoured. A
		// deliberate residual, documented on EffectiveCores.
		{"gomaxprocs between quota and machine", 64, 32, 32},
		// Degenerate inputs must not yield 0 — a 0 would make the compaction
		// threads default 0, which means "unset" to the subprocess.
		{"zero gomaxprocs", 8, 0, 8},
		{"zero both", 0, 0, 1},
		{"negative", -1, -1, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := effectiveCores(c.numCPU, c.gomaxp); got != c.want {
				t.Errorf("effectiveCores(%d, %d) = %d, want %d", c.numCPU, c.gomaxp, got, c.want)
			}
		})
	}
}

// TestEffectiveCores_MatchesRuntime pins that the exported live form reads both
// runtime values rather than only one of them.
func TestEffectiveCores_MatchesRuntime(t *testing.T) {
	want := effectiveCores(runtime.NumCPU(), runtime.GOMAXPROCS(0))
	if got := EffectiveCores(); got != want {
		t.Errorf("EffectiveCores() = %d, want %d", got, want)
	}
}

// CoresAtStartup must be a SNAPSHOT, not a live read.
//
// A licence enforces itself by pinning GOMAXPROCS down to its core limit, and
// every outbound report that happens after that pin — a re-activation, a
// telemetry tick — would otherwise carry the licence's number instead of the
// machine's.
//
// Pinning GOMAXPROCS is process-global, so this test restores it and does not
// run in parallel. It pins DOWN where there is room and UP otherwise, because
// gating the skip on CoresAtStartup() would make the whole test vanish on a
// runner started with GOMAXPROCS=1 — and that is the one case where a live
// read is indistinguishable from a snapshot, so the mutation this test exists
// to catch would survive the entire suite. Only a machine with a single
// logical CPU is genuinely uncoverable.
//
// Note when reproducing: `go test` result caching does not key on GOMAXPROCS,
// so re-running under a different value can report a stale green. Use
// -count=1.
func TestCoresAtStartupIgnoresALaterPin(t *testing.T) {
	if runtime.NumCPU() < 2 {
		t.Skip("a single-logical-CPU machine cannot pin GOMAXPROCS in either direction")
	}
	before := CoresAtStartup()
	target := 1
	if before == 1 {
		// Started with GOMAXPROCS=1; pin up instead, so the test still has a
		// pin to be indifferent to.
		target = 2
	}
	original := runtime.GOMAXPROCS(target)
	t.Cleanup(func() { runtime.GOMAXPROCS(original) })

	if live := EffectiveCores(); live != target {
		t.Fatalf("EffectiveCores() = %d after pinning GOMAXPROCS to %d; the live form must follow the pin", live, target)
	}
	if got := CoresAtStartup(); got != before {
		t.Errorf("CoresAtStartup() = %d after a pin, want %d: the snapshot moved", got, before)
	}
}

// On a machine with no CPU quota the two forms agree, which is what makes this
// change quota-only: nothing a bare-metal operator reports moves. The matrix
// row for that had no test.
func TestCoresAtStartupMatchesTheLiveFormWhenNothingHasPinned(t *testing.T) {
	if got, want := CoresAtStartup(), EffectiveCores(); got != want {
		t.Errorf("CoresAtStartup() = %d, EffectiveCores() = %d: they must agree before anything pins GOMAXPROCS", got, want)
	}
}
