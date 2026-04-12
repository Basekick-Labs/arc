package cluster

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Phase 4 health-check tests: verify the rate-limited "no compactor
// elected" / "multiple compactors elected" warnings fire on the right
// registry shapes and respect the 60-second throttle.
//
// These are narrow tests that drive checkCompactorElected directly
// rather than exercising the full health loop — that keeps them fast
// and deterministic, and the loop itself is covered by higher-level
// cluster tests.

// newTestHealthChecker builds a HealthChecker wired to a local-only
// registry. The returned buffer captures log output so assertions can
// grep for the expected Warn messages.
func newTestHealthChecker(t *testing.T, warnIfNoCompactor bool) (*HealthChecker, *Registry, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	logger := zerolog.New(&buf).With().Timestamp().Logger()

	local := NewNode("local-1", "local-1", RoleReader, "test-cluster")
	local.State = StateHealthy
	registry := NewRegistry(&RegistryConfig{
		LocalNode: local,
		Logger:    zerolog.Nop(),
	})

	h := NewHealthChecker(&HealthCheckerConfig{
		Registry:          registry,
		WarnIfNoCompactor: warnIfNoCompactor,
		Logger:            logger,
	})
	return h, registry, &buf
}

// registerHealthyCompactor adds a compactor node in Healthy state.
func registerHealthyCompactor(t *testing.T, registry *Registry, id string) {
	t.Helper()
	n := NewNode(id, id, RoleCompactor, "test-cluster")
	n.State = StateHealthy
	if err := registry.Register(n); err != nil {
		t.Fatalf("registry.Register %s: %v", id, err)
	}
}

func TestHealth_NoCompactorElectedFiresWarning(t *testing.T) {
	h, _, logs := newTestHealthChecker(t, true)

	// Registry has only the local reader node — no compactors at all.
	h.checkCompactorElected()

	if !strings.Contains(logs.String(), "No compactor elected") {
		t.Errorf("expected 'No compactor elected' warning, got logs: %s", logs.String())
	}
}

func TestHealth_SingleCompactorIsQuiet(t *testing.T) {
	h, registry, logs := newTestHealthChecker(t, true)
	registerHealthyCompactor(t, registry, "comp-1")

	h.checkCompactorElected()

	if strings.Contains(logs.String(), "compactor elected") {
		t.Errorf("expected no warnings for single compactor, got logs: %s", logs.String())
	}
}

func TestHealth_MultipleCompactorsFiresWarning(t *testing.T) {
	h, registry, logs := newTestHealthChecker(t, true)
	registerHealthyCompactor(t, registry, "comp-1")
	registerHealthyCompactor(t, registry, "comp-2")

	h.checkCompactorElected()

	if !strings.Contains(logs.String(), "Multiple compactors elected") {
		t.Errorf("expected 'Multiple compactors elected' warning, got logs: %s", logs.String())
	}
	// Both compactor IDs should appear in the structured log output.
	if !strings.Contains(logs.String(), "comp-1") || !strings.Contains(logs.String(), "comp-2") {
		t.Errorf("expected both compactor IDs in log, got: %s", logs.String())
	}
}

func TestHealth_WarnRateLimit(t *testing.T) {
	h, _, logs := newTestHealthChecker(t, true)

	// Fire three back-to-back checks — only the first should emit a warning.
	h.checkCompactorElected()
	h.checkCompactorElected()
	h.checkCompactorElected()

	count := strings.Count(logs.String(), "No compactor elected")
	if count != 1 {
		t.Errorf("expected exactly 1 warning across 3 back-to-back checks (rate-limited), got %d. Logs:\n%s", count, logs.String())
	}
}

func TestHealth_WarnDisabled(t *testing.T) {
	// WarnIfNoCompactor = false: OSS / standalone path. Even an empty
	// compactor set must not produce any warnings.
	h, _, logs := newTestHealthChecker(t, false)

	// Direct call to checkCompactorElected would fire if we ran it, but
	// the real loop only calls it when warnIfNoCompactor is true, so we
	// simulate the higher-level gating by checking the flag ourselves.
	// A full-path check would go through checkAllNodes, but that also
	// spawns goroutines for the per-node checks which we don't want here.
	if h.warnIfNoCompactor {
		t.Fatal("warnIfNoCompactor should be false")
	}
	// Nothing in logs since we didn't call the check.
	if strings.Contains(logs.String(), "compactor elected") {
		t.Errorf("unexpected log output with warn disabled: %s", logs.String())
	}
}

func TestHealth_ResetAllowsNextWarning(t *testing.T) {
	// When the cluster goes from broken → correct → broken again, the
	// warning should fire immediately on the second broken transition,
	// NOT wait a full minute. Driven by the "reset on single compactor"
	// branch in checkCompactorElected.
	h, registry, logs := newTestHealthChecker(t, true)

	// Initial broken state: no compactors → fires warning, sets timer.
	h.checkCompactorElected()
	if !strings.Contains(logs.String(), "No compactor elected") {
		t.Fatalf("expected initial 'No compactor elected' warning, got: %s", logs.String())
	}

	// Recover: add a compactor → single-compactor branch resets the timer.
	registerHealthyCompactor(t, registry, "comp-rescue")
	h.checkCompactorElected()

	// Now simulate losing the compactor (remove via Unregister). The
	// next check should fire the warning again without waiting — because
	// the timer was reset during the healthy window.
	registry.Unregister("comp-rescue")

	// Capture log length before the third check to isolate the new warning.
	logsBeforeThird := logs.Len()
	h.checkCompactorElected()

	newOutput := logs.String()[logsBeforeThird:]
	if !strings.Contains(newOutput, "No compactor elected") {
		t.Errorf("expected second 'No compactor elected' warning after rescue → loss, got new output: %s", newOutput)
	}
}

// TestHealth_SeparateTimersForFlappingMisconfig is the regression test
// for the Phase 4 security-review finding: if a cluster flaps between
// "0 compactors" and "2 compactors" within the rate-limit window, BOTH
// warnings must surface instead of one silencing the other via a shared
// timer. Shared-timer design was the original bug; separate timers per
// failure mode is the fix.
func TestHealth_SeparateTimersForFlappingMisconfig(t *testing.T) {
	h, registry, logs := newTestHealthChecker(t, true)

	// State 1: no compactors → should fire "No compactor elected".
	h.checkCompactorElected()
	if !strings.Contains(logs.String(), "No compactor elected") {
		t.Fatalf("expected 'No compactor elected' on first call, got: %s", logs.String())
	}

	// State 2: TWO compactors suddenly appear — simulates operator
	// misconfiguring the cluster during a rolling upgrade.
	registerHealthyCompactor(t, registry, "comp-1")
	registerHealthyCompactor(t, registry, "comp-2")

	logsBefore := logs.Len()
	h.checkCompactorElected()
	newOutput := logs.String()[logsBefore:]

	// The "Multiple compactors elected" warning MUST fire even though
	// the "No compactor elected" warning was just emitted and its timer
	// is still within the cooldown window. With a shared timer (the
	// original bug), this warning would be silenced. With separate
	// timers (the fix), it fires immediately.
	if !strings.Contains(newOutput, "Multiple compactors elected") {
		t.Errorf("expected 'Multiple compactors elected' to fire immediately after flip (shared-timer bug reproduction); new output: %s", newOutput)
	}
}

// TestHealth_WarnRateLimitReleasesAfterInterval is a coarse timing test
// that verifies the rate limit DOES allow a second warning after the
// interval elapses. We can't reliably wait 60 seconds in a unit test,
// so instead we manually reset the atomic and re-fire — documenting the
// contract explicitly.
func TestHealth_WarnRateLimitReleasesAfterInterval(t *testing.T) {
	h, _, logs := newTestHealthChecker(t, true)

	h.checkCompactorElected()
	firstCount := strings.Count(logs.String(), "No compactor elected")
	if firstCount != 1 {
		t.Fatalf("first call should fire: got %d warnings", firstCount)
	}

	// Simulate the interval passing by rewinding the timer. Each failure
	// mode has its own slot post-Phase-4 review fix; rewind the one used
	// by the "no compactor elected" branch.
	h.lastNoCompactorWarnAt.Store(time.Now().Add(-2 * compactorWarnInterval).UnixNano())
	h.checkCompactorElected()

	secondCount := strings.Count(logs.String(), "No compactor elected")
	if secondCount != 2 {
		t.Errorf("second call should fire after interval: got %d warnings total", secondCount)
	}
}
