package main

import (
	"runtime"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/license"
)

// These tests cover applyLicenseCoreLimits, whose job is to clamp every
// execution surface to the licensed core count. The bug they pin (#1030) is that
// the clamp used to be able to RAISE a limit: it gated on
// runtime.NumCPU() > lic.MaxCores and then assigned lic.MaxCores outright, so a
// 4-core licence in a 2-CPU pod on a 64-core host lifted GOMAXPROCS from the
// quota's 2 to 4 and set DuckDB threads=4.
//
// Both core counts are injected rather than read from the machine. That is not
// tidiness: an earlier version of this file set only GOMAXPROCS and relied on
// the host having more than four cores, which is true of a developer's laptop
// and false of the 2-4 vCPU runner that actually gates the PR. Three of these
// tests were green against the pre-fix function there. Injecting both makes the
// result identical everywhere.
//
// GOMAXPROCS itself is still real, because pinning it is one of the behaviours
// under test. Each test restores it; note that calling runtime.GOMAXPROCS(n > 0)
// at all pins the value for the rest of the process, which is deliberate in the
// production path and harmless in a test binary.

// withCores fixes what the function sees as the machine's core count and as the
// cores this process may use, and sets the real GOMAXPROCS to the latter so the
// pinning behaviour is exercised against a consistent picture.
func withCores(t *testing.T, machine, effective int) {
	t.Helper()
	previousNumCPU, previousEffective := numCPUFn, effectiveCoresFn
	numCPUFn = func() int { return machine }
	effectiveCoresFn = func() int { return effective }
	previousGOMAXPROCS := runtime.GOMAXPROCS(effective)
	t.Cleanup(func() {
		runtime.GOMAXPROCS(previousGOMAXPROCS)
		numCPUFn, effectiveCoresFn = previousNumCPU, previousEffective
	})
}

func baseConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Database.ThreadCount = 0 // the #1026 default: DuckDB reads the quota itself
	cfg.Ingest.FlushWorkers = 8
	return cfg
}

func TestApplyLicenseCoreLimits_UnlimitedIsNoOp(t *testing.T) {
	for _, maxCores := range []int{0, -1} {
		withCores(t, 64, 2)
		cfg := baseConfig()
		before := runtime.GOMAXPROCS(0)

		applyLicenseCoreLimits(&license.License{MaxCores: maxCores}, cfg)

		if cfg.Database.ThreadCount != 0 {
			t.Errorf("MaxCores=%d: ThreadCount = %d, want 0 (unlimited licence must not touch it)", maxCores, cfg.Database.ThreadCount)
		}
		if cfg.Ingest.FlushWorkers != 8 {
			t.Errorf("MaxCores=%d: FlushWorkers = %d, want 8", maxCores, cfg.Ingest.FlushWorkers)
		}
		if got := runtime.GOMAXPROCS(0); got != before {
			t.Errorf("MaxCores=%d: GOMAXPROCS = %d, want %d (unlimited licence must not pin it)", maxCores, got, before)
		}
	}
}

// TestApplyLicenseCoreLimits_NeverRaisesGOMAXPROCS is the #1030 regression.
// A 2-core quota with a 4-core licence: the licence is not the binding limit, so
// nothing may be raised to meet it.
func TestApplyLicenseCoreLimits_NeverRaisesGOMAXPROCS(t *testing.T) {
	withCores(t, 64, 2)
	cfg := baseConfig()

	applyLicenseCoreLimits(&license.License{MaxCores: 4}, cfg)

	if got := runtime.GOMAXPROCS(0); got != 2 {
		t.Errorf("GOMAXPROCS = %d after a 4-core licence under a 2-core quota, want 2 (a licence cap may only lower)", got)
	}
}

// TestApplyLicenseCoreLimits_ThreadCountClampedNotRaised covers the pair of
// opposing failures in the zero branch, which is the subtlest part of this
// function. DuckDB picks its own thread count when ThreadCount is 0, and it
// picks it from cpu.max — never from GOMAXPROCS. So:
//
//   - gating on the effective core count leaves the count UNSET whenever
//     effective <= licensed < machine, and DuckDB then runs with the machine's
//     cores: a 4-core licence not enforced at all.
//   - clamping to the licence alone can RAISE the count above the quota whenever
//     the effective count is inflated past it.
//
// Only a gate on the machine count with a value clamped by the effective count
// is right in both directions.
func TestApplyLicenseCoreLimits_ThreadCountClampedNotRaised(t *testing.T) {
	cases := []struct {
		name                       string
		machine, effective, licens int
		want                       int
	}{
		// The quota binds below the licence: DuckDB would pick 2 on its own, so 2
		// is both the enforcement and the no-raise answer.
		{"quota below licence below machine", 64, 2, 4, 2},
		// GOMAXPROCS env below the machine count, no quota. DuckDB cannot see it
		// and would run 14 threads on a 4-core licence. Gating on the effective
		// count would leave this unset — the regression this case exists for.
		{"effective equals licence below machine", 14, 4, 4, 4},
		// No quota at all: the licence is the only limit.
		{"no quota, licence below machine", 64, 64, 4, 4},
		// Licence at or above the machine: nothing to enforce, leave DuckDB alone.
		{"licence at machine", 8, 8, 8, 0},
		{"licence above machine", 8, 8, 32, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			withCores(t, c.machine, c.effective)
			cfg := baseConfig()

			applyLicenseCoreLimits(&license.License{MaxCores: c.licens}, cfg)

			if cfg.Database.ThreadCount != c.want {
				t.Errorf("machine=%d effective=%d licensed=%d: ThreadCount = %d, want %d",
					c.machine, c.effective, c.licens, cfg.Database.ThreadCount, c.want)
			}
			if cfg.Database.ThreadCount > c.licens {
				t.Errorf("ThreadCount = %d exceeds the licensed %d", cfg.Database.ThreadCount, c.licens)
			}
			if cfg.Database.ThreadCount > c.effective {
				t.Errorf("ThreadCount = %d exceeds the %d cores this process may use", cfg.Database.ThreadCount, c.effective)
			}
		})
	}
}

// TestApplyLicenseCoreLimits_ClampsFlushWorkersUnderQuota covers the half a
// cores-based early return would have skipped: flush_workers is resolved from
// the machine's core count, so it needs clamping even when the quota is already
// below the licence.
func TestApplyLicenseCoreLimits_ClampsFlushWorkersUnderQuota(t *testing.T) {
	withCores(t, 64, 2)
	cfg := baseConfig()
	cfg.Ingest.FlushWorkers = 8

	applyLicenseCoreLimits(&license.License{MaxCores: 4}, cfg)

	if cfg.Ingest.FlushWorkers != 4 {
		t.Errorf("FlushWorkers = %d, want 4: the licence is below the resolved pool size, quota or no quota", cfg.Ingest.FlushWorkers)
	}
}

// TestApplyLicenseCoreLimits_ClampsExplicitThreadCount: an operator-set
// database.thread_count must be capped even when the quota is already below the
// licence.
func TestApplyLicenseCoreLimits_ClampsExplicitThreadCount(t *testing.T) {
	withCores(t, 64, 2)
	cfg := baseConfig()
	cfg.Database.ThreadCount = 64

	applyLicenseCoreLimits(&license.License{MaxCores: 4}, cfg)

	if cfg.Database.ThreadCount != 4 {
		t.Errorf("ThreadCount = %d, want 4: an explicit 64 must not escape a 4-core licence", cfg.Database.ThreadCount)
	}
}

// TestApplyLicenseCoreLimits_ExplicitThreadCountBelowLicenceKept: clamping is
// one-directional, so a conservative explicit value stays conservative.
func TestApplyLicenseCoreLimits_ExplicitThreadCountBelowLicenceKept(t *testing.T) {
	withCores(t, 8, 8)
	cfg := baseConfig()
	cfg.Database.ThreadCount = 2

	applyLicenseCoreLimits(&license.License{MaxCores: 4}, cfg)

	if cfg.Database.ThreadCount != 2 {
		t.Errorf("ThreadCount = %d, want 2 (a licence cap may only lower)", cfg.Database.ThreadCount)
	}
}

// TestApplyLicenseCoreLimits_LowersEverythingWhenLicenceBinds is the case that
// always worked and must keep working: no quota, licence below the machine.
func TestApplyLicenseCoreLimits_LowersEverythingWhenLicenceBinds(t *testing.T) {
	withCores(t, 14, 14)
	cfg := baseConfig()
	cfg.Ingest.FlushWorkers = 16

	applyLicenseCoreLimits(&license.License{MaxCores: 1}, cfg)

	if got := runtime.GOMAXPROCS(0); got != 1 {
		t.Errorf("GOMAXPROCS = %d, want 1", got)
	}
	if cfg.Database.ThreadCount != 1 {
		t.Errorf("ThreadCount = %d, want 1", cfg.Database.ThreadCount)
	}
	if cfg.Ingest.FlushWorkers != 1 {
		t.Errorf("FlushWorkers = %d, want 1", cfg.Ingest.FlushWorkers)
	}
}

// TestEffectiveCoresSeam pins that the seam is wired to the real thing, so the
// injected tests above are testing the production path and not a fiction.
func TestEffectiveCoresSeam(t *testing.T) {
	if got, want := effectiveCoresFn(), config.EffectiveCores(); got != want {
		t.Errorf("effectiveCoresFn() = %d, want %d", got, want)
	}
	if got, want := numCPUFn(), runtime.NumCPU(); got != want {
		t.Errorf("numCPUFn() = %d, want %d", got, want)
	}
}
