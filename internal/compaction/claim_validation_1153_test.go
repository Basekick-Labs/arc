package compaction

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// A released, nil, or foreign claim must be refused rather than run.
//
// Running on a released claim is the dangerous case: cycleRunning is false, so
// another cycle can claim and run at the same time -- the exact exclusivity
// CycleClaim exists to provide. These are exported entry points, so the guard
// is the contract, not the doc comment.
func TestRunClaimedCycleRejectsInvalidClaimsIssue1153(t *testing.T) {
	m := NewManager(&ManagerConfig{CycleTimeout: time.Minute, Logger: zerolog.Nop()})
	other := NewManager(&ManagerConfig{CycleTimeout: time.Minute, Logger: zerolog.Nop()})

	released, err := m.ClaimCycle()
	if err != nil {
		t.Fatal(err)
	}
	released.Release()

	foreign, err := other.ClaimCycle()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(foreign.Release)

	for name, claim := range map[string]*CycleClaim{
		"nil":      nil,
		"released": released,
		"foreign":  foreign,
	} {
		t.Run(name, func(t *testing.T) {
			if err := m.RunClaimedCycleForTiers(context.Background(), claim, []string{"hourly"}); !errors.Is(err, errInvalidClaim) {
				t.Fatalf("RunClaimedCycleForTiers(%s) = %v, want errInvalidClaim", name, err)
			}
			if err := m.RunClaimedCycleForDatabase(context.Background(), claim, "db", []string{"hourly"}); !errors.Is(err, errInvalidClaim) {
				t.Fatalf("RunClaimedCycleForDatabase(%s) = %v, want errInvalidClaim", name, err)
			}
			if err := m.RunClaimedCycleForMeasurement(context.Background(), claim, "db", "cpu", []string{"hourly"}); !errors.Is(err, errInvalidClaim) {
				t.Fatalf("RunClaimedCycleForMeasurement(%s) = %v, want errInvalidClaim", name, err)
			}
		})
	}
}
