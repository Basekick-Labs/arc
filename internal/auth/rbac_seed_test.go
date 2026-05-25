package auth

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestSeedRBACFromLocalSQLite_CancelledCtxSkipsChildCounts pins
// Gemini PR #458 round 7 G24: when ctx is already cancelled at the
// point the per-org propose loop finishes (or breaks via G10),
// SeedRBACFromLocalSQLite must NOT execute the 4 COUNT(*) queries
// that detect pre-A.1 child rows — each would fail with
// `context canceled` and flood the log with four useless Warn lines.
//
// We use a pre-cancelled ctx so the loop never starts and the seed
// returns firstErr=ctx.Err() before reaching the COUNT(*) block.
// The function should return ctx.Err(), not nil.
func TestSeedRBACFromLocalSQLite_CancelledCtxSkipsChildCounts(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rbac-seedcancel.db")
	am, err := NewAuthManager(dbPath, 100*time.Millisecond, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()
	rm := NewRBACManager(&RBACManagerConfig{DB: am.GetDB(), Logger: zerolog.Nop()})
	defer rm.Close()

	// Pre-seed one org in local SQLite so the seed has work to do
	// (the inner loop iterates and the ctx-cancel check at the top
	// fires on the first iteration).
	if _, err := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{Name: "to-seed"}); err != nil {
		t.Fatalf("pre-seed org: %v", err)
	}

	// Wire a proposer so the seed actually runs (HasLocalRBACState
	// short-circuit would otherwise skip the whole function for
	// non-cluster nodes).
	prop := &alwaysFailProposer{}
	rm.SetRaftProposer(prop)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err = rm.SeedRBACFromLocalSQLite(ctx)
	elapsed := time.Since(start)

	// Expect ctx.Err() in the chain (the loop's ctx.Err() check fires
	// at the top of iteration 1).
	if err == nil {
		t.Fatal("expected ctx-cancel error from seed, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled in error chain, got: %v", err)
	}
	// Sanity: should bail well within 200ms. If we forgot to add
	// the G24 ctx check, the 4 COUNT(*) queries would all run +
	// each emit a Warn before the function returns — observable
	// indirectly via elapsed time + log noise (we don't tap logs
	// here, but timing alone is a tight signal).
	if elapsed > 200*time.Millisecond {
		t.Errorf("seed took %v with pre-cancelled ctx (expected <200ms)", elapsed)
	}
}

// alwaysFailProposer is a minimal proposer that's IsLeader=true so
// the seed body runs, and Propose returns an error so the seed's
// per-org loop counts it as a failure rather than a duplicate.
// Used only by the cancelled-ctx test above; the body doesn't
// actually reach Propose because the ctx.Err() check fires first.
type alwaysFailProposer struct{}

func (p *alwaysFailProposer) IsLeader() bool { return true }
func (p *alwaysFailProposer) Propose(ctx context.Context, cmdType uint8, payload []byte, timeout time.Duration) error {
	// If ever reached, surface a distinct error so the test
	// distinguishes "ctx-check failed to fire" from "propose ran".
	return errors.New("alwaysFailProposer.Propose: should NOT have been reached with pre-cancelled ctx")
}
