package compaction

// Compaction at shutdown (#1887): BeginShutdown stops a cycle at its
// boundaries as the pause does, with its own error and status, and refuses
// new claims; KillCycles cancels a running batch, and WaitIdle sees the
// claim's release.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func assertStoppedOutcome(t *testing.T, m *Manager, err error, starts *atomic.Int32) {
	t.Helper()
	if !errors.Is(err, ErrCompactionStopping) {
		t.Fatalf("cycle error = %v, want ErrCompactionStopping", err)
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("batches started = %d, want 1 (the running batch finishes, no new one starts)", got)
	}
	outcome := cycleOutcomeIssue915(t, m)
	if outcome["status"] != "stopped" {
		t.Fatalf("status = %v, want stopped", outcome["status"])
	}
	if got, _ := outcome["failed_batches"].(int64); got != 0 {
		t.Fatalf("failed_batches = %d: a stop is not a failure", got)
	}
}

func TestShutdownStopsBeforeTheNextPartition1887(t *testing.T) {
	rig := newPausedCycleRig(t, []Candidate{candidateIssue915("partition-one"), candidateIssue915("partition-two")})
	done := rig.runCycle(t)
	rig.manager.BeginShutdown()
	close(rig.release)
	assertStoppedOutcome(t, rig.manager, awaitCycle(t, done), &rig.starts)
}

func TestShutdownStopsBetweenTheBatchesOfOnePartition1887(t *testing.T) {
	big := candidateIssue915("partition-one")
	big.Files = []string{"one.parquet", "two.parquet", "three.parquet", "four.parquet"}
	big.FileCount = 4
	rig := newPausedCycleRig(t, []Candidate{big})
	rig.manager.MaxFilesPerBatch = 2
	done := rig.runCycle(t)
	rig.manager.BeginShutdown()
	close(rig.release)
	assertStoppedOutcome(t, rig.manager, awaitCycle(t, done), &rig.starts)
}

func TestShutdownStopsDiscoveryBetweenMeasurements1887(t *testing.T) {
	manager, backend, cleanup := setupTestManager(t)
	defer cleanup()
	manager.ManifestManager = nil
	ctx := context.Background()
	for _, p := range []string{"db/cpu/2026/01/01/00/a.parquet", "db/mem/2026/01/01/00/a.parquet"} {
		if err := backend.Write(ctx, p, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	var finds atomic.Int32
	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(context.Context, string, string) ([]Candidate, error) {
			finds.Add(1)
			manager.BeginShutdown() // shutdown begins during discovery
			return nil, nil
		},
	}}
	manager.compactBatchForTest = func(context.Context, Candidate) error { t.Error("a batch ran"); return nil }
	_, err := manager.RunCompactionCycleForDatabase(ctx, "db", []string{"hourly"})
	if !errors.Is(err, ErrCompactionStopping) {
		t.Fatalf("err = %v, want ErrCompactionStopping", err)
	}
	if finds.Load() != 1 {
		t.Fatalf("measurements discovered after the stop = %d, want 1", finds.Load())
	}
}

// After BeginShutdown no claim is taken: not by a trigger, a scheduler tick,
// or a deferred tick waking on a release.
func TestShutdownRefusesNewClaims1887(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()
	manager.BeginShutdown()
	if _, err := manager.ClaimCycle(); !errors.Is(err, ErrCompactionStopping) {
		t.Fatalf("ClaimCycle after BeginShutdown = %v, want ErrCompactionStopping", err)
	}
	if manager.IsCycleRunning() {
		t.Fatal("a refused claim left the cycle marked running")
	}
	if !manager.WaitIdle(context.Background()) {
		t.Fatal("WaitIdle with no cycle reported busy")
	}
}

// KillCycles cancels a running batch's context (its subprocess's, in
// production); WaitIdle returns once the cycle released its claim.
func TestKillCyclesCancelsTheRunningBatch1887(t *testing.T) {
	rig := newPausedCycleRig(t, []Candidate{candidateIssue915("partition-one")})
	done := rig.runCycle(t)
	rig.manager.BeginShutdown()
	short, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if rig.manager.WaitIdle(short) {
		t.Fatal("WaitIdle returned idle while a batch held the claim")
	}
	rig.manager.KillCycles()
	wctx, wcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer wcancel()
	if !rig.manager.WaitIdle(wctx) {
		t.Fatal("the cycle did not release its claim after the kill")
	}
	if err := awaitCycle(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("killed cycle = %v, want context.Canceled", err)
	}
}

// Without BeginShutdown, a split between its halves goes on: the stop check
// reads only the shutdown flag.
func TestHaltIsShutdownBeforePause1887(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()
	var paused atomic.Bool
	paused.Store(true)
	manager.SetPauseGate(paused.Load)
	if !errors.Is(manager.halt(), ErrCompactionPaused) {
		t.Fatal("halt under a pause is not ErrCompactionPaused")
	}
	manager.BeginShutdown()
	if !errors.Is(manager.halt(), ErrCompactionStopping) {
		t.Fatal("halt during shutdown is not ErrCompactionStopping")
	}
}
