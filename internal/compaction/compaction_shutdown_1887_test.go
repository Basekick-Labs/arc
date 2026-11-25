package compaction

// Compaction at shutdown (#1887): BeginShutdown stops a cycle at its
// boundaries as the pause does, with its own error and status, and refuses
// new claims; KillCycles cancels a running batch, and WaitIdle sees the
// claim's release.

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
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

// After BeginShutdown ClaimCycle refuses, which is what every claim path (a
// trigger, a scheduler tick, a deferred tick waking on a release) goes
// through.
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

// halt names shutdown before the pause, so a paused cluster's shutdown logs
// and records "stopped".
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

// Recovery stops between manifests: a manifest not reached stays for the
// next start.
func TestShutdownStopsRecoveryBetweenManifests1887(t *testing.T) {
	m, backend, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	m.ManifestManager = NewManifestManager(backend, zerolog.Nop())
	const out = "db/cpu/2026/01/01/00/out_compacted.parquet"
	if err := backend.Write(ctx, out, []byte("data")); err != nil {
		t.Fatal(err)
	}
	mpath, err := m.ManifestManager.WriteManifest(ctx, &Manifest{
		OutputPath: out, OutputSize: 4, InputFiles: []string{"db/cpu/2026/01/01/00/raw.parquet"},
		Database: "db", Measurement: "cpu", PartitionPath: "db/cpu/2026/01/01/00",
		Tier: "hourly", Status: ManifestStatusPending, CreatedAt: time.Now().UTC(), JobID: "j1887",
	})
	if err != nil {
		t.Fatal(err)
	}
	m.BeginShutdown()
	recovered, err := m.recoverOrphanedManifests(ctx, recoveryScope{})
	if !errors.Is(err, ErrCompactionStopping) || recovered != 0 {
		t.Fatalf("recovery during shutdown = (%d, %v), want (0, ErrCompactionStopping)", recovered, err)
	}
	if present, _ := backend.Exists(ctx, mpath); !present {
		t.Fatal("the manifest was recovered after shutdown began")
	}
}

// listingBackend calls onList for each directory listing.
type listingBackend struct {
	storage.Backend
	onList func(prefix string)
}

func (b *listingBackend) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	b.onList(prefix)
	return b.Backend.(storage.DirectoryLister).ListDirectories(ctx, prefix)
}

// The measurement listing stops between databases.
func TestShutdownStopsTheListingBetweenDatabases1887(t *testing.T) {
	m, backend, cleanup := setupTestManager(t)
	defer cleanup()
	m.ManifestManager = nil
	ctx := context.Background()
	for _, p := range []string{"db1/cpu/2026/01/01/00/a.parquet", "db2/cpu/2026/01/01/00/a.parquet"} {
		if err := backend.Write(ctx, p, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	var listed []string
	m.StorageBackend = &listingBackend{Backend: backend, onList: func(prefix string) {
		if prefix == "" {
			return // the database listing
		}
		listed = append(listed, prefix)
		m.BeginShutdown() // shutdown begins while the first database is listed
	}}
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) {
		t.Error("discovery ran after shutdown began")
		return nil, nil
	}}}
	_, err := m.RunCompactionCycleForTiers(ctx, []string{"hourly"})
	if !errors.Is(err, ErrCompactionStopping) {
		t.Fatalf("err = %v, want ErrCompactionStopping", err)
	}
	if len(listed) != 1 || !strings.HasPrefix(listed[0], "db1") {
		t.Fatalf("databases listed after the stop: %v, want only db1", listed)
	}
}

// A split stops before its first half, and before its second: neither is a
// failure or a too-wide strike, and the cycle ends "stopped".
func TestShutdownStopsASplitBetweenItsHalves1887(t *testing.T) {
	for _, tc := range []struct {
		name         string
		stopAtFiles  int // BeginShutdown during the attempt with this many files
		wantAttempts int32
	}{
		{"before the first half", 4, 1},
		{"before the second half", 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := []string{"a.parquet", "b.parquet", "c.parquet", "d.parquet"}
			var m *Manager
			m, attempts := wideManager(t, &files, func(c Candidate) error {
				if len(c.Files) == tc.stopAtFiles {
					m.BeginShutdown()
				}
				if len(c.Files) == 4 {
					return errMemory() // recoverable: split
				}
				return nil
			})
			rec := wideCycle(t, m)
			if attempts.Load() != tc.wantAttempts {
				t.Fatalf("attempts = %d, want %d", attempts.Load(), tc.wantAttempts)
			}
			if rec.Status != "stopped" || rec.Failed != 0 {
				t.Fatalf("status %q failed %d, want stopped and 0", rec.Status, rec.Failed)
			}
			if len(m.TooWidePartitions()) != 0 {
				t.Fatal("a split stopped for shutdown struck the partition as too wide")
			}
		})
	}
}

// StopDispatchAll never blocks on a Stop already holding the scheduler (an
// OnLoseCompactor stop waiting for its cycle): the hook runs first of all.
func TestStopDispatchAllDoesNotBlockOnAStopInProgress1887(t *testing.T) {
	s := &Scheduler{}
	s.mu.Lock() // a Stop waiting for its cycle under the lock
	defer s.mu.Unlock()
	done := make(chan struct{})
	go func() { StopDispatchAll(s, nil); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("StopDispatchAll blocked on a scheduler whose Stop holds its lock")
	}
}
