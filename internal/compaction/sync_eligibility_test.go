package compaction

// Regression tests for the issue-#610 sync eligibility gate: compaction on a
// syncing spoke may only consume files the edge sync ledger reports
// delivered, deferring the rest.

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
)

type stubTier struct {
	Tier
	minFiles int
}

func (s stubTier) GetMinFiles() int { return s.minFiles }

func gateTestManager(t *testing.T) *Manager {
	t.Helper()
	return &Manager{logger: zerolog.Nop()}
}

// No hook installed: candidates pass through untouched — edge sync absent or
// the operator opted out must be byte-identical to today's behavior.
func TestFilterSyncEligibility_NilHookPassesThrough(t *testing.T) {
	m := gateTestManager(t)
	c := Candidate{Files: []string{"a.parquet", "b.parquet"}, FileCount: 2}

	got, ok := m.filterSyncEligibility(context.Background(), c, stubTier{minFiles: 10})
	if !ok || len(got.Files) != 2 {
		t.Fatalf("nil hook altered the candidate: ok=%v files=%d", ok, len(got.Files))
	}
}

// Undelivered files are dropped and the TIER's MinFiles re-applies to the
// remainder — hourly (10) and daily (12) differ, so a single manager-wide
// threshold would compact partitions a tier would not.
func TestFilterSyncEligibility_FiltersAndReappliesTierMinFiles(t *testing.T) {
	m := gateTestManager(t)
	files := make([]string, 12)
	eligible := map[string]bool{}
	for i := range files {
		files[i] = string(rune('a'+i)) + ".parquet"
		eligible[files[i]] = i < 11 // 11 of 12 delivered
	}
	m.SetSyncEligibility(func(ctx context.Context, paths []string) (map[string]bool, error) {
		return eligible, nil
	})

	c := Candidate{Files: files, FileCount: len(files)}

	// Hourly (MinFiles 10): 11 eligible ≥ 10 → proceeds with the subset.
	got, ok := m.filterSyncEligibility(context.Background(), c, stubTier{minFiles: 10})
	if !ok {
		t.Fatal("hourly candidate deferred; 11 eligible files clear MinFiles 10")
	}
	if len(got.Files) != 11 || got.FileCount != 11 {
		t.Fatalf("files = %d (count %d), want 11", len(got.Files), got.FileCount)
	}

	// Daily (MinFiles 12): 11 eligible < 12 → the partition defers.
	if _, ok := m.filterSyncEligibility(context.Background(), c, stubTier{minFiles: 12}); ok {
		t.Fatal("daily candidate proceeded with 11 eligible files below MinFiles 12")
	}
}

// A hook error defers the whole partition: the fail-safe direction is to
// compact nothing rather than risk consuming undelivered rows.
func TestFilterSyncEligibility_ErrorDefersEverything(t *testing.T) {
	m := gateTestManager(t)
	m.SetSyncEligibility(func(ctx context.Context, paths []string) (map[string]bool, error) {
		return nil, errors.New("ledger unavailable")
	})

	c := Candidate{Files: []string{"a.parquet"}, FileCount: 1}
	if _, ok := m.filterSyncEligibility(context.Background(), c, stubTier{minFiles: 1}); ok {
		t.Fatal("a failing eligibility lookup must defer, never proceed")
	}
}

// The output observer receives the storage key from a successful result and
// ignores empty keys (zero-output successes).
func TestNotifyCompactedOutput(t *testing.T) {
	m := gateTestManager(t)
	var got []string
	m.SetOnCompactedOutput(func(key string) { got = append(got, key) })

	m.notifyCompactedOutput("db/cpu/2026/08/07/14/out_compacted.parquet")
	m.notifyCompactedOutput("")
	if len(got) != 1 || got[0] != "db/cpu/2026/08/07/14/out_compacted.parquet" {
		t.Fatalf("observer calls = %v", got)
	}
}

// Both tiers expose their MinFiles through the interface.
func TestTiersExposeMinFiles(t *testing.T) {
	if got := NewHourlyTier(&HourlyTierConfig{}).GetMinFiles(); got != 10 {
		t.Errorf("hourly default = %d, want 10", got)
	}
	if got := NewDailyTier(&DailyTierConfig{}).GetMinFiles(); got != 12 {
		t.Errorf("daily default = %d, want 12", got)
	}
}
