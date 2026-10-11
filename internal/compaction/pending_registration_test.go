package compaction

// #1212: compacting a file still pending cluster registration races its
// registration, so pending files wait a cycle.

import (
	"context"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

func TestDropPendingKeepsPendingFilesOutOfCandidates(t *testing.T) {
	m := NewManager(&ManagerConfig{Logger: zerolog.Nop()})
	objects := []string{"db/m/2026/10/11/10/a.parquet", "db/m/2026/10/11/10/b.parquet", "db/m/2026/10/11/10/c.parquet"}
	if got := m.dropPending(objects); len(got) != 3 {
		t.Fatalf("no filter set: kept %d of 3", len(got))
	}
	m.SetPendingFilter(func(p string) bool { return p == "db/m/2026/10/11/10/b.parquet" })
	got := m.dropPending(objects)
	if len(got) != 2 || got[0] != objects[0] || got[1] != objects[2] {
		t.Fatalf("kept %v, want a and c", got)
	}
	if len(objects) != 3 || objects[1] != "db/m/2026/10/11/10/b.parquet" {
		t.Fatal("dropPending modified the caller's listing")
	}
}

// Through the real candidate search: a pending file is never in a candidate.
func TestFindCandidatesLeavesPendingFilesOut(t *testing.T) {
	backend := newCountingBackend(t)
	writeHourFiles(t, backend, "db1", "m1", 7)
	m := newTwoTierManager(t, backend, 5)
	pending := map[string]bool{
		"db1/m1/2025/01/05/10/m1_20250105_100000_000000000.parquet": true,
		"db1/m1/2025/01/05/10/m1_20250105_100001_000000001.parquet": true,
	}
	m.SetPendingFilter(func(p string) bool { return pending[p] })

	candidates, err := m.FindCandidates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) == 0 {
		t.Fatal("five settled files should still make candidates")
	}
	for _, c := range candidates {
		for _, f := range c.Files {
			if pending[f] {
				t.Fatalf("%s candidate includes pending file %s", c.Tier, f)
			}
		}
	}
}

// Through a real compaction cycle, not the preview: a pending file never
// reaches a batch, and a partition left under MinFiles waits a cycle.
func TestCompactionCycleLeavesPendingFilesOut(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()
	manager.ManifestManager = nil
	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(context.Context, string, string) ([]Candidate, error) {
			return []Candidate{
				{Database: "db", Measurement: "cpu", PartitionPath: "big", Tier: "hourly",
					Files: []string{"a.parquet", "b.parquet", "pending.parquet"}, FileCount: 3},
				{Database: "db", Measurement: "cpu", PartitionPath: "small", Tier: "hourly",
					Files: []string{"c.parquet", "pending2.parquet"}, FileCount: 2},
			}, nil
		},
	}}
	manager.SetPendingFilter(func(p string) bool { return p == "pending.parquet" || p == "pending2.parquet" })

	var mu sync.Mutex
	var batches []Candidate
	manager.compactBatchForTest = func(_ context.Context, c Candidate) error {
		mu.Lock()
		batches = append(batches, c)
		mu.Unlock()
		return nil
	}
	if _, err := manager.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"}); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 1 || batches[0].PartitionPath != "big" {
		t.Fatalf("batches %+v; want only the partition still at MinFiles", batches)
	}
	for _, f := range batches[0].Files {
		if f == "pending.parquet" {
			t.Fatal("a pending file reached a compaction batch")
		}
	}
}
