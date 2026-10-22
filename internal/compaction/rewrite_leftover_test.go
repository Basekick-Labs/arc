package compaction

import (
	"context"
	"sync"
	"testing"
)

// A file a DELETE rewrote whose old copy is still in storage is not
// compacted, through a real cycle: compacting both would put the surviving
// rows in the output twice, for good.
func TestCompactionCycleLeavesRewriteLeftoversOut(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()
	manager.ManifestManager = nil
	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(context.Context, string, string) ([]Candidate, error) {
			return []Candidate{
				{Database: "db", Measurement: "cpu", PartitionPath: "big", Tier: "hourly",
					Files: []string{"h/a.parquet", "h/b.parquet", "h/x.parquet", "h/x_rewrite_5.parquet"}, FileCount: 4},
				{Database: "db", Measurement: "cpu", PartitionPath: "small", Tier: "hourly",
					Files: []string{"g/c.parquet", "g/y.parquet", "g/y_rewrite_7.parquet"}, FileCount: 3},
			}, nil
		},
	}}
	var mu sync.Mutex
	var batches []Candidate
	manager.compactBatchForTest = func(_ context.Context, c Candidate) error {
		mu.Lock()
		batches = append(batches, c)
		mu.Unlock()
		return nil
	}
	run := func() {
		batches = nil
		if _, err := manager.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"}); err != nil {
			t.Fatal(err)
		}
	}
	run()
	if len(batches) != 1 || batches[0].PartitionPath != "big" {
		t.Fatalf("batches %+v; want only the partition still at MinFiles", batches)
	}
	if f := batches[0].Files; len(f) != 2 || f[0] != "h/a.parquet" || f[1] != "h/b.parquet" {
		t.Fatalf("compacted %v; a rewrite family with two members reached a batch", f)
	}

	// Staged jobs compact only manifest-listed files: left alone.
	manager.SetStagedCommit(func() bool { return true }, nil, nil)
	run()
	if len(batches) != 2 {
		t.Fatalf("staged mode: batches %+v, want both partitions untouched by the filter", batches)
	}
}
