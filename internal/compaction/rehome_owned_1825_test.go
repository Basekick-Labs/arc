package compaction

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
)

// Files a re-home job in flight owns stay out of a compaction job: dropped
// when the candidate is filtered, and the job gives up, unregistering
// itself, when a re-home job began between that filter and its start.
func TestCompactionLeavesFilesARehomeJobOwns1825(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	part := "db/cpu/2026/01/01/03"
	owned := part + "/owned.parquet"
	files := []string{part + "/a.parquet", part + "/b.parquet", owned}
	for _, f := range files {
		if err := b.Write(ctx, f, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) {
		c := candidateIssue915(part)
		c.Files, c.FileCount = append([]string(nil), files...), len(files)
		return []Candidate{c}, nil
	}}}
	old := runJobInSubprocess
	t.Cleanup(func() { runJobInSubprocess = old })
	var jobs [][]string
	runJobInSubprocess = func(_ context.Context, cfg *SubprocessJobConfig, _ zerolog.Logger) (*SubprocessJobResult, error) {
		jobs = append(jobs, append([]string(nil), cfg.Files...))
		return &SubprocessJobResult{Success: true, FilesCompacted: len(cfg.Files), CompactedInputs: cfg.Files, SourcesDeleted: true}, nil
	}

	m.SetOwnedFilter(func(p string) bool { return p == owned })
	_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	if len(jobs) != 1 || len(jobs[0]) != 2 || jobs[0][0] == owned || jobs[0][1] == owned {
		t.Fatalf("jobs = %v, want one job of the two files no re-home job owns", jobs)
	}

	// A re-home job that began after the candidate was filtered: the
	// compaction job sees it once it has registered, runs nothing, and is
	// not left open on the measurement (the re-home job would refuse on it).
	jobs = nil
	m.SetOwnedFilter(func(string) bool { return true })
	c := candidateIssue915(part)
	c.Files, c.FileCount = files, len(files)
	if err := m.compactPartition(ctx, c, jobAttribution{}); err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("a job ran on files a re-home job owns: %v", jobs)
	}
	if m.OpenJobs.Open("db/cpu/") {
		t.Fatal("the compaction job that gave up is still open on the measurement")
	}
	// With no re-home job the same candidate compacts.
	m.SetOwnedFilter(nil)
	if err := m.compactPartition(ctx, c, jobAttribution{}); err != nil || len(jobs) != 1 || len(jobs[0]) != 3 {
		t.Fatalf("with no re-home job: jobs %v, err %v", jobs, err)
	}
}
