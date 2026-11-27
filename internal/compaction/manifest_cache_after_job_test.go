package compaction

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// A job that cannot delete its inputs leaves its manifest in storage, written
// by the subprocess's own ManifestManager. The parent's candidate filter must
// see that manifest on its next use, not up to a cache lifetime later: until
// then the inputs it protects are free to be compacted again, together with
// the output that already holds their rows.
func TestCandidateFilterSeesAManifestTheSubprocessLeftBehind(t *testing.T) {
	m := activeJobsRig(t, localBackend(t))
	ctx := context.Background()
	backend := m.StorageBackend

	// The parent's remembered set must be non-empty to be served from memory
	// at all, so another job's manifest is already there.
	if _, err := m.ManifestManager.WriteManifest(ctx, &Manifest{
		OutputPath: "db/mem/2026/10/13/09/out.parquet",
		InputFiles: []string{"db/mem/2026/10/13/09/x.parquet"},
		Database:   "db", Measurement: "mem", Tier: "hourly", Status: ManifestStatusPending,
		CreatedAt: time.Now().UTC(), JobID: "other-job",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ManifestManager.GetFilesInManifests(ctx); err != nil {
		t.Fatal(err)
	}

	left := []string{"db/cpu/2026/10/13/10/a.parquet", "db/cpu/2026/10/13/10/b.parquet"}
	runStubbed(t, m, ctx, &SubprocessJobResult{Success: true, FilesCompacted: 2}, nil, func(c *SubprocessJobConfig) {
		// What the subprocess does when its source deletion fails: the
		// manifest stays, written through a manager of its own.
		sub := NewManifestManager(backend, zerolog.Nop())
		if _, err := sub.WriteManifest(ctx, &Manifest{
			OutputPath: "db/cpu/2026/10/13/10/out.parquet",
			InputFiles: left,
			Database:   "db", Measurement: "cpu", Tier: "hourly", Status: ManifestStatusPending,
			CreatedAt: time.Now().UTC(), JobID: c.JobID,
		}); err != nil {
			t.Error(err)
		}
	})

	// The daily tier's view of that day: the same hour files.
	daily := Candidate{Database: "db", Measurement: "cpu", PartitionPath: "cpu/2026/10/13", Tier: "daily",
		Files: append([]string{"db/cpu/2026/10/13/11/c.parquet"}, left...)}
	filtered, _, err := m.filterCandidateFilesWithError(ctx, daily)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range filtered.Files {
		for _, held := range left {
			if f == held {
				t.Errorf("%s is still a compaction candidate although a manifest holds it", f)
			}
		}
	}
	if len(filtered.Files) != 1 {
		t.Errorf("filtered files = %v, want only the file no manifest holds", filtered.Files)
	}
}
