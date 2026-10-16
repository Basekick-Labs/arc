package compaction

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// #1207 d2a2a: the manager's side of the staged commit.

func TestStagedCandidateFilter(t *testing.T) {
	backend := localBackend(t)
	m := activeJobsRig(t, backend)
	ctx := context.Background()
	files := []string{"db/cpu/2026/10/13/10/a.parquet", "db/cpu/2026/10/13/10/b.parquet", "db/cpu/2026/10/13/10/c.parquet",
		"db/cpu/2026/10/13/10/d.parquet", "db/cpu/2026/10/13/10/e.parquet"}
	for _, f := range files {
		if err := backend.Write(ctx, f, []byte("12345")); err != nil {
			t.Fatal(err)
		}
	}
	cand := Candidate{Database: "db", PartitionPath: "cpu/2026/10/13/10", Files: files}
	if staged, _, _ := m.stagedCandidate(ctx, cand); staged {
		t.Fatal("staged with nothing wired")
	}
	on := false
	m.SetStagedCommit(func() bool { return on },
		func(p string) (int64, string, bool) {
			switch p {
			case files[1]:
				return 5, "", false // not registered, or owned by a staged job
			case files[2]:
				return 4, "sha-c", true // registered at another size than here
			}
			return 5, "sha-" + p[len(p)-9:len(p)-8], true
		},
		func(p string) bool { return p == files[3] }) // staged here, not yet in the cluster
	if staged, _, _ := m.stagedCandidate(ctx, cand); staged {
		t.Fatal("staged with the mode off")
	}
	on = true
	staged, kept, inputs := m.stagedCandidate(ctx, cand)
	if !staged || len(kept) != 2 || kept[0] != files[0] || kept[1] != files[4] || len(inputs) != 2 ||
		inputs[files[0]] != (StagedInput{Path: files[0], SizeBytes: 5, SHA256: "sha-a"}) {
		t.Fatalf("staged %v kept %v inputs %v", staged, kept, inputs)
	}

	// Fewer than two eligible files: no job at all.
	old := runJobInSubprocess
	t.Cleanup(func() { runJobInSubprocess = old })
	runJobInSubprocess = func(context.Context, *SubprocessJobConfig, zerolog.Logger) (*SubprocessJobResult, error) {
		t.Fatal("a job ran with one eligible file")
		return nil, nil
	}
	only := Candidate{Database: "db", PartitionPath: "cpu/2026/10/13/10", Files: files[:2], BatchNumber: 1}
	if err := m.compactPartition(ctx, only, jobAttribution{}); err != nil {
		t.Fatalf("compactPartition: %v", err)
	}
	if n := m.totalJobsCompleted + m.totalJobsFailed; n != 0 {
		t.Fatalf("a job ran: %d", n)
	}
}

func TestFinishStagedRunsTheObservers(t *testing.T) {
	m := activeJobsRig(t, localBackend(t))
	var ev []string
	m.SetOnCompactedOutput(func(k string) { ev = append(ev, "output "+k) })
	fail := true
	m.SetOnConsumedInputs(func(in []string) error {
		ev = append(ev, "consumed")
		if fail {
			return errors.New("ledger down")
		}
		return nil
	})
	m.SetOnCompactionComplete(func() { ev = append(ev, "complete") })
	m.BeforeStagedCommit("o.parquet")
	if err := m.FinishStaged([]string{"a", "b"}); err == nil {
		t.Fatal("a failed receipt marking reported success")
	}
	fail = false
	if err := m.FinishStaged([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if got := len(ev); got != 4 || ev[0] != "output o.parquet" || ev[1] != "consumed" || ev[3] != "complete" {
		t.Fatalf("events %v", ev)
	}
}

// The staged branch of compactPartition, through the subprocess seam: the job
// runs staged with the inputs as the cluster lists them, no crash-recovery
// manifest is handed to the parent, and the observers wait for the commit.
func TestCompactPartitionStaged(t *testing.T) {
	backend := localBackend(t)
	m := activeJobsRig(t, backend)
	ctx := context.Background()
	files := []string{"db/cpu/2026/10/13/10/a.parquet", "db/cpu/2026/10/13/10/b.parquet"}
	for _, f := range files {
		if err := backend.Write(ctx, f, []byte("12345")); err != nil {
			t.Fatal(err)
		}
	}
	var got *SubprocessJobConfig
	old := runJobInSubprocess
	t.Cleanup(func() { runJobInSubprocess = old })
	runJobInSubprocess = func(_ context.Context, c *SubprocessJobConfig, _ zerolog.Logger) (*SubprocessJobResult, error) {
		got = c
		return &SubprocessJobResult{Success: true, FilesCompacted: 2, OutputFile: "db/cpu/2026/10/13/10/o.parquet", CompactedInputs: files}, nil
	}
	var observed []string
	m.SetOnCompactedOutput(func(k string) { observed = append(observed, "output") })
	m.SetOnConsumedInputs(func([]string) error { observed = append(observed, "consumed"); return nil })
	m.SetOnCompactionComplete(func() { observed = append(observed, "complete") })
	m.SetStagedCommit(func() bool { return true },
		func(p string) (int64, string, bool) { return 5, "sha", true }, nil)
	cand := Candidate{Database: "db", PartitionPath: "cpu/2026/10/13/10", Files: files, BatchNumber: 1}
	if err := m.compactPartition(ctx, cand, jobAttribution{}); err != nil {
		t.Fatal(err)
	}
	if got == nil || !got.Staged || len(got.Inputs) != 2 || got.Inputs[files[0]].SHA256 != "sha" || got.ParentFinalizesManifest {
		t.Fatalf("job config %+v", got)
	}
	if len(observed) != 0 {
		t.Fatalf("observers ran before any commit: %v", observed)
	}
	// Mode off: the legacy job, observers at once.
	m.SetStagedCommit(func() bool { return false }, nil, nil)
	if err := m.compactPartition(ctx, cand, jobAttribution{}); err != nil {
		t.Fatal(err)
	}
	if got.Staged || got.Inputs != nil || !got.ParentFinalizesManifest || len(observed) != 3 {
		t.Fatalf("legacy job config %+v, observers %v", got, observed)
	}
}

func TestBoundJobID(t *testing.T) {
	short := "db_cpu_2026_10_13_10_1_b1"
	if boundJobID(short) != short {
		t.Fatal("a short id changed")
	}
	long := strings.Repeat("d", 150) + "_" + strings.Repeat("m", 150) + "_1_b1"
	a, b := boundJobID(long), boundJobID(long+"x")
	if len(a) > maxJobIDLen || a == b || validateJobID(a) != nil {
		t.Fatalf("bounded %q (%d) vs %q", a, len(a), b)
	}
}

// A long database and partition give a job id the cluster accepts.
func TestCompactPartitionBoundsTheJobID(t *testing.T) {
	backend := localBackend(t)
	m := activeJobsRig(t, backend)
	ctx := context.Background()
	db := strings.Repeat("d", 60)
	part := strings.Repeat("m", 120) + "/2026/10/13/10"
	files := []string{db + "/" + part + "/a.parquet", db + "/" + part + "/b.parquet"}
	for _, f := range files {
		if err := backend.Write(ctx, f, []byte("12345")); err != nil {
			t.Fatal(err)
		}
	}
	var got string
	old := runJobInSubprocess
	t.Cleanup(func() { runJobInSubprocess = old })
	runJobInSubprocess = func(_ context.Context, c *SubprocessJobConfig, _ zerolog.Logger) (*SubprocessJobResult, error) {
		got = c.JobID
		return &SubprocessJobResult{Success: true}, nil
	}
	if err := m.compactPartition(ctx, Candidate{Database: db, PartitionPath: part, Files: files, BatchNumber: 1}, jobAttribution{}); err != nil {
		t.Fatal(err)
	}
	if got == "" || len(got) > maxJobIDLen {
		t.Fatalf("job id %q (%d)", got, len(got))
	}
}
