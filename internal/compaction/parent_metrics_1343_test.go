package compaction

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// #1343: the parent counts each attempt's outcome, and the storage I/O the
// subprocess did, from its result; the subprocess's own registry is never
// scraped.

func snap() map[string]int64 {
	out := map[string]int64{}
	for k, v := range metrics.Get().Snapshot() {
		if n, ok := v.(int64); ok {
			out[k] = n
		}
	}
	return out
}

func delta(before, after map[string]int64, key string) int64 { return after[key] - before[key] }

func runStubbed(t *testing.T, m *Manager, ctx context.Context, res *SubprocessJobResult, err error, inspect func(*SubprocessJobConfig)) {
	t.Helper()
	backend := m.StorageBackend
	files := []string{"db/cpu/2026/10/13/10/a.parquet", "db/cpu/2026/10/13/10/b.parquet"}
	for _, f := range files {
		if werr := backend.Write(context.Background(), f, []byte("12345")); werr != nil {
			t.Fatal(werr)
		}
	}
	old := runJobInSubprocess
	t.Cleanup(func() { runJobInSubprocess = old })
	runJobInSubprocess = func(_ context.Context, c *SubprocessJobConfig, _ zerolog.Logger) (*SubprocessJobResult, error) {
		if inspect != nil {
			inspect(c)
		}
		return res, err
	}
	_ = m.compactPartition(ctx, Candidate{Database: "db", Measurement: "cpu", PartitionPath: "cpu/2026/10/13/10", Files: files, BatchNumber: 1, Tier: "hourly"}, jobAttribution{})
}

func TestParentCountsJobOutcomes(t *testing.T) {
	cases := []struct {
		name   string
		res    *SubprocessJobResult
		err    error
		cancel bool
		want   map[string]int64
	}{
		{"success", &SubprocessJobResult{Success: true, FilesCompacted: 2, BytesBefore: 10, BytesAfter: 4,
			Metrics: map[string]int64{"storage_writes": 3, "storage_write_bytes": 40, "unknown_key": 9}},
			nil, false, map[string]int64{"compaction_jobs_total": 1, "compaction_jobs_success": 1, "compaction_files_compacted": 2,
				"compaction_bytes_read": 10, "compaction_bytes_written": 4, "storage_writes_total": 3, "storage_write_bytes_total": 40}},
		{"failure", &SubprocessJobResult{Success: false, Error: "boom", Metrics: map[string]int64{"storage_errors": 1}},
			nil, false, map[string]int64{"compaction_jobs_total": 1, "compaction_jobs_failed": 1, "storage_errors_total": 1}},
		{"crash, no result", nil, errors.New("exit status 2"), false,
			map[string]int64{"compaction_jobs_total": 1, "compaction_jobs_failed": 1}},
		{"killed for a cancelled cycle", nil, context.Canceled, true,
			map[string]int64{"compaction_jobs_total": 1, "compaction_jobs_interrupted": 1}},
		{"stopped by the same signal, reported a failure", &SubprocessJobResult{Success: false, Error: "context canceled"}, nil, true,
			map[string]int64{"compaction_jobs_total": 1, "compaction_jobs_interrupted": 1, "compaction_jobs_failed": 0}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := activeJobsRig(t, localBackend(t))
			ctx := context.Background()
			if c.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			var before map[string]int64
			// Taken as the "subprocess" runs: the rig's own writes are not
			// the job's.
			runStubbed(t, m, ctx, c.res, c.err, func(*SubprocessJobConfig) { before = snap() })
			after := snap()
			for k, want := range c.want {
				if got := delta(before, after, k); got != want {
					t.Errorf("%s moved by %d, want %d", k, got, want)
				}
			}
		})
	}
}

func writeRecord(t *testing.T, dir, jobID string, state CompletionState) string {
	t.Helper()
	if err := writeCompletionManifest(dir, &CompletionManifest{JobID: jobID, State: state}); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, jobID+".json")
}

// A failed attempt's writing_output record is dropped once the subprocess
// has exited; any other state, or one whose recovery manifest survives, is
// kept.
func TestDeadCompletionRecordIsDropped(t *testing.T) {
	for _, c := range []struct {
		name     string
		state    CompletionState
		staged   bool
		manifest bool
		kept     bool
	}{
		{"writing_output, failed", CompletionStateWritingOutput, false, false, false},
		{"writing_output, staged", CompletionStateWritingOutput, true, false, false},
		{"writing_output, recovery manifest survives", CompletionStateWritingOutput, false, true, true},
		{"output_written", CompletionStateOutputWritten, false, false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			backend := localBackend(t)
			m := activeJobsRig(t, backend)
			m.CompletionDir = t.TempDir()
			var path string
			runStubbed(t, m, context.Background(), &SubprocessJobResult{Success: false, Error: "boom"}, nil, func(cfg *SubprocessJobConfig) {
				cfg.Staged = c.staged
				path = writeRecord(t, cfg.CompletionDir, cfg.JobID, c.state)
				if err := os.WriteFile(path+".tmp", []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
				if c.manifest {
					mp := m.ManifestManager.GenerateManifestPath("hourly", "db", "cpu/2026/10/13/10", cfg.JobID)
					if err := backend.Write(context.Background(), mp, []byte("{}")); err != nil {
						t.Fatal(err)
					}
				}
			})
			_, err := os.Stat(path)
			if kept := err == nil; kept != c.kept {
				t.Fatalf("record kept=%v, want %v", kept, c.kept)
			}
			if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
				t.Error("the killed rename's temporary file was left")
			}
		})
	}
}
