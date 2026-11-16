package compaction

import (
	"context"
	"testing"
	"time"
)

// #1722: the measurement a compaction replaced files in is reported once its
// inputs are gone and its output is in place, so the Iceberg export can catch
// up then rather than at its next timed pass.

type settledRecorder struct{ got []string }

func (r *settledRecorder) record(db, m string) { r.got = append(r.got, db+"/"+m) }

// A legacy job: reported once its output is in place, also when its delete
// failed part-way (the export then publishes the output in place of every
// input, the deleted ones included); not when the job failed or wrote
// nothing.
func TestLegacyJobReportsItsMeasurementOnceItsOutputIsInPlace(t *testing.T) {
	const out = "db/cpu/2026/10/13/10/cpu_compacted.parquet"
	for _, tc := range []struct {
		name string
		res  *SubprocessJobResult
		want int
	}{
		{"sources deleted", &SubprocessJobResult{Success: true, FilesCompacted: 2, OutputFile: out, SourcesDeleted: true}, 1},
		{"delete failed part-way", &SubprocessJobResult{Success: true, FilesCompacted: 2, OutputFile: out}, 1},
		{"nothing written", &SubprocessJobResult{Success: true}, 0},
		{"job failed", &SubprocessJobResult{Success: false, Error: "boom"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := activeJobsRig(t, localBackend(t))
			rec := &settledRecorder{}
			m.SetOnMeasurementCompacted(rec.record)
			runStubbed(t, m, context.Background(), tc.res, nil, nil)
			if len(rec.got) != tc.want || (tc.want == 1 && rec.got[0] != "db/cpu") {
				t.Fatalf("reported %v, want %d report(s) of db/cpu", rec.got, tc.want)
			}
		})
	}
}

// A local staged job: reported at its commit, once the inputs are removed and
// the output moved into place, not when it is staged.
func TestLocalStagedJobReportsItsMeasurementAtTheCommit(t *testing.T) {
	r := newStagedRig(t)
	rec := &settledRecorder{}
	r.w.SetStagedSettled(rec.record)
	r.poll()
	r.poll()
	if len(rec.got) != 0 {
		t.Fatalf("reported before the commit: %v", rec.got)
	}
	r.fc.status.Holders = 1
	r.poll()
	if len(rec.got) != 1 || rec.got[0] != "db/cpu" {
		t.Fatalf("reported %v, want db/cpu once", rec.got)
	}
	for _, in := range r.job.Inputs {
		if r.exists(in.Path) {
			t.Fatalf("reported with input %s still present", in.Path)
		}
	}
}

// An object staged job: not at the commit (the finisher copies and deletes
// after it), but when the cluster ended the job.
func TestObjectStagedJobReportsItsMeasurementWhenFinished(t *testing.T) {
	r := newObjectStagedRig(t, time.Now())
	rec := &settledRecorder{}
	r.w.SetStagedSettled(rec.record)
	r.poll()
	if len(r.committed) != 1 || len(rec.got) != 0 {
		t.Fatalf("committed %d, reported %v: want committed and not yet reported", len(r.committed), rec.got)
	}
	r.poll() // still finalizing
	if len(rec.got) != 0 {
		t.Fatalf("reported while finalizing: %v", rec.got)
	}
	r.fc.status = StagedStatus{Committed: true}
	r.poll()
	if len(rec.got) != 1 || rec.got[0] != "db/cpu" {
		t.Fatalf("reported %v, want db/cpu once the finisher ended the job", rec.got)
	}
}
