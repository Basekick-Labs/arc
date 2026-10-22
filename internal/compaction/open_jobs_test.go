package compaction

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func openJobsRig(t *testing.T) (*OpenJobs, storage.Backend) {
	t.Helper()
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return NewOpenJobs(b), b
}

func excluded(o *OpenJobs, prefix string) []string {
	out := o.ExcludedFor(context.Background(), prefix)
	sort.Strings(out)
	return out
}

// Before the output exists the output is left out; once it exists, the
// inputs the manifest lists (not the parent's candidates: a corrupted input
// the job skipped stays readable); a finished job leaves nothing out.
func TestOpenJobsExcludesOneSide(t *testing.T) {
	o, b := openJobsRig(t)
	ctx := context.Background()
	out := "db/m/2026/10/20/10/m_x_b1_compacted.parquet"
	man := "_compaction_state/hourly/db/job1.json"
	in1, in2, bad := "db/m/2026/10/20/10/a.parquet", "db/m/2026/10/20/10/b.parquet", "db/m/2026/10/20/10/corrupt.parquet"
	o.register(out, man, []string{in1, in2, bad})
	g0 := o.Generation()

	if got := excluded(o, "db/m/"); len(got) != 1 || got[0] != out {
		t.Fatalf("output absent: excluded %v, want the output", got)
	}
	if got := excluded(o, "db/other/"); len(got) != 0 {
		t.Fatalf("another measurement: excluded %v, want none", got)
	}
	data, _ := json.Marshal(&Manifest{OutputPath: out, InputFiles: []string{in1, in2}})
	if err := b.Write(ctx, man, data); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(ctx, out, []byte("parquet")); err != nil {
		t.Fatal(err)
	}
	if got := excluded(o, "db/m/"); len(got) != 2 || got[0] != in1 || got[1] != in2 {
		t.Fatalf("output present: excluded %v, want the manifest's inputs only", got)
	}
	// The subprocess reports its inputs deleted: the job is over.
	o.finish(out, true)
	if got := excluded(o, "db/m/"); len(got) != 0 {
		t.Fatalf("finished: excluded %v, want none", got)
	}
	if o.Generation() == g0 {
		t.Fatal("generation unchanged after the set changed")
	}
}

// A failed delete keeps the entry until its manifest is gone; a manifest
// found in storage (an earlier process, a crash) becomes an entry.
func TestOpenJobsSync(t *testing.T) {
	o, b := openJobsRig(t)
	ctx := context.Background()
	out := "db/m/2026/10/20/10/m_y_b1_compacted.parquet"
	man := "_compaction_state/hourly/db/job2.json"
	in := "db/m/2026/10/20/10/a.parquet"
	o.register(out, man, []string{in})
	_ = b.Write(ctx, out, []byte("parquet"))
	o.finish(out, false) // inputs not all deleted
	m := &Manifest{OutputPath: out, InputFiles: []string{in}}
	o.sync(map[string]*Manifest{man: m})
	if got := excluded(o, "db/m/"); len(got) != 1 || got[0] != in {
		t.Fatalf("after a failed delete: excluded %v, want the input", got)
	}
	o.sync(map[string]*Manifest{}) // recovery deleted the inputs and the manifest
	if got := excluded(o, "db/m/"); len(got) != 0 {
		t.Fatalf("after recovery: excluded %v, want none", got)
	}
	// Seeded from a surviving manifest at startup.
	o2, b2 := openJobsRig(t)
	_ = b2.Write(ctx, out, []byte("parquet"))
	o2.sync(map[string]*Manifest{man: m})
	if got := excluded(o2, "db/m/"); len(got) != 1 || got[0] != in {
		t.Fatalf("seeded: excluded %v, want the input", got)
	}
	// A running job without a manifest yet survives a sync.
	o3, _ := openJobsRig(t)
	o3.register(out, man, []string{in})
	o3.sync(map[string]*Manifest{})
	if got := excluded(o3, "db/m/"); len(got) != 1 || got[0] != out {
		t.Fatalf("running job dropped by sync: excluded %v", got)
	}
}

// The parent's output name and the job's are the same function.
func TestCompactionOutputNameShape(t *testing.T) {
	n := CompactionOutputName("cpu", "hourly", 3, mustTime("2026-10-20T10:00:00Z"))
	if want := "_b3_compacted.parquet"; len(n) < len(want) || n[len(n)-len(want):] != want {
		t.Fatalf("name %q", n)
	}
	if d := CompactionOutputName("cpu", "daily", 1, mustTime("2026-10-20T10:00:00Z")); d[len(d)-len("_b1_daily.parquet"):] != "_b1_daily.parquet" {
		t.Fatalf("daily name %q", d)
	}
}

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// Each cycle drops a finished job once recovery has removed its manifest,
// without listing.
func TestOpenJobsPruneFinished(t *testing.T) {
	o, b := openJobsRig(t)
	ctx := context.Background()
	out := "db/m/2026/10/20/10/m_z_b1_compacted.parquet"
	man := "_compaction_state/hourly/db/job3.json"
	in := "db/m/2026/10/20/10/a.parquet"
	_ = b.Write(ctx, man, []byte(`{"output_path":"`+out+`","input_files":["`+in+`"]}`))
	_ = b.Write(ctx, out, []byte("parquet"))
	o.register(out, man, []string{in})
	o.finish(out, false)
	o.pruneFinished(ctx)
	if got := excluded(o, "db/m/"); len(got) != 1 {
		t.Fatalf("manifest still there: excluded %v, want the input", got)
	}
	_ = b.Delete(ctx, man)
	o.pruneFinished(ctx)
	if got := excluded(o, "db/m/"); len(got) != 0 {
		t.Fatalf("manifest gone: excluded %v, want none", got)
	}
	// A running job is never pruned (its manifest may not be written yet):
	// with its output not written either, the output is left out.
	_ = b.Delete(ctx, out)
	o.register(out, man, []string{in})
	o.pruneFinished(ctx)
	if got := excluded(o, "db/m/"); len(got) != 1 || got[0] != out {
		t.Fatalf("running job pruned: excluded %v", got)
	}
}
