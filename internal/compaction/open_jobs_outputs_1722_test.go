package compaction

import (
	"context"
	"encoding/json"
	"testing"
)

// ExcludedWithOutputsStrict names, with the inputs it leaves out, the output
// they belong to, and only once that output is in storage (#1722): the
// export keeps exactly those outputs.
func TestOpenJobsNamesTheOutputOfTheInputsItLeavesOut(t *testing.T) {
	o, b := openJobsRig(t)
	ctx := context.Background()
	out := "db/m/2026/10/20/10/m_x_b1_compacted.parquet"
	man := "_compaction_state/hourly/db/job1.json"
	in1, in2 := "db/m/2026/10/20/10/a.parquet", "db/m/2026/10/20/10/b.parquet"
	o.register(out, man, []string{in1, in2})
	data, _ := json.Marshal(&Manifest{OutputPath: out, InputFiles: []string{in1, in2}})
	if err := b.Write(ctx, man, data); err != nil {
		t.Fatal(err)
	}

	keys, outs, err := o.ExcludedWithOutputsStrict(ctx, "db/m/")
	if err != nil || len(keys) != 0 || len(outs) != 0 {
		t.Fatalf("output not in storage: excluded %v, outputs %v, err %v; want nothing", keys, outs, err)
	}
	if err := b.Write(ctx, out, []byte("parquet")); err != nil {
		t.Fatal(err)
	}
	keys, outs, err = o.ExcludedWithOutputsStrict(ctx, "db/m/")
	if err != nil || len(keys) != 2 || len(outs) != 1 || outs[0] != out {
		t.Fatalf("output in storage: excluded %v, outputs %v, err %v; want both inputs and the output", keys, outs, err)
	}
	if keys, outs, _ = o.ExcludedWithOutputsStrict(ctx, "db/other/"); len(keys)+len(outs) != 0 {
		t.Fatalf("another measurement: %v %v, want nothing", keys, outs)
	}
	// The subprocess reports its inputs deleted: the job is over and nothing
	// is named any more.
	o.finish(out, true)
	if keys, outs, _ = o.ExcludedWithOutputsStrict(ctx, "db/m/"); len(keys)+len(outs) != 0 {
		t.Fatalf("job over: %v %v, want nothing", keys, outs)
	}
}
