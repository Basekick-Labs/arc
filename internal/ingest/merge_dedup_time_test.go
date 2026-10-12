package ingest

import "testing"

// The arc:dedup_time marker says a batch is one row per time. That is true of
// each batch on its own, not of a merge: a grouped continuous query without
// tag_columns marks a window that held one series and not one that held
// several. The merged flush keeps the marker only if every batch had it
// (#1227); before, any one marked batch marked the flush, and compaction then
// deduped the multi-series window on time.
func TestMergeBatches_DedupTimeRequiresEveryBatch(t *testing.T) {
	buffer := createTestArrowBuffer(t)
	tcb := func(times []int64, marked bool) *TypedColumnBatch {
		return &TypedColumnBatch{Data: map[string]interface{}{"time": times, "v": make([]float64, len(times))}, DedupTime: marked}
	}
	cases := []struct {
		name    string
		batches []interface{}
		want    bool
	}{
		{"single marked batch", []interface{}{tcb([]int64{1}, true)}, true},
		{"single unmarked batch", []interface{}{tcb([]int64{1}, false)}, false},
		{"every batch marked", []interface{}{tcb([]int64{1}, true), tcb([]int64{2}, true)}, true},
		{"marked then unmarked", []interface{}{tcb([]int64{1}, true), tcb([]int64{2, 2}, false)}, false},
		{"unmarked then marked", []interface{}{tcb([]int64{2, 2}, false), tcb([]int64{1}, true)}, false},
		// A bare map is a WAL-replayed batch: it carries no marker, so it
		// clears it — through the multi-batch loop, not the single-batch path.
		{"marked with a replayed map batch", []interface{}{
			tcb([]int64{1}, true),
			map[string]interface{}{"time": []int64{2}, "v": []float64{0}},
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			merged, err := buffer.mergeBatches(tc.batches)
			if err != nil {
				t.Fatalf("mergeBatches: %v", err)
			}
			if merged.DedupTime != tc.want {
				t.Errorf("DedupTime = %v, want %v", merged.DedupTime, tc.want)
			}
		})
	}
}

// Two marked emissions of the same window merged into one flush (a scheduled
// run and a manual execute, a retry) keep one row per time — the later one —
// so the flushed file is still truthfully one row per time and stays marked.
// Without this compaction would see a marked file with two rows per time,
// refuse to trust it, and keep both emissions for good (#1227).
func TestMergeBatches_MarkedReEmissionsKeepTheLaterRow(t *testing.T) {
	buffer := createTestArrowBuffer(t)
	first := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1, 2}, "v": []float64{10, 20}}, DedupTime: true,
		Validity: map[string][]bool{"v": {false, true}}}
	second := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{2, 3}, "v": []float64{21, 30}}, DedupTime: true}
	merged, err := buffer.mergeBatches([]interface{}{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if !merged.DedupTime {
		t.Fatal("merge of marked batches lost the marker")
	}
	times := merged.Data["time"].([]int64)
	vs := merged.Data["v"].([]float64)
	if len(times) != 3 || len(vs) != 3 {
		t.Fatalf("merged %d times / %d values, want 3 rows: %v %v", len(times), len(vs), times, vs)
	}
	got := map[int64]float64{}
	for i, ts := range times {
		got[ts] = vs[i]
	}
	if got[1] != 10 || got[2] != 21 || got[3] != 30 {
		t.Errorf("rows = %v, want 1→10, 2→21 (the later emission), 3→30", got)
	}
	// Time 1's value was null; validity must follow the row it belongs to.
	valid := merged.Validity["v"]
	if valid == nil {
		t.Fatal("merged batch lost its validity bitmap")
	}
	for i, ts := range times {
		if want := ts != 1; valid[i] != want {
			t.Errorf("validity of time %d = %v, want %v", ts, valid[i], want)
		}
	}

	// Unmarked batches are never deduplicated at merge: same times, both kept.
	plain, err := buffer.mergeBatches([]interface{}{
		&TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1}, "v": []float64{1}}},
		&TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1}, "v": []float64{2}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(plain.Data["time"].([]int64)); n != 2 {
		t.Errorf("unmarked merge kept %d rows, want 2", n)
	}
}
