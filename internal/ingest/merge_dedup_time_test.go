package ingest

import "testing"

// The arc:dedup_time marker opts continuous-query output into exact-row dedup
// at compaction (#1232). A merged flush carries it if any of its batches did —
// exact-row dedup never deletes a row that differs from the others, so marking
// a mix is safe — and the merge itself never removes a row: two batches with
// the same time and different values are different data.
func TestMergeBatches_DedupTimeMarkerAndRowsKept(t *testing.T) {
	buffer := createTestArrowBuffer(t)
	tcb := func(times []int64, vs []float64, marked bool) *TypedColumnBatch {
		return &TypedColumnBatch{Data: map[string]interface{}{"time": times, "v": vs}, DedupTime: marked}
	}
	cases := []struct {
		name     string
		batches  []interface{}
		wantMark bool
		wantRows int
	}{
		{"single marked batch", []interface{}{tcb([]int64{1}, []float64{1}, true)}, true, 1},
		{"single unmarked batch", []interface{}{tcb([]int64{1}, []float64{1}, false)}, false, 1},
		{"marked then unmarked", []interface{}{tcb([]int64{1}, []float64{1}, true), tcb([]int64{2}, []float64{2}, false)}, true, 2},
		{"unmarked then marked", []interface{}{tcb([]int64{2}, []float64{2}, false), tcb([]int64{1}, []float64{1}, true)}, true, 2},
		{"two marked emissions of one window, values differ", []interface{}{
			tcb([]int64{1, 2}, []float64{10, 20}, true),
			tcb([]int64{2, 3}, []float64{21, 30}, true),
		}, true, 4},
		{"marked with a replayed map batch", []interface{}{
			tcb([]int64{1}, []float64{1}, true),
			map[string]interface{}{"time": []int64{1}, "v": []float64{1}},
		}, true, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			merged, err := buffer.mergeBatches(tc.batches)
			if err != nil {
				t.Fatalf("mergeBatches: %v", err)
			}
			if merged.DedupTime != tc.wantMark {
				t.Errorf("DedupTime = %v, want %v", merged.DedupTime, tc.wantMark)
			}
			if n := len(merged.Data["time"].([]int64)); n != tc.wantRows {
				t.Errorf("merged %d rows, want %d: the merge must not drop rows", n, tc.wantRows)
			}
		})
	}
}
