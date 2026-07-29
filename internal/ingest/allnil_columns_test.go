package ingest

import (
	"strings"
	"testing"
)

// An all-nil column used to be dropped, because no type can be inferred from
// it. That made the column absent from the batch's parquet file; a column that
// is all-nil in every batch never appeared in any file, so querying it failed
// to bind instead of returning NULLs (#337).
func TestConvertColumnsToTyped_AllNilColumnIsPreserved(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	batch, n, err := buffer.convertColumnsToTyped("sensors", map[string][]interface{}{
		"time":  {int64(1609459200000000), int64(1609459200000001), int64(1609459200000002)},
		"value": {1.0, 2.0, 3.0},
		"depth": {nil, nil, nil},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 3 {
		t.Fatalf("numRecords = %d, want 3", n)
	}

	col, ok := batch.Data["depth"]
	if !ok {
		t.Fatal("all-nil column was dropped — it will be missing from the parquet file")
	}

	arr, ok := col.([]string)
	if !ok {
		t.Fatalf("all-nil column type = %T, want []string placeholder", col)
	}
	if len(arr) != 3 {
		t.Errorf("all-nil column length = %d, want 3 (must match the record count)", len(arr))
	}

	// Every entry must be marked NULL, not empty-string.
	valid, ok := batch.Validity["depth"]
	if !ok {
		t.Fatal("all-nil column has no validity mask — entries would be written as empty strings, not NULL")
	}
	if len(valid) != 3 {
		t.Fatalf("validity length = %d, want 3", len(valid))
	}
	for i, v := range valid {
		if v {
			t.Errorf("validity[%d] = true, want false — every value in the column is nil", i)
		}
	}
}

// The column must still line up with the rest of the batch, otherwise the
// Arrow record builder produces a ragged batch.
func TestConvertColumnsToTyped_AllNilColumnMatchesRecordCount(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	const rows = 5
	times := make([]interface{}, rows)
	values := make([]interface{}, rows)
	nils := make([]interface{}, rows)
	for i := 0; i < rows; i++ {
		times[i] = int64(1609459200000000 + i)
		values[i] = float64(i)
	}

	batch, n, err := buffer.convertColumnsToTyped("m", map[string][]interface{}{
		"time":    times,
		"value":   values,
		"missing": nils,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != rows {
		t.Fatalf("numRecords = %d, want %d", n, rows)
	}
	if got := len(batch.Data["missing"].([]string)); got != rows {
		t.Errorf("all-nil column length = %d, want %d", got, rows)
	}
}

// A later batch carrying real values must infer its own type — the string
// placeholder applies only to the batch where the column was entirely nil.
func TestConvertColumnsToTyped_AllNilDoesNotPinTypeForLaterBatches(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	if _, _, err := buffer.convertColumnsToTyped("m", map[string][]interface{}{
		"time":  {int64(1609459200000000)},
		"depth": {nil},
	}); err != nil {
		t.Fatalf("first batch: %v", err)
	}

	batch, _, err := buffer.convertColumnsToTyped("m", map[string][]interface{}{
		"time":  {int64(1609459200000001)},
		"depth": {12.5},
	})
	if err != nil {
		t.Fatalf("second batch: %v", err)
	}
	if _, ok := batch.Data["depth"].([]float64); !ok {
		t.Errorf("depth type = %T, want []float64 — a real value must infer its own type", batch.Data["depth"])
	}
}

// A partially-nil column already inferred its type from the non-nil values;
// that behavior must be unchanged.
func TestConvertColumnsToTyped_PartiallyNilColumnUnchanged(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	batch, _, err := buffer.convertColumnsToTyped("m", map[string][]interface{}{
		"time":  {int64(1609459200000000), int64(1609459200000001)},
		"depth": {nil, 12.5},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := batch.Data["depth"].([]float64); !ok {
		t.Errorf("depth type = %T, want []float64", batch.Data["depth"])
	}
	valid := batch.Validity["depth"]
	if len(valid) != 2 || valid[0] || !valid[1] {
		t.Errorf("validity = %v, want [false true]", valid)
	}
}

// Time is exempt: a VARCHAR time column makes the partition un-compactable
// (TIMESTAMP != VARCHAR bind failure), so an all-nil time is rejected rather
// than written as a string placeholder.
func TestConvertColumnsToTyped_AllNilTimeIsRejected(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	_, _, err := buffer.convertColumnsToTyped("m", map[string][]interface{}{
		"time":  {nil, nil},
		"value": {1.0, 2.0},
	})
	if err == nil {
		t.Fatal("expected an all-nil time column to be rejected")
	}
	if !strings.Contains(err.Error(), "time column") {
		t.Errorf("error should name the time column, got: %v", err)
	}
}
