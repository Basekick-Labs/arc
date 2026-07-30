package compaction

import (
	"fmt"
	"testing"
	"time"
)

func TestSplitCandidateIntoBatches_NoAlias(t *testing.T) {
	files := make([]string, MaxFilesPerBatch*3+5)
	for i := range files {
		files[i] = fmt.Sprintf("file_%d.parquet", i)
	}

	c := Candidate{
		Database:      "db",
		Measurement:   "cpu",
		PartitionPath: "2026/07/28",
		Files:         files,
		FileCount:     len(files),
		Tier:          "hourly",
		PartitionTime: time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC),
	}

	batches := SplitCandidateIntoBatches(c)

	// Mutate every batch's Files slice.
	for i := range batches {
		for j := range batches[i].Files {
			batches[i].Files[j] = "MUTATED"
		}
	}

	// Verify: original candidate must be untouched.
	for i, f := range c.Files {
		if f == "MUTATED" {
			t.Fatalf("original candidate Files[%d] was mutated through batch slice — aliased backing array", i)
		}
	}

	// Verify: batches must not alias each other.
	// Reset and mutate only batch 0.
	batches2 := SplitCandidateIntoBatches(c)
	if len(batches2) < 2 {
		t.Fatal("expected at least 2 batches")
	}
	for j := range batches2[0].Files {
		batches2[0].Files[j] = "BATCH0"
	}
	for i := 1; i < len(batches2); i++ {
		for j, f := range batches2[i].Files {
			if f == "BATCH0" {
				t.Fatalf("batch[%d].Files[%d] aliased with batch[0].Files — shared backing array", i, j)
			}
		}
	}
}

func TestSplitCandidateIntoBatches_SingleBatchNoAlias(t *testing.T) {
	// The <= MaxFilesPerBatch early-return path must copy too, so batch
	// isolation does not silently depend on file count.
	files := []string{"a.parquet", "b.parquet", "c.parquet"}
	c := Candidate{
		Database:    "db",
		Measurement: "cpu",
		Files:       files,
		FileCount:   len(files),
	}

	batches := SplitCandidateIntoBatches(c)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}

	batches[0].Files[0] = "MUTATED"
	if files[0] == "MUTATED" {
		t.Fatal("single-batch path aliased the caller's backing array")
	}
	if c.Files[0] == "MUTATED" {
		t.Fatal("single-batch path aliased the input candidate's backing array")
	}
}

func TestSplitCandidateIntoBatches_EmptyFiles(t *testing.T) {
	c := Candidate{
		Database:    "db",
		Measurement: "cpu",
		Files:       []string{},
		FileCount:   0,
	}

	batches := SplitCandidateIntoBatches(c)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch for empty files, got %d", len(batches))
	}
	if len(batches[0].Files) != 0 {
		t.Fatalf("expected 0 files, got %d", len(batches[0].Files))
	}
}

func TestSplitCandidateIntoBatches_MinimalSplit(t *testing.T) {
	// MaxFilesPerBatch+1 is the minimum case that exercises the multi-batch path.
	files := make([]string, MaxFilesPerBatch+1)
	for i := range files {
		files[i] = fmt.Sprintf("file_%d.parquet", i)
	}
	c := Candidate{
		Database:    "db",
		Measurement: "cpu",
		Files:       files,
		FileCount:   len(files),
	}

	batches := SplitCandidateIntoBatches(c)
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
	if len(batches[0].Files) != MaxFilesPerBatch {
		t.Fatalf("batch 0: expected %d files, got %d", MaxFilesPerBatch, len(batches[0].Files))
	}
	if len(batches[1].Files) != 1 {
		t.Fatalf("batch 1: expected 1 file, got %d", len(batches[1].Files))
	}

	// Verify no aliasing between the two batches.
	batches[0].Files[0] = "CHANGED"
	if batches[1].Files[0] == "CHANGED" {
		t.Fatal("batch[1].Files[0] aliased with batch[0].Files[0]")
	}
}

func TestSplitCandidateIntoBatches_UnderLimit(t *testing.T) {
	c := Candidate{
		Database:    "db",
		Measurement: "cpu",
		Files:       []string{"a.parquet", "b.parquet"},
		FileCount:   2,
	}

	batches := SplitCandidateIntoBatches(c)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if len(batches[0].Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(batches[0].Files))
	}
}

func TestSplitCandidateIntoBatches_ExactLimit(t *testing.T) {
	files := make([]string, MaxFilesPerBatch)
	for i := range files {
		files[i] = fmt.Sprintf("f_%d.parquet", i)
	}
	c := Candidate{
		Database:    "db",
		Measurement: "cpu",
		Files:       files,
		FileCount:   MaxFilesPerBatch,
	}

	batches := SplitCandidateIntoBatches(c)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch for exactly MaxFilesPerBatch files, got %d", len(batches))
	}
}

func TestSplitCandidateIntoBatches_CorrectPartitioning(t *testing.T) {
	n := MaxFilesPerBatch*2 + 3
	files := make([]string, n)
	for i := range files {
		files[i] = fmt.Sprintf("file_%d.parquet", i)
	}
	c := Candidate{
		Database:      "db",
		Measurement:   "cpu",
		PartitionPath: "2026/07/28",
		Files:         files,
		FileCount:     n,
		Tier:          "daily",
	}

	batches := SplitCandidateIntoBatches(c)

	expectedBatches := 3
	if len(batches) != expectedBatches {
		t.Fatalf("expected %d batches, got %d", expectedBatches, len(batches))
	}

	// First batch: full.
	if len(batches[0].Files) != MaxFilesPerBatch {
		t.Fatalf("batch 0: expected %d files, got %d", MaxFilesPerBatch, len(batches[0].Files))
	}
	// Second batch: full.
	if len(batches[1].Files) != MaxFilesPerBatch {
		t.Fatalf("batch 1: expected %d files, got %d", MaxFilesPerBatch, len(batches[1].Files))
	}
	// Last batch: remainder.
	if len(batches[2].Files) != 3 {
		t.Fatalf("batch 2: expected 3 files, got %d", len(batches[2].Files))
	}

	// Verify batch numbering.
	for i, b := range batches {
		if b.BatchNumber != i+1 {
			t.Errorf("batch %d: BatchNumber = %d, want %d", i, b.BatchNumber, i+1)
		}
		if b.TotalBatches != expectedBatches {
			t.Errorf("batch %d: TotalBatches = %d, want %d", i, b.TotalBatches, expectedBatches)
		}
	}

	// Verify file contents are correct (not just counts).
	want := 0
	for _, b := range batches {
		for _, f := range b.Files {
			expected := fmt.Sprintf("file_%d.parquet", want)
			if f != expected {
				t.Errorf("file mismatch at index %d: got %q, want %q", want, f, expected)
			}
			want++
		}
	}
}
