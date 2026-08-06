package compaction

import (
	"fmt"
	"testing"
	"time"
)

func TestSplitCandidateIntoBatches_NoAlias(t *testing.T) {
	files := make([]string, DefaultMaxFilesPerBatch*3+5)
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

	batches := SplitCandidateIntoBatches(c, DefaultMaxFilesPerBatch)

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
	batches2 := SplitCandidateIntoBatches(c, DefaultMaxFilesPerBatch)
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
	// The <= DefaultMaxFilesPerBatch early-return path must copy too, so batch
	// isolation does not silently depend on file count.
	files := []string{"a.parquet", "b.parquet", "c.parquet"}
	c := Candidate{
		Database:    "db",
		Measurement: "cpu",
		Files:       files,
		FileCount:   len(files),
	}

	batches := SplitCandidateIntoBatches(c, DefaultMaxFilesPerBatch)
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

	batches := SplitCandidateIntoBatches(c, DefaultMaxFilesPerBatch)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch for empty files, got %d", len(batches))
	}
	if len(batches[0].Files) != 0 {
		t.Fatalf("expected 0 files, got %d", len(batches[0].Files))
	}
}

func TestSplitCandidateIntoBatches_MinimalSplit(t *testing.T) {
	// DefaultMaxFilesPerBatch+1 files leaves a 1-file remainder. A 1-file batch
	// is unusable — compactFilesAdaptively rejects anything below
	// MinFilesPerBatch on its first attempt — so the remainder is folded into
	// the final batch rather than emitted as its own.
	files := make([]string, DefaultMaxFilesPerBatch+1)
	for i := range files {
		files[i] = fmt.Sprintf("file_%d.parquet", i)
	}
	c := Candidate{
		Database:    "db",
		Measurement: "cpu",
		Files:       files,
		FileCount:   len(files),
	}

	batches := SplitCandidateIntoBatches(c, DefaultMaxFilesPerBatch)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch (remainder absorbed), got %d", len(batches))
	}
	if len(batches[0].Files) != DefaultMaxFilesPerBatch+1 {
		t.Fatalf("batch 0: expected %d files, got %d", DefaultMaxFilesPerBatch+1, len(batches[0].Files))
	}

	// Two files over the limit must still split into two usable batches.
	files2 := make([]string, DefaultMaxFilesPerBatch+MinFilesPerBatch)
	for i := range files2 {
		files2[i] = fmt.Sprintf("file_%d.parquet", i)
	}
	c2 := c
	c2.Files = files2
	c2.FileCount = len(files2)

	batches2 := SplitCandidateIntoBatches(c2, DefaultMaxFilesPerBatch)
	if len(batches2) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches2))
	}
	if len(batches2[1].Files) != MinFilesPerBatch {
		t.Fatalf("batch 1: expected %d files, got %d", MinFilesPerBatch, len(batches2[1].Files))
	}

	// Verify no aliasing between the two batches.
	batches2[0].Files[0] = "CHANGED"
	if batches2[1].Files[0] == "CHANGED" {
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

	batches := SplitCandidateIntoBatches(c, DefaultMaxFilesPerBatch)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if len(batches[0].Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(batches[0].Files))
	}
}

func TestSplitCandidateIntoBatches_ExactLimit(t *testing.T) {
	files := make([]string, DefaultMaxFilesPerBatch)
	for i := range files {
		files[i] = fmt.Sprintf("f_%d.parquet", i)
	}
	c := Candidate{
		Database:    "db",
		Measurement: "cpu",
		Files:       files,
		FileCount:   DefaultMaxFilesPerBatch,
	}

	batches := SplitCandidateIntoBatches(c, DefaultMaxFilesPerBatch)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch for exactly DefaultMaxFilesPerBatch files, got %d", len(batches))
	}
}

func TestSplitCandidateIntoBatches_CorrectPartitioning(t *testing.T) {
	n := DefaultMaxFilesPerBatch*2 + 3
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

	batches := SplitCandidateIntoBatches(c, DefaultMaxFilesPerBatch)

	expectedBatches := 3
	if len(batches) != expectedBatches {
		t.Fatalf("expected %d batches, got %d", expectedBatches, len(batches))
	}

	// First batch: full.
	if len(batches[0].Files) != DefaultMaxFilesPerBatch {
		t.Fatalf("batch 0: expected %d files, got %d", DefaultMaxFilesPerBatch, len(batches[0].Files))
	}
	// Second batch: full.
	if len(batches[1].Files) != DefaultMaxFilesPerBatch {
		t.Fatalf("batch 1: expected %d files, got %d", DefaultMaxFilesPerBatch, len(batches[1].Files))
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

// newTestCandidate builds a candidate with n synthetic files.
func newTestCandidate(n int) Candidate {
	files := make([]string, n)
	for i := range files {
		files[i] = fmt.Sprintf("file_%d.parquet", i)
	}
	return Candidate{
		Database:      "db",
		Measurement:   "cpu",
		PartitionPath: "2026/08/06",
		Files:         files,
		FileCount:     n,
		Tier:          "hourly",
		PartitionTime: time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC),
	}
}

// TestSplitCandidateIntoBatches_HonorsConfiguredSize is the core behavioral
// test for making the batch size configurable: the SAME input must split
// differently depending on the argument. Before this was configurable, batching
// was fixed at the package const and no argument could change it.
func TestSplitCandidateIntoBatches_HonorsConfiguredSize(t *testing.T) {
	c := newTestCandidate(12)

	// At the default (30), 12 files fit in a single batch.
	atDefault := SplitCandidateIntoBatches(c, DefaultMaxFilesPerBatch)
	if len(atDefault) != 1 {
		t.Fatalf("at default: expected 1 batch for 12 files, got %d", len(atDefault))
	}

	// At 5, the same 12 files must split 5/5/2.
	atFive := SplitCandidateIntoBatches(c, 5)
	if len(atFive) != 3 {
		t.Fatalf("at 5: expected 3 batches for 12 files, got %d", len(atFive))
	}
	wantSizes := []int{5, 5, 2}
	for i, want := range wantSizes {
		if got := len(atFive[i].Files); got != want {
			t.Errorf("at 5: batch %d has %d files, want %d", i, got, want)
		}
		if atFive[i].FileCount != want {
			t.Errorf("at 5: batch %d FileCount = %d, want %d", i, atFive[i].FileCount, want)
		}
	}

	// Every file must appear exactly once across batches — a smaller batch size
	// must not drop or duplicate work.
	seen := make(map[string]int, 12)
	for _, b := range atFive {
		for _, f := range b.Files {
			seen[f]++
		}
	}
	if len(seen) != 12 {
		t.Errorf("at 5: expected 12 distinct files across batches, got %d", len(seen))
	}
	for f, n := range seen {
		if n != 1 {
			t.Errorf("at 5: file %s appeared %d times, want 1", f, n)
		}
	}
}

// TestSplitCandidateIntoBatches_ClampsOutOfRange covers the config-value domain.
// The value 1 is the important case: compactFilesAdaptively rejects any batch
// below MinFilesPerBatch outright, so a batch size of 1 would fail every batch
// of every partition rather than merely producing small outputs.
func TestSplitCandidateIntoBatches_ClampsOutOfRange(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		wantSize int
	}{
		{"negative falls back to default", -1, DefaultMaxFilesPerBatch},
		{"zero falls back to default", 0, DefaultMaxFilesPerBatch},
		{"one falls back to default (below adaptive-retry floor)", 1, DefaultMaxFilesPerBatch},
		{"minimum is honored", MinFilesPerBatch, MinFilesPerBatch},
		{"excessive value is capped", 10000, MaxAllowedFilesPerBatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := clampFilesPerBatch(tt.input); got != tt.wantSize {
				t.Errorf("clampFilesPerBatch(%d) = %d, want %d", tt.input, got, tt.wantSize)
			}

			// The clamp must also hold through the split itself, not only in
			// the helper — a zero divisor here would panic.
			c := newTestCandidate(DefaultMaxFilesPerBatch + 1)
			batches := SplitCandidateIntoBatches(c, tt.input)
			for i, b := range batches {
				// The final batch may absorb a sub-minimum remainder, so it can
				// overshoot by up to MinFilesPerBatch-1.
				if limit := tt.wantSize + MinFilesPerBatch - 1; len(b.Files) > limit {
					t.Errorf("batch %d has %d files, exceeds effective max %d (+remainder slack)",
						i, len(b.Files), limit)
				}
				// No batch may fall below the floor compactFilesAdaptively
				// rejects — that batch could never compact.
				if len(b.Files) < MinFilesPerBatch {
					t.Errorf("batch %d has %d files, below the adaptive-retry floor %d",
						i, len(b.Files), MinFilesPerBatch)
				}
			}
		})
	}
}

// TestSplitCandidateIntoBatches_ClampReportsAdjustment verifies the second
// return value, which drives the one-shot startup warning in NewManager.
func TestSplitCandidateIntoBatches_ClampReportsAdjustment(t *testing.T) {
	for _, in := range []int{-1, 0, 1, 10000} {
		if _, adjusted := clampFilesPerBatch(in); !adjusted {
			t.Errorf("clampFilesPerBatch(%d): expected adjusted=true", in)
		}
	}
	for _, in := range []int{MinFilesPerBatch, DefaultMaxFilesPerBatch, MaxAllowedFilesPerBatch} {
		if _, adjusted := clampFilesPerBatch(in); adjusted {
			t.Errorf("clampFilesPerBatch(%d): expected adjusted=false", in)
		}
	}
}

// TestSplitCandidateIntoBatches_BatchNumberAtCustomSize guards the identifiers
// that job and output filenames derive from. BatchNumber must be 1-based and
// distinct across siblings, including in the single-batch case — a zero there
// would collide with batch 1.
func TestSplitCandidateIntoBatches_BatchNumberAtCustomSize(t *testing.T) {
	t.Run("multi-batch", func(t *testing.T) {
		batches := SplitCandidateIntoBatches(newTestCandidate(12), 5)
		if len(batches) != 3 {
			t.Fatalf("expected 3 batches, got %d", len(batches))
		}
		seen := make(map[int]bool, len(batches))
		for i, b := range batches {
			if b.BatchNumber != i+1 {
				t.Errorf("batch %d: BatchNumber = %d, want %d", i, b.BatchNumber, i+1)
			}
			if b.TotalBatches != 3 {
				t.Errorf("batch %d: TotalBatches = %d, want 3", i, b.TotalBatches)
			}
			if seen[b.BatchNumber] {
				t.Errorf("duplicate BatchNumber %d — job IDs and output filenames would collide", b.BatchNumber)
			}
			seen[b.BatchNumber] = true
		}
	})

	t.Run("single batch is 1 of 1", func(t *testing.T) {
		batches := SplitCandidateIntoBatches(newTestCandidate(3), 5)
		if len(batches) != 1 {
			t.Fatalf("expected 1 batch, got %d", len(batches))
		}
		if batches[0].BatchNumber != 1 {
			t.Errorf("BatchNumber = %d, want 1 (0 would collide with batch 1 in derived identifiers)",
				batches[0].BatchNumber)
		}
		if batches[0].TotalBatches != 1 {
			t.Errorf("TotalBatches = %d, want 1", batches[0].TotalBatches)
		}
	})
}

// TestSplitCandidateIntoBatches_AtMinimumSize exercises the remainder logic at
// the smallest legal batch size, where it is tightest: with maxFilesPerBatch=2,
// every odd file count leaves a 1-file remainder that must be absorbed rather
// than emitted. Exact batch sizes are asserted because this is the arithmetic
// that decides whether a batch is compactable at all.
func TestSplitCandidateIntoBatches_AtMinimumSize(t *testing.T) {
	tests := []struct {
		files     int
		wantSizes []int
	}{
		{3, []int{3}},       // odd: remainder absorbed, single batch
		{4, []int{2, 2}},    // even: clean division
		{5, []int{2, 3}},    // odd: last batch absorbs the remainder
		{6, []int{2, 2, 2}}, // even
		{7, []int{2, 2, 3}}, // odd
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d_files", tt.files), func(t *testing.T) {
			batches := SplitCandidateIntoBatches(newTestCandidate(tt.files), MinFilesPerBatch)

			if len(batches) != len(tt.wantSizes) {
				t.Fatalf("got %d batches, want %d", len(batches), len(tt.wantSizes))
			}

			total := 0
			for i, want := range tt.wantSizes {
				got := len(batches[i].Files)
				if got != want {
					t.Errorf("batch %d: %d files, want %d", i, got, want)
				}
				if got < MinFilesPerBatch {
					t.Errorf("batch %d: %d files is below the adaptive-retry floor %d — this batch could never compact",
						i, got, MinFilesPerBatch)
				}
				total += got
			}

			// No file may be dropped or duplicated by the remainder handling.
			if total != tt.files {
				t.Errorf("batches account for %d files, want %d", total, tt.files)
			}
		})
	}
}

// TestSplitCandidateIntoBatches_EveryBatchIsCompactable is the integration-level
// guard between the two independent batch-sizing mechanisms: whatever
// SplitCandidateIntoBatches emits must survive compactFilesAdaptively's floor
// check, or the batch fails on its first attempt and its files never compact.
// Swept across batch sizes and file counts because the failure only appears at
// specific remainders.
func TestSplitCandidateIntoBatches_EveryBatchIsCompactable(t *testing.T) {
	for _, size := range []int{MinFilesPerBatch, 3, 5, 7, DefaultMaxFilesPerBatch} {
		for files := 1; files <= size*3+1; files++ {
			batches := SplitCandidateIntoBatches(newTestCandidate(files), size)

			total := 0
			for i, b := range batches {
				total += len(b.Files)
				// A single batch holding the whole (possibly tiny) partition is
				// fine — compactFilesAdaptively's floor only rejects a batch
				// that is small because it was split badly, and a partition
				// with fewer than MinFilesPerBatch files would not reach
				// compaction anyway (tier MinFiles is far higher).
				if len(batches) > 1 && len(b.Files) < MinFilesPerBatch {
					t.Errorf("size=%d files=%d: batch %d has %d files, below compactFilesAdaptively's floor %d",
						size, files, i, len(b.Files), MinFilesPerBatch)
				}
			}
			if total != files {
				t.Errorf("size=%d files=%d: batches account for %d files", size, files, total)
			}
		}
	}
}
