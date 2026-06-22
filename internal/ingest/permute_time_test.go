package ingest

import (
	"math/rand"
	"testing"
)

// permuteByTime replaced a closure-based sort.Slice on the time-only flush path with an
// LSD radix sort (see arrow_writer.go). These tests lock in correctness across the data
// shapes a flushed buffer actually takes, including the pass-skip edge cases.

// isPermutationSorting checks that idx is a valid permutation of [0,n) that orders times
// ascending. A nil idx means "identity" (data already sorted), valid only when times is
// already non-decreasing.
func isPermutationSorting(times []int64, idx []int) bool {
	n := len(times)
	if idx == nil {
		for i := 1; i < n; i++ {
			if times[i] < times[i-1] {
				return false
			}
		}
		return true
	}
	if len(idx) != n {
		return false
	}
	seen := make([]bool, n)
	for _, v := range idx {
		if v < 0 || v >= n || seen[v] {
			return false // out of range or duplicate => not a permutation
		}
		seen[v] = true
	}
	for i := 1; i < n; i++ {
		if times[idx[i]] < times[idx[i-1]] {
			return false
		}
	}
	return true
}

func TestPermuteByTime_Shapes(t *testing.T) {
	mkRuns := func(n, k int, seed int64) []int64 {
		rng := rand.New(rand.NewSource(seed))
		out := make([]int64, 0, n)
		per := n / k
		base := int64(1_700_000_000_000_000)
		for r := 0; r < k; r++ {
			tt := base + rng.Int63n(2_000_000)
			for i := 0; i < per && len(out) < n; i++ {
				tt += rng.Int63n(50) + 1
				out = append(out, tt)
			}
		}
		for len(out) < n {
			out = append(out, base)
		}
		return out
	}
	mkRandom := func(n int, seed int64) []int64 {
		rng := rand.New(rand.NewSource(seed))
		out := make([]int64, n)
		for i := range out {
			out[i] = rng.Int63n(int64(n))
		}
		return out
	}
	mkSorted := func(n int) []int64 {
		out := make([]int64, n)
		for i := range out {
			out[i] = int64(i)
		}
		return out
	}
	mkReverse := func(n int) []int64 {
		out := make([]int64, n)
		for i := range out {
			out[i] = int64(n - i)
		}
		return out
	}
	mkEqual := func(n int) []int64 {
		out := make([]int64, n)
		for i := range out {
			out[i] = 42
		}
		return out
	}
	// mkNarrowRange: all timestamps within ~1s of a recent base. Only the low ~3 bytes of
	// the micros vary, so radix skips the high passes — the real "all near now" production
	// shape, and the case that most exercises the pass-skip optimization.
	mkNarrowRange := func(n int, seed int64) []int64 {
		rng := rand.New(rand.NewSource(seed))
		base := int64(1_700_000_000_000_000)
		out := make([]int64, n)
		for i := range out {
			out[i] = base + rng.Int63n(1_000_000) // within 1s
		}
		return out
	}
	// mkPreEpochMixed: interleaves negative (pre-1970) and positive micros out of order.
	// Guards the radix sign-bit handling — without radixSortBias, negatives sort after
	// positives. Line Protocol and MessagePack both accept pre-epoch timestamps (#312).
	mkPreEpochMixed := func(n int) []int64 {
		out := make([]int64, n)
		for i := range out {
			if i%2 == 0 {
				out[i] = int64(-1_000_000 - i)
			} else {
				out[i] = int64(1_000_000 + i)
			}
		}
		return out
	}
	// mkAllNegative: entirely pre-1970 timestamps, descending (worst case for sign handling).
	mkAllNegative := func(n int) []int64 {
		out := make([]int64, n)
		for i := range out {
			out[i] = int64(-i - 1)
		}
		return out
	}

	cases := map[string][]int64{
		"empty":               {},
		"single":              {7},
		"two_sorted":          {1, 2},
		"two_reverse":         {2, 1},
		"all_equal":           mkEqual(1000),
		"already_sorted":      mkSorted(10000),
		"reverse_sorted":      mkReverse(10000),
		"realistic_12_runs":   mkRuns(120000, 12, 1),
		"realistic_64_runs":   mkRuns(128000, 64, 2),
		"large_random":        mkRandom(50000, 3),      // radix path on disordered data
		"narrow_second_range": mkNarrowRange(50000, 4), // all within ~1s: only low bytes vary (max pass-skipping)
		"pre_epoch_mixed":     mkPreEpochMixed(50000),  // negative+positive micros (#312): radix sign-bit handling
		"all_negative":        mkAllNegative(50000),    // entirely pre-1970
	}

	for name, times := range cases {
		idx := permuteByTime(times)
		if !isPermutationSorting(times, idx) {
			t.Errorf("%s: permuteByTime did not produce a valid sorting permutation (n=%d)", name, len(times))
		}
	}
}

// Cross-check permuteByTime against permuteByTimeSort (the fallback / reference) on random
// shapes: both must yield the same ascending time ordering. Covers signed (negative) keys
// and sizes both below and above radixSkipThreshold so the radix path is exercised.
func TestPermuteByTime_MatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	for trial := 0; trial < 80; trial++ {
		// Half the trials are large enough to take the radix path (n >= radixSkipThreshold),
		// half are small (comparison-sort path). All include negative timestamps (#312).
		var n int
		if trial%2 == 0 {
			n = radixSkipThreshold + rng.Intn(5000)
		} else {
			n = rng.Intn(5000)
		}
		times := make([]int64, n)
		for i := range times {
			times[i] = rng.Int63n(2000) - 1000 // range [-1000, 1000): negatives + ties
		}
		got := permuteByTime(times)
		// Materialize the ordered time sequence under the returned permutation.
		seq := make([]int64, n)
		if got == nil {
			copy(seq, times)
		} else {
			for i, p := range got {
				seq[i] = times[p]
			}
		}
		// Reference: sorted copy.
		ref := permuteByTimeSort(times)
		refSeq := make([]int64, n)
		for i, p := range ref {
			refSeq[i] = times[p]
		}
		for i := range seq {
			if seq[i] != refSeq[i] {
				t.Fatalf("trial %d: ordered time mismatch at %d (n=%d)", trial, i, n)
			}
		}
	}
}

// benchInterleaved models the REAL merged-buffer shape: many 1000-row batches (each
// internally time-sorted) whose rows interleave at the row level as concurrent producers
// append to the shared buffer. This yields ~millions of tiny sorted runs (the 2026-06-22
// profile measured ~2.5M runs in a 5M-row buffer), i.e. no exploitable run structure —
// which is why permuteByTime uses radix, not a run-merge.
func benchInterleaved(n, batchSz int, seed int64) []int64 {
	rng := rand.New(rand.NewSource(seed))
	nb := n / batchSz
	batches := make([][]int64, nb)
	base := int64(1_700_000_000_000_000)
	for i := range batches {
		tt := base + rng.Int63n(5_000_000) // 5s arrival skew between producers
		col := make([]int64, batchSz)
		for j := range col {
			tt += rng.Int63n(50) + 1
			col[j] = tt
		}
		batches[i] = col
	}
	out := make([]int64, 0, n)
	pos := make([]int, nb)
	for len(out) < n {
		bi := rng.Intn(nb)
		if pos[bi] < len(batches[bi]) {
			out = append(out, batches[bi][pos[bi]])
			pos[bi]++
		}
	}
	return out
}

// TestRadixPermuteByTime exercises the radix path directly (n >= radixSkipThreshold) on
// the realistic interleaved shape and confirms it produces a valid sorting permutation.
func TestRadixPermuteByTime(t *testing.T) {
	times := benchInterleaved(200_000, 1000, 5)
	idx := radixPermuteByTime(times)
	if !isPermutationSorting(times, idx) {
		t.Fatalf("radixPermuteByTime did not produce a valid sorting permutation")
	}
}

// BenchmarkPermuteByTime_Realistic measures the production path on the realistic
// row-interleaved 5M-row buffer (radix path).
func BenchmarkPermuteByTime_Realistic(b *testing.B) {
	times := benchInterleaved(5_000_000, 1000, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = permuteByTime(times)
	}
}

// BenchmarkPermuteByTime_AlreadySorted measures the in-order single-producer fast path.
func BenchmarkPermuteByTime_AlreadySorted(b *testing.B) {
	times := make([]int64, 5_000_000)
	for i := range times {
		times[i] = int64(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = permuteByTime(times)
	}
}
