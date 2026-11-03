package ingest

import (
	"io"
	"math/rand"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"

	"github.com/stretchr/testify/require"
)

func randomSortedBatches(rng *rand.Rand, k int, withNulls, shuffle bool) []interface{} {
	base := int64(1700000000000000)
	base -= base % microPerHour
	var out []interface{}
	for b := 0; b < k; b++ {
		n := 1 + rng.Intn(40)
		if k >= 300 {
			n = 20 + rng.Intn(40)
		}
		times := make([]int64, n)
		t := base + int64(rng.Intn(50))
		for i := range times {
			t += int64(rng.Intn(3)) // repeats within and across batches
			times[i] = t
		}
		if shuffle {
			rng.Shuffle(n, func(i, j int) { times[i], times[j] = times[j], times[i] })
		}
		vs, ns, ss, oks := make([]float64, n), make([]int64, n), make([]string, n), make([]bool, n)
		for i := 0; i < n; i++ {
			vs[i], ns[i], ss[i], oks[i] = rng.Float64(), int64(b*1000+i), string(rune('a'+rng.Intn(26))), rng.Intn(2) == 0
		}
		tb := &TypedColumnBatch{
			Data:       map[string]interface{}{"time": times, "v": vs, "n": ns, "s": ss, "ok": oks, "_x": []int64{int64(b)}},
			TagColumns: []string{"s"},
			DedupTime:  rng.Intn(4) == 0,
		}
		if b%2 == 1 {
			tb.Data["_x"] = []float64{1.5} // an unstored column may change type
		}
		if withNulls && rng.Intn(2) == 0 {
			valid := make([]bool, n)
			for i := range valid {
				valid[i] = rng.Intn(3) != 0
			}
			tb.Validity = map[string][]bool{"n": valid, "time": nil}
		}
		out = append(out, tb)
	}
	return out
}

// The merge gives exactly what concatenating and then sorting by time gives
// (rows, order including equal times, types, NULLs): it sorts the same
// concatenated time column the same way.
func TestMergeSortedByTimeMatchesMergeThenSort(t *testing.T) {
	b := diffBuffer(t)
	rng := rand.New(rand.NewSource(7))
	for iter := 0; iter < 300; iter++ {
		batches := randomSortedBatches(rng, 2+rng.Intn(12), iter%2 == 0, iter%3 == 0)
		if iter%50 == 0 {
			batches = randomSortedBatches(rng, 300, iter%2 == 0, true) // past the radix threshold
			require.Greater(t, rowCount(batches), radixSkipThreshold)
		}
		got, ok := b.mergeSortedByTime("m", batches)
		require.True(t, ok, "iteration %d not eligible", iter)
		merged, err := b.mergeBatches(batches)
		require.NoError(t, err)
		want, _ := sortTypedColumnBatchByKeys(merged, []string{"time"})
		rows := len(want.Data["time"].([]int64))
		require.Equal(t, normalize(want, rows), normalize(got, rows), "iteration %d", iter)
		_, has := got.Data["_x"]
		require.False(t, has)
		require.Equal(t, want.DedupTime, got.DedupTime)
		require.Equal(t, len(want.Validity), len(got.Validity), "iteration %d: NULL bitmaps", iter)
	}
}

// Not eligible: sort keys beyond time, two hours, a batch with different
// columns, a single batch.
func TestMergeSortedByTimeFallsBack(t *testing.T) {
	b := diffBuffer(t)
	mk := func(times []int64, cols ...string) *TypedColumnBatch {
		d := map[string]interface{}{"time": times}
		for _, c := range cols {
			d[c] = make([]float64, len(times))
		}
		return &TypedColumnBatch{Data: d}
	}
	base := int64(1700000000000000)
	base -= base % microPerHour
	_, ok := b.mergeSortedByTime("m", []interface{}{mk([]int64{base}, "v"), mk([]int64{base + microPerHour}, "v")})
	require.False(t, ok, "two hours")
	_, ok = b.mergeSortedByTime("m", []interface{}{mk([]int64{base}, "v"), mk([]int64{base + 1}, "w")})
	require.False(t, ok, "different columns")
	_, ok = b.mergeSortedByTime("m", []interface{}{mk([]int64{base}, "v")})
	require.False(t, ok, "one batch")
	_, ok = b.mergeSortedByTime("m", []interface{}{mk([]int64{base}, "v", "w"), mk([]int64{base + 1}, "v")})
	require.False(t, ok, "a batch without one of the columns")
	ints := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{base + 1}, "v": []int64{1}}}
	_, ok = b.mergeSortedByTime("m", []interface{}{mk([]int64{base}, "v"), ints})
	require.False(t, ok, "a column of another type")

	keyed := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 10, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 1, ShardCount: 1, DataPageVersion: "2.0", SortKeys: []string{"m:v,time"}}, &mockStorageBackend{}, zerolog.New(io.Discard))
	defer keyed.Close()
	_, ok = keyed.mergeSortedByTime("m", []interface{}{mk([]int64{base}, "v"), mk([]int64{base + 1}, "v")})
	require.False(t, ok, "sort keys beyond time")
}

func rowCount(batches []interface{}) int {
	n := 0
	for _, b := range batches {
		n += len(b.(*TypedColumnBatch).Data["time"].([]int64))
	}
	return n
}
