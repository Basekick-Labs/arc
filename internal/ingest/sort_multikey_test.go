package ingest

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/stretchr/testify/require"
)

// referencePermutation is the order multiKeyPermutation must produce: a
// stable sort by compareSortKeys.
func referencePermutation(cols []interface{}, n int) []int {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		c, _ := compareSortKeys(cols, idx[a], idx[b])
		return c < 0
	})
	return idx
}

var specialFloats = []float64{0, math.Copysign(0, -1), math.NaN(), math.Inf(1), math.Inf(-1), -1.5, 1.5, math.SmallestNonzeroFloat64, -math.MaxFloat64}

func randomKeyColumn(rng *rand.Rand, kind string, n int) interface{} {
	switch kind {
	case "int64":
		v := make([]int64, n)
		for i := range v {
			switch rng.Intn(10) {
			case 0:
				v[i] = math.MinInt64
			case 1:
				v[i] = math.MaxInt64
			default:
				v[i] = int64(rng.Intn(50)) - 25
			}
		}
		return v
	case "time": // microsecond timestamps around 2016, some pre-epoch
		v := make([]int64, n)
		for i := range v {
			v[i] = 1451606400000000 + int64(rng.Intn(20))*10_000_000
			if rng.Intn(20) == 0 {
				v[i] = -v[i]
			}
		}
		return v
	case "float64":
		v := make([]float64, n)
		for i := range v {
			if rng.Intn(3) == 0 {
				v[i] = specialFloats[rng.Intn(len(specialFloats))]
			} else {
				v[i] = float64(rng.Intn(20)) / 4
			}
		}
		return v
	case "tag": // low cardinality: rank + counting pass
		v := make([]string, n)
		for i := range v {
			v[i] = fmt.Sprintf("truck_%d", rng.Intn(30))
		}
		return v
	case "id": // high cardinality: stable comparison fallback
		v := make([]string, n)
		for i := range v {
			v[i] = fmt.Sprintf("req-%08x", rng.Uint32())
		}
		return v
	case "bytes":
		v := make([][]byte, n)
		for i := range v {
			v[i] = []byte{byte(rng.Intn(4)), byte(rng.Intn(256))}[:1+rng.Intn(2)]
		}
		return v
	case "bool":
		v := make([]bool, n)
		for i := range v {
			v[i] = rng.Intn(2) == 0
		}
		return v
	case "decimal":
		v := make([]decimal128.Num, n)
		for i := range v {
			v[i] = decimal128.FromI64(int64(rng.Intn(40)) - 20)
		}
		return v
	}
	panic(kind)
}

// TestMultiKeyPermutationMatchesReference is the property the sort rests on:
// for every key type and key combination, the linear sort equals a stable
// comparison sort exactly (ties keep arrival order).
func TestMultiKeyPermutationMatchesReference(t *testing.T) {
	kinds := []string{"int64", "float64", "tag", "id", "bytes", "bool", "decimal"}
	rng := rand.New(rand.NewSource(7))
	for _, n := range []int{0, 1, 2, 3, 7, 100, 4095, 4096, 5000} {
		for trial := 0; trial < 12; trial++ {
			nk := 1 + rng.Intn(3)
			var names []string
			var cols []interface{}
			for k := 0; k < nk; k++ {
				kind := kinds[rng.Intn(len(kinds))]
				names = append(names, kind)
				cols = append(cols, randomKeyColumn(rng, kind, n))
			}
			// Most configurations end with time, as getSortKeys appends it.
			if rng.Intn(4) != 0 {
				names = append(names, "time")
				cols = append(cols, randomKeyColumn(rng, "time", n))
			}
			want := referencePermutation(cols, n)
			got, ok := multiKeyPermutation(cols, n)
			require.True(t, ok, "keys %v", names)
			if got == nil { // already sorted: the identity
				got = make([]int, n)
				for i := range got {
					got[i] = i
				}
			}
			require.Equal(t, want, got, "n=%d keys=%v", n, names)
		}
	}
}

// Already-ordered rows return nil (the identity), as permuteByTime does, so
// the caller skips copying every column.
func TestMultiKeyPermutationAlreadySorted(t *testing.T) {
	cols := []interface{}{[]string{"a", "a", "b"}, []int64{1, 2, 1}}
	perm, ok := multiKeyPermutation(cols, 3)
	require.True(t, ok)
	require.Nil(t, perm)
}

// A key column of a type the linear sort does not handle is reported, and
// sortColumnsByKeysWithPermutation falls back to the comparison sort.
func TestMultiKeyPermutationUnknownType(t *testing.T) {
	_, ok := multiKeyPermutation([]interface{}{[]int32{2, 1}, []int64{1, 2}}, 2)
	require.False(t, ok)
	sorted, _, err := sortColumnsByKeysWithPermutation(map[string]interface{}{
		"k": []int32{2, 1}, "time": []int64{2, 1},
	}, []string{"k", "time"})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, sorted["time"])
}

// The float order: -0 equals +0 (ties keep arrival order), NaN sorts after +Inf.
func TestFloatSortKeyOrder(t *testing.T) {
	ordered := []float64{math.Inf(-1), -math.MaxFloat64, -1, -math.SmallestNonzeroFloat64, 0, math.SmallestNonzeroFloat64, 1, math.MaxFloat64, math.Inf(1)}
	for i := 1; i < len(ordered); i++ {
		require.Less(t, floatSortKey(ordered[i-1]), floatSortKey(ordered[i]), "%v < %v", ordered[i-1], ordered[i])
	}
	require.Equal(t, floatSortKey(0), floatSortKey(math.Copysign(0, -1)))
	require.Greater(t, floatSortKey(math.NaN()), floatSortKey(math.Inf(1)))
	require.Equal(t, floatSortKey(math.NaN()), floatSortKey(-math.NaN()))
}

// End to end: sortColumnsByKeysWithPermutation reorders every column by the
// permutation, a multi-key flush shaped like TSBS IoT.
func TestSortColumnsByKeysMultiKey(t *testing.T) {
	cols := map[string]interface{}{
		"fleet": []string{"West", "East", "West", "East"},
		"name":  []string{"t2", "t9", "t1", "t9"},
		"time":  []int64{40, 30, 20, 10},
		"v":     []float64{4, 3, 2, 1},
	}
	sorted, idx, err := sortColumnsByKeysWithPermutation(cols, []string{"fleet", "name", "time"})
	require.NoError(t, err)
	require.Equal(t, []int{3, 1, 2, 0}, idx)
	require.Equal(t, []string{"East", "East", "West", "West"}, sorted["fleet"])
	require.Equal(t, []int64{10, 30, 20, 40}, sorted["time"])
	require.Equal(t, []float64{1, 3, 2, 4}, sorted["v"])
}

func multiKeyBenchColumns(n, trucks int) map[string]interface{} {
	rng := rand.New(rand.NewSource(1))
	fleets := []string{"East", "North", "South", "West", "Central"}
	names := make([]string, trucks)
	for i := range names {
		names[i] = fmt.Sprintf("truck_%d", i)
	}
	fleet, name, tm := make([]string, n), make([]string, n), make([]int64, n)
	cols := map[string]interface{}{"fleet": fleet, "name": name, "time": tm}
	for f := 0; f < 8; f++ {
		v := make([]float64, n)
		for i := range v {
			v[i] = rng.Float64()
		}
		cols[fmt.Sprintf("f%d", f)] = v
	}
	// TSBS-like arrival: every truck each tick, batches interleaved.
	for i := 0; i < n; i++ {
		tr := i % trucks
		fleet[i], name[i] = fleets[tr%len(fleets)], names[tr]
		tm[i] = 1451606400000000 + int64(i/trucks)*10_000_000
	}
	for i := 0; i < n/10; i++ {
		a, b := rng.Intn(n), rng.Intn(n)
		fleet[a], fleet[b] = fleet[b], fleet[a]
		name[a], name[b] = name[b], name[a]
		tm[a], tm[b] = tm[b], tm[a]
	}
	return cols
}

func BenchmarkSortColumnsMultiKey(b *testing.B) {
	for _, n := range []int{50_000, 1_000_000, 5_000_000} {
		cols := multiKeyBenchColumns(n, 10_000)
		b.Run(fmt.Sprintf("fleet_name_time/%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, _ = sortColumnsByKeysWithPermutation(cols, []string{"fleet", "name", "time"})
			}
		})
		b.Run(fmt.Sprintf("time_only/%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, _ = sortColumnsByKeysWithPermutation(cols, []string{"time"})
			}
		})
	}
}
