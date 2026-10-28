package ingest

import (
	"bytes"
	"math"
	"slices"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// Multi-key sort for flushes with ingest.sort_keys (e.g. fleet,name,time).
//
// The comparison sort it replaces called a closure with a type switch per key
// on every one of its O(n log n) comparisons: 2.60 s for a 5M-row flush sorted
// by two string keys and time, against 0.21 s for time alone. This is a stable
// LSD sort instead: one stable pass per key, from the last key to the first,
// each linear in the rows:
//
//   - int64: LSD radix on the order-preserving unsigned key (radixSortBias),
//     skipping byte positions where every row agrees.
//   - float64: the same radix on an order-preserving key (see floatSortKey).
//   - string and []byte: each value is replaced by its rank among the sorted
//     distinct values, then a counting sort on the ranks.
//   - bool: a counting sort.
//
// A string or binary key with more distinct values than a quarter of the rows
// (an id rather than a tag), or a decimal key, has no cheap linear pass: then
// the whole sort is one comparison sort over all keys with typed comparators
// and the row position as the last key (compoundPermutation), about the
// old sort's cost, and the rank map stays bounded.
//
// Each pass is stable, so after the first key's pass the rows are ordered by
// every key, ties keeping arrival order. Measured on 5M rows (fleet, name,
// time; 10,000 names): 0.51 s including applying the permutation, 5x faster.
// Scratch memory: about 24 bytes a row (two int permutations and the radix
// keys) plus 4 a row for each string, binary or bool key, against 8 before.

// floatSortKey maps a float64 to a uint64 whose unsigned order is the float
// order: -0 and +0 are one key (they compare equal), every NaN is one key
// after +Inf. The comparison sort treated NaN as equal to everything, which
// is not an order; NaN sorting last is.
func floatSortKey(f float64) uint64 {
	if f != f {
		return math.MaxUint64
	}
	if f == 0 {
		f = 0 // -0 -> +0
	}
	b := math.Float64bits(f)
	if b>>63 == 1 {
		return ^b
	}
	return b | 1<<63
}

// compareSortKeys compares rows i and j by the cached key columns, in the
// order multiKeyPermutation sorts them (floats by floatSortKey). It returns
// -1, 0 or 1, and ok=false for a column type it does not handle.
func compareSortKeys(cols []interface{}, i, j int) (int, bool) {
	for _, col := range cols {
		c := 0
		switch v := col.(type) {
		case []int64:
			c = cmpOrdered(v[i], v[j])
		case []float64:
			c = cmpOrdered(floatSortKey(v[i]), floatSortKey(v[j]))
		case []string:
			c = cmpOrdered(v[i], v[j])
		case [][]byte:
			c = bytes.Compare(v[i], v[j])
		case []bool:
			if v[i] != v[j] {
				c = 1
				if !v[i] {
					c = -1
				}
			}
		case []decimal128.Num:
			c = v[i].Cmp(v[j])
		default:
			return 0, false
		}
		if c != 0 {
			return c, true
		}
	}
	return 0, true
}

func cmpOrdered[T int64 | uint64 | string](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// multiKeyPermutation returns the stable permutation that sorts n rows by
// the key columns (first key most significant). It returns nil, true when the
// rows are already in order (the identity, as permuteByTime does), and
// nil, false when a key column's type is not handled; the caller then sorts
// another way.
func multiKeyPermutation(cols []interface{}, n int) ([]int, bool) {
	for _, col := range cols {
		switch col.(type) {
		case []int64, []float64, []string, [][]byte, []bool, []decimal128.Num:
		default:
			return nil, false
		}
		// Every key column must hold exactly the n rows the permutation is
		// for; the passes below index by row. Anything else goes to the
		// caller's comparison sort, which only reads rows below n.
		if columnLen(col) != n {
			return nil, false
		}
	}
	sorted := true
	for i := 1; i < n; i++ {
		if c, _ := compareSortKeys(cols, i-1, i); c > 0 {
			sorted = false
			break
		}
	}
	if sorted {
		return nil, true
	}

	// Rank the string and binary keys first. One with too many distinct
	// values (an id, not a tag), or a decimal key, has no linear pass: then
	// one comparison sort over all the keys is cheaper than the other keys'
	// passes plus a stable comparison sort for that one.
	ranks := make([][]uint32, len(cols))
	cards := make([]int, len(cols))
	for k, col := range cols {
		var ok bool
		switch v := col.(type) {
		case []string:
			ranks[k], cards[k], ok = rankStrings(v, n/4)
		case [][]byte:
			ranks[k], cards[k], ok = rankBytes(v, n/4)
		case []decimal128.Num:
			ok = false
		default:
			ok = true
		}
		if !ok {
			return compoundPermutation(cols, n), true
		}
	}

	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	tmp := make([]int, n)
	var keys []uint64      // reused across radix passes
	var boolRanks []uint32 // reused across bool passes
	for k := len(cols) - 1; k >= 0; k-- {
		switch v := cols[k].(type) {
		case []int64:
			if keys == nil {
				keys = make([]uint64, n)
			}
			for i := range keys {
				keys[i] = radixSortBias(v[i])
			}
			perm, tmp = radixPassesUint64(perm, tmp, keys)
		case []float64:
			if keys == nil {
				keys = make([]uint64, n)
			}
			for i := range keys {
				keys[i] = floatSortKey(v[i])
			}
			perm, tmp = radixPassesUint64(perm, tmp, keys)
		case []string, [][]byte:
			perm, tmp = countingPass(perm, tmp, ranks[k], cards[k])
		case []bool:
			if boolRanks == nil {
				boolRanks = make([]uint32, n)
			}
			for i := range boolRanks {
				boolRanks[i] = 0
				if v[i] {
					boolRanks[i] = 1
				}
			}
			perm, tmp = countingPass(perm, tmp, boolRanks, 2)
		}
	}
	return perm, true
}

// compoundPermutation sorts by every key in one comparison sort, the row's
// position breaking ties, so it is stable. Each key's comparator is built
// once, so a comparison does no type switch.
func compoundPermutation(cols []interface{}, n int) []int {
	cmps := make([]func(a, b int) int, len(cols))
	for k, col := range cols {
		switch v := col.(type) {
		case []int64:
			cmps[k] = func(a, b int) int { return cmpOrdered(v[a], v[b]) }
		case []float64:
			cmps[k] = func(a, b int) int { return cmpOrdered(floatSortKey(v[a]), floatSortKey(v[b])) }
		case []string:
			cmps[k] = func(a, b int) int { return cmpOrdered(v[a], v[b]) }
		case [][]byte:
			cmps[k] = func(a, b int) int { return bytes.Compare(v[a], v[b]) }
		case []bool:
			cmps[k] = func(a, b int) int {
				if v[a] == v[b] {
					return 0
				}
				if v[a] {
					return 1
				}
				return -1
			}
		case []decimal128.Num:
			cmps[k] = func(a, b int) int { return v[a].Cmp(v[b]) }
		}
	}
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	slices.SortFunc(perm, func(a, b int) int {
		for _, c := range cmps {
			if r := c(a, b); r != 0 {
				return r
			}
		}
		return a - b
	})
	return perm
}

// radixPassesUint64 stably reorders perm by keys[perm[i]] ascending with an
// LSD radix sort, 8 bits a pass, skipping byte positions where every key
// agrees. It returns the sorted permutation and the other buffer.
func radixPassesUint64(perm, tmp []int, keys []uint64) ([]int, []int) {
	var or, and uint64 = 0, math.MaxUint64
	for _, k := range keys {
		or |= k
		and &= k
	}
	differ := or ^ and
	var count [256]int
	for shift := uint(0); shift < 64; shift += 8 {
		if (differ>>shift)&0xff == 0 {
			continue
		}
		count = [256]int{}
		for _, ix := range perm {
			count[(keys[ix]>>shift)&0xff]++
		}
		sum := 0
		for i := range count {
			c := count[i]
			count[i] = sum
			sum += c
		}
		for _, ix := range perm {
			b := (keys[ix] >> shift) & 0xff
			tmp[count[b]] = ix
			count[b]++
		}
		perm, tmp = tmp, perm
	}
	return perm, tmp
}

// countingPass stably reorders perm by ranks[perm[i]] ascending, ranks in
// [0, card).
func countingPass(perm, tmp []int, ranks []uint32, card int) ([]int, []int) {
	count := make([]int, card+1)
	for _, ix := range perm {
		count[ranks[ix]+1]++
	}
	for i := 1; i <= card; i++ {
		count[i] += count[i-1]
	}
	for _, ix := range perm {
		r := ranks[ix]
		tmp[count[r]] = ix
		count[r]++
	}
	return tmp, perm
}

// rankStrings replaces each value by its rank among the sorted distinct
// values. It gives up (ok=false) once there are more than limit distinct
// values, so the map stays bounded for an id-like key.
func rankStrings(v []string, limit int) ([]uint32, int, bool) {
	idx := make(map[string]uint32)
	uniq := make([]string, 0, 64)
	for _, s := range v {
		if _, ok := idx[s]; !ok {
			if len(uniq) >= limit && limit > 0 {
				return nil, 0, false
			}
			idx[s] = 0
			uniq = append(uniq, s)
		}
	}
	slices.Sort(uniq)
	for r, s := range uniq {
		idx[s] = uint32(r)
	}
	ranks := make([]uint32, len(v))
	for i, s := range v {
		ranks[i] = idx[s]
	}
	return ranks, len(uniq), true
}

// rankBytes is rankStrings for []byte values (compared as bytes.Compare,
// which is the order of their string conversions).
func rankBytes(v [][]byte, limit int) ([]uint32, int, bool) {
	idx := make(map[string]uint32)
	uniq := make([]string, 0, 64)
	for _, b := range v {
		if _, ok := idx[string(b)]; !ok {
			if len(uniq) >= limit && limit > 0 {
				return nil, 0, false
			}
			s := string(b)
			idx[s] = 0
			uniq = append(uniq, s)
		}
	}
	slices.Sort(uniq)
	for r, s := range uniq {
		idx[s] = uint32(r)
	}
	ranks := make([]uint32, len(v))
	for i, b := range v {
		ranks[i] = idx[string(b)]
	}
	return ranks, len(uniq), true
}
