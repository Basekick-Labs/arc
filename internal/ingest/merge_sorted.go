package ingest

import (
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// mergeBatchesForFlush merges a flush's batches. When the result is going to
// be sorted by time anyway, it writes each column once, already in time
// order: the time columns are concatenated and sorted (the permutation the
// flush's own sort would compute, so the order is the same), then every
// other column is gathered from the batches in that order. mergeBatches
// instead copies every column into one batch, and the sort then copies every
// column again.
//
// It applies only when the measurement sorts by time alone, every batch is a
// *TypedColumnBatch with the same stored columns and types, and all rows fall
// in one hour (a multi-hour flush maps rows back to coverage pieces by their
// position in the concatenation). Otherwise it is mergeBatches.
func (b *ArrowBuffer) mergeBatchesForFlush(measurement string, batches []interface{}) (*TypedColumnBatch, error) {
	if merged, ok := b.mergeSortedByTime(measurement, batches); ok {
		b.sortedMerges.Add(1)
		return merged, nil
	}
	return b.mergeBatches(batches)
}

func (b *ArrowBuffer) mergeSortedByTime(measurement string, batches []interface{}) (*TypedColumnBatch, bool) {
	if len(batches) < 2 {
		return nil, false
	}
	if keys := b.getSortKeys(measurement); len(keys) != 1 || keys[0] != "time" {
		return nil, false
	}
	typed := make([]*TypedColumnBatch, len(batches))
	var first *TypedColumnBatch
	stored := 0
	total := 0
	for i, raw := range batches {
		tb, ok := raw.(*TypedColumnBatch)
		if !ok {
			return nil, false
		}
		t, ok := tb.Data["time"].([]int64)
		if !ok || len(t) == 0 {
			return nil, false
		}
		n := 0
		for name, col := range tb.Data {
			if unstoredColumn(name) {
				continue
			}
			n++
			if columnLen(col) != len(t) {
				return nil, false
			}
			if i > 0 && !sameColumnType(first.Data[name], col) {
				return nil, false
			}
		}
		if i == 0 {
			first, stored = tb, n
		} else if n != stored {
			return nil, false
		}
		typed[i] = tb
		total += len(t)
	}

	// The time column, concatenated; its rows' batches.
	times := make([]int64, 0, total)
	src := make([]int32, total)
	for i, tb := range typed {
		start := len(times)
		times = append(times, tb.Data["time"].([]int64)...)
		for j := start; j < len(times); j++ {
			src[j] = int32(i)
		}
	}
	minT, maxT := times[0], times[0]
	for _, t := range times {
		minT, maxT = min(minT, t), max(maxT, t)
	}
	if HourBucketID(minT) != HourBucketID(maxT) {
		return nil, false
	}
	perm := permuteByTime(times) // nil: already in order
	// Row p of the concatenation is row p-off[src[p]] of batch src[p].
	off := make([]int, len(typed))
	for i := 1; i < len(typed); i++ {
		off[i] = off[i-1] + len(typed[i-1].Data["time"].([]int64))
	}

	out := &TypedColumnBatch{Data: make(map[string]interface{}, stored)}
	if perm == nil {
		out.Data["time"] = times
	} else {
		sorted := make([]int64, total)
		for i, p := range perm {
			sorted[i] = times[p]
		}
		out.Data["time"] = sorted
	}
	for name, col := range first.Data {
		if unstoredColumn(name) || name == "time" {
			continue
		}
		switch col.(type) {
		case []int64:
			out.Data[name] = gatherMerged[int64](typed, name, perm, src, off)
		case []float64:
			out.Data[name] = gatherMerged[float64](typed, name, perm, src, off)
		case []string:
			out.Data[name] = gatherMerged[string](typed, name, perm, src, off)
		case []bool:
			out.Data[name] = gatherMerged[bool](typed, name, perm, src, off)
		case []decimal128.Num:
			out.Data[name] = gatherMerged[decimal128.Num](typed, name, perm, src, off)
		default:
			return nil, false
		}
	}

	// NULLs: a column with a NULL in any batch gets a bitmap (nil entry or
	// none = all valid in that batch).
	tagSet := map[string]struct{}{}
	for _, tb := range typed {
		for _, tag := range tb.TagColumns {
			tagSet[tag] = struct{}{}
		}
		out.DedupTime = out.DedupTime || tb.DedupTime
		for name, valid := range tb.Validity {
			if valid == nil || unstoredColumn(name) || out.Validity[name] != nil {
				continue
			}
			if _, ok := out.Data[name]; !ok {
				continue
			}
			if out.Validity == nil {
				out.Validity = map[string][]bool{}
			}
			out.Validity[name] = gatherValidity(typed, name, perm, src, off)
		}
	}
	for name, valid := range out.Validity {
		allValid := true
		for _, v := range valid {
			if !v {
				allValid = false
				break
			}
		}
		if allValid {
			delete(out.Validity, name)
		}
	}
	if len(out.Validity) == 0 {
		out.Validity = nil
	}
	if len(tagSet) > 0 {
		out.TagColumns = make([]string, 0, len(tagSet))
		for tag := range tagSet {
			out.TagColumns = append(out.TagColumns, tag)
		}
	}
	return out, true
}

// gatherMerged is one column in sorted order: row i is the concatenation's
// row perm[i] (i itself when perm is nil), read from its batch.
func gatherMerged[T any](typed []*TypedColumnBatch, name string, perm []int, src []int32, off []int) []T {
	cols := make([][]T, len(typed))
	for i, tb := range typed {
		cols[i] = tb.Data[name].([]T)
	}
	out := make([]T, len(src))
	if perm == nil {
		k := 0
		for _, c := range cols {
			k += copy(out[k:], c)
		}
		return out
	}
	for i, p := range perm {
		b := src[p]
		out[i] = cols[b][p-off[b]]
	}
	return out
}

// gatherValidity is a column's NULL bitmap in sorted order.
func gatherValidity(typed []*TypedColumnBatch, name string, perm []int, src []int32, off []int) []bool {
	valids := make([][]bool, len(typed))
	for i, tb := range typed {
		valids[i] = tb.Validity[name] // nil: all valid
	}
	out := make([]bool, len(src))
	for i := range out {
		p := i
		if perm != nil {
			p = perm[i]
		}
		v, r := valids[src[p]], p-off[src[p]]
		out[i] = v == nil || (r < len(v) && v[r])
	}
	return out
}

func sameColumnType(a, b interface{}) bool {
	switch a.(type) {
	case []int64:
		_, ok := b.([]int64)
		return ok
	case []float64:
		_, ok := b.([]float64)
		return ok
	case []string:
		_, ok := b.([]string)
		return ok
	case []bool:
		_, ok := b.([]bool)
		return ok
	case []decimal128.Num:
		_, ok := b.([]decimal128.Num)
		return ok
	}
	return false
}

func columnLen(col interface{}) int {
	switch c := col.(type) {
	case []int64:
		return len(c)
	case []float64:
		return len(c)
	case []string:
		return len(c)
	case []bool:
		return len(c)
	case []decimal128.Num:
		return len(c)
	}
	return -1
}
