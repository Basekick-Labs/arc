package raft

import "sort"

// Intervals is a sorted list of disjoint, non-adjacent closed intervals of
// batch sequences. Every operation returns a new or reused slice; none keeps
// a reference to its argument.
type Intervals [][2]uint64

func (v Intervals) contains(x uint64) bool {
	i := sort.Search(len(v), func(i int) bool { return v[i][1] >= x })
	return i < len(v) && v[i][0] <= x
}

// overlaps reports whether any of [lo, hi] is in v.
func (v Intervals) overlaps(lo, hi uint64) bool {
	i := sort.Search(len(v), func(i int) bool { return v[i][1] >= lo })
	return i < len(v) && v[i][0] <= hi
}

// covers reports whether all of [lo, hi] is in v.
func (v Intervals) covers(lo, hi uint64) bool {
	i := sort.Search(len(v), func(i int) bool { return v[i][1] >= lo })
	return i < len(v) && v[i][0] <= lo && v[i][1] >= hi
}

// addRange returns v with [lo, hi] added.
func (v Intervals) addRange(lo, hi uint64) Intervals {
	if lo > hi {
		return v
	}
	var out Intervals
	placed := false
	for _, iv := range v {
		switch {
		case iv[1] < lo && iv[1]+1 < lo:
			out = append(out, iv)
		case hi < iv[0] && hi+1 < iv[0]:
			if !placed {
				out = append(out, [2]uint64{lo, hi})
				placed = true
			}
			out = append(out, iv)
		default: // overlapping or adjacent: absorb
			if iv[0] < lo {
				lo = iv[0]
			}
			if iv[1] > hi {
				hi = iv[1]
			}
		}
	}
	if !placed {
		out = append(out, [2]uint64{lo, hi})
	}
	return out
}

// removeRange returns v without [lo, hi].
func (v Intervals) removeRange(lo, hi uint64) Intervals {
	if lo > hi {
		return v
	}
	var out Intervals
	for _, iv := range v {
		if iv[1] < lo || iv[0] > hi {
			out = append(out, iv)
			continue
		}
		if iv[0] < lo {
			out = append(out, [2]uint64{iv[0], lo - 1})
		}
		if iv[1] > hi {
			out = append(out, [2]uint64{hi + 1, iv[1]})
		}
	}
	return out
}

// clip returns the part of v within [lo, hi].
func (v Intervals) clip(lo, hi uint64) Intervals {
	var out Intervals
	if lo > hi {
		return out
	}
	for _, iv := range v {
		a, b := iv[0], iv[1]
		if b < lo || a > hi {
			continue
		}
		if a < lo {
			a = lo
		}
		if b > hi {
			b = hi
		}
		out = append(out, [2]uint64{a, b})
	}
	return out
}

// minus returns v without every interval of w.
func (v Intervals) minus(w Intervals) Intervals {
	out := v
	for _, iv := range w {
		out = out.removeRange(iv[0], iv[1])
	}
	return out
}

// union returns v with every interval of w, in one pass over both.
func (v Intervals) union(w Intervals) Intervals {
	if len(w) == 0 {
		return append(Intervals(nil), v...)
	}
	all := append(append([][2]uint64(nil), v...), w...)
	return normalized(all)
}

// normalized builds a valid Intervals value from arbitrary pairs (wire
// input): sorted, then merged in one pass.
func normalized(pairs [][2]uint64) Intervals {
	v := make(Intervals, 0, len(pairs))
	for _, p := range pairs {
		if p[0] <= p[1] {
			v = append(v, p)
		}
	}
	sort.Slice(v, func(i, j int) bool { return v[i][0] < v[j][0] })
	out := v[:0]
	for _, p := range v {
		if n := len(out); n > 0 && (p[0] <= out[n-1][1] || p[0]-1 == out[n-1][1]) {
			if p[1] > out[n-1][1] {
				out[n-1][1] = p[1]
			}
			continue
		}
		out = append(out, p)
	}
	return out
}

// Exported set operations for the claimant (package cluster).

// Contains reports whether x is in v.
func (v Intervals) Contains(x uint64) bool { return v.contains(x) }

// Minus returns v without every interval of w.
func (v Intervals) Minus(w Intervals) Intervals { return v.minus(w) }

// Union returns v with every interval of w.
func (v Intervals) Union(w Intervals) Intervals { return v.union(w) }

// Count is the number of sequences in v.
func (v Intervals) Count() uint64 {
	var n uint64
	for _, iv := range v {
		n += iv[1] - iv[0] + 1
	}
	return n
}

// NormalizedIntervals builds a valid Intervals value from arbitrary pairs.
func NormalizedIntervals(pairs [][2]uint64) Intervals { return normalized(pairs) }

// complementRanges returns the parts of [0, total) outside the sorted row
// ranges rs.
func complementRanges(rs [][2]int, total int) [][2]int {
	var out [][2]int
	cur := 0
	for _, r := range rs {
		if r[0] > cur {
			hi := r[0]
			if hi > total {
				hi = total
			}
			out = append(out, [2]int{cur, hi})
		}
		if r[1] > cur {
			cur = r[1]
		}
	}
	if cur < total {
		out = append(out, [2]int{cur, total})
	}
	return out
}
