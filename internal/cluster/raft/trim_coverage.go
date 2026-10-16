package raft

import (
	"sort"
	"strings"
)

// TrimQuery is one (instance, key) whose batches a standby writer wants to
// drop from its copy of the primary's stream (#1266), with those batches.
type TrimQuery struct {
	Instance uint64
	Key      string // buffer key, "database/measurement"
	CSeqs    [][2]uint64
}

// TrimFile is a current file a standby must hold before it drops a batch.
type TrimFile struct {
	Path      string
	SizeBytes int64
}

// TrimFilesFor answers, per query, whether the instance is tracked (known
// and indexed), and which current hot files hold rows of the queried
// batches: those with a coverage entry for (Instance, Key) whose batches
// intersect CSeqs. The entry may be the file's own, inherited from a file a
// DELETE rewrite replaced, or a claimant's (ClaimEpoch set): a vanish takes
// back every one of them.
//
// No other file is required. Rows of the instance in a file without such an
// entry (compaction outputs, a replay that dropped the identity, a rewrite
// in another hour) are never republished from the copy: only a file's own
// coverage entries become claimable when it vanishes, and settled batches
// are never claimable. Keeping those rows is the compaction commit's job
// (peers pull a staged output before its inputs are deleted). An untracked
// instance is never claimed, so its batches need no file at all.
//
// Hot is HotTier, a superset of the claimant's "" / "hot": requiring more
// files only keeps batches longer.
//
// It runs under the FSM read lock on every trim pass, so an entry is
// rejected on its bounds before its batches are built, and compared only
// with the queries whose span it can reach.
func (f *ClusterFSM) TrimFilesFor(qs []TrimQuery) ([]bool, [][]TrimFile) {
	tracked := make([]bool, len(qs))
	files := make([][]TrimFile, len(qs))
	type ik struct {
		inst uint64
		key  string
	}
	type group struct {
		lo, hi uint64
		reqs   []int // sorted by their lowest batch
	}
	groups := map[ik]*group{}
	norm := make([]Intervals, len(qs))
	dbs := map[string]bool{}

	f.mu.RLock()
	defer f.mu.RUnlock()
	for i, q := range qs {
		if !trackedLocked(f.takeover, q.Instance) {
			continue
		}
		tracked[i] = true
		// A key without a database names no file; the claimant never
		// replays one, so nothing is required for it.
		db, _, ok := strings.Cut(q.Key, "/")
		norm[i] = normalized(q.CSeqs)
		if !ok || len(norm[i]) == 0 {
			continue
		}
		lo, hi := norm[i][0][0], norm[i][len(norm[i])-1][1]
		g := groups[ik{q.Instance, q.Key}]
		if g == nil {
			g = &group{lo: lo, hi: hi}
			groups[ik{q.Instance, q.Key}] = g
		}
		g.lo, g.hi = min(g.lo, lo), max(g.hi, hi)
		g.reqs = append(g.reqs, i)
		dbs[db] = true
	}
	for _, g := range groups {
		sort.Slice(g.reqs, func(a, b int) bool { return norm[g.reqs[a]][0][0] < norm[g.reqs[b]][0][0] })
	}
	for db := range dbs {
		for path := range f.filesByDB[db] {
			e := f.files[path]
			if e == nil || !HotTier(e.Tier) {
				continue
			}
			for _, c := range e.Coverage {
				g := groups[ik{c.Instance, c.Key}]
				if g == nil {
					continue
				}
				elo, ehi, found := coverageBounds(c)
				if !found || ehi < g.lo || elo > g.hi {
					continue
				}
				cs := c.cseqs()
				// Queries starting above the entry cannot meet it.
				n := sort.Search(len(g.reqs), func(k int) bool { return norm[g.reqs[k]][0][0] > ehi })
				for _, i := range g.reqs[:n] {
					v := norm[i]
					if v[len(v)-1][1] < elo || !intersects(cs, v) {
						continue
					}
					// A file's entries are walked together: one check dedupes.
					if l := len(files[i]); l == 0 || files[i][l-1].Path != e.Path {
						files[i] = append(files[i], TrimFile{Path: e.Path, SizeBytes: e.SizeBytes})
					}
				}
			}
		}
	}
	return tracked, files
}

// coverageBounds returns the lowest and highest batch of c, and whether it
// names one.
func coverageBounds(c InstanceCoverage) (lo, hi uint64, found bool) {
	for _, r := range c.Ranges {
		if !found || r[0] < lo {
			lo = r[0]
		}
		if !found || r[1] > hi {
			hi = r[1]
		}
		found = true
	}
	for _, p := range c.Partial {
		if !found || p.CSeq < lo {
			lo = p.CSeq
		}
		if !found || p.CSeq > hi {
			hi = p.CSeq
		}
		found = true
	}
	return lo, hi, found
}

// intersects reports whether two sorted, disjoint interval lists share a
// value.
func intersects(a, b Intervals) bool {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i][1] < b[j][0]:
			i++
		case b[j][1] < a[i][0]:
			j++
		default:
			return true
		}
	}
	return false
}
