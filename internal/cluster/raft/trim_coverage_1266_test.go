package raft

import (
	"sort"
	"testing"
)

// TrimFilesFor requires exactly the current hot files whose coverage entry
// for the queried (instance, key) holds one of the queried batches: own,
// inherited from a DELETE rewrite, or a claimant's (#1266).
func TestTrimFilesFor(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	x.announce("w2", 8, Continuity{})
	x.ok(CommandAnnounceInstance, AnnounceInstancePayload{Origin: "w3", Instance: 9}) // not a fresh start: untracked
	if r := x.register(regPayload("db/cpu/own.parquet", "w1", hourA, cov(7, "db/cpu", 1, 5))); r != "" {
		t.Fatalf("register: %s", r)
	}
	if r := x.register(regPayload("db/cpu/partial.parquet", "w1", hourB, InstanceCoverage{Instance: 7, Key: "db/cpu",
		Partial: []CoveragePiece{{CSeq: 40, RowLo: 0, RowHi: 1, RowTotal: 2}}})); r != "" {
		t.Fatalf("register: %s", r)
	}
	if r := x.register(regPayload("db/cpu/other-instance.parquet", "w2", hourA, cov(8, "db/cpu", 1, 100))); r != "" {
		t.Fatalf("register: %s", r)
	}
	// A DELETE rewrite of a file of batches 10-12: the rewrite inherits its
	// coverage, as production registers it (rewrite, then delete the original).
	if r := x.register(regPayload("db/cpu/pre-delete.parquet", "w1", hourA, cov(7, "db/cpu", 10, 12))); r != "" {
		t.Fatalf("register: %s", r)
	}
	rw := regPayload("db/cpu/rewrite.parquet", "w1", hourA)
	rw.InheritFrom, rw.Supersedes = "db/cpu/pre-delete.parquet", "db/cpu/pre-delete.parquet"
	if refused := x.batch(regOp(t, rw), delOp(t, "db/cpu/pre-delete.parquet", "delete")); refused != nil {
		t.Fatalf("rewrite refused: %+v", refused)
	}
	if e := x.f.files["db/cpu/rewrite.parquet"]; e == nil || !e.CoverageInherited || len(e.Coverage) != 1 {
		t.Fatalf("rewrite did not inherit coverage: %+v", e)
	}
	// A compaction output: registered with no coverage.
	if r := x.register(regPayload("db/cpu/compacted.parquet", "w1", hourA)); r != "" {
		t.Fatalf("register: %s", r)
	}
	// Coverage the FSM derives from a claim, a truncation or a tier move, set
	// directly: a claimant's entry, a truncated entry, a cold file, another key.
	plain := func(path, tier string, inherited bool, c ...InstanceCoverage) {
		x.ok(CommandRegisterFile, RegisterFilePayload{File: FileEntry{Path: path, SHA256: path, SizeBytes: 3, Database: "db",
			Measurement: "cpu", PartitionTime: t0, OriginNodeID: "w1", CreatedAt: t0, Tier: tier}})
		x.f.mu.Lock()
		x.f.files[path].Coverage, x.f.files[path].CoverageInherited = c, inherited
		x.f.mu.Unlock()
	}
	plain("db/cpu/claimed.parquet", "hourly", false, InstanceCoverage{Instance: 7, Key: "db/cpu", Ranges: [][2]uint64{{20, 20}}, ClaimEpoch: 3})
	plain("db/cpu/two-entries.parquet", "", false, cov(7, "db/cpu", 50, 50), InstanceCoverage{Instance: 7, Key: "db/cpu",
		Partial: []CoveragePiece{{CSeq: 51, RowLo: 0, RowHi: 1, RowTotal: 2}}})
	plain("db/cpu/truncated.parquet", "", false, InstanceCoverage{Instance: 7, Key: "db/cpu", Truncated: true})
	plain("db/cpu/cold.parquet", "cold", false, cov(7, "db/cpu", 30, 30))
	plain("db/cpu/mem-key.parquet", "", false, cov(7, "db/mem", 1, 100))

	cases := []struct {
		name    string
		q       TrimQuery
		tracked bool
		want    []string
	}{
		{"own entry intersects", TrimQuery{7, "db/cpu", [][2]uint64{{3, 3}}}, true, []string{"db/cpu/own.parquet"}},
		{"own entry, other batches", TrimQuery{7, "db/cpu", [][2]uint64{{6, 9}}}, true, nil},
		{"partial piece", TrimQuery{7, "db/cpu", [][2]uint64{{40, 40}}}, true, []string{"db/cpu/partial.parquet"}},
		{"inherited entry", TrimQuery{7, "db/cpu", [][2]uint64{{11, 11}}}, true, []string{"db/cpu/rewrite.parquet"}},
		{"claim epoch entry", TrimQuery{7, "db/cpu", [][2]uint64{{20, 20}}}, true, []string{"db/cpu/claimed.parquet"}},
		{"one file, two entries", TrimQuery{7, "db/cpu", [][2]uint64{{50, 51}}}, true, []string{"db/cpu/two-entries.parquet"}},
		{"cold file", TrimQuery{7, "db/cpu", [][2]uint64{{30, 30}}}, true, nil},
		{"span", TrimQuery{7, "db/cpu", [][2]uint64{{4, 10}, {19, 21}}}, true,
			[]string{"db/cpu/claimed.parquet", "db/cpu/own.parquet", "db/cpu/rewrite.parquet"}},
		{"other instance's file", TrimQuery{8, "db/cpu", [][2]uint64{{3, 3}}}, true, []string{"db/cpu/other-instance.parquet"}},
		{"untracked instance", TrimQuery{9, "db/cpu", [][2]uint64{{1, 1}}}, false, nil},
		{"unknown instance", TrimQuery{99, "db/cpu", [][2]uint64{{1, 1}}}, false, nil},
	}
	qs := make([]TrimQuery, len(cases))
	for i, c := range cases {
		qs[i] = c.q
	}
	tracked, files := x.f.TrimFilesFor(qs)
	for i, c := range cases {
		var got []string
		for _, f := range files[i] {
			got = append(got, f.Path)
		}
		sort.Strings(got)
		if tracked[i] != c.tracked || !equalStrings(got, c.want) {
			t.Errorf("%s: tracked %v files %v, want %v %v", c.name, tracked[i], got, c.tracked, c.want)
		}
	}
}

// An entry whose highest batch is the lowest one asked for is required:
// the bounds check keeps it.
func TestTrimFilesForEdgeOfSpan(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	if r := x.register(regPayload("db/cpu/own.parquet", "w1", hourA, cov(7, "db/cpu", 1, 5))); r != "" {
		t.Fatalf("register: %s", r)
	}
	for _, q := range [][2]uint64{{5, 9}, {0, 1}} {
		_, files := x.f.TrimFilesFor([]TrimQuery{{7, "db/cpu", [][2]uint64{q}}})
		if len(files[0]) != 1 {
			t.Fatalf("query %v: files %v, want own.parquet", q, files[0])
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestIntersects(t *testing.T) {
	for _, c := range []struct {
		a, b Intervals
		want bool
	}{
		{Intervals{{1, 3}}, Intervals{{3, 5}}, true},
		{Intervals{{1, 3}}, Intervals{{4, 5}}, false},
		{Intervals{{1, 1}, {10, 12}}, Intervals{{2, 9}, {12, 12}}, true},
		{Intervals{{1, 1}, {10, 12}}, Intervals{{2, 9}, {13, 20}}, false},
		{nil, Intervals{{1, 1}}, false},
	} {
		if got := intersects(c.a, c.b); got != c.want || intersects(c.b, c.a) != c.want {
			t.Errorf("intersects(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}
