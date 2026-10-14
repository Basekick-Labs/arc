package cluster

// #1269: with more missing files than one check probes, every file's wait
// starts when it is first seen missing — not when it first gets a probe — so
// they are taken back together after twice the grace, not in waves; and a
// file is taken back only on an answer from the same check.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

func TestUnpulledFilesAreTakenBackTogether(t *testing.T) {
	r := newClaimantRig(t)
	r.stored(t, 1, 2)
	const n = maxUnpulledProbesPerPass + 8
	paths := make([]string, n)
	for i := range paths {
		paths[i] = fmt.Sprintf("db/cpu/origin-%02d.parquet", i)
		propose(t, r.c, raft.CommandRegisterFile, raft.RegisterFilePayload{
			File: raft.FileEntry{Path: paths[i], SHA256: "x", SizeBytes: 1, Database: "db", Measurement: "cpu",
				PartitionTime: r.hour, OriginNodeID: clOrigin, CreatedAt: time.Now()},
			Cov2: []raft.InstanceCoverage{{Instance: clInst, Key: clKey, Partial: []raft.CoveragePiece{{CSeq: 1, RowLo: i, RowHi: i + 1, RowTotal: 1000}}}}})
	}
	asked := map[string]int{}
	r.cl.probe = func(_ context.Context, _ string, e raft.FileEntry) probeAnswer { asked[e.Path]++; return probeUnknown }
	r.healthyPeer(t)
	k := raft.StreamClaim{Instance: clInst, Origin: clOrigin}
	w := r.cl.workFor(k)
	fsm := r.c.raftNode.FSM()
	t0 := time.Now()
	// check runs a check at t0+d, past the throttle; again runs one without
	// clearing the throttle (only a removal may have cleared it).
	run := func(d time.Duration) {
		now := t0.Add(d)
		r.cl.now = func() time.Time { return now }
		r.cl.unpulledSettled(context.Background(), fsm, k, w)
	}
	at := func(d time.Duration) { w.unpulledAt = time.Time{}; run(d) }
	left := func() int {
		c := 0
		for _, p := range paths {
			if _, ok := fsm.GetFile(p); ok {
				c++
			}
		}
		return c
	}

	at(0)
	if len(w.unknownSince) != n {
		t.Fatalf("waits started for %d of %d missing files", len(w.unknownSince), n)
	}
	// The files not probed first get asked on the next checks.
	at(unpulledEvery)
	at(2 * unpulledEvery)
	for _, p := range paths {
		if asked[p] == 0 {
			t.Fatalf("%s never asked about within three checks", p)
		}
	}
	at(2*r.cl.grace - time.Second)
	if left() != n {
		t.Fatal("a file was taken back before twice the grace")
	}
	// Past the wait: one check takes back what it asked about (at most the
	// cap)...
	at(2*r.cl.grace + time.Second)
	if got := n - left(); got != maxUnpulledProbesPerPass {
		t.Fatalf("first check after the wait took back %d, want %d (only files asked in that check)", got, maxUnpulledProbesPerPass)
	}
	// ...and the next one is not throttled after a removal: the rest at once.
	run(2*r.cl.grace + 2*time.Second)
	if left() != 0 {
		t.Fatalf("%d files still registered: taken back in waves", left())
	}
}

// A check stops asking once its time budget is spent; the rest wait for the
// next check.
func TestUnpulledCheckStopsAtItsBudget(t *testing.T) {
	r := newClaimantRig(t)
	r.stored(t, 1, 2)
	for i := 0; i < 5; i++ {
		propose(t, r.c, raft.CommandRegisterFile, raft.RegisterFilePayload{
			File: raft.FileEntry{Path: fmt.Sprintf("db/cpu/slow-%d.parquet", i), SHA256: "x", SizeBytes: 1, Database: "db", Measurement: "cpu",
				PartitionTime: r.hour, OriginNodeID: clOrigin, CreatedAt: time.Now()},
			Cov2: []raft.InstanceCoverage{{Instance: clInst, Key: clKey, Partial: []raft.CoveragePiece{{CSeq: 1, RowLo: i, RowHi: i + 1, RowTotal: 1000}}}}})
	}
	r.healthyPeer(t)
	now := time.Now()
	r.cl.now = func() time.Time { return now }
	asked := 0
	r.cl.probe = func(context.Context, string, raft.FileEntry) probeAnswer {
		asked++
		now = now.Add(unpulledCheckBudget / 2) // each probe costs half the budget
		return probeUnknown
	}
	k := raft.StreamClaim{Instance: clInst, Origin: clOrigin}
	r.cl.unpulledSettled(context.Background(), r.c.raftNode.FSM(), k, r.cl.workFor(k))
	if asked > 3 {
		t.Fatalf("asked %d files in one check; the budget allows about 2", asked)
	}
}
