package cluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

func pendingDeletes(c *Coordinator) []string {
	c.deletePendingMu.Lock()
	defer c.deletePendingMu.Unlock()
	var out []string
	for _, d := range c.deletePending {
		out = append(out, d.path)
	}
	return out
}

// A local copy of a file the cluster deleted as vanished is removed; one
// registered again, waiting to register, or of another size is not.
func TestVanishedCopiesAreSwept(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	reg := func(path string) {
		propose(t, r.c, raft.CommandRegisterFile, raft.RegisterFilePayload{
			File: raft.FileEntry{Path: path, SHA256: "x", SizeBytes: 1, Database: "db", Measurement: "cpu",
				PartitionTime: r.hour, OriginNodeID: clOrigin, CreatedAt: time.Now()},
			Cov2: []raft.InstanceCoverage{{Instance: clInst, Key: clKey, Ranges: [][2]uint64{{1, 1}}}}})
	}
	for _, p := range []string{"db/cpu/a.parquet", "db/cpu/b.parquet", "db/cpu/c.parquet", "db/cpu/d.parquet"} {
		reg(p)
		propose(t, r.c, raft.CommandDeleteFile, raft.DeleteFilePayload{Path: p, Reason: raft.DeleteReasonVanished})
		data := []byte("x")
		if strings.HasSuffix(p, "c.parquet") {
			data = []byte("xy") // another file at that path
		}
		if err := r.backend.Write(ctx, p, data); err != nil {
			t.Fatal(err)
		}
	}
	reg("db/cpu/b.parquet") // registered again
	pending := func(p string) bool { return p == "db/cpu/d.parquet" }
	r.c.pendingRegistration.Store(&pending)
	st := &vanishedSweepState{swept: map[string]bool{}}
	r.c.vanishedSweep(ctx, st)
	if got := pendingDeletes(r.c); len(got) != 1 || got[0] != "db/cpu/a.parquet" || !st.swept["db/cpu/a.parquet"] {
		t.Fatalf("deletes %v", got)
	}
	st.lastSig = "" // force a pass: swept paths are not handed over twice
	r.c.vanishedSweep(ctx, st)
	if got := pendingDeletes(r.c); len(got) != 1 {
		t.Fatalf("swept twice: %v", got)
	}
	// The list unchanged: no pass (a new copy waits for the next one).
	if err := r.backend.Write(ctx, "db/cpu/d.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	pending = func(string) bool { return false }
	r.c.vanishedSweep(ctx, st)
	if got := pendingDeletes(r.c); len(got) != 1 {
		t.Fatalf("passed with the list unchanged: %v", got)
	}
	st.lastAt = time.Now().Add(-vanishedResweepEvery)
	r.c.vanishedSweep(ctx, st)
	if got := pendingDeletes(r.c); len(got) != 2 {
		t.Fatalf("periodic pass: %v", got)
	}
}

// A writer does not sweep before it knows its own pending registrations.
func TestVanishedSweepWaitsForThePendingGuard(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	propose(t, r.c, raft.CommandRegisterFile, raft.RegisterFilePayload{
		File: raft.FileEntry{Path: "db/cpu/a.parquet", SHA256: "x", SizeBytes: 1, Database: "db", Measurement: "cpu",
			PartitionTime: r.hour, OriginNodeID: clOrigin, CreatedAt: time.Now()},
		Cov2: []raft.InstanceCoverage{{Instance: clInst, Key: clKey, Ranges: [][2]uint64{{1, 1}}}}})
	propose(t, r.c, raft.CommandDeleteFile, raft.DeleteFilePayload{Path: "db/cpu/a.parquet", Reason: raft.DeleteReasonVanished})
	if err := r.backend.Write(ctx, "db/cpu/a.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	r.c.pendingRegistration.Store(nil)
	r.c.vanishedSweep(ctx, &vanishedSweepState{swept: map[string]bool{}})
	if got := pendingDeletes(r.c); len(got) != 0 {
		t.Fatalf("swept without the guard: %v", got)
	}
}
