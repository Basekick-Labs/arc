package cluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
)

// A compaction output a row DELETE rewrote, whose old object is still in the
// bucket, is a leftover: never adopted while the manifest lists its rewrite,
// so the reconciler treats it as an orphan rather than registering a second
// copy of its rows.
func TestRepairDoesNotAdoptARewriteLeftover(t *testing.T) {
	r := objectRig(t)
	ctx := context.Background()
	rw := strings.TrimSuffix(repairOutput, ".parquet") + "_rewrite_1792000000000000000.parquet"
	r.register("listed.parquet")
	r.writeOld(repairOutput, "the old rows")
	if !r.c.AdoptableObject(ctx, repairOutput) {
		t.Fatal("precondition: an unlisted compaction output is adoptable")
	}
	sha := r.writeOld(rw, "the rows a DELETE kept")
	if err := r.c.raftNode.RegisterFile(raft.FileEntry{Path: rw, SHA256: sha, SizeBytes: int64(len("the rows a DELETE kept")),
		Database: "testdb", Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if r.c.AdoptableObject(ctx, repairOutput) {
		t.Fatal("a leftover whose rewrite is listed is adoptable")
	}
	if !r.c.HasRewriteSibling(repairOutput) || r.c.HasRewriteSibling(rw) || r.c.HasRewriteSibling(stHour+"cpu_other.parquet") {
		t.Fatal("HasRewriteSibling: only another listed member counts")
	}
	if err := r.c.repairManifest(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.c.raftNode.FSM().GetFile(repairOutput); ok {
		t.Fatal("the repair registered the leftover")
	}
}

// A compaction output the DELETE journal names is never adopted; the
// reconciler keeps it too (it is the drainer's), rather than removing it as
// an orphan.
func TestJournaledObjectIsNeitherAdoptedNorAnOrphan(t *testing.T) {
	r := objectRig(t)
	ctx := context.Background()
	r.register("listed.parquet")
	r.writeOld(repairOutput, "the old rows")
	if !r.c.AdoptableObject(ctx, repairOutput) {
		t.Fatal("precondition: adoptable")
	}
	if err := r.store.Write(ctx, storage.RetiredKey(repairOutput), []byte(`{"v":1,"path":"`+repairOutput+`","replaced_by":"x"}`)); err != nil {
		t.Fatal(err)
	}
	r.c.retiredJournal.mu.Lock()
	r.c.retiredJournal.at = time.Time{} // drop the cached view
	r.c.retiredJournal.mu.Unlock()
	if r.c.AdoptableObject(ctx, repairOutput) {
		t.Fatal("a journaled object is adoptable")
	}
	if !r.c.ReconcilerKeeps(ctx, repairOutput) {
		t.Fatal("the reconciler would remove a journaled object as an orphan")
	}
	if r.c.ReconcilerKeeps(ctx, stHour+"stray.parquet") {
		t.Fatal("the reconciler keeps an unrelated unlisted object")
	}
}
