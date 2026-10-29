package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
)

// #1207: the manifest of a shared object store cluster, repaired against the
// bucket.

func TestParseCompactedOutputPath(t *testing.T) {
	cases := []struct {
		path string
		ok   bool
		want time.Time
	}{
		{"db/cpu/2026/10/13/21/cpu_20261014_015442_1791942882594780436_b1_compacted.parquet", true, time.Date(2026, 10, 13, 21, 0, 0, 0, time.UTC)},
		{"db/cpu/2026/10/13/cpu_20261014_015442_17919_b2_daily.parquet", true, time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)},
		// The shape and the suffix disagree.
		{"db/cpu/2026/10/13/cpu_20261014_015442_17919_b1_compacted.parquet", false, time.Time{}},
		{"db/cpu/2026/10/13/21/cpu_20261014_015442_17919_b1_daily.parquet", false, time.Time{}},
		// Another measurement's name, a flushed file, a rewrite.
		{"db/cpu/2026/10/13/21/mem_20261014_015442_17919_b1_compacted.parquet", false, time.Time{}},
		{"db/cpu/2026/10/13/21/cpu_20261014_004824_089626747.parquet", false, time.Time{}},
		{"db/cpu/2026/10/13/21/cpu_20261014_015442_17919_b1_compacted_rewrite_1.parquet", false, time.Time{}},
		// An edge-sync spoke namespace, a reserved root, an Iceberg warehouse.
		{"spoke/db/cpu/2026/10/13/21/cpu_20261014_015442_17919_b1_compacted.parquet", false, time.Time{}},
		{"_compaction_staged/cpu/2026/10/13/21/cpu_20261014_015442_17919_b1_compacted.parquet", false, time.Time{}},
		{"arc_db.db/cpu/2026/10/13/21/cpu_20261014_015442_17919_b1_compacted.parquet", false, time.Time{}},
		// No such day or hour.
		{"db/cpu/2026/02/30/21/cpu_20261014_015442_17919_b1_compacted.parquet", false, time.Time{}},
		{"db/cpu/2026/10/13/24/cpu_20261014_015442_17919_b1_compacted.parquet", false, time.Time{}},
	}
	for _, tc := range cases {
		db, m, pt, ok := parseCompactedOutputPath(tc.path)
		if ok != tc.ok {
			t.Errorf("%s: ok %v, want %v", tc.path, ok, tc.ok)
			continue
		}
		if ok && (db != "db" || m != "cpu" || !pt.Equal(tc.want)) {
			t.Errorf("%s: %s %s %v", tc.path, db, m, pt)
		}
	}
}

const repairOutput = stHour + "cpu_20261013_100000_1791900000000000000_b1_compacted.parquet"

// writeOld writes key and dates it past the repair grace.
func (r *objectRigT) writeOld(key, body string) string {
	r.t.Helper()
	sha := r.write(key, body)
	old := time.Now().Add(-2 * manifestRepairGrace)
	if err := os.Chtimes(filepath.Join(r.root, key), old, old); err != nil {
		r.t.Fatal(err)
	}
	return sha
}

// The drift a shared object store cluster has from before #1207: inputs
// listed with their objects gone, the output that replaced them unlisted.
// The repair drops the first and registers the second.
func TestRepairManifest(t *testing.T) {
	r := objectRig(t)
	in1, in2 := r.register("in-1.parquet"), r.register("in-2.parquet")
	kept := r.register("kept.parquet")
	for _, in := range []string{in1.Path, in2.Path} {
		if err := os.Remove(filepath.Join(r.root, in)); err != nil {
			t.Fatal(err)
		}
	}
	sha := r.writeOld(repairOutput, "compacted rows")
	if err := r.c.repairManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	fsm := r.c.raftNode.FSM()
	for _, in := range []string{in1.Path, in2.Path} {
		if _, ok := fsm.GetFile(in); ok {
			t.Fatalf("%s still listed", in)
		}
	}
	if _, ok := fsm.GetFile(kept.Path); !ok {
		t.Fatal("a listed file in the bucket was dropped")
	}
	e, ok := fsm.GetFile(repairOutput)
	if !ok {
		t.Fatal("output not registered")
	}
	if e.SHA256 != sha || e.SizeBytes != int64(len("compacted rows")) || e.Tier != "hot" || e.Database != "testdb" ||
		e.Measurement != "cpu" || !e.PartitionTime.Equal(time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("registered as %+v", e)
	}
	st := r.c.manifestRepairStatus()
	if st == nil || st.Dropped != 2 || st.Adopted != 1 {
		t.Fatalf("status %+v", st)
	}
	// A second run finds nothing.
	if err := r.c.repairManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := r.c.manifestRepairStatus(); st.Dropped != 0 || st.Adopted != 0 {
		t.Fatalf("second run: %+v", st)
	}
}

// What the repair leaves alone: a compaction output younger than the grace,
// an unlisted object that is not a compaction output (a leftover the
// reconciler owns), and the output of a finalizing staged job, absent from
// its path until its finisher copies it there.
func TestRepairManifestLeavesAlone(t *testing.T) {
	r := objectRig(t)
	fresh := stHour + "cpu_20261013_100001_1791900000000000001_b1_compacted.parquet"
	r.write(fresh, "fresh")
	leftover := stHour + "cpu_20261013_004824_089626747.parquet"
	r.writeOld(leftover, "leftover")
	job := r.objectJob("j1")
	if r.exists(job.Output.Path) {
		t.Fatal("the output is in place before its finisher ran")
	}
	if err := r.c.repairManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	fsm := r.c.raftNode.FSM()
	for _, p := range []string{fresh, leftover} {
		if _, ok := fsm.GetFile(p); ok {
			t.Fatalf("%s registered", p)
		}
	}
	if _, ok := fsm.GetFile(job.Output.Path); !ok {
		t.Fatal("a finalizing output was dropped")
	}
	// The job's inputs are out of the manifest and in the bucket until the
	// finisher removes them: not adopted (their names are no compaction
	// output's anyway, so check the FSM refusal directly too).
	for _, in := range job.Inputs {
		if _, ok := fsm.GetFile(in.Path); ok {
			t.Fatalf("input %s registered again", in.Path)
		}
	}
}

// The FSM refuses an adoption of a registered path or of a path a staged
// compaction owns, whatever the caller checked before.
func TestAdoptRefusedByTheFSM(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	reg := r.register("reg.parquet")
	adopt := func(path string) error {
		raw, _ := json.Marshal(raft.RegisterFilePayload{Adopt: true, File: raft.FileEntry{Path: path, SHA256: "x", SizeBytes: 1,
			Database: "testdb", Measurement: "cpu", OriginNodeID: "writer-2", Tier: "hot", CreatedAt: time.Now()}})
		return r.c.raftNode.Apply(&raft.Command{Type: raft.CommandRegisterFile, Payload: raw}, 5*time.Second)
	}
	for _, p := range []string{reg.Path, job.Inputs[0].Path, job.Output.Path} {
		if err := adopt(p); !raft.IsManifestChanged(err) {
			t.Fatalf("%s: %v", p, err)
		}
	}
	if e, _ := r.c.raftNode.FSM().GetFile(reg.Path); e.SHA256 != reg.SHA256 {
		t.Fatal("a refused adoption changed the entry")
	}
	if err := adopt(stHour + "new.parquet"); err != nil {
		t.Fatal(err)
	}
}

// An object replaced while it is read for its checksum is not adopted.
func TestAdoptObjectChecksAfterTheRead(t *testing.T) {
	r := objectRig(t)
	r.writeOld(repairOutput, "compacted rows")
	if _, err := r.c.AdoptObject(context.Background(), repairOutput); err != nil {
		t.Fatal(err)
	}
	// Recorded metadata wins over a read.
	other := stHour + "cpu_20261013_100002_1791900000000000002_b1_compacted.parquet"
	r.writeOld(other, "rows")
	r.store.meta[other] = "recorded"
	if e, err := r.c.AdoptObject(context.Background(), other); err != nil || e.SHA256 != "recorded" {
		t.Fatalf("%+v %v", e, err)
	}
	if _, err := r.c.AdoptObject(context.Background(), stHour+"cpu_20261013_100003_1_b1_compacted.parquet"); err == nil {
		t.Fatal("an absent object adopted")
	}
}

// The budget: what a run cannot read waits for the next.
func TestRepairManifestBudget(t *testing.T) {
	r := objectRig(t)
	defer func(n int) { manifestRepairMaxAdopt = n }(manifestRepairMaxAdopt)
	manifestRepairMaxAdopt = 1
	r.register("listed.parquet") // the database is in the manifest
	r.writeOld(repairOutput, "a")
	r.writeOld(stHour+"cpu_20261013_100002_1791900000000000002_b1_compacted.parquet", "b")
	if err := r.c.repairManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := r.c.manifestRepairStatus(); st.Adopted != 1 || st.Pending != 1 {
		t.Fatalf("%+v", st)
	}
	if err := r.c.repairManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := r.c.manifestRepairStatus(); st.Adopted != 1 || st.Pending != 0 {
		t.Fatalf("%+v", st)
	}
}

// Nothing on local storage, under a pause, or off the leader.
func TestRepairManifestScope(t *testing.T) {
	r := stagedRig(t)
	if r.c.SharedObjectStore() || r.c.AdoptableObject(context.Background(), repairOutput) {
		t.Fatal("local storage is not a shared object store")
	}
	o := objectRig(t)
	o.writeOld(repairOutput, "a")
	if !o.c.AdoptableObject(context.Background(), repairOutput) {
		t.Fatal("not adoptable on a shared object store")
	}
	o.c.raftFSM = o.c.raftNode.FSM()
	now := time.Now()
	cmd, err := raft.CompactionPauseCommand(raft.SetCompactionPausePayload{Paused: true, Generation: 1,
		RequestedBy: "writer-2", Reason: "restore b1", Now: now, ExpiresAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if err := o.c.raftNode.Apply(cmd, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if !o.c.CompactionPaused() {
		t.Fatal("not paused")
	}
	o.register("listed.parquet")
	if err := o.c.repairManifest(context.Background()); !errors.Is(err, errRepairStopped) {
		t.Fatal(err)
	}
	if _, ok := o.c.raftNode.FSM().GetFile(repairOutput); ok {
		t.Fatal("adopted under a compaction pause")
	}
}

// On a shared object store a listed file whose object is gone is not one a
// DELETE waits for, unless it is a finalizing output; elsewhere it is.
func TestMissingLocalFilesSharedObjectStore(t *testing.T) {
	r := objectRig(t)
	gone := r.register("gone.parquet")
	if err := os.Remove(filepath.Join(r.root, gone.Path)); err != nil {
		t.Fatal(err)
	}
	job := r.objectJob("j1")
	missing, err := r.c.MissingLocalFiles(context.Background(), "testdb", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0] != job.Output.Path {
		t.Fatalf("missing %v, want only the finalizing output", missing)
	}
	l := stagedRig(t)
	gone = l.register("gone.parquet")
	if err := os.Remove(filepath.Join(l.root, gone.Path)); err != nil {
		t.Fatal(err)
	}
	if missing, _ := l.c.MissingLocalFiles(context.Background(), "testdb", "cpu"); len(missing) != 1 {
		t.Fatalf("local storage: %v", missing)
	}
	// Another measurement's prefix is not this DELETE's.
	if missing, _ := l.c.MissingLocalFiles(context.Background(), "testdb", "mem"); len(missing) != 0 {
		t.Fatalf("other measurement: %v", missing)
	}
}

// Compaction registered its outputs with the compaction tier name before
// #1207. They are hot files: the DELETE scan set includes them, and the
// repair moves them to "hot" (each only if unchanged), so a staged
// compaction may take them as inputs. On any storage.
func TestLegacyCompactionTierIsHot(t *testing.T) {
	for tier, hot := range map[string]bool{"": true, "hot": true, "hourly": true, "daily": true, "cold": false} {
		if raft.HotTier(tier) != hot {
			t.Errorf("HotTier(%q) = %v", tier, !hot)
		}
	}
	r := stagedRig(t)
	var keys []string
	for i, tier := range []string{"hourly", "daily", "cold"} {
		key := stHour + fmt.Sprintf("legacy-%d.parquet", i)
		sha := r.write(key, "legacy")
		e := raft.FileEntry{Path: key, SHA256: sha, SizeBytes: 6, Database: "testdb", Measurement: "cpu",
			OriginNodeID: "writer-1", Tier: tier, CreatedAt: time.Now()}
		if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	if got := r.c.ManifestHotFiles("testdb", "cpu"); len(got) != 2 {
		t.Fatalf("DELETE scan set: %v", got)
	}
	if _, _, ok := r.c.CompactionInputLookup(keys[0]); ok {
		t.Fatal("an input the FSM refuses (tier hourly) offered to a staged compaction")
	}
	if err := r.c.repairManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	fsm := r.c.raftNode.FSM()
	for i, want := range []string{"hot", "hot", "cold"} {
		if e, _ := fsm.GetFile(keys[i]); e.Tier != want {
			t.Fatalf("%s: tier %q, want %q", keys[i], e.Tier, want)
		}
	}
	if _, _, ok := r.c.CompactionInputLookup(keys[0]); !ok {
		t.Fatal("not a compaction input once hot")
	}
	if st := r.c.manifestRepairStatus(); st == nil || st.Retiered != 2 {
		t.Fatalf("status %+v", st)
	}
}

// The tier move is conditional: an entry changed meanwhile (tiering made it
// cold) is not moved back to hot.
func TestRetierIsConditional(t *testing.T) {
	r := stagedRig(t)
	key := stHour + "legacy.parquet"
	e := raft.FileEntry{Path: key, SHA256: "a", SizeBytes: 6, Database: "testdb", Measurement: "cpu",
		OriginNodeID: "writer-1", Tier: "hourly", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	stale := e
	stale.Tier = "hot"
	raw, _ := json.Marshal(raft.UpdateFilePayload{File: stale, ExpectSHA: "a", ExpectTier: "hourly"})
	e.Tier = "cold"
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandUpdateFile, Payload: raw}, 5*time.Second); !raft.IsManifestChanged(err) {
		t.Fatalf("refusal expected: %v", err)
	}
	if got, _ := r.c.raftNode.FSM().GetFile(key); got.Tier != "cold" {
		t.Fatalf("tier %q", got.Tier)
	}
	// Content changed, tier as expected: refused too.
	e.Tier, e.SHA256 = "hourly", "b"
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandUpdateFile, Payload: raw}, 5*time.Second); !raft.IsManifestChanged(err) {
		t.Fatalf("refusal expected on a changed checksum: %v", err)
	}
}

// hookStore runs hooks inside StatFile and ReadTo, to change the store or
// the manifest between the repair's steps.
type hookStore struct {
	*fakeObjectStore
	onStat func(path string)
	onRead func(path string)
	hide   string // left out of listings
}

func (h *hookStore) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	all, err := h.fakeObjectStore.ListObjects(ctx, prefix)
	out := all[:0]
	for _, o := range all {
		if o.Path != h.hide {
			out = append(out, o)
		}
	}
	return out, err
}

func (h *hookStore) StatFile(ctx context.Context, path string) (int64, error) {
	if h.onStat != nil {
		h.onStat(path)
	}
	return h.fakeObjectStore.StatFile(ctx, path)
}

func (h *hookStore) ReadTo(ctx context.Context, path string, w io.Writer) error {
	err := h.fakeObjectStore.ReadTo(ctx, path, w)
	if h.onRead != nil {
		h.onRead(path)
	}
	return err
}

// An entry registered again with other content between the repair's check
// and its drop is not dropped: the drop carries the checksum it checked.
func TestRepairManifestDropIsConditional(t *testing.T) {
	r := objectRig(t)
	gone := r.register("gone.parquet")
	if err := os.Remove(filepath.Join(r.root, gone.Path)); err != nil {
		t.Fatal(err)
	}
	hs := &hookStore{fakeObjectStore: r.store}
	hs.onStat = func(path string) {
		if path != gone.Path {
			return
		}
		hs.onStat = nil
		e := raft.FileEntry{Path: path, SHA256: "other", SizeBytes: 9, Database: "testdb", Measurement: "cpu",
			OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
		if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
			t.Error(err)
		}
	}
	other := r.register("gone-too.parquet")
	if err := os.Remove(filepath.Join(r.root, other.Path)); err != nil {
		t.Fatal(err)
	}
	r.c.storage = hs
	if err := r.c.repairManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if e, ok := r.c.raftNode.FSM().GetFile(gone.Path); !ok || e.SHA256 != "other" {
		t.Fatalf("the entry registered again was dropped: %+v", e)
	}
	// The batch was refused for it; the other entry is dropped alone.
	if _, ok := r.c.raftNode.FSM().GetFile(other.Path); ok {
		t.Fatal("an unchanged gone entry kept")
	}
}

// An object replaced while it is read for its checksum is not adopted.
func TestAdoptObjectReplacedDuringTheRead(t *testing.T) {
	r := objectRig(t)
	r.writeOld(repairOutput, "compacted rows")
	hs := &hookStore{fakeObjectStore: r.store}
	hs.onRead = func(path string) { r.write(path, "compacted rows, replaced") }
	r.c.storage = hs
	if _, err := r.c.AdoptObject(context.Background(), repairOutput); err == nil {
		t.Fatal("adopted an object that changed during the read")
	}
	if _, ok := r.c.raftNode.FSM().GetFile(repairOutput); ok {
		t.Fatal("registered")
	}
}

// A file written and registered after the listing is not dropped: the
// repair checks the store again before it drops an entry.
func TestRepairManifestRechecksAnUnlistedEntry(t *testing.T) {
	r := objectRig(t)
	late := r.register("late.parquet")
	r.c.storage = &hookStore{fakeObjectStore: r.store, hide: late.Path}
	if err := r.c.repairManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.c.raftNode.FSM().GetFile(late.Path); !ok {
		t.Fatal("dropped an entry whose object is in the store")
	}
}

// A path the tiering metadata records as cold (a hot copy tiering failed to
// delete) is never adopted.
func TestRepairManifestSkipsAColdPath(t *testing.T) {
	r := objectRig(t)
	r.register("listed.parquet")
	r.writeOld(repairOutput, "a")
	r.c.SetColdPathCheck(func(_ context.Context, p string) bool { return p == repairOutput })
	if err := r.c.repairManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.c.raftNode.FSM().GetFile(repairOutput); ok {
		t.Fatal("a cold path adopted")
	}
}

// An object larger than the whole read budget is read alone rather than
// never; while adoptions wait, no entry of the measurement is dropped.
func TestRepairManifestBudgetEdges(t *testing.T) {
	r := objectRig(t)
	defer func(n int64) { manifestRepairMaxBytes = n }(manifestRepairMaxBytes)
	manifestRepairMaxBytes = 3
	gone := r.register("gone.parquet")
	if err := os.Remove(filepath.Join(r.root, gone.Path)); err != nil {
		t.Fatal(err)
	}
	r.writeOld(repairOutput, "larger than the budget")
	r.writeOld(stHour+"cpu_20261013_100002_1791900000000000002_b1_compacted.parquet", "also large")
	if err := r.c.repairManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := r.c.manifestRepairStatus()
	if st.Adopted != 1 || st.Pending != 1 || st.Dropped != 0 {
		t.Fatalf("first run %+v", st)
	}
	if err := r.c.repairManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := r.c.manifestRepairStatus(); st.Adopted != 1 || st.Pending != 0 || st.Dropped != 1 {
		t.Fatalf("second run %+v", st)
	}
}

// A DELETE rewrite of an object the manifest did not list is registered at
// its new path; nothing is registered off a shared object store.
func TestAdoptRewrite(t *testing.T) {
	r := objectRig(t)
	p := stHour + "cpu_rewrite_1.parquet"
	if _, err := r.c.AdoptRewrite(context.Background(), p, 5, "abc"); err != nil {
		t.Fatal(err)
	}
	if e, ok := r.c.raftNode.FSM().GetFile(p); !ok || e.SHA256 != "abc" || e.Tier != "hot" || e.Measurement != "cpu" {
		t.Fatalf("%+v", e)
	}
	if _, err := r.c.AdoptRewrite(context.Background(), p, 5, "abc"); !raft.IsManifestChanged(err) {
		t.Fatalf("registered twice: %v", err)
	}
	l := stagedRig(t)
	if _, err := l.c.AdoptRewrite(context.Background(), p, 5, "abc"); err == nil {
		t.Fatal("registered on local storage")
	}
}

// Inside a batch too, the FSM refuses an adoption of a registered path and
// an update whose expectation fails, and applies nothing of the batch.
func TestBatchRefusesAdoptAndConditionalUpdate(t *testing.T) {
	r := objectRig(t)
	reg := r.register("reg.parquet")
	fresh := raft.FileEntry{Path: stHour + "fresh.parquet", SHA256: "f", SizeBytes: 1, Database: "testdb", Measurement: "cpu",
		OriginNodeID: "writer-2", Tier: "hot", CreatedAt: time.Now()}
	batch := func(ops ...raft.BatchFileOp) error {
		raw, _ := json.Marshal(raft.BatchFileOpsPayload{Ops: ops})
		return r.c.raftNode.Apply(&raft.Command{Type: raft.CommandBatchFileOps, Payload: raw}, 5*time.Second)
	}
	op := func(typ raft.CommandType, p any) raft.BatchFileOp {
		raw, _ := json.Marshal(p)
		return raft.BatchFileOp{Type: typ, Payload: raw}
	}
	again := raft.FileEntry{Path: reg.Path, SHA256: "x", SizeBytes: 1, Database: "testdb", Measurement: "cpu",
		OriginNodeID: "writer-2", Tier: "hot", CreatedAt: time.Now()}
	if err := batch(op(raft.CommandRegisterFile, raft.RegisterFilePayload{File: fresh}),
		op(raft.CommandRegisterFile, raft.RegisterFilePayload{File: again, Adopt: true})); !raft.IsManifestChanged(err) {
		t.Fatalf("adopt in a batch: %v", err)
	}
	up, _ := r.c.raftNode.FSM().GetFile(reg.Path)
	moved := *up
	moved.Tier = "hot"
	if err := batch(op(raft.CommandRegisterFile, raft.RegisterFilePayload{File: fresh}),
		op(raft.CommandUpdateFile, raft.UpdateFilePayload{File: moved, ExpectSHA: up.SHA256, ExpectTier: "hourly"})); !raft.IsManifestChanged(err) {
		t.Fatalf("conditional update in a batch: %v", err)
	}
	if _, ok := r.c.raftNode.FSM().GetFile(fresh.Path); ok {
		t.Fatal("a refused batch applied part of itself")
	}
}

// With stream claims on, nothing is adopted (a claim decides what an
// unregistered batch of a gone writer becomes).
func TestNoAdoptionWithStreamClaims(t *testing.T) {
	r := objectRig(t)
	r.writeOld(repairOutput, "a")
	var ids []string
	for _, n := range r.c.raftNode.FSM().GetAllNodes() {
		ids = append(ids, n.ID)
	}
	raw, _ := json.Marshal(raft.EnableStreamClaimsPayload{Nodes: ids})
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandEnableStreamClaims, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if !r.c.StreamClaimsEnabled() {
		t.Fatal("claims not on")
	}
	if r.c.ManifestAdoption() || r.c.AdoptableObject(context.Background(), repairOutput) {
		t.Fatal("adoption with stream claims on")
	}
}

// On storage that is not a shared object store a listed file missing here
// may be on its way (replication): the repair drops nothing there.
func TestRepairManifestDropsNothingOffASharedStore(t *testing.T) {
	r := stagedRig(t)
	in := r.register("pulled-later.parquet")
	if err := os.Remove(filepath.Join(r.root, in.Path)); err != nil {
		t.Fatal(err)
	}
	if err := r.c.repairManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.c.raftNode.FSM().GetFile(in.Path); !ok {
		t.Fatal("dropped on local storage")
	}
}
