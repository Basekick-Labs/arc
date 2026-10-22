package cluster

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Reads count a staged compaction's rows once (open_compactions.go).

const openPrefix = "testdb/cpu/"

func sorted(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}

func sameSet(a, b []string) bool {
	a, b = sorted(a), sorted(b)
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

// copyToFinal puts an object job's output at its final key, as the
// finisher's CopyObject does.
func (r *objectRigT) copyToFinal(staged, final string) {
	r.t.Helper()
	body, err := os.ReadFile(filepath.Join(r.root, staged))
	if err != nil {
		r.t.Fatal(err)
	}
	r.write(final, string(body))
}

// An object job: nothing is left out until its output is at the final key,
// then its inputs are, on every read under its measurement, and the reads
// stay uncached while it is open.
func TestOpenExcludesObjectInputsOnceTheOutputIsThere(t *testing.T) {
	r := objectRig(t)
	ctx := context.Background()
	var hooks atomic.Int32
	r.c.SetOnObjectOutputSeen(func() { hooks.Add(1) })
	job := r.objectJob("j1")
	inputs := []string{job.Inputs[0].Path, job.Inputs[1].Path}

	if !r.c.OpenAny(openPrefix) {
		t.Fatal("an open object job does not make the read volatile")
	}
	if r.c.OpenAny("testdb/mem/") || r.c.OpenAny("otherdb/cpu/") {
		t.Fatal("an object job makes another measurement's reads volatile")
	}
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("output not copied yet, excluded %v", got)
	}
	g0 := r.c.OpenGeneration()

	r.copyToFinal(job.StagedKey, job.Output.Path)
	if got := r.c.OpenExcludedFor(ctx, openPrefix); !sameSet(got, inputs) {
		t.Fatalf("output at its key: excluded %v, want %v", got, inputs)
	}
	if hooks.Load() != 1 {
		t.Fatalf("output-seen hook ran %d times, want 1", hooks.Load())
	}
	if r.c.OpenGeneration() == g0 {
		t.Fatal("generation unchanged after the output was seen")
	}
	if got := r.c.OpenExcludedFor(ctx, "testdb/mem/"); len(got) != 0 {
		t.Fatalf("another measurement excluded %v", got)
	}
	// Seen once: the hook does not run again on the next read.
	r.c.OpenExcludedFor(ctx, openPrefix)
	if hooks.Load() != 1 {
		t.Fatalf("output-seen hook ran %d times on a second read", hooks.Load())
	}
}

// An input the manifest lists again (a restore, a new write reusing the
// path), or one a writer still holds for registration, is another file now:
// it is read, as the finisher keeps it.
func TestOpenKeepsAnObjectInputThatIsAnotherFileNow(t *testing.T) {
	r := objectRig(t)
	ctx := context.Background()
	job := r.objectJob("j1")
	a, b := job.Inputs[0].Path, job.Inputs[1].Path
	r.copyToFinal(job.StagedKey, job.Output.Path)

	e := raft.FileEntry{Path: a, SHA256: job.Inputs[0].SHA256, SizeBytes: job.Inputs[0].SizeBytes, Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := r.c.OpenExcludedFor(ctx, openPrefix); !sameSet(got, []string{b}) {
		t.Fatalf("re-registered input (same content): excluded %v, want only %s", got, b)
	}
	r.c.staged.holds = func(p string) bool { return p == b }
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("held input: excluded %v", got)
	}
}

// The output left the manifest after it was seen (an operator removed it,
// so the finisher reverts the job and registers the inputs again): from that
// apply on, nothing is left out, on every node.
func TestOpenStopsExcludingWhenTheOutputLeavesTheManifest(t *testing.T) {
	r := objectRig(t)
	ctx := context.Background()
	job := r.objectJob("j1")
	r.copyToFinal(job.StagedKey, job.Output.Path)
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 2 {
		t.Fatalf("output seen: excluded %v", got)
	}
	if err := r.c.raftNode.DeleteFile(job.Output.Path, "operator", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.staged("j1"); !ok {
		t.Fatal("job left the FSM with its output; the window this test pins is gone")
	}
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("output gone from the manifest: excluded %v (the inputs hold the only rows)", got)
	}
}

// A file at the output key that is not the output (size differs) opens no
// window.
func TestOpenNeedsTheOutputsSize(t *testing.T) {
	r := objectRig(t)
	ctx := context.Background()
	job := r.objectJob("j1")
	r.write(job.Output.Path, "something else entirely")
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("foreign object at the output key: excluded %v", got)
	}
}

// The real finisher: inside its window (the output copied, the inputs not
// yet deleted) a read leaves the inputs out; once the job leaves the FSM
// nothing is left out and reads are cacheable again.
func TestOpenThroughTheFinisher(t *testing.T) {
	r := objectRig(t)
	ctx := context.Background()
	var hooks atomic.Int32
	r.c.SetOnObjectOutputSeen(func() { hooks.Add(1) })
	job := r.objectJob("j1")
	inputs := []string{job.Inputs[0].Path, job.Inputs[1].Path}
	var inWindow []string
	r.store.onCopy = func() { inWindow = r.c.OpenExcludedFor(ctx, openPrefix) }
	if err := r.c.finishObjectJob(ctx, "j1"); err != nil {
		t.Fatal(err)
	}
	if !sameSet(inWindow, inputs) {
		t.Fatalf("between the copy and the deletes: excluded %v, want %v", inWindow, inputs)
	}
	if _, ok := r.staged("j1"); ok {
		t.Fatal("job still in the FSM after the finisher")
	}
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("after the job: excluded %v", got)
	}
	if r.c.OpenAny(openPrefix) {
		t.Fatal("reads still volatile after the job")
	}
	r.c.open.mu.Lock()
	n := len(r.c.open.seen)
	r.c.open.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d seen jobs kept after the job left", n)
	}
	if hooks.Load() != 1 {
		t.Fatalf("hook ran %d times, want 1", hooks.Load())
	}
}

// Local storage: a compaction input this node must still unlink is left out
// once the job's output is on this node's disk, not before.
func TestOpenExcludesALocalCompactionInputOnceTheOutputIsLocal(t *testing.T) {
	r := stagedRig(t)
	ctx := context.Background()
	job := r.job("j1")
	r.commitJob(job)
	in := job.Inputs[0].Path
	r.c.noteLocalDeleted(in, "compaction:j1")
	if !r.c.OpenAny(openPrefix) {
		t.Fatal("a pending local delete does not make the read volatile")
	}
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("output still staged: excluded %v", got)
	}
	// Something else at the output key (not its size): still read.
	r.write(job.Output.Path, "not the output at all")
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("foreign file at the output key: excluded %v", got)
	}
	if err := os.Remove(filepath.Join(r.root, job.Output.Path)); err != nil {
		t.Fatal(err)
	}
	e, _ := r.c.raftNode.FSM().GetFile(job.Output.Path)
	if ok, err := r.c.adoptStaged(ctx, e); !ok || err != nil {
		t.Fatalf("adopt: %v %v", ok, err)
	}
	if got := r.c.OpenExcludedFor(ctx, openPrefix); !sameSet(got, []string{in}) {
		t.Fatalf("output in place: excluded %v, want %s", got, in)
	}
	// Unknown job: read until the unlink.
	other := job.Inputs[1].Path
	r.c.noteLocalDeleted(other, "compaction:nope")
	if got := r.c.OpenExcludedFor(ctx, openPrefix); !sameSet(got, []string{in}) {
		t.Fatalf("unknown job: excluded %v", got)
	}
	// The output left the manifest: the input is read again.
	if err := r.c.raftNode.DeleteFile(job.Output.Path, "operator", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("output gone: excluded %v", got)
	}
}

// A pending delete whose path the manifest lists again with no register
// callback (a snapshot install) is read, and dropped.
func TestOpenReadsAPathListedAgainWithoutACallback(t *testing.T) {
	r := staleRig(t)
	ctx := context.Background()
	key := staleHour + "back.parquet"
	r.write(key, "rows")
	r.c.noteLocalDeleted(key, "delete")
	r.register(key, "rows") // no callbacks are wired on this rig
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("listed again: excluded %v", got)
	}
	if r.c.OpenAny(openPrefix) {
		t.Fatal("entry kept after the path was listed again")
	}
}

// Retention and whole-file DELETE removals are left out at once; reasons
// that do not account for the rows elsewhere are not even tracked.
func TestOpenLocalDeleteReasons(t *testing.T) {
	r := staleRig(t)
	ctx := context.Background()
	keys := map[string]string{
		staleHour + "ret.parquet":    "retention:7",
		staleHour + "del.parquet":    "delete",
		staleHour + "rw.parquet":     "delete-rewrite",
		staleHour + "snap.parquet":   "snapshot-removed",
		staleHour + "repair.parquet": "manifest-repair",
	}
	for k, reason := range keys {
		r.write(k, "rows")
		r.c.noteLocalDeleted(k, reason)
	}
	want := []string{staleHour + "ret.parquet", staleHour + "del.parquet"}
	if got := r.c.OpenExcludedFor(ctx, openPrefix); !sameSet(got, want) {
		t.Fatalf("excluded %v, want %v", got, want)
	}
	r.c.open.mu.Lock()
	n := len(r.c.open.localDeleted)
	r.c.open.mu.Unlock()
	// delete-rewrite is tracked too: excluded only once its rewrite is here
	// (TestOpenExcludesARewrittenFileOnceItsRewriteIsHere).
	if n != 3 {
		t.Fatalf("tracking %d deletes, want 3", n)
	}
}

// A DELETE rewrite's old file is left out once a registered member of its
// rewrite family is in storage at its registered size; only the stat is
// cached, so a member that leaves the manifest stops the exclusion.
func TestOpenExcludesARewrittenFileOnceItsRewriteIsHere(t *testing.T) {
	r := staleRig(t)
	ctx := context.Background()
	var hooks atomic.Int32
	r.c.SetOnObjectOutputSeen(func() { hooks.Add(1) })
	old := staleHour + "cpu_1.parquet"
	rw := staleHour + "cpu_1_rewrite_5.parquet"
	r.write(old, "all the rows")
	r.c.noteLocalDeleted(old, "delete-rewrite")
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("no rewrite yet: excluded %v", got)
	}
	// Registered but not here yet (a reader still pulling it).
	e := raft.FileEntry{Path: rw, SHA256: strings.Repeat("b", 64), SizeBytes: int64(len("the kept rows")), Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("rewrite registered, not here: excluded %v", got)
	}
	r.write(rw, "the kept rows")
	if got := r.c.OpenExcludedFor(ctx, openPrefix); !sameSet(got, []string{old}) {
		t.Fatalf("rewrite here: excluded %v", got)
	}
	r.c.OpenExcludedFor(ctx, openPrefix)
	if hooks.Load() != 1 {
		t.Fatalf("hook ran %d times, want 1", hooks.Load())
	}
	// The rewrite leaves the manifest (removed by something that does not
	// account for its rows): the old file is read again.
	r.unregister(rw)
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("rewrite unregistered: excluded %v", got)
	}
}

// An entry leaves the set when its copy is unlinked, when the manifest lists
// the path again, or when a read finds the file already gone.
func TestOpenForgetsLocalDeletes(t *testing.T) {
	r := staleRig(t)
	ctx := context.Background()
	unlinked, relisted, vanished := staleHour+"u.parquet", staleHour+"r.parquet", staleHour+"v.parquet"
	for _, k := range []string{unlinked, relisted, vanished} {
		r.write(k, "rows")
		r.c.noteLocalDeleted(k, "retention:1")
	}
	r.c.forgetLocal(unlinked) // the delete worker after its unlink
	r.c.forgetLocalDeleted(relisted)
	if err := os.Remove(filepath.Join(r.root, vanished)); err != nil {
		t.Fatal(err)
	}
	g := r.c.OpenGeneration()
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("excluded %v", got)
	}
	if r.c.OpenGeneration() != g {
		t.Fatal("forgetting moved the generation: every cached plan is dropped for nothing")
	}
	r.c.noteLocalDeleted(staleHour+"new.parquet", "delete")
	if r.c.OpenGeneration() == g {
		t.Fatal("a new pending delete did not move the generation")
	}
	r.c.forgetLocalDeleted(staleHour + "new.parquet")
	if r.c.OpenAny(openPrefix) {
		t.Fatal("reads still volatile with nothing pending")
	}
}

// Through the production wiring (startFilePullerLocked's delete callback and
// the delete workers): a manifest delete is left out of reads at once, and
// forgotten when the worker unlinks the copy.
func TestOpenDeleteCallbackRecordsAndWorkerForgets(t *testing.T) {
	origin := startHangingOrigin(t)
	root := t.TempDir()
	b, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	c := newManifestDeleteRigWithBackend(t, origin.addr(), b, zerolog.Nop())
	ctx := context.Background()
	key := staleHour + "cb.parquet"
	full := filepath.Join(root, key)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("rows"), 0o600); err != nil {
		t.Fatal(err)
	}
	e := raft.FileEntry{Path: key, SHA256: strings.Repeat("a", 64), SizeBytes: 4, Database: "testdb",
		Measurement: "cpu", OriginNodeID: manifestDeleteOrigin, Tier: "hot", CreatedAt: time.Now()}
	if err := c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.raftNode.DeleteFile(key, "retention:3", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	// The callback ran inside the apply; the worker waits deleteGrace.
	if got := c.OpenExcludedFor(ctx, openPrefix); !sameSet(got, []string{key}) {
		t.Fatalf("right after the delete applied: excluded %v", got)
	}
	deadline := time.Now().Add(10 * time.Second)
	for c.OpenAny(openPrefix) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if c.OpenAny(openPrefix) {
		t.Fatal("the worker's unlink did not forget the delete")
	}
	if _, err := os.Stat(full); !os.IsNotExist(err) {
		t.Fatalf("not unlinked: %v", err)
	}

	// Listed again before the unlink: read again at once.
	again := staleHour + "again.parquet"
	if err := os.WriteFile(filepath.Join(root, again), []byte("rows"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.Path = again
	if err := c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.raftNode.DeleteFile(again, "delete", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	// The register callback drops it, before any read looks.
	if c.OpenAny(openPrefix) {
		t.Fatal("registered again: reads still volatile")
	}
	if got := c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("registered again: excluded %v", got)
	}
}

// When the deletes look like a rolled-back manifest (the stale-local
// breaker), nothing local is left out: reading twice is the safe side.
func TestOpenLocalExclusionStopsWhenTheBreakerTrips(t *testing.T) {
	r := staleRig(t)
	ctx := context.Background()
	key := staleHour + "b.parquet"
	r.write(key, "rows")
	r.c.noteLocalDeleted(key, "retention:1")
	r.c.stale.exportBreaker.Store(true)
	r.c.stale.exportBreakerAt.Store(time.Now().UnixNano())
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("breaker tripped, excluded %v", got)
	}
	r.c.stale.exportBreaker.Store(false)
	if got := r.c.OpenExcludedFor(ctx, openPrefix); !sameSet(got, []string{key}) {
		t.Fatalf("breaker clear, excluded %v", got)
	}
}

// A nil or not-started coordinator answers nothing (OSS, a node without the
// FSM yet).
func TestOpenNilCoordinator(t *testing.T) {
	var c *Coordinator
	if c.OpenAny(openPrefix) || c.OpenExcludedFor(context.Background(), openPrefix) != nil || c.OpenGeneration() != 0 {
		t.Fatal("nil coordinator answered")
	}
	c.SetOnObjectOutputSeen(func() {})
}

// A staged job does not make reads volatile (its output cannot be at its key
// before the commit), so the commit must move the generation: a plan cached
// while it was only staged is not served once the output can appear.
func TestOpenCommitMovesTheGeneration(t *testing.T) {
	r := objectRig(t)
	ctx := context.Background()
	job := r.job("j1")
	job.Object = true
	r.store.mu.Lock()
	r.store.meta[job.StagedKey] = job.Output.SHA256
	r.store.mu.Unlock()
	sc := r.c.StagedCommitter()
	g0 := r.c.OpenGeneration()
	if out, err := sc.Stage(ctx, job); out != compaction.StagedOK {
		t.Fatalf("stage: %v %v", out, err)
	}
	if r.c.OpenAny(openPrefix) {
		t.Fatal("a staged, uncommitted job made reads volatile")
	}
	g1 := r.c.OpenGeneration()
	if g1 == g0 {
		t.Fatal("staging did not move the generation")
	}
	if out, err := sc.Commit(ctx, job); out != compaction.StagedOK {
		t.Fatalf("commit: %v %v", out, err)
	}
	if !r.c.OpenAny(openPrefix) || r.c.OpenGeneration() == g1 {
		t.Fatal("the commit did not make reads volatile and move the generation")
	}
}

// On a shared backend the delete callback records only a DELETE rewrite's
// old file (every node reads it until the handler removes it); other
// reasons are removed at once by their caller.
func TestSharedBackendRecordsOnlyRewrites(t *testing.T) {
	origin := startHangingOrigin(t)
	c, _ := newManifestDeleteRig(t, origin.addr())
	reg := func(p string) {
		e := raft.FileEntry{Path: p, SHA256: strings.Repeat("a", 64), SizeBytes: 4, Database: "testdb",
			Measurement: "cpu", OriginNodeID: manifestDeleteOrigin, Tier: "hot", CreatedAt: time.Now()}
		if err := c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	ret, rw := staleHour+"ret.parquet", staleHour+"rw.parquet"
	reg(ret)
	reg(rw)
	if err := c.raftNode.DeleteFile(ret, "retention:1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.raftNode.DeleteFile(rw, "delete-rewrite", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	c.open.mu.Lock()
	_, gotRet := c.open.localDeleted[ret]
	_, gotRw := c.open.localDeleted[rw]
	c.open.mu.Unlock()
	if gotRet || !gotRw {
		t.Fatalf("recorded retention %v, rewrite %v; want only the rewrite", gotRet, gotRw)
	}
}

// A registered family member at another size than registered is not the
// rewrite: the old file is still read.
func TestOpenRewriteSiblingMustBeAtItsSize(t *testing.T) {
	r := staleRig(t)
	ctx := context.Background()
	old := staleHour + "cpu_2.parquet"
	rw := staleHour + "cpu_2_rewrite_5.parquet"
	r.write(old, "all the rows")
	r.c.noteLocalDeleted(old, "delete-rewrite")
	e := raft.FileEntry{Path: rw, SHA256: strings.Repeat("b", 64), SizeBytes: int64(len("the kept rows")), Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.write(rw, "a partial")
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("sibling at another size: excluded %v", got)
	}
}
