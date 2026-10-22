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

// An input the manifest lists again with other content, or one a writer
// still holds for registration, is a different file now: it is read.
func TestOpenKeepsAnObjectInputThatIsAnotherFileNow(t *testing.T) {
	r := objectRig(t)
	ctx := context.Background()
	job := r.objectJob("j1")
	a, b := job.Inputs[0].Path, job.Inputs[1].Path
	r.copyToFinal(job.StagedKey, job.Output.Path)

	e := raft.FileEntry{Path: a, SHA256: strings.Repeat("e", 64), SizeBytes: 3, Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := r.c.OpenExcludedFor(ctx, openPrefix); !sameSet(got, []string{b}) {
		t.Fatalf("re-registered input: excluded %v, want only %s", got, b)
	}
	r.c.staged.holds = func(p string) bool { return p == b }
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("held input: excluded %v", got)
	}
}

// The real finisher: inside its window (the output copied, the inputs not
// yet deleted) a read leaves the inputs out; after the job leaves the FSM
// they stay excluded a while (a listing cached before the deletes), then are
// forgotten.
func TestOpenTombstonesAFinishedObjectJob(t *testing.T) {
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
	if got := r.c.OpenExcludedFor(ctx, openPrefix); !sameSet(got, inputs) {
		t.Fatalf("tombstone: excluded %v, want %v", got, inputs)
	}
	if !r.c.OpenAny(openPrefix) {
		t.Fatal("a tombstoned job does not keep reads volatile")
	}
	r.c.open.mu.Lock()
	j := r.c.open.seen["j1"]
	j.gone = time.Now().Add(-objectSeenTombstone - time.Second)
	r.c.open.seen["j1"] = j
	r.c.open.mu.Unlock()
	g := r.c.OpenGeneration()
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("after the tombstone: excluded %v", got)
	}
	if r.c.OpenAny(openPrefix) {
		t.Fatal("reads still volatile after the tombstone")
	}
	if r.c.OpenGeneration() == g {
		t.Fatal("generation unchanged when the tombstone expired")
	}
	if hooks.Load() != 2 {
		t.Fatalf("hook ran %d times, want 2 (seen, forgotten)", hooks.Load())
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
	if n != 2 {
		t.Fatalf("tracking %d deletes, want 2", n)
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
	if r.c.OpenGeneration() == g {
		t.Fatal("generation unchanged when a vanished copy was forgotten")
	}
	if r.c.OpenAny(openPrefix) {
		t.Fatal("reads still volatile with nothing pending")
	}
}

// The unlink path wires it: the FSM delete callback records the delete.
func TestOpenDeleteCallbackRecordsAndWorkerForgets(t *testing.T) {
	r := staleRig(t)
	ctx := context.Background()
	key := staleHour + "cb.parquet"
	r.register(key, "rows")
	r.c.noteLocalDeleted(key, "retention:3")
	if got := r.c.OpenExcludedFor(ctx, openPrefix); !sameSet(got, []string{key}) {
		t.Fatalf("excluded %v", got)
	}
	r.c.unlinkOne(deleteRequest{path: key, reason: "retention:3"}, nil)
	if r.exists(key) {
		t.Fatal("not unlinked")
	}
	if r.c.OpenAny(openPrefix) {
		t.Fatal("the unlink did not forget the delete")
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
