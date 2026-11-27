package rehome

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

func testStore(t *testing.T) storage.Backend {
	t.Helper()
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

const (
	srcKey = "db/cpu/2026/11/18/23/x.parquet"
	outA   = "db/cpu/2026/11/18/23/a.parquet"
	outB   = "db/cpu/2026/11/19/05/b.parquet"
	prefix = "db/cpu/"
)

func testJob(id string) *Job {
	return &Job{ID: id, Database: "db", Measurement: "cpu", Source: FileRef{Key: srcKey, Size: 10, SHA256: "s"}, CreatedAt: time.Unix(1_800_000_000, 0).UTC()}
}

func testOutputs(id string) []FileRef {
	return []FileRef{
		{Key: outA, Staged: StagedDir(id) + "/0.parquet", Size: 5, SHA256: "a"},
		{Key: outB, Staged: StagedDir(id) + "/1.parquet", Size: 6, SHA256: "b"},
	}
}

func seeded(t *testing.T, store storage.Backend) *Registry {
	t.Helper()
	r := NewRegistry(store)
	if err := r.Seed(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r
}

// What reads leave out, and what is owned, in each state.
func TestJobStates(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	r := seeded(t, store)
	if r.Active() || r.Open(prefix) || r.Owns(srcKey) {
		t.Fatal("an empty registry")
	}
	const id = "ab12"
	if err := r.Begin(testJob(id)); err != nil {
		t.Fatal(err)
	}
	if !r.Owns(srcKey) || r.Owns(outA) || r.Open(prefix) || len(r.ExcludedFor(ctx, prefix)) != 0 {
		t.Fatal("preparing: the source is owned, nothing is left out")
	}
	if err := r.SetOutputs(id, testOutputs(id)); err != nil {
		t.Fatal(err)
	}
	if !r.Owns(outA) || !r.Owns(outB) {
		t.Fatal("the outputs' final keys are owned once they are known")
	}
	gen := r.Generation()

	if err := r.Advance(ctx, id, StateStaged); err != nil {
		t.Fatal(err)
	}
	if r.Open(prefix) || len(r.ExcludedFor(ctx, prefix)) != 0 || r.Generation() != gen {
		t.Fatal("staged: nothing is left out, and cached transforms stay valid")
	}
	if ok, _ := store.Exists(ctx, journalKey(id)); !ok {
		t.Fatal("staged is journaled")
	}

	if err := r.Advance(ctx, id, StateCommitted); err != nil {
		t.Fatal(err)
	}
	if got := r.ExcludedFor(ctx, prefix); !r.Open(prefix) || len(got) != 2 || got[0] != outA || got[1] != outB {
		t.Fatalf("committed: reads leave out every output: %v", got)
	}
	if r.Generation() == gen {
		t.Fatal("committed must move the generation: a transform cached before the job has no exclusions")
	}
	if r.Open("db/mem/") || len(r.ExcludedFor(ctx, "db/mem/")) != 0 {
		t.Fatal("another measurement is not affected")
	}
	if got := r.Replaced(prefix); got != nil {
		t.Fatalf("committed: reads still use the source, so nothing is replaced yet: %v", got)
	}

	if err := r.Advance(ctx, id, StateLatched); err != nil {
		t.Fatal(err)
	}
	if got := r.ExcludedFor(ctx, prefix); !r.Open(prefix) || len(got) != 1 || got[0] != srcKey {
		t.Fatalf("latched: reads leave out the source: %v", got)
	}
	if strict, err := r.ExcludedForStrict(ctx, prefix); err != nil || len(strict) != 1 {
		t.Fatalf("strict: %v %v", strict, err)
	}
	if got := r.Replaced(prefix); len(got) != 1 || len(got[srcKey]) != 2 || got[srcKey][0] != outA || got[srcKey][1] != outB {
		t.Fatalf("latched: the source is replaced by the outputs: %v", got)
	}
	if got := r.Replaced("db/mem/"); got != nil {
		t.Fatalf("another measurement: %v", got)
	}

	// A state cannot be skipped or repeated.
	if err := r.Advance(ctx, id, StateCommitted); err == nil {
		t.Fatal("a latched job went back to committed")
	}
	if err := r.End(ctx, id); err != nil {
		t.Fatal(err)
	}
	if r.Active() || r.Open(prefix) || r.Owns(srcKey) || r.Owns(outA) || len(r.ExcludedFor(ctx, prefix)) != 0 {
		t.Fatal("an ended job owns and leaves out nothing")
	}
	if ok, _ := store.Exists(ctx, journalKey(id)); ok {
		t.Fatal("an ended job's record is gone")
	}
}

func TestAdvanceInOrderOnly(t *testing.T) {
	ctx := context.Background()
	r := seeded(t, testStore(t))
	if err := r.Begin(testJob("aa")); err != nil {
		t.Fatal(err)
	}
	_ = r.SetOutputs("aa", testOutputs("aa"))
	for _, to := range []State{StateCommitted, StateLatched, StatePreparing} {
		if err := r.Advance(ctx, "aa", to); err == nil {
			t.Fatalf("a preparing job became %s", to)
		}
	}
	if err := r.Advance(ctx, "nope", StateStaged); err == nil {
		t.Fatal("an unknown job advanced")
	}
}

// A second job on the same source, or one whose output key another job owns,
// is refused. So is a job on a file an operation holds a claim on, and a
// claim on a file a job owns.
func TestOwnershipAndClaims(t *testing.T) {
	r := seeded(t, testStore(t))
	if err := r.Begin(testJob("aa")); err != nil {
		t.Fatal(err)
	}
	if err := r.Begin(testJob("bb")); !errors.Is(err, ErrOwned) {
		t.Fatalf("a second job on the source: %v", err)
	}
	if err := r.SetOutputs("aa", testOutputs("aa")); err != nil {
		t.Fatal(err)
	}
	other := testJob("cc")
	other.Source.Key = "db/cpu/2026/11/18/22/y.parquet"
	if err := r.Begin(other); err != nil {
		t.Fatal(err)
	}
	if err := r.SetOutputs("cc", []FileRef{{Key: outA, Staged: StagedDir("cc") + "/0.parquet"}}); !errors.Is(err, ErrOwned) {
		t.Fatalf("an output key another job owns: %v", err)
	}
	if _, ok := r.Claim(srcKey); ok {
		t.Fatal("a claim on a file a job owns was granted")
	}
	r.Drop("aa")
	r.Drop("cc")

	release, ok := r.Claim(srcKey)
	if !ok {
		t.Fatal("a claim on a free file was refused")
	}
	if err := r.Begin(testJob("dd")); err == nil {
		t.Fatal("a job began on a file an operation is deleting")
	}
	release()
	release() // twice: counted once
	if err := r.Begin(testJob("dd")); err != nil {
		t.Fatalf("after the claim was released: %v", err)
	}
	releaseOut, _ := r.Claim(outA)
	if err := r.SetOutputs("dd", testOutputs("dd")); err == nil {
		t.Fatal("an output key under a claim was taken")
	}
	releaseOut()
}

// After a restart the registry is what the journal says, in every state.
func TestSeedReadsTheJournal(t *testing.T) {
	ctx := context.Background()
	for _, upTo := range []State{StateStaged, StateCommitted, StateLatched} {
		store := testStore(t)
		r := seeded(t, store)
		_ = r.Begin(testJob("ab"))
		_ = r.SetOutputs("ab", testOutputs("ab"))
		for _, s := range []State{StateStaged, StateCommitted, StateLatched} {
			if err := r.Advance(ctx, "ab", s); err != nil {
				t.Fatal(err)
			}
			if s == upTo {
				break
			}
		}
		again := seeded(t, store)
		j, ok := again.Get("ab")
		if !ok || j.State != upTo || len(j.Outputs) != 2 || j.Source.Key != srcKey {
			t.Fatalf("%s: after a restart the job is %+v", upTo, j)
		}
		if !again.Owns(srcKey) || !again.Owns(outB) {
			t.Fatalf("%s: ownership was not restored", upTo)
		}
		want := map[State]int{StateStaged: 0, StateCommitted: 2, StateLatched: 1}[upTo]
		if got := again.ExcludedFor(ctx, prefix); len(got) != want {
			t.Fatalf("%s: after a restart reads leave out %v", upTo, got)
		}
		if (upTo != StateStaged) != (again.Generation() > 0) {
			t.Fatalf("%s: generation %d", upTo, again.Generation())
		}
	}
}

// A preparing job has no record: a restart forgets it.
func TestPreparingJobIsNotJournaled(t *testing.T) {
	store := testStore(t)
	r := seeded(t, store)
	_ = r.Begin(testJob("ab"))
	_ = r.SetOutputs("ab", testOutputs("ab"))
	if again := seeded(t, store); again.Active() {
		t.Fatal("a job that never staged came back after a restart")
	}
}

// Until the journal is read, every file is owned and a strict read fails: an
// unread journal may hold a job whose files compaction would merge.
func TestUnseededRegistryOwnsEverything(t *testing.T) {
	r := NewRegistry(testStore(t))
	if !r.Owns("anything/at/all") {
		t.Fatal("an unseeded registry must own every file")
	}
	if _, ok := r.Claim(srcKey); ok {
		t.Fatal("an unseeded registry granted a claim")
	}
	if _, err := r.ExcludedForStrict(context.Background(), prefix); err == nil {
		t.Fatal("a strict read succeeded before the journal was read")
	}
	if err := r.Begin(testJob("ab")); err == nil {
		t.Fatal("a job began before the journal was read")
	}
}

// A record that is not a job's fails the seed, and the error names the file.
func TestSeedRefusesABadRecord(t *testing.T) {
	ctx := context.Background()
	good := func() Job {
		j := *testJob("ab")
		j.State, j.Outputs = StateCommitted, testOutputs("ab")
		return j
	}
	for name, mutate := range map[string]func(*Job){
		"another id":                  func(j *Job) { j.ID = "cd" },
		"a state that is not written": func(j *Job) { j.State = StatePreparing },
		"a source that is not data":   func(j *Job) { j.Source.Key = "_schema/cpu/2026/11/18/23/x.parquet" },
		"a source of another table":   func(j *Job) { j.Source.Key = "db/mem/2026/11/18/23/x.parquet" },
		"an output of another table":  func(j *Job) { j.Outputs[0].Key = "other/cpu/2026/11/18/23/a.parquet" },
		"an output named twice":       func(j *Job) { j.Outputs[1].Key = j.Outputs[0].Key },
		"the source as an output":     func(j *Job) { j.Outputs[1].Key = j.Source.Key },
		"a staged file elsewhere":     func(j *Job) { j.Outputs[0].Staged = "db/cpu/2026/11/18/23/victim.parquet" },
		"a staged file climbing out":  func(j *Job) { j.Outputs[0].Staged = StagedDir("ab") + "/../../db/x.parquet" },
		"no outputs":                  func(j *Job) { j.Outputs = nil },
		"an output with no size":      func(j *Job) { j.Outputs[0].Size = -1 },
		"an output with no hash":      func(j *Job) { j.Outputs[1].SHA256 = "" },
		"a source with no size":       func(j *Job) { j.Source.Size = 0 },
	} {
		store := testStore(t)
		j := good()
		mutate(&j)
		data, _ := json.Marshal(j)
		if err := store.Write(ctx, journalKey("ab"), data); err != nil {
			t.Fatal(err)
		}
		err := NewRegistry(store).Seed(ctx)
		if err == nil || !strings.Contains(err.Error(), journalKey("ab")) {
			t.Errorf("%s: seed returned %v, want an error naming the record", name, err)
		}
	}
	store := testStore(t)
	_ = store.Write(ctx, journalKey("ab"), []byte("{not json"))
	if err := NewRegistry(store).Seed(ctx); err == nil {
		t.Error("a record that is not JSON was accepted")
	}
	// A valid record seeds.
	store = testStore(t)
	j := good()
	data, _ := json.Marshal(j)
	_ = store.Write(ctx, journalKey("ab"), data)
	if err := NewRegistry(store).Seed(ctx); err != nil {
		t.Errorf("a valid record: %v", err)
	}
}

// An operation over a whole prefix (a database delete, a backup) and a job
// keep each other out, whichever registers first.
func TestBlock(t *testing.T) {
	r := seeded(t, testStore(t))
	release, ok := r.Block("db/")
	if !ok {
		t.Fatal("a block with no job in flight was refused")
	}
	if err := r.Begin(testJob("aa")); err == nil {
		t.Fatal("a job began under a blocked prefix")
	}
	other := testJob("bb")
	other.Database, other.Source.Key = "db2", "db2/cpu/2026/11/18/23/x.parquet"
	if err := r.Begin(other); err != nil {
		t.Fatalf("a job outside the blocked prefix: %v", err)
	}
	if _, ok := r.Block(""); ok {
		t.Fatal("a block over everything was granted with a job in flight")
	}
	if _, ok := r.Block("db2/"); ok {
		t.Fatal("a block over a database a job owns a file in was granted")
	}
	release()
	release()
	if err := r.Begin(testJob("aa")); err != nil {
		t.Fatalf("after the block was released: %v", err)
	}
	// An output key under a blocked prefix is refused too.
	r.Drop("aa")
	r.Drop("bb")
	rel, _ := r.Block("db/cpu/2026/11/19/")
	_ = r.Begin(testJob("cc"))
	if err := r.SetOutputs("cc", testOutputs("cc")); err == nil {
		t.Fatal("an output key under a blocked prefix was taken")
	}
	rel()
	if _, ok := NewRegistry(testStore(t)).Block(""); ok {
		t.Fatal("an unseeded registry granted a block")
	}
}

func TestNilRegistry(t *testing.T) {
	var r *Registry
	if r.Owns("x") || r.Open("x") || r.Active() || r.Generation() != 0 || r.ExcludedFor(context.Background(), "x") != nil {
		t.Fatal("a nil registry owns and leaves out nothing")
	}
	if release, ok := r.Claim("x"); !ok {
		t.Fatal("a nil registry refused a claim")
	} else {
		release()
	}
	if release, ok := r.Block(""); !ok {
		t.Fatal("a nil registry refused a block")
	} else {
		release()
	}
}

// A latched job whose source is in cold storage names it as a cold key, in
// the same answer as the keys to leave out.
func TestExcludedTiered(t *testing.T) {
	ctx := context.Background()
	for _, cold := range []bool{false, true} {
		r := seeded(t, testStore(t))
		const id = "cd34"
		job := testJob(id)
		if cold {
			job.Source.Tier, job.Source.ColdStore = TierCold, "s3://bucket/"
		}
		if err := r.Begin(job); err != nil {
			t.Fatal(err)
		}
		if err := r.SetOutputs(id, testOutputs(id)); err != nil {
			t.Fatal(err)
		}
		if err := r.Advance(ctx, id, StateStaged); err != nil {
			t.Fatal(err)
		}
		if k, c := r.ExcludedTiered(ctx, prefix); len(k) != 0 || len(c) != 0 {
			t.Fatalf("staged: %v %v", k, c)
		}
		if err := r.Advance(ctx, id, StateCommitted); err != nil {
			t.Fatal(err)
		}
		if k, c := r.ExcludedTiered(ctx, prefix); len(k) != 2 || len(c) != 0 {
			t.Fatalf("committed: the outputs are on the hot tier: %v %v", k, c)
		}
		if err := r.Advance(ctx, id, StateLatched); err != nil {
			t.Fatal(err)
		}
		k, c := r.ExcludedTiered(ctx, prefix)
		if len(k) != 1 || k[0] != srcKey || (cold && (len(c) != 1 || c[0] != srcKey)) || (!cold && len(c) != 0) {
			t.Fatalf("latched (cold source %v): keys %v, cold keys %v", cold, k, c)
		}
		if k, c := r.ExcludedTiered(ctx, "db/mem/"); len(k) != 0 || len(c) != 0 {
			t.Fatalf("another measurement: %v %v", k, c)
		}
		// The record keeps the tier across a restart.
		r2 := NewRegistry(r.store)
		if err := r2.Seed(ctx); err != nil {
			t.Fatal(err)
		}
		if got := r2.Jobs()[0].Source; got.Cold() != cold || (cold && got.ColdStore != "s3://bucket/") {
			t.Fatalf("after a restart: %+v", got)
		}
	}
}

// #1860: a cluster job is recorded durably in committed then latched, seeds
// back as such, and is left alone by nothing but End.
func TestMirrorClusterJob(t *testing.T) {
	store := testStore(t)
	r := seeded(t, store)
	j := &Job{ID: "ab12", Database: "db", Measurement: "cpu", State: StateCommitted,
		Source:  FileRef{Key: "db/cpu/2026/11/18/23/x.parquet", Size: 10, SHA256: "s"},
		Outputs: []FileRef{{Key: "db/cpu/2026/11/18/22/o.parquet", Staged: StagedDir("ab12") + "/000000.parquet", Size: 5, SHA256: "o"}}}
	if err := r.Mirror(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	if got := r.ExcludedFor(context.Background(), "db/cpu/"); len(got) != 1 || got[0] != j.Outputs[0].Key {
		t.Fatalf("committed excludes %v", got)
	}
	again := NewRegistry(store)
	if err := again.Seed(context.Background()); err != nil {
		t.Fatal(err)
	}
	if g, ok := again.Get("ab12"); !ok || !g.Cluster || g.State != StateCommitted {
		t.Fatalf("seeded %+v", g)
	}
	j.State = StateLatched
	if err := r.Mirror(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	if got := r.ExcludedFor(context.Background(), "db/cpu/"); len(got) != 1 || got[0] != j.Source.Key {
		t.Fatalf("latched excludes %v", got)
	}
	j.State = StateCommitted
	if err := r.Mirror(context.Background(), j); err == nil {
		t.Fatal("went back from latched")
	}
	if err := r.End(context.Background(), "ab12"); err != nil || r.Active() {
		t.Fatalf("end: %v", err)
	}
}

// #1860 5b: DropOwn keeps a cluster job's record; ClusterSource names only a
// recorded cluster job's source.
func TestDropOwnAndClusterSource(t *testing.T) {
	r := seeded(t, testStore(t))
	j := &Job{ID: "ab12", Database: "db", Measurement: "cpu", State: StateCommitted,
		Source:  FileRef{Key: "db/cpu/2026/11/18/23/x.parquet", Size: 10, SHA256: "s"},
		Outputs: []FileRef{{Key: "db/cpu/2026/11/18/22/o.parquet", Staged: StagedDir("ab12") + "/000000.parquet", Size: 5, SHA256: "o"}}}
	if err := r.Mirror(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	if !r.ClusterSource(j.Source.Key) || r.ClusterSource(j.Outputs[0].Key) {
		t.Fatal("ClusterSource")
	}
	gen := r.Generation()
	r.DropOwn("ab12")
	if !r.HasCluster("ab12") || r.Generation() != gen {
		t.Fatal("DropOwn removed a cluster record")
	}
	r.Drop("ab12")
	if r.HasCluster("ab12") || r.Generation() == gen {
		t.Fatal("Drop kept the record or left the generation")
	}
}
