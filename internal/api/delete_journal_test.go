package api

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
)

// The DELETE journal on clusters: an intent written before the manifest
// change, acted on only through retiredAction's gate; a manifest change with
// an unknown outcome keeps both files and the record.

// journalCoord is a manifest with registered entries, a controllable batch,
// and a sync that can fail.
type journalCoord struct {
	files      map[string]raft.FileEntry
	batchErr   error
	applyOnErr bool
	syncErr    error
	syncs      int
	onSync     func()
	primary    bool
	owned      map[string]bool
	batches    int
}

func newJournalCoord() *journalCoord {
	return &journalCoord{files: map[string]raft.FileEntry{}, primary: true, owned: map[string]bool{}}
}

func (c *journalCoord) BatchFileOpsInManifest(ops []raft.BatchFileOp) error {
	c.batches++
	if c.batchErr == nil || c.applyOnErr {
		for _, op := range ops {
			switch op.Type {
			case raft.CommandRegisterFile:
				var p raft.RegisterFilePayload
				_ = json.Unmarshal(op.Payload, &p)
				c.files[p.File.Path] = p.File
			case raft.CommandDeleteFile:
				var p raft.DeleteFilePayload
				_ = json.Unmarshal(op.Payload, &p)
				delete(c.files, p.Path)
			}
		}
	}
	return c.batchErr
}
func (c *journalCoord) GetFileEntry(p string) (*raft.FileEntry, bool) {
	e, ok := c.files[p]
	return &e, ok
}
func (c *journalCoord) IsPrimaryWriter() bool { return c.primary }
func (c *journalCoord) Role() string          { return "writer" }
func (c *journalCoord) LocalNodeID() string   { return "w1" }
func (c *journalCoord) SyncManifest(context.Context) error {
	c.syncs++
	if c.syncErr == nil && c.onSync != nil {
		c.onSync()
	}
	return c.syncErr
}
func (c *journalCoord) CompactionOwns(p string) bool { return c.owned[p] }
func (c *journalCoord) RewriteSiblings(p string) []raft.FileEntry {
	var out []raft.FileEntry
	for path, e := range c.files {
		if path != p && storage.RewriteFamily(path) == storage.RewriteFamily(p) {
			out = append(out, e)
		}
	}
	return out
}
func (c *journalCoord) HasRewriteSibling(p string) bool { return len(c.RewriteSiblings(p)) > 0 }

type journalRig struct {
	*leftoverRig
	coord    *journalCoord
	old, new string
}

// newJournalRig: L registered, its rewrite R written (not yet registered).
func newJournalRig(t *testing.T) *journalRig {
	t.Helper()
	r := newLeftoverRig(t)
	coord := newJournalCoord()
	r.h.coordinator = coord
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	rw := leftoverHour + "cpu_20261007_204851_558252000_rewrite_100.parquet"
	r.parquet(old, "h0", "h1", "h2")
	r.parquet(rw, "h1", "h2")
	coord.files[old] = raft.FileEntry{Path: old, SizeBytes: r.size(old), SHA256: "old", Database: "db", Measurement: "cpu", Tier: "hot"}
	return &journalRig{leftoverRig: r, coord: coord, old: old, new: rw}
}

func (r *leftoverRig) size(key string) int64 {
	r.t.Helper()
	fi, err := os.Stat(filepath.Join(r.root, key))
	if err != nil {
		r.t.Fatal(err)
	}
	return fi.Size()
}

func (r *journalRig) replace() error {
	return r.h.replaceManifestAfterRewrite(context.Background(), r.old, r.new, nil, "")
}

func TestRewriteJournalsTheIntentAndClearsIt(t *testing.T) {
	r := newJournalRig(t)
	prev := deleteReplaced
	t.Cleanup(func() { deleteReplaced = prev })
	var recordedAtBatch []string
	deleteReplaced = func(ctx context.Context, b storage.Backend, path string) error {
		recordedAtBatch = r.retired()
		return prev(ctx, b, path)
	}
	if err := r.replace(); err != nil {
		t.Fatal(err)
	}
	if len(recordedAtBatch) != 1 || recordedAtBatch[0] != r.old {
		t.Fatalf("no intent record while the old file was removed: %v", recordedAtBatch)
	}
	if r.exists(r.old) || len(r.retired()) != 0 {
		t.Fatalf("after success: old exists %v, journal %v", r.exists(r.old), r.retired())
	}
}

// A batch error with an unknown outcome that did not apply: both files and
// the record stay (removing the rewrite would lose its rows if the change
// applied later).
func TestAmbiguousBatchKeepsBothFilesAndTheRecord(t *testing.T) {
	r := newJournalRig(t)
	r.coord.batchErr = errors.New("forward to leader: context deadline exceeded")
	err := r.replace()
	if !errors.Is(err, errManifestFailure) {
		t.Fatalf("err %v", err)
	}
	if !r.exists(r.old) || !r.exists(r.new) {
		t.Fatal("a file was removed although the outcome is unknown")
	}
	if got := r.retired(); len(got) != 1 || got[0] != r.old {
		t.Fatalf("journal %v", got)
	}
}

// The batch applied although its caller saw an error: finish as a success.
func TestAmbiguousBatchThatAppliedFinishes(t *testing.T) {
	r := newJournalRig(t)
	r.coord.batchErr = errors.New("forward to leader: context deadline exceeded")
	r.coord.applyOnErr = true
	if err := r.replace(); err != nil {
		t.Fatalf("err %v", err)
	}
	if r.exists(r.old) || !r.exists(r.new) || len(r.retired()) != 0 {
		t.Fatalf("old exists %v, new exists %v, journal %v", r.exists(r.old), r.exists(r.new), r.retired())
	}
}

// A definitive refusal: the rewrite goes, the record is cleared.
func TestRefusedBatchRemovesTheRewriteAndTheRecord(t *testing.T) {
	r := newJournalRig(t)
	r.coord.batchErr = &raft.ManifestChangedError{Paths: []string{r.old}}
	if err := r.replace(); !errors.Is(err, errFileChanged) {
		t.Fatalf("err %v", err)
	}
	if !r.exists(r.old) || r.exists(r.new) || len(r.retired()) != 0 {
		t.Fatalf("old exists %v, new exists %v, journal %v", r.exists(r.old), r.exists(r.new), r.retired())
	}
}

// A source this node's manifest does not list: catch up first. Listed after
// all, it takes the batch path; the sync failing keeps the source.
func TestUnlistedSourceCatchesUpFirst(t *testing.T) {
	r := newJournalRig(t)
	entry := r.coord.files[r.old]
	delete(r.coord.files, r.old)
	r.coord.syncErr = errors.New("no leader")
	if err := r.replace(); !errors.Is(err, errNotRegisteredYet) {
		t.Fatalf("sync failed: err %v", err)
	}
	if !r.exists(r.old) || r.exists(r.new) {
		t.Fatal("sync failed: the source was removed or the rewrite kept")
	}
	r.parquet(r.new, "h1", "h2")
	r.coord.syncErr = nil
	syncs := r.coord.syncs
	r.coord.onSync = func() { r.coord.files[r.old] = entry } // what the sync brings
	if err := r.replace(); err != nil {
		t.Fatal(err)
	}
	if r.coord.syncs == syncs || r.coord.batches != 1 {
		t.Fatalf("syncs %d->%d, batches %d", syncs, r.coord.syncs, r.coord.batches)
	}
}

func (r *journalRig) record(age time.Duration) retiredRecord {
	r.t.Helper()
	rec := retiredRecord{Version: 1, Path: r.old, ReplacedBy: r.new, At: time.Now().Add(-age)}
	data, _ := json.Marshal(rec)
	if err := r.backend.Write(context.Background(), retiredKey(r.old), data); err != nil {
		r.t.Fatal(err)
	}
	return rec
}

func TestRetiredActionOnACluster(t *testing.T) {
	ctx := context.Background()
	r := newJournalRig(t)
	rec := r.record(time.Minute)
	// L listed, R not, too recent: keep (a forwarded change may still apply).
	if a := r.h.retiredAction(ctx, rec); a != retiredKeep {
		t.Fatalf("recent and unsettled: %v", a)
	}
	// Old enough: the rewrite never took, revert.
	rec = r.record(rewriteAmbiguityDelay + time.Minute)
	if a := r.h.retiredAction(ctx, rec); a != retiredRevert {
		t.Fatalf("unsettled after the delay: %v", a)
	}
	// Owned by a staged compaction: keep.
	r.coord.owned[r.old] = true
	if a := r.h.retiredAction(ctx, rec); a != retiredKeep {
		t.Fatalf("owned: %v", a)
	}
	delete(r.coord.owned, r.old)
	// The change applied: L unlisted, R listed and here at its size.
	delete(r.coord.files, r.old)
	r.coord.files[r.new] = raft.FileEntry{Path: r.new, SizeBytes: r.size(r.new)}
	if a := r.h.retiredAction(ctx, rec); a != retiredRemoveOld {
		t.Fatalf("settled: %v", a)
	}
	// Listed but not here at its size: keep (never remove the last copy).
	r.coord.files[r.new] = raft.FileEntry{Path: r.new, SizeBytes: 1}
	if a := r.h.retiredAction(ctx, rec); a != retiredKeep {
		t.Fatalf("sibling not at size: %v", a)
	}
}

func TestDrainerOnASharedStoreNeedsThePrimary(t *testing.T) {
	r := newJournalRig(t)
	r.record(rewriteAmbiguityDelay + time.Minute)
	if !r.h.drainHere() {
		t.Fatal("local storage: every node settles its own records")
	}
	// The settled case through the drainer.
	delete(r.coord.files, r.old)
	r.coord.files[r.new] = raft.FileEntry{Path: r.new, SizeBytes: r.size(r.new)}
	if n := r.h.DrainRetired(context.Background()); n != 1 || r.exists(r.old) || len(r.retired()) != 0 {
		t.Fatalf("drained %d; old exists %v; journal %v", n, r.exists(r.old), r.retired())
	}
}

// Standalone: a record is only acted on once its replacement (or a later,
// smaller member) is here; otherwise the file it names is the current one.
func TestRetiredActionStandalone(t *testing.T) {
	ctx := context.Background()
	r := newLeftoverRig(t)
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	rw := leftoverHour + "cpu_20261007_204851_558252000_rewrite_100.parquet"
	r.parquet(old, "h0", "h1", "h2")
	rec := retiredRecord{Path: old, ReplacedBy: rw, At: time.Now()}
	if a := r.h.retiredAction(ctx, rec); a != retiredClear {
		t.Fatalf("replacement never landed: %v", a)
	}
	later := leftoverHour + "cpu_20261007_204851_558252000_rewrite_200.parquet"
	r.parquet(later, "h1")
	if a := r.h.retiredAction(ctx, rec); a != retiredRemoveOld {
		t.Fatalf("a later, smaller rewrite is here: %v", a)
	}
	_ = os.Remove(filepath.Join(r.root, later))
	bigger := leftoverHour + "cpu_20261007_204851_558252000_rewrite_50.parquet"
	r.parquet(bigger, "h0", "h1", "h2", "h3")
	if a := r.h.retiredAction(ctx, rec); a != retiredClear {
		t.Fatalf("only a member with more rows: %v", a)
	}
	r.parquet(rw, "h1", "h2")
	if a := r.h.retiredAction(ctx, rec); a != retiredRemoveOld {
		t.Fatalf("replacement here: %v", a)
	}
}

func TestRetiredReadsSource(t *testing.T) {
	ctx := context.Background()
	r := newLeftoverRig(t)
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	rw := leftoverHour + "cpu_20261007_204851_558252000_rewrite_100.parquet"
	r.parquet(old, "h0", "h1")
	src := NewRetiredReads(r.backend)
	hooks := 0
	src.SetOnSeen(func() { hooks++ })
	g0 := src.Generation()
	src.add(old, rw)
	if src.Generation() == g0 || !src.Open("db/cpu/") || src.Open("db/mem/") {
		t.Fatal("generation / Open")
	}
	if got := src.ExcludedFor(ctx, "db/cpu/"); len(got) != 0 {
		t.Fatalf("replacement not here yet: excluded %v", got)
	}
	r.parquet(rw, "h1")
	if got := src.ExcludedFor(ctx, "db/cpu/"); len(got) != 1 || got[0] != old || hooks != 1 {
		t.Fatalf("excluded %v hooks %d", got, hooks)
	}
	src.ExcludedFor(ctx, "db/cpu/")
	if hooks != 1 {
		t.Fatal("hook ran again")
	}
	// Seeded from the journal, only with the replacement here.
	if err := r.h.journalRetired(ctx, old, rw); err != nil {
		t.Fatal(err)
	}
	other := leftoverHour + "cpu_20261007_210000_000000000.parquet"
	r.parquet(other, "h1")
	if err := r.h.journalRetired(ctx, other, other+".missing"); err != nil {
		t.Fatal(err)
	}
	seeded := NewRetiredReads(r.backend)
	if err := seeded.Seed(ctx); err != nil {
		t.Fatal(err)
	}
	if got := seeded.ExcludedFor(ctx, "db/cpu/"); len(got) != 1 || got[0] != old {
		t.Fatalf("seeded excluded %v", got)
	}
	// The old file gone: the entry drops.
	prevGone := retiredGoneCheckEvery
	retiredGoneCheckEvery = 0
	t.Cleanup(func() { retiredGoneCheckEvery = prevGone })
	_ = os.Remove(filepath.Join(r.root, old))
	if got := seeded.ExcludedFor(ctx, "db/cpu/"); len(got) != 0 || seeded.Open("db/cpu/") {
		t.Fatalf("after removal excluded %v", got)
	}
}
