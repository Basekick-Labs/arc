package api

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
)

// The batch "applied" with other content under the rewrite's name, or the
// old file still listed: not this DELETE's commit, so both files stay.
func TestAmbiguousBatchChecksTheCommitExactly(t *testing.T) {
	for _, tc := range []string{"other sha", "old still listed"} {
		r := newJournalRig(t)
		r.coord.batchErr = errors.New("forward to leader: context deadline exceeded")
		r.coord.onSync = func() {
			switch tc {
			case "other sha":
				r.coord.files[r.new] = raft.FileEntry{Path: r.new, SHA256: "someone else"}
				delete(r.coord.files, r.old)
			case "old still listed":
				sz, sha, _ := r.h.rewrittenFileMetadata(r.new, nil)
				r.coord.files[r.new] = raft.FileEntry{Path: r.new, SHA256: sha, SizeBytes: sz}
			}
		}
		if err := r.replace(); !errors.Is(err, errManifestFailure) {
			t.Fatalf("%s: err %v", tc, err)
		}
		if !r.exists(r.old) || !r.exists(r.new) {
			t.Fatalf("%s: a file was removed", tc)
		}
	}
}

// A shared object store: only the primary writer settles the journal.
type sharedLocal struct{ *storage.LocalBackend }

func (s sharedLocal) Type() string { return "s3" }
func (s sharedLocal) CopyObject(context.Context, string, string, int64, string) error {
	return errors.New("unused")
}
func (s sharedLocal) ObjectSHA(context.Context, string) (int64, string, error) {
	return 0, "", errors.New("unused")
}
func (s sharedLocal) WriteReaderWithSHA(ctx context.Context, p string, rd io.Reader, n int64, _ string) error {
	return s.LocalBackend.WriteReader(ctx, p, rd, n)
}

func TestDrainHereOnASharedStoreIsThePrimary(t *testing.T) {
	r := newJournalRig(t)
	r.h.storage = sharedLocal{r.backend}
	if !r.h.drainHere() {
		t.Fatal("the primary does not drain")
	}
	r.coord.primary = false
	if r.h.drainHere() {
		t.Fatal("a non-primary drains the shared journal")
	}
}

// The drainer undoes a rewrite that never reached the manifest: it removes
// the rewrite, never the original.
func TestDrainerRevertsARewriteThatNeverTook(t *testing.T) {
	r := newJournalRig(t)
	setAmbiguityDelay(t, 0)
	r.record(rewriteAmbiguityDelay + time.Minute)
	r.h.DrainRetired(context.Background())
	if !r.exists(r.old) || r.exists(r.new) || len(r.retired()) != 0 {
		t.Fatalf("old exists %v, new exists %v, journal %v", r.exists(r.old), r.exists(r.new), r.retired())
	}
}

// A rewrite the tiering migrator is copying is left for a later pass.
func TestDrainerRevertLeavesARewriteTheMigratorIsMoving(t *testing.T) {
	r := newJournalRig(t)
	setAmbiguityDelay(t, 0)
	r.record(rewriteAmbiguityDelay + time.Minute)
	claims := &fakeMoveClaims{moving: map[string]bool{r.new: true}}
	r.h.moveClaims = claims
	r.h.DrainRetired(context.Background())
	if !r.exists(r.new) || len(r.retired()) != 1 {
		t.Fatalf("new exists %v, journal %v: a moving rewrite was reverted", r.exists(r.new), r.retired())
	}
	delete(claims.moving, r.new)
	r.h.DrainRetired(context.Background())
	if r.exists(r.new) || len(r.retired()) != 0 {
		t.Fatalf("new exists %v, journal %v once free", r.exists(r.new), r.retired())
	}
}

// Standalone: the handler adds the old file to the read source when the
// rewrite is published, and drops it once removed.
func TestStandaloneRewriteFeedsTheReadSource(t *testing.T) {
	r := newLeftoverRig(t)
	r.h.reads = NewRetiredReads(r.backend)
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	r.parquet(old, "h0", "h1")
	failReplacedDeletes(t, old)
	_ = r.rewrite(old, "host = 'h0'")
	if got := r.h.reads.ExcludedFor(context.Background(), "db/cpu/"); len(got) != 1 || got[0] != old {
		t.Fatalf("while the old file stays: excluded %v", got)
	}
	deleteReplaced = storage.DeleteWithRetry
	r2 := newLeftoverRig(t)
	r2.h.reads = NewRetiredReads(r2.backend)
	r2.parquet(old, "h0", "h1")
	if err := r2.rewrite(old, "host = 'h0'"); err != nil {
		t.Fatal(err)
	}
	if r2.h.reads.Open("db/cpu/") {
		t.Fatal("entry kept after the old file was removed")
	}
}

// Cluster: a file the journal names and the manifest does not list is never
// scanned, even with no listed family member left.
func TestClusterLoneJournaledFileIsLeftOut(t *testing.T) {
	r := newJournalRig(t)
	delete(r.coord.files, r.old)
	if err := r.h.journalRetired(context.Background(), r.old, r.new); err != nil {
		t.Fatal(err)
	}
	files, _ := r.h.listParquet(context.Background(), "db", "cpu")
	_, left, err := r.h.leftoverFiles(context.Background(), files)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, l := range left {
		found = found || l.path == r.old
	}
	if !found {
		t.Fatalf("lone journaled file not left out: %v", left)
	}
}

// The drainer reads each record again under the lock: one a restore (or a
// DELETE) cleared since the listing is not acted on.
func TestDrainerRereadsTheRecordUnderTheLock(t *testing.T) {
	r := newLeftoverRig(t)
	a := leftoverHour + "cpu_20261007_204851_000000001.parquet"
	b := leftoverHour + "cpu_20261007_204851_000000002.parquet"
	r.parquet(a, "h1")
	r.parquet(b, "h1")
	ctx := context.Background()
	for _, p := range []string{a, b} {
		if err := r.h.journalRetired(ctx, p, p+".x"); err != nil {
			t.Fatal(err)
		}
	}
	prev := deleteReplaced
	t.Cleanup(func() { deleteReplaced = prev })
	deleteReplaced = func(ctx context.Context, bk storage.Backend, path string) error {
		// Whichever is drained first, the other's record is cleared
		// meanwhile (a restore putting it back as data).
		for _, other := range []string{a, b} {
			if other != path {
				_ = bk.Delete(ctx, retiredKey(other))
			}
		}
		return prev(ctx, bk, path)
	}
	if n := r.h.DrainRetired(ctx); n != 1 {
		t.Fatalf("drained %d, want 1", n)
	}
	if r.exists(a) == r.exists(b) {
		t.Fatalf("a %v b %v: exactly one should remain (the one whose record was cleared)", r.exists(a), r.exists(b))
	}
}
