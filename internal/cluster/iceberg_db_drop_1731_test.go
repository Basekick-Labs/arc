package cluster

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/cluster/filereplication"
)

type dropRecorder struct {
	mu  sync.Mutex
	got []string
}

func (r *dropRecorder) fn(db string) { r.mu.Lock(); r.got = append(r.got, db); r.mu.Unlock() }
func (r *dropRecorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]string(nil), r.got...)
	sort.Strings(out)
	return out
}

// #1731: a drop notice fires once the database's queued database:delete
// unlinks are done, never before; other reasons and other databases do not
// hold it; a notice before the dropper is wired is handed over when it is.
func TestIcebergDatabaseDropWaitsForTheUnlinks(t *testing.T) {
	c := &Coordinator{}
	rec := &dropRecorder{}
	c.SetIcebergDatabaseDropper(rec.fn)

	c.noteDatabaseDeleteQueued("a/cpu/2026/01/01/00/x.parquet")
	c.noteDatabaseDeleteQueued("a/mem/2026/01/01/00/y.parquet")
	c.noteDatabaseDeleteQueued("b/cpu/2026/01/01/00/z.parquet")
	c.onIcebergDatabaseDropped("a")
	if got := rec.list(); len(got) != 0 {
		t.Fatalf("fired with unlinks pending: %v", got)
	}
	c.noteDatabaseDeleteDone("a/cpu/2026/01/01/00/x.parquet")
	if got := rec.list(); len(got) != 0 {
		t.Fatalf("fired with one unlink pending: %v", got)
	}
	c.noteDatabaseDeleteDone("a/mem/2026/01/01/00/y.parquet")
	if got := rec.list(); len(got) != 1 || got[0] != "a" {
		t.Fatalf("after the last unlink: %v, want [a]", got)
	}
	// A notice for a database with nothing queued fires at once.
	c.onIcebergDatabaseDropped("c")
	if got := rec.list(); len(got) != 2 || got[1] != "c" {
		t.Fatalf("%v, want [a c]", got)
	}
	// b's unlink finishing with no notice fires nothing.
	c.noteDatabaseDeleteDone("b/cpu/2026/01/01/00/z.parquet")
	if got := rec.list(); len(got) != 2 {
		t.Fatalf("an unlink with no notice fired: %v", got)
	}
}

func TestIcebergDatabaseDropQueuedUntilTheDropperIsWired(t *testing.T) {
	c := &Coordinator{}
	c.onIcebergDatabaseDropped("early")
	rec := &dropRecorder{}
	c.SetIcebergDatabaseDropper(rec.fn)
	if got := rec.list(); len(got) != 1 || got[0] != "early" {
		t.Fatalf("queued notice: %v", got)
	}
}

// The delete worker retires the count whatever the unlink's outcome: an
// item that fails or is quarantined must not hold a drop forever.
func TestIcebergDatabaseDropCountRetiredInUnlinkBatch(t *testing.T) {
	c := &Coordinator{}
	rec := &dropRecorder{}
	c.SetIcebergDatabaseDropper(rec.fn)
	c.deletePendingMu.Lock()
	c.noteDatabaseDeleteQueued("a/cpu/f.parquet")
	c.deletePendingMu.Unlock()
	c.onIcebergDatabaseDropped("a")
	// The item is kept (relisted), so unlinkOne returns before any unlink:
	// the count must still be retired by unlinkBatch.
	kept := func(string) (bool, filereplication.RefreshOutcome) { return true, filereplication.RefreshOutcome(0) }
	c.unlinkBatch([]deleteRequest{{path: "a/cpu/f.parquet", reason: databaseDeleteReason}}, kept)
	if got := rec.list(); len(got) != 1 || got[0] != "a" {
		t.Fatalf("%v, want [a]", got)
	}
	if databaseOfKey("/spoke/db/m/x.parquet") != "spoke" || databaseOfKey("") != "" {
		t.Fatal("databaseOfKey")
	}
}

// Through the real path: enqueueLocalDelete counts the item, the worker's
// batch unlinks it, and the drop fires only after the file is gone.
func TestIcebergDatabaseDropFiresAfterTheUnlink(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	path := "a/cpu/2026/01/01/00/x.parquet"
	if err := backend.Write(ctx, path, []byte("data")); err != nil {
		t.Fatal(err)
	}
	c := &Coordinator{storage: backend}
	var sawFile []bool
	c.SetIcebergDatabaseDropper(func(db string) {
		n, _ := backend.StatFile(ctx, path)
		sawFile = append(sawFile, n >= 0)
	})
	c.enqueueLocalDelete(path, databaseDeleteReason)
	c.onIcebergDatabaseDropped("a")
	if len(sawFile) != 0 {
		t.Fatal("fired before the unlink was taken")
	}
	c.unlinkBatch(c.takePendingDeletes(), nil)
	if len(sawFile) != 1 || sawFile[0] {
		t.Fatalf("fired %d times, file present at fire = %v; want once, after the unlink", len(sawFile), sawFile)
	}
}
