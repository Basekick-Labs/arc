package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// discoveryRecorder records the writes of the discovery files and can fail
// the listing or the deletes of the metadata directory. It embeds the local
// backend itself, so the optional interfaces the exporter asks for stay.
type discoveryRecorder struct {
	*storage.LocalBackend
	mu          sync.Mutex
	writes      []string
	failList    bool
	failDeletes bool
}

func isDiscoveryFile(p string) bool {
	base := filepath.Base(p)
	return base == "version-hint.text" || (strings.HasPrefix(base, "v") && strings.HasSuffix(base, ".metadata.json"))
}

func (d *discoveryRecorder) note(p string) {
	if isDiscoveryFile(p) {
		d.mu.Lock()
		d.writes = append(d.writes, filepath.Base(p))
		d.mu.Unlock()
	}
}

func (d *discoveryRecorder) Write(ctx context.Context, p string, data []byte) error {
	d.note(p)
	return d.LocalBackend.Write(ctx, p, data)
}

func (d *discoveryRecorder) WriteIfAbsent(ctx context.Context, p string, data []byte) (bool, error) {
	d.note(p)
	return d.LocalBackend.WriteIfAbsent(ctx, p, data)
}

func (d *discoveryRecorder) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	if d.failList && strings.HasSuffix(prefix, "/metadata/") {
		return nil, errors.New("listing refused")
	}
	return d.LocalBackend.ListObjects(ctx, prefix)
}

func (d *discoveryRecorder) Delete(ctx context.Context, p string) error {
	if d.failDeletes && isDiscoveryFile(p) {
		return errors.New("delete refused")
	}
	return d.LocalBackend.Delete(ctx, p)
}

func (d *discoveryRecorder) take() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	w := d.writes
	d.writes = nil
	return w
}

// record puts a discoveryRecorder between the rig's exporter and its backend.
func (r *rig1686) record() *discoveryRecorder {
	r.t.Helper()
	local, ok := r.exp.backend.(*storage.LocalBackend)
	if !ok {
		r.t.Fatalf("the rig's backend is %T, want the local one", r.exp.backend)
	}
	d := &discoveryRecorder{LocalBackend: local}
	r.exp.backend = d
	return d
}

func (r *rig1686) passResult(files []FileRef, sc ArcSchema) reconcileResult {
	r.t.Helper()
	res, err := r.exp.reconcile(context.Background(), reconcileRequest{database: "mydb", measurement: "cpu", schema: sc, current: files})
	if err != nil {
		r.t.Fatal(err)
	}
	return res
}

func (r *rig1686) metaPath(name string) string {
	return filepath.Join(r.root, "arc_mydb.db", "cpu", "metadata", name)
}

// A pass that commits nothing and finds its own publish in place writes
// neither discovery file (#1787): it used to be refused its own v<N> copy,
// read it back and write the hint it had just read.
func TestPublish1787_APassThatFindsItsOwnPublishWritesNothing(t *testing.T) {
	r := newHealRig(t, 3)
	files, sc := r.history(3)
	rec := r.record()

	res := r.passResult(files, sc)
	if w := rec.take(); len(w) != 0 {
		t.Fatalf("a pass with nothing to commit and its publish in place wrote %v", w)
	}
	if !res.settled {
		t.Fatal("the pass that published nothing is not settled: the scheduler would repeat it")
	}
	cur := r.hint()

	// The hint names an older version (a publish that stopped between the
	// copy and the hint, a restore): published again.
	if err := os.WriteFile(r.metaPath("version-hint.text"), []byte(strconv.Itoa(cur-1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if res := r.passResult(files, sc); !res.settled {
		t.Fatal("the pass that found the hint behind is not settled")
	}
	if w := rec.take(); len(w) == 0 || r.hint() != cur {
		t.Fatalf("hint behind the table: wrote %v, hint %d, want version %d published again", w, r.hint(), cur)
	}

	// The hint is gone: published again.
	if err := os.Remove(r.metaPath("version-hint.text")); err != nil {
		t.Fatal(err)
	}
	r.passResult(files, sc)
	if w := rec.take(); len(w) == 0 || r.hint() != cur {
		t.Fatalf("hint missing: wrote %v, want version %d published again", w, cur)
	}

	// And again nothing once it is back.
	r.passResult(files, sc)
	if w := rec.take(); len(w) != 0 {
		t.Fatalf("the pass after the hint was restored wrote %v", w)
	}
}

// What another process published is not taken on trust: the first pass of a
// process publishes, as it did before, and with it runs what goes with a
// publish (the drop marker, the Glue notice).
func TestPublish1787_TheFirstPassOfAProcessPublishes(t *testing.T) {
	r := newHealRig(t, 3)
	files, sc := r.history(3)
	local := r.exp.backend.(*storage.LocalBackend)

	db, err := sql.Open("sqlite3", filepath.Join(r.root, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	restarted, err := NewExporter(db, local, "file://"+r.root, "arc", 3, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	restarted.resolveBases() // from the backend itself, before the recorder stands in front of it
	r.exp = restarted
	rec := r.record()
	restarted.SetWarehouseAuthoritative(true) // the adopt check confirms the row; that is not a publish
	notified := 0
	restarted.SetOnPublished(func(string, string) { notified++ })

	r.passResult(files, sc)
	if w := rec.take(); len(w) == 0 || notified != 1 {
		t.Fatalf("first pass of a process: wrote %v, %d publish notices; want the discovery files written and one notice", w, notified)
	}
	r.passResult(files, sc)
	if w := rec.take(); len(w) != 0 || notified != 1 {
		t.Fatalf("second pass of the process: wrote %v, %d publish notices; want nothing more", w, notified)
	}
}

// A dropped database's tables are forgotten: one recreated under the same
// name can reach the same metadata location, and must publish.
func TestPublish1787_ADroppedTableIsPublishedAgain(t *testing.T) {
	r := newHealRig(t, 3)
	r.history(2)
	key := strings.Join(r.exp.tableIdent("mydb", "cpu"), "\x00")
	hintedLoc := func() (string, bool) {
		r.exp.publishedMu.Lock()
		defer r.exp.publishedMu.Unlock()
		loc, ok := r.exp.hinted[key]
		return loc, ok
	}
	if loc, _ := hintedLoc(); loc == "" {
		t.Fatal("no record of the publish after two passes")
	}
	if err := r.exp.DropDatabase(context.Background(), "mydb"); err != nil {
		t.Fatal(err)
	}
	if loc, ok := hintedLoc(); ok {
		t.Fatalf("the dropped table's publish is still remembered (%s)", loc)
	}
}

// The prune reports when the versions it kept for their age can go: the
// moment the last of them has had its successor published for
// min_version_age. One pass then removes them all.
func TestPrune1787_ReportsWhenTheHeldVersionsComeOfAge(t *testing.T) {
	r := newHealRig(t, 1)
	r.exp.SetVersionRetention(time.Hour, 0)
	files, sc := r.history(6)
	all := r.vCopies() // ascending; retain+1 = 2 newest are kept by count
	if len(all) < 5 {
		t.Fatalf("copies %v, want every version kept inside the minimum age", all)
	}
	// Every copy was written 30 minutes ago, except the successor of the
	// newest version held by age: 10 minutes ago.
	now := time.Now()
	for _, v := range all {
		at := now.Add(-30 * time.Minute)
		if v == all[len(all)-2] {
			at = now.Add(-10 * time.Minute)
		}
		if err := os.Chtimes(r.metaPath("v"+strconv.Itoa(v)+".metadata.json"), at, at); err != nil {
			t.Fatal(err)
		}
	}
	res := r.passResult(files, sc)
	want := now.Add(50 * time.Minute)
	if d := res.pruneAgainAt.Sub(want); d < -2*time.Second || d > 2*time.Second {
		t.Fatalf("prune again at %v, want %v: the latest successor's write time plus the minimum age", res.pruneAgainAt, want)
	}

	// Past that time one pass removes them all, and nothing waits.
	old := now.Add(-2 * time.Hour)
	for _, v := range all {
		if err := os.Chtimes(r.metaPath("v"+strconv.Itoa(v)+".metadata.json"), old, old); err != nil {
			t.Fatal(err)
		}
	}
	res = r.passResult(files, sc)
	if got := r.vCopies(); len(got) != 2 {
		t.Fatalf("copies %v once every successor is old enough, want retain_snapshots+1 = 2", got)
	}
	if !res.pruneAgainAt.IsZero() {
		t.Fatalf("prune again at %v with nothing left to prune", res.pruneAgainAt)
	}
}

// A prune that could not list the directory or delete a copy asks for the
// next pass: a quiet table would otherwise keep those copies for good.
func TestPrune1787_AFailedPruneIsRetried(t *testing.T) {
	for _, minAge := range []time.Duration{time.Millisecond, 0} {
		r := newHealRig(t, 10)
		r.exp.SetVersionRetention(minAge, 0)
		files, sc := r.history(5)
		rec := r.record()
		r.exp.retain = 1 // the copies of the earlier passes are now over the bound
		time.Sleep(5 * time.Millisecond)

		if minAge > 0 { // only the listing by age is an ObjectLister's
			rec.failList = true
			before := time.Now()
			res := r.passResult(files, sc)
			if res.pruneAgainAt.Before(before) || res.pruneAgainAt.After(time.Now()) {
				t.Fatalf("min age %v, listing failed: prune again at %v, want now", minAge, res.pruneAgainAt)
			}
			rec.failList = false
		}

		rec.failDeletes = true
		before := time.Now()
		res := r.passResult(files, sc)
		if res.pruneAgainAt.Before(before) || res.pruneAgainAt.After(time.Now()) {
			t.Fatalf("min age %v, deletes failed: prune again at %v, want now", minAge, res.pruneAgainAt)
		}
		rec.failDeletes = false

		res = r.passResult(files, sc)
		if got := r.vCopies(); len(got) > 3 || !res.pruneAgainAt.IsZero() {
			t.Fatalf("min age %v: copies %v, prune again at %v after the retry; want them pruned and nothing waiting", minAge, got, res.pruneAgainAt)
		}
	}
}

// A table on an adopted row points at the published copy, v<N>.metadata.json:
// a name a dropped and recreated table reaches again, so it is not taken as
// proof of this process's publish. Such a table publishes at every pass.
func TestPublish1787_ATableOnAnAdoptedRowPublishesEveryPass(t *testing.T) {
	r := newHealRig(t, 3)
	files, sc := r.history(3)
	local := r.exp.backend.(*storage.LocalBackend)

	// A node with an empty catalog over the same warehouse adopts the
	// published version.
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "other.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	other, err := NewExporter(db, local, "file://"+r.root, "arc", 3, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	other.SetWarehouseAuthoritative(true)
	other.resolveBases()
	r.exp = other
	rec := r.record()
	for pass := 1; pass <= 2; pass++ {
		res := r.passResult(files, sc)
		if w := rec.take(); len(w) == 0 || !res.settled {
			t.Fatalf("pass %d on the adopted row: wrote %v, settled %v; want the discovery files published", pass, w, res.settled)
		}
	}
}

// A copy's write time ahead of this node's clock does not put the prune off
// past one minimum age from now.
func TestPrune1787_AWriteTimeInTheFutureIsBounded(t *testing.T) {
	r := newHealRig(t, 1)
	r.exp.SetVersionRetention(time.Hour, 0)
	files, sc := r.history(5)
	ahead := time.Now().Add(24 * time.Hour)
	for _, v := range r.vCopies() {
		if err := os.Chtimes(r.metaPath("v"+strconv.Itoa(v)+".metadata.json"), ahead, ahead); err != nil {
			t.Fatal(err)
		}
	}
	res := r.passResult(files, sc)
	if until := time.Until(res.pruneAgainAt); until <= 0 || until > time.Hour {
		t.Fatalf("prune due in %v with copies written a day ahead, want within the minimum age of an hour", until)
	}
}

// The copies a pass of several commits keeps for itself are over the bound
// when it ends. The pass asks for another, which removes them; a table that
// went quiet used to keep them.
func TestPrune1787_CopiesKeptForOnePassAreRemovedByTheNext(t *testing.T) {
	r := newHealRig(t, 1)
	r.exp.chunkMax = 1
	var files []FileRef
	for i := 0; i < 5; i++ {
		files = append(files, r.write(0, "k"+strconv.Itoa(i)))
	}
	sc := r.schema(files[0])
	unread := append([]FileRef(nil), files...)
	before := time.Now()
	res, err := r.exp.reconcile(context.Background(), reconcileRequest{database: "mydb", measurement: "cpu", schema: sc, current: files,
		unread: unread, readSchemas: func(context.Context, []FileRef) (ArcSchema, error) { return sc, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if res.commits < 3 {
		t.Fatalf("%d commits, want a pass of several", res.commits)
	}
	if got := r.vCopies(); len(got) <= 2 {
		t.Fatalf("copies %v after a pass of %d commits at retain 1: nothing was kept over the bound, the test shows nothing", got, res.commits)
	}
	if res.pruneAgainAt.Before(before) {
		t.Fatalf("prune again at %v after a pass that kept its own copies over the bound, want now", res.pruneAgainAt)
	}
	res = r.passResult(files, sc)
	if got := r.vCopies(); len(got) != 2 || !res.pruneAgainAt.IsZero() {
		t.Fatalf("copies %v, prune again at %v after the next pass; want retain_snapshots+1 = 2 and nothing waiting", got, res.pruneAgainAt)
	}
}
