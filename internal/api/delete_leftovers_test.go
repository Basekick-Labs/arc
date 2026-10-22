package api

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// A file an earlier DELETE replaced and could not remove (a leftover) is
// never read into a new file (delete_leftovers.go).

const leftoverHour = "db/cpu/2026/10/07/20/"

type leftoverRig struct {
	t       *testing.T
	root    string
	backend *storage.LocalBackend
	db      *database.DuckDB
	h       *DeleteHandler
}

func newLeftoverRig(t *testing.T) *leftoverRig {
	t.Helper()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	db, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 1, LocalStorageRoot: root}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &leftoverRig{t: t, root: root, backend: backend, db: db,
		h: &DeleteHandler{db: db, storage: backend, logger: zerolog.Nop()}}
}

// parquet writes key with one row per host.
func (r *leftoverRig) parquet(key string, hosts ...string) {
	r.t.Helper()
	full := filepath.Join(r.root, key)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		r.t.Fatal(err)
	}
	vals := make([]string, len(hosts))
	for i, h := range hosts {
		vals[i] = fmt.Sprintf("('%s', %d)", h, i)
	}
	q := fmt.Sprintf("COPY (SELECT * FROM (VALUES %s) AS v(host, v)) TO '%s' (FORMAT PARQUET)", strings.Join(vals, ", "), full)
	if _, err := r.db.DB().Exec(q); err != nil {
		r.t.Fatal(err)
	}
}

func (r *leftoverRig) files() []string {
	r.t.Helper()
	all, err := r.backend.List(context.Background(), "db/cpu/")
	if err != nil {
		r.t.Fatal(err)
	}
	var out []string
	for _, p := range all {
		if strings.HasSuffix(p, ".parquet") {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func (r *leftoverRig) exists(key string) bool {
	_, err := os.Stat(filepath.Join(r.root, key))
	return err == nil
}

func (r *leftoverRig) retired() []string {
	r.t.Helper()
	recs, err := r.h.retiredRecords(context.Background())
	if err != nil {
		r.t.Fatal(err)
	}
	var out []string
	for _, rec := range recs {
		out = append(out, rec.Path)
	}
	return out
}

func (r *leftoverRig) scan(where string) []string {
	r.t.Helper()
	affected, _, err := r.h.findAffectedFiles(context.Background(), "db", "cpu", where, scanOpts{})
	if err != nil {
		r.t.Fatal(err)
	}
	var out []string
	for _, a := range affected {
		out = append(out, a.relativePath)
	}
	sort.Strings(out)
	return out
}

// failReplacedDeletes makes the removal of a replaced file fail for paths.
func failReplacedDeletes(t *testing.T, paths ...string) {
	t.Helper()
	prev := deleteReplaced
	fail := map[string]bool{}
	for _, p := range paths {
		fail[p] = true
	}
	deleteReplaced = func(ctx context.Context, b storage.Backend, path string) error {
		if fail[path] {
			return errors.New("injected: storage refused the delete")
		}
		return prev(ctx, b, path)
	}
	t.Cleanup(func() { deleteReplaced = prev })
}

func (r *leftoverRig) rewrite(key, where string) error {
	r.t.Helper()
	_, err := r.h.rewriteFileWithoutDeletedRows(context.Background(), filepath.Join(r.root, key), key, where, "")
	return err
}

// The bug: the first DELETE's removal of the old file fails; the retry used
// to rewrite the leftover again, leaving two rewrites with the same rows. The
// retry now leaves it out of its scan and removes it.
func TestRetriedDeleteRemovesTheLeftoverInsteadOfRewritingIt(t *testing.T) {
	r := newLeftoverRig(t)
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	r.parquet(old, "h0", "h1", "h1", "h2")
	failReplacedDeletes(t, old)
	if err := r.rewrite(old, "host = 'h0'"); err == nil {
		t.Fatal("the rewrite reported success with the old file still in storage")
	}
	if got := r.retired(); len(got) != 1 || got[0] != old {
		t.Fatalf("retired journal %v, want the old file", got)
	}
	if files := r.files(); len(files) != 2 {
		t.Fatalf("files after the failed removal: %v", files)
	}
	deleteReplaced = storage.DeleteWithRetry

	// The retry: the leftover still matches the predicate, and must not be
	// scanned.
	if got := r.scan("host = 'h0'"); len(got) != 0 {
		t.Fatalf("retry scans %v; the leftover would be rewritten a second time", got)
	}
	if n := r.h.removeLeftovers(context.Background(), "db", "cpu"); n != 1 {
		t.Fatalf("removed %d leftovers, want 1", n)
	}
	files := r.files()
	if len(files) != 1 || files[0] == old || storage.RewriteFamily(files[0]) != old {
		t.Fatalf("files after the retry: %v", files)
	}
	if got := r.retired(); len(got) != 0 {
		t.Fatalf("retired journal not cleared: %v", got)
	}
}

// A rewrite whose old file goes leaves no record behind.
func TestRewriteClearsItsRetiredRecord(t *testing.T) {
	r := newLeftoverRig(t)
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	r.parquet(old, "h0", "h1")
	if err := r.rewrite(old, "host = 'h0'"); err != nil {
		t.Fatal(err)
	}
	if r.exists(old) || len(r.retired()) != 0 || len(r.files()) != 1 {
		t.Fatalf("after a clean rewrite: files %v, journal %v", r.files(), r.retired())
	}
}

// With no DELETE at all (a restart), the drainer removes a journaled
// leftover.
func TestDrainRetiredRemovesJournaledLeftovers(t *testing.T) {
	r := newLeftoverRig(t)
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	r.parquet(old, "h0", "h1")
	failReplacedDeletes(t, old)
	_ = r.rewrite(old, "host = 'h0'")
	if n := r.h.DrainRetired(context.Background()); n != 0 {
		t.Fatalf("drained %d while the delete still fails", n)
	}
	if len(r.retired()) != 1 {
		t.Fatal("record dropped while the file could not be removed")
	}
	deleteReplaced = storage.DeleteWithRetry
	if n := r.h.DrainRetired(context.Background()); n != 1 {
		t.Fatalf("drained %d, want 1", n)
	}
	if r.exists(old) || len(r.retired()) != 0 {
		t.Fatal("leftover or its record kept")
	}
	// A record whose file is already gone is cleared too.
	if err := r.h.journalRetired(context.Background(), old, old+"x"); err != nil {
		t.Fatal(err)
	}
	r.h.DrainRetired(context.Background())
	if len(r.retired()) != 0 {
		t.Fatal("record of a missing file kept")
	}
}

// No journal (a leftover from before it, or a crash before the record):
// in a rewrite family the member with the fewest rows is current, and every
// member with more is left out and removed.
func TestStandaloneLeftoverByRowCount(t *testing.T) {
	r := newLeftoverRig(t)
	orig := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	r1 := leftoverHour + "cpu_20261007_204851_558252000_rewrite_100.parquet"
	r2 := leftoverHour + "cpu_20261007_204851_558252000_rewrite_200.parquet"
	other := leftoverHour + "cpu_20261007_205000_000000000.parquet"
	r.parquet(orig, "h0", "h1", "h1", "h2")
	r.parquet(r1, "h1", "h1", "h2")
	r.parquet(r2, "h1", "h1") // the current one: the fewest rows
	r.parquet(other, "h1")
	if got := r.scan("host = 'h1'"); len(got) != 2 || got[0] != r2 || got[1] != other {
		t.Fatalf("scanned %v, want only the current member and the unrelated file", got)
	}
	if n := r.h.removeLeftovers(context.Background(), "db", "cpu"); n != 2 {
		t.Fatalf("removed %d, want 2", n)
	}
	if r.exists(orig) || r.exists(r1) || !r.exists(r2) || !r.exists(other) {
		t.Fatalf("files after removal: %v", r.files())
	}
	// A lone member is never a leftover.
	if n := r.h.removeLeftovers(context.Background(), "db", "cpu"); n != 0 || !r.exists(r2) {
		t.Fatal("removed the last member of a family")
	}
}

// Members with the same row count are not a chain: none is known to be
// current, so all are scanned and none is removed.
func TestStandaloneTiedRewritesAreAllScanned(t *testing.T) {
	r := newLeftoverRig(t)
	a := leftoverHour + "cpu_20261007_204851_558252000_rewrite_100.parquet"
	b := leftoverHour + "cpu_20261007_204851_558252000_rewrite_200.parquet"
	r.parquet(a, "h1", "h2")
	r.parquet(b, "h1", "h3")
	if got := r.scan("host = 'h1'"); len(got) != 2 {
		t.Fatalf("scanned %v, want both", got)
	}
	if n := r.h.removeLeftovers(context.Background(), "db", "cpu"); n != 0 {
		t.Fatalf("removed %d of a tied family", n)
	}
}

// A clause that matches nothing at the rewrite does not rewrite the file
// under another name (equal row counts in one family).
func TestRewriteMatchingNothingKeepsTheFile(t *testing.T) {
	r := newLeftoverRig(t)
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	r.parquet(old, "h1", "h2")
	n, err := r.h.rewriteFileWithoutDeletedRows(context.Background(), filepath.Join(r.root, old), old, "host = 'h9'", "")
	if err != nil || n != 0 {
		t.Fatalf("rewrite of nothing: %d %v", n, err)
	}
	if files := r.files(); len(files) != 1 || files[0] != old {
		t.Fatalf("files %v", files)
	}
}

// Cluster: an unlisted member is left out (never rewritten, never adopted)
// while another member of its family is listed in the manifest or held by
// the outbox; it is not removed (the manifest decides).
type leftoverCoordinator struct {
	rewriteManifestCoordinator
	listed  map[string]bool
	sizes   map[string]int64
	syncs   int
	syncErr error
	cas     bool
	owned   map[string]bool
}

func (c *leftoverCoordinator) SyncManifest(context.Context) error {
	c.syncs++
	return c.syncErr
}
func (c *leftoverCoordinator) CompactionCommitActive() bool { return c.cas }
func (c *leftoverCoordinator) WaitLocalFiles(context.Context, string, string, time.Duration) ([]string, error) {
	return nil, nil
}
func (c *leftoverCoordinator) ManifestHotFiles(string, string) []raft.FileEntry { return nil }
func (c *leftoverCoordinator) CompactionOwns(p string) bool                     { return c.owned[p] }

func (c *leftoverCoordinator) GetFileEntry(p string) (*raft.FileEntry, bool) {
	if c.listed[p] {
		return &raft.FileEntry{Path: p, SizeBytes: c.sizes[p]}, true
	}
	return nil, false
}

func (c *leftoverCoordinator) HasRewriteSibling(p string) bool {
	for l := range c.listed {
		if l != p && storage.RewriteFamily(l) == storage.RewriteFamily(p) {
			return true
		}
	}
	return false
}

type leftoverHolder struct{ held map[string]bool }

func (h leftoverHolder) ReplacePending(context.Context, string, string, int64, string) (cluster.ReplaceOutcome, raft.FileEntry, error) {
	return cluster.ReplaceNotPending, raft.FileEntry{}, nil
}
func (h leftoverHolder) Withdraw(context.Context, string) (bool, cluster.ReplaceOutcome, error) {
	return false, cluster.ReplaceNotPending, nil
}
func (h leftoverHolder) HoldsRegistration(p string) bool { return h.held[p] }

func TestClusterLeftoverIsLeftOut(t *testing.T) {
	r := newLeftoverRig(t)
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	rw := leftoverHour + "cpu_20261007_204851_558252000_rewrite_100.parquet"
	pend := leftoverHour + "cpu_20261007_210000_000000000.parquet"
	pendOld := leftoverHour + "cpu_20261007_210500_000000000.parquet"
	pendRw := leftoverHour + "cpu_20261007_210500_000000000_rewrite_7.parquet"
	r.parquet(old, "h0", "h1")
	r.parquet(rw, "h1")
	r.parquet(pend, "h1")
	r.parquet(pendOld, "h0", "h1")
	r.parquet(pendRw, "h1")
	// Held (being registered) while another member is listed: kept.
	heldOld := leftoverHour + "cpu_20261007_211000_000000000.parquet"
	heldRw := leftoverHour + "cpu_20261007_211000_000000000_rewrite_3.parquet"
	r.parquet(heldOld, "h1")
	r.parquet(heldRw, "h1")
	coord := &leftoverCoordinator{listed: map[string]bool{rw: true, heldOld: true}}
	r.h.coordinator = coord
	r.h.pending = leftoverHolder{held: map[string]bool{pend: true, pendRw: true, heldRw: true}}

	files, err := r.h.listParquet(context.Background(), "db", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	kept, left, err := r.h.leftoverFiles(context.Background(), files)
	if err != nil {
		t.Fatal(err)
	}
	var keptPaths, leftPaths []string
	for _, f := range kept {
		keptPaths = append(keptPaths, f.relativePath)
	}
	for _, l := range left {
		leftPaths = append(leftPaths, l.path)
	}
	sort.Strings(keptPaths)
	sort.Strings(leftPaths)
	wantKept := []string{rw, pend, pendRw, heldOld, heldRw}
	sort.Strings(wantKept)
	wantLeft := []string{old, pendOld}
	sort.Strings(wantLeft)
	if strings.Join(keptPaths, ",") != strings.Join(wantKept, ",") || strings.Join(leftPaths, ",") != strings.Join(wantLeft, ",") {
		t.Fatalf("kept %v left %v; want kept %v left %v", keptPaths, leftPaths, wantKept, wantLeft)
	}
	// An unlisted file with no listed or held sibling is scanned as before.
	delete(coord.listed, rw)
	_, left, _ = r.h.leftoverFiles(context.Background(), files)
	for _, l := range left {
		if l.path == old {
			t.Fatal("an unlisted file with no listed sibling was left out")
		}
	}
	// Nothing is removed on a cluster.
	if n := r.h.removeLeftovers(context.Background(), "db", "cpu"); n != 0 || !r.exists(old) {
		t.Fatal("a cluster DELETE removed an unlisted file")
	}
}

// The member that holds a leftover's rows is gone by the time the leftover
// would be removed (removed meanwhile): the leftover is then the last member
// and stays.
func TestRemoveLeftoversNeverRemovesTheLastMember(t *testing.T) {
	r := newLeftoverRig(t)
	orig := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	r1 := leftoverHour + "cpu_20261007_204851_558252000_rewrite_100.parquet"
	r2 := leftoverHour + "cpu_20261007_204851_558252000_rewrite_200.parquet"
	r.parquet(orig, "h0", "h1", "h1", "h2")
	r.parquet(r1, "h1", "h1", "h2")
	r.parquet(r2, "h1", "h1")
	prev := deleteReplaced
	t.Cleanup(func() { deleteReplaced = prev })
	deleteReplaced = func(ctx context.Context, b storage.Backend, path string) error {
		if path == orig {
			// Something else removes the current member at the same time.
			_ = os.Remove(filepath.Join(r.root, r2))
		}
		return prev(ctx, b, path)
	}
	r.h.removeLeftovers(context.Background(), "db", "cpu")
	if !r.exists(r1) {
		t.Fatalf("removed the last member of the family: %v", r.files())
	}
}

// A journal record stored under another file's key is not trusted.
func TestRetiredRecordUnderTheWrongKeyIsIgnored(t *testing.T) {
	r := newLeftoverRig(t)
	victim := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	r.parquet(victim, "h1")
	data := []byte(`{"v":1,"path":"` + victim + `","replaced_by":"x"}`)
	if err := r.backend.Write(context.Background(), deleteRetiredRoot+"/forged.json", data); err != nil {
		t.Fatal(err)
	}
	if got := r.retired(); len(got) != 0 {
		t.Fatalf("trusted a record under the wrong key: %v", got)
	}
	r.h.DrainRetired(context.Background())
	if !r.exists(victim) {
		t.Fatal("drained a file named by a record under the wrong key")
	}
}

// The cluster classification reads the manifest as the leader has it: it
// syncs first, and a failed sync stops a compare-and-set DELETE (a lagging
// manifest takes the current file for a leftover) or, without compaction
// commit, scans everything as before.
func TestClusterLeftoversSyncFirst(t *testing.T) {
	r := newLeftoverRig(t)
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	rw := leftoverHour + "cpu_20261007_204851_558252000_rewrite_100.parquet"
	r.parquet(old, "h0", "h1")
	r.parquet(rw, "h1")
	coord := &leftoverCoordinator{listed: map[string]bool{rw: true}}
	r.h.coordinator = coord
	files, _ := r.h.listParquet(context.Background(), "db", "cpu")
	if _, left, err := r.h.leftoverFiles(context.Background(), files); err != nil || len(left) != 1 || coord.syncs != 1 {
		t.Fatalf("left %v err %v syncs %d", left, err, coord.syncs)
	}
	coord.syncErr = errors.New("no leader")
	if kept, left, err := r.h.leftoverFiles(context.Background(), files); err != nil || len(left) != 0 || len(kept) != 2 {
		t.Fatalf("commit off, sync failed: kept %d left %v err %v", len(kept), left, err)
	}
	coord.cas = true
	if _, _, err := r.h.leftoverFiles(context.Background(), files); !errors.Is(err, errManifestNotSynced) {
		t.Fatalf("commit on, sync failed: %v", err)
	}
}

// With compaction commit on, a cluster DELETE removes a leftover that is
// provably older than a listed member: that member is here at its registered
// size and has fewer rows. Never a file with fewer rows (a rewrite still
// being committed), one a compaction owns, or one whose listed member is
// not here at its size.
func TestClusterRemovesOnlyProvablyOlderLeftovers(t *testing.T) {
	r := newLeftoverRig(t)
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	rw := leftoverHour + "cpu_20261007_204851_558252000_rewrite_100.parquet"
	inflight := leftoverHour + "cpu_20261007_220000_000000000_rewrite_9.parquet"
	inflightBase := leftoverHour + "cpu_20261007_220000_000000000.parquet"
	r.parquet(old, "h0", "h1")
	r.parquet(rw, "h1")
	r.parquet(inflightBase, "h0", "h1")
	r.parquet(inflight, "h1") // a newer rewrite another node is committing
	size := func(p string) int64 {
		fi, err := os.Stat(filepath.Join(r.root, p))
		if err != nil {
			t.Fatal(err)
		}
		return fi.Size()
	}
	coord := &leftoverCoordinator{cas: true, listed: map[string]bool{rw: true, inflightBase: true},
		sizes: map[string]int64{rw: size(rw), inflightBase: size(inflightBase)}, owned: map[string]bool{}}
	r.h.coordinator = coord

	coord.owned[old] = true
	if n := r.h.removeLeftovers(context.Background(), "db", "cpu"); n != 0 {
		t.Fatalf("removed %d with the leftover owned by a compaction", n)
	}
	delete(coord.owned, old)
	coord.sizes[rw] = 1
	if n := r.h.removeLeftovers(context.Background(), "db", "cpu"); n != 0 {
		t.Fatalf("removed %d while the listed member is not here at its size", n)
	}
	coord.sizes[rw] = size(rw)
	if n := r.h.removeLeftovers(context.Background(), "db", "cpu"); n != 1 || r.exists(old) {
		t.Fatalf("removed %d; old exists %v", n, r.exists(old))
	}
	if !r.exists(inflight) || !r.exists(inflightBase) || !r.exists(rw) {
		t.Fatal("removed a file with fewer rows than its listed member, or a listed one")
	}
	// Same rows as the listed copy: not provably older.
	r.parquet(old, "h1")
	if n := r.h.removeLeftovers(context.Background(), "db", "cpu"); n != 0 || !r.exists(old) {
		t.Fatalf("removed %d copies with as many rows as the listed one", n)
	}
	// Commit off: nothing removed.
	r.parquet(old, "h0", "h1")
	coord.cas = false
	if n := r.h.removeLeftovers(context.Background(), "db", "cpu"); n != 0 {
		t.Fatalf("removed %d with compaction commit off", n)
	}
}

// With compaction commit on, the scan classifies inside scanSet, on the
// manifest its one sync caught up: the leftover is never scanned, so never
// adopted again.
func TestCASScanLeavesTheLeftoverOutAfterItsSync(t *testing.T) {
	r := newLeftoverRig(t)
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	rw := leftoverHour + "cpu_20261007_204851_558252000_rewrite_100.parquet"
	r.parquet(old, "h0", "h1")
	r.parquet(rw, "h1")
	coord := &leftoverCoordinator{cas: true, listed: map[string]bool{rw: true}}
	r.h.coordinator = coord
	affected, _, n, err := r.h.scanFiles(context.Background(), "db", "cpu", "host = 'h1'", scanOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(affected) != 1 || affected[0].relativePath != rw {
		t.Fatalf("leftovers %d, affected %+v; want only the listed rewrite", n, affected)
	}
	if coord.syncs != 1 {
		t.Fatalf("synced %d times, want 1", coord.syncs)
	}
}

// A family member whose footer cannot be read stops the scan: scanning the
// family whole is what rewrote a leftover a second time.
func TestUnreadableFamilyFailsClosed(t *testing.T) {
	r := newLeftoverRig(t)
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	rw := leftoverHour + "cpu_20261007_204851_558252000_rewrite_100.parquet"
	r.parquet(old, "h0", "h1")
	if err := os.WriteFile(filepath.Join(r.root, rw), []byte("not parquet"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.h.findAffectedFiles(context.Background(), "db", "cpu", "host = 'h0'", scanOpts{}); err == nil {
		t.Fatal("scanned a family whose members could not be counted")
	}
}

// A row the clause cannot decide (NULL) is not matched, so the rewrite keeps
// it: `WHERE NOT (w)` dropped it without counting it deleted.
func TestRewriteKeepsRowsTheClauseCannotDecide(t *testing.T) {
	r := newLeftoverRig(t)
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	full := filepath.Join(r.root, old)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := r.db.DB().Exec(fmt.Sprintf("COPY (SELECT * FROM (VALUES ('a', 1), (NULL, 2), ('b', 3)) AS v(host, v)) TO '%s' (FORMAT PARQUET)", full)); err != nil {
		t.Fatal(err)
	}
	n, err := r.h.rewriteFileWithoutDeletedRows(context.Background(), full, old, "host <> 'a'", "")
	if err != nil || n != 1 {
		t.Fatalf("deleted %d (%v), want 1", n, err)
	}
	files := r.files()
	if len(files) != 1 {
		t.Fatalf("files %v", files)
	}
	var rows int
	if err := r.db.DB().QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", filepath.Join(r.root, files[0]))).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("rewrite holds %d rows, want 2 (the NULL host is not deleted)", rows)
	}
}
