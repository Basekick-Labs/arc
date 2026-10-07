package iceberg

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// sweepRig is a table on a real local backend, which the orphan sweep needs: it reads the
// metadata directory through storage.ObjectLister for the file ages, and newTestExporter passes a
// nil backend.
type sweepRig struct {
	exp     *Exporter
	backend *storage.LocalBackend
	root    string
	metaDir string // absolute path of the table's metadata directory
}

// newSweepRig builds a rig whose warehouse IS the storage root — the default wiring
// (DefaultWarehouse), where "trim the warehouse" and "trim the storage root" are the same string.
func newSweepRig(t *testing.T, retain int) *sweepRig {
	t.Helper()
	return newSweepRigAt(t, retain, "")
}

// newSweepRigAt puts the warehouse in a SUBDIRECTORY of the storage root when sub is non-empty.
// That is the non-default iceberg.warehouse an operator actually sets, and the configuration in
// which #534 shipped broken: at the default the two trims coincide, so only this shape proves the
// sweep derives its directory key from the warehouse rather than from the storage root.
func newSweepRigAt(t *testing.T, retain int, sub string) *sweepRig {
	t.Helper()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(root, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	warehouse := root
	if sub != "" {
		warehouse = filepath.Join(root, sub)
		if err := os.MkdirAll(warehouse, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	exp, err := NewExporter(db, backend, "file://"+warehouse, "arc", retain, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	return &sweepRig{exp: exp, backend: backend, root: root}
}

// pass reconciles the measurement to exactly the given local parquet files, the way a reconcile
// tick does, and records where the table's metadata lives.
func (r *sweepRig) pass(t *testing.T, ctx context.Context, files ...string) {
	t.Helper()
	refs := make([]FileRef, 0, len(files))
	for _, f := range files {
		refs = append(refs, refOf(t, f))
	}
	sc, err := UnionSchema(ctx, files)
	if err != nil {
		t.Fatalf("UnionSchema: %v", err)
	}
	if err := r.exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, refs); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	tbl := r.load(t, ctx)
	loc := strings.TrimPrefix(tbl.MetadataLocation(), "file://")
	r.metaDir = filepath.Dir(loc)
}

func (r *sweepRig) load(t *testing.T, ctx context.Context) *icetable.Table {
	t.Helper()
	tbl, err := r.exp.catalog.LoadTable(ctx, r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		t.Fatalf("load table: %v", err)
	}
	return tbl
}

// names returns the basenames in the metadata directory with the given suffix.
func (r *sweepRig) names(t *testing.T, suffix string) []string {
	t.Helper()
	ents, err := os.ReadDir(r.metaDir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		if !e.IsDir() && strings.HasSuffix(e.Name(), suffix) {
			out = append(out, e.Name())
		}
	}
	return out
}

// age backdates every .avro and .metadata.json in the metadata directory so the next sweep sees
// them as past the grace. Metadata files are aged too, only so a test cannot accidentally depend
// on their mtime.
func (r *sweepRig) age(t *testing.T, by time.Duration) {
	t.Helper()
	ents, err := os.ReadDir(r.metaDir)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Now().Add(-by)
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		p := filepath.Join(r.metaDir, e.Name())
		if err := os.Chtimes(p, when, when); err != nil {
			t.Fatal(err)
		}
	}
}

// plantOrphan writes a file into the metadata directory that no metadata references, aged past
// any grace.
func (r *sweepRig) plantOrphan(t *testing.T, name string, agedBy time.Duration) string {
	t.Helper()
	p := filepath.Join(r.metaDir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("not a real avro"), 0o600); err != nil {
		t.Fatal(err)
	}
	when := time.Now().Add(-agedBy)
	if err := os.Chtimes(p, when, when); err != nil {
		t.Fatal(err)
	}
	return p
}

func exists(t *testing.T, p string) bool {
	t.Helper()
	_, err := os.Stat(p)
	return err == nil
}

// assertEveryOnDiskMetadataStillResolves is the contract the sweep must never break: for EVERY
// metadata.json left in the directory — the current one, iceberg-go's NNNNN-* log and Arc's v<N>
// copies for directory readers — every snapshot's manifest list and every manifest in it must
// still be readable. This is what catches an over-deleting sweep, and it is deliberately not
// expressed as a file count: the right count is whatever reachability says.
func (r *sweepRig) assertEveryOnDiskMetadataStillResolves(t *testing.T, ctx context.Context) {
	t.Helper()
	tbl := r.load(t, ctx)
	fio, err := tbl.FS(ctx)
	if err != nil {
		t.Fatalf("table FS: %v", err)
	}
	metas := r.names(t, ".metadata.json")
	if len(metas) == 0 {
		t.Fatal("no metadata.json files on disk — rig is broken")
	}
	for _, name := range metas {
		raw, err := os.ReadFile(filepath.Join(r.metaDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		md, err := icetable.ParseMetadataBytes(raw)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, snap := range md.Snapshots() {
			if snap.ManifestList == "" {
				continue
			}
			manifests, err := snap.Manifests(fio)
			if err != nil {
				t.Fatalf("%s: snapshot %d manifest list is unreadable after the sweep: %v",
					name, snap.SnapshotID, err)
			}
			for _, m := range manifests {
				p := strings.TrimPrefix(m.FilePath(), "file://")
				if !exists(t, p) {
					t.Fatalf("%s: snapshot %d references manifest %s, deleted by the sweep",
						name, snap.SnapshotID, filepath.Base(p))
				}
			}
		}
	}
}

// livePaths is the data-file set an external engine sees in the current snapshot.
func (r *sweepRig) livePaths(t *testing.T, ctx context.Context) []string {
	t.Helper()
	tbl := r.load(t, ctx)
	if tbl.CurrentSnapshot() == nil {
		return nil
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatalf("PlanFiles after sweep: %v", err)
	}
	out := make([]string, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, filepath.Base(task.File.FilePath()))
	}
	return out
}

// churn runs enough add/remove passes to leave real orphans: expired snapshots' manifest lists
// and manifests. Returns the files that remain registered.
func (r *sweepRig) churn(t *testing.T, ctx context.Context) []string {
	t.Helper()
	dataDir := filepath.Join(r.root, "mydb", "cpu", "2026", "07", "14", "15")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	base := int64(1_752_500_000_000_000)
	var all []string
	for i := 0; i < 5; i++ {
		p := filepath.Join(dataDir, string(rune('a'+i))+".parquet")
		writeArcStyleParquet(t, p, base+int64(i)*3_600_000_000, 4)
		all = append(all, p)
	}
	// Grow, then shrink, then grow again: every pass commits, and the expiry at retain leaves the
	// superseded manifest lists and manifests behind.
	r.pass(t, ctx, all[0])
	r.pass(t, ctx, all[0], all[1])
	r.pass(t, ctx, all[0], all[1], all[2])
	r.pass(t, ctx, all[1], all[2])         // drops a[0]
	r.pass(t, ctx, all[1], all[2], all[3]) // adds d
	r.pass(t, ctx, all[2], all[3], all[4]) // drops b, adds e
	return []string{all[2], all[3], all[4]}
}

// TestSweepOrphanMetadata_ReclaimsExpiredSnapshotManifests is the #835 contract: the .avro files
// of expired snapshots are deleted once they are past the grace, and everything any on-disk
// metadata.json can still reach survives.
func TestSweepOrphanMetadata_ReclaimsExpiredSnapshotManifests(t *testing.T) {
	ctx := context.Background()
	r := newSweepRig(t, 1)
	live := r.churn(t, ctx)

	before := r.names(t, ".avro")
	if len(before) < 4 {
		t.Fatalf("rig produced only %d avro files (%v) — not enough history to orphan anything", len(before), before)
	}

	// Nothing is past the grace yet, so a pass now must delete nothing.
	r.pass(t, ctx, live...)
	if got := len(r.names(t, ".avro")); got != len(before) {
		t.Errorf("sweep deleted %d files that were within the grace window", len(before)-got)
	}

	r.age(t, 3*time.Hour)
	r.pass(t, ctx, live...) // converged pass: reaches the sweep via the early-return path

	after := r.names(t, ".avro")
	if len(after) >= len(before) {
		t.Errorf("sweep reclaimed nothing: %d avro before, %d after", len(before), len(after))
	}
	r.assertEveryOnDiskMetadataStillResolves(t, ctx)

	// The live data-file set is unchanged, so an external engine reads the same table.
	got := r.livePaths(t, ctx)
	if len(got) != len(live) {
		t.Errorf("live data files after sweep = %v, want %d files", got, len(live))
	}
	t.Logf("reclaimed %d of %d avro files (%d remain reachable)", len(before)-len(after), len(before), len(after))
}

// TestSweepOrphanMetadata_KeepsWhatOnlyAnOlderMetadataCopyReaches pins the reason reachability is
// computed from every metadata.json on disk rather than from the current snapshot: Arc keeps
// retain+1 v<N>.metadata.json copies and iceberg-go keeps its NNNNN-* log, and a directory reader
// can resolve any of them. A current-snapshot-only sweep deletes manifests those files name.
func TestSweepOrphanMetadata_KeepsWhatOnlyAnOlderMetadataCopyReaches(t *testing.T) {
	ctx := context.Background()
	r := newSweepRig(t, 1)
	live := r.churn(t, ctx)

	// Manifests reachable from the CURRENT snapshot only.
	tbl := r.load(t, ctx)
	fio, err := tbl.FS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	current := map[string]struct{}{}
	if snap := tbl.CurrentSnapshot(); snap != nil {
		current[filepath.Base(snap.ManifestList)] = struct{}{}
		manifests, err := snap.Manifests(fio)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range manifests {
			current[filepath.Base(m.FilePath())] = struct{}{}
		}
	}

	// Manifests reachable from everything on disk.
	all, err := r.exp.reachableManifestNames(ctx, tbl, r.metaKeys(t))
	if err != nil {
		t.Fatalf("reachableManifestNames: %v", err)
	}
	var onlyOlder []string
	for name := range all {
		if _, inCurrent := current[name]; !inCurrent {
			onlyOlder = append(onlyOlder, name)
		}
	}
	if len(onlyOlder) == 0 {
		t.Fatal("rig produced no manifest reachable only from an older metadata version — the test would prove nothing")
	}

	// Positive control, so the survival assertions below cannot pass against a dead sweep.
	mustGo := r.plantOrphan(t, "00000000-0000-0000-0000-0000000000ba-m3.avro", 3*time.Hour)

	r.age(t, 3*time.Hour)
	r.pass(t, ctx, live...)

	for _, name := range onlyOlder {
		if !exists(t, filepath.Join(r.metaDir, name)) {
			t.Errorf("sweep deleted %s, which an on-disk older metadata version still references", name)
		}
	}
	if exists(t, mustGo) {
		t.Error("the sweep did not run at all — the survival assertions above prove nothing")
	}
}

// metaKeys returns the backend keys of the metadata.json files, as sweepOrphanMetadata collects
// them.
func (r *sweepRig) metaKeys(t *testing.T) []string {
	t.Helper()
	rel, err := filepath.Rel(r.root, r.metaDir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, n := range r.names(t, ".metadata.json") {
		out = append(out, filepath.ToSlash(filepath.Join(rel, n)))
	}
	return out
}

// TestSweepOrphanMetadata_Grace is the grace window: an unreachable manifest younger than the
// grace survives, and the same file past the grace is deleted.
func TestSweepOrphanMetadata_Grace(t *testing.T) {
	ctx := context.Background()
	r := newSweepRig(t, 1)
	live := r.churn(t, ctx)

	young := r.plantOrphan(t, "00000000-0000-0000-0000-00000000dead-m9.avro", time.Minute)
	r.pass(t, ctx, live...)
	if !exists(t, young) {
		t.Error("sweep deleted an unreachable manifest that was one minute old")
	}

	when := time.Now().Add(-3 * time.Hour)
	if err := os.Chtimes(young, when, when); err != nil {
		t.Fatal(err)
	}
	r.pass(t, ctx, live...)
	if exists(t, young) {
		t.Error("sweep kept an unreachable manifest that was three hours old")
	}
}

// TestSweepOrphanMetadata_FailsClosed: an incomplete reachable set must never authorise a delete.
func TestSweepOrphanMetadata_FailsClosed(t *testing.T) {
	t.Run("unparsable metadata.json", func(t *testing.T) {
		ctx := context.Background()
		r := newSweepRig(t, 1)
		live := r.churn(t, ctx)
		orphan := r.plantOrphan(t, "00000000-0000-0000-0000-0000000000ff-m8.avro", 3*time.Hour)
		if err := os.WriteFile(filepath.Join(r.metaDir, "99999-bogus.metadata.json"), []byte("{"), 0o600); err != nil {
			t.Fatal(err)
		}
		r.age(t, 3*time.Hour)
		r.pass(t, ctx, live...)
		if !exists(t, orphan) {
			t.Error("sweep deleted while one metadata.json could not be parsed")
		}

		// Inverted control: remove the unparsable file and the same orphan goes.
		if err := os.Remove(filepath.Join(r.metaDir, "99999-bogus.metadata.json")); err != nil {
			t.Fatal(err)
		}
		r.age(t, 3*time.Hour)
		r.pass(t, ctx, live...)
		if exists(t, orphan) {
			t.Error("the orphan survived even with reachability intact — the fail-closed case proves nothing")
		}
	})

	t.Run("missing manifest list", func(t *testing.T) {
		ctx := context.Background()
		r := newSweepRig(t, 1)
		live := r.churn(t, ctx)
		orphan := r.plantOrphan(t, "00000000-0000-0000-0000-0000000000fe-m7.avro", 3*time.Hour)
		// Remove a manifest list an on-disk metadata version still names, so reachability can no
		// longer be established and nothing may be deleted. It has to be a REACHABLE one: snapshot
		// ids are random, so "the first snap-*.avro on disk" is sometimes an already-expired
		// snapshot's list, whose absence changes nothing and leaves the sweep free to run.
		var target string
		for _, key := range r.metaKeys(t) {
			raw, err := os.ReadFile(filepath.Join(r.root, filepath.FromSlash(key)))
			if err != nil {
				t.Fatal(err)
			}
			md, err := icetable.ParseMetadataBytes(raw)
			if err != nil {
				t.Fatal(err)
			}
			for _, snap := range md.Snapshots() {
				if snap.ManifestList != "" {
					target = filepath.Base(snap.ManifestList)
					break
				}
			}
			if target != "" {
				break
			}
		}
		if target == "" {
			t.Fatal("no referenced manifest list to remove — rig is broken")
		}
		saved, err := os.ReadFile(filepath.Join(r.metaDir, target))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(r.metaDir, target)); err != nil {
			t.Fatal(err)
		}
		r.age(t, 3*time.Hour)
		r.pass(t, ctx, live...)
		if !exists(t, orphan) {
			t.Error("sweep deleted while a referenced manifest list was missing")
		}

		// Inverted control: put the manifest list back and the same orphan goes.
		if err := os.WriteFile(filepath.Join(r.metaDir, target), saved, 0o600); err != nil {
			t.Fatal(err)
		}
		r.age(t, 3*time.Hour)
		r.pass(t, ctx, live...)
		if exists(t, orphan) {
			t.Error("the orphan survived even with reachability intact — the fail-closed case proves nothing")
		}
	})
}

// TestSweepOrphanMetadata_TouchesOnlyAvroInTheMetadataDir: the blast radius. Nothing but .avro
// files directly in the table's metadata directory may be deleted, however old and however
// unreferenced.
func TestSweepOrphanMetadata_TouchesOnlyAvroInTheMetadataDir(t *testing.T) {
	ctx := context.Background()
	r := newSweepRig(t, 1)
	live := r.churn(t, ctx)

	keep := []string{
		r.plantOrphan(t, "stray.parquet", 3*time.Hour),
		r.plantOrphan(t, "notes.txt", 3*time.Hour),
		r.plantOrphan(t, "orphan.avro.bak", 3*time.Hour),
		r.plantOrphan(t, filepath.Join("nested", "00000000-0000-0000-0000-0000000000aa-m1.avro"), 3*time.Hour),
	}
	// Positive control, so this test cannot pass merely because the sweep did nothing: an aged,
	// unreachable .avro directly in the directory must be gone by the end.
	mustGo := r.plantOrphan(t, "00000000-0000-0000-0000-0000000000bb-m2.avro", 3*time.Hour)
	hint := filepath.Join(r.metaDir, "version-hint.text")
	if !exists(t, hint) {
		t.Fatal("no version-hint.text — rig is broken")
	}
	metasBefore := len(r.names(t, ".metadata.json"))

	r.age(t, 3*time.Hour)
	r.pass(t, ctx, live...)

	for _, p := range keep {
		if !exists(t, p) {
			t.Errorf("sweep deleted %s, which is not an .avro directly in the metadata directory", p)
		}
	}
	if !exists(t, hint) {
		t.Error("sweep deleted version-hint.text")
	}
	if got := len(r.names(t, ".metadata.json")); got != metasBefore {
		t.Errorf("metadata.json count changed across the sweep: %d -> %d", metasBefore, got)
	}
	if exists(t, mustGo) {
		t.Error("the sweep did not run at all — the exclusions above prove nothing")
	}
}

// noListerBackend hides ListObjects: embedding the INTERFACE promotes only Backend's own methods,
// so a type assertion to storage.ObjectLister fails.
type noListerBackend struct{ storage.Backend }

// TestSweepOrphanMetadata_RequiresObjectLister: without file ages there is no way to tell an
// abandoned manifest from one a commit just wrote, so the sweep must not run at all.
func TestSweepOrphanMetadata_RequiresObjectLister(t *testing.T) {
	ctx := context.Background()
	r := newSweepRig(t, 1)
	live := r.churn(t, ctx)
	orphan := r.plantOrphan(t, "00000000-0000-0000-0000-0000000000ab-m6.avro", 3*time.Hour)

	r.exp.backend = noListerBackend{r.backend}
	r.age(t, 3*time.Hour)
	r.pass(t, ctx, live...)

	if !exists(t, orphan) {
		t.Error("sweep ran against a backend that cannot report file ages")
	}

	// Inverted control: with the lister back, the same orphan goes.
	r.exp.backend = r.backend
	r.pass(t, ctx, live...)
	if exists(t, orphan) {
		t.Error("restoring the lister did not reclaim the orphan — the no-lister case proves nothing")
	}
}

// TestSweepOrphanMetadata_Disabled: iceberg.orphan_sweep_enabled=false restores the pre-#835
// behaviour exactly.
func TestSweepOrphanMetadata_Disabled(t *testing.T) {
	ctx := context.Background()
	r := newSweepRig(t, 1)
	live := r.churn(t, ctx)
	orphan := r.plantOrphan(t, "00000000-0000-0000-0000-0000000000ac-m5.avro", 3*time.Hour)

	r.exp.ConfigureOrphanSweep(false, time.Hour)
	before := len(r.names(t, ".avro"))
	r.age(t, 3*time.Hour)
	r.pass(t, ctx, live...)

	if !exists(t, orphan) {
		t.Error("sweep ran while disabled")
	}
	if got := len(r.names(t, ".avro")); got != before {
		t.Errorf("avro count changed while the sweep was disabled: %d -> %d", before, got)
	}

	// Inverted control: re-enable and the same orphan goes. Without this the assertions above hold
	// just as well against a sweep that does nothing at all.
	r.exp.ConfigureOrphanSweep(true, time.Hour)
	r.pass(t, ctx, live...)
	if exists(t, orphan) {
		t.Error("re-enabling the sweep did not reclaim the orphan — the disabled case proves nothing")
	}
}

// TestSweepOrphanMetadata_AtDefaultRetain runs the whole thing at the DEFAULT retain_snapshots (10)
// rather than only at the most favourable setting, and pins what actually differs there: nothing is
// expired yet (the rig commits fewer than 10 snapshots, so expireSnapshots returns early), every
// manifest the retained versions name stays, and an unreachable file still goes. The O(retain)
// bound itself is what TestSweepOrphanMetadata_ReclaimsExpiredSnapshotManifests measures at
// retain=1, where expiry does run.
func TestSweepOrphanMetadata_AtDefaultRetain(t *testing.T) {
	ctx := context.Background()
	r := newSweepRig(t, 10)
	live := r.churn(t, ctx)
	orphan := r.plantOrphan(t, "00000000-0000-0000-0000-0000000000ad-m4.avro", 3*time.Hour)

	r.age(t, 3*time.Hour)
	r.pass(t, ctx, live...)

	// The planted file is reachable from nothing, so it goes even at retain=10.
	if exists(t, orphan) {
		t.Error("sweep kept an unreachable manifest at retain=10")
	}
	// Everything the retained metadata versions name is still there.
	r.assertEveryOnDiskMetadataStillResolves(t, ctx)
	if got := len(r.names(t, ".avro")); got == 0 {
		t.Error("retain=10 should leave the retained versions manifests in place")
	}
}

// TestSweepOrphanMetadata_NonDefaultWarehouse is the #534 shape: iceberg.warehouse pointed at a
// SUBDIRECTORY of the storage root, where "trim the warehouse" and "trim the storage root" stop
// being the same string. It also plants a decoy in a SIBLING directory whose name shares the
// warehouse as a prefix (wh vs wh-other), the mid-segment match that broke #534's first fix.
func TestSweepOrphanMetadata_NonDefaultWarehouse(t *testing.T) {
	ctx := context.Background()
	r := newSweepRigAt(t, 1, "wh")
	live := r.churn(t, ctx)

	if !strings.Contains(r.metaDir, filepath.Join("wh", "arc_mydb.db")) {
		t.Fatalf("rig did not put the warehouse in the subdirectory: %s", r.metaDir)
	}
	orphan := r.plantOrphan(t, "00000000-0000-0000-0000-0000000000ca-m3.avro", 3*time.Hour)

	// A sibling directory that shares "wh" as a string prefix, laid out like a warehouse. Nothing
	// in it is ours, and a prefix match rather than a path-boundary match would reach it.
	decoyDir := filepath.Join(r.root, "wh-other", "arc_mydb.db", "cpu", "metadata")
	if err := os.MkdirAll(decoyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	decoy := filepath.Join(decoyDir, "00000000-0000-0000-0000-0000000000cb-m3.avro")
	if err := os.WriteFile(decoy, []byte("not ours"), 0o600); err != nil {
		t.Fatal(err)
	}
	when := time.Now().Add(-3 * time.Hour)
	if err := os.Chtimes(decoy, when, when); err != nil {
		t.Fatal(err)
	}

	r.age(t, 3*time.Hour)
	r.pass(t, ctx, live...)

	if exists(t, orphan) {
		t.Error("sweep did not run with the warehouse in a subdirectory of the storage root")
	}
	if !exists(t, decoy) {
		t.Error("sweep reached into wh-other — the warehouse match is a prefix match, not a boundary match")
	}
	r.assertEveryOnDiskMetadataStillResolves(t, ctx)
}

func TestOrphanGraceFor(t *testing.T) {
	cases := []struct {
		interval time.Duration
		want     time.Duration
	}{
		{0, minOrphanGrace},
		{300 * time.Second, minOrphanGrace},  // the default interval: floor wins
		{30 * time.Minute, minOrphanGrace},   // exactly 2x the floor is not more than it
		{45 * time.Minute, 90 * time.Minute}, // a stretched interval widens the grace
		{-5 * time.Second, minOrphanGrace},   // nonsense input cannot shrink the grace
	}
	for _, c := range cases {
		if got := OrphanGraceFor(c.interval); got != c.want {
			t.Errorf("OrphanGraceFor(%s) = %s, want %s", c.interval, got, c.want)
		}
	}
}

func TestConfigureOrphanSweep_FloorsTheGrace(t *testing.T) {
	r := newSweepRig(t, 1)
	for _, grace := range []time.Duration{0, -time.Hour, time.Second} {
		r.exp.ConfigureOrphanSweep(true, grace)
		if r.exp.orphanGrace != minOrphanGrace {
			t.Errorf("ConfigureOrphanSweep(true, %s) left grace at %s, want the floor %s",
				grace, r.exp.orphanGrace, minOrphanGrace)
		}
	}
	r.exp.ConfigureOrphanSweep(true, 5*time.Hour)
	if r.exp.orphanGrace != 5*time.Hour {
		t.Errorf("a grace above the floor was not honoured: %s", r.exp.orphanGrace)
	}
}
