package backup

// Tests for #1084: a backup scoped to one or more databases.

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// scopeFixture is a store with two databases, an edge-sync spoke holding a
// copy of one of them, the field schema anchors and compaction recovery
// state of all three, and an Iceberg namespace directory for one database.
var scopeFixture = map[string]string{
	"a/cpu/2026/01/01/00/a1.parquet":           "A1",
	"a/cpu/2026/01/01/01/a2.parquet":           "A2",
	"b/cpu/2026/01/01/00/b1.parquet":           "B1",
	"_schema/a/cpu.parquet":                    "ANCHOR-A",
	"_schema/b/cpu.parquet":                    "ANCHOR-B",
	"_schema/spoke1/a/cpu.parquet":             "ANCHOR-SPOKE1-A",
	"_compaction_state/hourly/a/j.json":        "{}",
	"_compaction_state/hourly/b/j.json":        "{}",
	"_compaction_state/hourly/spoke1/a/j.json": "{}",
	"spoke1/a/cpu/2026/01/01/00/s1.parquet":    "S1",
	"arc_a.db/cpu/metadata/v1.metadata.json":   "ICE",
}

func seedScopeFixture(t *testing.T, b storage.Backend) {
	t.Helper()
	for k, v := range scopeFixture {
		mustWrite(t, b, k, []byte(v))
	}
}

// backupDataKeys lists the keys a backup holds under data/, sorted.
func backupDataKeys(t *testing.T, backupDir, backupID string) []string {
	t.Helper()
	bk := mustLocalBackend(t, backupDir, zerolog.Nop())
	keys, err := bk.List(context.Background(), backupID+"/data/")
	if err != nil {
		t.Fatalf("list backup: %v", err)
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, strings.TrimPrefix(k, backupID+"/data/"))
	}
	sort.Strings(out)
	return out
}

func readManifestJSON(t *testing.T, backupDir, backupID string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(backupDir, backupID, "manifest.json"))
	if err != nil {
		t.Fatalf("manifest.json: %v", err)
	}
	return string(raw)
}

func newScopedTestManager(t *testing.T, data storage.Backend, backupDir string) *Manager {
	t.Helper()
	m, err := NewManager(&ManagerConfig{DataStorage: data, BackupPath: backupDir, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// A backup scoped to one database holds exactly that database's data files,
// its schema anchors and its compaction recovery state: nothing from the
// other database, nothing from the spoke that carries a copy of it (the
// spoke's anchors are keyed by the spoke), and nothing from the Iceberg
// namespace directory. The manifest records the scope, keeps backup_type
// "full", and inventories only the scoped database; a restore of the backup
// brings back only that database.
func TestBackup_ScopedToOneDatabaseCopiesOnlyItsFiles(t *testing.T) {
	ctx := context.Background()
	data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	seedScopeFixture(t, data)
	backupDir := t.TempDir()
	m := newScopedTestManager(t, data, backupDir)

	res, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{"a"}})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	mf := res.Manifest
	want := []string{
		"_compaction_state/hourly/a/j.json",
		"_schema/a/cpu.parquet",
		"a/cpu/2026/01/01/00/a1.parquet",
		"a/cpu/2026/01/01/01/a2.parquet",
	}
	if got := backupDataKeys(t, backupDir, mf.BackupID); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("backup holds %v, want %v", got, want)
	}
	if strings.Join(mf.Scope, ",") != "a" || mf.BackupType != "full" {
		t.Errorf("scope=%v backup_type=%q, want [a] full", mf.Scope, mf.BackupType)
	}
	if len(mf.Databases) != 1 || mf.Databases[0].Name != "a" || mf.Databases[0].FileCount != 2 {
		t.Errorf("databases = %+v, want only a with 2 files", mf.Databases)
	}
	if mf.TotalFiles != 3 || mf.AuxiliaryFiles != 1 || mf.CompactionStateFiles != 1 || mf.HasMetadata || mf.IcebergNamespaceFilesExcluded != 0 {
		t.Errorf("total=%d aux=%d state=%d metadata=%v iceberg_excluded=%d", mf.TotalFiles, mf.AuxiliaryFiles, mf.CompactionStateFiles, mf.HasMetadata, mf.IcebergNamespaceFilesExcluded)
	}
	if p := m.GetProgress(); p.Status != "completed" || strings.Join(p.Scope, ",") != "a" {
		t.Errorf("progress status=%s scope=%v", p.Status, p.Scope)
	}
	js := readManifestJSON(t, backupDir, mf.BackupID)
	if !strings.Contains(js, `"scope": [`) || !strings.Contains(js, `"backup_type": "full"`) {
		t.Errorf("manifest JSON lacks the scope or changed backup_type:\n%s", js)
	}

	// Restoring it touches only database a.
	dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	m2 := &Manager{dataStorage: dest, backupStorage: m.backupStorage, logger: zerolog.Nop()}
	if _, err := m2.RestoreBackup(ctx, RestoreOptions{BackupID: mf.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	for _, k := range want {
		if !existsIn(t, dest, k) {
			t.Errorf("%s not restored", k)
		}
	}
	for _, k := range []string{"b/cpu/2026/01/01/00/b1.parquet", "_schema/b/cpu.parquet", "spoke1/a/cpu/2026/01/01/00/s1.parquet", "_schema/spoke1/a/cpu.parquet", "arc_a.db/cpu/metadata/v1.metadata.json"} {
		if existsIn(t, dest, k) {
			t.Errorf("%s restored by a backup scoped to a", k)
		}
	}
	if p := m2.GetProgress(); p.Status != "completed" || p.Mode != RestoreModeMerge {
		t.Errorf("standalone restore status=%s mode=%s, want completed merge", p.Status, p.Mode)
	}
}

// Scope by storage-root segment: ["spoke1"] takes the spoke's data, its
// anchors (_schema/spoke1/...) and its compaction state
// (_compaction_state/<tier>/spoke1/<db>/..., the prefix rule), and nothing of
// the database it mirrors.
func TestBackup_ScopedToASpokeTakesItsAnchorsAndState(t *testing.T) {
	ctx := context.Background()
	data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	seedScopeFixture(t, data)
	backupDir := t.TempDir()
	m := newScopedTestManager(t, data, backupDir)

	res, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{"spoke1"}})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	want := []string{
		"_compaction_state/hourly/spoke1/a/j.json",
		"_schema/spoke1/a/cpu.parquet",
		"spoke1/a/cpu/2026/01/01/00/s1.parquet",
	}
	if got := backupDataKeys(t, backupDir, res.Manifest.BackupID); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("backup holds %v, want %v", got, want)
	}
	if mf := res.Manifest; len(mf.Databases) != 1 || mf.Databases[0].Name != "spoke1" || strings.Join(mf.Scope, ",") != "spoke1" || mf.AuxiliaryFiles != 1 || mf.CompactionStateFiles != 1 {
		t.Errorf("manifest = scope %v databases %+v aux %d state %d", mf.Scope, mf.Databases, mf.AuxiliaryFiles, mf.CompactionStateFiles)
	}
}

// The ownership rules, one table: the second segment of an anchor must equal
// the name exactly, the database part of a compaction manifest may carry the
// name as a boundary prefix (a spoke), and the name rules refuse what cannot
// be a database.
func TestScopeOwnershipRules(t *testing.T) {
	sc, err := newScope([]string{"b", "a", "a", "spoke1"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(sc.names, ",") != "a,b,spoke1" {
		t.Errorf("names = %v, want sorted and de-duplicated", sc.names)
	}
	for p, want := range map[string]bool{
		"_schema/a/cpu.parquet":             true,
		"_schema/spoke1/a/cpu.parquet":      true, // the spoke is in scope
		"_schema/ab/cpu.parquet":            false,
		"_schema/c/cpu.parquet":             false,
		"_schema/x/a/cpu.parquet":           false, // a is scoped, x is not: the anchor is x's
		"a/cpu/2026/01/01/00/x.parquet":     false, // not an anchor
		"_compaction_state/hourly/a/j.json": false,
	} {
		if got := sc.ownsSchemaAnchor(p); got != want {
			t.Errorf("ownsSchemaAnchor(%q) = %v, want %v", p, got, want)
		}
	}
	for p, want := range map[string]bool{
		"_compaction_state/hourly/a/j.json":            true,
		"_compaction_state/daily/a/j.json.quarantined": true,
		"_compaction_state/hourly/spoke1/a/j.json":     true, // spoke pseudo-database "spoke1/a"
		"_compaction_state/hourly/spoke1/zzz/j.json":   true, // any database of the spoke
		"_compaction_state/hourly/ab/j.json":           false,
		"_compaction_state/hourly/spoke10/a/j.json":    false, // boundary, not substring
		"_compaction_state/hourly/x/a/j.json":          false, // a is scoped, but this is spoke x's
		"_compaction_state/hourly/c/j.json":            false,
		"_compaction_state/a/j.json":                   false, // no tier segment
		"_schema/a/cpu.parquet":                        false,
	} {
		if got := sc.ownsCompactionState(p); got != want {
			t.Errorf("ownsCompactionState(%q) = %v, want %v", p, got, want)
		}
	}
	for p, want := range map[string]bool{
		"a/cpu/2026/01/01/00/x.parquet":            true,
		"spoke1/a/cpu/2026/01/01/00/x.parquet":     true,
		"ab/cpu/2026/01/01/00/x.parquet":           false,
		"c/cpu/2026/01/01/00/x.parquet":            false,
		"_schema/a/cpu.parquet":                    true,
		"_schema/c/cpu.parquet":                    false,
		"_compaction_state/hourly/spoke1/a/j.json": true,
		"_compaction_state/hourly/c/j.json":        false,
		"arc_a.db/cpu/metadata/v1.metadata.json":   false,
	} {
		if got := sc.ownsPath(p); got != want {
			t.Errorf("ownsPath(%q) = %v, want %v", p, got, want)
		}
	}
	if !sc.ownsIcebergNamespaceDir("arc_a.db", "arc") || sc.ownsIcebergNamespaceDir("arc_c.db", "arc") || sc.ownsIcebergNamespaceDir("arc_a.db", "other") {
		t.Error("ownsIcebergNamespaceDir is not keyed on <prefix>_<db>.db")
	}
	// An empty scope owns everything: the unscoped call sites read as before.
	var none *scope
	for _, p := range []string{"_schema/c/cpu.parquet", "_compaction_state/hourly/c/j.json", "c/cpu/x.parquet"} {
		if !none.ownsPath(p) || !none.ownsData(p) || !none.ownsSchemaAnchor(p) || !none.ownsCompactionState(p) || !none.empty() {
			t.Errorf("an empty scope must own %q", p)
		}
	}
	for _, bad := range [][]string{{""}, {"a/b"}, {`a\b`}, {"a\x00b"}, {".."}, {"_schema"}, {"_compaction_state"}, {".hidden"}, {strings.Repeat("n", storage.MaxUsableKeySegmentLen+1)}} {
		if _, err := newScope(bad); err == nil {
			t.Errorf("newScope(%q) accepted", bad)
		}
	}
	many := make([]string, maxScopeDatabases+1)
	for i := range many {
		many[i] = "d" + strings.Repeat("x", 3) + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('a'+(i/676)%26))
	}
	if _, err := newScope(many); err == nil || !strings.Contains(err.Error(), "at most") {
		t.Errorf("newScope of %d names: err = %v, want the cap", len(many), err)
	}
	if sc, err := newScope(nil); err != nil || !sc.empty() || sc.names != nil {
		t.Errorf("newScope(nil) = %+v, %v; want an empty scope with nil names", sc, err)
	}
}

// probeCountingBackend counts the bounded probes and the listings the manager
// makes, so a test can assert which rule answered and that nothing listed.
type probeCountingBackend struct {
	*storage.LocalBackend
	probes atomic.Int32
	lists  atomic.Int32
}

func (b *probeCountingBackend) HasObjectsUnderPrefix(ctx context.Context, prefix string) (bool, error) {
	b.probes.Add(1)
	return b.LocalBackend.HasObjectsUnderPrefix(ctx, prefix)
}

func (b *probeCountingBackend) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	b.lists.Add(1)
	return b.LocalBackend.ListObjects(ctx, prefix)
}

// listOnlyBackend hides the prober: the storage.Backend interface is embedded,
// so only the Backend methods are promoted, the way test fakes elsewhere wrap
// a backend.
type listOnlyBackend struct {
	storage.Backend
	lists atomic.Int32
}

func (b *listOnlyBackend) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	b.lists.Add(1)
	return b.Backend.(storage.ObjectLister).ListObjects(ctx, prefix)
}

type fakeTierLookup struct {
	rows  map[string]bool
	err   error
	calls atomic.Int32
}

func (f *fakeTierLookup) DatabaseHasTierRows(_ context.Context, database string) (bool, error) {
	f.calls.Add(1)
	if f.err != nil {
		return false, f.err
	}
	return f.rows[database], nil
}

// The known-database check asks the tier metadata first (one indexed query,
// zero storage probes when it says yes), then probes the hot prefix, then the
// anchors, and never lists; an unknown name is reported by name, and a lookup
// error fails the check rather than guessing.
func TestCheckDatabasesKnown_RulesInOrderWithoutListing(t *testing.T) {
	ctx := context.Background()
	local := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	mustWrite(t, local, "hot/cpu/2026/01/01/00/x.parquet", []byte("X"))
	mustWrite(t, local, "_schema/anchored/cpu.parquet", []byte("ANCHOR"))
	data := &probeCountingBackend{LocalBackend: local}
	m := newScopedTestManager(t, data, t.TempDir())
	lookup := &fakeTierLookup{rows: map[string]bool{"cold": true}}
	m.SetTierLookup(lookup)

	check := func(t *testing.T, names ...string) (error, int32, int32) {
		t.Helper()
		data.probes.Store(0)
		data.lists.Store(0)
		lookup.calls.Store(0)
		err := m.CheckDatabasesKnown(ctx, names)
		return err, data.probes.Load(), lookup.calls.Load()
	}
	if err, probes, calls := check(t, "cold"); err != nil || probes != 0 || calls != 1 {
		t.Errorf("fully cold: err=%v probes=%d lookups=%d, want nil 0 1 (the tier rule answers first)", err, probes, calls)
	}
	if err, probes, _ := check(t, "hot"); err != nil || probes != 1 {
		t.Errorf("hot files: err=%v probes=%d, want nil 1", err, probes)
	}
	if err, probes, _ := check(t, "anchored"); err != nil || probes != 2 {
		t.Errorf("anchors only: err=%v probes=%d, want nil 2", err, probes)
	}
	err, probes, _ := check(t, "hot", "nope", "zzz")
	var unknown *UnknownDatabasesError
	if !errors.As(err, &unknown) || strings.Join(unknown.Names, ",") != "nope,zzz" {
		t.Fatalf("unknown names: err=%v, want UnknownDatabasesError naming nope and zzz", err)
	}
	if want := `unknown database "nope": no data files, no schema anchors and no tier rows`; !strings.Contains(err.Error(), want) {
		t.Errorf("error %q lacks %q", err, want)
	}
	if probes != 5 {
		t.Errorf("probes = %d, want 5 (1 for hot, 2 each for the unknown pair)", probes)
	}
	if data.lists.Load() != 0 {
		t.Errorf("CheckDatabasesKnown listed %d times; it must only probe", data.lists.Load())
	}
	// A database whose only file has a key no listing returns exists: the
	// hidden set under its prefix is consulted, after the bounded rules.
	writeRaw(t, local.GetBasePath(), "hiddenonly/cpu/2026/01/01/00/ba\\d.parquet", "PAR1")
	if err, probes, _ := check(t, "hiddenonly"); err != nil || probes != 2 {
		t.Errorf("hidden keys only: err=%v probes=%d, want known after both probes said no", err, probes)
	}

	lookup.err = errors.New("sqlite is locked")
	err, probes, _ = check(t, "hot")
	if err == nil || !strings.Contains(err.Error(), "sqlite is locked") || errors.As(err, &unknown) || probes != 0 {
		t.Errorf("lookup error: err=%v probes=%d, want the error text, not UnknownDatabasesError, and no probe", err, probes)
	}
	lookup.err = nil

	// Without tiering wired the storage rules decide alone.
	m2 := newScopedTestManager(t, data, t.TempDir())
	if err := m2.CheckDatabasesKnown(ctx, []string{"cold"}); !errors.As(err, &unknown) {
		t.Errorf("no tier lookup: a database with tier rows only must be unknown, got %v", err)
	}
	if err := m2.CheckDatabasesKnown(ctx, []string{"hot", "anchored"}); err != nil {
		t.Errorf("no tier lookup: hot and anchored must be known, got %v", err)
	}
	// Invalid names are refused before any probe.
	if err := m2.CheckDatabasesKnown(ctx, []string{"_schema"}); err == nil || !strings.Contains(err.Error(), "reserved storage root") {
		t.Errorf("reserved root: err = %v", err)
	}
}

// A backend without storage.PrefixProber gets the same answer through
// ListObjects.
func TestCheckDatabasesKnown_FallsBackToListObjectsWithoutAProber(t *testing.T) {
	ctx := context.Background()
	local := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	mustWrite(t, local, "hot/cpu/2026/01/01/00/x.parquet", []byte("X"))
	data := &listOnlyBackend{Backend: local}
	if _, ok := any(data).(storage.PrefixProber); ok {
		t.Fatal("premise: the wrapper must not implement PrefixProber")
	}
	m := newScopedTestManager(t, data, t.TempDir())
	if err := m.CheckDatabasesKnown(ctx, []string{"hot"}); err != nil {
		t.Errorf("known via the fallback: %v", err)
	}
	if data.lists.Load() != 1 {
		t.Errorf("ListObjects calls = %d, want 1 (the hot prefix answered)", data.lists.Load())
	}
	var unknown *UnknownDatabasesError
	if err := m.CheckDatabasesKnown(ctx, []string{"nope"}); !errors.As(err, &unknown) {
		t.Errorf("unknown via the fallback: err = %v", err)
	}
	if data.lists.Load() != 3 {
		t.Errorf("ListObjects calls = %d, want 3 (hot prefix, then nope/ and _schema/nope/)", data.lists.Load())
	}
}

// A fully cold database has no hot file and may have no anchor; its tier rows
// make it known, and its scoped backup completes with zero data files and the
// scope recorded. A name unknown by every rule fails the run inside the
// manager too, with the same error the API check gives, and writes no
// manifest.
func TestBackup_ScopedFullyColdDatabaseCompletesWithNoFiles(t *testing.T) {
	ctx := context.Background()
	data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	backupDir := t.TempDir()
	m := newScopedTestManager(t, data, backupDir)
	m.SetTierLookup(&fakeTierLookup{rows: map[string]bool{"cold": true}})

	res, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{"cold"}})
	if err != nil {
		t.Fatalf("CreateBackup of a fully cold database: %v", err)
	}
	mf := res.Manifest
	if mf.TotalFiles != 0 || len(mf.Databases) != 0 || strings.Join(mf.Scope, ",") != "cold" || mf.UnaddressableFiles != 0 {
		t.Errorf("manifest total=%d databases=%d scope=%v unaddressable=%d", mf.TotalFiles, len(mf.Databases), mf.Scope, mf.UnaddressableFiles)
	}
	if p := m.GetProgress(); p.Status != "completed" {
		t.Errorf("status = %s, want completed", p.Status)
	}
	if _, err := os.Stat(filepath.Join(backupDir, mf.BackupID, sidecarName)); err != nil {
		t.Errorf("sidecar missing: %v", err)
	}

	_, err = m.CreateBackup(ctx, BackupOptions{Databases: []string{"nope"}})
	var unknown *UnknownDatabasesError
	if !errors.As(err, &unknown) || strings.Join(unknown.Names, ",") != "nope" {
		t.Fatalf("unknown database: err = %v, want UnknownDatabasesError naming nope", err)
	}
	if p := m.GetProgress(); p.Status != "failed" || !strings.Contains(p.Error, `unknown database "nope"`) {
		t.Errorf("progress = %s %q", p.Status, p.Error)
	}
	if list, err := m.ListBackups(ctx); err != nil || len(list) != 1 || strings.Join(list[0].Scope, ",") != "cold" {
		t.Errorf("ListBackups = %+v, %v; want only the cold backup, with its scope in the summary", list, err)
	}
}

// Iceberg export on, warehouse under the storage root (at the root, and in a
// subdirectory of it, which is the #534 layout and a different key prefix): a
// scoped backup never lists the namespace directory, copies no metadata from
// it, carries no catalog, and says how many files it left out and where.
func TestBackup_ScopedIcebergNamespaceInRootIsExcludedAndCounted(t *testing.T) {
	ctx := context.Background()
	for _, layout := range []struct{ name, prefix string }{{"root", ""}, {"subdirectory", "wh/"}} {
		t.Run(layout.name, func(t *testing.T) {
			dataDir := t.TempDir()
			data := mustLocalBackend(t, dataDir, zerolog.Nop())
			mustWrite(t, data, "prod/sensors/2026/07/14/15/s1.parquet", []byte("PAR1"))
			mustWrite(t, data, "other/cpu/2026/07/14/15/c1.parquet", []byte("PAR1"))
			for _, k := range []string{
				layout.prefix + "arc_prod.db/sensors/metadata/v1.metadata.json",
				layout.prefix + "arc_prod.db/sensors/metadata/snap-1.avro",
				layout.prefix + "arc_other.db/cpu/metadata/v1.metadata.json",
			} {
				mustWrite(t, data, k, []byte("ICE"))
			}
			backupDir := t.TempDir()
			m, err := NewManager(&ManagerConfig{
				DataStorage: data, BackupPath: backupDir, Logger: zerolog.Nop(),
				IcebergWarehousePath: filepath.Join(dataDir, filepath.FromSlash(layout.prefix)),
			})
			if err != nil {
				t.Fatal(err)
			}
			if m.icebergWarehouse != "" || !m.icebergEnabled || m.icebergWarehouseKeyPrefix != layout.prefix {
				t.Fatalf("premise: warehouse=%q enabled=%v keyPrefix=%q, want under-root with prefix %q", m.icebergWarehouse, m.icebergEnabled, m.icebergWarehouseKeyPrefix, layout.prefix)
			}
			res, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{"prod"}})
			if err != nil {
				t.Fatalf("CreateBackup: %v", err)
			}
			mf := res.Manifest
			if got := backupDataKeys(t, backupDir, mf.BackupID); strings.Join(got, ",") != "prod/sensors/2026/07/14/15/s1.parquet" {
				t.Errorf("backup holds %v, want only the data file", got)
			}
			if mf.IcebergNamespaceFilesExcluded != 2 || strings.Join(mf.IcebergNamespacesExcluded, ",") != layout.prefix+"arc_prod.db" {
				t.Errorf("iceberg excluded = %d %v, want 2 [%sarc_prod.db]", mf.IcebergNamespaceFilesExcluded, mf.IcebergNamespacesExcluded, layout.prefix)
			}
			if mf.IcebergWarehouse != nil || mf.HasMetadata || mf.HasIcebergCatalog {
				t.Errorf("scoped manifest carries warehouse=%v metadata=%v catalog=%v", mf.IcebergWarehouse, mf.HasMetadata, mf.HasIcebergCatalog)
			}
			// Unscoped, the same store copies the metadata and counts no exclusion.
			full, err := m.CreateBackup(ctx, BackupOptions{})
			if err != nil {
				t.Fatalf("unscoped CreateBackup: %v", err)
			}
			if full.Manifest.IcebergNamespaceFilesExcluded != 0 || full.Manifest.IcebergNamespacesExcluded != nil {
				t.Errorf("unscoped manifest reports exclusions: %d %v", full.Manifest.IcebergNamespaceFilesExcluded, full.Manifest.IcebergNamespacesExcluded)
			}
			if got := backupDataKeys(t, backupDir, full.Manifest.BackupID); len(got) != 5 {
				t.Errorf("unscoped backup holds %v, want all 5 objects", got)
			}
		})
	}
}

// Iceberg warehouse outside the storage root: a scoped backup skips the
// warehouse pass entirely (nothing under iceberg/, no iceberg_warehouse
// group), counts the scoped databases' namespace files it left out through
// the depth-1 walk, and no longer fails when the warehouse cannot be read.
func TestBackup_ScopedIcebergWarehouseOutsideRootIsSkippedAndCounted(t *testing.T) {
	ctx := context.Background()
	wh := t.TempDir()
	warehouseTree(t, wh)
	backupDir := t.TempDir()
	m, data := newWarehouseManager(t, t.TempDir(), backupDir, wh)
	mustWrite(t, data, "prod/sensors/2026/07/14/15/s1.parquet", []byte("PAR1"))
	if m.icebergWarehouse == "" {
		t.Fatal("premise: warehouse must be outside the root")
	}

	res, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{"prod"}})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	mf := res.Manifest
	if mf.IcebergWarehouse != nil {
		t.Errorf("scoped manifest has an iceberg_warehouse group: %+v", mf.IcebergWarehouse)
	}
	bk := mustLocalBackend(t, backupDir, zerolog.Nop())
	if keys, _ := bk.List(ctx, mf.BackupID+"/"+icebergBackupPrefix+"/"); len(keys) != 0 {
		t.Errorf("scoped backup copied the warehouse: %v", keys)
	}
	if mf.IcebergNamespaceFilesExcluded != 6 || strings.Join(mf.IcebergNamespacesExcluded, ",") != "arc_prod.db" {
		t.Errorf("iceberg excluded = %d %v, want 6 [arc_prod.db] (arc_other.db is out of scope, decoys are not metadata)", mf.IcebergNamespaceFilesExcluded, mf.IcebergNamespacesExcluded)
	}

	if os.Geteuid() == 0 {
		t.Skip("permission bits do not bind root")
	}
	if err := os.Chmod(wh, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(wh, 0o700) })
	if _, err := m.CreateBackup(ctx, BackupOptions{}); err == nil || !strings.Contains(err.Error(), "not readable") {
		t.Fatalf("premise: an unscoped backup fails on an unreadable warehouse, got %v", err)
	}
	res, err = m.CreateBackup(ctx, BackupOptions{Databases: []string{"prod"}})
	if err != nil {
		t.Fatalf("scoped backup must not fail on an unreadable warehouse it does not copy: %v", err)
	}
	if res.Manifest.IcebergNamespaceFilesExcluded != 0 {
		t.Errorf("excluded count = %d from an unreadable warehouse, want 0 (logged, not counted)", res.Manifest.IcebergNamespaceFilesExcluded)
	}
}

// On a cluster the manifest describes every database; a scoped backup
// compares only its own. An entry of another database this node lacks is not
// a manifest-only gap, and when the node pulls that file during the run the
// end-of-run re-check does not copy it either.
func TestBackup_ScopedClusterCrossCheckIgnoresOtherDatabases(t *testing.T) {
	ctx := context.Background()
	data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	const (
		a1 = "a/cpu/2026/01/01/00/a1.parquet"
		b1 = "b/cpu/2026/01/01/00/b1.parquet"
	)
	mustWrite(t, data, a1, []byte("A1"))
	cm := &fakeClusterManifest{entries: []ManifestFile{entryFor(a1, []byte("A1")), entryFor(b1, []byte("B1"))}}
	cm.onRead = func(call int) {
		if call == 2 {
			// Pulled since the listing: an unscoped backup would copy it now.
			mustWrite(t, data, b1, []byte("B1"))
		}
	}
	backupDir := t.TempDir()
	m := newClusterManager(t, data, backupDir, cm, nil)

	res, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{"a"}})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	mf := res.Manifest
	if cm.reads != 2 {
		t.Fatalf("premise: manifest read %d times, want 2 (the re-check happened)", cm.reads)
	}
	if mf.ManifestOnlyFiles != 0 || mf.UnregisteredSkipped != 0 || mf.LeftManifestDuringRun != 0 || !mf.ClusterManifestChecked {
		t.Errorf("manifest_only=%d unregistered=%d left=%d checked=%v, want 0 0 0 true", mf.ManifestOnlyFiles, mf.UnregisteredSkipped, mf.LeftManifestDuringRun, mf.ClusterManifestChecked)
	}
	if got := backupDataKeys(t, backupDir, mf.BackupID); strings.Join(got, ",") != a1 {
		t.Errorf("backup holds %v, want only %s", got, a1)
	}
	if len(mf.Databases) != 1 || mf.Databases[0].Name != "a" || mf.TotalFiles != 1 {
		t.Errorf("databases = %+v total=%d", mf.Databases, mf.TotalFiles)
	}
	sc := readSidecarFile(t, backupDir, mf.BackupID)
	if len(sc.Files) != 1 || sc.Files[0].Path != a1 || !sc.FromClusterManifest {
		t.Errorf("sidecar = %+v, want one row for %s from the manifest", sc.Files, a1)
	}
}

// Hidden keys are enumerated over the whole root; a scoped backup counts only
// the ones the scope owns, so another database's bad key neither marks this
// backup incomplete nor trips the all-unaddressable refusal. A scope whose
// only files have bad keys still fails, as the unscoped case does.
func TestBackup_ScopedUnaddressableCountsOnlyTheScope(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	writeRaw(t, dataDir, "a/cpu/2026/09/12/13/good.parquet", "PAR1-good")
	writeRaw(t, dataDir, "b/cpu/2026/09/12/13/ba\\d.parquet", "PAR1-rows")
	writeRaw(t, dataDir, "_schema/b/ba\\d.parquet", "ANCHOR")
	m := newBackupManager(t, dataDir)

	res, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{"a"}})
	if err != nil {
		t.Fatalf("CreateBackup scoped to a: %v", err)
	}
	if res.Manifest.UnaddressableFiles != 0 || len(res.Manifest.UnaddressableSample) != 0 || res.Manifest.TotalFiles != 1 {
		t.Errorf("scope a: unaddressable=%d sample=%v total=%d, want 0 [] 1", res.Manifest.UnaddressableFiles, res.Manifest.UnaddressableSample, res.Manifest.TotalFiles)
	}
	if p := m.GetProgress(); p.UnaddressableFiles != 0 {
		t.Errorf("progress unaddressable = %d, want 0", p.UnaddressableFiles)
	}

	_, err = m.CreateBackup(ctx, BackupOptions{Databases: []string{"b"}})
	if err == nil || !strings.Contains(err.Error(), "cannot be addressed") {
		t.Fatalf("scope b (only bad keys): err = %v, want the all-unaddressable refusal", err)
	}
	if !strings.Contains(err.Error(), "all 2 data file(s)") {
		t.Errorf("scope b: err = %v, want both of b's hidden keys counted (data file and anchor)", err)
	}
}

func TestResolveRestoreMode(t *testing.T) {
	scoped := []string{"audit"}
	for _, tc := range []struct {
		raw       string
		scope     []string
		clustered bool
		want      string
		wantErr   bool
	}{
		{"", scoped, true, RestoreModeReplace, false},
		{"  ", scoped, true, RestoreModeReplace, false},
		{"merge", scoped, true, RestoreModeMerge, false},
		{"MERGE", scoped, true, RestoreModeMerge, false},
		{"replace", scoped, true, RestoreModeReplace, false},
		{"", scoped, false, RestoreModeMerge, false},
		{"", nil, true, RestoreModeMerge, false},
		{"", nil, false, RestoreModeMerge, false},
		{"replace", nil, true, RestoreModeReplace, false},
		{"overwrite", scoped, true, "", true},
	} {
		got, err := ResolveRestoreMode(tc.raw, tc.scope, tc.clustered)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("ResolveRestoreMode(%q, %v, %v) = %q, %v; want %q, err=%v", tc.raw, tc.scope, tc.clustered, got, err, tc.want, tc.wantErr)
		}
	}
}

// A replace-mode restore of a scoped backup selects the current manifest
// entries by the PATH first segment, not by the entry's Database label: a
// spoke file is labelled with its canonical database. Other databases are
// untouched. The mode is left blank: on a cluster node the manager resolves a
// scoped backup to replace on its own. A scoped database the backup holds no
// data file for (fully cold, so absent from Databases) makes that replace a
// pure delete, which is refused before anything is touched.
func TestRestore_ReplaceScopedSelectsByPathFirstSegment(t *testing.T) {
	ctx := context.Background()
	const (
		s1    = "spoke1/prod/cpu/2026/01/01/00/s1.parquet"
		stale = "spoke1/prod/cpu/2026/01/01/00/stale.parquet"
		keep  = "prod/cpu/2026/01/01/00/keep.parquet"
		cold  = "cold/cpu/2026/01/01/00/current.parquet"
	)
	labelled := func(path string, data []byte) ManifestFile {
		e := entryFor(path, data)
		e.Database, e.Measurement = "prod", "cpu" // the canonical labels of a spoke file
		return e
	}
	// The source node: a spoke file, registered under the canonical database.
	src := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	mustWrite(t, src, s1, []byte("S1"))
	backupDir := t.TempDir()
	srcMgr := newClusterManager(t, src, backupDir, &fakeClusterManifest{entries: []ManifestFile{labelled(s1, []byte("S1"))}}, nil)
	srcMgr.SetTierLookup(&fakeTierLookup{rows: map[string]bool{"cold": true}})
	spokeBackup, err := srcMgr.CreateBackup(ctx, BackupOptions{Databases: []string{"spoke1"}})
	if err != nil {
		t.Fatalf("scoped backup of the spoke: %v", err)
	}
	coldBackup, err := srcMgr.CreateBackup(ctx, BackupOptions{Databases: []string{"cold"}})
	if err != nil {
		t.Fatalf("scoped backup of the cold database: %v", err)
	}
	if strings.Join(spokeBackup.Manifest.Scope, ",") != "spoke1" || len(coldBackup.Manifest.Databases) != 0 {
		t.Fatalf("premise: spoke scope=%v, cold databases=%d", spokeBackup.Manifest.Scope, len(coldBackup.Manifest.Databases))
	}

	// The target node holds a stale spoke file, the canonical database's own
	// file, the cold database's current hot file, and the spoke file itself.
	dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	for _, k := range []string{s1, stale, keep, cold} {
		mustWrite(t, dest, k, []byte("current"))
	}
	coldEntry := entryFor(cold, []byte("current"))
	cm := &fakeClusterManifest{entries: []ManifestFile{
		labelled(s1, []byte("current")), labelled(stale, []byte("current")),
		entryFor(keep, []byte("current")), coldEntry,
	}}
	m := newClusterManager(t, dest, backupDir, cm, nil)

	if _, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: spokeBackup.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("restore of the spoke backup: %v", err)
	}
	p := m.GetProgress()
	if p.Status != "completed" || p.Mode != RestoreModeReplace || p.ReplacedFiles != 1 || p.FilesRegistered != 1 {
		t.Fatalf("status=%s mode=%s replaced=%d registered=%d, want completed replace 1 1", p.Status, p.Mode, p.ReplacedFiles, p.FilesRegistered)
	}
	if len(cm.deletes) != 1 || strings.Join(cm.deletes[0], ",") != stale {
		t.Errorf("deleted = %v, want only [%s]: not the canonical database's file, not the path being rewritten", cm.deletes, stale)
	}
	if got := cm.registeredPaths(); strings.Join(got, ",") != s1 {
		t.Errorf("registered = %v, want [%s]", got, s1)
	}

	// The fully cold database: the backup holds no data file for it, so a
	// replace would only remove its current hot files and restore nothing.
	// Refused, before the compaction pause is taken and before any manifest
	// write; the current file stays. An explicit merge restores nothing and
	// completes.
	cm.deletes, cm.registers = nil, nil
	pauses := len(cm.pauseReasons)
	_, err = m.RestoreBackup(ctx, RestoreOptions{BackupID: coldBackup.Manifest.BackupID, RestoreData: true})
	if err == nil || !strings.Contains(err.Error(), "mode replace is refused") || !strings.Contains(err.Error(), `database "cold"`) {
		t.Fatalf("replace of a backup with no data files for the scope: err = %v, want the refusal naming cold", err)
	}
	p = m.GetProgress()
	if p.Status != "failed" || p.Mode != RestoreModeReplace || p.ReplacedFiles != 0 || len(cm.deletes) != 0 || len(cm.pauseReasons) != pauses || strings.Join(p.Scope, ",") != "cold" {
		t.Errorf("cold refusal: status=%s mode=%s scope=%v replaced=%d deletes=%v pauses=%d, want failed replace [cold] 0 [] %d (no pause taken)", p.Status, p.Mode, p.Scope, p.ReplacedFiles, cm.deletes, len(cm.pauseReasons), pauses)
	}
	if !existsIn(t, dest, cold) {
		t.Error("the cold database lost its current hot file to a refused replace")
	}
	if _, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: coldBackup.Manifest.BackupID, RestoreData: true, Mode: RestoreModeMerge}); err != nil {
		t.Fatalf("merge restore of the cold backup: %v", err)
	}
	if p := m.GetProgress(); p.Status != "completed" || p.Mode != RestoreModeMerge || p.ReplacedFiles != 0 || p.FilesRegistered != 0 || len(cm.deletes) != 0 {
		t.Errorf("cold merge: status=%s mode=%s replaced=%d registered=%d deletes=%v, want completed merge 0 0 []", p.Status, p.Mode, p.ReplacedFiles, p.FilesRegistered, cm.deletes)
	}

	// An explicit merge is honoured for a scoped backup.
	cm.deletes = nil
	if _, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: spokeBackup.Manifest.BackupID, RestoreData: true, Mode: RestoreModeMerge}); err != nil {
		t.Fatalf("merge restore of the spoke backup: %v", err)
	}
	if p := m.GetProgress(); p.Mode != RestoreModeMerge || p.ReplacedFiles != 0 || len(cm.deletes) != 0 {
		t.Errorf("explicit merge: mode=%s replaced=%d deletes=%v", p.Mode, p.ReplacedFiles, cm.deletes)
	}
}

// A scoped restore on a standalone node is merge; the resolution never yields
// replace there.
func TestRestore_ScopedBackupOnAStandaloneNodeMerges(t *testing.T) {
	ctx := context.Background()
	data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	seedScopeFixture(t, data)
	m := newScopedTestManager(t, data, t.TempDir())
	res, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: res.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	if p := m.GetProgress(); p.Mode != RestoreModeMerge || p.Status != "completed" || strings.Join(p.Scope, ",") != "a" {
		t.Errorf("mode=%s status=%s scope=%v, want merge completed [a]", p.Mode, p.Status, p.Scope)
	}
}

// CheckScopedReplace refuses exactly the replace that would only delete: a
// scoped backup with no data file for one of its databases. Everything else
// passes, and the message names every such database.
func TestCheckScopedReplace(t *testing.T) {
	held := &Manifest{Scope: []string{"a", "b"}, Databases: []DatabaseInfo{{Name: "a"}, {Name: "b"}}}
	oneMissing := &Manifest{Scope: []string{"a", "cold"}, Databases: []DatabaseInfo{{Name: "a"}}}
	twoMissing := &Manifest{Scope: []string{"cold", "x"}}
	unscopedEmpty := &Manifest{}
	for _, tc := range []struct {
		name     string
		mode     string
		manifest *Manifest
		want     string // "" for nil
	}{
		{"replace, every scoped database held", RestoreModeReplace, held, ""},
		{"merge, one missing", RestoreModeMerge, oneMissing, ""},
		{"replace, unscoped empty backup", RestoreModeReplace, unscopedEmpty, ""},
		{"replace, one missing", RestoreModeReplace, oneMissing, `the backup holds no data files for database "cold", so replace would only remove its current files and restore nothing; use mode merge`},
		{"replace, two missing", RestoreModeReplace, twoMissing, `for databases "cold", "x", so replace`},
	} {
		err := CheckScopedReplace(tc.mode, tc.manifest)
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: err = %v, want nil", tc.name, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want) || !strings.HasPrefix(err.Error(), "mode replace is refused: ")):
			t.Errorf("%s: err = %v, want the refusal containing %q", tc.name, err, tc.want)
		case err != nil && strings.Contains(err.Error(), "'"):
			t.Errorf("%s: error text carries an apostrophe, which the log masker truncates at: %v", tc.name, err)
		}
	}
}

// The manager refuses include_metadata on a scoped backup before any work,
// so the promise that a scoped backup never carries the SQLite database holds
// for direct callers too, not only through the API.
func TestBackup_ScopedRefusesIncludeMetadata(t *testing.T) {
	ctx := context.Background()
	data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	seedScopeFixture(t, data)
	sqlite := filepath.Join(t.TempDir(), "arc.db")
	db, err := sql.Open("sqlite3", sqlite)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	backupDir := t.TempDir()
	m, err := NewManager(&ManagerConfig{DataStorage: data, BackupPath: backupDir, SQLiteDBPath: sqlite, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}

	_, err = m.CreateBackup(ctx, BackupOptions{Databases: []string{"a"}, IncludeMetadata: true})
	if err == nil || !strings.Contains(err.Error(), "include_metadata is not available on a scoped backup") {
		t.Fatalf("scoped + IncludeMetadata: err = %v, want the refusal", err)
	}
	if p := m.GetProgress(); p.Status != "failed" {
		t.Errorf("status = %s, want failed", p.Status)
	}
	if entries, _ := os.ReadDir(backupDir); len(entries) != 0 {
		t.Errorf("a refused backup wrote %d entries into the backup directory", len(entries))
	}
	// Scoped without metadata and unscoped with it both work, and only the
	// unscoped one carries the database.
	res, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{"a"}})
	if err != nil || res.Manifest.HasMetadata {
		t.Fatalf("scoped: err=%v metadata=%v, want ok false", err, res != nil && res.Manifest.HasMetadata)
	}
	res, err = m.CreateBackup(ctx, BackupOptions{IncludeMetadata: true})
	if err != nil || !res.Manifest.HasMetadata {
		t.Fatalf("unscoped: err=%v metadata=%v, want ok true", err, res != nil && res.Manifest.HasMetadata)
	}
}

// prefixRecordingUnusableLister records the prefixes the hidden-key
// enumeration was asked for.
type prefixRecordingUnusableLister struct {
	*storage.LocalBackend
	mu       sync.Mutex
	prefixes []string
}

func (b *prefixRecordingUnusableLister) ListUnusable(ctx context.Context, prefix string) ([]storage.UnusableObject, error) {
	b.mu.Lock()
	b.prefixes = append(b.prefixes, prefix)
	b.mu.Unlock()
	return b.LocalBackend.ListUnusable(ctx, prefix)
}

func (b *prefixRecordingUnusableLister) asked() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := append([]string(nil), b.prefixes...)
	b.prefixes = nil
	return out
}

// A scoped backup enumerates hidden keys under its own prefixes (each
// database root, then the two reserved roots whole) and never under "", so a
// one-database backup does not walk the whole store for its diagnostic; the
// answer is the same as before for what the scope owns.
func TestBackup_ScopedUnaddressableEnumeratesOnlyTheScopePrefixes(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	local := mustLocalBackend(t, dataDir, zerolog.Nop())
	seedScopeFixture(t, local)
	writeRaw(t, dataDir, "a/cpu/2026/09/12/13/ba\\d.parquet", "PAR1-a-hidden")
	writeRaw(t, dataDir, "b/cpu/2026/09/12/13/ba\\d.parquet", "PAR1-b-hidden")
	data := &prefixRecordingUnusableLister{LocalBackend: local}
	m := newScopedTestManager(t, data, t.TempDir())

	res, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{"a"}})
	if err != nil {
		t.Fatalf("scoped CreateBackup: %v", err)
	}
	if got := data.asked(); strings.Join(got, ",") != "a/,_schema/,_compaction_state/" {
		t.Errorf("scoped hidden-key prefixes = %q, want [a/ _schema/ _compaction_state/] and never \"\"", got)
	}
	if res.Manifest.UnaddressableFiles != 1 || len(res.Manifest.UnaddressableSample) != 1 || !strings.HasPrefix(res.Manifest.UnaddressableSample[0], "a/") {
		t.Errorf("scoped unaddressable = %d %v, want only the hidden key under a/", res.Manifest.UnaddressableFiles, res.Manifest.UnaddressableSample)
	}

	res, err = m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("unscoped CreateBackup: %v", err)
	}
	if got := data.asked(); strings.Join(got, ",") != "" {
		t.Errorf("unscoped hidden-key prefixes = %q, want exactly [\"\"]", got)
	}
	if res.Manifest.UnaddressableFiles != 2 {
		t.Errorf("unscoped unaddressable = %d, want 2", res.Manifest.UnaddressableFiles)
	}
}

// failingPrefixLister fails ListObjects for one prefix, to stand in for a
// data-store listing outage during the Iceberg exclusion count.
type failingPrefixLister struct {
	*storage.LocalBackend
	failPrefix string
}

func (b *failingPrefixLister) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	if prefix == b.failPrefix {
		return nil, errors.New("simulated listing outage")
	}
	return b.LocalBackend.ListObjects(ctx, prefix)
}

// The in-root Iceberg exclusion count lists the data store and can fail; it
// runs with the other pre-copy decisions, so a failure leaves nothing under
// <id>/ rather than a data tree with no manifest, which ListBackups can
// neither show nor clean up.
func TestBackup_ScopedIcebergCountFailureLeavesNoPartialTree(t *testing.T) {
	ctx := context.Background()
	local := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	seedScopeFixture(t, local)
	data := &failingPrefixLister{LocalBackend: local, failPrefix: "arc_a.db/"}
	backupDir := t.TempDir()
	m := newScopedTestManager(t, data, backupDir)
	// In-root Iceberg export, set directly: the wrapper is not a
	// *storage.LocalBackend, so configureIcebergWarehouse cannot classify it.
	m.icebergEnabled, m.icebergNSPrefix = true, "arc"

	_, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{"a"}})
	if err == nil || !strings.Contains(err.Error(), "simulated listing outage") {
		t.Fatalf("CreateBackup: err = %v, want the listing outage", err)
	}
	if p := m.GetProgress(); p.Status != "failed" {
		t.Errorf("status = %s, want failed", p.Status)
	}
	entries, _ := os.ReadDir(backupDir)
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("a backup that failed before copying left %v in the backup directory; nothing should have been written", names)
	}
}

// The unscoped manifest is byte-for-byte what it was: no scope key, no
// exclusion counts, backup_type full.
func TestBackup_UnscopedManifestHasNoScopeKey(t *testing.T) {
	ctx := context.Background()
	data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	seedScopeFixture(t, data)
	backupDir := t.TempDir()
	m := newScopedTestManager(t, data, backupDir)
	res, err := m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	js := readManifestJSON(t, backupDir, res.Manifest.BackupID)
	for _, forbidden := range []string{`"scope"`, `"iceberg_namespace_files_excluded"`, `"iceberg_namespaces_excluded"`} {
		if strings.Contains(js, forbidden) {
			t.Errorf("unscoped manifest carries %s:\n%s", forbidden, js)
		}
	}
	if !strings.Contains(js, `"backup_type": "full"`) {
		t.Errorf("backup_type changed:\n%s", js)
	}
	if got := backupDataKeys(t, backupDir, res.Manifest.BackupID); len(got) != len(scopeFixture) {
		t.Errorf("unscoped backup holds %d objects, want all %d", len(got), len(scopeFixture))
	}
	if p := m.GetProgress(); p.Scope != nil {
		t.Errorf("unscoped progress has a scope: %v", p.Scope)
	}
	if list, _ := m.ListBackups(ctx); len(list) != 1 || list[0].Scope != nil {
		t.Errorf("unscoped summary has a scope: %+v", list)
	}
}
