package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// fakeColdSource is a cold tier backed by a real local backend, so the walk,
// the copy and the restore all exercise genuine listing and streaming.
type fakeColdSource struct {
	backend  storage.Backend
	rows     map[string]int64
	rowsErr  error
	recorded map[string]int64
	// quarantined paths report wrote=false, as a quarantined row does.
	quarantined map[string]bool
	recordErr   error
	records     atomic.Int32
	recordedHot map[string]int64
	hotRecords  atomic.Int32
}

func (f *fakeColdSource) ColdBackend() storage.Backend { return f.backend }

func (f *fakeColdSource) ColdRows(_ context.Context) (map[string]int64, error) {
	if f.rowsErr != nil {
		return nil, f.rowsErr
	}
	return f.rows, nil
}

// recordedHot is what the forced hot recorder wrote: the row this node had to
// move because the backup's file was cold and this node has no cold tier.
func (f *fakeColdSource) RecordRestoredHotFile(_ context.Context, path string, size int64) (bool, error) {
	f.hotRecords.Add(1)
	if f.recordErr != nil {
		return false, f.recordErr
	}
	if f.quarantined[path] {
		return false, nil
	}
	if f.recordedHot == nil {
		f.recordedHot = map[string]int64{}
	}
	f.recordedHot[path] = size
	return true, nil
}

func (f *fakeColdSource) RecordRestoredColdFile(_ context.Context, path string, size int64) (bool, error) {
	f.records.Add(1)
	if f.recordErr != nil {
		return false, f.recordErr
	}
	if f.quarantined[path] {
		return false, nil
	}
	if f.recorded == nil {
		f.recorded = map[string]int64{}
	}
	f.recorded[path] = size
	return true, nil
}

// coldRig is routedRig plus a real cold-tier backend.
type coldRig struct {
	*routedRig
	coldDir string
	cold    *fakeColdSource
}

func newColdRig(t *testing.T, routing map[string]string) *coldRig {
	t.Helper()
	rig := newRoutedRig(t, routing)
	coldDir := t.TempDir()
	cb, err := storage.NewLocalBackend(coldDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cb.Close() })
	src := &fakeColdSource{backend: cb, rows: map[string]int64{}, quarantined: map[string]bool{}}
	rig.m.SetColdSource(src)
	return &coldRig{routedRig: rig, coldDir: coldDir, cold: src}
}

// writeCold seeds a cold object AND its tier row, the ordinary migrated state.
func (r *coldRig) writeCold(t *testing.T, path, body string) {
	t.Helper()
	if err := r.cold.backend.Write(context.Background(), path, []byte(body)); err != nil {
		t.Fatalf("seed cold %s: %v", path, err)
	}
	r.cold.rows[path] = int64(len(body))
}

// writeColdNoRow seeds the object only: the lagging-rows case.
func (r *coldRig) writeColdNoRow(t *testing.T, path, body string) {
	t.Helper()
	if err := r.cold.backend.Write(context.Background(), path, []byte(body)); err != nil {
		t.Fatalf("seed cold %s: %v", path, err)
	}
}

// A backup carries the cold tier. The headline row: a database whose data is
// entirely cold is no longer backed up empty.
func TestBackupCarriesColdTierFiles(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/hot.parquet", "PAR1hot")
	rig.writeCold(t, "archive/events/2026/01/01/00/cold.parquet", "PAR1cold")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.TotalFiles != 2 {
		t.Errorf("total_files = %d, want 2 (one hot, one cold)", m.TotalFiles)
	}
	if m.ColdFiles != 1 {
		t.Errorf("cold_files = %d, want 1", m.ColdFiles)
	}
	if m.ColdSizeBytes != int64(len("PAR1cold")) {
		t.Errorf("cold_size_bytes = %d, want %d", m.ColdSizeBytes, len("PAR1cold"))
	}
	// The cold database is in the inventory, which is the whole point.
	var names []string
	for _, db := range m.Databases {
		names = append(names, db.Name)
	}
	if len(names) != 2 {
		t.Errorf("inventory = %v, want both prod and archive", names)
	}
	// And the bytes are really in the backup.
	if !existsIn(t, rig.m.backupStorage, result.Manifest.BackupID+"/data/archive/events/2026/01/01/00/cold.parquet") {
		t.Error("the cold object is not under <id>/data/ in the backup")
	}
}

// The sidecar records the tier per file, and only for cold: a hot row carries
// no tier at all, so a hot-only backup's sidecar is what it always was.
func TestSidecarRecordsTheTierForColdFilesOnly(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/hot.parquet", "PAR1hot")
	rig.writeCold(t, "prod/cpu/2026/01/01/00/cold.parquet", "PAR1cold")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(rig.mainDir, result.Manifest.BackupID, sidecarName))
	if err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	var sc fileSidecar
	if err := json.Unmarshal(raw, &sc); err != nil {
		t.Fatalf("decode sidecar: %v", err)
	}
	tiers := map[string]string{}
	for _, row := range sc.Files {
		tiers[row.Path] = row.Tier
	}
	if got := tiers["prod/cpu/2026/01/01/00/cold.parquet"]; got != tierCold {
		t.Errorf("cold row tier = %q, want %q", got, tierCold)
	}
	if got, ok := tiers["prod/cpu/2026/10/07/00/hot.parquet"]; !ok || got != "" {
		t.Errorf("hot row tier = %q (present=%v), want empty so a hot-only sidecar is unchanged", got, ok)
	}
}

// Object with no tier row: COPIED anyway and counted. Refusing it would mean a
// node whose cold metadata is incomplete backs up no cold data at all.
func TestColdObjectWithNoRowIsStillCopiedAndCounted(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/hot.parquet", "PAR1hot")
	rig.writeColdNoRow(t, "prod/cpu/2026/01/01/00/orphan.parquet", "PAR1orphan")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.ColdFiles != 1 {
		t.Errorf("cold_files = %d, want 1: an object with no row must still be carried", m.ColdFiles)
	}
	if m.ColdObjectsUnrecorded != 1 {
		t.Errorf("cold_objects_unrecorded = %d, want 1", m.ColdObjectsUnrecorded)
	}
	if !existsIn(t, rig.m.backupStorage, result.Manifest.BackupID+"/data/prod/cpu/2026/01/01/00/orphan.parquet") {
		t.Error("the unrecorded cold object was not copied")
	}
}

// Row with no object: counted as the gap, not copied — and that gap is what
// cold_files_excluded now means.
func TestColdRowWithNoObjectIsTheReportedGap(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/hot.parquet", "PAR1hot")
	rig.writeCold(t, "prod/cpu/2026/01/01/00/present.parquet", "PAR1present")
	// A row whose object is gone.
	rig.cold.rows["prod/cpu/2025/01/01/00/vanished.parquet"] = 999
	rig.m.SetColdCounter(&fakeColdCounter{counts: map[string]int64{"prod": 2}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.ColdFiles != 1 {
		t.Errorf("cold_files = %d, want 1 (only the present object)", m.ColdFiles)
	}
	// NOT 2 (the counter's figure): with a cold source the marker is the gap,
	// not every cold row, because the rest are in the backup.
	if m.ColdFilesExcluded != 1 {
		t.Errorf("cold_files_excluded = %d, want 1 — the marker must be the ROW-WITHOUT-OBJECT gap once cold files are carried, not every cold row", m.ColdFilesExcluded)
	}
}

// Mid-migration: one path in both listings. The COLD copy wins, one copy, one
// sidecar row, counted.
func TestMidMigrationDedupPrefersTheColdCopy(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	const dup = "prod/cpu/2026/10/07/00/dup.parquet"
	rig.write(t, dup, "HOTBYTES")
	rig.writeCold(t, dup, "COLDBYTE")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID
	m := readManifestFile(t, rig.mainDir, id)
	if m.TotalFiles != 1 {
		t.Errorf("total_files = %d, want 1: one path must be carried once", m.TotalFiles)
	}
	if m.ColdFiles != 1 {
		t.Errorf("cold_files = %d, want 1: the cold copy wins", m.ColdFiles)
	}
	if m.ColdDedupSkipped != 1 {
		t.Errorf("cold_dedup_skipped = %d, want 1", m.ColdDedupSkipped)
	}
	// One sidecar row, tier cold.
	raw, err := os.ReadFile(filepath.Join(rig.mainDir, id, sidecarName))
	if err != nil {
		t.Fatal(err)
	}
	var sc fileSidecar
	if err := json.Unmarshal(raw, &sc); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, row := range sc.Files {
		if row.Path == dup {
			n++
			if row.Tier != tierCold {
				t.Errorf("the deduped row's tier = %q, want cold", row.Tier)
			}
		}
	}
	if n != 1 {
		t.Errorf("the sidecar has %d rows for one path, want 1", n)
	}
}

// An unreachable cold store FAILS the run. It must never become N skips and a
// green status: nothing in Arc deletes a cold object, so a read failure is an
// availability failure and not the "compaction raced us" case the hot path
// tolerates.
func TestUnreachableColdStoreFailsTheBackup(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	// ENOUGH HOT FILES that one skip stays well under the global skip ratio.
	// Without them the run fails on the ratio instead of on the cold guard,
	// and the test passes for a reason that has nothing to do with the tier —
	// which is exactly what the first version of it did.
	for i := 0; i < 30; i++ {
		rig.write(t, fmt.Sprintf("prod/cpu/2026/10/07/00/hot%d.parquet", i), "PAR1hot")
	}
	const unreadable = "prod/cpu/2026/01/01/00/cold.parquet"
	rig.writeCold(t, unreadable, "PAR1cold")
	// The object LISTS and then will not READ. Removing the file instead would
	// not test anything: it would simply be absent from the listing, so the
	// cold copy phase would have nothing to do and the run would pass for the
	// wrong reason.
	local, ok := rig.cold.backend.(*storage.LocalBackend)
	if !ok {
		t.Fatalf("cold backend is %T, want *storage.LocalBackend for this test", rig.cold.backend)
	}
	rig.cold.backend = &readFailingBackend{
		LocalBackend: local,
		failPaths:    map[string]bool{unreadable: true},
	}

	_, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err == nil {
		t.Fatal("the backup SUCCEEDED with an unreadable cold object; a cold read failure must fail the run, not be skipped like a hot one")
	}
	// Name the mechanism, so an unrelated failure cannot satisfy this.
	if !strings.Contains(err.Error(), "cold tier storage") {
		t.Errorf("error = %q, want it to name the cold tier source; any other failure would satisfy a bare non-nil check", err)
	}
	if p := rig.m.GetProgress(); p == nil || p.Status != "failed" {
		t.Errorf("progress = %+v, want failed", p)
	}
}

// No cold source: everything is exactly as it was before this stage.
func TestBackupWithoutAColdSourceIsUnchanged(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/hot.parquet", "PAR1hot")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.ColdFiles != 0 || m.ColdSizeBytes != 0 || m.ColdObjectsUnrecorded != 0 || m.ColdDedupSkipped != 0 {
		t.Errorf("cold fields are set without a cold source: %+v", m)
	}
	if m.TotalFiles != 1 {
		t.Errorf("total_files = %d, want 1", m.TotalFiles)
	}
}

// A cold tier configured but DISABLED takes the no-cold-source path, because
// ColdBackend() ANDs the enabled flag. Modelled here as a source whose backend
// is nil, which is what the tiering manager returns in that configuration.
func TestColdSourceWithNoBackendIsIgnored(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/hot.parquet", "PAR1hot")
	rig.m.SetColdSource(&fakeColdSource{backend: nil, rows: map[string]int64{"prod/x.parquet": 1}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.ColdFiles != 0 {
		t.Errorf("cold_files = %d, want 0 when the cold backend is nil", m.ColdFiles)
	}
}

// Routing applies to cold files by the same rule as hot ones, because a
// migrated object keeps its key.
func TestColdFilesFollowPerDatabaseRouting(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "prod/cpu/2026/10/07/00/hot.parquet", "PAR1hot")
	rig.writeCold(t, "audit/events/2026/01/01/00/cold.parquet", "PAR1cold")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID
	audit := readManifestFile(t, rig.auditDir, id)
	if audit.ColdFiles != 1 {
		t.Errorf("audit leg cold_files = %d, want 1", audit.ColdFiles)
	}
	main := readManifestFile(t, rig.mainDir, id)
	if main.ColdFiles != 0 {
		t.Errorf("default leg cold_files = %d, want 0: audit routes away", main.ColdFiles)
	}
	if !existsIn(t, rig.m.backupStorage, id+"/data/prod/cpu/2026/10/07/00/hot.parquet") {
		t.Error("the hot file is not on the default target")
	}
}

// A scoped backup carries the scope's cold files and nothing else's.
func TestScopedBackupCarriesOnlyItsOwnColdFiles(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/hot.parquet", "PAR1hot")
	rig.writeCold(t, "prod/cpu/2026/01/01/00/mine.parquet", "PAR1mine")
	rig.writeCold(t, "other/cpu/2026/01/01/00/theirs.parquet", "PAR1theirs")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{Databases: []string{"prod"}})
	if err != nil {
		t.Fatalf("CreateBackup scoped: %v", err)
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.ColdFiles != 1 {
		t.Errorf("cold_files = %d, want 1: only prod is in scope", m.ColdFiles)
	}
	if existsIn(t, rig.m.backupStorage, result.Manifest.BackupID+"/data/other/cpu/2026/01/01/00/theirs.parquet") {
		t.Error("a cold file outside the scope was copied")
	}
}

// The merged run view sums the cold counters across legs. A single leg takes
// the clone shortcut, so only two legs prove the merge.
func TestMergeRunManifestsSumsTheColdCounters(t *testing.T) {
	def := &Manifest{
		BackupID: "b1", IsDefaultTarget: true,
		ColdFiles: 2, ColdSizeBytes: 20, ColdObjectsUnrecorded: 1, ColdDedupSkipped: 1,
	}
	other := &Manifest{
		BackupID:  "b1",
		ColdFiles: 3, ColdSizeBytes: 30, ColdObjectsUnrecorded: 2, ColdDedupSkipped: 0,
	}
	merged := mergeRunManifests([]*Manifest{def, other})
	if merged.ColdFiles != 5 || merged.ColdSizeBytes != 50 {
		t.Errorf("merged cold_files=%d cold_size_bytes=%d, want 5 and 50", merged.ColdFiles, merged.ColdSizeBytes)
	}
	if merged.ColdObjectsUnrecorded != 3 || merged.ColdDedupSkipped != 1 {
		t.Errorf("merged unrecorded=%d dedup=%d, want 3 and 1", merged.ColdObjectsUnrecorded, merged.ColdDedupSkipped)
	}
	if s := SummaryFromManifest(merged); s.ColdFiles != 5 {
		t.Errorf("summary cold_files = %d, want 5", s.ColdFiles)
	}
	leg := runLeg{target: backupTarget{name: "audit"}, manifest: other}
	if got := leg.slice().ColdFiles; got != 3 {
		t.Errorf("TargetSlice.ColdFiles = %d, want 3", got)
	}
}

// A restore puts a cold file back in the COLD tier and records its row — the
// row is what makes it queryable — and does NOT report it as a hot pull.
func TestRestorePutsColdFilesBackInTheColdTier(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/hot.parquet", "PAR1hot")
	const coldPath = "prod/cpu/2026/01/01/00/cold.parquet"
	rig.writeCold(t, coldPath, "PAR1cold")
	recorder := &fakeTierRecorder{}
	rig.m.SetTierRecorder(recorder)

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	// Wipe both stores so the restore has to put each file back itself.
	if err := os.RemoveAll(filepath.Join(rig.dataDir, "prod")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(rig.coldDir, "prod")); err != nil {
		t.Fatal(err)
	}
	recorder.reports = nil

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}

	// The cold file is in the COLD store, not hot.
	if _, err := os.Stat(filepath.Join(rig.coldDir, coldPath)); err != nil {
		t.Errorf("the cold file is not in the cold store after the restore: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rig.dataDir, coldPath)); err == nil {
		t.Error("the cold file was written to HOT storage as well")
	}
	// Its row was recorded, which is what makes it queryable.
	if rig.cold.recorded[coldPath] == 0 {
		t.Errorf("no cold tier row recorded for %s; without it the query path cannot route to the file", coldPath)
	}
	// And it was NOT reported as a hot pull: that path stats the hot backend
	// and its upsert is guarded to hot rows, so the report would be dropped.
	for _, rep := range recorder.reports {
		if rep.path == coldPath {
			t.Error("the cold file was reported through RecordRestoredFile; that path is for hot files and silently drops a cold one")
		}
	}
	p := rig.m.GetProgress()
	if p == nil || p.ColdFilesRestoredToCold != 1 {
		t.Errorf("progress cold_files_restored_to_cold = %v, want 1", p)
	}
}

// Restoring onto a node with NO cold tier puts the file in hot and counts it.
func TestRestoreWithoutAColdTierFallsBackToHot(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	const coldPath = "prod/cpu/2026/01/01/00/cold.parquet"
	rig.writeCold(t, coldPath, "PAR1cold")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(rig.coldDir, "prod")); err != nil {
		t.Fatal(err)
	}
	// The restoring node has no cold tier at all.
	rig.m.coldSource = &fakeColdSource{backend: nil}

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rig.dataDir, coldPath)); err != nil {
		t.Errorf("the cold file was not restored to hot storage: %v", err)
	}
	p := rig.m.GetProgress()
	if p == nil || p.ColdFilesRestoredToHot != 1 {
		t.Errorf("progress cold_files_restored_to_hot = %v, want 1", p)
	}
	if p.ColdFilesRestoredToCold != 0 {
		t.Errorf("cold_files_restored_to_cold = %d, want 0", p.ColdFilesRestoredToCold)
	}
}

// A quarantined row is left alone and counted: its key is permanently unusable
// and a restore must not resurrect it.
func TestRestoreLeavesAQuarantinedColdRowAlone(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	const coldPath = "prod/cpu/2026/01/01/00/cold.parquet"
	rig.writeCold(t, coldPath, "PAR1cold")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(rig.coldDir, "prod")); err != nil {
		t.Fatal(err)
	}
	rig.cold.quarantined[coldPath] = true

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	p := rig.m.GetProgress()
	if p == nil || p.ColdRestoreQuarantineSkipped != 1 {
		t.Errorf("progress cold_restore_quarantine_skipped = %v, want 1", p)
	}
	if rig.cold.recorded[coldPath] != 0 {
		t.Error("a quarantined row was written anyway")
	}
}

// A row-recording failure warns and the restore completes: the bytes are in
// the cold store, only the row is missing.
func TestRestoreColdRowFailureDoesNotFailTheRestore(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	rig.writeCold(t, "prod/cpu/2026/01/01/00/cold.parquet", "PAR1cold")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(rig.coldDir, "prod")); err != nil {
		t.Fatal(err)
	}
	rig.cold.recordErr = errors.New("database is locked")

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("a cold row failure failed the whole restore: %v", err)
	}
	p := rig.m.GetProgress()
	if p == nil || p.ColdRowsNotRecorded != 1 {
		t.Errorf("progress cold_rows_not_recorded = %v, want 1", p)
	}
}

// THE STRUCTURAL REGRESSION GUARDS. Two separate cluster mechanisms would each
// destroy a cold file silently, and a fixture with no cold tier cannot see
// either. One test per mechanism, because fixing one does not fix the other.

// Mechanism 1: crossCheckManifest holds back any data file the Raft manifest
// does not list, and every cold file is absent from it by construction. If the
// cold set were merged into the hot one, the whole of it would be dropped and
// counted as unregistered_skipped.
func TestClusterColdFilesSurviveTheManifestCrossCheck(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	const hotPath = "prod/cpu/2026/10/07/00/hot.parquet"
	rig.write(t, hotPath, "PAR1hot")
	rig.writeCold(t, "prod/cpu/2026/01/01/00/cold.parquet", "PAR1cold")
	// The manifest lists the HOT file only, which is what a real cluster looks
	// like: tiering deletes a migrated file's entry as phase 2 of the
	// migration, and the manifest adapter drops cold entries anyway.
	rig.m.SetClusterManifest(&fakeClusterManifest{entries: []ManifestFile{
		{Path: hotPath, Database: "prod", Measurement: "cpu", SizeBytes: 7},
	}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup on a cluster: %v", err)
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.ColdFiles != 1 {
		t.Errorf("cold_files = %d, want 1: the cross-check must not hold back a cold file for being absent from the Raft manifest", m.ColdFiles)
	}
	if m.UnregisteredSkipped != 0 {
		t.Errorf("unregistered_skipped = %d, want 0: a cold file is not an unregistered hot file", m.UnregisteredSkipped)
	}
	if !existsIn(t, rig.m.backupStorage, result.Manifest.BackupID+"/data/prod/cpu/2026/01/01/00/cold.parquet") {
		t.Error("the cold object is not in the backup")
	}
}

// Mechanism 2: recheckClusterManifest walks the sidecar at end of run and
// DELETES the backup object for any row the fresh manifest snapshot lacks.
// Every cold row qualifies, so without the tier guard a cluster backup copies
// its cold set and then deletes it again, reporting success.
func TestClusterColdFilesSurviveTheEndOfRunRecheck(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	const hotPath = "prod/cpu/2026/10/07/00/hot.parquet"
	const coldPath = "prod/cpu/2026/01/01/00/cold.parquet"
	rig.write(t, hotPath, "PAR1hot")
	rig.writeCold(t, coldPath, "PAR1cold")
	rig.m.SetClusterManifest(&fakeClusterManifest{entries: []ManifestFile{
		{Path: hotPath, Database: "prod", Measurement: "cpu", SizeBytes: 7},
	}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup on a cluster: %v", err)
	}
	id := result.Manifest.BackupID
	// The object is still there AFTER the recheck ran.
	if !existsIn(t, rig.m.backupStorage, id+"/data/"+coldPath) {
		t.Fatal("the end-of-run recheck DELETED the cold object from the backup: it is never in the cluster manifest, so it must be skipped by that pass")
	}
	m := readManifestFile(t, rig.mainDir, id)
	if m.LeftManifestDuringRun != 0 {
		t.Errorf("left_manifest_during_run = %d, want 0: a cold file never left the manifest, it was never in it", m.LeftManifestDuringRun)
	}
	if m.TotalFiles != 2 {
		t.Errorf("total_files = %d, want 2: the recheck must not decrement for a cold file", m.TotalFiles)
	}
	// And the sidecar still has its row, with the tier.
	raw, err := os.ReadFile(filepath.Join(rig.mainDir, id, sidecarName))
	if err != nil {
		t.Fatal(err)
	}
	var sc fileSidecar
	if err := json.Unmarshal(raw, &sc); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range sc.Files {
		if row.Path == coldPath {
			found = true
			if row.Tier != tierCold {
				t.Errorf("surviving row tier = %q, want cold", row.Tier)
			}
		}
	}
	if !found {
		t.Error("the recheck dropped the cold file's sidecar row")
	}
}

// A skipped cold file must not be written off as "reconciled: not missing
// data" by the recheck's last pass. That pass walks skipTally.paths and treats
// any registrable path the fresh manifest lacks as reconciled — and a cold
// file is NEVER in the manifest, so it would satisfy the test automatically
// while genuinely being absent from the backup.
//
// Tested on the tally directly rather than through a backup run: the only
// skippable cold case is an overlong destination key (a cold READ failure is
// fatal), and a key long enough to trigger it is longer than a real
// filesystem will store, so a rig-level test would be fighting the OS instead
// of the logic.
func TestSkippedColdFileStaysOutOfTheReconciledPass(t *testing.T) {
	var tally skipTally
	tally.record("prod/cpu/2026/10/07/00/hot.parquet", true, false)
	tally.record("prod/cpu/2026/01/01/00/cold.parquet", true, true)

	// Both are reported to the operator.
	if len(tally.sample) != 2 {
		t.Errorf("sample = %v, want both paths: an operator needs to see a skipped cold file", tally.sample)
	}
	// Only the hot one is offered to the reconciled pass.
	if len(tally.paths) != 1 || tally.paths[0] != "prod/cpu/2026/10/07/00/hot.parquet" {
		t.Errorf("paths = %v, want the hot path only; a cold path here would be counted as reconciled because the manifest never listed it", tally.paths)
	}
	if tally.overlong != 2 {
		t.Errorf("overlong = %d, want 2", tally.overlong)
	}
}

// A tier row that says cold, has no cold object, and whose file the backup
// carried from HOT storage is NOT a gap: the data is in the backup. Counting
// it as one reports a file the run holds as permanently gone — and that state
// is persistent on a node whose reconciliation is role gated.
func TestStaleColdRowWithAHotFileIsNotReportedAsAGap(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	const path = "prod/cpu/2026/10/07/00/both.parquet"
	// In hot storage, and in the manifest-less standalone data set.
	rig.write(t, path, "PAR1hot")
	// A cold ROW for it, with no cold OBJECT: the shape a rolled-back
	// migration leaves, which the cold sync never downgrades.
	rig.cold.rows[path] = 7
	rig.m.SetColdCounter(&fakeColdCounter{counts: map[string]int64{"prod": 1}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.ColdFilesExcluded != 0 {
		t.Errorf("cold_files_excluded = %d, want 0: the file is in the backup, carried from hot, so it is not data that is gone", m.ColdFilesExcluded)
	}
	if m.ColdRowsStaleButHot != 1 {
		t.Errorf("cold_rows_stale_but_hot = %d, want 1: the disagreement must still be visible", m.ColdRowsStaleButHot)
	}
	if m.TotalFiles != 1 {
		t.Errorf("total_files = %d, want 1", m.TotalFiles)
	}
}

// The gap keeps its per-database breakdown. Stage B shipped that field and
// tooling may read it; it must not silently go empty once cold files are
// carried.
func TestTheGapKeepsItsPerDatabaseBreakdown(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/hot.parquet", "PAR1hot")
	// Two databases with rows whose objects are gone.
	rig.cold.rows["prod/cpu/2025/01/01/00/gone.parquet"] = 11
	rig.cold.rows["audit/events/2025/01/01/00/gone.parquet"] = 22
	rig.m.SetColdCounter(&fakeColdCounter{counts: map[string]int64{"prod": 1, "audit": 1}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.ColdFilesExcluded != 2 {
		t.Errorf("cold_files_excluded = %d, want 2", m.ColdFilesExcluded)
	}
	got := m.ColdFilesExcludedDatabases
	if got["prod"] != 1 || got["audit"] != 1 {
		t.Errorf("breakdown = %v, want {prod:1, audit:1}: the per-database detail must survive stage C", got)
	}
}

// A mid-migration path whose manifest entry is ALREADY gone is held back by
// the cross-check, so it never reaches the hot set and the cold walk carries it
// with no dedup. It must not then be reported as unregistered_skipped: it IS
// in the backup, and that count feeds the skip-ratio refusal of a replace-mode
// restore, so a run could make its own restore refuse.
func TestColdCarriedPathIsNotReportedUnregistered(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	const hotPath = "prod/cpu/2026/10/07/00/registered.parquet"
	const migrating = "prod/cpu/2026/01/01/00/migrating.parquet"
	rig.write(t, hotPath, "PAR1hot")
	// In hot storage AND in the cold store, with its manifest entry already
	// deleted: phase 2 done, hot copy not yet released.
	rig.write(t, migrating, "HOTCOPY")
	rig.writeCold(t, migrating, "COLDCOPY")
	rig.m.SetClusterManifest(&fakeClusterManifest{entries: []ManifestFile{
		{Path: hotPath, Database: "prod", Measurement: "cpu", SizeBytes: 7},
	}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.UnregisteredSkipped != 0 {
		t.Errorf("unregistered_skipped = %d, want 0: the cold tier supplied that path, so reporting it as not backed up is false — and the count refuses a replace-mode restore past the skip ratio", m.UnregisteredSkipped)
	}
	if m.ColdFiles != 1 {
		t.Errorf("cold_files = %d, want 1", m.ColdFiles)
	}
	if !existsIn(t, rig.m.backupStorage, result.Manifest.BackupID+"/data/"+migrating) {
		t.Error("the migrating path is in neither set")
	}
}

// H2: a cold file restored into HOT storage must have its tier row MOVED to
// hot. If the row still says cold, the query path omits the hot glob (nothing
// claims hot) and the cold glob (no cold backend) and the measurement reads as
// EMPTY — the data is invisible, not merely mis-tiered.
func TestColdFileRestoredToHotMovesItsTierRow(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	const coldPath = "prod/cpu/2026/01/01/00/cold.parquet"
	rig.writeCold(t, coldPath, "PAR1cold")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(rig.coldDir, "prod")); err != nil {
		t.Fatal(err)
	}
	// The restoring node has tiering, and a cold ROW for the path, but no cold
	// BACKEND — cold turned off, or its construction failed. Both are
	// supported configurations.
	noCold := &fakeColdSource{backend: nil, rows: map[string]int64{coldPath: 8}, quarantined: map[string]bool{}}
	rig.m.coldSource = noCold

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rig.dataDir, coldPath)); err != nil {
		t.Fatalf("the file was not restored to hot storage: %v", err)
	}
	if noCold.recordedHot[coldPath] == 0 {
		t.Error("the tier row was NOT moved to hot; with the row still saying cold and no cold backend the query path returns nothing for the measurement")
	}
	p := rig.m.GetProgress()
	if p == nil || p.ColdFilesRestoredToHot != 1 {
		t.Errorf("cold_files_restored_to_hot = %v, want 1", p)
	}
}

// With NO tiering at all there is no row to move, and the branch must not
// dereference a nil cold source.
func TestColdFileRestoredToHotWithNoTieringAtAll(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	const coldPath = "prod/cpu/2026/01/01/00/cold.parquet"
	rig.writeCold(t, coldPath, "PAR1cold")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(rig.coldDir, "prod")); err != nil {
		t.Fatal(err)
	}
	rig.m.coldSource = nil // no tiering on the restoring node

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup with no tiering: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rig.dataDir, coldPath)); err != nil {
		t.Errorf("the file was not restored to hot storage: %v", err)
	}
}

// Mechanism 1, the strongest of the five and the one the release notes claimed
// a test for: a FULLY cold scope on a cluster contributes zero manifest
// entries, and crossCheckManifest refuses a run whose manifest is empty while
// the listing is not. Merging cold into the hot set would make that fire.
func TestClusterFullyColdScopeIsNotRefused(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	// No hot files at all, one cold object, and an EMPTY cluster manifest —
	// which is exactly right, since tiering removes a migrated file's entry.
	rig.writeCold(t, "archive/events/2026/01/01/00/cold.parquet", "PAR1cold")
	rig.m.SetClusterManifest(&fakeClusterManifest{entries: nil})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("a fully cold scope on a cluster was REFUSED: %v\n"+
			"the manifest adapter drops cold entries, so entries is empty while the listing is not; "+
			"cold files must not reach crossCheckManifest or it refuses the whole run", err)
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.ColdFiles != 1 {
		t.Errorf("cold_files = %d, want 1", m.ColdFiles)
	}
}

// A corrupt sidecar must not fail a STANDALONE restore: it never read the file
// before stage C, so an undecodable one would be a new way for an old backup
// to stop restoring. Everything falls back to hot.
func TestCorruptSidecarDoesNotFailAStandaloneRestore(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/hot.parquet", "PAR1hot")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID
	if err := os.WriteFile(filepath.Join(rig.mainDir, id, sidecarName), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(rig.dataDir, "prod")); err != nil {
		t.Fatal(err)
	}

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: id, RestoreData: true}); err != nil {
		t.Fatalf("a corrupt sidecar failed a standalone restore: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rig.dataDir, "prod/cpu/2026/10/07/00/hot.parquet")); err != nil {
		t.Errorf("the data was not restored: %v", err)
	}
}
