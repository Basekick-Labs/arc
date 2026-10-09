package backup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
)

// fakeColdCounter is the tiering manager's grouped cold count, stubbed.
type fakeColdCounter struct {
	counts map[string]int64
	err    error
	calls  atomic.Int32
}

func (f *fakeColdCounter) CountColdFilesByDatabase(_ context.Context) (map[string]int64, error) {
	f.calls.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	return f.counts, nil
}

// A database's cold gap is reported on the leg its DATA routes to, and on no
// other leg. The bug this pins is the one that reads as correct: writing the
// whole count to every leg, or to the default leg only.
func TestColdFilesExcludedIsAttributedPerLeg(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "prod/cpu/2026/10/07/00/a.parquet", "PAR1")
	rig.write(t, "audit/events/2026/10/07/00/b.parquet", "PAR1")
	rig.m.SetColdCounter(&fakeColdCounter{counts: map[string]int64{"prod": 7, "audit": 11}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID

	audit := readManifestFile(t, rig.auditDir, id)
	if audit.ColdFilesExcluded != 11 {
		t.Errorf("audit leg cold_files_excluded = %d, want 11 (its own database only)", audit.ColdFilesExcluded)
	}
	if got := audit.ColdFilesExcludedDatabases; len(got) != 1 || got["audit"] != 11 {
		t.Errorf("audit leg breakdown = %v, want exactly {audit:11}; prod routes elsewhere and must not appear", got)
	}
	main := readManifestFile(t, rig.mainDir, id)
	if main.ColdFilesExcluded != 7 {
		t.Errorf("default leg cold_files_excluded = %d, want 7 (prod only)", main.ColdFilesExcluded)
	}
	if got := main.ColdFilesExcludedDatabases; len(got) != 1 || got["prod"] != 7 {
		t.Errorf("default leg breakdown = %v, want exactly {prod:7}", got)
	}

	// And the run-level view is the sum, which is what every consumer reads.
	merged := mergeRunManifests([]*Manifest{main, audit})
	if merged.ColdFilesExcluded != 18 {
		t.Errorf("merged cold_files_excluded = %d, want 18", merged.ColdFilesExcluded)
	}
	if got := merged.ColdFilesExcludedDatabases; len(got) != 2 || got["prod"] != 7 || got["audit"] != 11 {
		t.Errorf("merged breakdown = %v, want {prod:7, audit:11}", got)
	}
}

// A FULLY cold database has no hot file, so it is in no listing and in no
// manifest inventory — and it must still be counted. This is the prospect's
// shape and the reason the count does not read the inventory.
func TestColdFilesExcludedCountsAFullyColdDatabase(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "prod/cpu/2026/10/07/00/a.parquet", "PAR1")
	// "archive" has no files at all: every one of them is cold.
	rig.m.SetColdCounter(&fakeColdCounter{counts: map[string]int64{"archive": 42}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	main := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if main.ColdFilesExcluded != 42 {
		t.Errorf("cold_files_excluded = %d, want 42 for a database with nothing hot left", main.ColdFilesExcluded)
	}
	if got := main.ColdFilesExcludedDatabases["archive"]; got != 42 {
		t.Errorf("breakdown[archive] = %d, want 42", got)
	}
	for _, db := range main.Databases {
		if db.Name == "archive" {
			t.Error("archive is in the manifest inventory; the test no longer covers the fully-cold case")
		}
	}
}

// A scoped run reports only the scope's databases. An out-of-scope database
// has no leg of its own, so counting it would land its gap on the default leg
// and report one database's gap against another's slice.
func TestColdFilesExcludedRespectsTheScope(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "prod/cpu/2026/10/07/00/a.parquet", "PAR1")
	rig.write(t, "audit/events/2026/10/07/00/b.parquet", "PAR1")
	rig.m.SetColdCounter(&fakeColdCounter{counts: map[string]int64{"prod": 7, "audit": 11}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{Databases: []string{"prod"}})
	if err != nil {
		t.Fatalf("CreateBackup scoped: %v", err)
	}
	main := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if main.ColdFilesExcluded != 7 {
		t.Errorf("scoped cold_files_excluded = %d, want 7; audit is out of scope", main.ColdFilesExcluded)
	}
	if _, ok := main.ColdFilesExcludedDatabases["audit"]; ok {
		t.Errorf("breakdown names the out-of-scope audit database: %v", main.ColdFilesExcludedDatabases)
	}
}

// Tiering off: no counter, no field, and the manifest is what it was before
// this stage existed.
func TestColdFilesExcludedAbsentWithoutTiering(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/a.parquet", "PAR1")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.ColdFilesExcluded != 0 || m.ColdFilesExcludedDatabases != nil {
		t.Errorf("without a cold counter: excluded=%d breakdown=%v, want 0 and nil", m.ColdFilesExcluded, m.ColdFilesExcludedDatabases)
	}
}

// A counting failure warns and the backup COMPLETES. The count annotates a run
// that is already complete, so failing it would trade a healthy backup for no
// backup over a diagnostic.
func TestColdFileCountFailureDoesNotFailTheBackup(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/a.parquet", "PAR1")
	counter := &fakeColdCounter{err: errors.New("database is locked")}
	rig.m.SetColdCounter(counter)

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("a cold-count failure failed the whole backup: %v", err)
	}
	if counter.calls.Load() != 1 {
		t.Errorf("counter called %d times, want 1", counter.calls.Load())
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.ColdFilesExcluded != 0 {
		t.Errorf("cold_files_excluded = %d after a failed count, want 0", m.ColdFilesExcluded)
	}
	if m.TotalFiles != 1 {
		t.Errorf("the backup carried %d files, want 1: a count failure must not affect the copy", m.TotalFiles)
	}
}

// SetColdCounter ignores nil, like SetTierLookup: a caller holding a typed nil
// must check for nil itself (#713).
func TestSetColdCounterIgnoresNil(t *testing.T) {
	m := &Manager{logger: zerolog.Nop()}
	m.SetColdCounter(nil)
	if m.coldCounter != nil {
		t.Error("SetColdCounter(nil) stored a non-nil counter")
	}
}

// The merged run view must SUM the field. A single-leg merge takes the clone
// shortcut and would pass even if the field were missing from the merge, so
// the two-leg case is the only one that proves it.
func TestMergeRunManifestsSumsTheColdCount(t *testing.T) {
	def := &Manifest{
		BackupID:                   "b1",
		IsDefaultTarget:            true,
		ColdFilesExcluded:          7,
		ColdFilesExcludedDatabases: map[string]int64{"prod": 7},
	}
	other := &Manifest{
		BackupID:                   "b1",
		ColdFilesExcluded:          11,
		ColdFilesExcludedDatabases: map[string]int64{"audit": 11},
	}
	merged := mergeRunManifests([]*Manifest{def, other})
	if merged.ColdFilesExcluded != 18 {
		t.Errorf("merged = %d, want 18; the field is not summed in mergeRunManifests", merged.ColdFilesExcluded)
	}
	if got := merged.ColdFilesExcludedDatabases; got["prod"] != 7 || got["audit"] != 11 {
		t.Errorf("merged breakdown = %v, want both legs", got)
	}
	// Merging must not mutate a leg's own map — stated as the value, since
	// that is the claim; a key count would pass if the merge doubled it.
	if len(def.ColdFilesExcludedDatabases) != 1 || def.ColdFilesExcludedDatabases["prod"] != 7 {
		t.Errorf("the default leg's breakdown was mutated by the merge: %v, want exactly {prod:7}", def.ColdFilesExcludedDatabases)
	}
	// And the listing summary carries the run total.
	if s := SummaryFromManifest(merged); s.ColdFilesExcluded != 18 {
		t.Errorf("SummaryFromManifest cold_files_excluded = %d, want 18", s.ColdFilesExcluded)
	}
}

// The per-target detail carries each leg's own figure: this is the only API
// surface that says WHICH target holds the gap.
func TestTargetSliceCarriesTheColdCount(t *testing.T) {
	leg := runLeg{
		target:   backupTarget{name: "audit"},
		manifest: &Manifest{ColdFilesExcluded: 11},
	}
	if got := leg.slice().ColdFilesExcluded; got != 11 {
		t.Errorf("TargetSlice.ColdFilesExcluded = %d, want 11", got)
	}
}

// THE DECISION-1 REGRESSION TEST. A replace-mode restore of a partly-cold
// backup must SUCCEED. If somebody adds the cold count to the incompleteness
// refusal in replaceDatabases, this fails — which is the whole point: every
// partly-cold database would otherwise refuse a replace restore, and partly
// cold is the normal state of any deployment with tiering on.
//
// m.cluster MUST be stubbed. Replace mode is refused outright when
// m.cluster == nil ("only available on a cluster node"), so without a cluster
// this test would pass on that refusal and prove nothing about the gate it
// claims to cover.
func TestReplaceRestoreOfAPartlyColdBackupSucceeds(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, nil)
	const prodFile = "prod/cpu/2026/10/07/00/a.parquet"
	rig.write(t, prodFile, "PAR1prod")

	cm := &fakeClusterManifest{entries: []ManifestFile{
		{Path: prodFile, Database: "prod", Measurement: "cpu", SizeBytes: 8},
	}}
	rig.m.SetClusterManifest(cm)
	// prod has 412 files in the cold tier that this backup will not carry.
	rig.m.SetColdCounter(&fakeColdCounter{counts: map[string]int64{"prod": 412}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID
	if result.Manifest.ColdFilesExcluded != 412 {
		t.Fatalf("cold_files_excluded = %d, want 412; without it this test is not covering the partly-cold case",
			result.Manifest.ColdFilesExcluded)
	}

	// The precondition that makes a pass meaningful: replace is actually
	// available here. Were m.cluster nil, the restore below would be refused
	// for a reason that has nothing to do with this decision.
	if rig.m.cluster == nil {
		t.Fatal("m.cluster is nil, so mode replace is refused before any gate is reached")
	}

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{
		BackupID:    id,
		RestoreData: true,
		Mode:        RestoreModeReplace,
	}); err != nil {
		t.Fatalf("a replace-mode restore of a partly-cold backup was refused: %v\n"+
			"cold_files_excluded must NOT join the incompleteness refusal in replaceDatabases: a cold object "+
			"has no cluster-manifest entry, so replace cannot delete it, and refusing here would make tiering "+
			"and replace-mode restore mutually exclusive", err)
	}
	// Operation as well as Status: Status is shared across operations, so a
	// bare "completed" is also satisfiable by the BACKUP's own progress.
	if p := rig.m.GetProgress(); p == nil || p.Operation != "restore" || p.Status != "completed" {
		t.Errorf("restore progress = %+v, want operation=restore status=completed", p)
	}
}

// The restore reports the gap it inherited, from the MERGED run view, so a
// routed backup's restore names every leg's gap and not whichever leg
// answered. Separate from the incompleteness WARN: a cold gap is not a defect
// in the backup.
func TestRestoreReportsTheInheritedColdGap(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "prod/cpu/2026/10/07/00/a.parquet", "PAR1")
	rig.write(t, "audit/events/2026/10/07/00/b.parquet", "PAR1")
	rig.m.SetColdCounter(&fakeColdCounter{counts: map[string]int64{"prod": 7, "audit": 11}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{
		BackupID:    result.Manifest.BackupID,
		RestoreData: true,
	}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	p := rig.m.GetProgress()
	if p == nil || p.BackupColdFilesExcluded != 18 {
		t.Errorf("restore progress backup_cold_files_excluded = %v, want 18 (both legs summed)", p)
	}
}

// Matrix row: a FULLY cold database routed to a NON-DEFAULT target. The two
// properties combine here and nowhere else — the database has no data file to
// route, and the target it routes to therefore holds nothing of its own. A leg
// exists anyway because planRun builds one for every routed target from the
// CONFIG rather than from the files (`targetIsRouted`), and routing by bare
// name works because routeKey returns a name with no slash unchanged.
//
// Distinct from TestColdFilesExcludedCountsAFullyColdDatabase, which leaves the
// fully cold database UNROUTED and so only proves it reaches the default leg.
func TestColdFilesExcludedRoutesAFullyColdDatabaseByName(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"archive": "audit"})
	// Only prod has files. archive is entirely cold and routes to "audit",
	// which therefore receives no data file at all.
	rig.write(t, "prod/cpu/2026/10/07/00/a.parquet", "PAR1")
	rig.m.SetColdCounter(&fakeColdCounter{counts: map[string]int64{"archive": 42}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID

	audit := readManifestFile(t, rig.auditDir, id)
	if audit.TotalFiles != 0 {
		t.Fatalf("the audit leg holds %d files, want 0; without that this is not the fully-cold-and-routed case", audit.TotalFiles)
	}
	if audit.ColdFilesExcluded != 42 {
		t.Errorf("audit leg cold_files_excluded = %d, want 42: a fully cold database routes by NAME to the target it is configured for", audit.ColdFilesExcluded)
	}
	if got := audit.ColdFilesExcludedDatabases["archive"]; got != 42 {
		t.Errorf("audit leg breakdown[archive] = %d, want 42", got)
	}
	// And NOT on the default leg, which is where legFor's fallback would put
	// it if the routing map were consulted with the wrong key.
	main := readManifestFile(t, rig.mainDir, id)
	if main.ColdFilesExcluded != 0 {
		t.Errorf("default leg cold_files_excluded = %d, want 0; archive routes to audit", main.ColdFilesExcluded)
	}
	if _, ok := main.ColdFilesExcludedDatabases["archive"]; ok {
		t.Errorf("the default leg names archive: %v", main.ColdFilesExcludedDatabases)
	}
}

// A SCOPED backup of a fully cold database, with the counter wired: the scope
// filter must admit a name with no hot files, and the count must still be
// attributed. #1084 made such a name pass checkScopeKnown; this pins that B3
// does not then drop it.
func TestColdFilesExcludedOnAScopedFullyColdDatabase(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/a.parquet", "PAR1")
	rig.m.SetTierLookup(&fakeTierLookup{rows: map[string]bool{"archive": true}})
	rig.m.SetColdCounter(&fakeColdCounter{counts: map[string]int64{"archive": 9, "prod": 4}})

	result, err := rig.m.CreateBackup(ctx, BackupOptions{Databases: []string{"archive"}})
	if err != nil {
		t.Fatalf("CreateBackup scoped to a fully cold database: %v", err)
	}
	m := readManifestFile(t, rig.mainDir, result.Manifest.BackupID)
	if m.TotalFiles != 0 {
		t.Fatalf("the backup holds %d files, want 0 for a fully cold scope", m.TotalFiles)
	}
	if m.ColdFilesExcluded != 9 {
		t.Errorf("cold_files_excluded = %d, want 9: the scope names a database with nothing hot, and that is exactly the gap worth reporting", m.ColdFilesExcluded)
	}
	if _, ok := m.ColdFilesExcludedDatabases["prod"]; ok {
		t.Errorf("the out-of-scope prod database is named: %v", m.ColdFilesExcludedDatabases)
	}
}

// The ONE-LEG merge must not hand back a map that aliases the leg's own. The
// single-destination path takes a clone shortcut, and the breakdown is the
// manifest's first map field, so an in-place write through the merged view
// would reach into the leg it came from. The two-leg path builds a fresh map
// and was never exposed; this is the branch that was.
func TestOneLegMergeDoesNotAliasTheBreakdownMap(t *testing.T) {
	leg := &Manifest{
		BackupID:                   "b1",
		IsDefaultTarget:            true,
		ColdFilesExcluded:          7,
		ColdFilesExcludedDatabases: map[string]int64{"prod": 7},
	}
	merged := mergeRunManifests([]*Manifest{leg})
	if merged.ColdFilesExcluded != 7 || merged.ColdFilesExcludedDatabases["prod"] != 7 {
		t.Fatalf("one-leg merge lost the figure: %+v", merged.ColdFilesExcludedDatabases)
	}
	merged.ColdFilesExcludedDatabases["prod"] = 999
	merged.ColdFilesExcludedDatabases["injected"] = 1
	if leg.ColdFilesExcludedDatabases["prod"] != 7 {
		t.Errorf("writing through the merged view changed the leg: prod = %d, want 7", leg.ColdFilesExcludedDatabases["prod"])
	}
	if _, ok := leg.ColdFilesExcludedDatabases["injected"]; ok {
		t.Error("a key added to the merged view appeared on the leg: the one-leg clone aliases the map")
	}
}
