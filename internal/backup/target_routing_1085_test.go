package backup

// Tests for per-database backup routing (#1085 stage B2b-2): the routing key,
// per-target legs and manifests, the run index, the merged listing, delete and
// restore across targets, and per-target progress.
//
// Every one of these fails on the base commit BY CONSTRUCTION — config.Load
// refused a second target there, and ManagerConfig took one *Target — but that
// is proved rather than asserted: see the config-layer proof recorded in the
// PR notes, where the removed refusal was put back and the routing tests were
// watched to fail.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestRouteKeyMatchesTheScopeRules pins the routing key against the rules a
// SCOPED backup already applies to the same paths (scope.go).
//
// They have to agree or a scoped backup of a routed database would enumerate
// one set of files and write them to another target. The two unwrapped
// reserved roots are here with their edge cases, because those are the rows
// where "the first path segment" is the wrong answer.
func TestRouteKeyMatchesTheScopeRules(t *testing.T) {
	cases := []struct {
		name string
		path string
		want string
	}{
		{"a data file routes by its database", "prod/cpu/2026/10/07/00/a.parquet", "prod"},
		{"an edge-sync spoke routes by the SPOKE, as a scope names it", "spoke1/prod/cpu/2026/10/07/00/a.parquet", "spoke1"},
		{"a schema anchor routes by the database inside it", "_schema/prod/cpu.parquet", "prod"},
		{"a spoke schema anchor routes by the spoke", "_schema/spoke1/prod/cpu.parquet", "spoke1"},
		// scope.ownsSchemaAnchor requires three segments and answers false for
		// a two-segment key, so this routes by "_schema" and lands on the
		// default. Arc never writes such a key; a foreign one must not be
		// routed as though "prod" were its database.
		{"a two-segment _schema key is not an anchor", "_schema/prod", "_schema"},
		{"compaction state routes by the database part", "_compaction_state/hourly/prod/job-1.json", "prod"},
		{"a parked compaction manifest routes the same way", "_compaction_state/daily/prod/job-1.json.quarantined", "prod"},
		{"spoke compaction state routes by the spoke", "_compaction_state/hourly/spoke1/prod/job-1.json", "spoke1"},
		{"compaction state with no database part stays unrouted", "_compaction_state/hourly/job-1.json", "_compaction_state"},
		{"compaction state with no tier stays unrouted", "_compaction_state/job-1.json", "_compaction_state"},
		// IsReservedRootDir is "starts with _ or .", not an enumeration, so
		// every other reserved root routes by its own segment — which no
		// database name can equal, because config refuses a reserved root as a
		// database name.
		{"a dot-prefixed root routes by itself", ".edge-receive/prod/cpu/a.parquet", ".edge-receive"},
		{"another underscore root routes by itself", "_wal/prod/segment-1", "_wal"},
		// The in-root Iceberg metadata group is classified, never routed, for
		// exactly this reason: the key's first segment is arc_<db>.db only
		// while the warehouse is at its DEFAULT.
		{"an Iceberg namespace directory at the warehouse default", "arc_prod.db/cpu/metadata/v1.metadata.json", "arc_prod.db"},
		{"the same key under a warehouse subdirectory", "wh/arc_prod.db/cpu/metadata/v1.metadata.json", "wh"},
		{"a key with no separator at all", "stray.parquet", "stray.parquet"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := routeKey(c.path); got != c.want {
				t.Errorf("routeKey(%q) = %q, want %q", c.path, got, c.want)
			}
			// And the claim that matters: for a path a scope owns, the routing
			// key is the name the scope matched on.
			if sc, err := newScope([]string{c.want}); err == nil {
				if !sc.ownsPath(c.path) {
					t.Errorf("scope %q does not own %q although routeKey sent it there; routing and scoping must agree", c.want, c.path)
				}
			}
		})
	}
}

// routedRig is a manager with two local targets: "main" (the default) and
// "audit", with the databases in routing sent to "audit".
type routedRig struct {
	m        *Manager
	dataDir  string
	mainDir  string
	auditDir string
	data     *storage.LocalBackend
}

func newRoutedRig(t *testing.T, routing map[string]string) *routedRig {
	t.Helper()
	dataDir := t.TempDir()
	data, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { data.Close() })
	mainDir, auditDir := t.TempDir(), t.TempDir()
	m, err := NewManager(&ManagerConfig{
		DataStorage: data,
		Targets: []Target{
			{Name: "main", Spec: storage.BackendSpec{Type: "local", LocalPath: mainDir}},
			{Name: "audit", Spec: storage.BackendSpec{Type: "local", LocalPath: auditDir}},
		},
		DefaultTarget: "main",
		Routing:       routing,
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewManager with two targets: %v", err)
	}
	return &routedRig{m: m, dataDir: dataDir, mainDir: mainDir, auditDir: auditDir, data: data}
}

func (r *routedRig) write(t *testing.T, path string, body string) {
	t.Helper()
	if err := r.data.Write(context.Background(), path, []byte(body)); err != nil {
		t.Fatalf("seed %s: %v", path, err)
	}
}

// keysUnder lists every file under dir, relative, slash-separated.
func keysUnder(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

// TestARoutedBackupSplitsItsFilesBetweenTargets is the headline: a routed
// database's data files, its schema anchors and its compaction state land on
// its target, and everything else on the default — with a manifest, a sidecar
// and the run index describing it.
func TestARoutedBackupSplitsItsFilesBetweenTargets(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1audit")
	rig.write(t, "_schema/audit/events.parquet", "PAR1anchor")
	rig.write(t, "_compaction_state/hourly/audit/job-1.json", `{"output_path":"audit/events/2026/10/07/00/out.parquet"}`)
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1prod")
	rig.write(t, "_schema/prod/cpu.parquet", "PAR1prodanchor")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID

	auditKeys := keysUnder(t, rig.auditDir)
	mainKeys := keysUnder(t, rig.mainDir)
	wantAudit := []string{
		id + "/data/_compaction_state/hourly/audit/job-1.json",
		id + "/data/_schema/audit/events.parquet",
		id + "/data/audit/events/2026/10/07/00/a.parquet",
		id + "/manifest-files.json",
		id + "/manifest.json",
	}
	if strings.Join(auditKeys, "\n") != strings.Join(wantAudit, "\n") {
		t.Errorf("the audit target holds\n%s\nwant\n%s", strings.Join(auditKeys, "\n"), strings.Join(wantAudit, "\n"))
	}
	wantMain := []string{
		id + "/data/_schema/prod/cpu.parquet",
		id + "/data/prod/cpu/2026/10/07/00/b.parquet",
		id + "/index.json",
		id + "/manifest-files.json",
		id + "/manifest.json",
	}
	if strings.Join(mainKeys, "\n") != strings.Join(wantMain, "\n") {
		t.Errorf("the main target holds\n%s\nwant\n%s", strings.Join(mainKeys, "\n"), strings.Join(wantMain, "\n"))
	}

	// Each manifest describes its OWN slice, and both carry the run-level
	// fields so either one found alone is self-describing.
	audit := readManifestFile(t, rig.auditDir, id)
	main := readManifestFile(t, rig.mainDir, id)
	if audit.TotalFiles != 2 || main.TotalFiles != 2 {
		t.Errorf("per-leg total_files = audit %d, main %d; want 2 and 2 (one data file plus one anchor each)", audit.TotalFiles, main.TotalFiles)
	}
	if audit.CompactionStateFiles != 1 || main.CompactionStateFiles != 0 {
		t.Errorf("compaction_state_files = audit %d, main %d; want 1 and 0", audit.CompactionStateFiles, main.CompactionStateFiles)
	}
	if audit.AuxiliaryFiles != 1 || main.AuxiliaryFiles != 1 {
		t.Errorf("auxiliary_files = audit %d, main %d; want one anchor each", audit.AuxiliaryFiles, main.AuxiliaryFiles)
	}
	if len(audit.Databases) != 1 || audit.Databases[0].Name != "audit" {
		t.Errorf("the audit leg inventories %+v, want only audit", audit.Databases)
	}
	if len(main.Databases) != 1 || main.Databases[0].Name != "prod" {
		t.Errorf("the main leg inventories %+v, want only prod", main.Databases)
	}
	if audit.Target != "audit" || main.Target != "main" {
		t.Errorf("target labels = %q and %q, want audit and main", audit.Target, main.Target)
	}
	if !main.IsDefaultTarget || audit.IsDefaultTarget {
		t.Errorf("is_default_target = main %v, audit %v; want true and false", main.IsDefaultTarget, audit.IsDefaultTarget)
	}
	for _, mf := range []*Manifest{audit, main} {
		if strings.Join(mf.RunTargets, ",") != "audit,main" {
			t.Errorf("%s run_targets = %v, want both, sorted: a manifest found alone must describe the run", mf.Target, mf.RunTargets)
		}
	}

	// The merged run view is what the caller is handed, and it is the sum.
	if result.Manifest.TotalFiles != 4 {
		t.Errorf("the merged manifest total_files = %d, want 4", result.Manifest.TotalFiles)
	}
	if len(result.Manifest.Databases) != 2 {
		t.Errorf("the merged manifest inventories %+v, want both databases", result.Manifest.Databases)
	}

	// The index, on the default target, naming every target and the routing.
	index := readIndexFile(t, rig.mainDir, id)
	if strings.Join(index.Targets, ",") != "audit,main" {
		t.Errorf("index targets = %v, want both", index.Targets)
	}
	if index.DefaultTarget != "main" {
		t.Errorf("index default_target = %q, want main", index.DefaultTarget)
	}
	if index.DatabaseTargets["audit"] != "audit" {
		t.Errorf("index database_targets = %v, want audit routed to audit", index.DatabaseTargets)
	}
	if index.Status != runIndexComplete {
		t.Errorf("index status = %q, want %q", index.Status, runIndexComplete)
	}
}

func readManifestFile(t *testing.T, dir, id string) *Manifest {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, id, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest in %s: %v", dir, err)
	}
	manifest, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func readIndexFile(t *testing.T, dir, id string) *RunIndex {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, id, "index.json"))
	if err != nil {
		t.Fatalf("read index in %s: %v", dir, err)
	}
	index := &RunIndex{}
	if err := json.Unmarshal(data, index); err != nil {
		t.Fatal(err)
	}
	return index
}

// TestARoutedTargetWithNoFilesStillCommits: a routed database whose name is a
// typo produces a leg with total_files: 0 rather than no leg at all. That is
// the operator-facing signal, it keeps "named in run_targets" equal to "has a
// manifest", and it keeps the run restorable on a cluster, which refuses a
// backup with no sidecar.
func TestARoutedTargetWithNoFilesStillCommits(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"aduit": "audit"}) // the typo is the point
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID

	audit := readManifestFile(t, rig.auditDir, id)
	if audit.TotalFiles != 0 {
		t.Errorf("the typo'd target holds %d files, want 0", audit.TotalFiles)
	}
	if _, err := os.Stat(filepath.Join(rig.auditDir, id, sidecarName)); err != nil {
		t.Errorf("the empty leg has no sidecar (%v); a cluster restore refuses a backup without one, so an empty leg must still commit one", err)
	}
	// And the data went to the default, because no real database routes away.
	main := readManifestFile(t, rig.mainDir, id)
	if main.TotalFiles != 1 {
		t.Errorf("the default leg holds %d files, want the one data file", main.TotalFiles)
	}
}

// TestAScopedRunDoesNotTouchATargetItHasNoBusinessWith: a run scoped to
// ["prod"] must not write an empty Scope=[prod], Databases=[] manifest onto
// the audit target, where it would list as a prod backup holding nothing — and
// must not probe, or fail on, a target it has no business touching.
func TestAScopedRunDoesNotTouchATargetItHasNoBusinessWith(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "prod/cpu/2026/10/07/00/a.parquet", "PAR1")
	rig.write(t, "audit/events/2026/10/07/00/b.parquet", "PAR1")

	// The audit target is made unreachable: a scoped run that excludes it must
	// not even probe it.
	rig.m.targets["audit"] = backupTarget{
		backend: unreachableDestination{Backend: rig.m.targets["audit"].backend},
		name:    "audit",
	}

	result, err := rig.m.CreateBackup(ctx, BackupOptions{Databases: []string{"prod"}})
	if err != nil {
		t.Fatalf("a scoped run that touches no routed target must not fail on one: %v", err)
	}
	id := result.Manifest.BackupID
	if got := keysUnder(t, rig.auditDir); len(got) != 0 {
		t.Errorf("the audit target holds %v after a run scoped to prod; it must hold nothing", got)
	}
	main := readManifestFile(t, rig.mainDir, id)
	if strings.Join(main.RunTargets, ",") != "" {
		t.Errorf("run_targets = %v, want empty: one leg means there is nothing to name", main.RunTargets)
	}
	index := readIndexFile(t, rig.mainDir, id)
	if strings.Join(index.Targets, ",") != "main" {
		t.Errorf("index targets = %v, want only main", index.Targets)
	}
}

// TestTheListingUnionsEveryTarget: one entry per backup ID naming every
// destination holding a slice of it.
//
// NEUTRALISATION: replacing the per-target loop in listBackups with a
// default-only listing must make this fail. Verified by doing exactly that —
// see the PR notes.
func TestTheListingUnionsEveryTarget(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1")
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1")

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID

	listing, err := rig.m.ListBackupsDetailed(ctx, false)
	if err != nil {
		t.Fatalf("ListBackupsDetailed: %v", err)
	}
	if len(listing.Backups) != 1 {
		t.Fatalf("listing returned %d entries, want exactly 1 for one backup ID held on two targets: %+v", len(listing.Backups), listing.Backups)
	}
	entry := listing.Backups[0]
	if entry.BackupID != id {
		t.Errorf("entry backup_id = %q, want %q", entry.BackupID, id)
	}
	if strings.Join(entry.Targets, ",") != "audit,main" {
		t.Errorf("entry targets = %v, want both destinations named", entry.Targets)
	}
	if entry.Target != "" {
		t.Errorf("entry target = %q, want empty: the single-target field must not be handed one of several", entry.Target)
	}
	// The summary is the SUM, which is the whole reason the union merges the
	// manifests rather than picking one.
	if entry.TotalFiles != 2 {
		t.Errorf("entry total_files = %d, want 2: the merged run view, not one leg", entry.TotalFiles)
	}
	if len(listing.UnreachableTargets) != 0 || len(listing.IncompleteRuns) != 0 {
		t.Errorf("a healthy run reported unreachable=%v incomplete=%+v", listing.UnreachableTargets, listing.IncompleteRuns)
	}
}

// TestTheListingNamesAnUnreachableTargetAndKeepsGoing: one dead store must not
// hide the backups on the others, and it must be NAMED. The 503 is kept for
// the case it was introduced for: nothing could be read at all.
func TestTheListingNamesAnUnreachableTargetAndKeepsGoing(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1")
	if _, err := rig.m.CreateBackup(ctx, BackupOptions{}); err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}

	rig.m.targets["audit"] = backupTarget{
		backend: unreachableDestination{Backend: rig.m.targets["audit"].backend},
		name:    "audit",
	}
	listing, err := rig.m.ListBackupsDetailed(ctx, false)
	if err != nil {
		t.Fatalf("ListBackupsDetailed with one target down = %v, want the backups the others hold", err)
	}
	if len(listing.Backups) != 1 {
		t.Errorf("listing returned %d entries, want 1: a dead target must not hide the others", len(listing.Backups))
	}
	if strings.Join(listing.UnreachableTargets, ",") != "audit" {
		t.Errorf("unreachable_targets = %v, want [audit]", listing.UnreachableTargets)
	}

	// Every target down IS an error, naming one of them, which is the
	// single-destination contract unchanged.
	rig.m.backupStorage = unreachableDestination{Backend: rig.m.backupStorage}
	if _, err := rig.m.ListBackupsDetailed(ctx, false); err == nil {
		t.Error("ListBackupsDetailed with every target down succeeded, want an error")
	}
}

// TestTheListingSurfacesAnUnreadableTargetAndKeepsOtherTargets: a single
// manifest read failure must not silently remove that backup, while a
// multi-target listing should retain entries it can read from other targets.
func TestTheListingSurfacesAnUnreadableTargetAndKeepsOtherTargets(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1")
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1")
	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID
	rig.write(t, "prod/cpu/2026/10/07/00/c.parquet", "PAR1")
	other, err := rig.m.CreateBackup(ctx, BackupOptions{Databases: []string{"prod"}})
	if err != nil {
		t.Fatalf("CreateBackup for second run: %v", err)
	}

	backend := rig.m.targets["audit"].backend.(*storage.LocalBackend)
	rig.m.targets["audit"] = backupTarget{
		backend: &manifestFaultDestination{
			LocalBackend: backend,
			path:         id + "/manifest.json",
			err:          errors.New("simulated manifest read failure"),
		},
		name: "audit",
	}

	listing, err := rig.m.ListBackupsDetailed(ctx, false)
	if err != nil {
		t.Fatalf("ListBackupsDetailed with one unreadable target: %v", err)
	}
	if len(listing.Backups) != 2 {
		t.Fatalf("listing backups = %+v, want both runs despite one unreadable manifest", listing.Backups)
	}
	byID := make(map[string]BackupSummary, len(listing.Backups))
	for _, summary := range listing.Backups {
		byID[summary.BackupID] = summary
	}
	if partial, ok := byID[id]; !ok || !partial.PartialView {
		t.Errorf("first run = %+v, want a partial view when its audit manifest is unreadable", partial)
	}
	if complete, ok := byID[other.Manifest.BackupID]; !ok || complete.PartialView {
		t.Errorf("second run = %+v, want its complete main-only manifest retained", complete)
	}
	if strings.Join(listing.UnreachableTargets, ",") != "audit" {
		t.Errorf("unreachable_targets = %v, want [audit]", listing.UnreachableTargets)
	}
	if len(listing.IncompleteRuns) != 1 || listing.IncompleteRuns[0].BackupID != id || strings.Join(listing.IncompleteRuns[0].Unknown, ",") != "audit" {
		t.Errorf("incomplete_runs = %+v, want first run marked unknown on [audit]", listing.IncompleteRuns)
	}
}

// TestDeleteSweepsEveryTargetAndReportsTheOnesItCouldNot: a delete reaches
// every destination holding the ID under one lock, and an unreachable one is
// reported AFTER everything reachable was deleted, so a re-run finishes the
// job.
func TestDeleteSweepsEveryTargetAndReportsTheOnesItCouldNot(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1")
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1")
	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID

	if err := rig.m.DeleteBackup(ctx, id); err != nil {
		t.Fatalf("DeleteBackup: %v", err)
	}
	for _, dir := range []string{rig.mainDir, rig.auditDir} {
		if got := keysUnder(t, dir); len(got) != 0 {
			t.Errorf("%s still holds %v after the delete", dir, got)
		}
	}
	// And a second delete is an honest not-found, on every target.
	if err := rig.m.DeleteBackup(ctx, id); err == nil || !errorIsBackupNotFound(err) {
		t.Errorf("deleting a deleted backup = %v, want ErrBackupNotFound", err)
	}

	// With one target unreachable the delete still removes what it can, and
	// the error names the target that still holds objects.
	second, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id = second.Manifest.BackupID
	rig.m.targets["audit"] = backupTarget{
		backend: unreachableListing{Backend: rig.m.targets["audit"].backend},
		name:    "audit",
	}
	err = rig.m.DeleteBackup(ctx, id)
	if err == nil {
		t.Fatal("DeleteBackup with one target unreachable succeeded, want an error naming it")
	}
	if !strings.Contains(err.Error(), "audit") {
		t.Errorf("error = %q, want it to name the target that still holds the backup", err.Error())
	}
	if errorIsBackupNotFound(err) {
		t.Errorf("error = %v, want NOT ErrBackupNotFound: an unreachable target is not an absent backup", err)
	}
	if got := keysUnder(t, rig.mainDir); len(got) != 0 {
		t.Errorf("the reachable target still holds %v; a delete must remove what it can before reporting", got)
	}
}

// TestARoutedBackupRoundTrips: a restore reads every leg and puts every file
// back at its original key.
func TestARoutedBackupRoundTrips(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1audit")
	rig.write(t, "_schema/audit/events.parquet", "PAR1anchor")
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1prod")
	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}

	// Remove every source file, so only a restore can put them back.
	for _, p := range []string{"audit/events/2026/10/07/00/a.parquet", "_schema/audit/events.parquet", "prod/cpu/2026/10/07/00/b.parquet"} {
		if err := rig.data.Delete(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	for _, c := range []struct{ path, body string }{
		{"audit/events/2026/10/07/00/a.parquet", "PAR1audit"},
		{"_schema/audit/events.parquet", "PAR1anchor"},
		{"prod/cpu/2026/10/07/00/b.parquet", "PAR1prod"},
	} {
		data, err := rig.data.Read(ctx, c.path)
		if err != nil {
			t.Errorf("after the restore %s is missing: %v", c.path, err)
			continue
		}
		if string(data) != c.body {
			t.Errorf("%s = %q, want %q", c.path, data, c.body)
		}
	}
}

// TestARestoreRefusesAnUnconfiguredOrUnreachableTarget: the three refusals,
// each BEFORE anything is written.
//
// The alternative — restore the targets that answer — would report success
// over a set it did not restore, and in replace mode would delete the live
// files of a database whose backup bytes are on the target it could not read.
func TestARestoreRefusesAnUnconfiguredOrUnreachableTarget(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1audit")
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1prod")
	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID
	if err := rig.data.Delete(ctx, "prod/cpu/2026/10/07/00/b.parquet"); err != nil {
		t.Fatal(err)
	}

	t.Run("a target this node has not configured", func(t *testing.T) {
		// The audit target is dropped from the configuration, as it would be
		// on a node that never had it.
		m := rig.m
		saved := m.targets
		t.Cleanup(func() { m.targets = saved })
		m.targets = nil
		_, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: id, RestoreData: true})
		if err == nil {
			t.Fatal("the restore succeeded with a leg on an unconfigured target, want a refusal")
		}
		for _, want := range []string{"no backup target named", "audit"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), want)
			}
		}
		if _, statErr := rig.data.Exists(ctx, "prod/cpu/2026/10/07/00/b.parquet"); statErr != nil {
			t.Fatal(statErr)
		}
		if ok, _ := rig.data.Exists(ctx, "prod/cpu/2026/10/07/00/b.parquet"); ok {
			t.Error("the refused restore wrote the default leg's files; the refusal must come before any write")
		}
	})

	t.Run("a configured target that will not answer", func(t *testing.T) {
		m := rig.m
		saved := m.targets["audit"]
		t.Cleanup(func() { m.targets["audit"] = saved })
		m.targets["audit"] = backupTarget{backend: unreachableDestination{Backend: saved.backend}, name: "audit"}
		_, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: id, RestoreData: true})
		if err == nil {
			t.Fatal("the restore succeeded with an unreachable leg, want a refusal")
		}
		if !strings.Contains(err.Error(), "would not answer") || !strings.Contains(err.Error(), "audit") {
			t.Errorf("error = %q, want it to say which target would not answer", err.Error())
		}
	})

	t.Run("a reachable target with no manifest for the id", func(t *testing.T) {
		// The state "die after target X's manifest" produces: readManifest
		// answers ErrBackupNotFound here, which is what makes this
		// distinguishable from a transport failure.
		if err := os.Remove(filepath.Join(rig.auditDir, id, "manifest.json")); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.WriteFile(filepath.Join(rig.auditDir, id, "manifest.json"), []byte("{}"), 0o600); err != nil {
				t.Fatal(err)
			}
		})
		_, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: id, RestoreData: true})
		if err == nil {
			t.Fatal("the restore succeeded over a leg that never committed, want a refusal")
		}
		if !strings.Contains(err.Error(), "holds no manifest") || !strings.Contains(err.Error(), "audit") {
			t.Errorf("error = %q, want it to say the target holds no manifest for the id", err.Error())
		}
	})
}

// TestASingleTargetBackupStillRestoresWithAnUnknownTargetName: Manifest.Target
// is a LABEL, never a lookup, and stage B2b-1 promised that a backup whose
// Target names a target this node does not have still restores. RunTargets
// inherits the rule, so the resolution above must apply only to a run that
// spans SEVERAL targets.
func TestASingleTargetBackupStillRestoresWithAnUnknownTargetName(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m := newBackupAt(t, dir, "")
	result, err := m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	// Rewrite the manifest as a backup taken against a target this node has
	// never heard of, which is what a renamed target looks like.
	manifest := readManifestFile(t, dir, result.Manifest.BackupID)
	manifest.Target = "a_target_this_node_never_had"
	data, err := MarshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, result.Manifest.BackupID, "manifest.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Errorf("RestoreBackup = %v; a single-destination backup must restore whatever its Target label says", err)
	}
}

// TestAB2b1BackupListsAndRestores: an operator whose single B2b-1 target
// becomes a ROUTED target has manifests there, no index, and nothing on a
// newly added default. The union listing has to find it and a restore has to
// read it.
func TestAB2b1BackupListsAndRestores(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})

	// A backup as stage B2b-1 wrote one: a manifest and a sidecar on what is
	// now the audit target, no index, no run_targets.
	const id = "backup-20260101-010101-aaaaaaaa"
	legacy := &Manifest{
		Version: "dev", BackupID: id, CreatedAt: time.Now().UTC().Add(-72 * time.Hour),
		BackupType: "full", Target: "audit", TotalFiles: 1, TotalSizeBytes: 4,
		Databases: []DatabaseInfo{{Name: "audit", FileCount: 1, SizeBytes: 4,
			Measurements: []MeasurementInfo{{Name: "events", FileCount: 1, SizeBytes: 4}}}},
	}
	data, err := MarshalManifest(legacy)
	if err != nil {
		t.Fatal(err)
	}
	writeManifest(t, rig.auditDir, id, data)
	if err := os.WriteFile(filepath.Join(rig.auditDir, id, sidecarName), []byte(`{"version":1,"from_cluster_manifest":false,"files":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(rig.auditDir, id, "data", "audit", "events", "2026", "10", "07", "00"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rig.auditDir, id, "data", "audit", "events", "2026", "10", "07", "00", "a.parquet"), []byte("PAR1"), 0o600); err != nil {
		t.Fatal(err)
	}

	listing, err := rig.m.ListBackupsDetailed(ctx, false)
	if err != nil {
		t.Fatalf("ListBackupsDetailed: %v", err)
	}
	if len(listing.Backups) != 1 || listing.Backups[0].BackupID != id {
		t.Fatalf("listing = %+v, want the pre-index backup on the routed target", listing.Backups)
	}
	if listing.Backups[0].Target != "audit" {
		t.Errorf("entry target = %q, want audit: with one destination the single-target field stays set", listing.Backups[0].Target)
	}
	// No index and no run_targets: the targets it was found at are the whole
	// run, so it is COMPLETE, not an aborted one.
	if len(listing.IncompleteRuns) != 0 {
		t.Errorf("incomplete_runs = %+v, want none: a manifest with no run_targets is a single-destination backup", listing.IncompleteRuns)
	}
	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: id, RestoreData: true}); err != nil {
		t.Fatalf("restoring a pre-index backup held only on a routed target: %v", err)
	}
	if body, err := rig.data.Read(ctx, "audit/events/2026/10/07/00/a.parquet"); err != nil || string(body) != "PAR1" {
		t.Errorf("restored file = %q, %v; want the backup's bytes", body, err)
	}
}

// TestAnAbortedRunIsEnumerableAndDeletable: the hole stage B2b-1 recorded. A
// run that died after the index leaves objects nothing used to enumerate; with
// the index the listing names it and the delete sweeps it.
func TestAnAbortedRunIsEnumerableAndDeletable(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})

	// A run that wrote its index and one data object and then died: the index
	// is the only thing that names the target holding the leftovers.
	const id = "backup-20260101-010101-bbbbbbbb"
	index := &RunIndex{
		BackupID: id, CreatedAt: time.Now().UTC().Add(-72 * time.Hour),
		DefaultTarget: "main", Targets: []string{"audit", "main"},
		DatabaseTargets: map[string]string{"audit": "audit"}, Status: runIndexStarted,
	}
	body, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(rig.mainDir, id), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rig.mainDir, id, "index.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(rig.auditDir, id, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rig.auditDir, id, "data", "orphan.parquet"), []byte("PAR1"), 0o600); err != nil {
		t.Fatal(err)
	}

	listing, err := rig.m.ListBackupsDetailed(ctx, false)
	if err != nil {
		t.Fatalf("ListBackupsDetailed: %v", err)
	}
	if len(listing.Backups) != 0 {
		t.Errorf("backups = %+v, want none: a run with no manifest anywhere is not a backup", listing.Backups)
	}
	if len(listing.IncompleteRuns) != 1 {
		t.Fatalf("incomplete_runs = %+v, want exactly the aborted run", listing.IncompleteRuns)
	}
	run := listing.IncompleteRuns[0]
	if run.BackupID != id || run.State != IncompleteRunAborted {
		t.Errorf("incomplete run = %+v, want %s reported as %q", run, id, IncompleteRunAborted)
	}
	if strings.Join(run.Missing, ",") != "audit,main" {
		t.Errorf("missing targets = %v, want both: neither committed a manifest", run.Missing)
	}
	if err := rig.m.DeleteBackup(ctx, id); err != nil {
		t.Fatalf("DeleteBackup on an aborted run: %v", err)
	}
	for _, dir := range []string{rig.mainDir, rig.auditDir} {
		if got := keysUnder(t, dir); len(got) != 0 {
			t.Errorf("%s still holds %v after deleting the aborted run", dir, got)
		}
	}
}

// TestARunStillInFlightIsNotReportedAborted is the cluster-reader row.
//
// ListBackups has no primary-writer gate, so a READER listing a shared
// destination has no m.active for the primary's live run and would call it
// aborted — a visible false alarm where the behaviour before this was a silent
// skip. A run whose index landed within backup.operation_timeout may still be
// running somewhere, and says so.
func TestARunStillInFlightIsNotReportedAborted(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	const id = "backup-20260101-010101-cccccccc"
	index := &RunIndex{
		BackupID: id, CreatedAt: time.Now().UTC(), // just now: inside the window
		DefaultTarget: "main", Targets: []string{"audit", "main"}, Status: runIndexStarted,
	}
	body, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(rig.mainDir, id), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rig.mainDir, id, "index.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}

	// m.active is nil here, exactly as it is on a reader node.
	if p := rig.m.GetProgress(); p != nil {
		t.Fatalf("this manager has a live operation (%+v); the point of this test is that it does not", p)
	}
	listing, err := rig.m.ListBackupsDetailed(ctx, false)
	if err != nil {
		t.Fatalf("ListBackupsDetailed: %v", err)
	}
	if len(listing.IncompleteRuns) != 1 {
		t.Fatalf("incomplete_runs = %+v, want the one run", listing.IncompleteRuns)
	}
	if got := listing.IncompleteRuns[0].State; got != IncompleteRunPossiblyRunning {
		t.Errorf("state = %q, want %q: a run inside the operation timeout may still be running on another node", got, IncompleteRunPossiblyRunning)
	}
}

// TestAPartiallyCommittedRunIsBothABackupAndAnIncompleteRun: a commit-phase
// failure leaves some legs committed. The ID then appears in backups AND in
// incomplete_runs — the two states are not mutually exclusive.
//
// The entry names the targets the RUN recorded, not the ones the listing could
// read, with partial_view set and the single-target field cleared: a client
// that reads target= must never be handed one of several, and the counts in
// such an entry are summed over the readable legs only and so are a lower
// bound. incomplete_runs says which target is unaccounted for.
func TestAPartiallyCommittedRunIsBothABackupAndAnIncompleteRun(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1")
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1")
	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID
	// The audit leg's manifest never landed, which is what a commit-phase
	// failure on it looks like. Backdated so the run is past the in-flight
	// window.
	if err := os.Remove(filepath.Join(rig.auditDir, id, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	main := readManifestFile(t, rig.mainDir, id)
	main.CreatedAt = time.Now().UTC().Add(-72 * time.Hour)
	data, err := MarshalManifest(main)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rig.mainDir, id, "manifest.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}

	listing, err := rig.m.ListBackupsDetailed(ctx, false)
	if err != nil {
		t.Fatalf("ListBackupsDetailed: %v", err)
	}
	if len(listing.Backups) != 1 || listing.Backups[0].BackupID != id {
		t.Fatalf("backups = %+v, want the ID that committed on one target", listing.Backups)
	}
	entry := listing.Backups[0]
	if strings.Join(entry.Targets, ",") != "audit,main" {
		t.Errorf("entry targets = %v, want every target the run named", entry.Targets)
	}
	if entry.Target != "" {
		t.Errorf("entry target = %q, want it cleared for a backup that spans two targets", entry.Target)
	}
	if !entry.PartialView {
		t.Error("entry partial_view = false, want true: one of the two legs could not be accounted for")
	}
	if len(listing.IncompleteRuns) != 1 {
		t.Fatalf("incomplete_runs = %+v, want the partially committed run", listing.IncompleteRuns)
	}
	run := listing.IncompleteRuns[0]
	if strings.Join(run.Missing, ",") != "audit" || strings.Join(run.Committed, ",") != "main" {
		t.Errorf("incomplete run = %+v, want audit missing and main committed", run)
	}
	if run.State != IncompleteRunAborted {
		t.Errorf("state = %q, want %q for a run past the operation timeout", run.State, IncompleteRunAborted)
	}
}

// TestTheMergedRunViewFeedsCheckScopedReplace: a scoped backup routed to one
// target, read from the DEFAULT target's manifest, has Scope=[audit] and
// Databases=[] — which CheckScopedReplace refuses as "holds no data files".
// The merged view is what stops that.
func TestTheMergedRunViewFeedsCheckScopedReplace(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1")
	result, err := rig.m.CreateBackup(ctx, BackupOptions{Databases: []string{"audit"}})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID

	// The default leg really does hold nothing for the scope, which is the
	// trap: read alone it would refuse the restore.
	main := readManifestFile(t, rig.mainDir, id)
	if len(main.Databases) != 0 {
		t.Fatalf("the default leg inventories %+v; this test needs it to hold nothing", main.Databases)
	}
	if err := CheckScopedReplace(RestoreModeReplace, main); err == nil {
		t.Fatal("CheckScopedReplace accepted the default leg's manifest alone; the premise of this test is that it refuses it")
	}

	merged, err := rig.m.GetBackup(ctx, id)
	if err != nil {
		t.Fatalf("GetBackup: %v", err)
	}
	if err := CheckScopedReplace(RestoreModeReplace, merged); err != nil {
		t.Errorf("CheckScopedReplace on the merged run view = %v, want nil: the backup does hold audit data files", err)
	}
	if strings.Join(merged.Scope, ",") != "audit" {
		t.Errorf("merged scope = %v, want [audit]", merged.Scope)
	}
}

// TestGetBackupDetailCarriesThePerTargetSlices: GET /api/v1/backup/:id answers
// the merged view at the top level, where a one-manifest reader already looks
// for it, with the per-target slices beside it.
func TestGetBackupDetailCarriesThePerTargetSlices(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1")
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1")
	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}

	detail, err := rig.m.GetBackupDetail(ctx, result.Manifest.BackupID)
	if err != nil {
		t.Fatalf("GetBackupDetail: %v", err)
	}
	body, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	// The manifest fields are promoted, so an existing client still reads the
	// response as a manifest.
	if decoded["total_files"] != float64(2) {
		t.Errorf("total_files = %v, want the merged 2 at the TOP level", decoded["total_files"])
	}
	if decoded["backup_id"] == nil {
		t.Error("backup_id is absent; the embedded manifest must stay promoted")
	}
	targets, ok := decoded["targets"].([]any)
	if !ok || len(targets) != 2 {
		t.Fatalf("targets = %v, want one entry per leg", decoded["targets"])
	}
	first, _ := targets[0].(map[string]any)
	if first["name"] != "main" || first["is_default"] != true {
		t.Errorf("the first target entry = %v, want the default leg first", first)
	}
}

// TestPerTargetProgressIsReplacedWholesale is the -race row.
//
// setProgress publishes a SHALLOW copy, so every published snapshot shares the
// Targets slice header: an element written after publication races every
// reader of /status. Progress.Targets is therefore rebuilt on each publish,
// never mutated in place, and this hammers GetProgress while a routed backup
// runs.
func TestPerTargetProgressIsReplacedWholesale(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	for i := 0; i < 40; i++ {
		rig.write(t, fmt.Sprintf("audit/events/2026/10/07/00/a%d.parquet", i), "PAR1audit")
		rig.write(t, fmt.Sprintf("prod/cpu/2026/10/07/00/b%d.parquet", i), "PAR1prod")
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if p := rig.m.GetProgress(); p != nil {
					// Read every field a /status marshal would touch.
					for _, tp := range p.Targets {
						_ = tp.Name + tp.Status
						_ = tp.Files + tp.Bytes + tp.Skipped
					}
				}
			}
		}()
	}
	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if result.Manifest.TotalFiles != 80 {
		t.Errorf("total_files = %d, want 80", result.Manifest.TotalFiles)
	}
	p := rig.m.GetProgress()
	if p == nil || len(p.Targets) != 2 {
		t.Fatalf("final progress = %+v, want one entry per target", p)
	}
	byName := map[string]TargetProgress{}
	for _, tp := range p.Targets {
		byName[tp.Name] = tp
	}
	for _, name := range []string{"main", "audit"} {
		tp, ok := byName[name]
		if !ok {
			t.Errorf("progress has no entry for %q", name)
			continue
		}
		if tp.Files != 40 {
			t.Errorf("%s copied %d files, want 40", name, tp.Files)
		}
		if tp.Status != legCommitted {
			t.Errorf("%s status = %q, want %q", name, tp.Status, legCommitted)
		}
	}
	if !byName["main"].IsDefault || byName["audit"].IsDefault {
		t.Error("is_default is on the wrong leg")
	}
}

// TestASingleDestinationRunPublishesNoPerTargetProgress: with one destination
// the run-wide counters describe it in full, and an array of one would be
// noise on every existing deployment's /status.
func TestASingleDestinationRunPublishesNoPerTargetProgress(t *testing.T) {
	ctx := context.Background()
	m := newBackupAt(t, t.TempDir(), "")
	if _, err := m.CreateBackup(ctx, BackupOptions{}); err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if p := m.GetProgress(); p == nil || len(p.Targets) != 0 {
		t.Errorf("progress targets = %+v, want none for a single destination", p.Targets)
	}
}

// TestAFailedIndexWriteFailsTheRunBeforeAnyCopy: the index is the first write a
// run makes, and a failure there is clean — nothing has been copied, so there
// is nothing to compensate.
func TestAFailedIndexWriteFailsTheRunBeforeAnyCopy(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1")
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1")

	// Refuse ONLY the index write, so this tests the index and not some other
	// call site that happens to come first.
	rig.m.backupStorage = writeRefusingDestination{
		Backend: rig.m.backupStorage,
		allow:   func(path string) bool { return !strings.HasSuffix(path, "/index.json") },
	}
	_, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err == nil {
		t.Fatal("CreateBackup succeeded although the index write was refused")
	}
	if !strings.Contains(err.Error(), "backup index") {
		t.Errorf("error = %q, want it to name the index write", err.Error())
	}
	if !strings.Contains(err.Error(), "backup target main") {
		t.Errorf("error = %q, want it to name the destination", err.Error())
	}
	// Nothing copied, anywhere: the index precedes the first copy.
	for _, dir := range []string{rig.mainDir, rig.auditDir} {
		if got := keysUnder(t, dir); len(got) != 0 {
			t.Errorf("%s holds %v after a refused index write; the run must fail before any copy", dir, got)
		}
	}
	if p := rig.m.GetProgress(); p == nil || p.Status != "failed" {
		t.Errorf("progress = %+v, want a failed status", p)
	}
}

// TestAnUnreachableRoutedTargetFailsTheRunBeforeAnyCopy: every leg is probed
// before anything is written, so one unreachable target cannot leave the others
// holding objects an operator then has to clean up.
func TestAnUnreachableRoutedTargetFailsTheRunBeforeAnyCopy(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1")
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1")
	rig.m.targets["audit"] = backupTarget{
		backend: unreachableDestination{Backend: rig.m.targets["audit"].backend},
		name:    "audit",
	}

	_, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err == nil {
		t.Fatal("CreateBackup succeeded with an unreachable routed target")
	}
	if !strings.Contains(err.Error(), "backup target audit") || !strings.Contains(err.Error(), "did not answer") {
		t.Errorf("error = %q, want it to name the target that did not answer", err.Error())
	}
	for _, dir := range []string{rig.mainDir, rig.auditDir} {
		if got := keysUnder(t, dir); len(got) != 0 {
			t.Errorf("%s holds %v; the probe must fail the run before any write", dir, got)
		}
	}
}

// TestTheKeyLengthLimitIsPerTarget: two targets with different object-key
// prefixes have different usable source-key lengths, and the skip message has
// to name whose limit was hit.
func TestTheKeyLengthLimitIsPerTarget(t *testing.T) {
	dataDir := t.TempDir()
	data, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { data.Close() })
	m, err := NewManager(&ManagerConfig{
		DataStorage: data,
		Targets: []Target{
			{Name: "main", Spec: storage.BackendSpec{Type: "local", LocalPath: t.TempDir()}},
			{Name: "audit", Spec: storage.BackendSpec{Type: "local", LocalPath: t.TempDir()}, KeyPrefix: "arc/backups/"},
		},
		DefaultTarget: "main",
		Routing:       map[string]string{"audit": "audit"},
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := m.defaultDestination().maxSourceKeyBytes(); got != 982 {
		t.Errorf("the default target max_source_key_bytes = %d, want 982", got)
	}
	if got := m.destination("audit").maxSourceKeyBytes(); got != 970 {
		t.Errorf("the routed target max_source_key_bytes = %d, want 970 (982 minus the 12-byte prefix \"arc/backups/\")", got)
	}
	// And the run-wide ratio message quotes the SMALLEST, because "no backup
	// destination key can hold" is only true of every target there.
	run := m.planRun("backup-20260101-010101-dddddddd", time.Now(), &Progress{}, nil)
	if got := run.minMaxSourceKeyBytes(); got != 970 {
		t.Errorf("minMaxSourceKeyBytes = %d, want the smallest of the run's targets (970)", got)
	}
}

// TestNewManagerLeavesTargetsNilForEverySingleDestinationShape is the
// compatibility net for the twelve tests that swap m.backupStorage.
//
// A populated map, or a cached default destination, would route past their
// fakes and leave every one of them green against a broken implementation.
func TestNewManagerLeavesTargetsNilForEverySingleDestinationShape(t *testing.T) {
	data, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { data.Close() })

	t.Run("no target at all", func(t *testing.T) {
		m, err := NewManager(&ManagerConfig{DataStorage: data, BackupPath: t.TempDir(), Logger: zerolog.Nop()})
		if err != nil {
			t.Fatal(err)
		}
		if m.targets != nil || m.routing != nil {
			t.Errorf("targets = %v, routing = %v; want both nil", m.targets, m.routing)
		}
	})

	t.Run("exactly one target", func(t *testing.T) {
		m, err := NewManager(&ManagerConfig{
			DataStorage:   data,
			Targets:       []Target{{Name: "audit", Spec: storage.BackendSpec{Type: "local", LocalPath: t.TempDir()}}},
			DefaultTarget: "audit",
			// Routing against a single destination is a no-op and is dropped
			// with the map it would address, so a lookup never has to
			// special-case the default name.
			Routing: map[string]string{"prod": "audit"},
			Logger:  zerolog.Nop(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if m.targets != nil || m.routing != nil {
			t.Errorf("targets = %v, routing = %v; want both nil with one target", m.targets, m.routing)
		}
		if m.TargetName() != "audit" {
			t.Errorf("TargetName() = %q, want audit", m.TargetName())
		}
	})

	t.Run("the default destination re-reads the flat fields on every call", func(t *testing.T) {
		m, err := NewManager(&ManagerConfig{DataStorage: data, BackupPath: t.TempDir(), Logger: zerolog.Nop()})
		if err != nil {
			t.Fatal(err)
		}
		before := m.defaultDestination().backend
		swapped := unreachableDestination{Backend: m.backupStorage}
		m.backupStorage = swapped
		after := m.defaultDestination().backend
		if after == before {
			t.Error("defaultDestination() returned the pre-swap backend; a cached destination routes past every test fake")
		}
		m.targetName = "renamed"
		if got := m.defaultDestination().describe(); got != "backup target renamed" {
			t.Errorf("describe() = %q, want it to re-read targetName", got)
		}
	})

	t.Run("a routing entry naming an unconfigured target is refused", func(t *testing.T) {
		_, err := NewManager(&ManagerConfig{
			DataStorage: data,
			Targets: []Target{
				{Name: "main", Spec: storage.BackendSpec{Type: "local", LocalPath: t.TempDir()}},
				{Name: "audit", Spec: storage.BackendSpec{Type: "local", LocalPath: t.TempDir()}},
			},
			DefaultTarget: "main",
			Routing:       map[string]string{"prod": "nowhere"},
			Logger:        zerolog.Nop(),
		})
		if err == nil {
			t.Fatal("NewManager accepted a routing entry naming a target it was not given")
		}
		if !strings.Contains(err.Error(), "nowhere") {
			t.Errorf("error = %q, want it to name the unconfigured target", err.Error())
		}
	})
}

// TestTheRoutedRecheckDeletesFromTheLegThatHoldsTheFile is decision 7 of the
// B2b-2 plan, and the reason the end-of-run cluster re-check is a step of its
// own rather than part of the copy phase.
//
// recheckClusterManifest both WRITES and DELETES after every leg has copied. Its
// delete removes a copied file the cluster has stopped listing — the inputs of a
// compaction whose phase 2 landed during the run above all. Sent to the DEFAULT
// destination it would REPORT SUCCESS while the file stayed on the routed
// target: LocalBackend.Delete returns nil for a missing key and S3's
// DeleteObject is idempotent. That is precisely the double-serve #1083 and #930
// exist to prevent, arrived at through a delete that looks fine.
func TestTheRoutedRecheckDeletesFromTheLegThatHoldsTheFile(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	const auditFile = "audit/events/2026/10/07/00/a.parquet"
	const prodFile = "prod/cpu/2026/10/07/00/b.parquet"
	rig.write(t, auditFile, "PAR1audit")
	rig.write(t, prodFile, "PAR1prod")

	// Both files are registered at the first snapshot and the ROUTED one has
	// left the manifest by the second: exactly the compaction-phase-2 window.
	cm := &fakeClusterManifest{entries: []ManifestFile{
		{Path: auditFile, Database: "audit", Measurement: "events", SizeBytes: 9},
		{Path: prodFile, Database: "prod", Measurement: "cpu", SizeBytes: 8},
	}}
	cm.onRead = func(call int) {
		if call != 2 {
			return
		}
		cm.mu.Lock()
		defer cm.mu.Unlock()
		cm.entries = []ManifestFile{{Path: prodFile, Database: "prod", Measurement: "cpu", SizeBytes: 8}}
	}
	rig.m.SetClusterManifest(cm)

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID

	// The copy is gone from the ROUTED target, which is where it was written.
	if got := keysUnder(t, rig.auditDir); len(got) != 2 {
		t.Errorf("the audit target holds %v; want only its manifest and sidecar, the data file having been removed again", got)
	}
	for _, key := range keysUnder(t, rig.auditDir) {
		if strings.Contains(key, "a.parquet") {
			t.Errorf("%s survived the re-check on the routed target; an unrouted delete reports success while the file stays, which is the double-serve this prevents", key)
		}
	}
	// And the prod file, which never left the manifest, is untouched on the
	// default target.
	if !existsIn(t, rig.m.backupStorage, id+"/data/"+prodFile) {
		t.Error("the default leg's data file was removed; only the file that left the manifest may be")
	}

	// The per-leg manifests account for it: the routed leg lost a file and
	// says so, the default leg is unaffected.
	audit := readManifestFile(t, rig.auditDir, id)
	main := readManifestFile(t, rig.mainDir, id)
	if audit.LeftManifestDuringRun != 1 || audit.TotalFiles != 0 {
		t.Errorf("the audit leg = left_manifest_during_run %d, total_files %d; want 1 and 0", audit.LeftManifestDuringRun, audit.TotalFiles)
	}
	if main.LeftManifestDuringRun != 0 || main.TotalFiles != 1 {
		t.Errorf("the main leg = left_manifest_during_run %d, total_files %d; want 0 and 1", main.LeftManifestDuringRun, main.TotalFiles)
	}
	if result.Manifest.LeftManifestDuringRun != 1 {
		t.Errorf("the merged run view left_manifest_during_run = %d, want 1", result.Manifest.LeftManifestDuringRun)
	}
	// The routed leg's sidecar must not still describe a file the backup no
	// longer holds, or a cluster restore would refuse the run as damaged.
	entries, ok, err := rig.m.readSidecar(ctx, rig.m.destination("audit"), id)
	if err != nil || !ok {
		t.Fatalf("readSidecar on the routed leg = %v, %v", ok, err)
	}
	if _, stale := entries[auditFile]; stale {
		t.Error("the routed leg's sidecar still lists the file that left the manifest")
	}
}

// TestALateArrivalIsCopiedIntoItsOwnLeg is the other half of the routed
// re-check: a file whose registration had not reached this node at the first
// snapshot is copied at the end, into the leg its own path routes to and not
// into the default one.
func TestALateArrivalIsCopiedIntoItsOwnLeg(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	const auditFile = "audit/events/2026/10/07/00/a.parquet"
	const prodFile = "prod/cpu/2026/10/07/00/b.parquet"
	rig.write(t, auditFile, "PAR1audit")
	rig.write(t, prodFile, "PAR1prod")

	// The routed file is listed but UNREGISTERED at the first snapshot, and
	// registered by the second.
	cm := &fakeClusterManifest{entries: []ManifestFile{
		{Path: prodFile, Database: "prod", Measurement: "cpu", SizeBytes: 8},
	}}
	cm.onRead = func(call int) {
		if call != 2 {
			return
		}
		cm.mu.Lock()
		defer cm.mu.Unlock()
		cm.entries = append(cm.entries, ManifestFile{Path: auditFile, Database: "audit", Measurement: "events", SizeBytes: 9})
	}
	rig.m.SetClusterManifest(cm)

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID

	if !existsIn(t, rig.m.destination("audit").backend, id+"/data/"+auditFile) {
		t.Error("the late arrival is not on its own target; a late copy must route like every other")
	}
	if existsIn(t, rig.m.backupStorage, id+"/data/"+auditFile) {
		t.Error("the late arrival landed on the DEFAULT target as well; routing has to hold at the end of the run too")
	}
	audit := readManifestFile(t, rig.auditDir, id)
	if audit.TotalFiles != 1 || audit.UnregisteredSkipped != 0 {
		t.Errorf("the audit leg = total_files %d, unregistered_skipped %d; want 1 and 0", audit.TotalFiles, audit.UnregisteredSkipped)
	}
}

// TestAPartiallyCommittedRunIsReportedOnceWhicheverLegCommitted: the same run
// as the test above with the legs swapped — the DEFAULT target is the one that
// did not commit.
//
// It is here because the fan-in walks targets in a fixed order, and a run whose
// committed leg came SECOND was reported twice: once from its manifest and
// again from the index, because the index pass looked at IDs collected before
// the later target's manifest had been seen. Whether the committed leg comes
// first or second must not decide the answer.
func TestAPartiallyCommittedRunIsReportedOnceWhicheverLegCommitted(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1")
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1")
	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID
	// The DEFAULT leg's manifest never landed: the index is on that target,
	// and the only committed manifest is on the one listed second.
	if err := os.Remove(filepath.Join(rig.mainDir, id, "manifest.json")); err != nil {
		t.Fatal(err)
	}

	listing, err := rig.m.ListBackupsDetailed(ctx, false)
	if err != nil {
		t.Fatalf("ListBackupsDetailed: %v", err)
	}
	if len(listing.Backups) != 1 {
		t.Errorf("backups = %+v, want the one ID", listing.Backups)
	}
	if len(listing.IncompleteRuns) != 1 {
		t.Fatalf("incomplete_runs = %+v, want exactly ONE entry for one run", listing.IncompleteRuns)
	}
	run := listing.IncompleteRuns[0]
	if strings.Join(run.Missing, ",") != "main" || strings.Join(run.Committed, ",") != "audit" {
		t.Errorf("incomplete run = %+v, want main missing and audit committed", run)
	}
}

// TestAnUnreachableTargetIsNotReportedAsAMissingManifest: "missing" has to mean
// the listing LOOKED and found no manifest.
//
// Counting an unreachable target as missing would report a perfectly complete
// backup as a run that did not finish, every time a store was briefly down.
// The honest report is unreachable_targets, and unknown_targets on the entry.
func TestAnUnreachableTargetIsNotReportedAsAMissingManifest(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})
	rig.write(t, "audit/events/2026/10/07/00/a.parquet", "PAR1")
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1")
	if _, err := rig.m.CreateBackup(ctx, BackupOptions{}); err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	// A COMPLETE run, with one of its targets now unreachable.
	rig.m.targets["audit"] = backupTarget{
		backend: unreachableDestination{Backend: rig.m.targets["audit"].backend},
		name:    "audit",
	}

	listing, err := rig.m.ListBackupsDetailed(ctx, false)
	if err != nil {
		t.Fatalf("ListBackupsDetailed: %v", err)
	}
	if len(listing.IncompleteRuns) != 1 {
		t.Fatalf("incomplete_runs = %+v, want one entry reporting what could not be established", listing.IncompleteRuns)
	}
	run := listing.IncompleteRuns[0]
	if len(run.Missing) != 0 {
		t.Errorf("missing_targets = %v, want empty: the listing could not look, so nothing is known to be missing", run.Missing)
	}
	if strings.Join(run.Unknown, ",") != "audit" {
		t.Errorf("unknown_targets = %v, want [audit]", run.Unknown)
	}
	if strings.Join(listing.UnreachableTargets, ",") != "audit" {
		t.Errorf("unreachable_targets = %v, want [audit]", listing.UnreachableTargets)
	}
	// Nothing is known to be missing, so neither "aborted" nor
	// "possibly_in_flight" is a true statement about this run.
	if run.State != IncompleteRunUndetermined {
		t.Errorf("state = %q, want %q: a complete backup with one store briefly down is not a run that did not finish", run.State, IncompleteRunUndetermined)
	}

	// And the BACKUP entry has to say the same thing. It names the targets the
	// RUN recorded, clears the single-target field, and admits the counts are
	// summed over fewer legs than that — a client handed target="main" and one
	// file for a two-target, two-file backup has been told something false.
	if len(listing.Backups) != 1 {
		t.Fatalf("backups = %+v, want the one complete backup", listing.Backups)
	}
	entry := listing.Backups[0]
	if strings.Join(entry.Targets, ",") != "audit,main" {
		t.Errorf("entry targets = %v, want both targets the run named", entry.Targets)
	}
	if entry.Target != "" {
		t.Errorf("entry target = %q, want it cleared: the backup spans two targets", entry.Target)
	}
	if !entry.PartialView {
		t.Error("entry partial_view = false, want true: one leg could not be read, so the counts are a lower bound")
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Fold-in round: the anchor leg, the inert target, the credential warning and
// the routed replace-mode restore.
// ───────────────────────────────────────────────────────────────────────────

// migratedRig is the shape every anchor-leg test needs: a backup that was
// written when ONE target existed and was therefore the default, read back on
// a node that has since added a second target and re-pointed
// backup.default_target at it.
//
// The dirs are reused across the two managers, so nothing is copied: the
// manifest that says it is the whole run sits on what is now a ROUTED target,
// and the current default holds nothing at all.
type migratedRig struct {
	m                     *Manager
	oldDir, newDefaultDir string
	sqlitePath            string
	configPath            string
	warehouse             string
}

// newMigratedRig writes a stage B2b-1 backup on target "old" — manifest,
// SQLite metadata, arc.toml and an outside-root Iceberg warehouse, all on that
// one target, with no RunTargets and no IsDefaultTarget, which is exactly what
// a single-destination run writes — and returns a Manager whose DEFAULT is a
// different, empty target.
func newMigratedRig(t *testing.T, backupID string) *migratedRig {
	t.Helper()
	dataDir := t.TempDir()
	data, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { data.Close() })

	oldDir, newDefaultDir := t.TempDir(), t.TempDir()
	stateDir := t.TempDir()
	sqlitePath := filepath.Join(stateDir, "arc.db")
	configPath := filepath.Join(stateDir, "arc.toml")
	// Outside the data root, which is what makes the warehouse its own backup
	// group rather than part of the data listing.
	warehouse := filepath.Join(t.TempDir(), "warehouse")
	if err := os.MkdirAll(warehouse, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte("# the node's current config\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// The backup, as a single-target run wrote it: HasMetadata, HasConfig and
	// an IcebergWarehouse, no RunTargets, no IsDefaultTarget.
	manifest := &Manifest{
		Version: "dev", BackupID: backupID, CreatedAt: time.Now().UTC().Add(-72 * time.Hour),
		BackupType: "full", Target: "old", TotalFiles: 1, TotalSizeBytes: 4,
		HasMetadata: true, HasConfig: true,
		IcebergWarehouse: &IcebergWarehouseInfo{Path: warehouse, ConfiguredPath: warehouse, FileCount: 1, SizeBytes: 7},
		Databases: []DatabaseInfo{{Name: "prod", FileCount: 1, SizeBytes: 4,
			Measurements: []MeasurementInfo{{Name: "cpu", FileCount: 1, SizeBytes: 4}}}},
	}
	body, err := MarshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	writeManifest(t, oldDir, backupID, body)
	seed := func(rel, content string) {
		p := filepath.Join(oldDir, backupID, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	seed("metadata/arc.db", "SQLITE")
	seed("config/arc.toml", "# the config this backup holds\n")
	seed("iceberg/arc_sales.db/t/metadata/v1.metadata.json", "{\"x\":1}")
	seed("data/prod/cpu/2026/10/07/00/b.parquet", "PAR1")

	m, err := NewManager(&ManagerConfig{
		DataStorage: data,
		Targets: []Target{
			{Name: "old", Spec: storage.BackendSpec{Type: "local", LocalPath: oldDir}},
			{Name: "new", Spec: storage.BackendSpec{Type: "local", LocalPath: newDefaultDir}},
		},
		// The operator re-pointed the default at the target they added.
		DefaultTarget:        "new",
		Routing:              map[string]string{"audit": "old"},
		SQLiteDBPath:         sqlitePath,
		ConfigPath:           configPath,
		IcebergWarehousePath: warehouse,
		Logger:               zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewManager after the default moved: %v", err)
	}
	return &migratedRig{m: m, oldDir: oldDir, newDefaultDir: newDefaultDir,
		sqlitePath: sqlitePath, configPath: configPath, warehouse: warehouse}
}

// TestTheDefaultLegRestoreStepsReadFromTheLegThatWroteThem is the anchor-leg
// rule: SQLite, arc.toml and an outside-root Iceberg warehouse are read from
// the leg whose manifest DECLARED them, never from whatever target is the
// default now.
//
// The two are the same target until an operator adds a second target and
// re-points backup.default_target. After that the gates still fire — they read
// the MERGED view, so HasMetadata is true because some leg set it — while the
// read goes to a target that holds nothing. SQLite and config fail outright,
// and in replace mode they fail AFTER the data restore has already rewritten
// live files. The warehouse arm does not fail at all: a zero-object listing is
// a legitimate "this run had no warehouse", so the restore reports completed
// and the outside-root metadata is never written back, while the catalog that
// was just restored points at absolute paths under it (#637).
func TestTheDefaultLegRestoreStepsReadFromTheLegThatWroteThem(t *testing.T) {
	ctx := context.Background()
	const id = "backup-20260101-010101-cccccccc"
	rig := newMigratedRig(t, id)

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{
		BackupID:        id,
		RestoreData:     true,
		RestoreMetadata: true,
		RestoreConfig:   true,
	}); err != nil {
		t.Fatalf("RestoreBackup after the default target moved: %v", err)
	}

	// (a) SQLite. Staged as a pending restore rather than written in place, so
	// the staged file is what proves the read reached the right leg.
	pending, err := os.ReadFile(rig.sqlitePath + PendingRestoreSuffix)
	if err != nil {
		t.Fatalf("no staged SQLite restore: %v", err)
	}
	if string(pending) != "SQLITE" {
		t.Errorf("staged SQLite = %q, want the bytes held on the leg that wrote it", pending)
	}

	// (b) arc.toml, written in place.
	cfg, err := os.ReadFile(rig.configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(cfg), "the config this backup holds") {
		t.Errorf("restored config = %q, want the bytes held on the leg that wrote it", cfg)
	}

	// (c) the outside-root Iceberg warehouse, the SILENT arm: the file has to
	// be on disk, because a wrong-leg read reports success having written
	// nothing.
	whFile := filepath.Join(rig.warehouse, "arc_sales.db", "t", "metadata", "v1.metadata.json")
	if body, err := os.ReadFile(whFile); err != nil {
		t.Errorf("outside-root warehouse file not restored: %v; a zero-object listing on the wrong leg reports a completed restore that wrote no warehouse", err)
	} else if string(body) != "{\"x\":1}" {
		t.Errorf("restored warehouse file = %q, want the backup bytes", body)
	}
}

// TestAnAbortedRunIsFoundWhenTheDefaultTargetMoved: the run index is WRITTEN
// to the default target, so a reader that only ever looks at the current
// default stops enumerating every run that predates a default_target change —
// re-opening the hole the index exists to close, in silence.
func TestAnAbortedRunIsFoundWhenTheDefaultTargetMoved(t *testing.T) {
	ctx := context.Background()
	rig := newRoutedRig(t, map[string]string{"audit": "audit"})

	// A run that died after its index, written when "audit" was the default.
	const id = "backup-20260101-010101-dddddddd"
	index := &RunIndex{
		BackupID: id, CreatedAt: time.Now().UTC().Add(-72 * time.Hour),
		DefaultTarget: "audit", Targets: []string{"audit", "main"},
		Status: runIndexStarted,
	}
	body, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(rig.auditDir, id), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rig.auditDir, id, "index.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	// One leftover object, so the directory is enumerable on that target.
	leftover := filepath.Join(rig.auditDir, id, "data", "audit", "events", "2026", "10", "07", "00", "a.parquet")
	if err := os.MkdirAll(filepath.Dir(leftover), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leftover, []byte("PAR1"), 0o600); err != nil {
		t.Fatal(err)
	}

	// The node default is "main" now, which holds nothing for this ID.
	listing, err := rig.m.ListBackupsDetailed(ctx, false)
	if err != nil {
		t.Fatalf("ListBackupsDetailed: %v", err)
	}
	if len(listing.IncompleteRuns) != 1 || listing.IncompleteRuns[0].BackupID != id {
		t.Fatalf("incomplete_runs = %+v, want the aborted run whose index is on a non-default target", listing.IncompleteRuns)
	}
	if strings.Join(listing.IncompleteRuns[0].Targets, ",") != "audit,main" {
		t.Errorf("targets = %v, want both targets the index named", listing.IncompleteRuns[0].Targets)
	}
}

// TestAnInertTargetIsNotAPreconditionOfEveryBackup: a configured target that
// nothing is routed to must not be a leg.
//
// Taking every CONFIGURED target as a leg makes an unrelated store a hard
// dependency of every backup in the instance: each leg is probed before
// anything is copied, so an operator who adds the [backup.targets.x] block in
// one commit and its databases= in the next, or whose credentials for it are
// wrong, breaks whole-instance backups that have no business touching it.
func TestAnInertTargetIsNotAPreconditionOfEveryBackup(t *testing.T) {
	ctx := context.Background()
	// Nothing routed to audit at all: the block exists, the routing does not.
	rig := newRoutedRig(t, nil)
	rig.write(t, "prod/cpu/2026/10/07/00/b.parquet", "PAR1")
	rig.m.targets["audit"] = backupTarget{
		backend: unreachableDestination{Backend: rig.m.targets["audit"].backend},
		name:    "audit",
	}

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("an unscoped backup failed because of a target nothing routes to: %v", err)
	}
	if n := len(keysUnder(t, rig.auditDir)); n != 0 {
		t.Errorf("the inert target holds %d objects, want none", n)
	}
	if len(result.Manifest.RunTargets) != 0 {
		t.Errorf("run_targets = %v, want none: the run had one leg", result.Manifest.RunTargets)
	}
}

// TestAnInertTargetIsWarnedAboutAtStartup: inert is reported, not refused.
//
// A boot failure over a destination that is merely unused is the worse
// outcome — adding the block and the routing in separate commits is ordinary —
// but an operator has to be told that nothing will ever be written to it.
func TestAnInertTargetIsWarnedAboutAtStartup(t *testing.T) {
	dataDir := t.TempDir()
	data, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { data.Close() })
	var logs bytes.Buffer
	if _, err := NewManager(&ManagerConfig{
		DataStorage: data,
		Targets: []Target{
			{Name: "main", Spec: storage.BackendSpec{Type: "local", LocalPath: t.TempDir()}},
			{Name: "audit", Spec: storage.BackendSpec{Type: "local", LocalPath: t.TempDir()}},
		},
		DefaultTarget: "main",
		Logger:        zerolog.New(&logs),
	}); err != nil {
		t.Fatalf("NewManager refused a configuration with an inert target: %v", err)
	}
	if !strings.Contains(logs.String(), "no databases routed to them") {
		t.Errorf("no inert-target warning; got: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "audit") {
		t.Errorf("the warning does not name the inert target; got: %s", logs.String())
	}
}

// TestTheConfigLeakWarningNamesEveryConfiguredRemoteTarget: include_config
// copies arc.toml, and arc.toml carries EVERY target credentials.
//
// So the warning has to come from the configured set, not from the legs this
// run happened to write. A SCOPED request is exactly the one that can turn
// include_config on without a refusal, and its legs are usually just the local
// default — which made the per-leg warning silent on the one request that
// needed it.
func TestTheConfigLeakWarningNamesEveryConfiguredRemoteTarget(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	data, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { data.Close() })
	stateDir := t.TempDir()
	configPath := filepath.Join(stateDir, "arc.toml")
	if err := os.WriteFile(configPath, []byte("# secrets live here\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	m, err := NewManager(&ManagerConfig{
		DataStorage: data,
		Targets: []Target{
			{Name: "main", Spec: storage.BackendSpec{Type: "local", LocalPath: t.TempDir()}},
			// Local backend, flagged remote: Remote decides the
			// include_config default and nothing else, so this is the whole
			// difference an object store makes here.
			{Name: "audit", Spec: storage.BackendSpec{Type: "local", LocalPath: t.TempDir()}, Remote: true},
		},
		DefaultTarget: "main",
		Routing:       map[string]string{"audit": "audit"},
		ConfigPath:    configPath,
		Logger:        zerolog.New(&logs),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := data.Write(ctx, "prod/cpu/2026/10/07/00/b.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}

	// Scoped to a database routed NOWHERE, so the run has one leg: the local
	// default. The remote target is untouched and its credentials still leak.
	if _, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{"prod"}, IncludeConfig: true}); err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	out := logs.String()
	if !strings.Contains(out, "carries the credentials of every one of them") {
		t.Fatalf("no credential-leak warning on a scoped run with include_config; got: %s", out)
	}
	if !strings.Contains(out, "\"remote_targets\":[\"audit\"]") {
		t.Errorf("warning does not name the configured remote target; got: %s", out)
	}
}

// TestARoutedReplaceModeRestoreOnlyDeletesWhatTheRunHolds is decision 9
// central hazard: replaceDatabases runs ONCE, over the union of every leg, and
// the per-leg copy happens inside the one compaction pause.
//
// Run it per leg instead and the first leg's replace deletes the live files of a
// database the OTHER leg holds, having never seen them. The deletion goes
// through the cluster manifest and is irreversible; the gap shows up at the
// next query.
//
// The edge-sync shape is the sharp end, and is why both files here are
// LABELLED prod in the cluster manifest while living on different legs: a
// spoke file is routed by the SPOKE (spoke1/prod/... goes to the audit
// target), so the audit leg's manifest and the default leg's manifest both claim
// the database prod. A per-leg replace of prod from the audit leg would delete
// the default leg's file, which it is not holding a copy of.
func TestARoutedReplaceModeRestoreOnlyDeletesWhatTheRunHolds(t *testing.T) {
	ctx := context.Background()
	const (
		spokeFile = "spoke1/prod/cpu/2026/10/07/00/a.parquet"
		rootFile  = "prod/cpu/2026/10/07/00/b.parquet"
	)
	rig := newRoutedRig(t, map[string]string{"spoke1": "audit"})
	rig.write(t, spokeFile, "PAR1spoke")
	rig.write(t, rootFile, "PAR1prod")

	// Both entries labelled prod, which is what the registrar records for an
	// edge-sync spoke file: the label is the inner database, the routing key
	// is the spoke.
	entry := func(path, body string) ManifestFile {
		return ManifestFile{Path: path, SHA256: shaOf([]byte(body)), SizeBytes: int64(len(body)),
			Database: "prod", Measurement: "cpu", CreatedAt: fixedCreatedAt}
	}
	cm := &fakeClusterManifest{entries: []ManifestFile{
		entry(spokeFile, "PAR1spoke"), entry(rootFile, "PAR1prod"),
	}}
	rig.m.SetClusterManifest(cm)

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := result.Manifest.BackupID
	if strings.Join(result.Manifest.RunTargets, ",") != "audit,main" {
		t.Fatalf("run_targets = %v, want both legs", result.Manifest.RunTargets)
	}
	// Each leg really does hold one of the two, which is what makes a per-leg
	// replace destructive rather than merely redundant.
	if n := len(keysUnder(t, rig.auditDir)); n == 0 {
		t.Fatal("the audit leg holds nothing; the test does not exercise two legs")
	}

	// Two stale entries the backup does NOT hold, one in each leg's namespace,
	// so the replace has real work to do and the delete list is observable.
	// Both are labelled prod, like the two above.
	const (
		staleSpoke = "spoke1/prod/cpu/2026/10/06/00/gone.parquet"
		staleRoot  = "prod/cpu/2026/10/06/00/gone.parquet"
	)
	rig.write(t, staleSpoke, "STALE1")
	rig.write(t, staleRoot, "STALE2")
	cm.entries = append(cm.entries, entry(staleSpoke, "STALE1"), entry(staleRoot, "STALE2"))

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{
		BackupID:    id,
		RestoreData: true,
		Mode:        RestoreModeReplace,
	}); err != nil {
		t.Fatalf("replace-mode restore of a routed run: %v", err)
	}

	// Exactly one manifest delete call, over the union of the legs.
	if len(cm.deletes) != 1 {
		t.Fatalf("manifest delete calls = %d, want exactly one for the whole run", len(cm.deletes))
	}
	held := map[string]bool{spokeFile: true, rootFile: true}
	for _, p := range cm.deletes[0] {
		if held[p] {
			t.Errorf("replace deleted %q from the cluster manifest, and the run holds it: a per-leg replace removes the other leg's files", p)
		}
	}
	// Both stale entries go, from both legs' namespaces: the replace saw the
	// whole run, not one leg of it.
	deleted := map[string]bool{}
	for _, p := range cm.deletes[0] {
		deleted[p] = true
	}
	for _, p := range []string{staleSpoke, staleRoot} {
		if !deleted[p] {
			t.Errorf("replace did not delete the stale entry %q; delete list = %v", p, cm.deletes[0])
		}
	}
	// And both survive on disk with the backup bytes.
	for path, want := range map[string]string{spokeFile: "PAR1spoke", rootFile: "PAR1prod"} {
		body, err := rig.data.Read(ctx, path)
		if err != nil || string(body) != want {
			t.Errorf("after the replace restore %s = %q, %v; want %q", path, body, err, want)
		}
	}
}

// TestALegacyDestinationAbortedRunIsReportedAborted: the backup.local_path
// destination that predates targets has no target NAME, so its run index names
// no targets at all.
//
// That must not read as "nothing is known to be missing". A run with no
// manifest anywhere is missing in its entirety whether or not its index names
// targets, and nothing here is unreachable — the manifest is simply absent. The
// undetermined state means something narrower: every target that has not
// committed is one that would not answer.
func TestALegacyDestinationAbortedRunIsReportedAborted(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	data, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { data.Close() })
	backupDir := t.TempDir()
	m, err := NewManager(&ManagerConfig{DataStorage: data, BackupPath: backupDir, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}

	// An index as the single, unnamed destination writes one: no
	// default_target, no targets.
	const id = "backup-20260101-010101-eeeeeeee"
	index := &RunIndex{
		BackupID: id, CreatedAt: time.Now().UTC().Add(-72 * time.Hour),
		Targets: []string{}, Status: runIndexStarted,
	}
	body, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(backupDir, id), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, id, "index.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	leftover := filepath.Join(backupDir, id, "data", "prod", "cpu", "2026", "10", "07", "00", "a.parquet")
	if err := os.MkdirAll(filepath.Dir(leftover), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leftover, []byte("PAR1"), 0o600); err != nil {
		t.Fatal(err)
	}

	listing, err := m.ListBackupsDetailed(ctx, false)
	if err != nil {
		t.Fatalf("ListBackupsDetailed: %v", err)
	}
	if len(listing.IncompleteRuns) != 1 {
		t.Fatalf("incomplete_runs = %+v, want the aborted run", listing.IncompleteRuns)
	}
	if got := listing.IncompleteRuns[0].State; got != IncompleteRunAborted {
		t.Errorf("state = %q, want %q: a run with no manifest anywhere is missing in its entirety, and nothing here was unreachable", got, IncompleteRunAborted)
	}
}
