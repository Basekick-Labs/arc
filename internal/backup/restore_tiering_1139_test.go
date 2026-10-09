package backup

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRestoreDoesNotResurrectMatchingColdCopyForHotBackupFile(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	const path = "prod/cpu/2026/01/01/00/data.parquet"
	const body = "PAR1payload"
	if err := rig.data.Write(ctx, path, []byte(body)); err != nil {
		t.Fatal(err)
	}
	backup, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if err := os.Remove(filepath.Join(rig.dataDir, path)); err != nil {
		t.Fatalf("remove hot copy: %v", err)
	}
	rig.writeCold(t, path, body)

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: backup.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rig.dataDir, path)); !os.IsNotExist(err) {
		t.Errorf("hot duplicate exists after restore (stat error %v)", err)
	}
	if got := rig.cold.rows[path]; got != int64(len(body)) {
		t.Errorf("cold row size = %d, want %d", got, len(body))
	}
	if got := rig.m.GetProgress().ColdFilesSkippedAlreadyCold; got != 1 {
		t.Errorf("cold_files_skipped_already_cold = %d, want 1", got)
	}
	status, err := json.Marshal(rig.m.GetProgress())
	if err != nil {
		t.Fatalf("marshal restore status: %v", err)
	}
	if !strings.Contains(string(status), `"cold_files_skipped_already_cold":1`) {
		t.Errorf("restore status does not expose the skipped-cold counter: %s", status)
	}
}

// TestRestoreRoutesHotBackupFileToColdWhenItsRowSaysCold covers the case the
// backup holds as hot but this node has since migrated: the local tier row is
// the truth about where the file belongs, so the bytes go to COLD and no hot
// copy is created.
//
// Writing them hot instead — which is what a forced hot row implies — leaves
// the row saying cold with a recent migrated_at until the batched flush lands,
// and that is precisely the state Migrator.ReconcileOrphanedFiles acts on: it
// would delete the hot copy this restore had just written and keep the stale
// cold object. See TestRestoreLeavesNothingForTheOrphanSweepToDelete below.
func TestRestoreRoutesHotBackupFileToColdWhenItsRowSaysCold(t *testing.T) {
	for _, tc := range []struct {
		name       string
		coldObject string
		staged     bool
	}{
		{name: "missing"},
		{name: "different size", coldObject: "PAR1different"},
		{name: "matching staged file is not a final object", staged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			rig := newColdRig(t, nil)
			const path = "prod/cpu/2026/01/01/00/data.parquet"
			const body = "PAR1payload"
			if err := rig.data.Write(ctx, path, []byte(body)); err != nil {
				t.Fatal(err)
			}
			backup, err := rig.m.CreateBackup(ctx, BackupOptions{})
			if err != nil {
				t.Fatalf("CreateBackup: %v", err)
			}
			if err := os.Remove(filepath.Join(rig.dataDir, path)); err != nil {
				t.Fatalf("remove hot copy: %v", err)
			}
			rig.cold.rows[path] = int64(len(body))
			if tc.staged {
				staged := filepath.Join(rig.coldDir, path+".part")
				if err := os.MkdirAll(filepath.Dir(staged), 0o755); err != nil {
					t.Fatalf("create staged directory: %v", err)
				}
				if err := os.WriteFile(staged, []byte(body), 0o600); err != nil {
					t.Fatalf("seed staged object: %v", err)
				}
			} else if tc.coldObject != "" {
				if err := rig.cold.backend.Write(ctx, path, []byte(tc.coldObject)); err != nil {
					t.Fatalf("seed mismatched cold object: %v", err)
				}
			}

			if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: backup.Manifest.BackupID, RestoreData: true}); err != nil {
				t.Fatalf("RestoreBackup: %v", err)
			}

			// The backup bytes are in COLD storage, where the row says they are.
			got, err := rig.cold.backend.Read(ctx, path)
			if err != nil {
				t.Fatalf("cold copy was not restored: %v", err)
			}
			if string(got) != body {
				t.Errorf("restored cold bytes = %q, want %q", got, body)
			}
			// And no hot copy exists, so the row and the objects agree.
			if _, err := os.Stat(filepath.Join(rig.dataDir, path)); err == nil {
				t.Error("restore wrote a hot copy for a file whose tier row says cold")
			} else if !os.IsNotExist(err) {
				t.Fatalf("stat hot copy: %v", err)
			}
			if got := rig.cold.recorded[path]; got != int64(len(body)) {
				t.Errorf("cold row size = %d, want %d", got, len(body))
			}
			if got := rig.cold.recordedHot[path]; got != 0 {
				t.Errorf("a hot row was forced for a cold-row file: size %d", got)
			}
			if got := rig.m.GetProgress().HotBackupFilesRoutedToCold; got != 1 {
				t.Errorf("hot_backup_files_routed_to_cold = %d, want 1", got)
			}
			if got := rig.m.GetProgress().ColdFilesSkippedAlreadyCold; got != 0 {
				t.Errorf("cold_files_skipped_already_cold = %d, want 0", got)
			}
		})
	}
}

// TestRestoreLeavesNothingForTheOrphanSweepToDelete states the invariant
// directly, as the sweep's own precondition rather than as a behaviour.
//
// Migrator.ReconcileOrphanedFiles deletes a hot copy when, for one path: the
// tier row says cold with a recent migrated_at, a hot object exists, a cold
// backend is configured, and the cold object exists. A restore must never
// leave that conjunction true, whatever it decided to do with the file —
// otherwise the sweep removes data the restore just wrote and reports success.
func TestRestoreLeavesNothingForTheOrphanSweepToDelete(t *testing.T) {
	for _, tc := range []struct {
		name       string
		coldObject string
	}{
		{name: "cold object matches the sidecar", coldObject: "PAR1payload"},
		{name: "cold object differs in size", coldObject: "PAR1different"},
		{name: "cold object is missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			rig := newColdRig(t, nil)
			const path = "prod/cpu/2026/01/01/00/data.parquet"
			const body = "PAR1payload"
			if err := rig.data.Write(ctx, path, []byte(body)); err != nil {
				t.Fatal(err)
			}
			backup, err := rig.m.CreateBackup(ctx, BackupOptions{})
			if err != nil {
				t.Fatalf("CreateBackup: %v", err)
			}
			if err := os.Remove(filepath.Join(rig.dataDir, path)); err != nil {
				t.Fatalf("remove hot copy: %v", err)
			}
			rig.cold.rows[path] = int64(len(body))
			if tc.coldObject != "" {
				if err := rig.cold.backend.Write(ctx, path, []byte(tc.coldObject)); err != nil {
					t.Fatalf("seed cold object: %v", err)
				}
			}

			if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: backup.Manifest.BackupID, RestoreData: true}); err != nil {
				t.Fatalf("RestoreBackup: %v", err)
			}

			// The row still says cold unless a hot row was forced.
			rowSaysCold := rig.cold.recordedHot[path] == 0
			_, hotErr := os.Stat(filepath.Join(rig.dataDir, path))
			hotExists := hotErr == nil
			coldExists, err := rig.cold.backend.Exists(ctx, path)
			if err != nil {
				t.Fatalf("cold Exists: %v", err)
			}
			if rowSaysCold && hotExists && coldExists {
				t.Error("restore left the orphan sweep's delete precondition true: " +
					"cold row, hot object and cold object all present, so the sweep would " +
					"delete the restored hot copy")
			}
		})
	}
}

func TestRestoreForcesHotWhenColdBackendIsUnavailable(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	const path = "prod/cpu/2026/01/01/00/data.parquet"
	const body = "PAR1payload"
	if err := rig.data.Write(ctx, path, []byte(body)); err != nil {
		t.Fatal(err)
	}
	backup, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if err := os.Remove(filepath.Join(rig.dataDir, path)); err != nil {
		t.Fatalf("remove hot copy: %v", err)
	}
	rig.m.SetColdSource(&fakeColdSource{backend: nil, rows: map[string]int64{path: int64(len(body))}})

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: backup.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rig.dataDir, path)); err != nil {
		t.Errorf("hot copy was not restored: %v", err)
	}
	if got := rig.m.coldSource.(*fakeColdSource).recordedHot[path]; got != int64(len(body)) {
		t.Errorf("forced hot row size = %d, want %d", got, len(body))
	}
}
