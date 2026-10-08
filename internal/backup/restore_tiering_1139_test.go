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

func TestRestoreForcesHotWhenColdRowHasNoMatchingObjectForHotBackupFile(t *testing.T) {
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
			got, err := os.ReadFile(filepath.Join(rig.dataDir, path))
			if err != nil {
				t.Fatalf("hot copy was not restored: %v", err)
			}
			if string(got) != body {
				t.Errorf("restored hot bytes = %q, want %q", got, body)
			}
			if got := rig.cold.recordedHot[path]; got != int64(len(body)) {
				t.Errorf("forced hot row size = %d, want %d", got, len(body))
			}
			if got := rig.m.GetProgress().ColdFilesSkippedAlreadyCold; got != 0 {
				t.Errorf("cold_files_skipped_already_cold = %d, want 0", got)
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
