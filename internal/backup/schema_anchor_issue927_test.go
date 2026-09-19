package backup

import (
	"context"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// Field schema anchors (#914) live under _schema/ and are Parquet objects.
// They are backed up and restored with the data, counted in total_files
// (the restore compares that count against every Parquet object present),
// and kept out of the database inventory.
func TestBackupRestore_SchemaAnchorsAreAuxiliaryIssue927(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	backupDir := t.TempDir()
	data, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	dataKey := "prod/sensors/2026/07/14/15/sensors_1.parquet"
	anchorKey := "_schema/prod/sensors.parquet"
	spokeAnchorKey := "_schema/spoke1/prod/sensors.parquet"
	manifestKey := "_compaction_state/hourly/prod/job.json"
	for k, body := range map[string]string{dataKey: "PAR1-data", anchorKey: "PAR1-anchor", spokeAnchorKey: "PAR1-spoke-anchor", manifestKey: "{}"} {
		if err := data.Write(ctx, k, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	mgr, err := NewManager(&ManagerConfig{DataStorage: data, BackupPath: backupDir, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	res, err := mgr.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	m := res.Manifest
	if len(m.Databases) != 1 || m.Databases[0].Name != "prod" || len(m.Databases[0].Measurements) != 1 {
		t.Fatalf("anchors must not appear as databases: %+v", m.Databases)
	}
	if m.TotalFiles != 3 || m.AuxiliaryFiles != 2 || m.SkippedFiles != 0 {
		t.Fatalf("total=%d aux=%d skipped=%d", m.TotalFiles, m.AuxiliaryFiles, m.SkippedFiles)
	}
	backupStore, _ := storage.NewLocalBackend(backupDir, zerolog.Nop())
	for _, k := range []string{dataKey, anchorKey, spokeAnchorKey} {
		if _, err := backupStore.Read(ctx, m.BackupID+"/data/"+k); err != nil {
			t.Errorf("%s not in backup: %v", k, err)
		}
	}
	// Compaction recovery manifests are still not backed up (#930): pinned
	// so a change there is deliberate.
	if ok, _ := backupStore.Exists(ctx, m.BackupID+"/data/"+manifestKey); ok {
		t.Fatal("compaction state unexpectedly backed up; update #930 and this test")
	}

	// Restore brings the anchors back byte for byte.
	for _, k := range []string{anchorKey, spokeAnchorKey} {
		if err := data.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := mgr.RestoreBackup(ctx, RestoreOptions{BackupID: m.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	if got, err := data.Read(ctx, anchorKey); err != nil || string(got) != "PAR1-anchor" {
		t.Fatalf("anchor not restored: %q %v", got, err)
	}
	if got, err := data.Read(ctx, spokeAnchorKey); err != nil || string(got) != "PAR1-spoke-anchor" {
		t.Fatalf("spoke anchor not restored: %q %v", got, err)
	}
	if p := mgr.GetProgress(); p.MissingFiles != 0 || p.Status != "completed" {
		t.Fatalf("clean restore reported missing=%d status=%s", p.MissingFiles, p.Status)
	}

	// A data file missing from the backup is still detected: the anchors
	// inside total_files cannot stand in for it.
	if err := backupStore.Delete(ctx, m.BackupID+"/data/"+dataKey); err != nil {
		t.Fatal(err)
	}
	dest, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	m2 := &Manager{dataStorage: dest, backupStorage: backupStore, logger: zerolog.Nop()}
	_, err = m2.RestoreBackup(ctx, RestoreOptions{BackupID: m.BackupID, RestoreData: true})
	if p := m2.GetProgress(); p.MissingFiles != 1 || err == nil {
		t.Fatalf("missing data file hidden behind anchors: missing=%d err=%v", p.MissingFiles, err)
	}
}

func TestIsReservedRootParquet(t *testing.T) {
	for p, want := range map[string]bool{
		"_schema/db/m.parquet":                true,
		"_schema/spoke/db/m.parquet":          true,
		"_compaction_state/x.parquet":         true,
		"db/m/2026/01/01/00/a.parquet":        false,
		"db_schema/m/2026/01/01/00/a.parquet": false,
		".sync-staging/s/db/m/a.parquet":      false, // dot roots keep today's treatment
	} {
		if got := isReservedRootParquet(p); got != want {
			t.Errorf("isReservedRootParquet(%q) = %v, want %v", p, got, want)
		}
	}
}
