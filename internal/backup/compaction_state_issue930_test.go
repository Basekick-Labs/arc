package backup_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
)

const (
	partition930 = "db/cpu/2026/01/01/00"
	output930    = partition930 + "/x_compacted.parquet"
	manifest930  = "_compaction_state/hourly/db/job1.json"
	parked930    = "_compaction_state/hourly/db/old.json.quarantined"
)

var inputs930 = []string{partition930 + "/a.parquet", partition930 + "/b.parquet"}

// seedMidJob lays out a store as a compaction job leaves it between its
// output upload and its input deletion: inputs, output and the recovery
// manifest all present. Extra unrelated files keep the skip ratio low in
// the tests that make copies fail.
func seedMidJob(t *testing.T, b storage.Backend, withOutput bool, unrelated int) {
	t.Helper()
	ctx := context.Background()
	for _, k := range inputs930 {
		if err := b.Write(ctx, k, []byte("IN")); err != nil {
			t.Fatal(err)
		}
	}
	if withOutput {
		if err := b.Write(ctx, output930, []byte("OUT")); err != nil {
			t.Fatal(err)
		}
	}
	m := compaction.Manifest{
		OutputPath: output930, OutputSize: 3, InputFiles: inputs930,
		Database: "db", Measurement: "cpu", PartitionPath: partition930, Tier: "hourly",
		Status: compaction.ManifestStatusPending, CreatedAt: time.Now().UTC(), JobID: "job1",
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Write(ctx, manifest930, data); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(ctx, parked930, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < unrelated; i++ {
		if err := b.Write(ctx, fmt.Sprintf("db/mem/2026/01/01/00/u%02d.parquet", i), []byte("U")); err != nil {
			t.Fatal(err)
		}
	}
}

func newBackupManager(t *testing.T, data storage.Backend, backupDir string) *backup.Manager {
	t.Helper()
	mgr, err := backup.NewManager(&backup.ManagerConfig{DataStorage: data, BackupPath: backupDir, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	return mgr
}

// A backup taken mid-job holds inputs, output and manifest. The restore
// puts the output and the manifest back and leaves the consumed inputs
// out, so the restored store serves each row once from the moment the
// restore finishes; the next recovery pass then retires the manifest.
func TestBackupRestore_MidCompactionBackupIssue930(t *testing.T) {
	ctx := context.Background()
	data, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	seedMidJob(t, data, true, 1)
	backupDir := t.TempDir()
	res, err := newBackupManager(t, data, backupDir).CreateBackup(ctx, backup.BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	m := res.Manifest
	if m.CompactionStateFiles != 2 || m.TotalFiles != 4 || m.SkippedFiles != 0 || m.SkippedMetadataFiles != 0 {
		t.Fatalf("state=%d total=%d skipped=%d metaSkipped=%d", m.CompactionStateFiles, m.TotalFiles, m.SkippedFiles, m.SkippedMetadataFiles)
	}
	if len(m.Databases) != 1 || len(m.Databases[0].Measurements) != 2 {
		t.Fatalf("inventory: %+v", m.Databases)
	}
	backupStore, _ := storage.NewLocalBackend(backupDir, zerolog.Nop())
	for _, k := range []string{manifest930, parked930} {
		if ok, _ := backupStore.Exists(ctx, m.BackupID+"/data/"+k); !ok {
			t.Fatalf("%s not in backup", k)
		}
	}

	// Restore into an empty store.
	dest, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	mgr := newBackupManager(t, dest, backupDir)
	if _, err := mgr.RestoreBackup(ctx, backup.RestoreOptions{BackupID: m.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	p := mgr.GetProgress()
	if p.Status != "completed" || p.MissingFiles != 0 || p.SkippedFiles != 0 {
		t.Fatalf("restore status=%s missing=%d skipped=%d", p.Status, p.MissingFiles, p.SkippedFiles)
	}
	if p.ConsumedInputsSkipped != 2 || p.CompactionStateRestored != 1 {
		t.Fatalf("consumed=%d stateRestored=%d", p.ConsumedInputsSkipped, p.CompactionStateRestored)
	}
	for _, k := range inputs930 {
		if ok, _ := dest.Exists(ctx, k); ok {
			t.Fatalf("consumed input %s restored next to its output", k)
		}
	}
	for _, k := range []string{output930, manifest930, parked930, "db/mem/2026/01/01/00/u00.parquet"} {
		if ok, _ := dest.Exists(ctx, k); !ok {
			t.Fatalf("%s not restored", k)
		}
	}

	// The next compaction cycle's recovery retires the manifest: output kept,
	// absent inputs tolerated, no error.
	recovered, err := compaction.NewManifestManager(dest, zerolog.Nop()).RecoverOrphanedManifests(ctx, nil, nil)
	if err != nil || recovered != 1 {
		t.Fatalf("recovery: recovered=%d err=%v", recovered, err)
	}
	if ok, _ := dest.Exists(ctx, manifest930); ok {
		t.Fatal("manifest not retired by recovery")
	}
	if ok, _ := dest.Exists(ctx, output930); !ok {
		t.Fatal("output lost by recovery")
	}
	if ok, _ := dest.Exists(ctx, parked930); !ok {
		t.Fatal("parked manifest must stay as an operator record")
	}
}

// A backup taken between the manifest write and the upload holds inputs
// and manifest but no output: the inputs must be restored, and recovery
// deletes the manifest so compaction retries.
func TestRestore_ManifestWithoutOutputKeepsInputsIssue930(t *testing.T) {
	ctx := context.Background()
	data, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	seedMidJob(t, data, false, 0)
	backupDir := t.TempDir()
	res, err := newBackupManager(t, data, backupDir).CreateBackup(ctx, backup.BackupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dest, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	mgr := newBackupManager(t, dest, backupDir)
	if _, err := mgr.RestoreBackup(ctx, backup.RestoreOptions{BackupID: res.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatal(err)
	}
	if p := mgr.GetProgress(); p.ConsumedInputsSkipped != 0 || p.Status != "completed" {
		t.Fatalf("progress %+v", p)
	}
	for _, k := range inputs930 {
		if ok, _ := dest.Exists(ctx, k); !ok {
			t.Fatalf("input %s must be restored when its output never uploaded", k)
		}
	}
	if _, err := compaction.NewManifestManager(dest, zerolog.Nop()).RecoverOrphanedManifests(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	if ok, _ := dest.Exists(ctx, manifest930); ok {
		t.Fatal("manifest without output must be deleted by recovery")
	}
	for _, k := range inputs930 {
		if ok, _ := dest.Exists(ctx, k); !ok {
			t.Fatalf("input %s deleted although no output exists", k)
		}
	}
}

// finishingJob wraps the data store: the first time a data file is read for
// the backup, the job "finishes" (inputs and manifest deleted underneath).
// It records the order objects were read in.
type finishingJob struct {
	*storage.LocalBackend
	mu       sync.Mutex
	order    []string
	finished bool
	// finishBefore, when set, finishes the job before the manifest itself
	// is read, so the manifest copy fails.
	finishBefore bool
}

func (f *finishingJob) finish(ctx context.Context) {
	for _, k := range append(append([]string{}, inputs930...), manifest930) {
		_ = f.LocalBackend.Delete(ctx, k)
	}
	f.finished = true
}

func (f *finishingJob) ReadTo(ctx context.Context, path string, w io.Writer) error {
	f.mu.Lock()
	f.order = append(f.order, path)
	if !f.finished && ((f.finishBefore && path == manifest930) || (!f.finishBefore && strings.HasSuffix(path, ".parquet"))) {
		f.finish(ctx)
	}
	f.mu.Unlock()
	return f.LocalBackend.ReadTo(ctx, path, w)
}

func TestBackup_ManifestsCopiedBeforeDataIssue930(t *testing.T) {
	ctx := context.Background()
	t.Run("job finishes during the data copy", func(t *testing.T) {
		local, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
		seedMidJob(t, local, true, 40)
		src := &finishingJob{LocalBackend: local}
		backupDir := t.TempDir()
		res, err := newBackupManager(t, src, backupDir).CreateBackup(ctx, backup.BackupOptions{})
		if err != nil {
			t.Fatalf("CreateBackup: %v", err)
		}
		// Order: every compaction-state read precedes the first data read.
		firstData, lastState := -1, -1
		for i, k := range src.order {
			if strings.HasPrefix(k, "_compaction_state/") {
				lastState = i
			} else if firstData < 0 {
				firstData = i
			}
		}
		if lastState < 0 || firstData < 0 || lastState > firstData {
			t.Fatalf("compaction state must be copied before data: %v", src.order)
		}
		m := res.Manifest
		// The inputs vanished before their copy: data skips, reported as
		// such; the manifest was already in the backup.
		if m.SkippedFiles != 2 || m.SkippedMetadataFiles != 0 {
			t.Fatalf("skipped=%d metaSkipped=%d", m.SkippedFiles, m.SkippedMetadataFiles)
		}
		backupStore, _ := storage.NewLocalBackend(backupDir, zerolog.Nop())
		if ok, _ := backupStore.Exists(ctx, m.BackupID+"/data/"+manifest930); !ok {
			t.Fatal("manifest missing from the backup")
		}
		// Restore: output + manifest, no inputs (they were never copied), and
		// recovery retires the manifest.
		dest, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
		mgr := newBackupManager(t, dest, backupDir)
		_, _ = mgr.RestoreBackup(ctx, backup.RestoreOptions{BackupID: m.BackupID, RestoreData: true})
		if ok, _ := dest.Exists(ctx, output930); !ok {
			t.Fatal("output not restored")
		}
		if n, err := compaction.NewManifestManager(dest, zerolog.Nop()).RecoverOrphanedManifests(ctx, nil, nil); err != nil || n != 1 {
			t.Fatalf("recovery n=%d err=%v", n, err)
		}
	})
	t.Run("job finishes before the manifest copy", func(t *testing.T) {
		local, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
		seedMidJob(t, local, true, 40)
		src := &finishingJob{LocalBackend: local, finishBefore: true}
		backupDir := t.TempDir()
		res, err := newBackupManager(t, src, backupDir).CreateBackup(ctx, backup.BackupOptions{})
		if err != nil {
			t.Fatalf("CreateBackup: %v", err)
		}
		m := res.Manifest
		// The manifest skip is a metadata skip and must not count against
		// the data population; the two inputs are data skips.
		if m.SkippedMetadataFiles != 1 || m.SkippedFiles != 2 {
			t.Fatalf("metaSkipped=%d skipped=%d", m.SkippedMetadataFiles, m.SkippedFiles)
		}
		// Restore arithmetic still detects a missing data file.
		backupStore, _ := storage.NewLocalBackend(backupDir, zerolog.Nop())
		if err := backupStore.Delete(ctx, m.BackupID+"/data/db/mem/2026/01/01/00/u00.parquet"); err != nil {
			t.Fatal(err)
		}
		dest, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
		mgr := newBackupManager(t, dest, backupDir)
		_, err = mgr.RestoreBackup(ctx, backup.RestoreOptions{BackupID: m.BackupID, RestoreData: true})
		if p := mgr.GetProgress(); err == nil || p.MissingFiles != 1 {
			t.Fatalf("missing data file hidden: missing=%d err=%v", p.MissingFiles, err)
		}
	})
}

// A damaged copy of the output in the backup must not turn reconciliation
// into data loss: the inputs are restored and recovery discards the short
// output, keeping the rows.
func TestRestore_DamagedOutputKeepsInputsIssue930(t *testing.T) {
	ctx := context.Background()
	data, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	seedMidJob(t, data, true, 0)
	backupDir := t.TempDir()
	res, err := newBackupManager(t, data, backupDir).CreateBackup(ctx, backup.BackupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	backupStore, _ := storage.NewLocalBackend(backupDir, zerolog.Nop())
	if err := backupStore.Write(ctx, res.Manifest.BackupID+"/data/"+output930, []byte("X")); err != nil {
		t.Fatal(err)
	}
	dest, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	mgr := newBackupManager(t, dest, backupDir)
	if _, err := mgr.RestoreBackup(ctx, backup.RestoreOptions{BackupID: res.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatal(err)
	}
	if p := mgr.GetProgress(); p.ConsumedInputsSkipped != 0 {
		t.Fatalf("inputs skipped although the output copy is damaged: %+v", p)
	}
	if _, err := compaction.NewManifestManager(dest, zerolog.Nop()).RecoverOrphanedManifests(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	if ok, _ := dest.Exists(ctx, output930); ok {
		t.Fatal("short output must be discarded by recovery")
	}
	for _, k := range inputs930 {
		if ok, _ := dest.Exists(ctx, k); !ok {
			t.Fatalf("input %s lost: rows gone", k)
		}
	}
}

// throttledManifest fails the manifest read once while the manifest still
// exists: the backup must fail instead of continuing without it.
type throttledManifest struct {
	*storage.LocalBackend
}

func (f *throttledManifest) ReadTo(ctx context.Context, path string, w io.Writer) error {
	if path == manifest930 {
		return fmt.Errorf("simulated throttling")
	}
	return f.LocalBackend.ReadTo(ctx, path, w)
}

func TestBackup_UnreadableLiveManifestFailsBackupIssue930(t *testing.T) {
	ctx := context.Background()
	local, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	seedMidJob(t, local, true, 40)
	_, err := newBackupManager(t, &throttledManifest{LocalBackend: local}, t.TempDir()).CreateBackup(ctx, backup.BackupOptions{})
	if err == nil || !strings.Contains(err.Error(), "could not be read but still exists") {
		t.Fatalf("backup must fail when a live manifest cannot be copied: %v", err)
	}
}
