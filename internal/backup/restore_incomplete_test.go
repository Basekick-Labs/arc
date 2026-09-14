package backup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// Regression tests for #762 item 1: a restore that could not restore every
// data file must not report "completed". Assertions here use only fields that
// existed before the fix, so the file compiles against the pre-fix code and
// each test can be shown to fail there.

// makeRestorableBackup writes n parquet-named objects into a fresh data store,
// backs them up, and returns the backup directory, the backup ID, and the keys.
func makeRestorableBackup(t *testing.T, n int) (string, string, []string) {
	t.Helper()
	ctx := context.Background()
	src := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("db/cpu/2026/09/13/00/f_%03d.parquet", i)
		if err := src.Write(ctx, k, bytes.Repeat([]byte{'p'}, 32)); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
		keys = append(keys, k)
	}
	backupDir := t.TempDir()
	mgr, err := NewManager(&ManagerConfig{DataStorage: src, BackupPath: backupDir, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	res, err := mgr.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	return backupDir, res.Manifest.BackupID, keys
}

// readFailingBackend makes ReadTo fail for chosen backup objects without
// touching the writer, the way a damaged or unreachable object fails.
type readFailingBackend struct {
	*storage.LocalBackend
	failPaths map[string]bool
}

func (b *readFailingBackend) ReadTo(ctx context.Context, path string, w io.Writer) error {
	if b.failPaths[path] {
		return fmt.Errorf("simulated backup storage read failure")
	}
	return b.LocalBackend.ReadTo(ctx, path, w)
}

// keyedPartialWriteBackend fails WriteReader for one key, leaving a staging
// partial behind the way LocalBackend does, and delegates every other write.
// Embeds the concrete backend so the StagingInspector methods survive.
type keyedPartialWriteBackend struct {
	*storage.LocalBackend
	dir     string
	failKey string
}

func (b *keyedPartialWriteBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	if path != b.failKey {
		return b.LocalBackend.WriteReader(ctx, path, reader, size)
	}
	stagingPath := filepath.Join(b.dir, path+".part")
	if err := os.MkdirAll(filepath.Dir(stagingPath), 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(stagingPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, _ = io.CopyN(f, reader, 4)
	f.Close()
	return fmt.Errorf("simulated data storage write failure")
}

func restoreInto(t *testing.T, backupStorage storage.Backend, dataStorage storage.Backend, backupID string) (*Manager, error) {
	t.Helper()
	m := &Manager{dataStorage: dataStorage, backupStorage: backupStorage, logger: zerolog.Nop()}
	_, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: true})
	return m, err
}

// One unreadable object among many: every other file is restored, the gap is
// counted, and the restore does not report success.
func TestRestore_UnreadableBackupObjectFailsTheRestore(t *testing.T) {
	backupDir, backupID, keys := makeRestorableBackup(t, 21)
	bad := backupID + "/data/" + keys[0]
	backupStorage := &readFailingBackend{
		LocalBackend: mustLocalBackend(t, backupDir, zerolog.Nop()),
		failPaths:    map[string]bool{bad: true},
	}
	dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())

	m, err := restoreInto(t, backupStorage, dest, backupID)
	if err == nil {
		t.Fatal("restore with an unreadable backup object returned nil error")
	}
	p := m.GetProgress()
	if p.Status != "failed" {
		t.Errorf("Status = %q, want failed", p.Status)
	}
	if !strings.Contains(p.Error, "restore incomplete") {
		t.Errorf("Error = %q, want it to say the restore is incomplete", p.Error)
	}
	if p.SkippedFiles != 1 {
		t.Errorf("SkippedFiles = %d, want 1", p.SkippedFiles)
	}
	if p.ProcessedFiles != 20 {
		t.Errorf("ProcessedFiles = %d, want 20 (every readable file restored)", p.ProcessedFiles)
	}
	if ok, _ := dest.Exists(context.Background(), keys[20]); !ok {
		t.Errorf("a readable file was not restored: %s", keys[20])
	}
	if ok, _ := dest.Exists(context.Background(), keys[0]); ok {
		t.Errorf("the unreadable file appeared in data storage: %s", keys[0])
	}
}

// Objects removed from the backup after it was written list short. Every listed
// file restores, so only the manifest inventory can reveal the gap.
func TestRestore_MissingBackupObjectsFailTheRestore(t *testing.T) {
	backupDir, backupID, keys := makeRestorableBackup(t, 10)
	if err := os.Remove(filepath.Join(backupDir, backupID, "data", filepath.FromSlash(keys[3]))); err != nil {
		t.Fatalf("remove backup object: %v", err)
	}
	dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())

	m, err := restoreInto(t, mustLocalBackend(t, backupDir, zerolog.Nop()), dest, backupID)
	if err == nil {
		t.Fatal("restore of a backup missing an inventoried object returned nil error")
	}
	p := m.GetProgress()
	if p.Status != "failed" {
		t.Errorf("Status = %q, want failed", p.Status)
	}
	if !strings.Contains(p.Error, "missing") {
		t.Errorf("Error = %q, want it to mention missing files", p.Error)
	}
	if p.ProcessedFiles != 9 {
		t.Errorf("ProcessedFiles = %d, want 9", p.ProcessedFiles)
	}
}

// A write into the live data store that fails is the environment breaking, not
// a file to skip: the restore aborts, says why, and does not leave the staging
// partial behind.
func TestRestore_DataWriteFailureIsFatal(t *testing.T) {
	backupDir, backupID, keys := makeRestorableBackup(t, 10)
	destDir := t.TempDir()
	dest := &keyedPartialWriteBackend{
		LocalBackend: mustLocalBackend(t, destDir, zerolog.Nop()),
		dir:          destDir,
		failKey:      keys[5],
	}

	m, err := restoreInto(t, mustLocalBackend(t, backupDir, zerolog.Nop()), dest, backupID)
	if err == nil {
		t.Fatal("restore with a failing data write returned nil error")
	}
	p := m.GetProgress()
	if p.Status != "failed" {
		t.Errorf("Status = %q, want failed", p.Status)
	}
	if !strings.Contains(p.Error, "failed to write to data storage") {
		t.Errorf("Error = %q, want the data write failure", p.Error)
	}
	if p.SkippedFiles != 0 {
		t.Errorf("SkippedFiles = %d, want 0 (a write failure is not a skip)", p.SkippedFiles)
	}
	if p.ProcessedFiles == 0 || p.ProcessedFiles >= 10 {
		t.Errorf("ProcessedFiles = %d, want the loop to have stopped at the failing key", p.ProcessedFiles)
	}
	if n, err := dest.StagedSize(context.Background(), keys[5]); err != nil {
		t.Errorf("StagedSize: %v", err)
	} else if n >= 0 {
		t.Errorf("staging partial for %s survived the failed write (%d bytes)", keys[5], n)
	}
}

// A temp file that cannot be created is fatal, not a skip: previously every
// per-file error, including this one, was skipped and the restore completed.
func TestRestore_TempFileFailureIsFatal(t *testing.T) {
	backupDir, backupID, _ := makeRestorableBackup(t, 2)
	dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "does-not-exist"))

	m, err := restoreInto(t, mustLocalBackend(t, backupDir, zerolog.Nop()), dest, backupID)
	if err == nil {
		t.Fatal("restore with an unusable temp dir returned nil error")
	}
	p := m.GetProgress()
	if p.Status != "failed" {
		t.Errorf("Status = %q, want failed", p.Status)
	}
	if !strings.Contains(p.Error, "failed to create temp file") {
		t.Errorf("Error = %q, want the temp file failure", p.Error)
	}
	if p.SkippedFiles != 0 {
		t.Errorf("SkippedFiles = %d, want 0", p.SkippedFiles)
	}
}
