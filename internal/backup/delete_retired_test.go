package backup_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/storage"
)

// A file a row DELETE replaced and has not removed yet (named by the
// standalone retired journal) is left out of a backup: its rewrite holds its
// rows, and restoring both would count them twice with nothing left to say
// which is old. A record stored under another file's key is not trusted.
func TestBackupLeavesOutFilesADeleteReplaced(t *testing.T) {
	ctx := context.Background()
	data, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	const (
		old    = "db/cpu/2026/01/01/00/cpu_1.parquet"
		rw     = "db/cpu/2026/01/01/00/cpu_1_rewrite_5.parquet"
		victim = "db/cpu/2026/01/01/00/cpu_2.parquet"
	)
	for _, k := range []string{old, rw, victim} {
		if err := data.Write(ctx, k, []byte("P")); err != nil {
			t.Fatal(err)
		}
	}
	key := func(p string) string {
		sum := sha256.Sum256([]byte(p))
		return "_delete_retired/" + hex.EncodeToString(sum[:16]) + ".json"
	}
	if err := data.Write(ctx, key(old), []byte(`{"v":1,"path":"`+old+`","replaced_by":"`+rw+`"}`)); err != nil {
		t.Fatal(err)
	}
	if err := data.Write(ctx, "_delete_retired/forged.json", []byte(`{"v":1,"path":"`+victim+`"}`)); err != nil {
		t.Fatal(err)
	}
	backupDir := t.TempDir()
	res, err := newBackupManager(t, data, backupDir).CreateBackup(ctx, backup.BackupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store, _ := storage.NewLocalBackend(backupDir, zerolog.Nop())
	has := func(k string) bool {
		ok, _ := store.Exists(ctx, res.Manifest.BackupID+"/data/"+k)
		return ok
	}
	if has(old) {
		t.Fatal("the replaced file is in the backup")
	}
	if !has(rw) || !has(victim) {
		t.Fatal("a current file is missing from the backup")
	}
}

// A record is only an intent: while its replacement is not in the backup,
// the file it names is backed up.
func TestBackupKeepsAJournaledFileWhoseReplacementIsMissing(t *testing.T) {
	ctx := context.Background()
	data, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	const old = "db/cpu/2026/01/01/00/cpu_1.parquet"
	if err := data.Write(ctx, old, []byte("P")); err != nil {
		t.Fatal(err)
	}
	if err := data.Write(ctx, storage.RetiredKey(old), []byte(`{"v":1,"path":"`+old+`","replaced_by":"db/cpu/2026/01/01/00/cpu_1_rewrite_5.parquet"}`)); err != nil {
		t.Fatal(err)
	}
	backupDir := t.TempDir()
	res, err := newBackupManager(t, data, backupDir).CreateBackup(ctx, backup.BackupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store, _ := storage.NewLocalBackend(backupDir, zerolog.Nop())
	if ok, _ := store.Exists(ctx, res.Manifest.BackupID+"/data/"+old); !ok {
		t.Fatal("a file whose replacement never landed was left out of the backup")
	}
}

// A restored file is data again: the restore clears any DELETE journal
// record naming it before writing it, so no drainer removes it.
func TestRestoreClearsTheJournalRecordOfARestoredFile(t *testing.T) {
	ctx := context.Background()
	data, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	const f = "db/cpu/2026/01/01/00/cpu_1.parquet"
	if err := data.Write(ctx, f, []byte("P")); err != nil {
		t.Fatal(err)
	}
	backupDir := t.TempDir()
	res, err := newBackupManager(t, data, backupDir).CreateBackup(ctx, backup.BackupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dest, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	other := "db/cpu/2026/01/01/00/cpu_9.parquet"
	for _, p := range []string{f, other} {
		if err := dest.Write(ctx, storage.RetiredKey(p), []byte(`{"v":1,"path":"`+p+`"}`)); err != nil {
			t.Fatal(err)
		}
	}
	mgr := newBackupManager(t, dest, backupDir)
	if _, err := mgr.RestoreBackup(ctx, backup.RestoreOptions{BackupID: res.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatal(err)
	}
	if ok, _ := dest.Exists(ctx, storage.RetiredKey(f)); ok {
		t.Fatal("the record of a restored file survived the restore")
	}
	if ok, _ := dest.Exists(ctx, storage.RetiredKey(other)); !ok {
		t.Fatal("the restore cleared a record of a file it did not restore")
	}
}
