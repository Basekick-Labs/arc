package backup

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// writeRaw puts a file straight on disk, bypassing Backend.Write, because the
// whole point is a file whose key Backend cannot address.
func writeRaw(t *testing.T, root, rel, body string) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
		t.Skipf("filesystem will not hold %q: %v", rel, err)
	}
}

func newBackupManager(t *testing.T, dataDir string) *Manager {
	t.Helper()
	data, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(&ManagerConfig{
		DataStorage: data,
		BackupPath:  t.TempDir(),
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// A backup that could not copy a file which exists in storage must say so.
//
// This is the regression for #756 and it fails against the code before it. The
// file is a real Parquet file holding rows, the query path still serves it, and
// every mechanism that makes an incomplete backup visible (SkippedFiles, the
// "backup will be incomplete" warning, checkSkipRatio) reads the inventory,
// which by construction cannot contain it. The backup copied one file out of
// two and reported a clean success.
func TestBackupReportsFilesItCouldNotAddress(t *testing.T) {
	dataDir := t.TempDir()
	writeRaw(t, dataDir, "db/cpu/2026/09/12/13/good.parquet", "PAR1-good")
	writeRaw(t, dataDir, "db/cpu/2026/09/12/13/ba\\d.parquet", "PAR1-rows-that-exist")

	m := newBackupManager(t, dataDir)
	res, err := m.CreateBackup(context.Background(), BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}

	if res.Manifest.TotalFiles != 1 {
		t.Fatalf("premise: exactly one file is addressable, got TotalFiles=%d", res.Manifest.TotalFiles)
	}
	if res.Manifest.UnaddressableFiles != 1 {
		t.Errorf("UnaddressableFiles = %d, want 1; the backup is missing a file and does not say so",
			res.Manifest.UnaddressableFiles)
	}
	if len(res.Manifest.UnaddressableSample) != 1 ||
		!strings.Contains(res.Manifest.UnaddressableSample[0], "ba\\d.parquet") {
		t.Errorf("UnaddressableSample = %v, want it to name the file so an operator can find it",
			res.Manifest.UnaddressableSample)
	}
}

// The case the skip-ratio guard structurally cannot see: every data file is
// unaddressable, so the inventory is empty, so `totalFiles == 0` short-circuits
// and the run reports completed over a backup containing no data at all.
func TestBackupFailsWhenNoDataFileIsAddressable(t *testing.T) {
	dataDir := t.TempDir()
	for _, name := range []string{"a", "b", "c"} {
		writeRaw(t, dataDir, "db/cpu/2026/09/12/13/"+name+"\\x.parquet", "PAR1-rows")
	}

	m := newBackupManager(t, dataDir)
	_, err := m.CreateBackup(context.Background(), BackupOptions{})
	if err == nil {
		t.Fatal("a backup containing no data must fail, not report success")
	}
	if !strings.Contains(err.Error(), "cannot be addressed") {
		t.Errorf("error %q should name the cause and the remedy", err)
	}
	if p := m.GetProgress(); p != nil && p.Status == "completed" {
		t.Errorf("progress status = %q, want it not to be completed", p.Status)
	}
}

// A healthy store reports nothing, so the field stays absent from the manifest
// JSON and this cannot cry wolf on every deployment.
func TestBackupReportsNothingOnAHealthyStore(t *testing.T) {
	dataDir := t.TempDir()
	writeRaw(t, dataDir, "db/cpu/2026/09/12/13/good.parquet", "PAR1-good")
	// In-flight write and an ordinary staging partial: both are normal and
	// neither is a file an operator lost.
	writeRaw(t, dataDir, "db/cpu/2026/09/12/13/.arc-9988.tmp", "in-flight")
	writeRaw(t, dataDir, "db/cpu/2026/09/12/13/good.parquet.part", "staging")

	m := newBackupManager(t, dataDir)
	res, err := m.CreateBackup(context.Background(), BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if res.Manifest.UnaddressableFiles != 0 {
		t.Errorf("UnaddressableFiles = %d on a healthy store, want 0 (sample: %v)",
			res.Manifest.UnaddressableFiles, res.Manifest.UnaddressableSample)
	}
	if res.Manifest.TotalFiles != 1 {
		t.Errorf("TotalFiles = %d, want 1", res.Manifest.TotalFiles)
	}
}

// Non-data debris is hidden from listings for good reason and must not be
// reported as data loss.
func TestBackupIgnoresNonDataDebris(t *testing.T) {
	dataDir := t.TempDir()
	writeRaw(t, dataDir, "db/cpu/2026/09/12/13/good.parquet", "PAR1-good")
	writeRaw(t, dataDir, "db/cpu/2026/09/12/13/.DS_Store", "macos debris")

	m := newBackupManager(t, dataDir)
	res, err := m.CreateBackup(context.Background(), BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if res.Manifest.UnaddressableFiles != 0 {
		t.Errorf("UnaddressableFiles = %d, want 0; .DS_Store is not data (sample: %v)",
			res.Manifest.UnaddressableFiles, res.Manifest.UnaddressableSample)
	}
}

// The sample is bounded: the over-length key shape makes each path up to a
// kilobyte, and the manifest is a single JSON blob.
func TestUnaddressableSampleIsBounded(t *testing.T) {
	dataDir := t.TempDir()
	writeRaw(t, dataDir, "db/cpu/2026/09/12/13/good.parquet", "PAR1-good")
	for i := 0; i < unaddressableSampleCap+10; i++ {
		writeRaw(t, dataDir, "db/cpu/2026/09/12/13/b"+string(rune('a'+i%26))+strings.Repeat("z", i)+"\\x.parquet", "rows")
	}

	m := newBackupManager(t, dataDir)
	res, err := m.CreateBackup(context.Background(), BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if res.Manifest.UnaddressableFiles <= int64(unaddressableSampleCap) {
		t.Fatalf("premise: need more than the cap, got %d", res.Manifest.UnaddressableFiles)
	}
	if len(res.Manifest.UnaddressableSample) != unaddressableSampleCap {
		t.Errorf("sample length = %d, want it capped at %d",
			len(res.Manifest.UnaddressableSample), unaddressableSampleCap)
	}
}

// Iceberg metadata is a backup payload too, and an unaddressable one is worse
// than an unaddressable data file: losing a metadata.json loses a whole table
// even when every Parquet file it references survives. A ".parquet"-only filter
// on the hidden set threw these away, so they were reported by nothing at all.
func TestBackupReportsUnaddressableIcebergMetadata(t *testing.T) {
	dataDir := t.TempDir()
	writeRaw(t, dataDir, "db/cpu/2026/09/12/13/good.parquet", "PAR1-good")
	writeRaw(t, dataDir, "arc_db.db/cpu/metadata/00001-ba\\d.metadata.json", "{\"iceberg\":true}")

	m := newBackupManager(t, dataDir)
	res, err := m.CreateBackup(context.Background(), BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if res.Manifest.UnaddressableFiles != 1 {
		t.Fatalf("UnaddressableFiles = %d, want 1; unaddressable Iceberg metadata must be reported (sample %v)",
			res.Manifest.UnaddressableFiles, res.Manifest.UnaddressableSample)
	}
	if len(res.Manifest.UnaddressableSample) != 1 ||
		!strings.Contains(res.Manifest.UnaddressableSample[0], "metadata.json") {
		t.Errorf("sample = %v, want it to name the metadata file", res.Manifest.UnaddressableSample)
	}
}

// The fatal case must be decided before any copying, or a run that is going to
// fail still writes a partial backupID/data/ tree that ListBackups can neither
// show nor clean up, because it keys on the manifest that never gets written.
func TestNoDataFailureLeavesNothingBehind(t *testing.T) {
	dataDir := t.TempDir()
	writeRaw(t, dataDir, "db/cpu/2026/09/12/13/a\\x.parquet", "PAR1-rows")
	// Iceberg metadata IS addressable, so the old ordering would have copied it
	// all and only then failed.
	writeRaw(t, dataDir, "arc_db.db/cpu/metadata/00001-x.metadata.json", "{}")

	backupDir := t.TempDir()
	data, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(&ManagerConfig{DataStorage: data, BackupPath: backupDir, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := m.CreateBackup(context.Background(), BackupOptions{}); err == nil {
		t.Fatal("a backup with no addressable data file must fail")
	}

	// Nothing copied means nothing to orphan.
	dest, err := storage.NewLocalBackend(backupDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	left, err := dest.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List backup dir: %v", err)
	}
	if len(left) != 0 {
		t.Errorf("the failed run left %v in backup storage, with no manifest to find it by", left)
	}
}
