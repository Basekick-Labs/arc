package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

// Tests for the fields and helpers #762 adds. Kept apart from
// restore_incomplete_test.go so that file still compiles against pre-fix code.

func rewriteManifest(t *testing.T, backupDir, backupID string, edit func(*Manifest)) {
	t.Helper()
	p := filepath.Join(backupDir, backupID, "manifest.json")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	m, err := UnmarshalManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	edit(m)
	out, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, out, 0o600); err != nil {
		t.Fatal(err)
	}
}

// A backup that was incomplete when it was taken says so on the way in, whether
// or not data is being restored, and its own gap is not counted as missing.
func TestRestore_SurfacesIncompleteBackup(t *testing.T) {
	for _, restoreData := range []bool{true, false} {
		t.Run(fmt.Sprintf("restore_data=%v", restoreData), func(t *testing.T) {
			backupDir, backupID, _ := makeRestorableBackup(t, 7)
			rewriteManifest(t, backupDir, backupID, func(m *Manifest) {
				m.TotalFiles += 3 // inventoried but not copied at backup time
				m.SkippedFiles = 3
				m.UnaddressableFiles = 2
				m.UnaddressableSample = []string{"db/cpu/legacy\\name.parquet"}
			})
			m := &Manager{
				dataStorage:   mustLocalBackend(t, t.TempDir(), zerolog.Nop()),
				backupStorage: mustLocalBackend(t, backupDir, zerolog.Nop()),
				logger:        zerolog.Nop(),
			}
			if _, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: restoreData}); err != nil {
				t.Fatalf("RestoreBackup: %v", err)
			}
			p := m.GetProgress()
			if p.Status != "completed" {
				t.Errorf("Status = %q, want completed", p.Status)
			}
			if p.BackupSkippedFiles != 3 || p.BackupUnaddressableFiles != 2 {
				t.Errorf("backup-time counts = (%d, %d), want (3, 2)", p.BackupSkippedFiles, p.BackupUnaddressableFiles)
			}
			if p.MissingFiles != 0 {
				t.Errorf("MissingFiles = %d, want 0 (the backup's own skips are not missing)", p.MissingFiles)
			}
		})
	}
}

func TestRestore_MissingFilesAreCounted(t *testing.T) {
	backupDir, backupID, keys := makeRestorableBackup(t, 10)
	for _, k := range keys[:2] {
		if err := os.Remove(filepath.Join(backupDir, backupID, "data", filepath.FromSlash(k))); err != nil {
			t.Fatal(err)
		}
	}
	m, err := restoreInto(t, mustLocalBackend(t, backupDir, zerolog.Nop()), mustLocalBackend(t, t.TempDir(), zerolog.Nop()), backupID)
	if err == nil {
		t.Fatal("expected an incomplete restore")
	}
	if p := m.GetProgress(); p.MissingFiles != 2 {
		t.Errorf("MissingFiles = %d, want 2", p.MissingFiles)
	}
}

func TestRestore_SkippedSampleIsBounded(t *testing.T) {
	backupDir, backupID, keys := makeRestorableBackup(t, 60)
	fail := map[string]bool{}
	for _, k := range keys[:40] {
		fail[backupID+"/data/"+k] = true
	}
	backupStorage := &readFailingBackend{LocalBackend: mustLocalBackend(t, backupDir, zerolog.Nop()), failPaths: fail}
	m, err := restoreInto(t, backupStorage, mustLocalBackend(t, t.TempDir(), zerolog.Nop()), backupID)
	if err == nil {
		t.Fatal("expected an incomplete restore")
	}
	p := m.GetProgress()
	if p.SkippedFiles != 40 || p.ProcessedFiles != 20 {
		t.Errorf("counts = (skipped %d, restored %d), want (40, 20)", p.SkippedFiles, p.ProcessedFiles)
	}
	if len(p.SkippedSample) != unaddressableSampleCap {
		t.Errorf("len(SkippedSample) = %d, want %d", len(p.SkippedSample), unaddressableSampleCap)
	}
}

func TestIsRestoreReadError(t *testing.T) {
	readErr := errors.New("boom")
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain error", readErr, false},
		{"wrapped sentinel", fmt.Errorf("x: %w", errRestoreRead), true},
		{"classified source read", classifyReadTo("k", readErr, nil), true},
		{"classified destination failure", classifyReadTo("k", readErr, errors.New("disk full")), false},
	}
	for _, c := range cases {
		if got := isRestoreReadError(c.err); got != c.want {
			t.Errorf("%s: isRestoreReadError = %v, want %v", c.name, got, c.want)
		}
	}
}

type failingWriter struct{ errs []error }

func (f *failingWriter) Write(p []byte) (int, error) {
	if len(f.errs) == 0 {
		return len(p), nil
	}
	err := f.errs[0]
	f.errs = f.errs[1:]
	return 0, err
}

func TestTrackingWriterRecordsFirstError(t *testing.T) {
	e1, e2 := errors.New("first"), errors.New("second")
	tw := &trackingWriter{w: &failingWriter{errs: []error{e1, e2}}}
	_, _ = tw.Write([]byte("a"))
	_, _ = tw.Write([]byte("b"))
	if tw.err != e1 {
		t.Errorf("recorded %v, want the first error %v", tw.err, e1)
	}
	ok := &trackingWriter{w: &failingWriter{}}
	if _, err := ok.Write([]byte("a")); err != nil || ok.err != nil {
		t.Errorf("healthy writer recorded an error: %v / %v", err, ok.err)
	}
}

// Guard, not a regression test: restore already published immutable snapshots
// before #762. It pins that the sample slice is published safely under -race.
func TestGetProgress_DoesNotRaceALiveRestore(t *testing.T) {
	backupDir, backupID, keys := makeRestorableBackup(t, 200)
	fail := map[string]bool{}
	for _, k := range keys[:5] {
		fail[backupID+"/data/"+k] = true
	}
	backupStorage := &readFailingBackend{LocalBackend: mustLocalBackend(t, backupDir, zerolog.Nop()), failPaths: fail}
	m := &Manager{dataStorage: mustLocalBackend(t, t.TempDir(), zerolog.Nop()), backupStorage: backupStorage, logger: zerolog.Nop()}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			if p := m.GetProgress(); p != nil {
				_ = p.Status
				_ = p.SkippedFiles
				_ = p.MissingFiles
				for _, s := range p.SkippedSample {
					_ = s
				}
			}
		}
	}()
	_, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: true})
	close(done)
	wg.Wait()
	if err == nil {
		t.Fatal("expected an incomplete restore")
	}
	if p := m.GetProgress(); p.Status != "failed" || p.SkippedFiles != 5 {
		t.Errorf("final progress = (%s, skipped %d), want (failed, 5)", p.Status, p.SkippedFiles)
	}
}

// A data file the backup holds under a name the listing hides (an object store
// returned a dot-prefixed key at backup time) is reported as unaddressable and
// named, not as missing: renaming it in the backup recovers it.
func TestRestore_UnlistableBackupObjectsAreReportedNotMissing(t *testing.T) {
	backupDir, backupID, keys := makeRestorableBackup(t, 5)
	dir := filepath.Join(backupDir, backupID, "data", filepath.Dir(filepath.FromSlash(keys[0])))
	from := filepath.Join(dir, filepath.Base(keys[0]))
	to := filepath.Join(dir, "._"+filepath.Base(keys[0]))
	if err := os.Rename(from, to); err != nil {
		t.Fatal(err)
	}
	m, err := restoreInto(t, mustLocalBackend(t, backupDir, zerolog.Nop()), mustLocalBackend(t, t.TempDir(), zerolog.Nop()), backupID)
	if err == nil {
		t.Fatal("expected an incomplete restore")
	}
	p := m.GetProgress()
	if p.Status != "failed" {
		t.Errorf("Status = %q, want failed", p.Status)
	}
	if p.UnaddressableFiles != 1 || p.MissingFiles != 0 {
		t.Errorf("counts = (unaddressable %d, missing %d), want (1, 0)", p.UnaddressableFiles, p.MissingFiles)
	}
	want := backupID + "/data/" + filepath.ToSlash(filepath.Dir(keys[0])) + "/._" + filepath.Base(keys[0])
	if len(p.UnaddressableSample) != 1 || p.UnaddressableSample[0] != want {
		t.Errorf("UnaddressableSample = %v, want [%s]", p.UnaddressableSample, want)
	}
	if !strings.Contains(p.Error, "no listing returns") || strings.Contains(p.Error, "absent from backup storage") {
		t.Errorf("Error = %q, want the unaddressable wording and not the missing wording", p.Error)
	}
	if p.ProcessedFiles != 4 {
		t.Errorf("ProcessedFiles = %d, want 4", p.ProcessedFiles)
	}
}

// makeBackupWithSkippedMetadata backs up n parquet files plus one Iceberg
// metadata file whose source read fails, so the backup records a metadata skip.
func makeBackupWithSkippedMetadata(t *testing.T, n int) (string, string, []string, *Manifest) {
	t.Helper()
	ctx := context.Background()
	srcDir := t.TempDir()
	src := mustLocalBackend(t, srcDir, zerolog.Nop())
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("db/cpu/2026/09/13/00/f_%03d.parquet", i)
		if err := src.Write(ctx, k, []byte("parquet")); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	meta := "arc_db.db/cpu/metadata/v1.metadata.json"
	if err := src.Write(ctx, meta, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	backupDir := t.TempDir()
	m := &Manager{
		dataStorage:   &readFailingBackend{LocalBackend: src, failPaths: map[string]bool{meta: true}},
		backupStorage: mustLocalBackend(t, backupDir, zerolog.Nop()),
		logger:        zerolog.Nop(),
	}
	res, err := m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	return backupDir, res.Manifest.BackupID, keys, res.Manifest
}

// The manifest's skipped_files must count the same population as total_files
// (data files); an Iceberg metadata skip goes in skipped_metadata_files.
func TestBackup_ManifestSkippedFilesCountsDataOnly(t *testing.T) {
	_, _, keys, manifest := makeBackupWithSkippedMetadata(t, 20)
	if manifest.TotalFiles != int64(len(keys)) {
		t.Errorf("TotalFiles = %d, want %d", manifest.TotalFiles, len(keys))
	}
	if manifest.SkippedFiles != 0 {
		t.Errorf("SkippedFiles = %d, want 0 (metadata skips are not data skips)", manifest.SkippedFiles)
	}
	if manifest.SkippedMetadataFiles != 1 {
		t.Errorf("SkippedMetadataFiles = %d, want 1", manifest.SkippedMetadataFiles)
	}
}

// With the counts conflated, one metadata skip let one missing parquet file go
// undetected: expected was deflated by exactly that much.
func TestRestore_MetadataSkipsDoNotMaskMissingDataFiles(t *testing.T) {
	backupDir, backupID, keys, _ := makeBackupWithSkippedMetadata(t, 20)
	if err := os.Remove(filepath.Join(backupDir, backupID, "data", filepath.FromSlash(keys[1]))); err != nil {
		t.Fatal(err)
	}
	m, err := restoreInto(t, mustLocalBackend(t, backupDir, zerolog.Nop()), mustLocalBackend(t, t.TempDir(), zerolog.Nop()), backupID)
	if err == nil {
		t.Fatal("a missing data file went undetected behind a metadata skip")
	}
	if p := m.GetProgress(); p.MissingFiles != 1 || p.Status != "failed" {
		t.Errorf("progress = (%s, missing %d), want (failed, 1)", p.Status, p.MissingFiles)
	}
}

// A ReadTo failure caused by the temp file (full temp filesystem) is fatal,
// not a skipped object. The temp file is opened read-only so its writes fail.
func TestRestore_TempWriteFailureIsFatalNotSkipped(t *testing.T) {
	backupDir, backupID, _ := makeRestorableBackup(t, 3)
	orig := createRestoreTemp
	t.Cleanup(func() { createRestoreTemp = orig })
	createRestoreTemp = func() (*os.File, error) {
		p := filepath.Join(t.TempDir(), "ro.parquet")
		if err := os.WriteFile(p, nil, 0o600); err != nil {
			return nil, err
		}
		return os.OpenFile(p, os.O_RDONLY, 0)
	}
	m, err := restoreInto(t, mustLocalBackend(t, backupDir, zerolog.Nop()), mustLocalBackend(t, t.TempDir(), zerolog.Nop()), backupID)
	if err == nil {
		t.Fatal("expected a fatal temp-write failure")
	}
	p := m.GetProgress()
	if p.Status != "failed" || !strings.Contains(p.Error, "failed to write temp file") {
		t.Errorf("progress = (%s, %q), want failed with the temp-write error", p.Status, p.Error)
	}
	if p.SkippedFiles != 0 || p.ProcessedFiles != 0 {
		t.Errorf("counts = (skipped %d, restored %d), want (0, 0)", p.SkippedFiles, p.ProcessedFiles)
	}
}

// Skips accumulated before a fatal abort are still published, so the status
// names the unreadable objects even when something else ended the restore.
func TestRestore_SkipsArePublishedBeforeFatalAbort(t *testing.T) {
	backupDir, backupID, keys := makeRestorableBackup(t, 10)
	bad := backupID + "/data/" + keys[0]
	backupStorage := &readFailingBackend{LocalBackend: mustLocalBackend(t, backupDir, zerolog.Nop()), failPaths: map[string]bool{bad: true}}
	destDir := t.TempDir()
	dest := &keyedPartialWriteBackend{LocalBackend: mustLocalBackend(t, destDir, zerolog.Nop()), dir: destDir, failKey: keys[5]}
	m, err := restoreInto(t, backupStorage, dest, backupID)
	if err == nil {
		t.Fatal("expected a fatal write failure")
	}
	p := m.GetProgress()
	if !strings.Contains(p.Error, "failed to write to data storage") {
		t.Errorf("Error = %q, want the write failure to win", p.Error)
	}
	if p.SkippedFiles != 1 || len(p.SkippedSample) != 1 || p.SkippedSample[0] != bad {
		t.Errorf("skips not published before the abort: skipped=%d sample=%v", p.SkippedFiles, p.SkippedSample)
	}
}
