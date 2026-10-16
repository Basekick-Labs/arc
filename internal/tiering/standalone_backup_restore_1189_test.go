package tiering

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestStandaloneRestorePreservesColdCopyWhenLegacyBackupHasNoSidecar(t *testing.T) {
	const path = gateDailyA
	fixture := newStandaloneLegacyRestoreFixture(t,
		map[string][]byte{path: []byte("PAR1-from-legacy-backup")},
		map[string][]byte{path: []byte("PAR1-existing-cold-copy")}, true)
	if err := fixture.restore(); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	fixture.assertBytes(path, fixture.cold, []byte("PAR1-existing-cold-copy"))
	fixture.assertAbsent(path, fixture.hot)
	fixture.assertCompleted(1, 0)
}

func TestStandaloneLegacyRestoreRestoresWhenColdObjectIsAbsent(t *testing.T) {
	const path = gateDailyA
	body := []byte("PAR1-legacy-file")
	fixture := newStandaloneLegacyRestoreFixture(t, map[string][]byte{path: body}, nil, true)
	if err := fixture.restore(); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	fixture.assertBytes(path, fixture.hot, fixture.backupBodies[path])
	fixture.assertAbsent(path, fixture.cold)
	fixture.assertCompleted(0, 0)
}

func TestStandaloneLegacyRestoreHandlesMixedObjectsAndRepeatedRuns(t *testing.T) {
	const coldPath = gateDailyA
	const hotPath = gateDailyB
	backupColdBytes := []byte("PAR1-cold-source-from-legacy-backup")
	backupHotBytes := []byte("PAR1-hot-source-from-legacy-backup")
	coldBytes := []byte("PAR1-existing-cold-object")
	fixture := newStandaloneLegacyRestoreFixture(t,
		map[string][]byte{coldPath: backupColdBytes, hotPath: backupHotBytes},
		map[string][]byte{coldPath: coldBytes}, true)

	for run := 1; run <= 2; run++ {
		if err := fixture.restore(); err != nil {
			t.Fatalf("restore run %d: %v", run, err)
		}
		fixture.assertBytes(coldPath, fixture.cold, coldBytes)
		fixture.assertAbsent(coldPath, fixture.hot)
		fixture.assertBytes(hotPath, fixture.hot, backupHotBytes)
		fixture.assertAbsent(hotPath, fixture.cold)
		fixture.assertCompleted(1, 0)
	}
}

func TestStandaloneLegacyRestoreKeepsCurrentSidecarBehavior(t *testing.T) {
	const path = gateDailyA
	body := []byte("PAR1-current-backup-contract")
	fixture := newStandaloneLegacyRestoreFixture(t, map[string][]byte{path: body}, map[string][]byte{path: body}, false)
	if err := fixture.restore(); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	fixture.assertBytes(path, fixture.cold, body)
	fixture.assertAbsent(path, fixture.hot)
	fixture.assertCompleted(0, 1)
}

func TestStandaloneLegacyRestoreReadAndWriteFailuresAreReported(t *testing.T) {
	t.Run("cold metadata read", func(t *testing.T) {
		fixture := oneColdLegacyRestoreFixture(t)
		errRead := errors.New("injected cold metadata read failure")
		fixture.backupManager.SetColdSource(failingColdRows{ColdSource: fixture.tierManager, err: errRead})
		fixture.assertRestoreFailure()
	})

	t.Run("cold object existence read", func(t *testing.T) {
		fixture := oneCurrentColdRestoreFixture(t)
		fixture.backupManager.SetColdSource(failingColdBackendSource{
			ColdSource: fixture.tierManager,
			backend:    failingColdBackend{Backend: fixture.cold, existsErr: errors.New("injected cold Exists failure")},
		})
		fixture.assertRestoreFailure()
	})

	t.Run("cold object size read", func(t *testing.T) {
		fixture := oneCurrentColdRestoreFixture(t)
		fixture.backupManager.SetColdSource(failingColdBackendSource{
			ColdSource: fixture.tierManager,
			backend:    failingColdBackend{Backend: fixture.cold, statErr: errors.New("injected cold StatFile failure")},
		})
		fixture.assertRestoreFailure()
	})

	t.Run("missing legacy backup object", func(t *testing.T) {
		const path = gateDailyA
		fixture := newStandaloneLegacyRestoreFixture(t, map[string][]byte{path: []byte("PAR1-from-legacy-backup")}, nil, true)
		backupPath := fixture.backupID + "/data/" + path
		if err := fixture.backupStore.Delete(context.Background(), backupPath); err != nil {
			t.Fatalf("remove backup object to inject read failure: %v", err)
		}
		fixture.assertRestoreFailure()
		if got := fixture.backupManager.GetProgress().MissingFiles; got != 1 {
			t.Fatalf("missing files = %d, want 1 for the unavailable backup object", got)
		}
	})

	t.Run("hot destination write", func(t *testing.T) {
		const path = gateDailyA
		fixture := newStandaloneLegacyRestoreFixture(t, map[string][]byte{path: []byte("PAR1-from-legacy-backup")}, nil, true)
		restoreManager, err := backup.NewManager(&backup.ManagerConfig{
			DataStorage: refusingRestoreWrite{Backend: fixture.hot, err: errors.New("injected hot restore write failure")},
			BackupPath:  fixture.backupDir,
			Logger:      zerolog.Nop(),
		})
		if err != nil {
			t.Fatalf("create restore manager: %v", err)
		}
		restoreManager.SetColdSource(fixture.tierManager)
		fixture.backupManager = restoreManager
		fixture.assertRestoreFailure()
		fixture.assertAbsent(path, fixture.hot)
		fixture.assertAbsent(path, fixture.cold)
	})
}

type standaloneLegacyRestoreFixture struct {
	t             *testing.T
	ctx           context.Context
	tierManager   *Manager
	backupManager *backup.Manager
	backupStore   *storage.LocalBackend
	backupDir     string
	backupID      string
	hot           storage.Backend
	cold          storage.Backend
	backupBodies  map[string][]byte
	coldBodies    map[string][]byte
}

func newStandaloneLegacyRestoreFixture(t *testing.T, backupBodies, coldBodies map[string][]byte, legacy bool) *standaloneLegacyRestoreFixture {
	t.Helper()
	ctx := context.Background()
	tierManager, _, _, cleanup := setupIntegrationTest(t, true)
	t.Cleanup(cleanup)

	workDir := t.TempDir()
	if stateDir := os.Getenv("ARC_1189_TEST_STATE_DIR"); stateDir != "" {
		var err error
		workDir, err = os.MkdirTemp(stateDir, "issue1189-")
		if err != nil {
			t.Fatalf("create isolated test data directory in mounted project volume: %v", err)
		}
		t.Cleanup(func() {
			if err := os.RemoveAll(workDir); err != nil {
				t.Errorf("remove this test's isolated work directory: %v", err)
			}
		})
	}
	hot, err := storage.NewLocalBackend(filepath.Join(workDir, "hot"), zerolog.Nop())
	if err != nil {
		t.Fatalf("create local hot backend: %v", err)
	}
	cold, err := storage.NewLocalBackend(filepath.Join(workDir, "cold"), zerolog.Nop())
	if err != nil {
		t.Fatalf("create local cold backend: %v", err)
	}
	tierManager.hotBackend = hot
	tierManager.coldBackend = cold
	t.Cleanup(func() { _ = cold.Close(); _ = hot.Close() })

	backups := make(map[string][]byte, len(backupBodies))
	for path, body := range backupBodies {
		body = existingInstallBytes(t, path, body)
		backups[path] = append([]byte(nil), body...)
		if err := hot.Write(ctx, path, body); err != nil {
			t.Fatalf("write hot source %s: %v", path, err)
		}
	}

	backupDir := filepath.Join(workDir, "backups")
	backupManager, err := backup.NewManager(&backup.ManagerConfig{DataStorage: hot, BackupPath: backupDir, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatalf("create backup manager: %v", err)
	}
	created, err := backupManager.CreateBackup(ctx, backup.BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	backupStore, err := storage.NewLocalBackend(backupDir, zerolog.Nop())
	if err != nil {
		t.Fatalf("open backup store: %v", err)
	}
	t.Cleanup(func() { _ = backupStore.Close() })
	if legacy {
		if err := backupStore.Delete(ctx, created.Manifest.BackupID+"/manifest-files.json"); err != nil {
			t.Fatalf("remove sidecar to model a pre-stage-C backup: %v", err)
		}
	}

	for path := range backups {
		if err := hot.Delete(ctx, path); err != nil {
			t.Fatalf("remove hot copy before restore %s: %v", path, err)
		}
		recordHotRow(t, tierManager, path, time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC))
	}
	for path, body := range coldBodies {
		if err := cold.Write(ctx, path, body); err != nil {
			t.Fatalf("write existing cold object %s: %v", path, err)
		}
	}
	scan, err := tierManager.ScanTiers(ctx)
	if err != nil {
		t.Fatalf("standalone ScanTiers: %v", err)
	}
	if scan.ColdSynced != len(coldBodies) || scan.ColdSyncFailed {
		t.Fatalf("standalone ScanTiers = %+v, want %d cold rows synced without failure", scan, len(coldBodies))
	}
	for path := range coldBodies {
		if got := fileMeta(t, tierManager, path).Tier; got != TierCold {
			t.Fatalf("tier for %s = %s, want cold after standalone scan", path, got)
		}
	}
	backupManager.SetColdSource(tierManager)
	return &standaloneLegacyRestoreFixture{
		t:   t,
		ctx: ctx, tierManager: tierManager, backupManager: backupManager,
		backupStore: backupStore, backupDir: backupDir, backupID: created.Manifest.BackupID,
		hot: hot, cold: cold, backupBodies: backups, coldBodies: coldBodies,
	}
}

func existingInstallBytes(t *testing.T, path string, fallback []byte) []byte {
	t.Helper()
	legacyFixture := bytes.Equal(fallback, []byte("PAR1-from-legacy-backup")) || bytes.Equal(fallback, []byte("PAR1-cold-source-from-legacy-backup"))
	if os.Getenv("ARC_1189_INSTALL_STATE") != "existing" || path != gateDailyA || !legacyFixture {
		return fallback
	}
	fixturePath := os.Getenv("ARC_1189_EXISTING_FIXTURE")
	if fixturePath == "" {
		t.Fatal("existing-data Docker run did not provide the copied Arc Parquet fixture")
	}
	data, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read copied existing-installation fixture: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("existing-installation Parquet fixture is empty")
	}
	t.Logf("using %d bytes from the read-only existing Arc installation fixture", len(data))
	return data
}

func oneColdLegacyRestoreFixture(t *testing.T) *standaloneLegacyRestoreFixture {
	t.Helper()
	const path = gateDailyA
	return newStandaloneLegacyRestoreFixture(t,
		map[string][]byte{path: []byte("PAR1-from-legacy-backup")},
		map[string][]byte{path: []byte("PAR1-existing-cold-copy")}, true)
}

func oneCurrentColdRestoreFixture(t *testing.T) *standaloneLegacyRestoreFixture {
	t.Helper()
	const path = gateDailyA
	body := []byte("PAR1-current-backup-contract")
	return newStandaloneLegacyRestoreFixture(t, map[string][]byte{path: body}, map[string][]byte{path: body}, false)
}

func (f *standaloneLegacyRestoreFixture) restore() error {
	_, err := f.backupManager.RestoreBackup(f.ctx, backup.RestoreOptions{BackupID: f.backupID, RestoreData: true})
	return err
}

func (f *standaloneLegacyRestoreFixture) assertBytes(path string, backend storage.Backend, want []byte) {
	f.t.Helper()
	got, err := backend.Read(f.ctx, path)
	if err != nil {
		f.t.Fatalf("read %s: %v", path, err)
	}
	if !bytes.Equal(got, want) {
		f.t.Fatalf("bytes at %s = %q, want %q", path, got, want)
	}
}

func (f *standaloneLegacyRestoreFixture) assertAbsent(path string, backend storage.Backend) {
	f.t.Helper()
	exists, err := backend.Exists(f.ctx, path)
	if err != nil {
		f.t.Fatalf("check %s: %v", path, err)
	}
	if exists {
		f.t.Fatalf("%s unexpectedly exists", path)
	}
}

func (f *standaloneLegacyRestoreFixture) assertCompleted(skippedUnverifiable, alreadyCold int64) {
	f.t.Helper()
	progress := f.backupManager.GetProgress()
	if progress == nil || progress.Status != "completed" {
		f.t.Fatalf("restore status = %v, want completed", progress)
	}
	if progress.ColdRowsSkippedUnverifiable != skippedUnverifiable || progress.ColdFilesSkippedAlreadyCold != alreadyCold {
		f.t.Fatalf("restore counters (unverifiable=%d, already-cold=%d), want (%d, %d)", progress.ColdRowsSkippedUnverifiable, progress.ColdFilesSkippedAlreadyCold, skippedUnverifiable, alreadyCold)
	}
}

func (f *standaloneLegacyRestoreFixture) assertRestoreFailure() {
	f.t.Helper()
	err := f.restore()
	if err == nil {
		f.t.Fatal("restore should fail after the injected read/write error")
	}
	progress := f.backupManager.GetProgress()
	if progress == nil || progress.Status != "failed" || progress.Error == "" {
		f.t.Fatalf("restore failure was not reflected in progress: %v", progress)
	}
	f.assertAbsent(gateDailyA, f.hot)
	for path, want := range f.coldBodies {
		f.assertBytes(path, f.cold, want)
	}
}

type failingColdRows struct {
	backup.ColdSource
	err error
}

type failingColdBackendSource struct {
	backup.ColdSource
	backend storage.Backend
}

func (s failingColdBackendSource) ColdBackend() storage.Backend { return s.backend }

func (s failingColdRows) ColdRows(context.Context) (map[string]int64, error) {
	return nil, s.err
}

type failingColdBackend struct {
	storage.Backend
	existsErr error
	statErr   error
}

func (b failingColdBackend) Exists(ctx context.Context, path string) (bool, error) {
	if b.existsErr != nil {
		return false, b.existsErr
	}
	return b.Backend.Exists(ctx, path)
}

func (b failingColdBackend) StatFile(ctx context.Context, path string) (int64, error) {
	if b.statErr != nil {
		return 0, b.statErr
	}
	return b.Backend.StatFile(ctx, path)
}

type refusingRestoreWrite struct {
	storage.Backend
	err error
}

func (b refusingRestoreWrite) WriteReader(context.Context, string, io.Reader, int64) error {
	return b.err
}
