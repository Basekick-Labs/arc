package backup

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	_ "github.com/mattn/go-sqlite3"

	"github.com/basekick-labs/arc/internal/storage"
)

func TestIsIcebergDiscoveryFile(t *testing.T) {
	for key, want := range map[string]bool{
		"arc_db.db/cpu/metadata/version-hint.text":         true,
		"arc_db.db/cpu/metadata/v0.metadata.json":          true,
		"arc_db.db/cpu/metadata/v24.metadata.json":         true,
		"wh/arc_db.db/cpu/metadata/v3.metadata.json":       true, // warehouse in a subdirectory
		"arc_hub.db/spoke1/cpu/metadata/version-hint.text": true, // spoke namespace
		"arc_%22d.b%22.db/cpu/metadata/v3.metadata.json":   true, // encoded namespace directory
		"arc_db.db/cpu/metadata/00003-uuid.metadata.json":  false,
		"arc_db.db/cpu/metadata/abcd-m1.avro":              false,
		"arc_db.db/cpu/metadata/v+3.metadata.json":         false,
		"arc_db.db/cpu/metadata/v-1.metadata.json":         false,
		"arc_db.db/cpu/metadata/v.metadata.json":           false,
		"arc_db.db/cpu/metadata/v3.metadata.json.tmp":      false,
		"arc_db.db/cpu/metadata/sub/v3.metadata.json":      false,
		"arc_db.db/cpu/notmetadata/version-hint.text":      false,
		"db/cpu/2026/11/21/10/version-hint.text":           false,
		"db/cpu/2026/11/21/10/cpu_20261121_100000.parquet": false,
	} {
		if got := isIcebergDiscoveryFile(key); got != want {
			t.Errorf("isIcebergDiscoveryFile(%q) = %v, want %v", key, got, want)
		}
	}
}

const tblMeta = "arc_prod.db/sensors/metadata/"

func writeAll(t *testing.T, b storage.Backend, files map[string]string) {
	t.Helper()
	for k, v := range files {
		if err := b.Write(context.Background(), k, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
}

func readOrEmpty(b storage.Backend, k string) string {
	got, err := b.Read(context.Background(), k)
	if err != nil {
		return ""
	}
	return string(got)
}

// backupAtV3 lays out a table at v3 and backs it up; the caller then moves
// the live table on.
func backupAtV3(t *testing.T, data storage.Backend, mgr *Manager) string {
	t.Helper()
	writeAll(t, data, map[string]string{
		"prod/sensors/2026/07/14/15/sensors_1.parquet": "PAR1",
		tblMeta + "00003-a.metadata.json":              "meta 3",
		tblMeta + "abcd-m1.avro":                       "avro",
		tblMeta + "v0.metadata.json":                   "v0 old",
		tblMeta + "v3.metadata.json":                   "v3 old",
		tblMeta + "version-hint.text":                  "3",
	})
	res, err := mgr.CreateBackup(context.Background(), BackupOptions{IncludeMetadata: false, IncludeConfig: false})
	if err != nil {
		t.Fatal(err)
	}
	return res.Manifest.BackupID
}

// The table moved on since the backup: a restore leaves its live hint, and
// every v<N> of that directory, as they are, and still restores the rest
// (#1813). Before, the hint went back to 3 and directory readers read the
// backup's table until the next commit or restart.
func TestRestore_KeepsALiveTablesDiscoveryFiles(t *testing.T) {
	ctx := context.Background()
	mgr, data := newWarehouseManager(t, t.TempDir(), t.TempDir(), "")
	id := backupAtV3(t, data, mgr)

	// Live: at v24, v0..v9 pruned, its own v3 gone too.
	for _, k := range []string{tblMeta + "v0.metadata.json", tblMeta + "v3.metadata.json", tblMeta + "00003-a.metadata.json"} {
		if err := data.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	writeAll(t, data, map[string]string{tblMeta + "v24.metadata.json": "v24 live", tblMeta + "version-hint.text": "24"})

	if _, err := mgr.RestoreBackup(ctx, RestoreOptions{BackupID: id, RestoreData: true}); err != nil {
		t.Fatal(err)
	}
	if got := readOrEmpty(data, tblMeta+"version-hint.text"); got != "24" {
		t.Fatalf("live hint = %q after the restore, want 24", got)
	}
	for _, k := range []string{tblMeta + "v0.metadata.json", tblMeta + "v3.metadata.json"} {
		if ok, _ := data.Exists(ctx, k); ok {
			t.Errorf("%s restored beside a live hint", k)
		}
	}
	if got := readOrEmpty(data, tblMeta+"00003-a.metadata.json"); got != "meta 3" {
		t.Errorf("the backup's 0000N metadata file not restored: %q", got)
	}
	if got := readOrEmpty(data, "prod/sensors/2026/07/14/15/sensors_1.parquet"); got != "PAR1" {
		t.Errorf("parquet not restored: %q", got)
	}
	p := mgr.GetProgress()
	if p == nil || p.IcebergDiscoveryFilesKept != 3 {
		t.Fatalf("progress = %+v, want 3 discovery files kept", p)
	}
}

// A directory with no live hint gets the backup's discovery files, but never
// over one already there.
func TestRestore_DiscoveryFilesWithoutALiveHint(t *testing.T) {
	ctx := context.Background()
	mgr, data := newWarehouseManager(t, t.TempDir(), t.TempDir(), "")
	id := backupAtV3(t, data, mgr)

	if err := data.Delete(ctx, tblMeta+"version-hint.text"); err != nil {
		t.Fatal(err)
	}
	if err := data.Delete(ctx, tblMeta+"v0.metadata.json"); err != nil {
		t.Fatal(err)
	}
	writeAll(t, data, map[string]string{tblMeta + "v3.metadata.json": "v3 other lineage"})

	if _, err := mgr.RestoreBackup(ctx, RestoreOptions{BackupID: id, RestoreData: true}); err != nil {
		t.Fatal(err)
	}
	if got := readOrEmpty(data, tblMeta+"version-hint.text"); got != "3" {
		t.Errorf("hint = %q, want the backup's 3 where there was none", got)
	}
	if got := readOrEmpty(data, tblMeta+"v0.metadata.json"); got != "v0 old" {
		t.Errorf("absent v0 = %q, want restored", got)
	}
	if got := readOrEmpty(data, tblMeta+"v3.metadata.json"); got != "v3 other lineage" {
		t.Errorf("existing v3 = %q, want left as it was", got)
	}
}

// existsFails fails Exists for one key.
type existsFails struct {
	*storage.LocalBackend
	key string
}

func (e *existsFails) Exists(ctx context.Context, key string) (bool, error) {
	if key == e.key {
		return false, errors.New("injected exists failure")
	}
	return e.LocalBackend.Exists(ctx, key)
}

// A hint check that fails keeps the directory's files: overwriting a live
// hint is the harm, and a skipped file is counted.
func TestRestore_DiscoveryCheckFailureKeeps(t *testing.T) {
	ctx := context.Background()
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	data := &existsFails{LocalBackend: local, key: tblMeta + "version-hint.text"}
	mgr, err := NewManager(&ManagerConfig{DataStorage: data, BackupPath: t.TempDir(), Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	id := backupAtV3(t, data, mgr)
	for _, k := range []string{tblMeta + "v0.metadata.json", tblMeta + "v3.metadata.json"} {
		if err := data.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	writeAll(t, data, map[string]string{tblMeta + "version-hint.text": "24"})
	if _, err := mgr.RestoreBackup(ctx, RestoreOptions{BackupID: id, RestoreData: true}); err != nil {
		t.Fatal(err)
	}
	if got := readOrEmpty(data, tblMeta+"version-hint.text"); got != "24" {
		t.Fatalf("hint = %q after a failed check, want the live 24", got)
	}
	// The absent v<N> stay out too: the directory may hold a live hint.
	for _, k := range []string{tblMeta + "v0.metadata.json", tblMeta + "v3.metadata.json"} {
		if ok, _ := local.Exists(ctx, k); ok {
			t.Errorf("%s restored although the hint check failed", k)
		}
	}
}

// fakePauser records the pause and the resume.
type fakePauser struct {
	mu       sync.Mutex
	paused   int
	resumed  int
	err      error
	duringOK func() bool // checked when the pause is taken
	before   bool
}

func (f *fakePauser) PauseExportForRestore(ctx context.Context, reason string) (func(), error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.duringOK != nil {
		f.before = f.duringOK()
	}
	if f.err != nil {
		return nil, f.err
	}
	f.paused++
	return func() { f.mu.Lock(); f.resumed++; f.mu.Unlock() }, nil
}

func metadataManager(t *testing.T, data storage.Backend) *Manager {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "arc.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	mgr, err := NewManager(&ManagerConfig{DataStorage: data, BackupPath: t.TempDir(), SQLiteDBPath: dbPath, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	return mgr
}

// A restore that stages a catalog pauses the export before it writes
// anything and leaves it paused for the restart; one that stages none does
// not pause; a pause refused refuses the restore (#1813).
func TestRestore_PausesIcebergExportUntilTheRestart(t *testing.T) {
	ctx := context.Background()
	data, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	mgr := metadataManager(t, data)
	writeAll(t, data, map[string]string{"prod/sensors/2026/07/14/15/sensors_1.parquet": "PAR1"})
	res, err := mgr.CreateBackup(ctx, BackupOptions{IncludeMetadata: true})
	if err != nil {
		t.Fatal(err)
	}
	id := res.Manifest.BackupID
	if err := data.Delete(ctx, "prod/sensors/2026/07/14/15/sensors_1.parquet"); err != nil {
		t.Fatal(err)
	}

	p := &fakePauser{duringOK: func() bool {
		ok, _ := data.Exists(ctx, "prod/sensors/2026/07/14/15/sensors_1.parquet")
		return !ok
	}}
	mgr.SetIcebergExportPauser(p)
	if _, err := mgr.RestoreBackup(ctx, RestoreOptions{BackupID: id, RestoreData: true, RestoreMetadata: true}); err != nil {
		t.Fatal(err)
	}
	if p.paused != 1 || p.resumed != 0 || !p.before {
		t.Fatalf("paused %d resumed %d before-writes %v; want paused once before any write, not resumed", p.paused, p.resumed, p.before)
	}
	if got := mgr.GetProgress(); got == nil || got.IcebergExportPause != "paused_until_restart" {
		t.Fatalf("progress = %+v", got)
	}

	// No catalog staged: no pause.
	p2 := &fakePauser{}
	mgr.SetIcebergExportPauser(p2)
	if _, err := mgr.RestoreBackup(ctx, RestoreOptions{BackupID: id, RestoreData: true}); err != nil {
		t.Fatal(err)
	}
	if p2.paused != 0 {
		t.Fatal("a data-only restore paused the export")
	}

	// Refused pause: refused restore, nothing written.
	if err := data.Delete(ctx, "prod/sensors/2026/07/14/15/sensors_1.parquet"); err != nil {
		t.Fatal(err)
	}
	p3 := &fakePauser{err: errors.New("an export was still running")}
	mgr.SetIcebergExportPauser(p3)
	_, err = mgr.RestoreBackup(ctx, RestoreOptions{BackupID: id, RestoreData: true, RestoreMetadata: true})
	if err == nil || !strings.Contains(err.Error(), "restore refused") {
		t.Fatalf("restore with a refused pause = %v, want refused", err)
	}
	if ok, _ := data.Exists(ctx, "prod/sensors/2026/07/14/15/sensors_1.parquet"); ok {
		t.Fatal("a refused restore wrote data")
	}
}

// writeFails fails every data write.
type writeFails struct{ *storage.LocalBackend }

func (w *writeFails) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	return errors.New("injected write failure")
}

// A restore that fails before it stages the catalog lifts the pause.
func TestRestore_ResumesIcebergExportWhenNothingIsStaged(t *testing.T) {
	ctx := context.Background()
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	mgr := metadataManager(t, local)
	writeAll(t, local, map[string]string{"prod/sensors/2026/07/14/15/sensors_1.parquet": "PAR1"})
	res, err := mgr.CreateBackup(ctx, BackupOptions{IncludeMetadata: true})
	if err != nil {
		t.Fatal(err)
	}
	mgr.dataStorage = &writeFails{LocalBackend: local}
	p := &fakePauser{}
	mgr.SetIcebergExportPauser(p)
	if _, err := mgr.RestoreBackup(ctx, RestoreOptions{BackupID: res.Manifest.BackupID, RestoreData: true, RestoreMetadata: true}); err == nil {
		t.Fatal("restore succeeded with every data write failing")
	}
	if p.paused != 1 || p.resumed != 1 {
		t.Fatalf("paused %d resumed %d; want the pause lifted when no catalog was staged", p.paused, p.resumed)
	}
	if got := mgr.GetProgress(); got == nil || got.IcebergExportPause != "resumed" {
		t.Fatalf("progress = %+v", got)
	}
}
