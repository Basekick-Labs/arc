package backup

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// warehouseTree lays out a realistic exporter warehouse plus decoys that the
// scoped walk must ignore: a foreign namespace, a non-metadata directory, a
// dotfile inside metadata/, and a file at the namespace level.
func warehouseTree(t *testing.T, wh string) (want map[string]string, decoys []string) {
	t.Helper()
	want = map[string]string{
		"arc_prod.db/sensors/metadata/00000-uuid.metadata.json": "meta-0",
		"arc_prod.db/sensors/metadata/00001-uuid.metadata.json": "meta-1",
		"arc_prod.db/sensors/metadata/v1.metadata.json":         "v1",
		"arc_prod.db/sensors/metadata/abcd-m1.avro":             "manifest",
		"arc_prod.db/sensors/metadata/snap-123-abcd.avro":       "snapshot",
		"arc_prod.db/sensors/metadata/version-hint.text":        "2",
		"arc_other.db/cpu/metadata/00000-uuid.metadata.json":    "other-meta",
	}
	decoys = []string{
		"spark_ns.db/t/metadata/00000-x.metadata.json", // foreign namespace prefix
		"arc_prod.db/sensors/data/part-0.parquet",      // not a metadata dir
		"arc_prod.db/sensors/metadata/.DS_Store",       // dotfile
		"arc_prod.db/README.txt",                       // file at table level
		"backups/backup-1/metadata/arc.db",             // an earlier backup's SQLite snapshot
	}
	for rel, content := range want {
		p := filepath.Join(wh, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, rel := range decoys {
		p := filepath.Join(wh, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("decoy"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return want, decoys
}

func newWarehouseManager(t *testing.T, dataDir, backupDir, warehouse string) (*Manager, storage.Backend) {
	t.Helper()
	data, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := NewManager(&ManagerConfig{
		DataStorage:          data,
		BackupPath:           backupDir,
		IcebergWarehousePath: warehouse,
		Logger:               zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return mgr, data
}

// TestBackupRestore_IcebergWarehouseOutsideRoot (regression, #637): a warehouse
// outside the storage root is walked, stored under <id>/iceberg/, recorded in
// the manifest, and written back into the configured warehouse on restore —
// including when only the metadata flag is set. On main the backup has no
// iceberg/ objects and the manifest has no such field.
func TestBackupRestore_IcebergWarehouseOutsideRoot(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	backupDir := t.TempDir()
	wh := t.TempDir()
	want, decoys := warehouseTree(t, wh)
	mgr, data := newWarehouseManager(t, dataDir, backupDir, wh)
	if err := data.Write(ctx, "prod/sensors/2026/07/14/15/sensors_1.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}

	res, err := mgr.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := res.Manifest.BackupID
	for rel := range want {
		if _, err := os.Stat(filepath.Join(backupDir, id, icebergBackupPrefix, filepath.FromSlash(rel))); err != nil {
			t.Errorf("warehouse file %q not in backup under iceberg/: %v", rel, err)
		}
	}
	for _, rel := range decoys {
		if _, err := os.Stat(filepath.Join(backupDir, id, icebergBackupPrefix, filepath.FromSlash(rel))); err == nil {
			t.Errorf("decoy %q was copied; the walk must be scoped to <prefix>_*.db/<table>/metadata/", rel)
		}
	}
	info := res.Manifest.IcebergWarehouse
	if info == nil || info.FileCount != int64(len(want)) || info.SkippedFiles != 0 {
		t.Fatalf("manifest.IcebergWarehouse = %+v, want %d files, 0 skipped", info, len(want))
	}
	if info.Path != resolveExistingPath(wh) {
		t.Errorf("manifest warehouse path = %q, want resolved %q", info.Path, resolveExistingPath(wh))
	}
	if abs, _ := filepath.Abs(wh); info.ConfiguredPath != filepath.Clean(abs) {
		t.Errorf("manifest configured_path = %q, want %q", info.ConfiguredPath, filepath.Clean(abs))
	}
	// The manifest round-trips through backup storage with the field intact.
	stored, err := mgr.GetBackup(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.IcebergWarehouse == nil || stored.IcebergWarehouse.FileCount != int64(len(want)) {
		t.Fatalf("stored manifest lost the iceberg_warehouse field: %+v", stored.IcebergWarehouse)
	}

	// DR: the warehouse is gone; metadata-only restore must bring it back.
	if err := os.RemoveAll(wh); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.RestoreBackup(ctx, RestoreOptions{BackupID: id, RestoreData: false, RestoreMetadata: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	for rel, content := range want {
		p := filepath.Join(wh, filepath.FromSlash(rel))
		got, err := os.ReadFile(p)
		if err != nil {
			t.Errorf("warehouse file %q not restored: %v", rel, err)
			continue
		}
		if string(got) != content {
			t.Errorf("restored %q content = %q, want %q", rel, got, content)
		}
		if runtime.GOOS != "windows" {
			st, _ := os.Stat(p)
			if st.Mode().Perm() != 0o600 {
				t.Errorf("restored %q mode = %o, want 0600", rel, st.Mode().Perm())
			}
			dst, _ := os.Stat(filepath.Dir(p))
			if dst.Mode().Perm() != 0o700 {
				t.Errorf("restored dir of %q mode = %o, want 0700", rel, dst.Mode().Perm())
			}
		}
	}
	// Restore never writes outside the exporter layout, and never a decoy.
	for _, rel := range decoys {
		if _, err := os.Stat(filepath.Join(wh, filepath.FromSlash(rel))); err == nil {
			t.Errorf("decoy %q reappeared after restore", rel)
		}
	}
	// Nothing landed inside the data store.
	if keys, _ := data.List(ctx, icebergBackupPrefix+"/"); len(keys) != 0 {
		t.Errorf("warehouse files were written into data storage: %v", keys)
	}
}

// TestBackup_IcebergWarehouseSymlinkedIsWalked (regression, #637): the walk uses
// the resolved directory. filepath.WalkDir does not follow a symlinked root, so
// walking the configured spelling would copy nothing and report success.
func TestBackup_IcebergWarehouseSymlinkedIsWalked(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks")
	}
	ctx := context.Background()
	real := t.TempDir()
	want, _ := warehouseTree(t, real)
	link := filepath.Join(t.TempDir(), "wh-link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	mgr, data := newWarehouseManager(t, t.TempDir(), t.TempDir(), link)
	if err := data.Write(ctx, "prod/sensors/2026/07/14/15/sensors_1.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}
	res, err := mgr.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Manifest.IcebergWarehouse; got == nil || got.FileCount != int64(len(want)) {
		t.Fatalf("symlinked warehouse backed up %+v, want %d files", got, len(want))
	}
}

// TestBackup_IcebergWarehouseUnderRootIsCoveredByListing (guard): a warehouse
// inside the storage root — the default, a subdirectory, or a symlinked spelling
// of one — is not walked twice; its metadata travels under data/ as before.
func TestBackup_IcebergWarehouseUnderRootIsCoveredByListing(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	spellings := map[string]string{
		"root":   dataDir,
		"subdir": filepath.Join(dataDir, "wh"),
	}
	if runtime.GOOS != "windows" {
		link := filepath.Join(t.TempDir(), "data-link")
		if err := os.Symlink(dataDir, link); err != nil {
			t.Fatal(err)
		}
		spellings["symlinked-subdir"] = filepath.Join(link, "wh")
	}
	for name, wh := range spellings {
		t.Run(name, func(t *testing.T) {
			mgr, data := newWarehouseManager(t, dataDir, t.TempDir(), wh)
			if mgr.icebergWarehouse != "" {
				t.Fatalf("warehouse %q classified outside the root %q", wh, dataDir)
			}
			if err := data.Write(ctx, "wh/arc_prod.db/sensors/metadata/v1.metadata.json", []byte("v1")); err != nil {
				t.Fatal(err)
			}
			res, err := mgr.CreateBackup(ctx, BackupOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if res.Manifest.IcebergWarehouse != nil {
				t.Errorf("manifest has an iceberg_warehouse group for an under-root warehouse: %+v", res.Manifest.IcebergWarehouse)
			}
			bk, _ := storage.NewLocalBackend(mgr.backupStorage.(*storage.LocalBackend).GetBasePath(), zerolog.Nop())
			if _, err := bk.Read(ctx, res.Manifest.BackupID+"/data/wh/arc_prod.db/sensors/metadata/v1.metadata.json"); err != nil {
				t.Errorf("under-root metadata missing from data/: %v", err)
			}
			if keys, _ := bk.List(ctx, res.Manifest.BackupID+"/"+icebergBackupPrefix+"/"); len(keys) != 0 {
				t.Errorf("under-root warehouse was also copied under iceberg/: %v", keys)
			}
		})
	}
}

// TestRestore_IcebergWarehouseSkippedWhenNotConfigured (guard): a node with
// Iceberg off restores the data, reports the skipped group and completes; a node
// running Iceberg with its warehouse under the root would stage an unloadable
// catalog, so with restore_metadata it fails instead (second half).
func TestRestore_IcebergWarehouseSkippedWhenNotConfigured(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	backupDir := t.TempDir()
	wh := t.TempDir()
	want, _ := warehouseTree(t, wh)
	src, data := newWarehouseManager(t, dataDir, backupDir, wh)
	if err := data.Write(ctx, "prod/sensors/2026/07/14/15/sensors_1.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}
	res, err := src.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(wh); err != nil {
		t.Fatal(err)
	}
	// Target node: same backup store, no warehouse configured.
	dst, _ := newWarehouseManager(t, t.TempDir(), backupDir, "")
	if _, err := dst.RestoreBackup(ctx, RestoreOptions{BackupID: res.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("restore must complete without a warehouse: %v", err)
	}
	p := dst.GetProgress()
	if p == nil || p.IcebergWarehouseFilesSkipped != int64(len(want)) {
		t.Fatalf("progress = %+v, want iceberg_warehouse_files_skipped=%d", p, len(want))
	}
	if _, err := os.Stat(wh); err == nil {
		t.Error("the removed warehouse was recreated on a node that has none configured")
	}

	// Iceberg on, warehouse under the root, catalog restored: must fail loudly.
	if err := os.MkdirAll(filepath.Join(backupDir, res.Manifest.BackupID, "metadata"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, res.Manifest.BackupID, "metadata", "arc.db"), []byte("sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	m2 := res.Manifest
	m2.HasMetadata = true
	data2, _ := MarshalManifest(m2)
	if err := os.WriteFile(filepath.Join(backupDir, res.Manifest.BackupID, "manifest.json"), data2, 0o600); err != nil {
		t.Fatal(err)
	}
	dataDir2 := t.TempDir()
	dst2, _ := newWarehouseManager(t, dataDir2, backupDir, filepath.Join(dataDir2, "wh"))
	dst2.sqliteDBPath = filepath.Join(t.TempDir(), "arc.db")
	_, err = dst2.RestoreBackup(ctx, RestoreOptions{BackupID: res.Manifest.BackupID, RestoreData: true, RestoreMetadata: true})
	if err == nil || !strings.Contains(err.Error(), "iceberg.warehouse") {
		t.Fatalf("restore with Iceberg on and no outside-root warehouse must fail naming iceberg.warehouse, got %v", err)
	}
}

// TestPathWithin pins the boundary rule (#534): a sibling whose name merely
// starts with the directory's name is outside it.
func TestPathWithin(t *testing.T) {
	sep := string(filepath.Separator)
	dir := filepath.Join(sep, "data", "wh")
	cases := []struct {
		p    string
		want bool
	}{
		{dir, true},
		{dir + sep, true},
		{filepath.Join(dir, "arc_db.db", "cpu", "metadata"), true},
		{filepath.Join(sep, "data", "wh-other"), false},
		{filepath.Join(sep, "data", "wharf", "x"), false},
		{filepath.Join(sep, "data"), false},
	}
	for _, c := range cases {
		if got := pathWithin(c.p, dir); got != c.want {
			t.Errorf("pathWithin(%q, %q) = %v, want %v", c.p, dir, got, c.want)
		}
	}
}

// TestWarehouseRestorePath: the one write outside a storage backend refuses
// every key that is not a plain relative path of clean segments.
func TestWarehouseRestorePath(t *testing.T) {
	root := t.TempDir()
	good := "arc_prod.db/sensors/metadata/v1.metadata.json"
	if got, err := warehouseRestorePath(root, good); err != nil || got != filepath.Join(root, filepath.FromSlash(good)) {
		t.Fatalf("good key: %q, %v", got, err)
	}
	for _, bad := range []string{"", "/etc/passwd", "../x", "a/../../x", "a//b", "./a", "a/./b"} {
		if _, err := warehouseRestorePath(root, bad); err == nil {
			t.Errorf("key %q accepted", bad)
		}
	}
}

// TestResolveExistingPath_MissingTail: a warehouse that does not exist yet
// resolves through its deepest existing (symlinked) ancestor.
func TestResolveExistingPath_MissingTail(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks")
	}
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	got := resolveExistingPath(filepath.Join(link, "not", "yet"))
	realResolved, _ := filepath.EvalSymlinks(real)
	if want := filepath.Join(realResolved, "not", "yet"); got != want {
		t.Fatalf("resolveExistingPath = %q, want %q", got, want)
	}
}
