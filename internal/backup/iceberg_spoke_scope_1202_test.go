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

// The three directory spellings the exporter writes for edge-sync spoke
// databases, plus one plain database and the decoys the ownership rule must
// not claim (#1202).
const (
	spokePlainNS   = "arc_rocket01.telemetry.db"                                // dot-free spoke
	spokeLegacyNS  = "arc_rocket.01.telemetry.db"                               // dotted spoke, rekeyed by #1129, directory not moved
	spokeEncodedNS = `__iceberg_namespace_v1__:["arc_rocket.01","engine"].db`   // dotted spoke, written after #1129
	siblingNS      = "arc_rocket010.telemetry.db"                               // a different spoke sharing a prefix
	plainNS        = "arc_prod.db"                                              // a plain database
	foreignEncNS   = `__iceberg_namespace_v1__:["spark_rocket.01","engine"].db` // another prefix's encoded namespace
)

func writeSpokeWarehouse(t *testing.T, write func(rel string)) {
	t.Helper()
	for _, ns := range []string{spokePlainNS, spokeLegacyNS, spokeEncodedNS, siblingNS, plainNS, foreignEncNS} {
		write(ns + "/m/metadata/v1.metadata.json")
	}
	write(spokePlainNS + "/m2/metadata/snap-1.avro") // a second table in one namespace
}

// Scoped to a spoke, the backup reports every namespace directory of that
// spoke, in every spelling, and none of its neighbours'. On main it reported
// zero for all of them: the computed "arc_<spoke>.db" never exists.
func TestBackup_ScopedSpokeIcebergNamespacesAreCounted(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		scope string
		want  []string
		files int64
	}{
		{"dot-free spoke", "rocket01", []string{spokePlainNS}, 2},
		{"dotted spoke", "rocket.01", []string{spokeEncodedNS, spokeLegacyNS}, 2}, // sorted: "_" < "a"
		{"plain database", "prod", []string{plainNS}, 1},
	} {
		t.Run("in-root/"+tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			data := mustLocalBackend(t, dataDir, zerolog.Nop())
			mustWrite(t, data, tc.scope+"/m/2026/07/14/15/f.parquet", []byte("PAR1"))
			writeSpokeWarehouse(t, func(rel string) { mustWrite(t, data, "wh/"+rel, []byte("ICE")) })
			m, err := NewManager(&ManagerConfig{
				DataStorage: data, BackupPath: t.TempDir(), Logger: zerolog.Nop(),
				IcebergWarehousePath: filepath.Join(dataDir, "wh"),
			})
			if err != nil {
				t.Fatal(err)
			}
			if m.icebergWarehouse != "" || m.icebergWarehouseKeyPrefix != "wh/" {
				t.Fatalf("premise: want an in-root warehouse at wh/, got warehouse=%q prefix=%q", m.icebergWarehouse, m.icebergWarehouseKeyPrefix)
			}
			res, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{tc.scope}})
			if err != nil {
				t.Fatalf("CreateBackup: %v", err)
			}
			want := make([]string, len(tc.want))
			for i, ns := range tc.want {
				want[i] = "wh/" + ns
			}
			assertExcluded(t, res.Manifest, want, tc.files)
		})
		t.Run("outside-root/"+tc.name, func(t *testing.T) {
			wh := t.TempDir()
			writeSpokeWarehouse(t, func(rel string) {
				p := filepath.Join(wh, filepath.FromSlash(rel))
				if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte("ICE"), 0o600); err != nil {
					t.Fatal(err)
				}
			})
			m, data := newWarehouseManager(t, t.TempDir(), t.TempDir(), wh)
			mustWrite(t, data, tc.scope+"/m/2026/07/14/15/f.parquet", []byte("PAR1"))
			if m.icebergWarehouse == "" {
				t.Fatal("premise: warehouse must be outside the root")
			}
			res, err := m.CreateBackup(ctx, BackupOptions{Databases: []string{tc.scope}})
			if err != nil {
				t.Fatalf("CreateBackup: %v", err)
			}
			assertExcluded(t, res.Manifest, tc.want, tc.files)
		})
	}
}

func assertExcluded(t *testing.T, mf *Manifest, want []string, files int64) {
	t.Helper()
	got := append([]string(nil), mf.IcebergNamespacesExcluded...)
	if strings.Join(got, "|") != strings.Join(want, "|") || mf.IcebergNamespaceFilesExcluded != files {
		t.Errorf("excluded = %d %q, want %d %q", mf.IcebergNamespaceFilesExcluded, got, files, want)
	}
}

// An unscoped backup of an outside-root warehouse copies the JSON-encoded
// namespace directory iceberg-go writes for a dotted spoke. On main the
// depth-1 filter required "<prefix>_", so the backup carried the catalog rows
// for those tables and none of their metadata. A foreign prefix's encoded
// directory stays out.
func TestBackup_UnscopedWarehouseWalkIncludesEncodedNamespace(t *testing.T) {
	ctx := context.Background()
	wh := t.TempDir()
	writeSpokeWarehouse(t, func(rel string) {
		p := filepath.Join(wh, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("ICE:"+rel), 0o600); err != nil {
			t.Fatal(err)
		}
	})
	backupDir := t.TempDir()
	m, data := newWarehouseManager(t, t.TempDir(), backupDir, wh)
	mustWrite(t, data, "prod/m/2026/07/14/15/f.parquet", []byte("PAR1"))
	res, err := m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	id := res.Manifest.BackupID
	encoded := spokeEncodedNS + "/m/metadata/v1.metadata.json"
	if _, err := os.Stat(filepath.Join(backupDir, id, icebergBackupPrefix, filepath.FromSlash(encoded))); err != nil {
		t.Errorf("encoded namespace metadata not in the backup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(backupDir, id, icebergBackupPrefix, filepath.FromSlash(foreignEncNS+"/m/metadata/v1.metadata.json"))); err == nil {
		t.Error("a foreign prefix's encoded namespace was copied")
	}
	if info := res.Manifest.IcebergWarehouse; info == nil || info.FileCount != 6 {
		t.Fatalf("manifest.IcebergWarehouse = %+v, want 6 files (every arc namespace, not spark's)", info)
	}

	// And it restores to the same spelling.
	if err := os.RemoveAll(wh); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: id, RestoreMetadata: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(wh, filepath.FromSlash(encoded)))
	if err != nil || string(got) != "ICE:"+encoded {
		t.Errorf("restored encoded metadata = %q, %v", got, err)
	}
}

// A namespace directory whose name cannot be stored as a backup key fails the
// backup rather than leaving its tables out silently: the #1100 rule for any
// warehouse file, which now reaches encoded namespace directories too. The
// encoding lengthens a spoke's name, so this is where a long one surfaces.
func TestBackup_OverlongEncodedNamespaceFailsLoudly(t *testing.T) {
	ctx := context.Background()
	wh := t.TempDir()
	long := strings.Repeat("s", 203) + ".x"
	name := `__iceberg_namespace_v1__:["arc_` + long + `","telemetry"].db`
	if len(name) <= storage.MaxUsableKeySegmentLen || len(name) > 255 {
		t.Fatalf("premise: name length %d must exceed %d and fit NAME_MAX", len(name), storage.MaxUsableKeySegmentLen)
	}
	p := filepath.Join(wh, name, "m", "metadata", "v1.metadata.json")
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Skipf("filesystem refuses a %d-byte name: %v", len(name), err)
	}
	if err := os.WriteFile(p, []byte("ICE"), 0o600); err != nil {
		t.Fatal(err)
	}
	m, _ := newWarehouseManager(t, t.TempDir(), t.TempDir(), wh)
	if _, err := m.CreateBackup(ctx, BackupOptions{}); err == nil || !strings.Contains(err.Error(), "cannot be stored under a valid backup key") {
		t.Fatalf("CreateBackup: err = %v, want the key-validity failure", err)
	}
}
