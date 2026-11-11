package backup

import (
	"context"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

func TestBackupHintBody(t *testing.T) {
	for _, tc := range []struct {
		name          string
		live          string
		listed        []int
		want          string
		rewritten, ok bool
	}{
		{"hint names a listed version", "3", []int{1, 2, 3}, "3", false, true},
		{"hint ahead of the listing", "5", []int{2, 3}, "3", true, true},
		{"hint behind the listing is kept", "2", []int{2, 3}, "2", false, true},
		{"nothing listed", "4", nil, "", false, false},
		{"not a number is copied as it is", "garbage", nil, "garbage", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listed := map[int]bool{}
			for _, v := range tc.listed {
				listed[v] = true
			}
			body, rewritten, ok := backupHintBody([]byte(tc.live), listed)
			if string(body) != tc.want || rewritten != tc.rewritten || ok != tc.ok {
				t.Fatalf("got %q rewritten=%v ok=%v, want %q rewritten=%v ok=%v", body, rewritten, ok, tc.want, tc.rewritten, tc.ok)
			}
		})
	}
}

// commitDuringBackup is the data store with an Iceberg export committing
// right after the backup's first whole-root listing: v2 and its metadata
// file land, then the hint moves on to a v3 whose files never do (a commit
// in flight when the hint is read).
type commitDuringBackup struct {
	*storage.LocalBackend
	once sync.Once
	t    *testing.T
}

func (c *commitDuringBackup) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	objs, err := c.LocalBackend.ListObjects(ctx, prefix)
	if prefix == "" {
		c.once.Do(func() {
			for k, v := range map[string]string{
				"arc_prod.db/sensors/metadata/00002-uuid.metadata.json": "m2",
				"arc_prod.db/sensors/metadata/v2.metadata.json":         "m2",
				"arc_prod.db/sensors/metadata/version-hint.text":        "3",
			} {
				if err := c.LocalBackend.Write(ctx, k, []byte(v)); err != nil {
					c.t.Error(err)
				}
			}
		})
	}
	return objs, err
}

// #1613: step 3b lists the in-root metadata after the SQLite snapshot, not
// from the step-1 listing, so a commit made during the data copy is in the
// backup; and the hint stored names a v<N> the backup holds.
func TestBackup_InRootIcebergMetadataListedAfterTheSnapshot(t *testing.T) {
	ctx := context.Background()
	dataDir, backupDir := t.TempDir(), t.TempDir()
	local, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{
		"prod/sensors/2026/07/14/15/sensors_1.parquet":          "PAR1-fake",
		"arc_prod.db/sensors/metadata/00001-uuid.metadata.json": "m1",
		"arc_prod.db/sensors/metadata/v1.metadata.json":         "m1",
		"arc_prod.db/sensors/metadata/version-hint.text":        "1",
	} {
		if err := local.Write(ctx, k, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	mgr, err := NewManager(&ManagerConfig{
		DataStorage: &commitDuringBackup{LocalBackend: local, t: t},
		BackupPath:  backupDir,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := mgr.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	backup, _ := storage.NewLocalBackend(backupDir, zerolog.Nop())
	read := func(k string) string {
		t.Helper()
		b, err := backup.Read(ctx, res.Manifest.BackupID+"/data/"+k)
		if err != nil {
			t.Fatalf("%s not in the backup: %v", k, err)
		}
		return string(b)
	}
	read("arc_prod.db/sensors/metadata/00002-uuid.metadata.json")
	read("arc_prod.db/sensors/metadata/v2.metadata.json")
	if got := read("arc_prod.db/sensors/metadata/version-hint.text"); got != "2" {
		t.Fatalf("stored version hint %q, want 2: the newest version the backup holds, not the live 3", got)
	}
}
