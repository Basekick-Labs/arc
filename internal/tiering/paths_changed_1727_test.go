package tiering

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// A migration batch reports the hot paths it moved, after their hot copies
// are released, so the Iceberg export can name the cold ones (#1727). A batch
// that moved nothing reports nothing.
func TestMigrateBatchReportsMovedPaths(t *testing.T) {
	m, hotBackend, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	var reported []string
	hotAtReport := map[string]bool{}
	m.SetOnPathsChanged(func(keys []string) {
		reported = append(reported, keys...)
		for _, k := range keys {
			exists, _ := hotBackend.Exists(ctx, k)
			hotAtReport[k] = exists
		}
	})
	old := time.Now().UTC().AddDate(0, 0, -30)
	var files []MigrationCandidate
	for _, n := range []string{"a", "b"} {
		path := "testdb/cpu/2025/01/01/data_" + n + ".parquet"
		data := []byte("data " + n)
		if err := hotBackend.Write(ctx, path, data); err != nil {
			t.Fatal(err)
		}
		if err := m.RecordNewFile(ctx, &FileMetadata{Path: path, Database: "testdb", Measurement: "cpu", PartitionTime: old, SizeBytes: int64(len(data))}); err != nil {
			t.Fatal(err)
		}
		files = append(files, MigrationCandidate{Path: path, Database: "testdb", Measurement: "cpu", PartitionTime: old,
			SizeBytes: int64(len(data)), CurrentTier: TierHot, TargetTier: TierCold})
	}
	if migrated, _ := m.migrator.MigrateBatch(ctx, files); migrated != 2 {
		t.Fatalf("migrated %d, want 2", migrated)
	}
	sort.Strings(reported)
	if strings.Join(reported, ",") != files[0].Path+","+files[1].Path {
		t.Fatalf("reported %v, want both moved paths", reported)
	}
	for k, hot := range hotAtReport {
		if hot {
			t.Fatalf("%s reported while its hot copy was still there", k)
		}
	}
	reported = nil
	if _, _ = m.migrator.MigrateBatch(ctx, nil); len(reported) != 0 {
		t.Fatalf("empty batch reported %v", reported)
	}
}
