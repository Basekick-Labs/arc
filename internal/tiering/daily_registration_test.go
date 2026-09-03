package tiering

// Regression tests for #683: daily-compacted files live at DAY level
// ({db}/{measurement}/{Y}/{M}/{D}/{name}_daily.parquet) — the only files the
// migrator moves to cold — but the scanner rejected any path without an hour
// segment, so scan-registered deployments never produced migration candidates.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// ListObjects makes mockBackend a storage.ObjectLister so
// ScanAndRegisterFiles can walk it.
func (m *mockBackend) ListObjects(_ context.Context, prefix string) ([]storage.ObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []storage.ObjectInfo{}
	for path, data := range m.files {
		if strings.HasPrefix(path, prefix) {
			out = append(out, storage.ObjectInfo{Path: path, Size: int64(len(data)), LastModified: time.Now().UTC()})
		}
	}
	return out, nil
}

func TestParseFilePath_DayAndHourLevels(t *testing.T) {
	m := &Manager{}

	day, err := m.parseFilePath("db1/cpu/2024/03/15/cpu_20240315_daily.parquet")
	if err != nil {
		t.Fatalf("day-level path rejected: %v", err)
	}
	if want := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC); !day.PartitionTime.Equal(want) {
		t.Fatalf("day-level partition time = %v, want %v", day.PartitionTime, want)
	}

	hour, err := m.parseFilePath("db1/cpu/2024/03/15/14/cpu_x.parquet")
	if err != nil {
		t.Fatalf("hour-level path rejected: %v", err)
	}
	if want := time.Date(2024, 3, 15, 14, 0, 0, 0, time.UTC); !hour.PartitionTime.Equal(want) {
		t.Fatalf("hour-level partition time = %v, want %v", hour.PartitionTime, want)
	}

	for _, bad := range []string{
		"db1/cpu/2024/03/file.parquet",       // 5 parts: too short
		"db1/cpu/2024/03/xx/f_daily.parquet", // non-numeric day
		"db1/cpu/2024/03/15/xx/f.parquet",    // non-numeric hour at hour level
	} {
		if _, err := m.parseFilePath(bad); err == nil {
			t.Fatalf("path %q unexpectedly accepted", bad)
		}
	}
}

func TestScanRegistersAndMigratesDailyFiles(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()

	dailyPath := "db1/cpu/2024/03/15/cpu_20240315_daily.parquet"
	rawPath := "db1/cpu/2024/03/15/14/cpu_20240315_140000_1.parquet"
	if err := hot.Write(ctx, dailyPath, []byte("daily")); err != nil {
		t.Fatal(err)
	}
	if err := hot.Write(ctx, rawPath, []byte("raw")); err != nil {
		t.Fatal(err)
	}

	res, err := m.ScanAndRegisterFiles(ctx)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if res.Errors != 0 || res.FilesRegistered != 2 {
		t.Fatalf("scan result = %+v, want 2 registered, 0 errors (the #683 failure was a path-too-short error here)", res)
	}

	candidates, err := m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Path != dailyPath {
		t.Fatalf("candidates = %+v, want exactly the day-level daily file", candidates)
	}

	migrated, errs := m.migrator.MigrateBatch(ctx, candidates)
	if migrated != 1 || errs != 0 {
		t.Fatalf("MigrateBatch = (%d migrated, %d errors), want (1, 0)", migrated, errs)
	}
	if ok, _ := cold.Exists(ctx, dailyPath); !ok {
		t.Fatal("daily file missing from cold backend after migration")
	}
	if ok, _ := hot.Exists(ctx, dailyPath); ok {
		t.Fatal("daily file still present on hot backend after migration")
	}
	meta, err := m.metadata.GetFile(ctx, dailyPath)
	if err != nil || meta.Tier != TierCold {
		t.Fatalf("metadata after migration = (%+v, %v), want tier cold", meta, err)
	}
}

// A hot file whose row already says cold is the orphan ReconcileOrphanedFiles
// deletes; the scan must not re-register it as hot, which would hide it from
// reconciliation and re-upload it every cycle.
func TestScanDoesNotDowngradeColdRows(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()

	dailyPath := "db1/cpu/2024/03/15/cpu_20240315_daily.parquet"
	if err := hot.Write(ctx, dailyPath, []byte("daily")); err != nil {
		t.Fatal(err)
	}
	if err := cold.Write(ctx, dailyPath, []byte("daily")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ScanAndRegisterFiles(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.metadata.UpdateTier(ctx, dailyPath, TierCold); err != nil {
		t.Fatal(err)
	}

	res, err := m.ScanAndRegisterFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.FilesSkipped != 1 {
		t.Fatalf("rescan FilesSkipped = %d, want 1 (cold row must not be re-registered)", res.FilesSkipped)
	}
	meta, err := m.metadata.GetFile(ctx, dailyPath)
	if err != nil || meta.Tier != TierCold {
		t.Fatalf("row after rescan = (%+v, %v), want tier still cold", meta, err)
	}
}

// Current compaction output names embed timestamp, nanos, and batch index
// (job.go), so a regenerated daily file for a re-compacted day gets a NEW
// path and registers as a fresh hot row; only the legacy date-only name could
// ever collide with its migrated predecessor. Pin the modern shape.
func TestScanRegistersModernDailyFilename(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()

	modern := "db1/cpu/2024/03/15/cpu_20240315_231459_123456789_b1_daily.parquet"
	if err := hot.Write(ctx, modern, []byte("daily")); err != nil {
		t.Fatal(err)
	}
	res, err := m.ScanAndRegisterFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Errors != 0 || res.FilesRegistered != 1 {
		t.Fatalf("scan result = %+v, want the modern-named daily file registered", res)
	}
	candidates, err := m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if err != nil || len(candidates) != 1 || candidates[0].Path != modern {
		t.Fatalf("candidates = (%+v, %v), want the modern-named daily file", candidates, err)
	}
}
