package tiering

import (
	"context"
	"errors"
	"testing"
	"time"
)

type failingDeleteBackend struct {
	*mockBackend
	path string
}

func (b *failingDeleteBackend) Delete(ctx context.Context, path string) error {
	if path == b.path {
		return errors.New("object lock prevents deletion")
	}
	return b.mockBackend.Delete(ctx, path)
}

func TestPrepareDatabaseDeleteRemovesClusterManifestBeforeHotStorage(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	hotPath := "db1/cpu/hot.parquet"
	peerPath := "db1/cpu/peer-only.parquet"
	mustWrite(t, hot, hotPath)
	fake := &fakeManifest{
		hot:     hot,
		entries: map[string]int64{peerPath: 9, hotPath: 7},
	}
	m.manifest = fake

	if err := m.PrepareDatabaseDelete(ctx, "db1", []string{hotPath, "db1/cpu/unregistered.parquet"}); err != nil {
		t.Fatalf("PrepareDatabaseDelete: %v", err)
	}
	if len(fake.calls) != 1 {
		t.Fatalf("manifest calls = %d, want 1", len(fake.calls))
	}
	call := fake.calls[0]
	if call.reason != manifestReasonDatabaseDelete {
		t.Fatalf("manifest reason = %q", call.reason)
	}
	if want := []string{hotPath, peerPath, "db1/cpu/unregistered.parquet"}; !equalPathSlices(call.paths, want) {
		t.Fatalf("manifest paths = %v, want sorted union %v", call.paths, want)
	}
	if !call.hotPresent[hotPath] {
		t.Fatal("manifest was updated after the local hot object had already been removed")
	}
	if len(fake.entries) != 0 {
		t.Fatalf("manifest still contains database entries: %v", fake.entries)
	}
}

func TestCleanupDatabaseDeleteRemovesTierRowsAndColdObjects(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	hotPath := "db1/cpu/hot.parquet"
	coldPath := "db1/cpu/cold.parquet"
	orphanColdPath := "db1/cpu/untracked.parquet"
	mustWrite(t, hot, hotPath)
	mustWrite(t, cold, coldPath)
	mustWrite(t, cold, orphanColdPath)
	recordHotRow(t, m, hotPath, manifestPartition)
	coldRow := *coldRowFor(coldPath)
	coldRow.Tier = TierCold
	if _, err := m.metadata.RecordColdFile(ctx, &coldRow, time.Now()); err != nil {
		t.Fatal(err)
	}

	removed, errs := m.CleanupDatabaseDelete(ctx, "db1", []string{hotPath}, nil)
	if len(errs) != 0 {
		t.Fatalf("cleanup errors: %v", errs)
	}
	if removed != 2 {
		t.Fatalf("removed cold objects = %d, want 2", removed)
	}
	for _, path := range []string{hotPath, coldPath} {
		if row, err := m.metadata.GetFile(ctx, path); err != nil || row != nil {
			t.Fatalf("tier row %q remains: row=%+v err=%v", path, row, err)
		}
	}
	for _, path := range []string{coldPath, orphanColdPath} {
		if exists, err := cold.Exists(ctx, path); err != nil || exists {
			t.Fatalf("cold object %q remains: exists=%v err=%v", path, exists, err)
		}
	}
}

func TestCleanupDatabaseDeleteRetainsColdRowWhenObjectDeleteFails(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	path := "db1/cpu/locked.parquet"
	mustWrite(t, cold, path)
	row := *coldRowFor(path)
	row.Tier = TierCold
	if _, err := m.metadata.RecordColdFile(ctx, &row, time.Now()); err != nil {
		t.Fatal(err)
	}
	m.coldBackend = &failingDeleteBackend{mockBackend: cold, path: path}

	removed, errs := m.CleanupDatabaseDelete(ctx, "db1", nil, nil)
	if removed != 0 || len(errs) == 0 {
		t.Fatalf("cleanup = (%d, %v), want no removal and a reported error", removed, errs)
	}
	if row, err := m.metadata.GetFile(ctx, path); err != nil || row == nil || row.Tier != TierCold {
		t.Fatalf("cold tier row after failed delete = (%+v, %v), want retained cold row", row, err)
	}
	if exists, err := cold.Exists(ctx, path); err != nil || !exists {
		t.Fatalf("locked object after failed delete: exists=%v err=%v, want present", exists, err)
	}
}

func TestBeginDatabaseDeleteSerializesWithTierCycles(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	release, err := m.BeginDatabaseDelete()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.ScanTiers(context.Background()); !errors.Is(err, ErrScanRunning) {
		t.Fatalf("ScanTiers error = %v, want %v", err, ErrScanRunning)
	}
	release()
	release, err = m.BeginDatabaseDelete()
	if err != nil {
		t.Fatalf("BeginDatabaseDelete after release: %v", err)
	}
	release()
}

func equalPathSlices(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
