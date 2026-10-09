package tiering

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestScanTiers_StandaloneSyncsColdMetadata(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	partition := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	mustWrite(t, cold, gateDailyA)
	recordHotRow(t, m, gateDailyA, partition)

	result, err := m.ScanTiers(ctx)
	if err != nil {
		t.Fatalf("ScanTiers: %v", err)
	}
	if result.ColdSynced != 1 || result.ColdSyncFailed {
		t.Fatalf("ScanTiers = %+v, want one cold row synced without failure", result)
	}
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierCold {
		t.Fatalf("tier = %s, want cold after the standalone scan found the cold object", got)
	}
}

func TestRunCycle_StandaloneSkipsMigrationWhenColdSyncFails(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	coldWithFailedListing := &coldLister{
		mockBackend: cold,
		listErr:     errors.New("cold tier unavailable"),
	}
	m.coldBackend = coldWithFailedListing

	ctx := context.Background()
	mustWrite(t, hot, gateDailyA)
	recordHotRow(t, m, gateDailyA, time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC))

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle: %v", err)
	}
	if got := coldWithFailedListing.lists.Load(); got != 1 {
		t.Fatalf("cold listings = %d, want one failed listing", got)
	}
	if got := coldWithFailedListing.writes.Load(); got != 0 {
		t.Fatalf("cold writes = %d, want no migration after a failed cold listing", got)
	}
	if exists, _ := hot.Exists(ctx, gateDailyA); !exists {
		t.Fatal("hot object was removed after the cold metadata listing failed")
	}
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierHot {
		t.Fatalf("tier = %s, want hot after the failed cold listing", got)
	}
}
