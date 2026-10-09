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

// #1179's headline case, end to end: the cold object is there, the hot copy is
// NOT, and the row still says hot. That is what a restored or lost arc.db looks
// like, and it is the case the two tests above do not reach, because both keep
// the hot copy in place.
//
// It also pins the ORDERING the fix depends on, which is the part that is easy
// to break later. scanTiers reads the cold listing before the hot walk, and the
// hot walk retires rows whose file has left hot storage (retireVanishedHotRows).
// Run the other way round, the row would be retired as vanished and the cold
// object would end up with no row at all — which is strictly worse than the bug
// being fixed, because the query layer omits the cold glob for a measurement no
// row claims as cold, so the data would read as absent rather than mis-tiered.
func TestScanTiers_StandaloneRebuildsAColdRowWhoseHotCopyIsGone(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	partition := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	// Cold holds the object; hot never gets it. The row is the stale one a
	// metadata restore leaves behind.
	mustWrite(t, cold, gateDailyA)
	recordHotRow(t, m, gateDailyA, partition)

	result, err := m.ScanTiers(ctx)
	if err != nil {
		t.Fatalf("ScanTiers: %v", err)
	}
	if result.ColdSynced != 1 {
		t.Fatalf("ColdSynced = %d, want 1: the cold listing is what rebuilds this row", result.ColdSynced)
	}
	if result.ColdSyncFailed {
		t.Fatal("ColdSyncFailed set on a healthy cold backend")
	}

	// The row survived the hot walk's retire pass, and it says cold.
	meta := fileMeta(t, m, gateDailyA)
	if meta.Tier != TierCold {
		t.Fatalf("tier = %s, want cold", meta.Tier)
	}
	if result.HotRetired != 0 {
		t.Fatalf("HotRetired = %d, want 0: the row was already cold by the time the hot walk ran, so it is not a vanished hot file", result.HotRetired)
	}
}
