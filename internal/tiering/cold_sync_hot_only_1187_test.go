package tiering

import (
	"context"
	"testing"
	"time"
)

func TestRunCycleDoesNotSyncColdMetadataForHotOnlyDatabase(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	path := gateDailyA
	lastModified := time.Now().UTC().Add(-time.Hour)
	m.coldBackend = &coldLister{mockBackend: cold, lastModified: lastModified}

	mustWrite(t, hot, path)
	mustWrite(t, cold, path)
	recordHotRow(t, m, path, lastModified)
	if err := m.policies.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
		t.Fatalf("set hot-only policy: %v", err)
	}

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle: %v", err)
	}

	if got := fileMeta(t, m, path).Tier; got != TierHot {
		t.Errorf("tier = %s, want hot for a hot-only database", got)
	}
	if exists, err := hot.Exists(ctx, path); err != nil {
		t.Fatalf("check hot object: %v", err)
	} else if !exists {
		t.Error("runCycle removed the hot copy of a hot-only database")
	}
	if result, _ := m.LastScan(); result == nil || result.ColdSynced != 0 {
		t.Errorf("cold sync result = %+v, want no cold rows synced", result)
	}
}
