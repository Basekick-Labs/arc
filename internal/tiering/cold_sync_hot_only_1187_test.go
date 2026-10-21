package tiering

import (
	"context"
	"errors"
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

func TestColdSyncRecordsPrePinColdObjectForHotOnlyDatabase(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	path := gateDailyA
	m.coldBackend = &coldLister{mockBackend: cold, lastModified: time.Now().UTC().Add(-time.Hour)}
	// This object migrated before the pin; this node has neither its hot
	// copy nor a metadata row, so cold sync must restore its query visibility.
	mustWrite(t, cold, path)
	if exists, err := hot.Exists(ctx, path); err != nil || exists {
		t.Fatalf("hot copy exists = %v, err = %v; want absent", exists, err)
	}
	if f, err := m.metadata.GetFile(ctx, path); err != nil || f != nil {
		t.Fatalf("initial row = %+v, err = %v; want no row", f, err)
	}
	if err := m.policies.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
		t.Fatalf("set hot-only policy: %v", err)
	}

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle: %v", err)
	}

	if f, err := m.metadata.GetFile(ctx, path); err != nil || f == nil || f.Tier != TierCold {
		t.Fatalf("row = %+v, err = %v; want a cold row for the pre-pin cold object", f, err)
	}
	if result, _ := m.LastScan(); result == nil || result.ColdSynced != 1 {
		t.Errorf("cold sync result = %+v, want one cold row synced", result)
	}
}

type hotOnlyExistsErrorBackend struct {
	*mockBackend
	existsCalls int
}

func (b *hotOnlyExistsErrorBackend) Exists(context.Context, string) (bool, error) {
	b.existsCalls++
	return false, errors.New("hot storage existence check failed")
}

func TestColdSyncPreservesHotOnlyFileWhenHotExistenceCheckFails(t *testing.T) {
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
	faultyHot := &hotOnlyExistsErrorBackend{mockBackend: hot}
	m.hotBackend = faultyHot

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle: %v", err)
	}

	if faultyHot.existsCalls == 0 {
		t.Error("hot storage existence check was not attempted")
	}
	if got := fileMeta(t, m, path).Tier; got != TierHot {
		t.Errorf("tier = %s, want hot when hot storage existence is unknown", got)
	}
	if exists, err := hot.Exists(ctx, path); err != nil || !exists {
		t.Errorf("hot copy exists = %v, err = %v; want preserved", exists, err)
	}
	if result, _ := m.LastScan(); result == nil || result.ColdSynced != 0 {
		t.Errorf("cold sync result = %+v, want no cold rows synced", result)
	}
}
