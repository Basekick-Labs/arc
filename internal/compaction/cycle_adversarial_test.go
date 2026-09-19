package compaction

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func seedRecoveryScope(t *testing.T, mm *ManifestManager, b storage.Backend, db, measurement, tier, id string) (string, string) {
	t.Helper()
	ctx := context.Background()
	partition := db + "/" + measurement + "/2026/01/01/00"
	input, output := partition+"/"+id+".parquet", partition+"/"+id+"_compacted.parquet"
	for _, p := range []string{input, output} {
		if err := b.Write(ctx, p, []byte("row")); err != nil {
			t.Fatal(err)
		}
	}
	path, err := mm.WriteManifest(ctx, &Manifest{Database: db, Measurement: measurement, Tier: tier, JobID: id, PartitionPath: partition, InputFiles: []string{input}, OutputPath: output, OutputSize: 3, Status: ManifestStatusPending, CreatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	return path, input
}

func TestRecoveryScopeBoundaries(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	type fixture struct {
		db, measurement, tier, id string
		selected                  bool
	}
	fixtures := []fixture{{"db", "cpu", "hourly", "target", true}, {"db", "cpu_extra", "hourly", "othermeasurement", false}, {"db_extra", "cpu", "hourly", "otherdatabase", false}, {"db", "cpu", "daily", "othertier", false}, {"db/child", "cpu", "hourly", "namespacechild", false}}
	paths := make([][2]string, len(fixtures))
	for i, f := range fixtures {
		paths[i][0], paths[i][1] = seedRecoveryScope(t, m.ManifestManager, b, f.db, f.measurement, f.tier, f.id)
	}
	count, err := m.ManifestManager.recoverOrphanedManifests(context.Background(), recoveryScope{Databases: []string{"db"}, Measurement: "cpu", Tiers: []string{"hourly", "hourly"}}, nil, nil)
	if err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	for i, f := range fixtures {
		for _, p := range paths[i] {
			exists, err := b.Exists(context.Background(), p)
			if err != nil || exists == f.selected {
				t.Errorf("%s exists=%v selected=%v err=%v", p, exists, f.selected, err)
			}
		}
	}
}

func TestRecoveryUnreadableManifestRemainsProtected(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	path := ManifestBasePath + "/hourly/db/corrupt.json"
	if err := b.Write(ctx, path, []byte("{corrupt")); err != nil {
		t.Fatal(err)
	}
	count, err := m.ManifestManager.recoverOrphanedManifests(ctx, recoveryScope{Databases: []string{"db"}, Measurement: "cpu", Tiers: []string{"hourly"}}, nil, nil)
	if err == nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if exists, _ := b.Exists(ctx, path); !exists {
		t.Fatal("unreadable recovery state deleted")
	}
	if files, err := m.ManifestManager.GetFilesInManifests(ctx); err == nil {
		t.Fatalf("incomplete manifest cache accepted: %v", files)
	}
}

func TestManifestCacheProtectsOutputOnHit(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	path, _ := seedRecoveryScope(t, m.ManifestManager, b, "db", "cpu", "hourly", "cached")
	manifest, err := m.ManifestManager.ReadManifest(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		files, err := m.ManifestManager.GetFilesInManifests(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := files[manifest.OutputPath]; !ok {
			t.Fatalf("output lost on cache access %d", i)
		}
	}
}

func TestCycleHealthyEligibilityDeferral(t *testing.T) {
	m, _, cleanup := setupTestManager(t)
	defer cleanup()
	m.ManifestManager = nil
	m.SetSyncEligibility(func(context.Context, []string) (map[string]bool, error) { return map[string]bool{}, nil })
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) {
		return []Candidate{candidateIssue915("partition")}, nil
	}}}
	m.compactBatchForTest = func(context.Context, Candidate) error { t.Error("deferred work started"); return nil }
	if _, err := m.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"}); err != nil {
		t.Fatal(err)
	}
	if got := cycleOutcomeIssue915(t, m); got["status"] != "completed" || got["discovery_errors"] != int64(0) {
		t.Fatal(got)
	}
}

type recoveryReadFailure struct {
	storage.Backend
	path string
	fail error
}

func (b recoveryReadFailure) Read(ctx context.Context, path string) ([]byte, error) {
	if path == b.path {
		return nil, b.fail
	}
	return b.Backend.Read(ctx, path)
}

func TestRecoveryAggregatesErrorsAndKeepsProgress(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	bad, _ := seedRecoveryScope(t, m.ManifestManager, b, "db", "cpu", "hourly", "a_bad")
	good, _ := seedRecoveryScope(t, m.ManifestManager, b, "db", "cpu", "hourly", "b_good")
	failure := errors.New("transient storage read failure")
	mm := NewManifestManager(recoveryReadFailure{b, bad, failure}, zerolog.Nop())
	recovered, err := mm.RecoverOrphanedManifests(context.Background(), nil, nil)
	if recovered != 1 || !errors.Is(err, failure) {
		t.Fatalf("recovered=%d err=%v", recovered, err)
	}
	if exists, _ := b.Exists(context.Background(), bad); !exists {
		t.Fatal("failed manifest deleted")
	}
	if exists, _ := b.Exists(context.Background(), good); exists {
		t.Fatal("healthy recovery skipped")
	}
}

func TestRecoveryEmptyScopeDoesNothing(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	path, _ := seedRecoveryScope(t, m.ManifestManager, b, "db", "cpu", "hourly", "target")
	for _, scope := range []recoveryScope{{Databases: []string{}}, {Tiers: []string{}}} {
		count, err := m.ManifestManager.recoverOrphanedManifests(context.Background(), scope, nil, nil)
		if count != 0 || err != nil {
			t.Fatalf("count=%d err=%v", count, err)
		}
	}
	if exists, _ := b.Exists(context.Background(), path); !exists {
		t.Fatal("empty scope recovered unrelated work")
	}
}

func TestCycleIndependentDiscoveryTimeoutIsFailure(t *testing.T) {
	m, _, cleanup := setupTestManager(t)
	defer cleanup()
	m.ManifestManager = nil
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) { return nil, context.DeadlineExceeded }}}
	_, err := m.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"})
	if err == nil {
		t.Fatal("timeout swallowed")
	}
	if got := cycleOutcomeIssue915(t, m); got["status"] != "failed" {
		t.Fatalf("operation timeout mistaken for cycle expiration: %v", got)
	}
}

func TestRecoveryScopeListsSelectedDatabasePrefix(t *testing.T) {
	_, b, cleanup := setupTestManager(t)
	defer cleanup()
	backend := &scopeListingBackend{Backend: b}
	mm := NewManifestManager(backend, zerolog.Nop())
	_, err := mm.recoverOrphanedManifests(context.Background(), recoveryScope{Databases: []string{"db"}, Measurement: "cpu", Tiers: []string{"hourly"}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(backend.prefixes) != 1 || backend.prefixes[0] != "_compaction_state/hourly/db/" {
		t.Fatal(backend.prefixes)
	}
}

type scopeListingBackend struct {
	storage.Backend
	prefixes []string
}

func (b *scopeListingBackend) List(ctx context.Context, prefix string) ([]string, error) {
	if strings.HasPrefix(prefix, ManifestBasePath) {
		b.prefixes = append(b.prefixes, prefix)
	}
	return b.Backend.List(ctx, prefix)
}

func TestCycleNamespaceScopeRecovery(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	target, _ := seedRecoveryScope(t, m.ManifestManager, b, "spoke/child", "cpu", "hourly", "target")
	unrelated, _ := seedRecoveryScope(t, m.ManifestManager, b, "other/child", "cpu", "hourly", "unrelated")
	m.SetNamespaceExpander(func(context.Context) (map[string]struct{}, error) { return map[string]struct{}{"spoke": {}}, nil })
	m.Tiers = []Tier{cycleTierIssue915{find: func(_ context.Context, db, measurement string) ([]Candidate, error) {
		if db != "spoke/child" || measurement != "cpu" {
			t.Errorf("unexpected discovery %s/%s", db, measurement)
		}
		return nil, nil
	}}}
	if _, err := m.RunCompactionCycleForMeasurement(context.Background(), "spoke", "cpu", []string{"hourly"}); err != nil {
		t.Fatal(err)
	}
	if exists, _ := b.Exists(context.Background(), target); exists {
		t.Fatal("selected namespace not recovered")
	}
	if exists, _ := b.Exists(context.Background(), unrelated); !exists {
		t.Fatal("unrelated namespace recovered")
	}
}

func TestScheduledCycleExpiresAtConfiguredBudget(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	m.ManifestManager = nil
	m.CycleTimeout = 20 * time.Millisecond
	if err := b.Write(context.Background(), "db/cpu/2026/01/01/00/input.parquet", []byte("fixture")); err != nil {
		t.Fatal(err)
	}
	m.Tiers = []Tier{cycleTierIssue915{find: func(ctx context.Context, _, _ string) ([]Candidate, error) { <-ctx.Done(); return nil, ctx.Err() }}}
	scheduler := &Scheduler{manager: m, tierNames: []string{"hourly"}, logger: zerolog.Nop()}
	scheduler.runCompaction()
	if outcome := cycleOutcomeIssue915(t, m); outcome["status"] != "timed_out" || outcome["failed_batches"] != int64(0) || outcome["discovery_errors"] != int64(0) {
		t.Fatal(outcome)
	}
	if m.IsCycleRunning() {
		t.Fatal("expired scheduled cycle retained exclusion guard")
	}
}
