package compaction

import (
	"context"
	"errors"
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
	// Recovery spans tiers on purpose: the daily orphan of the selected
	// database/measurement is recovered by an hourly-scoped cycle.
	fixtures := []fixture{{"db", "cpu", "hourly", "target", true}, {"db", "cpu_extra", "hourly", "othermeasurement", false}, {"db_extra", "cpu", "hourly", "otherdatabase", false}, {"db", "cpu", "daily", "othertier", true}, {"db/child", "cpu", "hourly", "namespacechild", false}}
	paths := make([][2]string, len(fixtures))
	for i, f := range fixtures {
		paths[i][0], paths[i][1] = seedRecoveryScope(t, m.ManifestManager, b, f.db, f.measurement, f.tier, f.id)
	}
	count, err := m.ManifestManager.recoverOrphanedManifests(context.Background(), recoveryScope{Databases: []string{"db"}, Measurement: "cpu"}, nil, nil)
	if err != nil || count != 2 {
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

// A transient read failure is the case that must fail closed: the manifest
// may name inputs of an in-flight job, so it is retained and the cache
// refuses to publish a partial view.
func TestRecoveryTransientReadFailureRetainsManifest(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	path, _ := seedRecoveryScope(t, m.ManifestManager, b, "db", "cpu", "hourly", "flaky")
	failure := errors.New("transient storage read failure")
	mm := NewManifestManager(recoveryReadFailure{b, path, failure}, zerolog.Nop())
	count, err := mm.recoverOrphanedManifests(ctx, recoveryScope{Databases: []string{"db"}, Measurement: "cpu"}, nil, nil)
	if !errors.Is(err, failure) || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if exists, _ := b.Exists(ctx, path); !exists {
		t.Fatal("manifest deleted on a transient read failure")
	}
	// Twice: a regression that stored the partial cache before returning
	// the error would answer the second call from it, without the error.
	for i := 0; i < 2; i++ {
		if files, err := mm.GetFilesInManifests(ctx); !errors.Is(err, failure) {
			t.Fatalf("call %d: incomplete manifest cache accepted: files=%v err=%v", i, files, err)
		}
	}
}

// An undecodable manifest (zero-length after a crash, or garbage) must not
// hold the node's compaction hostage: recovery parks it, the cache ignores
// it, and the healthy manifest next to it is still honored.
func TestUnparseableManifestsAreParkedAndDoNotBlockCompaction(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	empty := ManifestBasePath + "/hourly/db/empty.json"
	garbage := ManifestBasePath + "/daily/db/garbage.json"
	for path, body := range map[string][]byte{empty: nil, garbage: []byte("{corrupt")} {
		if err := b.Write(ctx, path, body); err != nil {
			t.Fatal(err)
		}
	}
	healthy, healthyInput := seedRecoveryScope(t, m.ManifestManager, b, "db", "cpu", "hourly", "healthy")
	// A real partition file, so listDatabases finds "db" on its own.
	eligible := []string{"db/cpu/2026/01/01/01/a.parquet", "db/cpu/2026/01/01/01/b.parquet"}
	for _, p := range eligible {
		if err := b.Write(ctx, p, []byte("row")); err != nil {
			t.Fatal(err)
		}
	}
	// Before recovery runs, the cache must already see past the bad files.
	files, err := m.ManifestManager.GetFilesInManifests(ctx)
	if err != nil {
		t.Fatalf("cache failed closed on an unparseable manifest: %v", err)
	}
	if _, ok := files[healthyInput]; !ok {
		t.Fatalf("healthy manifest lost with the unparseable ones: %v", files)
	}
	m.Tiers = []Tier{cycleTierIssue915{find: func(_ context.Context, db, meas string) ([]Candidate, error) {
		if db != "db" || meas != "cpu" {
			return nil, nil
		}
		c := candidateIssue915("db/cpu/2026/01/01/01")
		c.Files = eligible
		return []Candidate{c}, nil
	}}}
	started := 0
	m.compactBatchForTest = func(context.Context, Candidate) error { started++; return nil }
	if _, err := m.RunCompactionCycleForTiers(ctx, []string{"hourly"}); err != nil {
		t.Fatalf("cycle failed: %v outcome=%v", err, cycleOutcomeIssue915(t, m))
	}
	if outcome := cycleOutcomeIssue915(t, m); outcome["status"] != "completed" || outcome["discovery_errors"] != int64(0) || started != 1 {
		t.Fatalf("started=%d outcome=%v", started, outcome)
	}
	for _, path := range []string{empty, garbage} {
		if exists, _ := b.Exists(ctx, path); exists {
			t.Fatalf("unparseable manifest still in the work set: %s", path)
		}
		parked, _ := quarantinePathFor(path)
		if exists, _ := b.Exists(ctx, parked); !exists {
			t.Fatalf("unparseable manifest deleted instead of parked: %s", path)
		}
	}
	if exists, _ := b.Exists(ctx, healthy); exists {
		t.Fatal("healthy orphan not recovered alongside the parked ones")
	}
	if got := m.Stats()["total_manifests_recover"]; got != 1 {
		t.Fatalf("parked manifests must not count as recovered: %v", got)
	}
}

// Parking honors the database scope from the path; the measurement cannot
// be known, so a measurement-scoped cycle parks any undecodable manifest of
// its database and none of another database's.
func TestUnparseableManifestParkingHonorsDatabaseScope(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	mine := ManifestBasePath + "/hourly/db/mine.json"
	spoke := ManifestBasePath + "/hourly/spoke/child/theirs.json"
	other := ManifestBasePath + "/hourly/db_extra/theirs.json"
	for _, path := range []string{mine, spoke, other} {
		if err := b.Write(ctx, path, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.ManifestManager.recoverOrphanedManifests(ctx, recoveryScope{Databases: []string{"db"}, Measurement: "cpu"}, nil, nil); err != nil {
		t.Fatal(err)
	}
	for path, wantParked := range map[string]bool{mine: true, spoke: false, other: false} {
		exists, _ := b.Exists(ctx, path)
		if exists == wantParked {
			t.Errorf("%s exists=%v wantParked=%v", path, exists, wantParked)
		}
	}
	if _, err := m.ManifestManager.recoverOrphanedManifests(ctx, recoveryScope{Databases: []string{"spoke/child"}}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if exists, _ := b.Exists(ctx, spoke); exists {
		t.Error("spoke pseudo-database not matched from the manifest path")
	}
	if exists, _ := b.Exists(ctx, other); !exists {
		t.Error("out-of-scope unparseable manifest parked")
	}
}

// The hourly scheduler's cycle must recover a daily orphan: an orphan is the
// post-upload, pre-deletion state in which the output and its inputs coexist,
// and waiting for the daily tick (or forever, once daily is disabled) leaves
// that state in place.
func TestScheduledHourlyCycleRecoversDailyOrphan(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	path, input := seedRecoveryScope(t, m.ManifestManager, b, "db", "cpu", "daily", "orphan")
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) { return nil, nil }}}
	scheduler := &Scheduler{manager: m, tierNames: []string{"hourly"}, logger: zerolog.Nop()}
	scheduler.runCompaction()
	if exists, _ := b.Exists(ctx, path); exists {
		t.Fatal("daily orphan left for the daily tick by the hourly cycle")
	}
	if exists, _ := b.Exists(ctx, input); exists {
		t.Fatal("consumed input of the daily orphan not deleted")
	}
	if got := m.Stats()["total_manifests_recover"]; got != 1 {
		t.Fatalf("total_manifests_recover=%v", got)
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
	count, err := m.ManifestManager.recoverOrphanedManifests(context.Background(), recoveryScope{Databases: []string{}}, nil, nil)
	if count != 0 || err != nil {
		t.Fatalf("count=%d err=%v", count, err)
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
