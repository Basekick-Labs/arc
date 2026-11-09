package tiering

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// #1550: whether this node's cold tier rows may vouch for an empty cold
// range when the cold tier cannot be listed.

// tiering.Manager is the backup package's cold restore guard.
var _ interface {
	BeginColdRestore() error
	EndColdRestore()
} = (*Manager)(nil)

func markerIn(t *testing.T) string {
	t.Helper()
	return ColdMetadataMarkerPath(filepath.Join(t.TempDir(), "arc.db"))
}

func coldObjects(paths ...string) []storage.ObjectInfo {
	out := make([]storage.ObjectInfo, 0, len(paths))
	for _, p := range paths {
		out = append(out, storage.ObjectInfo{Path: p, Size: 10, LastModified: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)})
	}
	return out
}

// authManager is an integration manager whose trust state was loaded from
// cfg, as NewManager does.
func authManager(t *testing.T, cfg ManagerConfig) (*Manager, *mockBackend, *mockBackend) {
	t.Helper()
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	t.Cleanup(cleanup)
	m.initColdAuthority(&cfg)
	return m, hot, cold
}

// completeSync runs a cold sync over objects that records every one.
func completeSync(t *testing.T, m *Manager, cold *mockBackend, paths ...string) {
	t.Helper()
	if len(paths) == 0 {
		paths = []string{"db1/cpu/2024/03/15/cpu_a_daily.parquet"}
	}
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: coldObjects(paths...)}
	if _, err := m.syncColdTierMetadata(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

// A database no complete sync has filled is not trusted: a fresh, lost or
// moved arc.db over a cold bucket that already holds data.
func TestColdRowsUntrustedUntilACompleteSync(t *testing.T) {
	m, _, cold := authManager(t, ManagerConfig{ColdMetadataMarkerPath: markerIn(t)})
	ok, reason := m.ColdMetadataAuthority()
	if ok || reason != coldAuthorityReasonNoSync {
		t.Fatalf("fresh database: authoritative = %v (%q), want false, no complete sync", ok, reason)
	}
	completeSync(t, m, cold)
	if ok, reason := m.ColdMetadataAuthority(); !ok {
		t.Fatalf("after a complete sync: authoritative = false (%q)", reason)
	}
}

// The flag is in the database: a manager started later on the same database
// reads it (the NewManager → ManagerConfig path).
func TestCompleteSyncSurvivesARestart(t *testing.T) {
	m, _, cold := authManager(t, ManagerConfig{ColdMetadataMarkerPath: markerIn(t)})
	completeSync(t, m, cold)

	var again coldAuthority
	if err := again.load(context.Background(), m.metadata, "", false); err != nil {
		t.Fatal(err)
	}
	if !again.complete.Load() {
		t.Fatal("a restarted manager did not read the complete-sync flag from the database")
	}
}

// A clustered deployment is never trusted, with or without a coordinator
// (and so a manifest): the cluster.enabled node whose coordinator failed to
// start or whose licence lacks clustering.
func TestClusteredDeploymentNeverTrusted(t *testing.T) {
	m, _, cold := authManager(t, ManagerConfig{ClusteredDeployment: true})
	completeSync(t, m, cold)
	if m.manifest != nil {
		t.Fatal("test setup: expected no manifest")
	}
	ok, reason := m.ColdMetadataAuthority()
	if ok || !strings.Contains(reason, "cluster") {
		t.Fatalf("clustered deployment without a coordinator: authoritative = %v (%q)", ok, reason)
	}
}

func TestClusterManifestNeverTrusted(t *testing.T) {
	m, hot, cold := authManager(t, ManagerConfig{})
	completeSync(t, m, cold)
	m.manifest = &fakeManifest{hot: hot, entries: map[string]int64{}}
	if m.ColdMetadataAuthoritative() {
		t.Fatal("a node with a cluster manifest was trusted")
	}
}

func TestColdMetadataAuthority_NilManager(t *testing.T) {
	var m *Manager
	if m.ColdMetadataAuthoritative() {
		t.Fatal("nil manager reported authoritative")
	}
	m.BeginColdRestore()
	m.EndColdRestore()
}

// The boot marker (a staged metadata restore) overrides the restored
// database's flag; a complete sync removes it.
func TestRestoreMarkerOverridesTheRestoredFlag(t *testing.T) {
	marker := markerIn(t)
	m, _, cold := authManager(t, ManagerConfig{ColdMetadataMarkerPath: marker})
	completeSync(t, m, cold) // the backup's database had the flag

	if err := MarkColdMetadataUnverified(strings.TrimSuffix(marker, ColdMetadataUnverifiedSuffix), ColdMetadataReasonRestored); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(marker); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("marker stat = %v, %v; want a 0600 file", info, err)
	}
	m.initColdAuthority(&ManagerConfig{ColdMetadataMarkerPath: marker}) // the boot after the swap
	ok, reason := m.ColdMetadataAuthority()
	if ok || !strings.Contains(reason, ColdMetadataReasonRestored) {
		t.Fatalf("after a restore marker: authoritative = %v (%q)", ok, reason)
	}

	completeSync(t, m, cold)
	if !m.ColdMetadataAuthoritative() {
		t.Fatal("a complete sync did not clear the restore marker")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("marker still on disk after a complete sync: %v", err)
	}
}

// A sync that returns nil but could not record a listed object proves rows
// are missing: it marks them, and only a later complete sync clears that.
func TestUnrecordedObjectMarksTheRows(t *testing.T) {
	marker := markerIn(t)
	m, _, cold := authManager(t, ManagerConfig{ColdMetadataMarkerPath: marker})
	completeSync(t, m, cold)

	bad := "db1/cpu/2024/03/16/cpu_bad_daily.parquet"
	if _, err := m.metadata.db.Exec(`CREATE TRIGGER fail_one_cold_row BEFORE INSERT ON tier_files WHEN NEW.path = '` + bad + `' BEGIN SELECT RAISE(ABORT, 'forced row failure'); END`); err != nil {
		t.Fatal(err)
	}
	completeSync(t, m, cold, "db1/cpu/2024/03/16/cpu_ok_daily.parquet", bad)
	if m.ColdMetadataAuthoritative() {
		t.Fatal("a sync that left a listed object without a row kept the rows trusted")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("no marker after an incomplete sync: %v", err)
	}
	if complete, _ := m.metadata.coldRowsComplete(context.Background()); complete {
		t.Fatal("the complete-sync flag survived an incomplete sync")
	}
}

// A hot-only database's cold object whose hot copy could not be checked is
// skipped by the sync without a row: the rows are marked.
func TestSkippedHotOnlyObjectMarksTheRows(t *testing.T) {
	m, hot, cold := authManager(t, ManagerConfig{})
	completeSync(t, m, cold)
	ctx := context.Background()
	if err := m.policies.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
		t.Fatal(err)
	}
	m.hotBackend = &hotOnlyExistsErrorBackend{mockBackend: hot}
	completeSync(t, m, cold, "db1/cpu/2024/03/17/cpu_a_daily.parquet")
	if m.ColdMetadataAuthoritative() {
		t.Fatal("a sync that skipped an object it could not check kept the rows trusted")
	}
}

// A marking that lands while a sync runs concerns objects its listing may
// not hold: that sync must not clear it.
func TestMarkingDuringASyncSurvivesIt(t *testing.T) {
	m, _, cold := authManager(t, ManagerConfig{})
	completeSync(t, m, cold)
	ctx := context.Background()
	start := m.coldAuth.gen.Load()
	if _, err := m.coldAuth.mark(ctx, m.metadata, coldAuthorityReasonRestore); err != nil {
		t.Fatal(err)
	}
	if cleared, err := m.coldAuth.clear(ctx, m.metadata, start); cleared || err != nil {
		t.Fatalf("clear after a mid-sync marking = %v, %v; want kept", cleared, err)
	}
	if cleared, err := m.coldAuth.clear(ctx, m.metadata, m.coldAuth.gen.Load()); !cleared || err != nil {
		t.Fatalf("clear by a later sync = %v, %v; want cleared", cleared, err)
	}
}

// A restore that may write to cold marks the rows before it starts, and no
// sync clears that until it ends: a crash mid-restore leaves it marked.
func TestColdRestoreWindowBlocksClearing(t *testing.T) {
	marker := markerIn(t)
	m, _, cold := authManager(t, ManagerConfig{ColdMetadataMarkerPath: marker})
	completeSync(t, m, cold)

	m.BeginColdRestore()
	if m.ColdMetadataAuthoritative() {
		t.Fatal("rows trusted while a restore into cold runs")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("no marker on disk at restore start: %v", err)
	}
	completeSync(t, m, cold) // a sync during the restore
	if m.ColdMetadataAuthoritative() {
		t.Fatal("a sync during the restore cleared the marking")
	}

	m.EndColdRestore()
	completeSync(t, m, cold)
	if !m.ColdMetadataAuthoritative() {
		t.Fatal("a complete sync after the restore did not clear the marking")
	}
}

// A marker that could not be written still leaves the rows untrusted after a
// restart, because the database flag is forgotten too.
func TestMarkForgetsTheFlagEvenWithoutAMarkerFile(t *testing.T) {
	m, _, cold := authManager(t, ManagerConfig{ColdMetadataMarkerPath: filepath.Join(t.TempDir(), "missing-dir", "arc.db"+ColdMetadataUnverifiedSuffix)})
	completeSync(t, m, cold)
	// The flag was forgotten, so the marking is persisted: no error.
	if err := m.markColdMetadataUnverified(coldAuthorityReasonRestore); err != nil {
		t.Fatalf("marking with the flag forgotten reported %v", err)
	}

	var again coldAuthority
	if err := again.load(context.Background(), m.metadata, "", false); err != nil {
		t.Fatal(err)
	}
	if again.complete.Load() {
		t.Fatal("after a marking whose file could not be written, a restart trusts the rows")
	}
}

// HasAnyColdRowsInRange counts a quarantined cold row as cold data, where
// HasColdFilesInRange (#1105) does not: DuckDB's glob still reads the object
// behind it, so a read that leaves the cold tier out on metadata's word would
// drop its rows (#1550). Bounds behave as in HasColdFilesInRange.
func TestHasAnyColdRowsInRangeCountsQuarantinedRows(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	recordAt(t, store, "db3/cpu/2025/01/01/q_daily.parquet", "db3", TierCold) // 2025-01-01 00:00
	if err := store.QuarantineFile(ctx, "db3/cpu/2025/01/01/q_daily.parquet", "x"); err != nil {
		t.Fatal(err)
	}
	recordAt(t, store, "db2/cpu/2025/01/01/00/h.parquet", "db2", TierHot)
	m := &Manager{metadata: store}
	at := func(s string) *time.Time { v, _ := time.Parse(time.RFC3339, s); return &v }
	for _, c := range []struct {
		name       string
		db         string
		start, end *time.Time
		want       bool
	}{
		{"quarantined, open range", "db3", nil, nil, true},
		{"quarantined, range covering it", "db3", at("2025-01-01T15:00:00Z"), at("2025-01-01T16:00:00Z"), true},
		{"quarantined, range the next day", "db3", at("2025-01-02T01:00:00Z"), nil, false},
		{"hot rows only", "db2", nil, nil, false},
	} {
		got, err := m.HasAnyColdRowsInRange(ctx, c.db, "cpu", c.start, c.end)
		if err != nil || got != c.want {
			t.Errorf("%s: %v, %v; want %v", c.name, got, err, c.want)
		}
	}
	// The #1105 guard's view is unchanged.
	if got, err := m.HasColdFilesInRange(ctx, "db3", "cpu", nil, nil); err != nil || got {
		t.Errorf("HasColdFilesInRange on a quarantined-only measurement = %v, %v; want false", got, err)
	}
}

// A marking neither the marker file nor the database flag recorded would be
// lost on a restart: BeginColdRestore reports it and leaves no window open.
func TestBeginColdRestoreFailsWhenTheMarkingCannotPersist(t *testing.T) {
	m, _, cold := authManager(t, ManagerConfig{ColdMetadataMarkerPath: filepath.Join(t.TempDir(), "missing-dir", "arc.db"+ColdMetadataUnverifiedSuffix)})
	completeSync(t, m, cold)
	// The flag write fails too: the database is closed.
	if err := m.metadata.db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.BeginColdRestore(); err == nil {
		t.Fatal("BeginColdRestore succeeded with nothing persisted")
	}
	if n := m.coldAuth.restoring.Load(); n != 0 {
		t.Fatalf("restoring = %d after a refused begin; want 0", n)
	}
	if m.ColdMetadataAuthoritative() {
		t.Fatal("rows trusted in memory after a failed marking")
	}
}

// An object the sync cannot parse under a measurement that has cold rows is
// read by that measurement's fallback glob (db/measurement/**/*.parquet), so
// it leaves the rows untrusted, on the first sync as on later ones. One
// outside every tiered measurement (a shared bucket's other contents) does
// not.
func TestUnparseableObjectUnderATieredMeasurementKeepsRowsUntrusted(t *testing.T) {
	const good = "db1/cpu/2024/03/15/cpu_a_daily.parquet"
	for _, c := range []struct {
		name    string
		objects []string
		trusted bool
	}{
		{"under a tiered measurement", []string{good, "db1/cpu/stray.parquet"}, false},
		{"outside every tiered measurement", []string{good, "warehouse/t/stray.parquet"}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			m, _, cold := authManager(t, ManagerConfig{ColdMetadataMarkerPath: markerIn(t)})
			completeSync(t, m, cold, c.objects...)
			if ok, reason := m.ColdMetadataAuthority(); ok != c.trusted {
				t.Fatalf("authoritative = %v (%q), want %v", ok, reason, c.trusted)
			}
		})
	}
}
