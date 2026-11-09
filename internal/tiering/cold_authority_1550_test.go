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

func TestColdMetadataAuthority_Standalone(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	if ok, reason := m.ColdMetadataAuthority(); !ok {
		t.Fatalf("standalone node without a marker: authoritative = false (%s)", reason)
	}
}

func TestColdMetadataAuthority_ClusterNodeNever(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	m.manifest = &fakeManifest{hot: hot, entries: map[string]int64{}}
	ok, reason := m.ColdMetadataAuthority()
	if ok || !strings.Contains(reason, "cluster") {
		t.Fatalf("cluster node: authoritative = %v (%q), want false naming the cluster", ok, reason)
	}
}

func TestColdMetadataAuthority_NilManager(t *testing.T) {
	var m *Manager
	if m.ColdMetadataAuthoritative() {
		t.Fatal("nil manager reported authoritative")
	}
}

// A marker written before the manager starts (main.go writes it when a staged
// metadata restore is applied) makes the rows non-authoritative, and a
// complete cold sync clears it from disk and memory.
func TestColdMetadataMarkerClearedByACompleteSync(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	marker := markerIn(t)
	if err := MarkColdMetadataUnverified(strings.TrimSuffix(marker, ColdMetadataUnverifiedSuffix), ColdMetadataReasonRestored); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(marker); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("marker stat = %v, %v; want a 0600 file", info, err)
	}
	if err := m.coldAuth.load(marker); err != nil {
		t.Fatal(err)
	}
	ok, reason := m.ColdMetadataAuthority()
	if ok || !strings.Contains(reason, ColdMetadataReasonRestored) {
		t.Fatalf("after a restore marker: authoritative = %v (%q)", ok, reason)
	}

	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: coldObjects("db1/cpu/2024/03/15/cpu_a_daily.parquet")}
	if _, err := m.syncColdTierMetadata(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if !m.ColdMetadataAuthoritative() {
		t.Fatal("a complete cold sync did not make the rows authoritative again")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("marker still on disk after a complete sync: %v", err)
	}
}

// A sync that returns nil but could not record a listed object is not
// complete: the marker stays.
func TestColdMetadataMarkerKeptWhenARowCannotBeRecorded(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	marker := markerIn(t)
	if err := m.coldAuth.load(marker); err != nil {
		t.Fatal(err)
	}
	m.markColdMetadataUnverified(ColdMetadataReasonRestored)

	bad := "db1/cpu/2024/03/15/cpu_bad_daily.parquet"
	if _, err := m.metadata.db.Exec(`CREATE TRIGGER fail_one_cold_row BEFORE INSERT ON tier_files WHEN NEW.path = '` + bad + `' BEGIN SELECT RAISE(ABORT, 'forced row failure'); END`); err != nil {
		t.Fatal(err)
	}
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: coldObjects("db1/cpu/2024/03/15/cpu_ok_daily.parquet", bad)}
	if _, err := m.syncColdTierMetadata(context.Background()); err != nil {
		t.Fatalf("sync returned %v; a per-row failure is logged, not returned", err)
	}
	if m.ColdMetadataAuthoritative() {
		t.Fatal("a sync that left a listed object without a row cleared the marker")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("marker gone after an incomplete sync: %v", err)
	}
}

// A hot-only database's cold object whose hot copy could not be checked is
// skipped by the sync; that leaves it without a row, so the marker stays.
func TestColdMetadataMarkerKeptWhenAHotOnlyObjectIsSkipped(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	m.markColdMetadataUnverified(ColdMetadataReasonRestored)
	if err := m.policies.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
		t.Fatal(err)
	}
	m.hotBackend = &hotOnlyExistsErrorBackend{mockBackend: hot}
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: coldObjects("db1/cpu/2024/03/15/cpu_a_daily.parquet")}
	if _, err := m.syncColdTierMetadata(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if m.ColdMetadataAuthoritative() {
		t.Fatal("a sync that skipped an object it could not check cleared the marker")
	}
}

// A marking that lands while a sync runs concerns objects the sync's listing
// may not hold: that sync must not clear it.
func TestColdMetadataMarkingDuringASyncSurvivesIt(t *testing.T) {
	var a coldAuthority
	if err := a.load(markerIn(t)); err != nil {
		t.Fatal(err)
	}
	if err := a.mark(ColdMetadataReasonRestored); err != nil {
		t.Fatal(err)
	}
	start := a.generation()
	if err := a.mark(coldAuthorityReasonRecord); err != nil {
		t.Fatal(err)
	}
	if cleared, err := a.clear(start); cleared || err != nil {
		t.Fatalf("clear after a mid-sync marking = %v, %v; want kept", cleared, err)
	}
	if cleared, err := a.clear(a.generation()); !cleared || err != nil {
		t.Fatalf("clear by a later sync = %v, %v; want cleared", cleared, err)
	}
}

// The marker persists: a manager started after a marking reads it.
func TestColdMetadataMarkerSurvivesARestart(t *testing.T) {
	marker := markerIn(t)
	var first coldAuthority
	if err := first.load(marker); err != nil {
		t.Fatal(err)
	}
	if err := first.mark(coldAuthorityReasonRecord); err != nil {
		t.Fatal(err)
	}
	var second coldAuthority
	if err := second.load(marker); err != nil {
		t.Fatal(err)
	}
	if unverified, reason := second.state(); !unverified || reason != coldAuthorityReasonRecord {
		t.Fatalf("after restart: unverified = %v, reason %q", unverified, reason)
	}
}

// A restore that could not record some of its cold rows marks the rows
// incomplete, on disk.
func TestRestoredColdFilesWithoutRowsMarkTheRows(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	marker := markerIn(t)
	if err := m.coldAuth.load(marker); err != nil {
		t.Fatal(err)
	}
	_, failed, err := m.RecordRestoredColdFiles(context.Background(), map[string]int64{"not-a-partition-path.parquet": 10})
	if err != nil || len(failed) != 1 {
		t.Fatalf("RecordRestoredColdFiles = failed %v, err %v; want the unparseable path failed", failed, err)
	}
	if m.ColdMetadataAuthoritative() {
		t.Fatal("cold rows still authoritative after a restore could not record one")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("marker not written: %v", err)
	}
}

// A restore that recorded every row leaves the rows authoritative.
func TestRestoredColdFilesAllRecordedKeepAuthority(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	if _, failed, err := m.RecordRestoredColdFiles(context.Background(), map[string]int64{"db1/cpu/2024/03/15/cpu_a_daily.parquet": 10}); err != nil || len(failed) != 0 {
		t.Fatalf("RecordRestoredColdFiles: failed %v, err %v", failed, err)
	}
	if !m.ColdMetadataAuthoritative() {
		t.Fatal("a fully recorded restore marked the rows incomplete")
	}
}
