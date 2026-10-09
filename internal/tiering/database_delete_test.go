package tiering

import (
	"context"
	"errors"
	"strings"
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

// TestPrepareDatabaseDeleteIgnoresManifestPathsOutsideTheDatabase is the
// regression for the cross-database delete.
//
// The cluster manifest indexes entries by their Database FIELD, and for
// edge-sync files that field is deliberately not the first path segment: the
// hub registers Path as the namespaced "{spoke}/{db}/..." while Database comes
// from the spoke's own pre-namespacing path, so filesByDB["telemetry"] holds
// "rocket-01/telemetry/...". Proposing those paths for deletion removed a
// SPOKE's files from the manifest, and the delete callback then unlinks them
// on every node.
func TestPrepareDatabaseDeleteIgnoresManifestPathsOutsideTheDatabase(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	const own = "telemetry/cpu/2024/03/15/00/own.parquet"
	const spoke = "rocket-01/telemetry/cpu/2024/03/15/00/spoke.parquet"
	mustWrite(t, hot, own)
	mustWrite(t, hot, spoke)
	fm := &fakeManifest{
		hot:     hot,
		entries: map[string]int64{own: 7, spoke: 7},
		byDatabase: map[string][]string{
			// Exactly what the FSM returns: both, because both entries carry
			// Database "telemetry".
			"telemetry": {own, spoke},
		},
	}
	m.manifest = fm

	if err := m.PrepareDatabaseDelete(context.Background(), "telemetry", nil); err != nil {
		t.Fatalf("PrepareDatabaseDelete: %v", err)
	}

	var proposed []string
	for _, call := range fm.calls {
		proposed = append(proposed, call.paths...)
	}
	for _, p := range proposed {
		if p == spoke {
			t.Errorf("proposed a path outside the database for deletion: %q — this unlinks a spoke's file on every node", p)
		}
	}
	found := false
	for _, p := range proposed {
		if p == own {
			found = true
		}
	}
	if !found {
		t.Errorf("the database's own manifest path was not proposed; proposed=%v", proposed)
	}
}

// failingListBackend makes the cold LIST fail while leaving the object there,
// which is the state a cold-store blip produces.
type failingListBackend struct {
	*mockBackend
}

func (b *failingListBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, errors.New("cold store unreachable")
}

// TestCleanupDatabaseDeleteRetainsColdRowsWhenTheColdListFails pins the guard
// that decides nothing when it knows nothing. A failed LIST means the cold
// contents are unknown; retiring rows then would strand every object it could
// not see — present on the cold store, referenced by nothing, invisible to
// queries and billed forever.
func TestCleanupDatabaseDeleteRetainsColdRowsWhenTheColdListFails(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	path := "db1/cpu/unlisted.parquet"
	mustWrite(t, cold, path)
	row := *coldRowFor(path)
	row.Tier = TierCold
	if _, err := m.metadata.RecordColdFile(ctx, &row, time.Now()); err != nil {
		t.Fatal(err)
	}
	m.coldBackend = &failingListBackend{mockBackend: cold}

	removed, errs := m.CleanupDatabaseDelete(ctx, "db1", nil, nil)
	if removed != 0 {
		t.Errorf("removed %d cold objects despite an unreadable cold store, want 0", removed)
	}
	if len(errs) == 0 {
		t.Error("a failed cold listing was not reported")
	}
	got, err := m.metadata.GetFile(ctx, path)
	if err != nil || got == nil || got.Tier != TierCold {
		t.Fatalf("cold row after a failed listing = (%+v, %v), want retained cold row", got, err)
	}
	if exists, err := cold.Exists(ctx, path); err != nil || !exists {
		t.Fatalf("cold object after a failed listing: exists=%v err=%v, want present", exists, err)
	}
}

// TestCleanupDatabaseDeleteRetainsColdRowsWhenColdIsDisabled covers the other
// half: cold storage turned off, so there is no backend to verify or delete
// through. The rows and objects genuinely survive, so the deletion is
// incomplete and must say so — once, with the remedy, not once per row.
func TestCleanupDatabaseDeleteRetainsColdRowsWhenColdIsDisabled(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	for _, p := range []string{"db1/cpu/a.parquet", "db1/cpu/b.parquet", "db1/cpu/c.parquet"} {
		mustWrite(t, cold, p)
		row := *coldRowFor(p)
		row.Tier = TierCold
		if _, err := m.metadata.RecordColdFile(ctx, &row, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	// What tiered_storage.cold.enabled=false produces: ColdBackend() is nil.
	m.config.Cold.Enabled = false

	removed, errs := m.CleanupDatabaseDelete(ctx, "db1", nil, nil)
	if removed != 0 {
		t.Errorf("removed %d cold objects with no cold backend, want 0", removed)
	}
	if len(errs) != 1 {
		t.Fatalf("errors = %d (%v), want exactly 1 aggregated error for all three rows", len(errs), errs)
	}
	if !strings.Contains(errs[0].Error(), "cold.enabled") {
		t.Errorf("the error does not name the remedy: %v", errs[0])
	}
	for _, p := range []string{"db1/cpu/a.parquet", "db1/cpu/b.parquet", "db1/cpu/c.parquet"} {
		got, err := m.metadata.GetFile(ctx, p)
		if err != nil || got == nil || got.Tier != TierCold {
			t.Fatalf("cold row %q = (%+v, %v), want retained", p, got, err)
		}
	}
}

// TestBeginDatabaseDeleteRefusesWhenRoleGated pins the gate that makes the
// reservation meaningful. BeginDatabaseDelete reserves two NODE-LOCAL atomics,
// while a migration cycle runs only on the primary writer — so a delete served
// by a reader reserves nothing on the node that migrates, and the cycle can
// copy a hot file to cold after the delete has already swept that tier.
//
// The nil-gate case is the other half, and it must NOT refuse: a local-storage
// cluster without replication wires no gate deliberately, because each node's
// metadata is authoritative for its own disk.
func TestBeginDatabaseDeleteRefusesWhenRoleGated(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	gate := &mockGate{role: "reader"}
	gate.primary.Store(false)
	m.clusterGate = gate

	release, err := m.BeginDatabaseDelete()
	if !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("BeginDatabaseDelete on a non-primary node = %v, want ErrMigrationRoleGated", err)
	}
	if release != nil {
		t.Error("a refused reservation returned a release func")
	}
	if gated, role := m.MigrationGate(); !gated || role != "reader" {
		t.Errorf("MigrationGate() = (%v, %q), want (true, \"reader\")", gated, role)
	}

	// Promoted: the same node may now delete.
	gate.primary.Store(true)
	release, err = m.BeginDatabaseDelete()
	if err != nil {
		t.Fatalf("BeginDatabaseDelete on the primary writer = %v, want nil", err)
	}
	release()

	// No gate at all: allowed, by design.
	m.clusterGate = nil
	release, err = m.BeginDatabaseDelete()
	if err != nil {
		t.Fatalf("BeginDatabaseDelete with no cluster gate = %v, want nil", err)
	}
	release()
}

// TestCleanupDatabaseDeleteLeavesForeignColdObjectsAlone pins the filter that
// keeps a shared cold bucket safe. The cold store may hold something that is
// not Arc — an Iceberg warehouse, say — and a database-name prefix is not
// licence to delete a foreign object under it. The cold metadata sync applies
// the same .parquet filter for the same reason.
func TestCleanupDatabaseDeleteLeavesForeignColdObjectsAlone(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()

	const arcObject = "db1/cpu/real.parquet"
	const foreign = "db1/metadata/v1.metadata.json"
	mustWrite(t, cold, arcObject)
	mustWrite(t, cold, foreign)
	row := *coldRowFor(arcObject)
	row.Tier = TierCold
	if _, err := m.metadata.RecordColdFile(ctx, &row, time.Now()); err != nil {
		t.Fatal(err)
	}

	removed, errs := m.CleanupDatabaseDelete(ctx, "db1", nil, nil)
	if len(errs) != 0 {
		t.Fatalf("cleanup errors = %v, want none", errs)
	}
	if removed != 1 {
		t.Errorf("removed %d cold objects, want 1 (only the parquet)", removed)
	}
	if exists, err := cold.Exists(ctx, foreign); err != nil || !exists {
		t.Errorf("a non-Arc object under the database prefix was deleted: exists=%v err=%v", exists, err)
	}
	if exists, err := cold.Exists(ctx, arcObject); err != nil || exists {
		t.Errorf("the database's own cold object survived: exists=%v err=%v", exists, err)
	}
}
