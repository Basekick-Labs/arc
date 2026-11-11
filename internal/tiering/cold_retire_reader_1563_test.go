package tiering

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

// #1563: a cluster reader retires cold rows whose object the primary removed,
// once the cold object and the hot copy are both confirmed gone.

// otherCold is a listed object under another measurement, so the listing is
// not empty (an empty listing with an unknown store identity retires nothing).
const otherCold = "db1/mem/2024/03/15/mem_a_daily.parquet"

func readerRig(t *testing.T) (*Manager, *mockBackend, *mockBackend) {
	t.Helper()
	m, hot, cold := missingRig(t) // rows for missA and missB, recorded as primary
	cold.seedRaw(otherCold, []byte("x"))
	m.clusterGate = &mockGate{role: "reader"}
	return m, hot, cold
}

func coldRow(t *testing.T, m *Manager, path string) *FileMetadata {
	t.Helper()
	row, err := m.metadata.GetFile(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func TestReaderRetiresColdRowWhoseObjectsAreGone(t *testing.T) {
	m, _, cold := readerRig(t)
	before := metrics.Get().Snapshot()["tiering_cold_rows_retired_by_reader_total"].(int64)

	syncListing(t, m, cold, otherCold) // missA and missB gone from cold, no hot copy

	for _, p := range []string{missA, missB} {
		if row := coldRow(t, m, p); row != nil {
			t.Errorf("%s: row %+v still there, want it retired", p, row)
		}
	}
	if got := metrics.Get().Snapshot()["tiering_cold_rows_retired_by_reader_total"].(int64) - before; got != 2 {
		t.Errorf("retired counter grew by %d, want 2", got)
	}
}

// The primary and a standalone node keep the row: their DELETE and retention
// retire it, and #1551 reports it meanwhile.
func TestPrimaryDoesNotRetireColdRowsOnSync(t *testing.T) {
	m, _, cold := missingRig(t)
	cold.seedRaw(otherCold, []byte("x"))
	syncListing(t, m, cold, otherCold)
	if coldRow(t, m, missA) == nil {
		t.Fatal("a standalone/primary sync retired a cold row")
	}
}

// A cold object that exists at the check (written after the listing) keeps
// its row.
func TestReaderKeepsRowWhoseObjectExistsAtTheCheck(t *testing.T) {
	m, _, cold := readerRig(t)
	cold.seedRaw(missA, []byte("x"))
	syncListing(t, m, cold, otherCold) // the listing misses missA, the stat finds it
	if row := coldRow(t, m, missA); row == nil || row.Tier != TierCold {
		t.Fatalf("missA row = %+v, want it kept cold", row)
	}
	if coldRow(t, m, missB) != nil {
		t.Fatal("missB not retired")
	}
}

// A hot copy that the manifest still lists turns the row back to hot, for any
// database on a reader (it used to be only hot-only databases), as the
// primary's DELETE and retention do.
func TestReaderRevertsColdRowWithAHotCopy(t *testing.T) {
	m, hot, cold := readerRig(t)
	hot.seedRaw(missA, []byte("x"))
	row := coldRow(t, m, missA)
	m.manifest = &fakeManifest{hot: hot, entries: map[string]int64{missA: row.SizeBytes}}

	syncListing(t, m, cold, otherCold)

	if got := coldRow(t, m, missA); got == nil || got.Tier != TierHot {
		t.Fatalf("missA row = %+v, want it pointed back at hot", got)
	}
	if coldRow(t, m, missB) != nil {
		t.Fatal("missB not retired")
	}
}

// A hot copy the manifest does not list is not reverted, and not retired.
func TestReaderKeepsColdRowWithAnUnlistedHotCopy(t *testing.T) {
	m, hot, cold := readerRig(t)
	hot.seedRaw(missA, []byte("x"))
	m.manifest = &fakeManifest{hot: hot, entries: map[string]int64{}}
	syncListing(t, m, cold, otherCold)
	if got := coldRow(t, m, missA); got == nil || got.Tier != TierCold {
		t.Fatalf("missA row = %+v, want it kept cold", got)
	}
}

// failingHot fails every existence check.
type failingHot struct{ *mockBackend }

func (failingHot) Exists(context.Context, string) (bool, error) {
	return false, errors.New("hot stat failed")
}

func TestReaderKeepsRowWhenAStatFails(t *testing.T) {
	m, hot, cold := readerRig(t)
	m.hotBackend = failingHot{hot}
	syncListing(t, m, cold, otherCold)
	if coldRow(t, m, missA) == nil || coldRow(t, m, missB) == nil {
		t.Fatal("a row was retired although its hot copy could not be checked")
	}
}

// A writer that records the row again between the check and the delete wrote
// the object first: the stat after the delete finds it, and the row is
// written back.
func TestReaderWritesBackARowReRecordedDuringTheRetire(t *testing.T) {
	m, _, cold := readerRig(t)
	m.beforeReaderColdRetire = func(path string) {
		if path != missA {
			return
		}
		cold.seedRaw(missA, []byte("x"))
		if _, err := m.metadata.RecordColdFilesBatch(context.Background(), []FileMetadata{{
			Path: missA, Database: "db1", Measurement: "cpu", Tier: TierCold, SizeBytes: 1,
		}}, time.Time{}); err != nil {
			t.Error(err)
		}
	}
	syncListing(t, m, cold, otherCold)
	if row := coldRow(t, m, missA); row == nil || row.Tier != TierCold {
		t.Fatalf("missA row = %+v, want it written back", row)
	}
	if coldRow(t, m, missB) != nil {
		t.Fatal("missB not retired")
	}
}

// coldIdentityManifest is a manifest whose cluster cold mark reports a store.
type coldIdentityManifest struct {
	*fakeManifest
	backend          string
	seeded, conflict bool
}

func (c coldIdentityManifest) ColdMarkBackend() (string, bool, bool) {
	return c.backend, c.seeded, c.conflict
}

func TestReaderRetiresNothingUnderAGuard(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(m *Manager, hot *mockBackend)
		list  []string
	}{
		{"restore running", func(m *Manager, _ *mockBackend) { m.coldAuth.restoring.Add(1) }, []string{otherCold}},
		{"seeded from another store", func(m *Manager, hot *mockBackend) {
			m.manifest = coldIdentityManifest{fakeManifest: &fakeManifest{hot: hot}, backend: "s3://elsewhere/b/p", seeded: true}
		}, []string{otherCold}},
		{"seed conflict", func(m *Manager, hot *mockBackend) {
			m.manifest = coldIdentityManifest{fakeManifest: &fakeManifest{hot: hot}, conflict: true}
		}, []string{otherCold}},
		{"empty listing, identity unknown", func(*Manager, *mockBackend) {}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, hot, cold := readerRig(t)
			tc.setup(m, hot)
			syncListing(t, m, cold, tc.list...)
			if coldRow(t, m, missA) == nil || coldRow(t, m, missB) == nil {
				t.Fatal("rows retired despite the guard")
			}
		})
	}
}

// A marking during the sync (the generation moved) retires nothing.
func TestReaderRetireAllowedRefusesAMovedGeneration(t *testing.T) {
	m, _, _ := readerRig(t)
	gen := m.coldAuth.gen.Load()
	m.coldAuth.gen.Add(1)
	if ok, _ := m.readerRetireAllowed(gen, 1); ok {
		t.Fatal("allowed after the generation moved")
	}
	if ok, why := m.readerRetireAllowed(gen+1, 1); !ok {
		t.Fatalf("refused with an unchanged generation: %s", why)
	}
}

// Seeded from this node's own store: allowed even with an empty listing (a
// database delete can empty the bucket).
func TestReaderRetiresFromAnEmptyListingOfTheSeededStore(t *testing.T) {
	m, hot, cold := readerRig(t)
	m.config.Cold.Backend, m.config.Cold.S3Bucket = "s3", "bucket"
	m.manifest = coldIdentityManifest{fakeManifest: &fakeManifest{hot: hot}, backend: m.ColdBackendIdentity(), seeded: true}
	delete(cold.files, otherCold)
	syncListing(t, m, cold)
	if coldRow(t, m, missA) != nil || coldRow(t, m, missB) != nil {
		t.Fatal("rows not retired")
	}
}

// Retirement is bounded by time, not by #1551's 1000-candidate cap: a day of
// retention removes more than that.
func TestReaderRetiresMoreThanAThousandRowsInOneSync(t *testing.T) {
	m, _, cold := readerRig(t)
	const n = 1200
	paths := make([]string, n)
	for i := range paths {
		paths[i] = fmt.Sprintf("db1/disk/2024/03/15/disk_%04d_daily.parquet", i)
	}
	m.clusterGate = &mockGate{role: "reader"}
	m.clusterGate.(*mockGate).primary.Store(true) // record the rows as a primary would
	syncListing(t, m, cold, append(paths, otherCold)...)
	m.clusterGate.(*mockGate).primary.Store(false)

	syncListing(t, m, cold, otherCold)
	rows, err := m.metadata.ColdFilePathsAndSizes(context.Background(), TierCold)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		if _, ok := rows[p]; ok {
			t.Fatalf("%s not retired; %d cold rows left", p, len(rows))
		}
	}
}

// A quarantined row is never retired (#758).
func TestRetireColdRowIfUnquarantinedSkipsQuarantined(t *testing.T) {
	m, _, _ := missingRig(t)
	ctx := context.Background()
	if err := m.metadata.QuarantineFile(ctx, missA, "test"); err != nil {
		t.Fatal(err)
	}
	if ok, err := m.metadata.RetireColdRowIfUnquarantined(ctx, missA); err != nil || ok {
		t.Fatalf("retired a quarantined row: %v %v", ok, err)
	}
	if ok, err := m.metadata.RetireColdRowIfUnquarantined(ctx, missB); err != nil || !ok {
		t.Fatalf("did not retire an ordinary cold row: %v %v", ok, err)
	}
}

var _ storage.Backend = failingHot{}
