package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	iceberg "github.com/apache/iceberg-go"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// #939 B2: files migrated to the cold tier stay in the Iceberg table. Two local backends stand in
// for the hot and the cold tier; the logic under test does not depend on the backend type.

// countingSource counts footer reads.
type countingSource struct {
	*StorageWalkSource
	footers atomic.Int32
}

func (c *countingSource) FileSchema(ctx context.Context, f FileRef) (ArcSchema, error) {
	c.footers.Add(1)
	return c.StorageWalkSource.FileSchema(ctx, f)
}

type tierRig struct {
	t           *testing.T
	hotRoot     string
	coldRoot    string
	hot, cold   storage.Backend
	src         *countingSource
	exp         *Exporter
	sched       *Scheduler
	coldKeys    map[string]bool // the tier record: keys migrated to cold
	coldStatErr error
}

type coldFaults struct {
	storage.Backend
	r *tierRig
}

func (c coldFaults) StatFile(ctx context.Context, p string) (int64, error) {
	if c.r.coldStatErr != nil {
		return 0, c.r.coldStatErr
	}
	return c.Backend.StatFile(ctx, p)
}

func (c coldFaults) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	return c.Backend.(storage.ObjectLister).ListObjects(ctx, prefix)
}

func (c coldFaults) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	return c.Backend.(dirLister).ListDirectories(ctx, prefix)
}

// newTierRig builds the rig. With faulty, the cold backend goes through coldFaults; its URIs then
// do not resolve (the resolver keys on the concrete backend type), so only a test that never
// registers a cold URI uses it.
func newTierRig(t *testing.T, rescan time.Duration, faulty ...bool) *tierRig {
	t.Helper()
	r := &tierRig{t: t, hotRoot: t.TempDir(), coldRoot: t.TempDir(), coldKeys: map[string]bool{}}
	var err error
	if r.hot, err = storage.NewLocalBackend(r.hotRoot, zerolog.Nop()); err != nil {
		t.Fatal(err)
	}
	coldLocal, err := storage.NewLocalBackend(r.coldRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	r.cold = coldLocal
	if len(faulty) > 0 && faulty[0] {
		r.cold = coldFaults{Backend: coldLocal, r: r}
	}
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if r.exp, err = NewExporter(db, r.hot, DefaultWarehouse(r.hot), "arc", 0, zerolog.Nop()); err != nil {
		t.Fatal(err)
	}
	r.src = &countingSource{StorageWalkSource: NewStorageWalkSource(r.hot, "arc", zerolog.Nop())}
	if err := r.src.SetColdTier(r.cold, func(_ context.Context, key string) (bool, error) { return r.coldKeys[key], nil }, rescan); err != nil {
		t.Fatal(err)
	}
	r.sched = NewScheduler(SchedulerConfig{Exporter: r.exp, Source: r.src, Logger: zerolog.Nop()})
	return r
}

func (r *tierRig) key(i int) string { return fmt.Sprintf("mydb/cpu/2023/11/14/22/f%d.parquet", i) }

func (r *tierRig) writeHot(i int) {
	r.t.Helper()
	writeArcStyleParquet(r.t, filepath.Join(r.hotRoot, r.key(i)), 1_700_000_000_000_000+int64(i)*1_000_000, 10)
}

// migrate moves file i to the cold tier as tiering does: copy (same key), flip the tier record,
// delete the hot copy. garbage writes same-size bytes that are not Parquet instead of the real
// copy: a pass that reads the cold object then fails.
func (r *tierRig) migrate(i int, garbage, deleteHot bool) {
	r.t.Helper()
	data, err := os.ReadFile(filepath.Join(r.hotRoot, r.key(i)))
	if err != nil {
		r.t.Fatal(err)
	}
	if garbage {
		data = []byte(strings.Repeat("x", len(data)))
	}
	dst := filepath.Join(r.coldRoot, r.key(i))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		r.t.Fatal(err)
	}
	r.coldKeys[r.key(i)] = true
	if deleteHot {
		if err := os.Remove(filepath.Join(r.hotRoot, r.key(i))); err != nil {
			r.t.Fatal(err)
		}
	}
}

func (r *tierRig) pass() {
	r.t.Helper()
	r.sched.runPass(context.Background())
}

func (r *tierRig) liveURIs() map[string]int64 {
	r.t.Helper()
	ctx := context.Background()
	tbl, err := r.exp.catalog.LoadTable(ctx, r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		r.t.Fatal(err)
	}
	files, err := r.exp.tableDataFiles(ctx, tbl)
	if err != nil {
		r.t.Fatal(err)
	}
	return files
}

func (r *tierRig) liveDataFiles() map[string]iceberg.DataFile {
	r.t.Helper()
	ctx := context.Background()
	tbl, err := r.exp.catalog.LoadTable(ctx, r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		r.t.Fatal(err)
	}
	live, err := r.exp.tableLiveFiles(ctx, tbl)
	if err != nil {
		r.t.Fatal(err)
	}
	return live
}

func (r *tierRig) onCold(uris map[string]int64) (cold, hot int) {
	for u := range uris {
		if strings.Contains(u, r.coldRoot) {
			cold++
		} else {
			hot++
		}
	}
	return cold, hot
}

// A migrated file moves to its cold URI with the same stats, and neither the exporter nor the
// schema derivation reads the cold object (its bytes here are garbage of the right size).
func TestCold_MigrationMovesTheURIAndReadsNothingCold(t *testing.T) {
	r := newTierRig(t, time.Hour)
	r.writeHot(1)
	r.writeHot(2)
	r.pass()
	before := r.liveDataFiles()
	footers := r.src.footers.Load()

	r.migrate(1, true, true)
	r.pass()
	live := r.liveURIs()
	if cold, hot := r.onCold(live); cold != 1 || hot != 1 {
		t.Fatalf("live = %v: want f1 on cold, f2 on hot", live)
	}
	if got := r.src.footers.Load(); got != footers {
		t.Fatalf("the migration read %d footers, want none", got-footers)
	}
	after := r.liveDataFiles()
	var hotDF, coldDF iceberg.DataFile
	for p, df := range before {
		if strings.HasSuffix(p, "/"+r.key(1)) {
			hotDF = df
		}
	}
	for p, df := range after {
		if strings.HasSuffix(p, "/"+r.key(1)) {
			coldDF = df
		}
	}
	if hotDF == nil || coldDF == nil {
		t.Fatalf("missing entries: hot %v cold %v", hotDF != nil, coldDF != nil)
	}
	if d := dataFileDiff(coldDF, hotDF); d != "" {
		t.Fatalf("cold entry differs from the hot one: %s", d)
	}
}

// On both tiers (between a migration's copy and its hot delete, or a hot-only database): hot,
// unless the tier record says cold.
func TestCold_BothTiersFollowTheTierRecord(t *testing.T) {
	r := newTierRig(t, 0)
	r.writeHot(1)
	r.pass()
	r.migrate(1, false, false)
	r.coldKeys[r.key(1)] = false // copied, not flipped yet
	r.pass()
	if cold, hot := r.onCold(r.liveURIs()); cold != 0 || hot != 1 {
		t.Fatal("copied but not flipped: want the hot URI")
	}
	r.coldKeys[r.key(1)] = true
	r.pass()
	if cold, hot := r.onCold(r.liveURIs()); cold != 1 || hot != 0 {
		t.Fatal("flipped: want the cold URI")
	}
}

// A file that leaves the hot tier after the cold listing was cached is looked for on cold before
// the table drops it: the table never goes without it.
func TestCold_HotRemovalLooksOnColdFirst(t *testing.T) {
	r := newTierRig(t, time.Hour) // the cold listing stays cached all test long
	r.writeHot(1)
	r.writeHot(2)
	r.pass() // caches an empty cold listing
	r.migrate(1, false, true)
	r.pass()
	live := r.liveURIs()
	if len(live) != 2 {
		t.Fatalf("live = %v: the migrated file left the table", live)
	}
	if cold, _ := r.onCold(live); cold != 1 {
		t.Fatalf("live = %v: want f1 under its cold URI", live)
	}
	// A real removal (retention on hot, nothing on cold) still leaves the table.
	if err := os.Remove(filepath.Join(r.hotRoot, r.key(2))); err != nil {
		t.Fatal(err)
	}
	r.pass()
	if live := r.liveURIs(); len(live) != 1 {
		t.Fatalf("live = %v after removing f2 from hot, want f1 only", live)
	}
}

// A cold read error fails the measurement instead of dropping files.
func TestCold_ColdErrorFailsThePass(t *testing.T) {
	r := newTierRig(t, time.Hour, true)
	r.writeHot(1)
	r.writeHot(2)
	r.pass()
	if err := os.Remove(filepath.Join(r.hotRoot, r.key(1))); err != nil {
		t.Fatal(err)
	}
	r.coldStatErr = errors.New("throttled")
	if _, _, err := r.src.FilesAndLocal(context.Background(), Measurement{Database: "mydb", Measurement: "cpu"}); err == nil {
		t.Fatal("a failing cold HEAD must fail the measurement")
	}
	r.pass()
	if live := r.liveURIs(); len(live) != 2 {
		t.Fatalf("live = %v: a failed cold read dropped a file", live)
	}
}

// A measurement whose every file migrated (gone from the primary entirely, as on an object
// store) is still enumerated and reconciled.
func TestCold_ColdOnlyMeasurementIsStillExported(t *testing.T) {
	r := newTierRig(t, time.Hour)
	r.writeHot(1)
	r.pass()
	r.migrate(1, false, true)
	if err := os.RemoveAll(filepath.Join(r.hotRoot, "mydb")); err != nil {
		t.Fatal(err)
	}
	ms, err := r.src.Measurements(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 || ms[0].Measurement != "cpu" {
		t.Fatalf("measurements = %v, want mydb/cpu from the cold tier", ms)
	}
	r.pass()
	if cold, hot := r.onCold(r.liveURIs()); cold != 1 || hot != 0 {
		t.Fatal("want the file under its cold URI")
	}
}

// InvalidateCold makes the next pass list cold again: a cold-only change (cold retention) leaves
// the table without waiting for the rescan interval.
func TestCold_InvalidationSeesColdOnlyChanges(t *testing.T) {
	r := newTierRig(t, time.Hour)
	r.writeHot(1)
	r.writeHot(2)
	r.pass()
	r.migrate(1, false, true)
	r.pass()
	if err := os.Remove(filepath.Join(r.coldRoot, r.key(1))); err != nil { // cold retention
		t.Fatal(err)
	}
	r.pass()
	if live := r.liveURIs(); len(live) != 2 {
		t.Fatalf("before invalidation the cached listing still names f1: live = %v", live)
	}
	r.src.InvalidateCold("mydb")
	r.pass()
	if live := r.liveURIs(); len(live) != 1 {
		t.Fatalf("after invalidation: live = %v, want f2 only", live)
	}
}

// A cold file Arc's reads leave out (a DELETE replaced it) is left out of the table too.
func TestCold_ExclusionsApplyToColdFiles(t *testing.T) {
	r := newTierRig(t, 0)
	r.writeHot(1)
	r.writeHot(2)
	r.pass()
	r.migrate(1, false, true)
	ex := &fakeExclusions{}
	r.src.SetExclusions(ex)
	ex.set(r.key(1))
	r.pass()
	live := r.liveURIs()
	if len(live) != 1 {
		t.Fatalf("live = %v: the excluded cold file was exported", live)
	}
	for u := range live {
		if strings.HasSuffix(u, "/"+r.key(1)) {
			t.Fatalf("live = %v: the excluded cold file was exported", live)
		}
	}
}
