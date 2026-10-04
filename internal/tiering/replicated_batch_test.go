package tiering

// Tests for how the tier-event drainer behaves under the load a real cluster
// puts on it: steady-state replication, a nightly migration chunk, and the
// unlink side outrunning the probe side.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// hotRowFor seeds a hot row for path under the database/measurement the
// drainer itself would derive from the path, so the tier-set bookkeeping in
// the tests below sees the same key the drainer does.
func hotRowFor(t *testing.T, m *Manager, path string) {
	t.Helper()
	info, ok := m.tierEventFileInfo(tierEvent{kind: tierEventPulled, path: path, sizeBytes: 16})
	if !ok {
		t.Fatalf("path %q did not parse", path)
	}
	if _, err := m.metadata.recordHotFileIfNotCold(context.Background(), info); err != nil {
		t.Fatalf("seed hot row: %v", err)
	}
}

// On a replicating cluster every pulled file is a new path, so every drainer
// batch writes a row. If each written row dropped the query layer's caches,
// a node pulling its peers' flushes at the shipped 100 ms buffer age would
// never keep a pruned path or a cached SQL transform for longer than one
// batch — and would log two Info lines per batch. Only a change to the SET
// of tiers a measurement has rows in makes those caches stale.
func TestApplyTierEventBatch_NotifiesOnlyWhenATierSetChanges(t *testing.T) {
	cold := newMockBackend("s3")
	m := newTierEventManager(t, cold, true)

	var notified atomic.Int64
	m.SetOnMigrationComplete(func() { notified.Add(1) })

	pulled := func(hour string) tierEvent {
		return tierEvent{kind: tierEventPulled, path: "db1/cpu/2026/10/03/" + hour + "/a.parquet", sizeBytes: 16}
	}

	// First hot row for the measurement: {} -> {hot}. A transition.
	m.applyTierEventBatch([]tierEvent{pulled("10")})
	if got := notified.Load(); got != 1 {
		t.Fatalf("first hot row: notified %d times, want 1", got)
	}

	// Steady state: more pulled files for a measurement that is already hot.
	m.applyTierEventBatch([]tierEvent{pulled("11"), pulled("12"), pulled("13")})
	if got := notified.Load(); got != 1 {
		t.Fatalf("steady-state pulls: notified %d times in total, want still 1 — the tier set is unchanged, and this fires once per batch on a replicating node", got)
	}

	// A compaction unlink that leaves other hot rows: still {hot}.
	m.applyTierEventBatch([]tierEvent{{
		kind: tierEventUnlinked, path: "db1/cpu/2026/10/03/11/a.parquet", reason: "compaction:job-1", sizeBytes: 16,
	}})
	if got := notified.Load(); got != 1 {
		t.Fatalf("compaction unlink with hot rows remaining: notified %d times in total, want still 1", got)
	}

	// A migration unlink whose object is in cold: {hot} -> {hot, cold}.
	cold.seedRaw("db1/cpu/2026/10/03/12/a.parquet", []byte("cold copy"))
	m.applyTierEventBatch([]tierEvent{{
		kind: tierEventUnlinked, path: "db1/cpu/2026/10/03/12/a.parquet", reason: "tiering:migrated", sizeBytes: 16,
	}})
	if got := notified.Load(); got != 2 {
		t.Fatalf("first cold row: notified %d times in total, want 2", got)
	}

	// The remaining hot rows retired one by one: {hot, cold} -> {cold} on the last.
	m.applyTierEventBatch([]tierEvent{{
		kind: tierEventUnlinked, path: "db1/cpu/2026/10/03/10/a.parquet", reason: "retention:p1", sizeBytes: 16,
	}})
	if got := notified.Load(); got != 2 {
		t.Fatalf("retire with a hot row remaining: notified %d times in total, want still 2", got)
	}
	m.applyTierEventBatch([]tierEvent{{
		kind: tierEventUnlinked, path: "db1/cpu/2026/10/03/13/a.parquet", reason: "retention:p1", sizeBytes: 16,
	}})
	if got := notified.Load(); got != 3 {
		t.Fatalf("last hot row retired: notified %d times in total, want 3", got)
	}
}

// slowExistsBackend is a cold tier whose existence check costs a round trip,
// as an S3 HEAD does.
type slowExistsBackend struct {
	*mockBackend
	delay       time.Duration
	calls       atomic.Int64
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
}

func (b *slowExistsBackend) Exists(ctx context.Context, path string) (bool, error) {
	b.calls.Add(1)
	n := b.inFlight.Add(1)
	defer b.inFlight.Add(-1)
	for {
		cur := b.maxInFlight.Load()
		if n <= cur || b.maxInFlight.CompareAndSwap(cur, n) {
			break
		}
	}
	select {
	case <-time.After(b.delay):
	case <-ctx.Done():
		return false, ctx.Err()
	}
	return b.mockBackend.Exists(ctx, path)
}

// A nightly migration reaches a non-primary node as hundreds to thousands of
// tiering:migrated unlinks in quick succession, and each one is worth one
// cold-tier existence check. Done one at a time under a single batch
// deadline, the check latency alone decides how many of them land: the rest
// are counted failed, and a measurement whose unlinks all fall in that tail
// has its hot files gone and no cold row — invisible on this node until the
// next cold sync. The drainer has to apply a chunk whose probes, run
// sequentially, would take several times its deadline.
func TestApplyTierEventBatch_AppliesAMigrationChunkTheProbeLatencyWouldTimeOut(t *testing.T) {
	// Sequentially the probes alone take n*probe = 8 s against a 2 s chunk
	// deadline; in parallel a chunk's probes take ~0.3 s. The gap on each
	// side is deliberately wide: the SQL writes under -race on a small CI
	// runner cost real time too, and the test must fail only for the
	// mechanism it is about.
	const n = 400
	const probe = 20 * time.Millisecond

	restore := tierEventDrainTimeout
	tierEventDrainTimeout = 2 * time.Second
	t.Cleanup(func() { tierEventDrainTimeout = restore })

	cold := &slowExistsBackend{mockBackend: newMockBackend("s3"), delay: probe}
	m := newTierEventManager(t, nil, true)
	m.coldBackend = cold

	// 20 measurements x 20 files, in measurement order — the order a
	// migration proposes its deletes in.
	paths := make([]string, 0, n)
	batch := make([]tierEvent, 0, n)
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("db1/m%02d/2026/10/03/%02d/f%03d.parquet", i/20, i%20, i)
		paths = append(paths, p)
		cold.seedRaw(p, []byte("cold copy"))
		hotRowFor(t, m, p)
		batch = append(batch, tierEvent{kind: tierEventUnlinked, path: p, reason: "tiering:migrated", sizeBytes: 16})
	}

	m.applyTierEventBatch(batch)

	applied, dropped, failed := m.TierEventStats()
	if applied != n || failed != 0 || dropped != 0 {
		t.Fatalf("applied %d failed %d dropped %d of %d migration unlinks, want all applied", applied, failed, dropped, n)
	}
	for _, p := range paths {
		tier, ok := rowTier(t, m, p)
		if !ok || tier != string(TierCold) {
			t.Fatalf("%s: tier=%q present=%v after a migration unlink with the object in cold, want cold", p, tier, ok)
		}
	}
	if got := cold.calls.Load(); got != n {
		t.Fatalf("cold probed %d times for %d paths, want exactly one probe per path", got, n)
	}
	if got := cold.maxInFlight.Load(); got < 2 {
		t.Fatalf("probes never overlapped (max in flight %d); the chunk only fits its deadline because they run in parallel", got)
	}
}

// A migration chunk has the two delete workers unlinking at disk speed while
// each unlink costs the drainer a cold-tier round trip. The queue absorbs the
// difference: a report never blocks the worker and is never dropped while the
// drainer is alive, however far behind it is.
func TestRecordUnlinkedFile_NeverBlocksAndNeverDropsUnderABurst(t *testing.T) {
	m := &Manager{
		logger:        zerolog.Nop(),
		tierEvents:    newTierEventQueue(),
		tierEventStop: make(chan struct{}),
	}
	// No drainer at all: the worst case for a queue, and the burst must still
	// land in full without holding the caller.
	const n = 10000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			m.RecordUnlinkedFile(fmt.Sprintf("db1/cpu/2026/10/03/%02d/f%05d.parquet", i%24, i), "tiering:migrated", 1)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("unlink reports blocked the delete worker")
	}
	if _, dropped, _ := m.TierEventStats(); dropped != 0 {
		t.Fatalf("dropped = %d, want 0", dropped)
	}
	if got := m.tierEvents.pending(); got != n {
		t.Fatalf("pending = %d, want %d", got, n)
	}
}

// The puller removes a copy it had just finished pulling when the path left
// the manifest in transit, and reports it under a reason of its own because it
// never learns the manifest delete's. That reason is treated like a tiering
// one — the cold tier decides — so a file the primary migrated out from under
// an in-flight pull still ends up with a cold row here, and one that was simply
// deleted ends up with no row.
func TestRecordUnlinkedFile_AbandonedPullIsDecidedByTheColdTier(t *testing.T) {
	cold := newMockBackend("s3")
	const migrated = "db1/cpu/2026/10/03/a_daily.parquet"
	const deleted = "db1/cpu/2026/10/03/14/b.parquet"
	cold.seedRaw(migrated, []byte("cold copy"))

	m := newTierEventManager(t, cold, true)
	hotRowFor(t, m, migrated) // an earlier generation this node held
	hotRowFor(t, m, deleted)
	m.RecordUnlinkedFile(migrated, unlinkReasonAbandonedPull, 9)
	m.RecordUnlinkedFile(deleted, unlinkReasonAbandonedPull, 9)
	waitTierEvents(t, m, 2)

	if tier, ok := rowTier(t, m, migrated); !ok || tier != string(TierCold) {
		t.Fatalf("migrated-mid-pull path: tier=%q present=%v, want cold", tier, ok)
	}
	if tier, ok := rowTier(t, m, deleted); ok {
		t.Fatalf("deleted-mid-pull path: row left at tier %q, want none", tier)
	}
}

// errStatBackend is a hot tier that cannot answer a stat right now.
type errStatBackend struct {
	*mockBackend
}

func (b *errStatBackend) StatFile(context.Context, string) (int64, error) {
	return -1, errors.New("stat: transient failure")
}

// The re-stat in retireVanishedHotRows exists to confirm a file is gone
// before its row goes. A stat that ERRORS has confirmed nothing, and must not
// fall through to the delete: a row kept one scan too long costs an empty
// glob, a row retired for a file still on disk costs the measurement its
// local reads once its other rows are cold.
func TestRetireKeepsARowWhenTheStatCannotAnswer(t *testing.T) {
	const path = "db1/cpu/2026/10/03/14/a.parquet"
	m := newTierEventManager(t, nil, false)
	ctx := context.Background()

	old := replicatedTestFile(path, 9)
	old.CreatedAt = time.Now().UTC().Add(-48 * time.Hour)
	if _, err := m.metadata.recordHotFileIfNotCold(ctx, old); err != nil {
		t.Fatalf("seed: %v", err)
	}
	m.hotBackend = &errStatBackend{newMockBackend("local")}

	result := &ScanResult{}
	retired := m.retireVanishedHotRows(ctx, nil /* empty listing */, time.Now().UTC(), result)

	if _, ok := rowTier(t, m, path); !ok {
		t.Fatalf("row retired (retired=%d) on a stat that returned an error, not a not-found", retired)
	}
	if result.Errors == 0 {
		t.Fatal("a stat failure during retirement was not counted as an error")
	}
}

// created_at is compared in SQL now (DeleteFileInTier's createdBefore), and
// go-sqlite3 stores a time.Time as text in the zone the value carries — so
// every writer has to bind UTC or the comparison is a string compare across
// zones. The scan hands in the file's mtime, which a local backend reports in
// the host zone.
func TestHotRowWritersBindCreatedAtInUTC(t *testing.T) {
	m := newTierEventManager(t, nil, false)
	ctx := context.Background()
	tokyo := time.FixedZone("UTC+9", 9*3600)
	stamp := time.Date(2026, 10, 3, 23, 0, 0, 0, tokyo) // 14:00Z

	cases := []struct {
		name  string
		path  string
		write func(f *FileMetadata) error
	}{
		{"recordHotFileIfNotCold", "db1/cpu/2026/10/03/14/a.parquet", func(f *FileMetadata) error {
			_, err := m.metadata.recordHotFileIfNotCold(ctx, f)
			return err
		}},
		{"markFileCold", "db1/cpu/2026/10/03/14/b.parquet", func(f *FileMetadata) error {
			_, err := m.metadata.markFileCold(ctx, f)
			return err
		}},
		{"RecordFile", "db1/cpu/2026/10/03/14/c.parquet", func(f *FileMetadata) error {
			f.Tier = TierHot
			return m.metadata.RecordFile(ctx, f)
		}},
	}
	for _, tc := range cases {
		f := replicatedTestFile(tc.path, 9)
		f.CreatedAt = stamp
		if err := tc.write(f); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		var raw string
		if err := m.metadata.db.QueryRow(`SELECT CAST(created_at AS TEXT) FROM tier_files WHERE path = ?`, tc.path).Scan(&raw); err != nil {
			t.Fatalf("%s: read created_at: %v", tc.name, err)
		}
		if len(raw) < 6 || raw[len(raw)-6:] != "+00:00" {
			t.Errorf("%s stored created_at as %q; want a UTC-bound value so the SQL-side created_at comparison is one domain", tc.name, raw)
		}
		if got := raw[:19]; got != "2026-10-03 14:00:00" {
			t.Errorf("%s stored created_at as %q; want the instant 2026-10-03 14:00:00 UTC", tc.name, raw)
		}
	}
}
