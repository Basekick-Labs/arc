package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	stdsync "sync"
	"testing"
	"time"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

func setupTestLedger(t *testing.T) *Ledger {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "sync_ledger_test_*.db")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	tmpFile.Close()

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		os.Remove(tmpFile.Name())
		t.Fatalf("open sqlite: %v", err)
	}

	// Bulk-insert tests write hundreds of rows; without this each insert
	// fsyncs in its own implicit transaction and the suite crawls.
	if _, err := db.Exec("PRAGMA synchronous = OFF"); err != nil {
		t.Fatalf("set synchronous: %v", err)
	}

	l, err := NewLedger(db, zerolog.Nop())
	if err != nil {
		db.Close()
		os.Remove(tmpFile.Name())
		t.Fatalf("new ledger: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
		os.Remove(tmpFile.Name())
	})

	return l
}

func testEntry(path string) *LedgerEntry {
	return &LedgerEntry{
		HubID:         DefaultHubID,
		Path:          path,
		SHA256:        "abc123",
		SizeBytes:     1024,
		Database:      "default",
		Measurement:   "cpu",
		PartitionTime: time.Date(2026, 8, 6, 14, 0, 0, 0, time.UTC),
	}
}

func TestLedger_TrackIsIdempotent(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	e := testEntry("db/cpu/2026/08/06/14/a.parquet")
	if err := l.Track(ctx, e); err != nil {
		t.Fatalf("first track: %v", err)
	}

	// Discovery re-walks the manifest every tick. Re-tracking must not reset
	// state, or a synced file would be re-sent forever.
	if err := l.MarkSynced(ctx, DefaultHubID, e.Path); err != nil {
		t.Fatalf("mark synced: %v", err)
	}
	if err := l.Track(ctx, e); err != nil {
		t.Fatalf("re-track: %v", err)
	}

	got, err := l.Get(ctx, DefaultHubID, e.Path)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != StateSynced {
		t.Errorf("state = %q after re-track, want %q — re-discovery reset a synced entry",
			got.State, StateSynced)
	}

	stats, err := l.Stats(ctx, DefaultHubID)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Synced != 1 || stats.Pending != 0 {
		t.Errorf("synced=%d pending=%d, want 1/0 — re-track created a duplicate row",
			stats.Synced, stats.Pending)
	}
}

func TestLedger_TrackBatchCountsOnlyNewEntries(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	entries := []*LedgerEntry{
		testEntry("db/cpu/2026/08/06/14/a.parquet"),
		testEntry("db/cpu/2026/08/06/14/b.parquet"),
	}
	n, err := l.TrackBatch(ctx, entries)
	if err != nil {
		t.Fatalf("track batch: %v", err)
	}
	if n != 2 {
		t.Errorf("inserted = %d, want 2", n)
	}

	// Re-running with one overlap must report only the genuinely new row.
	entries = append(entries, testEntry("db/cpu/2026/08/06/14/c.parquet"))
	n, err = l.TrackBatch(ctx, entries)
	if err != nil {
		t.Fatalf("second track batch: %v", err)
	}
	if n != 1 {
		t.Errorf("inserted = %d on re-run, want 1 (only the new path)", n)
	}
}

func TestLedger_PendingIsNewestFirst(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	// Newest-first ordering is a design guarantee: when a contact window
	// closes mid-backlog the freshest telemetry must already have landed.
	times := []time.Time{
		time.Date(2026, 8, 6, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 6, 14, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC),
	}
	for i, pt := range times {
		e := testEntry(fmt.Sprintf("db/cpu/f%d.parquet", i))
		e.PartitionTime = pt
		if err := l.Track(ctx, e); err != nil {
			t.Fatalf("track %d: %v", i, err)
		}
	}

	pending, err := l.Pending(ctx, DefaultHubID, 0)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("got %d pending, want 3", len(pending))
	}

	for i := 1; i < len(pending); i++ {
		if pending[i].PartitionTime.After(pending[i-1].PartitionTime) {
			t.Errorf("entry %d (%s) is newer than %d (%s) — not newest-first",
				i, pending[i].PartitionTime, i-1, pending[i-1].PartitionTime)
		}
	}
	if !pending[0].PartitionTime.Equal(times[1]) {
		t.Errorf("first entry partition = %s, want the newest %s",
			pending[0].PartitionTime, times[1])
	}
}

func TestLedger_RecoverInFlight(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	paths := []string{"db/cpu/a.parquet", "db/cpu/b.parquet", "db/cpu/c.parquet"}
	for _, p := range paths {
		if err := l.Track(ctx, testEntry(p)); err != nil {
			t.Fatalf("track %s: %v", p, err)
		}
	}

	// Two transfers were running when the process died; one had completed.
	if err := l.MarkInFlight(ctx, DefaultHubID, paths[0]); err != nil {
		t.Fatalf("mark in-flight: %v", err)
	}
	if err := l.MarkInFlight(ctx, DefaultHubID, paths[1]); err != nil {
		t.Fatalf("mark in-flight: %v", err)
	}
	if err := l.MarkSynced(ctx, DefaultHubID, paths[2]); err != nil {
		t.Fatalf("mark synced: %v", err)
	}

	n, err := l.RecoverInFlight(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if n != 2 {
		t.Errorf("recovered = %d, want 2", n)
	}

	stats, err := l.Stats(ctx, DefaultHubID)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.InFlight != 0 {
		t.Errorf("in_flight = %d after recovery, want 0 — a dead transfer stayed in_flight forever",
			stats.InFlight)
	}
	if stats.Pending != 2 {
		t.Errorf("pending = %d, want 2", stats.Pending)
	}
	if stats.Synced != 1 {
		t.Errorf("synced = %d, want 1 — recovery must not touch synced entries", stats.Synced)
	}
}

func TestLedger_ResumeCheckpointSurvivesFailure(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	e := testEntry("db/cpu/big.parquet")
	e.SizeBytes = 500 << 20 // 500 MB
	if err := l.Track(ctx, e); err != nil {
		t.Fatalf("track: %v", err)
	}

	if err := l.MarkInFlight(ctx, DefaultHubID, e.Path); err != nil {
		t.Fatalf("mark in-flight: %v", err)
	}
	const sent = 200 << 20
	if err := l.RecordProgress(ctx, DefaultHubID, e.Path, sent); err != nil {
		t.Fatalf("record progress: %v", err)
	}

	// The link drops. The bytes the hub accepted are still valid — discarding
	// the checkpoint would restart a 500MB file from zero on exactly the link
	// least able to afford it.
	if err := l.MarkFailed(ctx, DefaultHubID, e.Path, "connection reset", 3); err != nil {
		t.Fatalf("mark failed: %v", err)
	}

	got, err := l.Get(ctx, DefaultHubID, e.Path)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.BytesSent != sent {
		t.Errorf("bytes_sent = %d after failure, want %d — resume checkpoint lost",
			got.BytesSent, sent)
	}
	if got.State != StatePending {
		t.Errorf("state = %q, want %q (retries remain)", got.State, StatePending)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", got.Attempts)
	}
}

func TestLedger_MarkFailedTerminalAtMaxAttempts(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	e := testEntry("db/cpu/doomed.parquet")
	if err := l.Track(ctx, e); err != nil {
		t.Fatalf("track: %v", err)
	}

	const maxAttempts = 3
	for i := 1; i <= maxAttempts; i++ {
		if err := l.MarkInFlight(ctx, DefaultHubID, e.Path); err != nil {
			t.Fatalf("attempt %d in-flight: %v", i, err)
		}
		if err := l.MarkFailed(ctx, DefaultHubID, e.Path, "nope", maxAttempts); err != nil {
			t.Fatalf("attempt %d failed: %v", i, err)
		}

		got, err := l.Get(ctx, DefaultHubID, e.Path)
		if err != nil {
			t.Fatalf("get after attempt %d: %v", i, err)
		}
		want := StatePending
		if i >= maxAttempts {
			want = StateFailed
		}
		if got.State != want {
			t.Errorf("after attempt %d/%d: state = %q, want %q", i, maxAttempts, got.State, want)
		}
	}
}

func TestLedger_MarkSyncedClearsErrorAndCompletesBytes(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	e := testEntry("db/cpu/a.parquet")
	if err := l.Track(ctx, e); err != nil {
		t.Fatalf("track: %v", err)
	}
	if err := l.MarkInFlight(ctx, DefaultHubID, e.Path); err != nil {
		t.Fatalf("in-flight: %v", err)
	}
	if err := l.MarkFailed(ctx, DefaultHubID, e.Path, "transient blip", 5); err != nil {
		t.Fatalf("failed: %v", err)
	}
	if err := l.MarkSynced(ctx, DefaultHubID, e.Path); err != nil {
		t.Fatalf("synced: %v", err)
	}

	got, err := l.Get(ctx, DefaultHubID, e.Path)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.LastError != "" {
		t.Errorf("last_error = %q after sync, want empty — a stale error would mislead operators",
			got.LastError)
	}
	if got.BytesSent != e.SizeBytes {
		t.Errorf("bytes_sent = %d, want %d (full size)", got.BytesSent, e.SizeBytes)
	}
	if got.SyncedAt == nil {
		t.Error("synced_at is nil after MarkSynced")
	}
}

func TestLedger_UnknownPathIsNotFound(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	// A state transition against an untracked path must fail loudly. A silent
	// no-op would let the agent believe it advanced an entry that doesn't exist.
	for name, fn := range map[string]func() error{
		"MarkInFlight":   func() error { return l.MarkInFlight(ctx, DefaultHubID, "nope.parquet") },
		"MarkSynced":     func() error { return l.MarkSynced(ctx, DefaultHubID, "nope.parquet") },
		"RecordProgress": func() error { return l.RecordProgress(ctx, DefaultHubID, "nope.parquet", 1) },
		"MarkFailed":     func() error { return l.MarkFailed(ctx, DefaultHubID, "nope.parquet", "x", 3) },
	} {
		if err := fn(); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s on unknown path: err = %v, want ErrNotFound", name, err)
		}
	}

	if _, err := l.Get(ctx, DefaultHubID, "nope.parquet"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get on unknown path: err = %v, want ErrNotFound", err)
	}
}

func TestLedger_HubIsolation(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	// The (hub_id, path) key exists so the same file can be tracked
	// independently per hub — multi-hub is config later, not a field migration.
	const path = "db/cpu/shared.parquet"
	for _, hub := range []string{"cloud", "factory"} {
		e := testEntry(path)
		e.HubID = hub
		if err := l.Track(ctx, e); err != nil {
			t.Fatalf("track for %s: %v", hub, err)
		}
	}

	if err := l.MarkSynced(ctx, "cloud", path); err != nil {
		t.Fatalf("mark synced on cloud: %v", err)
	}

	cloud, err := l.Stats(ctx, "cloud")
	if err != nil {
		t.Fatalf("cloud stats: %v", err)
	}
	factory, err := l.Stats(ctx, "factory")
	if err != nil {
		t.Fatalf("factory stats: %v", err)
	}

	if cloud.Synced != 1 {
		t.Errorf("cloud synced = %d, want 1", cloud.Synced)
	}
	if factory.Synced != 0 || factory.Pending != 1 {
		t.Errorf("factory synced=%d pending=%d, want 0/1 — hubs are not isolated",
			factory.Synced, factory.Pending)
	}
}

func TestLedger_StatsPendingBytesExcludesSentPrefix(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	e := testEntry("db/cpu/partial.parquet")
	e.SizeBytes = 1000
	if err := l.Track(ctx, e); err != nil {
		t.Fatalf("track: %v", err)
	}
	if err := l.MarkInFlight(ctx, DefaultHubID, e.Path); err != nil {
		t.Fatalf("in-flight: %v", err)
	}
	if err := l.RecordProgress(ctx, DefaultHubID, e.Path, 400); err != nil {
		t.Fatalf("progress: %v", err)
	}

	stats, err := l.Stats(ctx, DefaultHubID)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	// Operators use this to estimate how much link time a drain needs; counting
	// already-transferred bytes would overstate it.
	if stats.PendingBytes != 600 {
		t.Errorf("pending_bytes = %d, want 600 (1000 total - 400 sent)", stats.PendingBytes)
	}
}

func TestLedger_PruneSyncedBatchesAndSpares(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	// Created first so it holds id=1, below the prune's maxID bound — that is
	// what makes the DELETE's state filter load-bearing rather than incidental.
	if err := l.Track(ctx, testEntry("db/cpu/still_pending.parquet")); err != nil {
		t.Fatalf("track pending: %v", err)
	}

	// Just above the 1000-row batch limit, to exercise the loop's second pass.
	const total = 1050
	entries := make([]*LedgerEntry, 0, total)
	for i := 0; i < total; i++ {
		entries = append(entries, testEntry(fmt.Sprintf("db/cpu/old_%d.parquet", i)))
	}
	if _, err := l.TrackBatch(ctx, entries); err != nil {
		t.Fatalf("track batch: %v", err)
	}
	for i := 0; i < total; i++ {
		if err := l.MarkSynced(ctx, DefaultHubID, fmt.Sprintf("db/cpu/old_%d.parquet", i)); err != nil {
			t.Fatalf("mark synced %d: %v", i, err)
		}
	}

	// Backdate every synced row past the retention horizon.
	cutoff := time.Now().UTC().AddDate(0, 0, -30)
	if _, err := l.db.ExecContext(ctx,
		`UPDATE sync_ledger SET synced_at = ? WHERE state = ?`, cutoff, string(StateSynced)); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	if err := l.Track(ctx, testEntry("db/cpu/fresh.parquet")); err != nil {
		t.Fatalf("track fresh: %v", err)
	}
	if err := l.MarkSynced(ctx, DefaultHubID, "db/cpu/fresh.parquet"); err != nil {
		t.Fatalf("mark fresh synced: %v", err)
	}

	// A failed entry that was synced once and later re-sent carries an old
	// synced_at. Only the state filter keeps it out of the prune —
	// `synced_at IS NOT NULL` does not discriminate here — so this is what
	// makes that clause load-bearing rather than redundant.
	resurrect := testEntry("db/cpu/failed_but_synced_before.parquet")
	if err := l.Track(ctx, resurrect); err != nil {
		t.Fatalf("track resurrect: %v", err)
	}
	if _, err := l.db.ExecContext(ctx,
		`UPDATE sync_ledger SET state = ?, synced_at = ? WHERE path = ?`,
		string(StateFailed), cutoff, resurrect.Path); err != nil {
		t.Fatalf("stage failed-with-old-synced_at: %v", err)
	}

	deleted, err := l.PruneSynced(ctx, 7)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != total {
		t.Errorf("deleted = %d, want %d", deleted, total)
	}

	stats, err := l.Stats(ctx, DefaultHubID)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Pending != 1 {
		t.Errorf("pending = %d after prune, want 1 — prune must never touch unsynced work",
			stats.Pending)
	}
	if stats.Synced != 1 {
		t.Errorf("synced = %d after prune, want 1 (the fresh entry)", stats.Synced)
	}
	if stats.Failed != 1 {
		t.Errorf("failed = %d after prune, want 1 — prune deleted a failed entry because it had an old synced_at", stats.Failed)
	}
}

func TestLedger_PruneSyncedNoOpForNonPositiveRetention(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	if err := l.Track(ctx, testEntry("db/cpu/a.parquet")); err != nil {
		t.Fatalf("track: %v", err)
	}
	if err := l.MarkSynced(ctx, DefaultHubID, "db/cpu/a.parquet"); err != nil {
		t.Fatalf("synced: %v", err)
	}

	for _, days := range []int{0, -1} {
		deleted, err := l.PruneSynced(ctx, days)
		if err != nil {
			t.Fatalf("prune(%d): %v", days, err)
		}
		if deleted != 0 {
			t.Errorf("prune(%d) deleted %d rows, want 0 — retention disabled must not delete", days, deleted)
		}
	}
}

func TestLedger_TimestampsRoundTripAsUTC(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	// Arc's convention is UTC everywhere. Writing a non-UTC time must not
	// change the instant that comes back.
	loc := time.FixedZone("UTC-6", -6*60*60)
	e := testEntry("db/cpu/tz.parquet")
	e.PartitionTime = time.Date(2026, 8, 6, 14, 0, 0, 0, loc)
	if err := l.Track(ctx, e); err != nil {
		t.Fatalf("track: %v", err)
	}

	got, err := l.Get(ctx, DefaultHubID, e.Path)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.PartitionTime.Equal(e.PartitionTime) {
		t.Errorf("partition_time = %s, want the same instant as %s",
			got.PartitionTime, e.PartitionTime)
	}
	if got.PartitionTime.Location() != time.UTC {
		t.Errorf("partition_time location = %s, want UTC", got.PartitionTime.Location())
	}
}

func TestNewLedger_RejectsNilDB(t *testing.T) {
	if _, err := NewLedger(nil, zerolog.Nop()); err == nil {
		t.Error("NewLedger(nil) succeeded, want an error")
	}
}

func TestLedger_IllegalTransitionsRejected(t *testing.T) {
	ctx := context.Background()

	// Each case sets an entry up in some state, then attempts a transition
	// that must be refused. Before state guards existed every one of these
	// silently succeeded — leaving rows that claimed both "the hub has it"
	// and "we are sending it", or resurrecting terminally failed entries.
	tests := []struct {
		name  string
		setup func(t *testing.T, l *Ledger, path string)
		act   func(l *Ledger, path string) error
	}{
		{
			name: "MarkInFlight on synced",
			setup: func(t *testing.T, l *Ledger, p string) {
				mustSync(t, l, p)
			},
			act: func(l *Ledger, p string) error { return l.MarkInFlight(ctx, DefaultHubID, p) },
		},
		{
			name: "MarkSynced on failed",
			setup: func(t *testing.T, l *Ledger, p string) {
				mustFail(t, l, p, 1)
			},
			act: func(l *Ledger, p string) error { return l.MarkSynced(ctx, DefaultHubID, p) },
		},
		{
			name: "MarkSynced twice",
			setup: func(t *testing.T, l *Ledger, p string) {
				mustSync(t, l, p)
			},
			act: func(l *Ledger, p string) error { return l.MarkSynced(ctx, DefaultHubID, p) },
		},
		{
			name: "RecordProgress on synced",
			setup: func(t *testing.T, l *Ledger, p string) {
				mustSync(t, l, p)
			},
			act: func(l *Ledger, p string) error { return l.RecordProgress(ctx, DefaultHubID, p, 10) },
		},
		{
			name:  "MarkFailed on pending (never started)",
			setup: func(t *testing.T, l *Ledger, p string) {},
			act:   func(l *Ledger, p string) error { return l.MarkFailed(ctx, DefaultHubID, p, "x", 3) },
		},
		{
			name: "MarkInFlight on already in-flight",
			setup: func(t *testing.T, l *Ledger, p string) {
				if err := l.MarkInFlight(ctx, DefaultHubID, p); err != nil {
					t.Fatalf("setup in-flight: %v", err)
				}
			},
			act: func(l *Ledger, p string) error { return l.MarkInFlight(ctx, DefaultHubID, p) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := setupTestLedger(t)
			const path = "db/cpu/x.parquet"
			if err := l.Track(ctx, testEntry(path)); err != nil {
				t.Fatalf("track: %v", err)
			}
			tt.setup(t, l, path)

			err := tt.act(l, path)
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("err = %v, want ErrInvalidTransition", err)
			}
			// A refused transition must be distinguishable from a missing
			// entry — they are different bugs for the caller to react to.
			if errors.Is(err, ErrNotFound) {
				t.Error("illegal transition reported as ErrNotFound; caller cannot tell the two apart")
			}
		})
	}
}

func mustSync(t *testing.T, l *Ledger, path string) {
	t.Helper()
	ctx := context.Background()
	if err := l.MarkInFlight(ctx, DefaultHubID, path); err != nil {
		t.Fatalf("setup in-flight: %v", err)
	}
	if err := l.MarkSynced(ctx, DefaultHubID, path); err != nil {
		t.Fatalf("setup synced: %v", err)
	}
}

func mustFail(t *testing.T, l *Ledger, path string, maxAttempts int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < maxAttempts; i++ {
		if err := l.MarkInFlight(ctx, DefaultHubID, path); err != nil {
			t.Fatalf("setup in-flight %d: %v", i, err)
		}
		if err := l.MarkFailed(ctx, DefaultHubID, path, "boom", maxAttempts); err != nil {
			t.Fatalf("setup failed %d: %v", i, err)
		}
	}
}

func TestLedger_MarkFailedCapHoldsUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	// The cap decision must be made inside the UPDATE. A read-then-write pair
	// lets a concurrent worker bump attempts past the cap between the two
	// statements, and the losing writer then resurrects the entry to pending —
	// so it retries forever. §8.2 makes concurrent workers the default.
	const path = "db/cpu/contended.parquet"
	const maxAttempts = 2
	if err := l.Track(ctx, testEntry(path)); err != nil {
		t.Fatalf("track: %v", err)
	}

	for round := 0; round < 20; round++ {
		got, err := l.Get(ctx, DefaultHubID, path)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.State == StateFailed {
			break
		}
		if err := l.MarkInFlight(ctx, DefaultHubID, path); err != nil {
			t.Fatalf("round %d in-flight: %v", round, err)
		}

		var wg stdsync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Only one racer wins the in_flight guard; the rest get
				// ErrInvalidTransition, which is the correct outcome.
				_ = l.MarkFailed(ctx, DefaultHubID, path, "boom", maxAttempts)
			}()
		}
		wg.Wait()
	}

	got, err := l.Get(ctx, DefaultHubID, path)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != StateFailed {
		t.Errorf("state = %q with attempts=%d and max=%d — cap not enforced, entry retries forever",
			got.State, got.Attempts, maxAttempts)
	}
}

func TestLedger_ProgressClampedToFileSize(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	e := testEntry("db/cpu/small.parquet")
	e.SizeBytes = 100
	if err := l.Track(ctx, e); err != nil {
		t.Fatalf("track: %v", err)
	}
	if err := l.MarkInFlight(ctx, DefaultHubID, e.Path); err != nil {
		t.Fatalf("in-flight: %v", err)
	}
	if err := l.RecordProgress(ctx, DefaultHubID, e.Path, 5000); err != nil {
		t.Fatalf("progress: %v", err)
	}

	got, err := l.Get(ctx, DefaultHubID, e.Path)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.BytesSent != e.SizeBytes {
		t.Errorf("bytes_sent = %d, want clamped to %d", got.BytesSent, e.SizeBytes)
	}

	// PendingBytes is a SUM across the backlog, so one unclamped offset would
	// go negative and cancel out other files' real pending bytes — understating
	// exactly the figure operators use to size a contact window.
	stats, err := l.Stats(ctx, DefaultHubID)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.PendingBytes < 0 {
		t.Errorf("pending_bytes = %d, must never be negative", stats.PendingBytes)
	}
}

func TestLedger_DiscoveredAtNormalizedToUTC(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	// go-sqlite3 stores a time.Time with its offset intact, so the same
	// instant in two zones yields two different strings — breaking SQL
	// equality and ORDER BY. Both Track and TrackBatch must normalize.
	loc := time.FixedZone("UTC-6", -6*60*60)
	instant := time.Date(2026, 8, 6, 20, 0, 0, 0, time.UTC)

	a := testEntry("db/cpu/utc.parquet")
	a.DiscoveredAt = instant
	if err := l.Track(ctx, a); err != nil {
		t.Fatalf("track a: %v", err)
	}

	b := testEntry("db/cpu/local.parquet")
	b.DiscoveredAt = instant.In(loc)
	if _, err := l.TrackBatch(ctx, []*LedgerEntry{b}); err != nil {
		t.Fatalf("track batch b: %v", err)
	}

	var equal int
	err := l.db.QueryRowContext(ctx, `
		SELECT (SELECT discovered_at FROM sync_ledger WHERE path = ?)
		     = (SELECT discovered_at FROM sync_ledger WHERE path = ?)`,
		a.Path, b.Path).Scan(&equal)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if equal != 1 {
		t.Error("the same instant stored via Track and TrackBatch is not string-equal in SQLite; discovered_at is not UTC-normalized")
	}
}

func TestLedger_TrackBatchReportsZeroOnRollback(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	// A CHECK constraint gives the batch a deterministic mid-loop failure.
	// Empty strings satisfy NOT NULL, so forcing a real violation needs an
	// explicit predicate — without one this test would pass vacuously with
	// both rows inserted and nothing rolled back.
	if _, err := l.db.ExecContext(ctx,
		`CREATE TRIGGER reject_bad BEFORE INSERT ON sync_ledger
		 WHEN NEW.path = 'db/cpu/reject.parquet'
		 BEGIN SELECT RAISE(ABORT, 'rejected'); END`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	good := testEntry("db/cpu/good.parquet")
	bad := testEntry("db/cpu/reject.parquet")

	n, err := l.TrackBatch(ctx, []*LedgerEntry{good, bad})
	if err == nil {
		t.Fatal("TrackBatch succeeded; the trigger should have aborted the second insert")
	}

	var rows int
	if qerr := l.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sync_ledger`).Scan(&rows); qerr != nil {
		t.Fatalf("count: %v", qerr)
	}
	if rows != 0 {
		t.Errorf("%d rows persisted after a failed batch, want 0 — the transaction did not roll back", rows)
	}
	// The first insert reported RowsAffected=1 before the abort, but rollback
	// discarded it. Reporting that count would tell the caller it tracked a
	// file that does not exist.
	if n != 0 {
		t.Errorf("TrackBatch reported %d inserted but rollback discarded every row", n)
	}
}

func TestLedger_PendingToSyncedIsReconcilePath(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	// §5.1: batch reconcile returns files the hub already holds under
	// `present` — typically because a previous ack was lost. The spoke then
	// advances them to synced WITHOUT a transfer, re-sending zero bytes.
	// That makes pending -> synced a legitimate transition, not a bypass of
	// the in_flight step, and it is the whole reason a lost ack costs one
	// redundant reconcile entry rather than a duplicate upload.
	e := testEntry("db/cpu/hub_already_has_it.parquet")
	if err := l.Track(ctx, e); err != nil {
		t.Fatalf("track: %v", err)
	}

	before, err := l.Get(ctx, DefaultHubID, e.Path)
	if err != nil {
		t.Fatalf("get before: %v", err)
	}
	if before.State != StatePending {
		t.Fatalf("setup: state = %q, want pending", before.State)
	}

	if err := l.MarkSynced(ctx, DefaultHubID, e.Path); err != nil {
		t.Fatalf("pending -> synced must be permitted (lost-ack recovery): %v", err)
	}

	got, err := l.Get(ctx, DefaultHubID, e.Path)
	if err != nil {
		t.Fatalf("get after: %v", err)
	}
	if got.State != StateSynced {
		t.Errorf("state = %q, want %q", got.State, StateSynced)
	}
	if got.SyncedAt == nil {
		t.Error("synced_at not set on the reconcile path")
	}
	// No transfer ran, so the attempt counter must not have moved — otherwise
	// repeated lost acks would burn through the retry budget.
	if got.Attempts != 0 {
		t.Errorf("attempts = %d, want 0 — reconcile advanced an entry without a transfer", got.Attempts)
	}
	if got.BytesSent != e.SizeBytes {
		t.Errorf("bytes_sent = %d, want %d (a synced row reads as fully sent)", got.BytesSent, e.SizeBytes)
	}
}
