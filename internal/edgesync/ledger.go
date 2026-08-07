// Package edgesync implements edge-to-cloud replication: a spoke (an Arc instance
// at the edge) ships its immutable Parquet files to a hub (a central Arc).
//
// The unit of sync is the FILE, not the row. Arc already produces immutable
// content-addressed Parquet with a SHA256 in the manifest, so shipping files
// gives end-to-end integrity for free, costs the hub no re-ingestion, and makes
// idempotency trivial: (path, sha256) IS the file's identity.
//
// The package is layered so each piece can be tested on its own:
//
//   - ledger.go — the durable record of what has been sent to which hub, and
//     how far. Deliberately dumb: it tracks state and knows nothing about
//     transports, HTTP, or hubs beyond their ID.
//   - transport.go — the SyncTransport interface the agent talks to, so the
//     wire format (HTTPS, S3 relay, sneakernet bundle) stays swappable.
//   - transport_memory.go — an in-process transport for tests.
//
// See docs/progress/2026-06-04-edge-sync-architecture-converged.md.
package edgesync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// SyncState is the lifecycle of one file's journey to one hub.
type SyncState string

const (
	// StatePending — discovered locally, not yet sent. The starting state, and
	// the state an interrupted transfer reverts to on restart.
	StatePending SyncState = "pending"

	// StateInFlight — a transfer is currently running. Any row left in this
	// state after a crash is stale by definition (the transfer died with the
	// process), which is why RecoverInFlight reverts them at startup.
	StateInFlight SyncState = "in_flight"

	// StateSynced — the hub acknowledged receipt. Terminal on the happy path.
	// Only ever set from a 2xx/AlreadyPresent, never optimistically: the
	// design's ack-then-advance rule means a lost ack costs one extra entry in
	// the next reconcile, but never a silent gap.
	StateSynced SyncState = "synced"

	// StateExported — written to an air-gap bundle, awaiting acknowledgment.
	//
	// A file on a physical drive in transit is neither pending (re-exporting or
	// re-sending it wastes the scarce resource: media, or a contact window) nor
	// synced (no hub has confirmed anything). Without this state, exported rows
	// stay pending, and because Pending orders partition_time DESC a capped
	// export takes the newest N every time — the oldest files are never
	// exported at all. That is a treadmill, not eventual consistency.
	//
	// NOT terminal. It advances to synced when an ack bundle returns, and an
	// operator can revert it to pending if the drive is lost.
	StateExported SyncState = "exported"

	// StateFailed — exhausted retries. Terminal until an operator intervenes.
	StateFailed SyncState = "failed"

	// StateSkipped — deliberately excluded (e.g. a future filtering policy).
	//
	// TODO(#569): reserved, nothing sets it in phase 1. Stats deliberately
	// does not count it, so a skipped row would be invisible to operators —
	// whoever introduces the first writer must add it to Stats at the same
	// time, or drop this constant.
	StateSkipped SyncState = "skipped"
)

// DefaultHubID is the hub identifier used when multi-hub is not configured.
//
// The ledger is keyed (hub_id, path) from day one even though phase 1 syncs to
// a single hub. Turning on multi-hub later is then configuration, not a
// migration of a live edge database — which on a disconnected box in the field
// is the difference between a config push and a site visit.
const DefaultHubID = "default"

// ErrNotFound is returned when a ledger lookup matches no row.
var ErrNotFound = errors.New("edgesync: ledger entry not found")

// ErrInvalidTransition is returned when a state change is attempted from a
// state that does not permit it — e.g. marking a terminally failed entry
// synced, or re-sending an entry the hub has already acknowledged.
//
// The ledger enforces these rather than trusting callers because §6.2's
// exactly-once-effect property depends on `synced` being reached only via a
// hub acknowledgment and never being silently walked back.
var ErrInvalidTransition = errors.New("edgesync: invalid state transition")

// LedgerEntry is one file's sync state with respect to one hub.
type LedgerEntry struct {
	ID            int64
	HubID         string
	Path          string // storage-relative path, as the spoke knows it
	SHA256        string // from the manifest; the integrity anchor
	SizeBytes     int64
	Database      string
	Measurement   string
	PartitionTime time.Time
	DiscoveredAt  time.Time
	State         SyncState
	Attempts      int
	LastAttempt   *time.Time
	SyncedAt      *time.Time

	// BytesSent is the resume checkpoint: how many bytes of this file the hub
	// has already accepted. A transfer that dies mid-file resumes from here
	// rather than restarting, which is the difference between "eventually
	// drains" and "never completes" on a link whose contact window is shorter
	// than the file.
	BytesSent int64

	LastError string

	// ExportedAt / ExportedBundleID identify the air-gap bundle a file left on,
	// for the operator whose drive did not arrive. Nil and empty unless the
	// entry is (or once was) exported. RevertExported filters on the bundle ID
	// in SQL, but an operator needs to READ it to know which ID to revert.
	ExportedAt       *time.Time
	ExportedBundleID string
}

// Ledger is the spoke-side record of sync progress, backed by the shared
// SQLite database (cfg.Auth.DBPath).
//
// Concurrency: *sql.DB is already safe for concurrent use and this type holds
// no in-memory state, so it takes no mutex of its own. Holding an application
// lock across SQLite I/O would serialize readers behind writers for no benefit
// (see the SQLite Review Checklist in CLAUDE.md).
type Ledger struct {
	db     *sql.DB
	logger zerolog.Logger
}

// NewLedger creates the ledger and initializes its schema.
func NewLedger(db *sql.DB, logger zerolog.Logger) (*Ledger, error) {
	if db == nil {
		return nil, errors.New("edgesync: ledger requires a non-nil database")
	}

	l := &Ledger{
		db:     db,
		logger: logger.With().Str("component", "sync-ledger").Logger(),
	}

	if err := l.initSchema(); err != nil {
		return nil, fmt.Errorf("edgesync: initialize ledger schema: %w", err)
	}

	return l, nil
}

// initSchema creates the ledger tables if they do not exist.
func (l *Ledger) initSchema() error {
	const schema = `
	-- One row per (hub, file). The UNIQUE key is what makes discovery
	-- idempotent: re-discovering a file that is already tracked is a no-op
	-- rather than a duplicate transfer.
	CREATE TABLE IF NOT EXISTS sync_ledger (
		id             INTEGER PRIMARY KEY AUTOINCREMENT,
		hub_id         TEXT NOT NULL DEFAULT 'default',
		path           TEXT NOT NULL,
		sha256         TEXT NOT NULL,
		size_bytes     INTEGER NOT NULL,
		database       TEXT NOT NULL,
		measurement    TEXT NOT NULL,
		partition_time TIMESTAMP NOT NULL,
		discovered_at  TIMESTAMP NOT NULL,
		state          TEXT NOT NULL DEFAULT 'pending',
		attempts       INTEGER NOT NULL DEFAULT 0,
		last_attempt   TIMESTAMP,
		synced_at      TIMESTAMP,
		bytes_sent     INTEGER NOT NULL DEFAULT 0,
		last_error     TEXT,
		exported_at    TIMESTAMP,
		exported_bundle_id TEXT,
		UNIQUE(hub_id, path)
	);

	-- Drives the hot query: "what is pending for this hub, newest first".
	-- partition_time is in the index because the agent drains newest-first —
	-- freshest telemetry reaches the hub before a contact window closes.
	CREATE INDEX IF NOT EXISTS idx_sync_ledger_state
		ON sync_ledger(hub_id, state, partition_time);

	-- Serves two queries that idx_sync_ledger_state cannot, both verified with
	-- EXPLAIN QUERY PLAN:
	--   * PruneSynced's cutoff (state + synced_at, no hub_id — so the leading
	--     hub_id column of the other index is unusable and it degrades to a
	--     full scan on exactly the large table batching exists to protect).
	--   * Stats' newest-synced lookup, which is walked in reverse for its
	--     ORDER BY instead of sorting into a temp B-tree on every status call.
	-- Column order is (state, synced_at, hub_id) and both queries must lead
	-- with state to use it — hub_id trails because the prune is hub-agnostic.
	CREATE INDEX IF NOT EXISTS idx_sync_ledger_synced
		ON sync_ledger(state, synced_at, hub_id);

	-- One row per contact session (connectivity acquired -> lost), which is
	-- the unit an operator reasons about. A session may span many agent ticks.
	--
	-- TODO(#569): created here but not yet written. The session lifecycle
	-- belongs to the sync agent (PR 8). Defined now so the schema lands in one
	-- migration rather than altering a live edge database later — on a
	-- disconnected box that is a site visit, not a config push. If PR 8 ships
	-- without using it, delete the table rather than leaving it dead.
	CREATE TABLE IF NOT EXISTS sync_history (
		id           INTEGER PRIMARY KEY AUTOINCREMENT,
		hub_id       TEXT NOT NULL,
		started_at   TIMESTAMP NOT NULL,
		completed_at TIMESTAMP,
		files_synced INTEGER NOT NULL DEFAULT 0,
		bytes_synced INTEGER NOT NULL DEFAULT 0,
		transport    TEXT NOT NULL,
		status       TEXT NOT NULL DEFAULT 'in_progress'
	);

	CREATE INDEX IF NOT EXISTS idx_sync_history_started
		ON sync_history(hub_id, started_at);
	`

	if _, err := l.db.Exec(schema); err != nil {
		return fmt.Errorf("create sync tables: %w", err)
	}

	// CREATE TABLE IF NOT EXISTS leaves an existing table untouched, so a
	// ledger created before the air-gap columns existed keeps its old shape.
	// 26.09.1 is unreleased, so this only affects development databases — but
	// silently running against a table missing exported_at would fail every
	// export with a confusing SQL error rather than a clear one.
	//
	// SQLite has no ADD COLUMN IF NOT EXISTS; a duplicate-column error is the
	// expected outcome on an up-to-date database and is not a failure.
	for _, col := range []string{
		"ALTER TABLE sync_ledger ADD COLUMN exported_at TIMESTAMP",
		"ALTER TABLE sync_ledger ADD COLUMN exported_bundle_id TEXT",
	} {
		if _, err := l.db.Exec(col); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("add air-gap columns: %w", err)
		}
	}

	l.logger.Info().Msg("Sync ledger schema initialized")
	return nil
}

// Track records a file as pending sync to a hub. It is idempotent: a file
// already tracked for this hub is left untouched, whatever state it is in.
//
// This matters because discovery re-walks the manifest every tick. Without
// DO NOTHING, a file already synced would be reset to pending and re-sent
// forever.
//
// PRECONDITION — caller must guarantee path immutability. Because a conflict
// does nothing, re-tracking a path whose content changed keeps the ORIGINAL
// sha256 and size, and the ledger would then assert the hub holds content it
// has never seen. Arc satisfies this: compaction and retention produce new
// immutable paths rather than rewriting one in place (§13), so a given path's
// bytes never change. If a future producer breaks that, this must become an
// upsert that detects the divergence — §6.1 treats same-path-different-SHA as
// a 409-class alarm, and silently discarding the evidence here would hide it.
func (l *Ledger) Track(ctx context.Context, e *LedgerEntry) error {
	hubID := e.HubID
	if hubID == "" {
		hubID = DefaultHubID
	}
	// .UTC() on every timestamp: go-sqlite3 serializes a time.Time with its
	// offset intact, so a caller passing a local-zone value would store
	// "2026-08-06 14:00:00-06:00" while another stores the SAME INSTANT as
	// "2026-08-06 20:00:00+00:00". Those are different strings, so SQL
	// equality and ORDER BY both go wrong. Arc's convention is UTC everywhere.
	discoveredAt := e.DiscoveredAt.UTC()
	if e.DiscoveredAt.IsZero() {
		discoveredAt = time.Now().UTC()
	}

	_, err := l.db.ExecContext(ctx, `
		INSERT INTO sync_ledger
			(hub_id, path, sha256, size_bytes, database, measurement,
			 partition_time, discovered_at, state)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(hub_id, path) DO NOTHING`,
		hubID, e.Path, e.SHA256, e.SizeBytes, e.Database, e.Measurement,
		e.PartitionTime.UTC(), discoveredAt, string(StatePending))
	if err != nil {
		return fmt.Errorf("edgesync: track %q: %w", e.Path, err)
	}
	return nil
}

// TrackBatch records many files in one transaction.
//
// Discovery after a long disconnection can surface thousands of files at once;
// inserting them individually would mean one implicit transaction (and one
// fsync) each. A single transaction turns that into one fsync.
func (l *Ledger) TrackBatch(ctx context.Context, entries []*LedgerEntry) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}

	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("edgesync: begin track batch: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op after a successful Commit

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO sync_ledger
			(hub_id, path, sha256, size_bytes, database, measurement,
			 partition_time, discovered_at, state)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(hub_id, path) DO NOTHING`)
	if err != nil {
		return 0, fmt.Errorf("edgesync: prepare track batch: %w", err)
	}
	defer stmt.Close()

	now := time.Now().UTC()
	// Every error path returns 0, not the running count. The deferred
	// Rollback discards all rows Exec'd so far, so reporting a partial count
	// would tell the caller it tracked files that do not exist in the table —
	// and a caller accumulating `total += n` across batches would drift.
	var inserted int
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		hubID := e.HubID
		if hubID == "" {
			hubID = DefaultHubID
		}
		discoveredAt := e.DiscoveredAt.UTC() // see Track: UTC-normalize every timestamp
		if e.DiscoveredAt.IsZero() {
			discoveredAt = now
		}

		res, err := stmt.ExecContext(ctx,
			hubID, e.Path, e.SHA256, e.SizeBytes, e.Database, e.Measurement,
			e.PartitionTime.UTC(), discoveredAt, string(StatePending))
		if err != nil {
			return 0, fmt.Errorf("edgesync: track %q in batch: %w", e.Path, err)
		}
		// RowsAffected is 0 for a conflict that hit DO NOTHING, so this counts
		// genuinely new entries rather than rows considered.
		if n, err := res.RowsAffected(); err == nil {
			inserted += int(n)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("edgesync: commit track batch: %w", err)
	}
	return inserted, nil
}

// Pending returns entries awaiting transfer to a hub, newest partition first.
//
// Newest-first is a deliberate ordering, not an incidental one: when a contact
// window closes mid-backlog, the freshest telemetry has already reached the
// hub and backfill catches up on a later pass.
//
// A limit <= 0 returns all pending entries.
// Unfinished returns entries that have not reached the hub, in either state
// an operator cares about: still queued, or given up on.
//
// Separate from Pending rather than a widening of it, because the sync pass
// depends on Pending returning ONLY 'pending' rows — a failed entry re-offered
// to the hub would restart a transfer the retry cap deliberately stopped.
// This is the troubleshooting view: a file that exhausted its attempts is the
// one an operator most needs to see, and it is exactly the one Pending hides.
//
// Failed entries sort first: they need a decision, whereas pending ones are
// simply waiting for the next pass.
//
// A limit <= 0 returns everything unfinished.
func (l *Ledger) Unfinished(ctx context.Context, hubID string, limit int) ([]*LedgerEntry, error) {
	if hubID == "" {
		hubID = DefaultHubID
	}

	query := `
		SELECT id, hub_id, path, sha256, size_bytes, database, measurement,
		       partition_time, discovered_at, state, attempts, last_attempt,
		       synced_at, bytes_sent, COALESCE(last_error, ''),
		       exported_at, COALESCE(exported_bundle_id, '')
		FROM sync_ledger
		WHERE hub_id = ? AND state IN (?, ?, ?, ?)
		ORDER BY CASE state WHEN ? THEN 0 ELSE 1 END, partition_time DESC, id ASC`
	args := []any{hubID, string(StateFailed), string(StatePending), string(StateInFlight),
		string(StateExported), string(StateFailed)}

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := l.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("edgesync: query unfinished: %w", err)
	}
	defer rows.Close()

	var out []*LedgerEntry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("edgesync: iterate unfinished: %w", err)
	}
	return out, nil
}

func (l *Ledger) Pending(ctx context.Context, hubID string, limit int) ([]*LedgerEntry, error) {
	if hubID == "" {
		hubID = DefaultHubID
	}

	query := `
		SELECT id, hub_id, path, sha256, size_bytes, database, measurement,
		       partition_time, discovered_at, state, attempts, last_attempt,
		       synced_at, bytes_sent, COALESCE(last_error, ''),
		       exported_at, COALESCE(exported_bundle_id, '')
		FROM sync_ledger
		WHERE hub_id = ? AND state = ?
		ORDER BY partition_time DESC, id ASC`
	args := []any{hubID, string(StatePending)}

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := l.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("edgesync: query pending: %w", err)
	}
	defer rows.Close()

	var out []*LedgerEntry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("edgesync: iterate pending: %w", err)
	}
	return out, nil
}

// MarkInFlight moves an entry to in_flight and increments its attempt count.
func (l *Ledger) MarkInFlight(ctx context.Context, hubID, path string) error {
	if hubID == "" {
		hubID = DefaultHubID
	}
	// Only a pending entry may start a transfer. Guarding this is what stops a
	// synced entry from being re-sent while keeping its synced_at — a row that
	// simultaneously claims "the hub has it" and "we are sending it", which
	// would double-count in Stats and make LastSyncedAt point at a file
	// currently in flight.
	res, err := l.db.ExecContext(ctx, `
		UPDATE sync_ledger
		SET state = ?, attempts = attempts + 1, last_attempt = ?
		WHERE hub_id = ? AND path = ? AND state = ?`,
		string(StateInFlight), time.Now().UTC(), hubID, path, string(StatePending))
	if err != nil {
		return fmt.Errorf("edgesync: mark in-flight %q: %w", path, err)
	}
	return l.checkTransition(ctx, res, hubID, path, StatePending)
}

// RecordProgress updates the resume checkpoint for an in-flight transfer.
//
// bytesSent is the absolute offset the hub has accepted, not a delta, so a
// retry that re-sends from an earlier offset cannot inflate it.
func (l *Ledger) RecordProgress(ctx context.Context, hubID, path string, bytesSent int64) error {
	if hubID == "" {
		hubID = DefaultHubID
	}
	if bytesSent < 0 {
		return fmt.Errorf("edgesync: negative progress %d for %q", bytesSent, path)
	}

	// Clamped to size_bytes in SQL. An offset larger than the file would make
	// Stats.PendingBytes negative, and because that column is a SUM across the
	// backlog, one bad offset silently cancels out other files' real pending
	// bytes — understating exactly the number operators use to size a contact
	// window. Only an in-flight transfer has progress to record.
	res, err := l.db.ExecContext(ctx, `
		UPDATE sync_ledger SET bytes_sent = MIN(?, size_bytes)
		WHERE hub_id = ? AND path = ? AND state = ?`,
		bytesSent, hubID, path, string(StateInFlight))
	if err != nil {
		return fmt.Errorf("edgesync: record progress %q: %w", path, err)
	}
	return l.checkTransition(ctx, res, hubID, path, StateInFlight)
}

// MarkSynced records that the hub has the file.
//
// Call this ONLY on a 2xx or AlreadyPresent from the hub — never optimistically
// before the ack. The whole exactly-once-effect property rests on this rule:
// advancing early converts a lost ack into permanent data loss, whereas
// advancing late costs one redundant entry in the next reconcile.
//
// bytes_sent is set to size_bytes so a synced row reads consistently.
func (l *Ledger) MarkSynced(ctx context.Context, hubID, path string) error {
	if hubID == "" {
		hubID = DefaultHubID
	}
	// Reachable only from pending or in_flight. Two exclusions matter:
	//   - `synced` -> `synced` would move synced_at forward on a redundant ack,
	//     making the acknowledgment timestamp unstable.
	//   - `failed` -> `synced` would silently resurrect a terminally failed
	//     entry, hiding the failure from operators.
	// Pending is permitted because §5.1's reconcile advances entries the hub
	// reports as `present` — the lost-ack recovery path — without a transfer.
	// Exported is permitted because that is exactly what an ack bundle
	// acknowledges: the file left on physical media and a hub has now
	// confirmed it. Without this, an air-gap spoke could never reach synced.
	res, err := l.db.ExecContext(ctx, `
		UPDATE sync_ledger
		SET state = ?, synced_at = ?, bytes_sent = size_bytes, last_error = NULL
		WHERE hub_id = ? AND path = ? AND state IN (?, ?, ?)`,
		string(StateSynced), time.Now().UTC(), hubID, path,
		string(StatePending), string(StateInFlight), string(StateExported))
	if err != nil {
		return fmt.Errorf("edgesync: mark synced %q: %w", path, err)
	}
	return l.checkTransition(ctx, res, hubID, path, StatePending, StateInFlight, StateExported)
}

// MarkExported records that a file was written to an air-gap bundle.
//
// Only from pending: an in_flight row belongs to a running network transfer,
// and exporting it concurrently would put the same file on two paths with no
// way to reconcile which acknowledgment arrived.
//
// This is what stops a capped export from starving old files. Pending orders
// partition_time DESC, so without a state change every export would re-take the
// newest N and the oldest files would never leave the box.
func (l *Ledger) MarkExported(ctx context.Context, hubID, path, bundleID string) error {
	if hubID == "" {
		hubID = DefaultHubID
	}
	if bundleID == "" {
		// The bundle ID is how an ack finds these rows again. Without it an
		// exported row is unattributable and could only be recovered by hand.
		return fmt.Errorf("edgesync: mark exported %q: a bundle ID is required", path)
	}

	res, err := l.db.ExecContext(ctx, `
		UPDATE sync_ledger
		SET state = ?, exported_at = ?, exported_bundle_id = ?, last_error = NULL
		WHERE hub_id = ? AND path = ? AND state = ?`,
		string(StateExported), time.Now().UTC(), bundleID, hubID, path,
		string(StatePending))
	if err != nil {
		return fmt.Errorf("edgesync: mark exported %q: %w", path, err)
	}
	return l.checkTransition(ctx, res, hubID, path, StatePending)
}

// RevertExported returns a bundle's files to pending, for a drive that was lost,
// damaged, or never delivered.
//
// Without this an exported row is a dead end until an ack that will never come:
// the files are neither retryable nor visible as a problem. Scoped to one
// bundle ID so recovering one lost drive does not disturb others in transit.
//
// Returns the number of rows reverted.
func (l *Ledger) RevertExported(ctx context.Context, hubID, bundleID string) (int64, error) {
	if hubID == "" {
		hubID = DefaultHubID
	}
	if bundleID == "" {
		return 0, fmt.Errorf("edgesync: revert exported: a bundle ID is required")
	}

	// Only exported rows. A file already acknowledged by some other route must
	// not be dragged back into the queue by an operator recovering a drive.
	res, err := l.db.ExecContext(ctx, `
		UPDATE sync_ledger
		SET state = ?, exported_at = NULL, exported_bundle_id = NULL
		WHERE hub_id = ? AND exported_bundle_id = ? AND state = ?`,
		string(StatePending), hubID, bundleID, string(StateExported))
	if err != nil {
		return 0, fmt.Errorf("edgesync: revert exported bundle %q: %w", bundleID, err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("edgesync: revert exported bundle %q: %w", bundleID, err)
	}
	return n, nil
}

// Unexported returns files eligible for an air-gap bundle, newest first.
//
// Distinct from Pending only in intent today — both select pending rows — but
// they diverge the moment anything else can enqueue work, and the export path
// should not silently inherit a change made for the network path. Ordering
// matches Pending so a bundle carries the freshest telemetry first.
//
// A limit <= 0 returns everything eligible.
func (l *Ledger) Unexported(ctx context.Context, hubID string, limit int) ([]*LedgerEntry, error) {
	if hubID == "" {
		hubID = DefaultHubID
	}

	query := `
		SELECT id, hub_id, path, sha256, size_bytes, database, measurement,
		       partition_time, discovered_at, state, attempts, last_attempt,
		       synced_at, bytes_sent, COALESCE(last_error, ''),
		       exported_at, COALESCE(exported_bundle_id, '')
		FROM sync_ledger
		WHERE hub_id = ? AND state = ?
		ORDER BY partition_time DESC, id ASC`
	args := []any{hubID, string(StatePending)}

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := l.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("edgesync: query unexported: %w", err)
	}
	defer rows.Close()

	var out []*LedgerEntry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("edgesync: iterate unexported: %w", err)
	}
	return out, nil
}

// MarkFailed records a transfer failure. If the entry has reached maxAttempts
// it becomes terminally failed; otherwise it returns to pending for retry.
//
// bytes_sent is deliberately preserved on the retry path — a failure is
// usually a dropped link, and the bytes the hub already accepted are still
// valid. Discarding the checkpoint would restart a large file from zero on
// exactly the link least able to afford it.
// MarkConflicted terminally fails an entry the hub reports as conflicting.
//
// Separate from MarkFailed because that transition is pending→in_flight→failed
// and requires the row to be in_flight: a conflict found during RECONCILE is
// still 'pending' — no transfer was ever started for it — so MarkFailed would
// match no rows and silently leave it pending, to be re-offered in every
// future reconcile payload forever.
//
// Terminal immediately, with no attempt counting: retrying cannot resolve a
// content disagreement, and the same path holding different bytes on both
// sides needs a human to decide which copy is right.
func (l *Ledger) MarkConflicted(ctx context.Context, hubID, path, errMsg string) error {
	if hubID == "" {
		hubID = DefaultHubID
	}

	// Only from pending: an in_flight row is owned by a running transfer,
	// which resolves it through sendOne's own conflict path.
	res, err := l.db.ExecContext(ctx, `
		UPDATE sync_ledger
		SET state = ?, last_error = ?
		WHERE hub_id = ? AND path = ? AND state = ?`,
		string(StateFailed), errMsg, hubID, path, string(StatePending))
	if err != nil {
		return fmt.Errorf("edgesync: mark conflicted %q: %w", path, err)
	}
	return l.checkTransition(ctx, res, hubID, path, StatePending)
}

func (l *Ledger) MarkFailed(ctx context.Context, hubID, path, errMsg string, maxAttempts int) error {
	if hubID == "" {
		hubID = DefaultHubID
	}
	if maxAttempts <= 0 {
		return fmt.Errorf("edgesync: mark failed %q: maxAttempts must be >= 1, got %d", path, maxAttempts)
	}

	// The cap decision is made INSIDE the UPDATE rather than by reading
	// attempts first and deciding in Go. A read-then-write pair is a
	// lost-update race: between the SELECT and the UPDATE another worker can
	// bump attempts past the cap, and this call would then write back
	// 'pending' — resurrecting an entry that should be terminally failed, so
	// it retries forever. §8.2 configures max_concurrent_files (default 2), so
	// concurrent workers are the designed steady state, not a hypothetical.
	res, err := l.db.ExecContext(ctx, `
		UPDATE sync_ledger
		SET state = CASE WHEN attempts >= ? THEN ? ELSE ? END,
		    last_error = ?
		WHERE hub_id = ? AND path = ? AND state = ?`,
		maxAttempts, string(StateFailed), string(StatePending),
		errMsg, hubID, path, string(StateInFlight))
	if err != nil {
		return fmt.Errorf("edgesync: mark failed %q: %w", path, err)
	}
	return l.checkTransition(ctx, res, hubID, path, StateInFlight)
}

// RecoverInFlight reverts in_flight entries to pending. Call once at startup.
//
// An in_flight row can only have been written by a transfer that is no longer
// running — the process that owned it is gone. Reverting makes those files
// eligible for the next reconcile, where the hub reports any that actually
// landed as `present` and the spoke advances them without re-sending a byte.
func (l *Ledger) RecoverInFlight(ctx context.Context) (int64, error) {
	res, err := l.db.ExecContext(ctx,
		`UPDATE sync_ledger SET state = ? WHERE state = ?`,
		string(StatePending), string(StateInFlight))
	if err != nil {
		return 0, fmt.Errorf("edgesync: recover in-flight entries: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("edgesync: count recovered entries: %w", err)
	}
	if n > 0 {
		l.logger.Info().
			Int64("entries", n).
			Msg("Reverted interrupted transfers to pending")
	}
	return n, nil
}

// Stats summarizes ledger state for one hub.
type Stats struct {
	HubID    string
	Pending  int64
	InFlight int64

	// Exported — on physical media, awaiting an ack. Counted separately
	// because it is neither "still queued" nor "delivered", and folding it
	// into either would misreport how far behind an air-gap spoke is.
	Exported int64

	Synced int64
	Failed int64

	// PendingBytes is what has not reached a hub, INCLUDING exported bytes:
	// a file on a drive in transit has not arrived. Excluding it would make an
	// air-gap spoke's backlog appear to shrink the moment a bundle is written,
	// which is precisely when nothing has been delivered yet.
	PendingBytes int64

	LastSyncedAt *time.Time
}

// Stats returns a summary for a hub, for the status endpoint and operators.
func (l *Ledger) Stats(ctx context.Context, hubID string) (*Stats, error) {
	if hubID == "" {
		hubID = DefaultHubID
	}

	s := &Stats{HubID: hubID}

	err := l.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(state = 'pending'), 0),
			COALESCE(SUM(state = 'in_flight'), 0),
			COALESCE(SUM(state = 'exported'), 0),
			COALESCE(SUM(state = 'synced'), 0),
			COALESCE(SUM(state = 'failed'), 0),
			COALESCE(SUM(CASE WHEN state IN ('pending','in_flight','exported')
			                  THEN size_bytes - bytes_sent ELSE 0 END), 0)
		FROM sync_ledger WHERE hub_id = ?`, hubID).
		Scan(&s.Pending, &s.InFlight, &s.Exported, &s.Synced, &s.Failed, &s.PendingBytes)
	if err != nil {
		return nil, fmt.Errorf("edgesync: ledger stats: %w", err)
	}

	// The newest synced_at is fetched by ORDER BY ... LIMIT 1 rather than
	// MAX(synced_at) in the aggregate above. go-sqlite3 maps a column to
	// time.Time from its declared TIMESTAMP type, and that association is lost
	// through an aggregate function — MAX() returns a bare string that
	// sql.NullTime cannot scan. Selecting the column directly keeps the
	// declared type, and the ORDER BY is index-eligible.
	// The predicate leads with state so idx_sync_ledger_synced(state,
	// synced_at, hub_id) can be walked in reverse for the ORDER BY instead of
	// sorting into a temp B-tree. Filtering on hub_id first (its natural
	// phrasing) picks idx_sync_ledger_state, whose columns cannot serve an
	// ORDER BY on synced_at — verified with EXPLAIN QUERY PLAN.
	var lastSynced sql.NullTime
	err = l.db.QueryRowContext(ctx, `
		SELECT synced_at FROM sync_ledger
		WHERE state = ? AND synced_at IS NOT NULL AND hub_id = ?
		ORDER BY synced_at DESC LIMIT 1`, string(StateSynced), hubID).Scan(&lastSynced)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("edgesync: ledger last-synced: %w", err)
	}
	if lastSynced.Valid {
		t := lastSynced.Time.UTC()
		s.LastSyncedAt = &t
	}
	return s, nil
}

// Get returns a single entry, or ErrNotFound.
func (l *Ledger) Get(ctx context.Context, hubID, path string) (*LedgerEntry, error) {
	if hubID == "" {
		hubID = DefaultHubID
	}
	row := l.db.QueryRowContext(ctx, `
		SELECT id, hub_id, path, sha256, size_bytes, database, measurement,
		       partition_time, discovered_at, state, attempts, last_attempt,
		       synced_at, bytes_sent, COALESCE(last_error, ''),
		       exported_at, COALESCE(exported_bundle_id, '')
		FROM sync_ledger WHERE hub_id = ? AND path = ?`, hubID, path)

	e, err := scanEntry(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return e, err
}

// PruneSynced deletes synced entries older than retentionDays, across ALL
// hubs. Unlike every other method here it is deliberately not hub-scoped:
// retention is a local disk-space concern, not a per-hub policy. When
// multi-hub becomes real and per-hub retention is wanted, this needs a hubID
// parameter — the signature is the place that will have to change.
//
// Batched at 1000 rows with an ORDER BY on the primary key: a single unbounded
// DELETE on a table with millions of rows holds the SQLite write lock for the
// duration, blocking ingest file registration and auth token updates. The
// context is checked between batches so shutdown is not delayed.
//
// No incremental_vacuum: the freed pages are reused by subsequent inserts, and
// vacuuming a continuously-written table just causes write amplification.
func (l *Ledger) PruneSynced(ctx context.Context, retentionDays int) (int64, error) {
	if retentionDays <= 0 {
		return 0, nil
	}

	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays)

	// Resolve the predicate once to a primary-key bound, so the batch loop
	// deletes by rowid instead of re-evaluating the date predicate per batch.
	//
	// Time domain: go-sqlite3 serializes a time.Time as
	// "2006-01-02 15:04:05.999999999-07:00" — space-separated WITH an offset
	// suffix. That is neither RFC3339 nor SQLite's datetime() output (which is
	// space-separated with NO offset). Comparing these strings against a
	// datetime()-produced value is therefore unsafe even though both use a
	// space: a whole-second UTC time serializes as "…00:00:00+00:00" while
	// datetime() gives "…00:00:00", and the longer string sorts AFTER the
	// shorter one at the same instant. So: values written as Go time.Time are
	// compared ONLY against Go time.Time parameters, as here. Never mix.
	var maxID int64
	err := l.db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(id), 0) FROM sync_ledger
		WHERE state = ? AND synced_at IS NOT NULL AND synced_at < ?`,
		string(StateSynced), cutoff).Scan(&maxID)
	if err != nil {
		return 0, fmt.Errorf("edgesync: find prune cutoff: %w", err)
	}
	if maxID == 0 {
		return 0, nil
	}

	const batchSize = 1000
	var total int64

	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}

		// The state/synced_at predicate is repeated here even though maxID
		// already bounds the set: id <= maxID alone would also match rows
		// interleaved below the cutoff that are pending or failed. Either
		// filter alone is sufficient (removing one and keeping the other
		// still protects unsynced work) — they are deliberate belt-and-braces
		// on a DELETE, where the cost of being wrong is unrecoverable.
		// ORDER BY id is on the primary key, so each batch is deterministic.
		res, err := l.db.ExecContext(ctx, `
			DELETE FROM sync_ledger WHERE id IN (
				SELECT id FROM sync_ledger
				WHERE id <= ? AND state = ? AND synced_at IS NOT NULL AND synced_at < ?
				ORDER BY id ASC LIMIT ?
			)`, maxID, string(StateSynced), cutoff, batchSize)
		if err != nil {
			return total, fmt.Errorf("edgesync: prune synced entries: %w", err)
		}

		deleted, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("edgesync: count pruned entries: %w", err)
		}
		total += deleted

		if deleted < batchSize {
			break
		}
	}
	return total, nil
}

// rowScanner is satisfied by both *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanEntry(s rowScanner) (*LedgerEntry, error) {
	var (
		e           LedgerEntry
		lastAttempt sql.NullTime
		syncedAt    sql.NullTime
		exportedAt  sql.NullTime
		state       string
	)

	err := s.Scan(
		&e.ID, &e.HubID, &e.Path, &e.SHA256, &e.SizeBytes, &e.Database,
		&e.Measurement, &e.PartitionTime, &e.DiscoveredAt, &state,
		&e.Attempts, &lastAttempt, &syncedAt, &e.BytesSent, &e.LastError,
		&exportedAt, &e.ExportedBundleID,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, err // caller maps to ErrNotFound
		}
		return nil, fmt.Errorf("edgesync: scan ledger entry: %w", err)
	}

	e.State = SyncState(state)
	e.PartitionTime = e.PartitionTime.UTC()
	e.DiscoveredAt = e.DiscoveredAt.UTC()
	if lastAttempt.Valid {
		t := lastAttempt.Time.UTC()
		e.LastAttempt = &t
	}
	if syncedAt.Valid {
		t := syncedAt.Time.UTC()
		e.SyncedAt = &t
	}
	if exportedAt.Valid {
		t := exportedAt.Time.UTC()
		e.ExportedAt = &t
	}
	return &e, nil
}

// checkAffected turns a zero-row UPDATE into ErrNotFound. Without this a state
// transition against a path that isn't tracked would silently succeed, and the
// agent would believe it had advanced an entry that does not exist.
func checkAffected(res sql.Result, path string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("edgesync: rows affected for %q: %w", path, err)
	}
	if n == 0 {
		return fmt.Errorf("edgesync: %q: %w", path, ErrNotFound)
	}
	return nil
}

// checkTransition reports the outcome of a guarded state transition. A guarded
// UPDATE carries `AND state = ?`, so zero rows affected is ambiguous: either
// the path is untracked, or it is tracked but in the wrong state. The two are
// different bugs — an untracked path means discovery is broken, a wrong state
// means the agent's state machine is — so they get different errors.
func (l *Ledger) checkTransition(ctx context.Context, res sql.Result, hubID, path string, from ...SyncState) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("edgesync: rows affected for %q: %w", path, err)
	}
	if n > 0 {
		return nil
	}

	var current string
	err = l.db.QueryRowContext(ctx,
		`SELECT state FROM sync_ledger WHERE hub_id = ? AND path = ?`, hubID, path).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("edgesync: %q: %w", path, ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("edgesync: read state for %q: %w", path, err)
	}
	return fmt.Errorf("edgesync: %q: %w: state is %q, expected one of %v",
		path, ErrInvalidTransition, current, from)
}
