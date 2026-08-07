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

// HubIndex records what a hub has received, so reconcile can answer without
// touching parquet bytes.
//
// §5.1 assumes the hub answers missing/present/conflicts "from its manifest in
// O(N) lookups, no I/O on the parquet bytes". That manifest is the Raft FSM,
// which exists only in cluster mode — in OSS standalone there is no file index
// at all, and distinguishing present from conflict would mean reading and
// hashing every candidate file. For a spoke returning from a long outage that
// is gigabytes of disk reads on the one request that exists to be cheap.
//
// So the hub keeps its own index. The receive path already knows every file's
// path, digest, and size at commit time, so recording them costs one small
// write on a path that has just done a full hash and a promote.
//
// It is keyed by the SPOKE's path rather than the hub's namespaced path,
// because that is what a spoke asks about: it has no idea the hub prepends a
// namespace.
type HubIndex struct {
	db     *sql.DB
	logger zerolog.Logger
}

// ReceivedRecord is one file the hub holds, as the spoke knows it.
type ReceivedRecord struct {
	SpokeID    string
	SourcePath string // the spoke's own path, before hub namespacing
	HubPath    string // where the hub actually stored it
	SHA256     string
	SizeBytes  int64
	ReceivedAt time.Time
}

// NewHubIndex creates the index and initializes its schema.
func NewHubIndex(db *sql.DB, logger zerolog.Logger) (*HubIndex, error) {
	if db == nil {
		return nil, errors.New("edgesync: hub index requires a non-nil database")
	}
	h := &HubIndex{db: db, logger: logger}
	if err := h.initSchema(); err != nil {
		return nil, fmt.Errorf("edgesync: initialize hub index schema: %w", err)
	}
	return h, nil
}

func (h *HubIndex) initSchema() error {
	const schema = `
	CREATE TABLE IF NOT EXISTS sync_received (
		id           INTEGER PRIMARY KEY AUTOINCREMENT,
		spoke_id     TEXT NOT NULL,
		source_path  TEXT NOT NULL,
		hub_path     TEXT NOT NULL,
		sha256       TEXT NOT NULL,
		size_bytes   INTEGER NOT NULL,
		received_at  TIMESTAMP NOT NULL,
		UNIQUE(spoke_id, source_path)
	);

	-- Reconcile looks up (spoke_id, source_path) for every entry in a batch,
	-- so the UNIQUE index above is the hot path. This one serves operator
	-- queries ("what has rocket-07 sent, newest first") without scanning.
	CREATE INDEX IF NOT EXISTS idx_sync_received_spoke
		ON sync_received(spoke_id, received_at);
	`

	if _, err := h.db.Exec(schema); err != nil {
		return fmt.Errorf("create sync_received table: %w", err)
	}
	h.logger.Debug().Msg("Sync hub index schema initialized")
	return nil
}

// Record notes that the hub holds a file.
//
// Upserts on (spoke_id, source_path): the digest is allowed to change here
// because the receive path only reaches this point after verifying the bytes,
// so a differing digest means the file was legitimately replaced rather than
// that two spokes collided. A conflicting upload never gets this far — it is
// refused with 409 before promotion.
func (h *HubIndex) Record(ctx context.Context, r *ReceivedRecord) error {
	receivedAt := r.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now()
	}

	_, err := h.db.ExecContext(ctx, `
		INSERT INTO sync_received
			(spoke_id, source_path, hub_path, sha256, size_bytes, received_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(spoke_id, source_path) DO UPDATE SET
			hub_path    = excluded.hub_path,
			sha256      = excluded.sha256,
			size_bytes  = excluded.size_bytes,
			received_at = excluded.received_at`,
		r.SpokeID, r.SourcePath, r.HubPath, r.SHA256, r.SizeBytes, receivedAt.UTC())
	if err != nil {
		return fmt.Errorf("edgesync: record received %q: %w", r.SourcePath, err)
	}
	return nil
}

// Lookup returns the digests the hub holds for the given paths from one spoke.
//
// Batched deliberately: reconcile asks about thousands of paths at once, and
// issuing one query per path would make the round-trip the design saved on the
// wire reappear as a query storm against SQLite. The result maps source path
// to digest; a path absent from the map is one the hub does not hold.
func (h *HubIndex) Lookup(ctx context.Context, spokeID string, paths []string) (map[string]string, error) {
	out := make(map[string]string, len(paths))
	if len(paths) == 0 {
		return out, nil
	}

	// Chunked well below SQLITE_MAX_VARIABLE_NUMBER. The limit in this build
	// is 32766, not the 999 of older SQLite — measured, not assumed — but 900
	// is kept deliberately: it stays under the historical limit so the code
	// does not depend on which SQLite a deployment links, and it bounds how
	// long any single statement holds the shared writer.
	const chunkSize = 900

	for start := 0; start < len(paths); start += chunkSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := start + chunkSize
		if end > len(paths) {
			end = len(paths)
		}
		chunk := paths[start:end]

		query := `SELECT source_path, sha256 FROM sync_received WHERE spoke_id = ? AND source_path IN (?` +
			strings.Repeat(",?", len(chunk)-1) + `)`

		args := make([]any, 0, len(chunk)+1)
		args = append(args, spokeID)
		for _, p := range chunk {
			args = append(args, p)
		}

		// Scanned inside a closure so rows.Close is deferred rather than
		// called manually on each path. sharedSQLiteHandle sets
		// SetMaxOpenConns(1), so a single leaked cursor blocks every
		// subsequent query on a handle shared with auth, audit, and ingest
		// file-registration — a scan error here would wedge all of them.
		if err := func() error {
			rows, err := h.db.QueryContext(ctx, query, args...)
			if err != nil {
				return fmt.Errorf("edgesync: lookup received: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				var p, sha string
				if err := rows.Scan(&p, &sha); err != nil {
					return fmt.Errorf("edgesync: scan received: %w", err)
				}
				out[p] = sha
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("edgesync: iterate received: %w", err)
			}
			return nil
		}(); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// Forget removes a spoke's record for a path.
//
// TODO(#569): NOT YET CALLED IN PRODUCTION, and the gap has teeth. If hub-side
// retention deletes a synced file from storage without calling this, the index
// keeps claiming the hub holds it, so reconcile reports it `present` and the
// spoke marks it synced. A spoke configured with delete_after_sync (phase 3)
// would then be free to delete its only copy of data the hub no longer has.
//
// Two things keep that from being a live hazard today: delete_after_sync does
// not exist yet, and nothing on the hub deletes files inside a spoke namespace
// — Arc retention operates on its own ingested databases. Whoever adds either
// MUST call this, and phase 3 must not ship before it is wired.
func (h *HubIndex) Forget(ctx context.Context, spokeID, sourcePath string) error {
	_, err := h.db.ExecContext(ctx,
		`DELETE FROM sync_received WHERE spoke_id = ? AND source_path = ?`,
		spokeID, sourcePath)
	if err != nil {
		return fmt.Errorf("edgesync: forget %q: %w", sourcePath, err)
	}
	return nil
}

// ForgetBatch removes several of a spoke's records in one statement per chunk.
//
// Batched because the alternative — one DELETE per stale row inside a request
// handler — lands on the SQLite handle shared with ingest file-registration,
// auth, and audit. A spoke reconciling after a retention sweep can present
// thousands of stale rows at once, and issuing that many separate implicit
// transactions on the single writer would contend with ingest for the duration
// of the request.
func (h *HubIndex) ForgetBatch(ctx context.Context, spokeID string, paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	const chunkSize = 900
	for start := 0; start < len(paths); start += chunkSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + chunkSize
		if end > len(paths) {
			end = len(paths)
		}
		chunk := paths[start:end]

		query := `DELETE FROM sync_received WHERE spoke_id = ? AND source_path IN (?` +
			strings.Repeat(",?", len(chunk)-1) + `)`
		args := make([]any, 0, len(chunk)+1)
		args = append(args, spokeID)
		for _, p := range chunk {
			args = append(args, p)
		}

		if _, err := h.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("edgesync: forget batch: %w", err)
		}
	}
	return nil
}

// CountForSpoke returns how many files the hub holds from a spoke.
func (h *HubIndex) CountForSpoke(ctx context.Context, spokeID string) (int64, error) {
	var n int64
	err := h.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sync_received WHERE spoke_id = ?`, spokeID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("edgesync: count received for %q: %w", spokeID, err)
	}
	return n, nil
}
