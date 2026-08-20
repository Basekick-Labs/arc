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

	// compacted_at marks a receipt whose FILE the hub's own compaction has
	// consumed (#619): the content still exists inside a compacted output,
	// so the receipt is not stale — reconcile keeps answering "present" and
	// receive treats a re-sent copy as already delivered. Tolerant ALTER for
	// databases created before the column existed (SQLite has no ADD COLUMN
	// IF NOT EXISTS; the duplicate error is the up-to-date outcome).
	if _, err := h.db.Exec(`ALTER TABLE sync_received ADD COLUMN compacted_at TIMESTAMP`); err != nil &&
		!strings.Contains(err.Error(), "duplicate column name") {
		return fmt.Errorf("add compacted_at column: %w", err)
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
			hub_path     = excluded.hub_path,
			sha256       = excluded.sha256,
			size_bytes   = excluded.size_bytes,
			received_at  = excluded.received_at,
			compacted_at = NULL`,
		r.SpokeID, r.SourcePath, r.HubPath, r.SHA256, r.SizeBytes, receivedAt.UTC())
	if err != nil {
		return fmt.Errorf("edgesync: record received %q: %w", r.SourcePath, err)
	}
	return nil
}

// HeldFile is what Lookup reports for one receipt: the delivered content's
// digest, and whether the hub's own compaction has since consumed the FILE
// (the content lives on inside a compacted output — the receipt stays valid,
// but nothing at the original path exists to stat).
type HeldFile struct {
	SHA256    string
	Compacted bool
}

// Lookup returns the receipts the hub holds for the given paths from one spoke.
//
// Batched deliberately: reconcile asks about thousands of paths at once, and
// issuing one query per path would make the round-trip the design saved on the
// wire reappear as a query storm against SQLite. A path absent from the map
// is one the hub does not hold.
func (h *HubIndex) Lookup(ctx context.Context, spokeID string, paths []string) (map[string]HeldFile, error) {
	out := make(map[string]HeldFile, len(paths))
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

		query := `SELECT source_path, sha256, compacted_at IS NOT NULL FROM sync_received WHERE spoke_id = ? AND source_path IN (?` +
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
				var (
					p, sha    string
					compacted bool
				)
				if err := rows.Scan(&p, &sha, &compacted); err != nil {
					return fmt.Errorf("edgesync: scan received: %w", err)
				}
				out[p] = HeldFile{SHA256: sha, Compacted: compacted}
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

// MarkCompacted stamps compacted_at on the receipts for source paths whose
// files the hub's own compaction consumed (#619). Idempotent — recovery can
// re-fire it — and deliberately an UPDATE, never an INSERT: a path with no
// receipt was not received from this spoke and gets no bookkeeping. Chunked
// like Lookup.
func (h *HubIndex) MarkCompacted(ctx context.Context, spokeID string, sourcePaths []string) error {
	if len(sourcePaths) == 0 {
		return nil
	}
	const chunkSize = 900
	now := time.Now().UTC()
	for start := 0; start < len(sourcePaths); start += chunkSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + chunkSize
		if end > len(sourcePaths) {
			end = len(sourcePaths)
		}
		chunk := sourcePaths[start:end]
		query := `UPDATE sync_received SET compacted_at = ? WHERE spoke_id = ? AND source_path IN (?` +
			strings.Repeat(",?", len(chunk)-1) + `)`
		args := make([]any, 0, len(chunk)+2)
		args = append(args, now, spokeID)
		for _, p := range chunk {
			args = append(args, p)
		}
		if _, err := h.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("edgesync: mark receipts compacted: %w", err)
		}
	}
	return nil
}

// Forget removes a spoke's record for a path.
//
// TODO(#611): NOT YET CALLED IN PRODUCTION, and the gap has teeth. If a
// GENUINE hub-side removal deletes a synced file without calling this, the
// index keeps claiming the hub holds it, so reconcile reports it `present`
// and the spoke marks it synced. A spoke configured with delete_after_sync
// (phase 3) would then be free to delete its only copy of data the hub no
// longer has.
//
// Forget is for genuine removals ONLY — content the hub really no longer
// holds. Hub-side COMPACTION of spoke namespaces (#619) deletes files too,
// but it PRESERVES their content inside the compacted output, so it goes
// through MarkCompacted instead: the receipt stays valid and reconcile
// keeps vouching, which is correct. Do not "fix" that to Forget.
//
// What keeps the Forget gap from being a live hazard today:
// delete_after_sync does not exist yet, and no hub-side path REMOVES spoke
// content — retention operates on Arc's own ingested databases (and its
// partition parsing does not currently reach spoke namespaces). Whoever
// adds hub retention over spoke prefixes, or ships phase 3, MUST wire this
// first.
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
