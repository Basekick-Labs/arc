package edgesync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
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
	// removed_at / removed_reason mark a receipt whose CONTENT the hub
	// removed on purpose: a confirmed database delete or retention (#1186).
	// Unlike compacted_at nothing survives, but the receipt stays so
	// reconcile keeps answering "present" and the spoke does not re-send what
	// was deleted. Recovery is an explicit ForgetRemoved. Same tolerant ALTER.
	// compacted_into: the hub storage path that now holds a compacted
	// receipt's content (#1683), so a verify can check that holder exists.
	for _, col := range []string{"removed_at TIMESTAMP", "removed_reason TEXT", "compacted_into TEXT"} {
		if _, err := h.db.Exec(`ALTER TABLE sync_received ADD COLUMN ` + col); err != nil &&
			!strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("add %s column: %w", col, err)
		}
	}
	// The holder chain (MarkCompactedInto) updates by compacted_into on every
	// hub compaction job; without this it scans a spoke's receipts under the
	// shared write lock (#1683).
	if _, err := h.db.Exec(`CREATE INDEX IF NOT EXISTS idx_sync_received_holder
		ON sync_received(spoke_id, compacted_into) WHERE compacted_into IS NOT NULL`); err != nil {
		return fmt.Errorf("create holder index: %w", err)
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
			compacted_at = NULL,
			compacted_into = NULL,
			removed_at   = NULL,
			removed_reason = NULL`,
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
	// Removed: the hub removed the content on purpose (#1186). Reconcile and
	// receive vouch for it like a compacted receipt, so the spoke does not
	// re-send it; unlike Compacted, nothing of it survives on the hub.
	Removed bool
	// CompactedInto: for a compacted receipt, the hub storage path holding
	// its content, or "" when unknown (#1683).
	CompactedInto string
}

// Vouched reports whether the receipt stands although no file exists at its
// path: compacted (content inside a compacted output) or removed (deleted on
// purpose; re-sending it would undo the delete).
func (f HeldFile) Vouched() bool { return f.Compacted || f.Removed }

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

		query := `SELECT source_path, sha256, compacted_at IS NOT NULL, removed_at IS NOT NULL, COALESCE(compacted_into, '') FROM sync_received WHERE spoke_id = ? AND source_path IN (?` +
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
					p, sha, into       string
					compacted, removed bool
				)
				if err := rows.Scan(&p, &sha, &compacted, &removed, &into); err != nil {
					return fmt.Errorf("edgesync: scan received: %w", err)
				}
				out[p] = HeldFile{SHA256: sha, Compacted: compacted, Removed: removed, CompactedInto: into}
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
//
// Reached from three places, all through cmd/arc's receipt marker, and all
// preserve the content: compaction's consumed inputs, tiering's hot-file
// removals (migration, #687), and a row-level DELETE's partial rewrite (#1666). Content removed on purpose is MarkRemoved
// (#1186); see Forget for the contract.
func (h *HubIndex) MarkCompacted(ctx context.Context, spokeID string, sourcePaths []string) error {
	if len(sourcePaths) > 0 {
		// A DELETE rewrites ordinary databases' files on a hub too; only a
		// spoke namespace holds receipts.
		if has, err := h.hasReceipts(ctx, spokeID); err != nil || !has {
			return err
		}
	}
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

// MarkCompactedNoHolder marks receipts compacted with no known holder,
// clearing any it had: a row-level DELETE about to rewrite the file (#1666),
// whose old path is about to go and whose rewrite is not a holder a spoke
// may be deleted against (#1683). UPDATE only; chunked.
func (h *HubIndex) MarkCompactedNoHolder(ctx context.Context, spokeID string, sourcePaths []string) error {
	return h.updateBySourcePath(ctx, spokeID, sourcePaths,
		`UPDATE sync_received SET compacted_at = ?, compacted_into = NULL WHERE spoke_id = ? AND source_path IN (`,
		time.Now().UTC())
}

// MarkCompactedInPlace marks receipts compacted whose content stays at the
// same key in another tier: tiering's hot->cold migration (#687). A receipt
// with no holder yet gets its own hub path as holder; one already pointing
// at a compaction output keeps it, so a later removal of the cold copy cannot
// leave it pointing at a deleted file (#1683). UPDATE only; chunked.
func (h *HubIndex) MarkCompactedInPlace(ctx context.Context, spokeID string, sourcePaths []string) error {
	return h.updateBySourcePath(ctx, spokeID, sourcePaths,
		`UPDATE sync_received SET compacted_at = ?, compacted_into = COALESCE(compacted_into, ? || '/' || source_path) WHERE spoke_id = ? AND source_path IN (`,
		time.Now().UTC(), spokeID)
}

// MarkCompactedInto records a hub compaction job (#619, #1683): the receipts
// of its consumed inputs are marked compacted with the job's output as their
// holder, and receipts whose holder was one of the consumed files (a
// compaction of compactions) move to the new output. sourcePaths are the
// inputs as source paths (spoke prefix stripped); consumed are the same
// inputs as full hub storage paths; output is a full hub storage path.
//
// A re-fired mark (recovery keeps the manifest after a failure) must not move
// a receipt back from a later holder to this, deleted, one: an input's
// receipt is re-pointed only while it has no holder or holds itself (a cold
// copy). One whose holder was a consumed file moves through the chain update.
func (h *HubIndex) MarkCompactedInto(ctx context.Context, spokeID string, sourcePaths, consumed []string, output string) error {
	if len(sourcePaths) == 0 || output == "" {
		return nil
	}
	if has, err := h.hasReceipts(ctx, spokeID); err != nil || !has {
		return err
	}
	now := time.Now().UTC()
	consumedSet := make(map[string]struct{}, len(consumed))
	for _, c := range consumed {
		consumedSet[c] = struct{}{}
	}
	for start := 0; start < len(sourcePaths); start += removalChunk {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := sourcePaths[start:min(start+removalChunk, len(sourcePaths))]
		query := `UPDATE sync_received SET compacted_at = ?, compacted_into = ?
			WHERE spoke_id = ? AND source_path IN (?` + strings.Repeat(",?", len(chunk)-1) + `)
			  AND (compacted_into IS NULL OR compacted_into = ? || '/' || source_path)`
		args := make([]any, 0, len(chunk)+4)
		args = append(args, now, output, spokeID)
		for _, p := range chunk {
			args = append(args, p)
		}
		args = append(args, spokeID)
		if _, err := h.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("edgesync: mark receipts compacted into %q: %w", output, err)
		}
	}
	// The chain: receipts whose holder this job consumed.
	chain := make([]string, 0, len(consumedSet))
	for c := range consumedSet {
		chain = append(chain, c)
	}
	sort.Strings(chain)
	for start := 0; start < len(chain); start += removalChunk {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := chain[start:min(start+removalChunk, len(chain))]
		query := `UPDATE sync_received SET compacted_into = ?
			WHERE spoke_id = ? AND compacted_into IN (?` + strings.Repeat(",?", len(chunk)-1) + `)`
		args := make([]any, 0, len(chunk)+2)
		args = append(args, output, spokeID)
		for _, c := range chunk {
			args = append(args, c)
		}
		if _, err := h.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("edgesync: move receipt holders to %q: %w", output, err)
		}
	}
	return nil
}

// updateBySourcePath runs prefix + "?,?,...)" over sourcePaths in chunks,
// with lead args before spokeID. Skips spokes with no receipts.
func (h *HubIndex) updateBySourcePath(ctx context.Context, spokeID string, sourcePaths []string, prefix string, lead ...any) error {
	if len(sourcePaths) == 0 {
		return nil
	}
	if has, err := h.hasReceipts(ctx, spokeID); err != nil || !has {
		return err
	}
	for start := 0; start < len(sourcePaths); start += removalChunk {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := sourcePaths[start:min(start+removalChunk, len(sourcePaths))]
		query := prefix + `?` + strings.Repeat(",?", len(chunk)-1) + `)`
		args := make([]any, 0, len(chunk)+len(lead)+1)
		args = append(args, lead...)
		args = append(args, spokeID)
		for _, p := range chunk {
			args = append(args, p)
		}
		if _, err := h.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("edgesync: mark receipts: %w", err)
		}
	}
	return nil
}

// Removal reasons recorded in removed_reason.
const (
	RemovedByOperatorDelete = "operator_delete"
	RemovedByRetention      = "retention"
	RemovedByRowDelete      = "row_delete"
)

// hasReceipts reports whether the spoke has any receipt, from the
// UNIQUE(spoke_id, source_path) index.
func (h *HubIndex) hasReceipts(ctx context.Context, spokeID string) (bool, error) {
	var one int
	err := h.db.QueryRowContext(ctx, `SELECT 1 FROM sync_received WHERE spoke_id = ? LIMIT 1`, spokeID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("edgesync: probe receipts of %q: %w", spokeID, err)
	}
	return true, nil
}

// removalChunk bounds one UPDATE or DELETE on the SQLite handle shared with
// ingest file registration, auth and audit (SQLite checklist: <= 1000 rows).
const removalChunk = 900

// MarkRemoved marks the receipts of spoke files the hub removed on purpose
// (#1186). A path with a receipt of its own is a delivered file: its receipt
// is marked. A path with none is a hub-side compacted OUTPUT, whose content
// came from inputs whose receipts say compacted: those under the output's
// directory are marked, since their content lived in outputs there. That can
// also mark an input whose content sits in another output of the directory
// that survives; reconcile answers "present" for both states, and the forget
// route refuses while any file remains, so the only effect is the receipt
// saying removed rather than compacted. Idempotent; UPDATE only.
//
// Directory matches use a range on source_path ([dir/, dir0)), which the
// UNIQUE(spoke_id, source_path) index serves; LIKE would treat "_" in a
// database name as a wildcard.
func (h *HubIndex) MarkRemoved(ctx context.Context, spokeID string, sourcePaths []string, reason string) error {
	if len(sourcePaths) == 0 {
		return nil
	}
	// One indexed probe first: on a hub, retention and deletes reach every
	// database, and only spoke namespaces hold receipts.
	if has, err := h.hasReceipts(ctx, spokeID); err != nil || !has {
		return err
	}
	held, err := h.Lookup(ctx, spokeID, sourcePaths)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for start := 0; start < len(sourcePaths); start += removalChunk {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(start+removalChunk, len(sourcePaths))
		chunk := sourcePaths[start:end]
		query := `UPDATE sync_received SET removed_at = ?, removed_reason = ? WHERE spoke_id = ? AND removed_at IS NULL AND source_path IN (?` +
			strings.Repeat(",?", len(chunk)-1) + `)`
		args := make([]any, 0, len(chunk)+3)
		args = append(args, now, reason, spokeID)
		for _, p := range chunk {
			args = append(args, p)
		}
		if _, err := h.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("edgesync: mark receipts removed: %w", err)
		}
	}
	dirs := make(map[string]struct{})
	for _, p := range sourcePaths {
		if _, delivered := held[p]; delivered {
			continue
		}
		if i := strings.LastIndexByte(p, '/'); i > 0 {
			dirs[p[:i+1]] = struct{}{}
		}
	}
	for dir := range dirs {
		if err := ctx.Err(); err != nil {
			return err
		}
		// "0" is the byte after "/", so [dir, dir0) is exactly "starts with dir".
		upper := dir[:len(dir)-1] + "0"
		if _, err := h.db.ExecContext(ctx,
			`UPDATE sync_received SET removed_at = ?, removed_reason = ?
			 WHERE spoke_id = ? AND source_path >= ? AND source_path < ?
			   AND compacted_at IS NOT NULL AND removed_at IS NULL`,
			now, reason, spokeID, dir, upper); err != nil {
			return fmt.Errorf("edgesync: mark compacted receipts removed under %q: %w", dir, err)
		}
	}
	return nil
}

// MarkSpokeRemoved marks every receipt of a spoke removed: a confirmed delete
// of the spoke's whole namespace on the hub (a database delete takes one path
// segment, which for a spoke namespace is the spoke). Batched, ordered by source_path (the UNIQUE index),
// so a spoke with millions of receipts does not hold the shared writer.
func (h *HubIndex) MarkSpokeRemoved(ctx context.Context, spokeID, reason string) error {
	now := time.Now().UTC()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		res, err := h.db.ExecContext(ctx,
			`UPDATE sync_received SET removed_at = ?, removed_reason = ?
			 WHERE id IN (SELECT id FROM sync_received WHERE spoke_id = ? AND removed_at IS NULL ORDER BY source_path LIMIT ?)`,
			now, reason, spokeID, removalChunk)
		if err != nil {
			return fmt.Errorf("edgesync: mark spoke %q removed: %w", spokeID, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n < removalChunk {
			return nil
		}
	}
}

// ForgetRemoved deletes a spoke's REMOVED receipts under one of its
// databases (source paths starting with "<database>/"), so the next
// reconcile or verify reports them missing and the spoke re-sends them: the explicit,
// backup-free recovery from a delete (#1186). Receipts that are only
// compacted are kept: their content still exists on the hub, and forgetting
// them would re-accept duplicates next to the compacted output (#611).
// Batched, ordered by source_path (the UNIQUE index). Returns how many receipts were forgotten.
func (h *HubIndex) ForgetRemoved(ctx context.Context, spokeID, database string) (int64, error) {
	lower := database + "/"
	upper := database + "0"
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		res, err := h.db.ExecContext(ctx,
			`DELETE FROM sync_received WHERE id IN (
			   SELECT id FROM sync_received
			   WHERE spoke_id = ? AND source_path >= ? AND source_path < ? AND removed_at IS NOT NULL
			   ORDER BY source_path LIMIT ?)`,
			spokeID, lower, upper, removalChunk)
		if err != nil {
			return total, fmt.Errorf("edgesync: forget removed receipts of %s/%s: %w", spokeID, database, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, err
		}
		total += n
		if n < removalChunk {
			return total, nil
		}
	}
}

// Forget removes a spoke's record for a path.
//
// Not called in production. A receipt whose file is gone is dropped instead
// by reconcile (confirmPresent, then ForgetBatch), which checks every receipt
// that is neither compacted nor removed against storage before vouching for
// it. A vouched receipt (HeldFile.Vouched) is never checked.
//
// Which mark a hub-side path writes is the contract, so keep it:
//
//   - Content preserved elsewhere on the hub: MarkCompacted. Hub compaction of
//     spoke namespaces (#619), tiering's hot->cold migration (#687), and a
//     row-level DELETE that rewrites a file without the matching rows, whose
//     survivors live in the new file (#1666). Do not "fix" these to Forget:
//     the spoke would re-send and duplicate rows.
//   - Content removed on purpose: MarkRemoved / MarkSpokeRemoved (#1186). A
//     confirmed database delete of a spoke namespace, retention, hot and
//     cold, and a row-level DELETE whose rows all match (row_delete), in every
//     configuration. Reconcile keeps answering "present", so
//     the delete sticks; the explicit recovery is ForgetRemoved, behind
//     POST /api/v1/sync-spokes/:spokeID/forget.
//
// A narrow race remains: a receive that looked its receipt up before a
// database delete marked the spoke, and records after it, leaves an unmarked
// receipt over a deleted file, which reconcile then forgets. The spoke
// re-sends that one file. Re-running the delete removes it again.
//
// On a cluster hub these marks run on the node that performs the operation
// (the writer), while receipts live on the node the spoke syncs to. There the
// cluster manifest stands in (#1667): before reconcile forgets a receipt whose
// file is gone, it asks the manifest, and a file the manifest no longer lists
// is marked removed (manifest_removed) instead. See ClusterManifest.
//
// Phase 3a (delete_after_sync) settles what a removed receipt means for the
// spoke's own copy: it is kept. A spoke deletes only what a verify reconcile
// answers "held" (Reconciler.Verify), which a removed or compacted receipt
// never is.
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
