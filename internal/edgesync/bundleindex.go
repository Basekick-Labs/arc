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

// ErrBundleAlreadyImported means this bundle's contents are already on the hub.
var ErrBundleAlreadyImported = errors.New("edgesync: bundle already imported")

// BundleIndex records which bundles a hub has imported.
//
// This is the replay protection for the air-gap transport. The online families
// bind a nonce and a 5-minute timestamp window, which works because a request
// is in flight and the network guarantees "recent". A bundle legitimately sits
// on a drive for weeks, so replay protection has to be durable state rather
// than a measurement — a record that survives a restart, as a nonce cache does
// not.
type BundleIndex struct {
	db     *sql.DB
	logger zerolog.Logger
}

// NewBundleIndex creates the table if needed and returns a ready index.
func NewBundleIndex(db *sql.DB, logger zerolog.Logger) (*BundleIndex, error) {
	if db == nil {
		return nil, errors.New("edgesync: bundle index requires a database")
	}
	i := &BundleIndex{
		db:     db,
		logger: logger.With().Str("component", "edgesync-bundleindex").Logger(),
	}
	if err := i.initSchema(); err != nil {
		return nil, err
	}
	return i, nil
}

func (i *BundleIndex) initSchema() error {
	const schema = `
	-- One row per imported bundle.
	--
	-- Keyed (spoke_id, bundle_id), NOT bundle_id alone. The ID is chosen by
	-- whoever holds a spoke's secret, so a global key would let a compromised
	-- spoke burn IDs in another spoke's namespace — permanently 409-ing a
	-- legitimate future bundle with no operator-visible cause. Namespacing
	-- confines that to the compromised spoke, the same reasoning that makes
	-- received paths spoke-namespaced.
	CREATE TABLE IF NOT EXISTS sync_imported_bundles (
		spoke_id     TEXT NOT NULL,
		bundle_id    TEXT NOT NULL,
		created_at   TIMESTAMP NOT NULL,
		imported_at  TIMESTAMP NOT NULL,
		file_count   INTEGER NOT NULL DEFAULT 0,
		bytes_total  INTEGER NOT NULL DEFAULT 0,
		conflicts    INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (spoke_id, bundle_id)
	);

	-- "What has this spoke sent me lately", the operator's question.
	CREATE INDEX IF NOT EXISTS idx_sync_imported_bundles_spoke
		ON sync_imported_bundles(spoke_id, imported_at DESC);
	`

	if _, err := i.db.Exec(schema); err != nil {
		return fmt.Errorf("create sync_imported_bundles: %w", err)
	}
	i.logger.Info().Msg("Bundle index schema initialized")
	return nil
}

// ImportedBundle is one row of the dedup ledger.
type ImportedBundle struct {
	SpokeID    string
	BundleID   string
	CreatedAt  time.Time
	ImportedAt time.Time
	FileCount  int64
	BytesTotal int64
	Conflicts  int64
}

// Seen reports whether this spoke's bundle has already been imported.
func (i *BundleIndex) Seen(ctx context.Context, spokeID, bundleID string) (*ImportedBundle, error) {
	row := i.db.QueryRowContext(ctx, `
		SELECT spoke_id, bundle_id, created_at, imported_at, file_count, bytes_total, conflicts
		FROM sync_imported_bundles
		WHERE spoke_id = ? AND bundle_id = ?`, spokeID, bundleID)

	var b ImportedBundle
	err := row.Scan(&b.SpokeID, &b.BundleID, &b.CreatedAt, &b.ImportedAt,
		&b.FileCount, &b.BytesTotal, &b.Conflicts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("edgesync: look up bundle %q: %w", bundleID, err)
	}
	b.CreatedAt = b.CreatedAt.UTC()
	b.ImportedAt = b.ImportedAt.UTC()
	return &b, nil
}

// Record marks a bundle imported.
//
// Written AFTER the import completes, so a run that died partway can be
// retried — re-importing is idempotent because every file the hub already
// holds resolves to already_present.
//
// Conflicts count as completion, not failure: they are a reported outcome
// needing a human, and refusing to record would mean re-importing every file
// to retry a handful — or, if the operator resolves them and re-imports,
// hitting a 409 on a bundle that was never actually recorded.
func (i *BundleIndex) Record(ctx context.Context, b *ImportedBundle) error {
	if b == nil {
		return errors.New("edgesync: record bundle: nil bundle")
	}
	// INSERT, not UPSERT: reaching here for an already-recorded bundle means
	// the dedup check was skipped, which is a bug worth surfacing rather than
	// silently overwriting an earlier import's provenance.
	_, err := i.db.ExecContext(ctx, `
		INSERT INTO sync_imported_bundles
			(spoke_id, bundle_id, created_at, imported_at, file_count, bytes_total, conflicts)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		b.SpokeID, b.BundleID, b.CreatedAt.UTC(), time.Now().UTC(),
		b.FileCount, b.BytesTotal, b.Conflicts)
	if err != nil {
		// A primary-key conflict means the row already exists — another import
		// of this same bundle recorded it. Distinguished because the two mean
		// opposite things to an operator: a generic failure means a re-import
		// will NOT be refused, whereas this means it WILL be.
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return fmt.Errorf("%w: %s was recorded concurrently", ErrBundleAlreadyImported, b.BundleID)
		}
		return fmt.Errorf("edgesync: record bundle %q: %w", b.BundleID, err)
	}
	return nil
}

// ListBySpoke returns a spoke's imported bundles, newest first.
func (i *BundleIndex) ListBySpoke(ctx context.Context, spokeID string, limit int) ([]*ImportedBundle, error) {
	query := `
		SELECT spoke_id, bundle_id, created_at, imported_at, file_count, bytes_total, conflicts
		FROM sync_imported_bundles
		WHERE spoke_id = ?
		ORDER BY imported_at DESC`
	args := []any{spokeID}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := i.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("edgesync: list bundles for %q: %w", spokeID, err)
	}
	defer rows.Close()

	var out []*ImportedBundle
	for rows.Next() {
		var b ImportedBundle
		if err := rows.Scan(&b.SpokeID, &b.BundleID, &b.CreatedAt, &b.ImportedAt,
			&b.FileCount, &b.BytesTotal, &b.Conflicts); err != nil {
			return nil, fmt.Errorf("edgesync: scan bundle row: %w", err)
		}
		b.CreatedAt = b.CreatedAt.UTC()
		b.ImportedAt = b.ImportedAt.UTC()
		out = append(out, &b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("edgesync: iterate bundles: %w", err)
	}
	return out, nil
}

// No cleanup job, deliberately. 200 spokes shipping weekly bundles for five
// years is ~52k rows — a table that never needs pruning. A batched DELETE here
// would be risk (holding the write lock that ingest file-registration shares)
// for no benefit.
