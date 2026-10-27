// Package credscrub removes credential material from error text that earlier
// Arc versions stored in the shared SQLite database.
package credscrub

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// column is one stored error-text column.
type column struct{ table, name string }

// columns are every column that has held storage error text. A table or
// column this node never created is skipped.
var columns = []column{
	{"tier_migrations", "error"},
	{"tier_files", "quarantine_reason"},
	{"retention_executions", "error_message"},
	{"retention_executions_dormant", "error_message"},
	{"continuous_query_executions", "error_message"},
	{"continuous_query_executions_dormant", "error_message"},
	{"mqtt_subscriptions", "error_message"},
	{"sync_ledger", "last_error"},
}

// batchSize bounds each UPDATE transaction, so the scrub never holds the
// write lock for long.
const batchSize = 1000

// candidates is the SQL filter for rows whose text may hold credential
// material; RedactCredentialText decides.
const candidates = `(%[1]s LIKE '%%sig=%%' OR %[1]s LIKE '%%SharedAccessSignature%%' OR %[1]s LIKE '%%AccountKey=%%' OR %[1]s LIKE '%%://%%?%%')`

// markerTable records the columns already scrubbed in this database, so the
// pass runs once per column: error text written by this version is clean at
// the source, and re-reading every old row at every start would delay
// readiness on a large history.
const markerTable = "arc_credential_scrub"

// Run rewrites stored error text through storage.RedactCredentialText, once
// per column per database. It returns the number of rows rewritten; an error
// stops the scrub of that column only, which is then retried at the next
// start.
func Run(ctx context.Context, db *sql.DB, logger zerolog.Logger) (int, error) {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+markerTable+` (col TEXT PRIMARY KEY, done_at TIMESTAMP)`); err != nil {
		return 0, err
	}
	total := 0
	var firstErr error
	for _, c := range columns {
		key := c.table + "." + c.name
		var done int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+markerTable+` WHERE col = ?`, key).Scan(&done); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if done > 0 {
			continue
		}
		ok, err := columnExists(ctx, db, c)
		if err != nil {
			logger.Warn().Err(err).Str("table", c.table).Str("column", c.name).
				Msg("Could not inspect a table holding stored error messages; it will be retried at the next start")
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if ok {
			n, err := scrubColumn(ctx, db, c)
			total += n
			if err != nil {
				logger.Warn().Err(err).Str("table", c.table).Str("column", c.name).
					Msg("Could not finish clearing credential text from stored errors; it will be retried at the next start")
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
		}
		// Absent columns are marked too: a table created by this version
		// only ever holds clean text.
		if _, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO `+markerTable+` (col, done_at) VALUES (?, ?)`, key, time.Now().UTC()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if total > 0 {
		logger.Info().Int("rows", total).Msg("Cleared credential text from stored error messages")
	}
	return total, firstErr
}

func columnExists(ctx context.Context, db *sql.DB, c column) (bool, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%q)", c.table))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == c.name {
			return true, nil
		}
	}
	return false, rows.Err()
}

// scrubColumn walks candidate rows in rowid order, one bounded transaction
// per batch.
func scrubColumn(ctx context.Context, db *sql.DB, c column) (int, error) {
	// Identifiers come from the columns table above, never from input.
	col := fmt.Sprintf("%q", c.name)
	sel := fmt.Sprintf(`SELECT rowid, %s FROM %q WHERE rowid > ? AND %s ORDER BY rowid LIMIT %d`,
		col, c.table, fmt.Sprintf(candidates, col), batchSize)
	// Only if the text is still what was read: a subsystem running alongside
	// (an MQTT subscription clearing its error, a retry recording a new one)
	// must not have its write replaced by a stale, scrubbed copy.
	upd := fmt.Sprintf(`UPDATE %q SET %s = ? WHERE rowid = ? AND %s = ?`, c.table, col, col)
	changed := 0
	var after int64
	for {
		if err := ctx.Err(); err != nil {
			return changed, err
		}
		type row struct {
			id   int64
			text string
		}
		rows, err := db.QueryContext(ctx, sel, after)
		if err != nil {
			return changed, err
		}
		var batch []row
		for rows.Next() {
			var r row
			var text sql.NullString
			if err := rows.Scan(&r.id, &text); err != nil {
				rows.Close()
				return changed, err
			}
			r.text = text.String
			batch = append(batch, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return changed, err
		}
		if len(batch) == 0 {
			return changed, nil
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return changed, err
		}
		for _, r := range batch {
			if clean := storage.RedactCredentialText(r.text); clean != r.text {
				if _, err := tx.ExecContext(ctx, upd, clean, r.id, r.text); err != nil {
					_ = tx.Rollback()
					return changed, err
				}
				changed++
			}
		}
		if err := tx.Commit(); err != nil {
			return changed, err
		}
		after = batch[len(batch)-1].id
		if len(batch) < batchSize {
			return changed, nil
		}
	}
}
