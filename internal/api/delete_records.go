package api

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/basekick-labs/arc/internal/cluster"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
)

// DELETE records (#1207 d1): with stream takeover on, a DELETE is recorded in
// the cluster before it runs, and rows acknowledged before it that are not
// in a file yet (in a standby's stream store, a writer's WAL or outbox) are
// rewritten without the matching rows before they register.

// DeleteFlusher flushes the ingest buffers of a database so a DELETE sees
// every row acknowledged before it in a file.
type DeleteFlusher interface {
	// FlushScopeAndWait returns, per buffer key, the last batch it handed
	// to the flush: the marks a DELETE record carries.
	FlushScopeAndWait(ctx context.Context, database, measurement string) (map[string]uint64, error)
	HasFlushFailure() bool
}

// SetFlusher installs the ingest buffer (nil on nodes that do not ingest).
func (h *DeleteHandler) SetFlusher(f DeleteFlusher) { h.flusher = f }

// deleteRecorder is the coordinator's side of DELETE records.
type deleteRecorder interface {
	DeleteRecordsActive() bool
	RecordDelete(ctx context.Context, database, measurement, where string, marks map[string]uint64) (uint64, error)
	WaitLocalFiles(ctx context.Context, database, measurement string, budget time.Duration) ([]string, error)
}

func recorderOf(coord any) deleteRecorder {
	r, ok := coord.(deleteRecorder)
	if !ok || !r.DeleteRecordsActive() {
		return nil
	}
	return r
}

// deleteLocalWait bounds how long a DELETE waits for files the manifest lists
// to reach this node before it records or runs.
var deleteLocalWait = 30 * time.Second

// deleteNow is the clock a DELETE fixes its time functions to (a test seam).
var deleteNow = time.Now

// errFlushBeforeDelete: the rows acknowledged before the DELETE could not be
// flushed, so it would not see them.
var errFlushBeforeDelete = errors.New("buffered rows could not be flushed before the delete")

// flushBeforeDelete flushes the database's buffered rows (measurement "" =
// all of them), so a DELETE removes rows acknowledged before it that were
// not in a file yet. Refused while a flush failure is outstanding: those
// rows are only in the WAL, and its recovery would bring them back.
func (h *DeleteHandler) flushBeforeDelete(ctx context.Context, database, measurement string) (map[string]uint64, error) {
	return flushBeforeDelete(ctx, h.flusher, database, measurement)
}

func flushBeforeDelete(ctx context.Context, f DeleteFlusher, database, measurement string) (map[string]uint64, error) {
	if f == nil {
		return nil, nil
	}
	if f.HasFlushFailure() {
		return nil, fmt.Errorf("%w: an earlier flush failed and its rows wait for WAL recovery", errFlushBeforeDelete)
	}
	marks, err := f.FlushScopeAndWait(ctx, database, measurement)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errFlushBeforeDelete, err)
	}
	return marks, nil
}

// Time functions DuckDB evaluates per statement. A DELETE record is applied
// again later (to rows replayed after a failover), so each is fixed to the
// DELETE's own time first, in UTC (the engine's time zone).
var (
	pinTimestampTZ = regexp.MustCompile(`(?i)\b(?:(?:now|get_current_timestamp|transaction_timestamp)\s*\(\s*\)|current_timestamp\b(?:\s*\(\s*\))?)`)
	pinTimestamp   = regexp.MustCompile(`(?i)\b(?:current_)?localtimestamp\b(?:\s*\(\s*\))?`)
	pinDate        = regexp.MustCompile(`(?i)\b(?:today\s*\(\s*\)|current_date\b(?:\s*\(\s*\))?)`)
	pinTime        = regexp.MustCompile(`(?i)\b(?:current_time|(?:current_)?localtime)\b(?:\s*\(\s*\))?`)
	// Functions whose value no later evaluation can reproduce.
	volatileFunction = regexp.MustCompile(`(?i)\b(random|uuid|uuidv4|uuidv7|gen_random_uuid|setseed|nextval|currval)\s*\(`)
)

// pinDeleteTime returns where with every per-statement time function
// replaced by a literal of now, and refuses functions with a value no later
// evaluation reproduces. String literals and quoted identifiers are left
// alone.
func pinDeleteTime(where string, now time.Time) (string, error) {
	in := backticksToDoubleQuotes(where)
	masked, masks := sqlutil.MaskStringLiterals(in, sqlutil.HasQuotes(in))
	if m := volatileFunction.FindStringSubmatch(masked); m != nil {
		return "", fmt.Errorf("WHERE clause may not use %s() while stream takeover is on: a DELETE is applied again to rows replayed after a failover, and must match the same rows", strings.ToLower(m[1]))
	}
	now = now.UTC()
	out := pinTimestampTZ.ReplaceAllLiteralString(masked, "TIMESTAMPTZ '"+now.Format("2006-01-02 15:04:05.999999")+"+00'")
	out = pinTimestamp.ReplaceAllLiteralString(out, "TIMESTAMP '"+now.Format("2006-01-02 15:04:05.999999")+"'")
	out = pinDate.ReplaceAllLiteralString(out, "DATE '"+now.Format("2006-01-02")+"'")
	out = pinTime.ReplaceAllLiteralString(out, "TIME '"+now.Format("15:04:05.999999")+"'")
	if out == masked {
		return where, nil
	}
	for i := len(masks) - 1; i >= 0; i-- {
		out = strings.ReplaceAll(out, masks[i].Placeholder, masks[i].Original)
	}
	return out, nil
}

// RewriteForRecords applies DELETE records to a pending local file (the
// outbox's cluster.DeleteRewriter): the rows any of wheres matches are
// removed, exactly as the DELETE removes them from the files it rewrites.
// A WHERE the file cannot bind (a column it lacks) matches nothing in it, as
// a DELETE skips such a file.
func (h *DeleteHandler) RewriteForRecords(ctx context.Context, relativePath string, wheres []string) (cluster.DeleteRewriteResult, error) {
	if !h.rewriteMu.TryLock() {
		return cluster.DeleteRewriteResult{}, cluster.ErrDeleteRewriteBusy
	}
	defer h.rewriteMu.Unlock()
	if h.isRemoteBackend() {
		return cluster.DeleteRewriteResult{}, errors.New("delete records apply to local storage only")
	}
	queryPath, err := readParquetPath(h.storage, relativePath)
	if err != nil {
		return cluster.DeleteRewriteResult{}, err
	}
	// Each record on its own, as each DELETE ran: one whose WHERE this file
	// cannot bind (a column it lacks) matches nothing here, and must not
	// keep the others from applying.
	source := sqlutil.ReadParquet(sqlutil.QuoteStringLiteral(queryPath))
	var bound []string
	for _, w := range wheres {
		if strings.TrimSpace(w) == "" {
			bound = append(bound, w)
			continue
		}
		var n int64
		err := h.db.DB().QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s`, source, w)).Scan(&n)
		var de *duckdb.Error
		switch {
		case err == nil:
			if n > 0 {
				bound = append(bound, w)
			}
		case errors.As(err, &de) && de.Type == duckdb.ErrorTypeBinder:
		default:
			return cluster.DeleteRewriteResult{}, fmt.Errorf("count rows matching a DELETE record: %w", err)
		}
	}
	var before, after int64
	if len(bound) == 0 {
		if err := h.db.DB().QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, source)).Scan(&before); err != nil {
			return cluster.DeleteRewriteResult{}, fmt.Errorf("count rows: %w", err)
		}
		return cluster.DeleteRewriteResult{Left: before}, nil
	}
	where := cluster.CombineDeleteWheres(bound)
	count := fmt.Sprintf(`SELECT COUNT(*), COUNT(*) FILTER (WHERE NOT (%s)) FROM %s`, where, source)
	if err := h.db.DB().QueryRowContext(ctx, count).Scan(&before, &after); err != nil {
		return cluster.DeleteRewriteResult{}, fmt.Errorf("count rows matching DELETE records: %w", err)
	}
	res := cluster.DeleteRewriteResult{Matched: before - after, Left: after}
	if res.Matched == 0 || after == 0 {
		return res, nil
	}
	_, newRel, err := h.rewriteLocalFile(ctx, queryPath, relativePath, where, before, after)
	if err != nil {
		return cluster.DeleteRewriteResult{}, err
	}
	size, sha, err := fileMetadata(filepath.Join(filepath.Dir(queryPath), filepath.Base(newRel)))
	if err != nil {
		return cluster.DeleteRewriteResult{}, err
	}
	res.NewPath, res.Size, res.SHA256 = newRel, size, sha
	return res, nil
}

// waitLocalFiles waits for the files the manifest lists of the measurement to
// be on this node, where the DELETE scans. It returns the ones still missing
// when the caller allowed them (their rows are not deleted), or
// errUnreachableFiles.
func (h *DeleteHandler) waitLocalFiles(ctx context.Context, rec deleteRecorder, database, measurement string, allowUnreachable bool) (missing []string, err error) {
	missing, err = rec.WaitLocalFiles(ctx, database, measurement, deleteLocalWait)
	if err != nil {
		return nil, err
	}
	if len(missing) > 0 && !allowUnreachable {
		return missing, errUnreachableFiles
	}
	return missing, nil
}

var errUnreachableFiles = errors.New("files the cluster manifest lists are not on this node yet")

// deleteSeenStamper and deleteWithdrawer are the registration outbox's
// DELETE-record side (#1207 d1).
type deleteSeenStamper interface {
	SetDeleteSeen(path string, record uint64)
}

type deleteWithdrawer interface {
	WithdrawForDelete(ctx context.Context, path string, record uint64) (bool, cluster.ReplaceOutcome, error)
}
