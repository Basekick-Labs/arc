package api

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

// A row DELETE and the cold tier (#1350). Files tiering moved to cold storage
// keep their hot path, are no longer in the hot listing nor (on a cluster) in
// the manifest, and queries read them through the cold glob. A DELETE reaches
// them this far:
//
//   - a cold file whose rows ALL match is deleted, object first and then this
//     node's row, as retention deletes cold files (retention_cold.go);
//   - a cold file with only SOME matching rows is left as it is and reported
//     (207, cold_files): rewriting part of a cold file is not supported. Cold
//     storage has none of what makes a hot rewrite safe (the read exclusion of
//     the old file, the journal, the manifest swap), so every node would count
//     the kept rows twice while it ran.
//
// Candidates come from listing the cold prefix, not from tier rows: queries
// read every object under the cold glob once a node has any cold row for the
// measurement, so an object without a row here is still read.

// coldDeleteChunk bounds the files one counting query reads.
const coldDeleteChunk = 100

// coldPartialWhy says why a cold file with matching rows was left.
const coldPartialWhy = "are in cold storage and only some of their rows match; a DELETE removes a cold file only when every row in it matches, and does not rewrite part of one"

// coldHotCopyWhy says why a cold file with matching rows was left because its
// hot copy is still there.
const coldHotCopyWhy = "are in cold storage while a copy is still in hot storage and were left as they are; a retry handles them once the move completes (a copy a policy change left in cold storage stays until the hot-only cleanup removes it, or until the file is moved again under the new policy)"

// deleteColdTier is what a DELETE needs from tiering. *tiering.Manager.
type deleteColdTier interface {
	// ColdBackend is nil when this node has no usable cold tier.
	ColdBackend() storage.Backend
	// ColdConfigured: cold storage is enabled in this node's configuration,
	// whether or not its backend could be built.
	ColdConfigured() bool
	ColdFilesForMeasurement(ctx context.Context, database, measurement string) ([]tiering.FileMetadata, error)
	RetireColdRows(ctx context.Context, paths []string) (int, error)
	RevertColdRowToHot(ctx context.Context, path string) error
}

// coldFile is one cold object a DELETE counted.
type coldFile struct {
	path      string // storage key (the hot path the file had)
	queryPath string
	size      int64
	total     int64
	remaining int64 // rows the clause does not match (NULL included)
}

func (f coldFile) matched() int64 { return f.total - f.remaining }

// coldPass is what a DELETE found in cold storage for one measurement.
type coldPass struct {
	backend storage.Backend
	// whole: every row matches. Deleted by run (not in a dry run).
	whole []coldFile
	// partial: some rows match; left and reported.
	partial []coldFile
	// hotCopy: matching rows, but a hot copy of the file exists (a move not
	// finished, or a leftover): the hot side decides, the cold copy is left.
	hotCopy []coldFile
	// failed: objects whose rows could not be counted, or deleted.
	failed []string
	// missingRows: tier rows of this measurement whose object the listing
	// does not show (checked again before anything is done with them).
	missingRows []string
	// notExamined says why cold storage was not looked at, "" when it was or
	// when this node has no cold storage configured.
	notExamined string
	// After run:
	deleted     []string
	deletedRows int64
}

// wholeRows is the rows a run would delete from cold storage.
func (c *coldPass) wholeRows() int64 {
	if c == nil {
		return 0
	}
	var n int64
	for _, f := range c.whole {
		n += f.matched()
	}
	return n
}

// partialRows is the matching rows a DELETE leaves in cold storage.
func (c *coldPass) partialRows() int64 {
	if c == nil {
		return 0
	}
	var n int64
	for _, f := range c.partial {
		n += f.matched()
	}
	return n
}

// matters reports anything the response must carry.
func (c *coldPass) matters() bool {
	return c != nil && (len(c.whole) > 0 || len(c.partial) > 0 || len(c.hotCopy) > 0 ||
		len(c.failed) > 0 || len(c.missingRows) > 0 || c.notExamined != "")
}

// scanCold finds the cold files of database/measurement holding rows that
// match where. nil when this node has no cold storage configured. Errors that
// make cold storage unreadable are reported in notExamined, never as "no
// match": the caller answers 207 rather than 200 for them.
func (h *DeleteHandler) scanCold(ctx context.Context, database, measurement, where string) *coldPass {
	tier := h.coldTier
	if tier == nil || !tier.ColdConfigured() {
		return nil
	}
	c := &coldPass{backend: tier.ColdBackend()}
	if c.backend == nil {
		c.notExamined = "cold storage is enabled but not usable on this node"
		return c
	}
	lister, ok := c.backend.(storage.ObjectLister)
	if !ok {
		c.notExamined = "the cold storage backend cannot list its objects"
		return c
	}
	prefix := database + "/" + measurement + "/"
	objects, err := lister.ListObjects(ctx, prefix)
	if err != nil {
		c.notExamined = "listing cold storage failed: " + err.Error()
		return c
	}
	// The raw hot listing, not the scan set: a file still in hot storage is
	// the hot side's, whatever the scan made of it (#1350 review B2).
	hotList, err := h.storage.List(ctx, prefix)
	if err != nil {
		c.notExamined = "listing hot storage to compare failed: " + err.Error()
		return c
	}
	inHot := make(map[string]bool, len(hotList))
	for _, p := range hotList {
		inHot[p] = true
	}

	listed := make(map[string]bool, len(objects))
	var files []coldFile
	for _, o := range objects {
		if !strings.HasSuffix(strings.ToLower(o.Path), ".parquet") {
			continue
		}
		if first, _, _ := strings.Cut(o.Path, "/"); storage.IsReservedRootDir(first) {
			continue
		}
		listed[o.Path] = true
		qp, err := readParquetPath(c.backend, o.Path)
		if err != nil {
			c.failed = append(c.failed, o.Path)
			continue
		}
		files = append(files, coldFile{path: o.Path, queryPath: qp, size: o.Size})
	}

	counted, failed := h.countCold(ctx, files, where)
	c.failed = append(c.failed, failed...)
	for _, f := range counted {
		if f.matched() <= 0 {
			continue
		}
		hot := inHot[f.path]
		if !hot && h.coordinator != nil {
			// A manifest entry is a hot file the cluster still lists, here
			// or on a peer that may pull it back.
			if e, ok := h.coordinator.GetFileEntry(f.path); ok && e != nil {
				hot = raft.HotTier(e.Tier)
			}
		}
		switch {
		case hot:
			c.hotCopy = append(c.hotCopy, f)
		case f.remaining == 0:
			c.whole = append(c.whole, f)
		default:
			c.partial = append(c.partial, f)
		}
	}

	// Rows whose object is gone (#1350 review M2): cold sync never retires
	// them, so a DELETE that looked would otherwise stat them on every run.
	rows, err := tier.ColdFilesForMeasurement(ctx, database, measurement)
	if err != nil {
		h.logger.Debug().Err(err).Msg("Could not read cold rows to find ones whose object is gone")
	}
	for _, r := range rows {
		if !listed[r.Path] {
			c.missingRows = append(c.missingRows, r.Path)
		}
	}
	return c
}

// countCold counts each file's rows and the rows where does not match, in
// chunks with union_by_name (a file older than a column the clause names
// reads it as NULL when another file in the chunk has it, so its rows are not
// matched). A chunk that fails is counted file by file: a file the clause
// cannot bind (none of the chunk had the column) matches nothing; one that
// fails otherwise is returned in failed — never skipped as "no match".
func (h *DeleteHandler) countCold(ctx context.Context, files []coldFile, where string) ([]coldFile, []string) {
	var out []coldFile
	var failed []string
	for start := 0; start < len(files); start += coldDeleteChunk {
		chunk := files[start:min(start+coldDeleteChunk, len(files))]
		got, err := h.countColdChunk(ctx, chunk, where)
		if err == nil {
			out = append(out, got...)
			continue
		}
		h.logger.Warn().Err(err).Int("files", len(chunk)).Msg("Counting cold files in one query failed; counting them one by one")
		for _, f := range chunk {
			one, err := h.countColdChunk(ctx, []coldFile{f}, where)
			var de *duckdb.Error
			if errors.As(err, &de) && de.Type == duckdb.ErrorTypeBinder {
				// The clause names a column this file does not have (an
				// older file): it matches nothing here, as a DELETE record
				// that cannot bind does (delete_records.go). Not a failure,
				// or every DELETE naming a newer column would answer 207.
				continue
			}
			if err != nil {
				h.logger.Warn().Err(err).Str("file", f.path).Msg("Could not count the rows of a cold file; it is reported as failed")
				failed = append(failed, f.path)
				continue
			}
			out = append(out, one...)
		}
	}
	return out, failed
}

func (h *DeleteHandler) countColdChunk(ctx context.Context, files []coldFile, where string) ([]coldFile, error) {
	if len(files) == 0 {
		return nil, nil
	}
	var list strings.Builder
	list.WriteString("[")
	byQuery := make(map[string]int, len(files))
	for i, f := range files {
		if i > 0 {
			list.WriteString(", ")
		}
		list.WriteString(sqlutil.QuoteStringLiteral(f.queryPath))
		byQuery[f.queryPath] = i
	}
	list.WriteString("]")
	// The same shape as the hot rewrite's count: a row the clause cannot
	// decide is not matched, so it is kept.
	query := fmt.Sprintf(`
		SELECT filename, COUNT(*) AS total, COUNT(*) FILTER (WHERE NOT COALESCE((%s), false)) AS remaining
		FROM %s
		GROUP BY filename`,
		where, sqlutil.ReadParquet(list.String(), "filename=true", "union_by_name=true"))
	rows, err := h.db.DB().QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]coldFile, 0, len(files))
	for rows.Next() {
		var name string
		var total, remaining int64
		if err := rows.Scan(&name, &total, &remaining); err != nil {
			return nil, err
		}
		i, ok := byQuery[name]
		if !ok {
			// DuckDB may hand back another spelling of the path.
			for qp, j := range byQuery {
				if strings.HasSuffix(name, filepath.Base(qp)) {
					i, ok = j, true
					break
				}
			}
		}
		if !ok {
			return nil, fmt.Errorf("cold count returned an unknown file %q", name)
		}
		f := files[i]
		f.total, f.remaining = total, remaining
		out = append(out, f)
	}
	return out, rows.Err()
}

// run deletes the cold files whose rows all match, under the caller's
// rewriteMu. Each is counted again and checked at its listed size first (the
// count and the delete are not atomic, and an object at the same key could
// have been replaced, by a restore say): one that no longer matches whole is
// reported instead. Object first, then the row: a failure in between leaves a
// row for a missing object, which the next DELETE or retention run retires.
func (h *DeleteHandler) runCold(ctx context.Context, c *coldPass, where string) {
	if c == nil || c.backend == nil {
		return
	}
	var retire []string
	for _, f := range c.whole {
		if ctx.Err() != nil {
			c.failed = append(c.failed, f.path)
			continue
		}
		size, err := c.backend.StatFile(ctx, f.path)
		switch {
		case err != nil:
			c.failed = append(c.failed, f.path)
			continue
		case size < 0:
			// Gone already (another DELETE, retention): not this run's rows.
			retire = append(retire, f.path)
			continue
		case size != f.size:
			h.logger.Warn().Str("file", f.path).Int64("listed", f.size).Int64("now", size).
				Msg("Cold file changed between the count and the delete; left as it is")
			c.failed = append(c.failed, f.path)
			continue
		}
		again, err := h.countColdChunk(ctx, []coldFile{f}, where)
		if err != nil || len(again) != 1 {
			c.failed = append(c.failed, f.path)
			continue
		}
		if g := again[0]; g.remaining != 0 || g.total == 0 {
			if g.matched() > 0 {
				c.partial = append(c.partial, g)
			}
			continue
		}
		if err := c.backend.Delete(ctx, f.path); err != nil {
			// A delete can fail after the object went (a timed-out response).
			if after, serr := c.backend.StatFile(ctx, f.path); serr != nil || after >= 0 {
				h.logger.Error().Err(err).Str("file", f.path).Msg("Failed to delete a cold file whose rows all match")
				c.failed = append(c.failed, f.path)
				continue
			}
		}
		h.logger.Info().Str("file", f.path).Int64("rows", f.total).Msg("Deleted a cold file whose rows all match the DELETE")
		c.deleted = append(c.deleted, f.path)
		c.deletedRows += f.total
		retire = append(retire, f.path)
	}
	// Rows whose object the listing did not show: checked again now, as
	// retention checks at the moment it acts (#1350 review H1). A migration
	// may have copied the object since the listing; its row must stay. Then
	// pointed back at a hot copy that still exists, or retired.
	for _, p := range c.missingRows {
		if ctx.Err() != nil {
			break
		}
		coldSize, err := c.backend.StatFile(ctx, p)
		if err != nil || coldSize >= 0 {
			continue
		}
		hotSize, err := h.storage.StatFile(ctx, p)
		switch {
		case err != nil:
			continue
		case hotSize >= 0:
			if err := h.coldTier.RevertColdRowToHot(context.WithoutCancel(ctx), p); err != nil {
				h.logger.Warn().Err(err).Str("file", p).Msg("Could not point a cold row whose object is gone back at its hot copy")
			}
		default:
			retire = append(retire, p)
		}
	}
	if len(retire) > 0 {
		// Detached: the objects are gone whatever happens to the request.
		if n, err := h.coldTier.RetireColdRows(context.WithoutCancel(ctx), retire); err != nil {
			h.logger.Warn().Err(err).Int("removed", n).Int("rows", len(retire)).
				Msg("Failed to retire cold tier rows; the next DELETE or retention run retires them")
		}
	}
}

// coldBases is the base names of files, sorted.
func coldBases(files []coldFile) []string {
	out := make([]string, 0, len(files))
	for _, f := range files {
		out = append(out, filepath.Base(f.path))
	}
	sort.Strings(out)
	return out
}

func baseNames(paths []string) []string {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		out = append(out, filepath.Base(p))
	}
	sort.Strings(out)
	return out
}

// applyCold adds what the cold pass did (or, in a dry run, would do) to a
// response, and reports whether it must be a 207: a matching row that stays
// in cold storage, or cold storage that could not be looked at. Partial rows
// are never in deleted_count.
func (c *coldPass) applyCold(resp *DeleteResponse, dryRun bool) bool {
	if c == nil {
		return false
	}
	if dryRun {
		resp.DeletedCount += c.wholeRows()
		resp.ColdDeletedFiles = coldBases(c.whole)
	} else {
		resp.DeletedCount += c.deletedRows
		resp.ColdDeletedFiles = baseNames(c.deleted)
	}
	// A file with a hot copy is the hot side's: already counted (and, when
	// refused, reported) there.
	resp.AffectedFiles += len(resp.ColdDeletedFiles) + len(c.partial)
	resp.ColdFiles = coldBases(c.partial)
	resp.ColdRowsNotDeleted = c.partialRows()
	resp.ColdNotExamined = c.notExamined

	var why []string
	if n := len(c.partial); n > 0 {
		why = append(why, fmt.Sprintf("%d rows in %d files %s", c.partialRows(), n, coldPartialWhy))
	}
	if n := len(c.hotCopy); n > 0 {
		why = append(why, fmt.Sprintf("%d cold files %s", n, coldHotCopyWhy))
	}
	if n := len(c.failed); n > 0 {
		why = append(why, fmt.Sprintf("%d cold files could not be read or deleted and were left as they are", n))
	}
	if c.notExamined != "" {
		why = append(why, "cold storage was not examined: "+c.notExamined+"; rows in it may still match")
	}
	if len(why) == 0 {
		return false
	}
	report := !dryRun || c.notExamined != "" || len(c.failed) > 0
	if !report {
		// A dry run's partial and hot-copy matches are in cold_files and
		// cold_rows_not_deleted; nothing failed.
		return false
	}
	if !dryRun {
		resp.FailedFiles = append(resp.FailedFiles, coldBases(c.partial)...)
		for _, b := range coldBases(c.hotCopy) {
			if !slices.Contains(resp.FailedFiles, b) {
				resp.FailedFiles = append(resp.FailedFiles, b)
			}
		}
		resp.FailedFiles = append(resp.FailedFiles, baseNames(c.failed)...)
	}
	resp.Success = false
	msg := "Cold storage: " + strings.Join(why, "; ")
	if resp.Error != "" {
		resp.Error += "; " + msg
	} else {
		resp.Error = msg
	}
	return true
}

var _ deleteColdTier = (*tiering.Manager)(nil)
