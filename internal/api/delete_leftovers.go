package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
)

// A row DELETE rewrites a file into a new one and then removes the old one.
// If that removal fails, the old file stays in storage next to its rewrite (a
// leftover): reads count the surviving rows twice. Nothing may read a
// leftover into a new file, or the double count is permanent: a retried
// DELETE rewrote it again (two rewrites holding the same rows), and on a
// shared object store registered that second rewrite.
//
// Every rewrite of a file is named after it (rewritePath), so the file and
// its rewrites share a family (storage.RewriteFamily). Each member is the
// original file minus rows some DELETE matched, so removing a member while
// another remains never drops a row nobody deleted: at worst deleted rows
// come back, and the DELETE that reads the kept member matches them again.
//
// A DELETE scan leaves leftovers out (leftoverFiles):
//   - Standalone: a file the retired journal names, and a family member with
//     more rows than the family's smallest one (an older member holds every
//     row of a newer one). The DELETE removes them under its lock
//     (removeLeftovers), and the drainer removes journaled ones even when no
//     DELETE runs.
//   - Cluster: a file the manifest does not list and the outbox does not
//     hold, while another member of its family is listed or held. It is only
//     left out: whether it is a leftover or a rewrite still being committed
//     is the manifest's to say, and a staged commit (the DELETE rewrite as a
//     staged compaction) is what removes it durably.

// deleteRetiredRoot holds the standalone retired journal: one object per
// source a DELETE replaced and could not remove yet. A reserved root
// (storage.IsReservedRootDir), so no database walker reads it.
const deleteRetiredRoot = "_delete_retired"

// retiredRecordVersion is written on every record.
const retiredRecordVersion = 1

// retiredDrainEvery spaces the standalone drainer's passes.
var retiredDrainEvery = time.Minute

// retiredRecord names a file a DELETE replaced (ReplacedBy was in storage
// when it was written) that is still to be removed.
type retiredRecord struct {
	Version    int       `json:"v"`
	Path       string    `json:"path"`
	ReplacedBy string    `json:"replaced_by"`
	At         time.Time `json:"at"`
}

func retiredKey(path string) string {
	sum := sha256.Sum256([]byte(path))
	return deleteRetiredRoot + "/" + hex.EncodeToString(sum[:16]) + ".json"
}

// journalRetired records that old is replaced by new and still to be
// removed. Written after new is in storage, before old's removal.
func (h *DeleteHandler) journalRetired(ctx context.Context, old, new string) error {
	data, err := json.Marshal(retiredRecord{Version: retiredRecordVersion, Path: old, ReplacedBy: new, At: time.Now().UTC()})
	if err != nil {
		return err
	}
	return h.storage.Write(ctx, retiredKey(old), data)
}

// clearRetired drops old's record once old is gone. A record left behind
// costs the drainer one removal of a missing file.
func (h *DeleteHandler) clearRetired(ctx context.Context, old string) {
	if err := h.storage.Delete(ctx, retiredKey(old)); err != nil && !storage.IsNotFound(err) {
		h.logger.Warn().Err(err).Str("file", old).Msg("Could not clear the retired record of a removed file; the drainer clears it")
	}
}

// retiredRecords reads the journal. A record that does not parse, or names
// no data file, is skipped (and kept, for an operator to read).
func (h *DeleteHandler) retiredRecords(ctx context.Context) ([]retiredRecord, error) {
	keys, err := h.storage.List(ctx, deleteRetiredRoot+"/")
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	var out []retiredRecord
	for _, k := range keys {
		if !strings.HasSuffix(k, ".json") {
			continue
		}
		data, err := h.storage.Read(ctx, k)
		if err != nil {
			if storage.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		var r retiredRecord
		if err := json.Unmarshal(data, &r); err != nil || !retiredPathValid(r.Path) || retiredKey(r.Path) != k {
			h.logger.Warn().Str("record", k).Msg("Ignoring a retired-file record that does not name a data file")
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

// retiredPathValid: a parquet file under a database, no traversal.
func retiredPathValid(p string) bool {
	if !strings.HasSuffix(p, ".parquet") || strings.HasPrefix(p, "/") || strings.Contains(p, "\\") {
		return false
	}
	parts := strings.Split(p, "/")
	if len(parts) < 3 || storage.IsReservedRootDir(parts[0]) {
		return false
	}
	for _, s := range parts {
		if s == "" || s == "." || s == ".." {
			return false
		}
	}
	return true
}

// rewriteSiblings is the coordinator's view of a file's rewrite family in
// the manifest.
type rewriteSiblings interface {
	HasRewriteSibling(path string) bool
}

// leftover is a scanned file a DELETE leaves out.
type leftover struct {
	path string
	// journaled: the retired journal names it. Otherwise keeper is the
	// family member that holds its rows (standalone) or is listed.
	journaled bool
	keeper    string
}

// leftoverFiles splits files into those a DELETE scans and the leftovers it
// leaves out (see the top of this file).
func (h *DeleteHandler) leftoverFiles(ctx context.Context, files []fileInfo) ([]fileInfo, []leftover) {
	if h.coordinator != nil {
		return h.clusterLeftovers(files)
	}
	return h.standaloneLeftovers(ctx, files)
}

func (h *DeleteHandler) clusterLeftovers(files []fileInfo) ([]fileInfo, []leftover) {
	holder, _ := h.pending.(registrationHolder)
	held := func(p string) bool { return holder != nil && holder.HoldsRegistration(p) }
	registered := func(p string) bool {
		_, ok := h.coordinator.GetFileEntry(p)
		return ok
	}
	siblings, _ := h.coordinator.(rewriteSiblings)
	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = f.relativePath
	}
	families := storage.RewriteFamilies(paths)
	kept := files[:0:0]
	var left []leftover
	for _, f := range files {
		p := f.relativePath
		if registered(p) || held(p) {
			kept = append(kept, f)
			continue
		}
		keeper := ""
		for _, m := range families[storage.RewriteFamily(p)] {
			if m != p && held(m) {
				keeper = m
				break
			}
		}
		if keeper == "" && siblings != nil && siblings.HasRewriteSibling(p) {
			keeper = storage.RewriteFamily(p)
		}
		if keeper == "" {
			kept = append(kept, f)
			continue
		}
		left = append(left, leftover{path: p, keeper: keeper})
	}
	return kept, left
}

func (h *DeleteHandler) standaloneLeftovers(ctx context.Context, files []fileInfo) ([]fileInfo, []leftover) {
	retired := map[string]bool{}
	if recs, err := h.retiredRecords(ctx); err != nil {
		h.logger.Warn().Err(err).Msg("Could not read the retired-file journal; leftovers are found by their rewrite family only")
	} else {
		for _, r := range recs {
			retired[r.Path] = true
		}
	}
	byPath := make(map[string]fileInfo, len(files))
	var paths []string
	var left []leftover
	for _, f := range files {
		if retired[f.relativePath] {
			left = append(left, leftover{path: f.relativePath, journaled: true})
			continue
		}
		byPath[f.relativePath] = f
		paths = append(paths, f.relativePath)
	}
	families := storage.RewriteFamilies(paths)
	if len(families) > 0 {
		var members []fileInfo
		for _, ms := range families {
			for _, m := range ms {
				members = append(members, byPath[m])
			}
		}
		rows, err := h.rowCounts(ctx, members)
		if err != nil {
			h.logger.Warn().Err(err).Msg("Could not count the rows of files with more than one rewrite in storage; scanning them all")
			families = nil
		}
		for family, ms := range families {
			sort.Strings(ms)
			least, keeper, tied := int64(-1), "", false
			for _, m := range ms {
				n, ok := rows[m]
				if !ok {
					least = -2 // a member not counted: leave the family as it is
					break
				}
				switch {
				case least < 0 || n < least:
					least, keeper, tied = n, m, false
				case n == least:
					tied = true
				}
			}
			if least == -2 {
				continue
			}
			if tied {
				h.logger.Warn().Str("family", family).Strs("files", ms).
					Msg("Rewrites of one file hold the same number of rows; none is known to be the current one, so all are scanned")
			}
			for _, m := range ms {
				if rows[m] > least {
					left = append(left, leftover{path: m, keeper: keeper})
					delete(byPath, m)
				}
			}
		}
	}
	kept := files[:0:0]
	for _, f := range files {
		if _, ok := byPath[f.relativePath]; ok {
			kept = append(kept, f)
		}
	}
	return kept, left
}

// rowCounts reads each file's row count from its footer.
func (h *DeleteHandler) rowCounts(ctx context.Context, files []fileInfo) (map[string]int64, error) {
	var list strings.Builder
	list.WriteString("[")
	byQuery := make(map[string]string, len(files))
	for i, f := range files {
		if i > 0 {
			list.WriteString(", ")
		}
		list.WriteString(sqlutil.QuoteStringLiteral(f.queryPath))
		byQuery[f.queryPath] = f.relativePath
	}
	list.WriteString("]")
	rows, err := h.db.DB().QueryContext(ctx, fmt.Sprintf("SELECT file_name, SUM(num_rows) FROM parquet_file_metadata(%s) GROUP BY file_name", list.String()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int64, len(files))
	for rows.Next() {
		var name string
		var n int64
		if err := rows.Scan(&name, &n); err != nil {
			return nil, err
		}
		rel, ok := byQuery[name]
		if !ok {
			for q, r := range byQuery {
				if strings.HasSuffix(name, filepath.Base(q)) && filepath.Base(name) == filepath.Base(q) {
					rel, ok = r, true
					break
				}
			}
		}
		if ok {
			out[rel] = n
		}
	}
	return out, rows.Err()
}

// removeLeftovers removes the standalone leftovers of database/measurement,
// classified again from a fresh listing. The caller holds rewriteMu (no
// rewrite runs meanwhile) and is not a dry run. A removal that fails leaves
// the file out of this DELETE's scan anyway.
func (h *DeleteHandler) removeLeftovers(ctx context.Context, database, measurement string) int {
	if h.coordinator != nil {
		return 0
	}
	files, err := h.listParquet(ctx, database, measurement)
	if err != nil {
		h.logger.Warn().Err(err).Msg("Could not list files to remove leftovers of earlier DELETEs")
		return 0
	}
	_, left := h.standaloneLeftovers(ctx, files)
	removed := 0
	for _, l := range left {
		if !l.journaled {
			// Never remove the last member: the keeper must still be here.
			if size, err := h.storage.StatFile(ctx, l.keeper); err != nil || size < 0 {
				continue
			}
		}
		if err := storage.DeleteWithRetry(ctx, h.storage, l.path); err != nil {
			h.logger.Warn().Err(err).Str("file", l.path).
				Msg("Could not remove a file an earlier DELETE replaced; it stays out of DELETEs, and queries read its rows twice until it is removed")
			continue
		}
		if l.journaled {
			h.clearRetired(ctx, l.path)
		}
		removed++
		h.logger.Warn().Str("file", l.path).Str("kept", l.keeper).Bool("journaled", l.journaled).
			Msg("Removed a file an earlier DELETE replaced and could not remove")
	}
	return removed
}

// listParquet lists the parquet files of database/measurement.
func (h *DeleteHandler) listParquet(ctx context.Context, database, measurement string) ([]fileInfo, error) {
	files, err := h.storage.List(ctx, fmt.Sprintf("%s/%s/", database, measurement))
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}
	var out []fileInfo
	for _, f := range files {
		if !strings.HasSuffix(strings.ToLower(f), ".parquet") {
			continue
		}
		queryPath, err := readParquetPath(h.storage, f)
		if err != nil {
			return nil, fmt.Errorf("listed file %q has no usable storage path: %w", f, err)
		}
		out = append(out, fileInfo{queryPath: queryPath, relativePath: f})
	}
	return out, nil
}

// DrainRetired removes the files the standalone retired journal names (each
// one replaced by a DELETE rewrite) and clears their records. It returns how
// many it removed.
func (h *DeleteHandler) DrainRetired(ctx context.Context) int {
	if h.coordinator != nil {
		return 0
	}
	h.rewriteMu.Lock()
	defer h.rewriteMu.Unlock()
	recs, err := h.retiredRecords(ctx)
	if err != nil {
		h.logger.Warn().Err(err).Msg("Could not read the retired-file journal")
		return 0
	}
	removed := 0
	for _, r := range recs {
		if err := storage.DeleteWithRetry(ctx, h.storage, r.Path); err != nil {
			h.logger.Warn().Err(err).Str("file", r.Path).Str("replaced_by", r.ReplacedBy).
				Msg("Still cannot remove a file a DELETE replaced; queries read its rows twice until it is removed")
			continue
		}
		h.clearRetired(ctx, r.Path)
		removed++
		h.logger.Info().Str("file", r.Path).Str("replaced_by", r.ReplacedBy).
			Msg("Removed a file a DELETE replaced and could not remove at the time")
	}
	if removed > 0 {
		h.filesRemoved()
	}
	return removed
}

// StartRetiredDrainer runs DrainRetired now and every retiredDrainEvery
// until ctx ends. Standalone only (a cluster's DELETE does not journal).
func (h *DeleteHandler) StartRetiredDrainer(ctx context.Context) {
	if h.coordinator != nil {
		return
	}
	go func() {
		t := time.NewTicker(retiredDrainEvery)
		defer t.Stop()
		for {
			h.DrainRetired(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}
