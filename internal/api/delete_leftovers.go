package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
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
//     hold, while another member of its family is listed or held, judged on
//     the manifest after catching up with the leader. With compaction commit
//     on, the DELETE removes one only when it is provably the older copy
//     (removeLeftovers); otherwise it is only left out.

// deleteRetiredRoot holds the standalone retired journal
// (storage.DeleteRetiredRoot).
const deleteRetiredRoot = storage.DeleteRetiredRoot

// retiredRecordVersion is written on every record.
const retiredRecordVersion = 1

// deleteReplaced removes a file a DELETE replaced (a variable so tests can
// make it fail).
var deleteReplaced = storage.DeleteWithRetry

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

func retiredKey(path string) string { return storage.RetiredKey(path) }

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
		if _, ok := storage.ParseRetired(k, data); !ok || json.Unmarshal(data, &r) != nil {
			h.logger.Warn().Str("record", k).Msg("Ignoring a retired-file record that does not name a data file")
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

// rewriteSiblings is the coordinator's view of a file's rewrite family in
// the manifest.
type rewriteSiblings interface {
	HasRewriteSibling(path string) bool
}

// manifestSyncer catches this node up with the cluster manifest.
type manifestSyncer interface {
	SyncManifest(ctx context.Context) error
}

// compactionOwner reports a path a staged compaction owns.
type compactionOwner interface {
	CompactionOwns(path string) bool
}

// leftover is a scanned file a DELETE leaves out.
type leftover struct {
	path string
	// journaled: the retired journal names it. Otherwise keeper is the
	// family member that holds its rows: in a cluster, a listed member in
	// the same listing ("" when only the manifest knows of one, or one is
	// held), which removal needs.
	journaled bool
	keeper    string
}

// leftoverFiles splits files into those a DELETE scans and the leftovers it
// leaves out (see the top of this file).
func (h *DeleteHandler) leftoverFiles(ctx context.Context, files []fileInfo) ([]fileInfo, []leftover, error) {
	if h.coordinator != nil {
		return h.clusterLeftovers(ctx, files)
	}
	return h.standaloneLeftovers(ctx, files)
}

// clusterLeftovers decides on the manifest as the leader has it: a node
// whose manifest lags would take the current file for a leftover (the
// rewrite registered elsewhere, not here yet) and the leftover for the
// current file. Without compaction commit a sync that fails leaves every
// file scanned, as before; with it the DELETE stops, as its scan would.
func (h *DeleteHandler) clusterLeftovers(ctx context.Context, files []fileInfo) ([]fileInfo, []leftover, error) {
	if s, ok := h.coordinator.(manifestSyncer); ok {
		if err := s.SyncManifest(ctx); err != nil {
			if casActive(h.coordinator) {
				return nil, nil, fmt.Errorf("%w: %v", errManifestNotSynced, err)
			}
			h.logger.Warn().Err(err).Msg("Could not catch up with the cluster manifest; files an earlier DELETE replaced are not told apart this time")
			return files, nil, nil
		}
	} else {
		return files, nil, nil
	}
	kept, left := h.clusterClassify(files, h.journaledPaths(ctx))
	return kept, left, nil
}

// journaledPaths reads the files the retired journal names, for the cluster
// classification. A journal that cannot be read names none (Warn).
func (h *DeleteHandler) journaledPaths(ctx context.Context) map[string]bool {
	recs, err := h.retiredRecords(ctx)
	if err != nil {
		h.logger.Warn().Err(err).Msg("Could not read the retired-file journal")
		return nil
	}
	out := make(map[string]bool, len(recs))
	for _, r := range recs {
		out[r.Path] = true
	}
	return out
}

// clusterClassify is clusterLeftovers on a manifest the caller just caught
// up (scanSet classifies right after its own sync).
//
// A file the retired journal names and the manifest does not list is a
// leftover even with no listed family member left (its replacement since
// compacted or tiered away): it is never scanned, so never adopted again.
func (h *DeleteHandler) clusterClassify(files []fileInfo, journaled map[string]bool) ([]fileInfo, []leftover) {
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
		keeper, isLeftover := "", false
		for _, m := range families[storage.RewriteFamily(p)] {
			if m == p {
				continue
			}
			if registered(m) {
				keeper, isLeftover = m, true
				break
			}
			if held(m) {
				isLeftover = true
			}
		}
		if !isLeftover && siblings != nil && siblings.HasRewriteSibling(p) {
			isLeftover = true
		}
		if !isLeftover && journaled[p] {
			isLeftover = true
		}
		if !isLeftover {
			kept = append(kept, f)
			continue
		}
		left = append(left, leftover{path: p, keeper: keeper})
	}
	return kept, left
}

func (h *DeleteHandler) standaloneLeftovers(ctx context.Context, files []fileInfo) ([]fileInfo, []leftover, error) {
	retired := map[string]retiredRecord{}
	if recs, err := h.retiredRecords(ctx); err != nil {
		h.logger.Warn().Err(err).Msg("Could not read the retired-file journal; leftovers are found by their rewrite family only")
	} else {
		for _, r := range recs {
			retired[r.Path] = r
		}
	}
	byPath := make(map[string]fileInfo, len(files))
	var paths []string
	var left []leftover
	for _, f := range files {
		// A record is only an intent: its file is a leftover once its
		// replacement is here (retiredAction).
		if rec, ok := retired[f.relativePath]; ok && h.replacedHere(ctx, rec) {
			left = append(left, leftover{path: f.relativePath, journaled: true})
			continue
		}
		byPath[f.relativePath] = f
		paths = append(paths, f.relativePath)
	}
	families := storage.RewriteFamilies(paths)
	names := make([]string, 0, len(families))
	for family := range families {
		names = append(names, family)
	}
	sort.Strings(names)
	for _, family := range names {
		ms := families[family]
		sort.Strings(ms)
		members := make([]fileInfo, len(ms))
		for i, m := range ms {
			members[i] = byPath[m]
		}
		// One family at a time: a footer that cannot be read stops the
		// DELETE (scanning that family whole is what made the leftover a
		// second copy), and costs no other family anything.
		rows, err := h.rowCounts(ctx, members)
		if err != nil {
			return nil, nil, fmt.Errorf("count the rows of %s and its rewrites: %w", family, err)
		}
		least, keeper, tied := int64(-1), "", false
		for _, m := range ms {
			n, ok := rows[m]
			if !ok {
				return nil, nil, fmt.Errorf("count the rows of %s: no count for %s", family, m)
			}
			switch {
			case least < 0 || n < least:
				least, keeper, tied = n, m, false
			case n == least:
				tied = true
			}
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
	kept := files[:0:0]
	for _, f := range files {
		if _, ok := byPath[f.relativePath]; ok {
			kept = append(kept, f)
		}
	}
	return kept, left, nil
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
				if filepath.Base(name) == filepath.Base(q) {
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

// removeLeftovers removes the leftovers of database/measurement, classified
// again from a fresh listing. The caller holds rewriteMu (no rewrite runs
// meanwhile), is the primary writer, and is not a dry run. A removal that
// fails leaves the file out of this DELETE's scan anyway.
//
// In a cluster only with compaction commit on (a synced, compare-and-set
// manifest), and only a leftover that is provably older than a listed
// member: that member is in storage at its registered size and holds
// strictly fewer rows. A rewrite another node is still committing has fewer
// rows than the file it replaces, so it is never the one removed.
func (h *DeleteHandler) removeLeftovers(ctx context.Context, database, measurement string) int {
	if h.coordinator != nil && !casActive(h.coordinator) {
		return 0
	}
	files, err := h.listParquet(ctx, database, measurement)
	if err != nil {
		h.logger.Warn().Err(err).Msg("Could not list files to remove leftovers of earlier DELETEs")
		return 0
	}
	_, left, err := h.leftoverFiles(ctx, files)
	if err != nil {
		h.logger.Warn().Err(err).Msg("Could not tell files an earlier DELETE replaced apart; none removed")
		return 0
	}
	if h.coordinator != nil {
		left = h.provablyOlder(ctx, files, left)
	}
	removed := 0
	for _, l := range left {
		if !l.journaled {
			// Never remove the last member: the keeper must still be here.
			if size, err := h.storage.StatFile(ctx, l.keeper); err != nil || size < 0 {
				continue
			}
		}
		if err := deleteReplaced(ctx, h.storage, l.path); err != nil {
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

// provablyOlder keeps the cluster leftovers removeLeftovers may remove (see
// there).
func (h *DeleteHandler) provablyOlder(ctx context.Context, files []fileInfo, left []leftover) []leftover {
	byPath := make(map[string]fileInfo, len(files))
	for _, f := range files {
		byPath[f.relativePath] = f
	}
	owner, _ := h.coordinator.(compactionOwner)
	holder, _ := h.pending.(registrationHolder)
	var out []leftover
	for _, l := range left {
		if l.keeper == "" || (owner != nil && owner.CompactionOwns(l.path)) || (holder != nil && holder.HoldsRegistration(l.path)) {
			continue
		}
		e, ok := h.coordinator.GetFileEntry(l.keeper)
		if !ok {
			continue
		}
		if size, err := h.storage.StatFile(ctx, l.keeper); err != nil || size != e.SizeBytes {
			continue
		}
		rows, err := h.rowCounts(ctx, []fileInfo{byPath[l.path], byPath[l.keeper]})
		if err != nil {
			continue
		}
		lr, okL := rows[l.path]
		kr, okK := rows[l.keeper]
		if !okL || !okK || lr <= kr {
			continue
		}
		out = append(out, l)
	}
	return out
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

// A record is an intent: "Path is being (or was) replaced by ReplacedBy".
// It may be written before the replacement is published or registered, so
// nothing acts on it without retiredAction's gate.
//
//   - Standalone: the replacement (or a member of the family with fewer
//     rows: a later rewrite) is in storage.
//   - Cluster, on the manifest just caught up: Path is not registered, not
//     held by the outbox, not owned by a staged compaction, and a registered
//     member of its family is in storage at its registered size.
//
// A record whose replacement never took (the manifest still lists Path, and
// not the replacement, after rewriteAmbiguityDelay) is undone: the
// replacement is removed and the record cleared.

// retiredAction is what to do with one record.
type retiredAction int

const (
	retiredKeep      retiredAction = iota // not decidable now
	retiredRemoveOld                      // remove Path, clear the record
	retiredRevert                         // remove ReplacedBy, clear the record
	retiredClear                          // clear the record only
)

// Tunables, variables so tests can shrink them.
var (
	// rewriteFinishTimeout bounds the work after a rewrite is in storage
	// (record, manifest, removals), which outlives the request.
	rewriteFinishTimeout = 2 * time.Minute
	// rewriteAmbiguityDelay: how long a record whose manifest change has
	// not shown is left alone (a forwarded proposal can still apply).
	rewriteAmbiguityDelay = 5 * time.Minute
	// staleRetiredAge: a record naming nothing actionable this long is
	// cleared.
	staleRetiredAge = 7 * 24 * time.Hour
)

// rewriteSiblingEntries lists the registered members of a file's rewrite
// family (the coordinator, from the FSM index).
type rewriteSiblingEntries interface {
	RewriteSiblings(path string) []raft.FileEntry
}

// journalIntent writes the record for a cluster rewrite before its manifest
// change. A failure leaves the rewrite without one (as before).
func (h *DeleteHandler) journalIntent(ctx context.Context, old, new string) {
	if err := h.journalRetired(ctx, old, new); err != nil {
		h.logger.Warn().Err(err).Str("file", old).Msg("Could not record the file this DELETE replaces before changing the manifest")
	}
}

// rewriteCommitted reports whether a manifest change that returned an error
// applied anyway: on the manifest caught up with the leader, new is
// registered with sha and old is not.
func (h *DeleteHandler) rewriteCommitted(ctx context.Context, old, new, sha string) bool {
	s, ok := h.coordinator.(manifestSyncer)
	if !ok || s.SyncManifest(ctx) != nil {
		return false
	}
	e, ok := h.coordinator.GetFileEntry(new)
	if !ok || (sha != "" && e.SHA256 != sha) {
		return false
	}
	_, oldListed := h.coordinator.GetFileEntry(old)
	return !oldListed
}

// ambiguousRewrite leaves a rewrite whose manifest change has an unknown
// outcome as it is: the old file, the rewrite and the record stay, and the
// drainer settles it once the manifest says (removing the rewrite only
// rewriteAmbiguityDelay later). Removing the rewrite now would lose its rows
// if the change still applies.
func (h *DeleteHandler) ambiguousRewrite(old, new string, err error) error {
	h.logger.Error().Err(err).Str("file", old).Str("new_file", new).
		Msg("The manifest change of a DELETE rewrite has an unknown outcome; both files and the record are kept, and the retired-file drainer settles it")
	return errors.Join(errManifestFailure, fmt.Errorf("replace rewritten file in manifest (outcome unknown, settled later): %w", err))
}

// retiredAction decides what to do with rec. The caller holds rewriteMu and,
// on a cluster, caught up with the manifest.
func (h *DeleteHandler) retiredAction(ctx context.Context, rec retiredRecord) retiredAction {
	size, err := h.storage.StatFile(ctx, rec.Path)
	if err != nil {
		return retiredKeep
	}
	if h.coordinator == nil {
		if size < 0 {
			return retiredClear
		}
		if h.replacedHere(ctx, rec) {
			return retiredRemoveOld
		}
		// The replacement never landed (a crash before it was published),
		// or is gone: rec.Path is the current file.
		return retiredClear
	}
	holder, _ := h.pending.(registrationHolder)
	owner, _ := h.coordinator.(compactionOwner)
	busy := func(p string) bool {
		return (holder != nil && holder.HoldsRegistration(p)) || (owner != nil && owner.CompactionOwns(p))
	}
	if busy(rec.Path) {
		return retiredKeep
	}
	age := time.Since(rec.At)
	if _, listed := h.coordinator.GetFileEntry(rec.Path); !listed {
		if size >= 0 {
			if sibs, ok := h.coordinator.(rewriteSiblingEntries); ok {
				for _, e := range sibs.RewriteSiblings(rec.Path) {
					if n, err := h.storage.StatFile(ctx, e.Path); err == nil && n == e.SizeBytes {
						return retiredRemoveOld
					}
				}
			}
		} else {
			return retiredClear
		}
		if age > staleRetiredAge {
			if n, err := h.storage.StatFile(ctx, rec.ReplacedBy); err == nil && n < 0 {
				return retiredClear
			}
		}
		return retiredKeep
	}
	// rec.Path is listed: the replacement did not take, unless it is
	// listed too (then nothing is decidable here).
	if _, listed := h.coordinator.GetFileEntry(rec.ReplacedBy); listed || age < rewriteAmbiguityDelay || busy(rec.ReplacedBy) {
		return retiredKeep
	}
	return retiredRevert
}

// replacedHere (standalone): rec.ReplacedBy, or a member of rec.Path's
// family with fewer rows (a later rewrite of the replacement), is in storage.
func (h *DeleteHandler) replacedHere(ctx context.Context, rec retiredRecord) bool {
	if rec.ReplacedBy != "" {
		if n, err := h.storage.StatFile(ctx, rec.ReplacedBy); err == nil && n >= 0 {
			return true
		}
	}
	dir := rec.Path[:strings.LastIndex(rec.Path, "/")+1]
	listed, err := h.storage.List(ctx, dir)
	if err != nil {
		return false
	}
	family := storage.RewriteFamily(rec.Path)
	var members []fileInfo
	for _, p := range listed {
		if p == rec.Path || storage.RewriteFamily(p) != family {
			continue
		}
		if q, err := readParquetPath(h.storage, p); err == nil {
			members = append(members, fileInfo{queryPath: q, relativePath: p})
		}
	}
	if len(members) == 0 {
		return false
	}
	self, err := readParquetPath(h.storage, rec.Path)
	if err != nil {
		return false
	}
	rows, err := h.rowCounts(ctx, append(members, fileInfo{queryPath: self, relativePath: rec.Path}))
	if err != nil {
		return false
	}
	mine, ok := rows[rec.Path]
	if !ok {
		return false
	}
	for _, m := range members {
		if n, ok := rows[m.relativePath]; ok && n < mine {
			return true
		}
	}
	return false
}

// drainHere: standalone, a cluster on local storage (this node's own
// records), or the primary writer of a shared object store (the records are
// shared).
func (h *DeleteHandler) drainHere() bool {
	if h.coordinator == nil || !cluster.IsSharedObjectStore(h.storage) {
		return true
	}
	return h.coordinator.IsPrimaryWriter()
}

// DrainRetired settles the retired journal (retiredAction) and returns how
// many replaced files it removed.
func (h *DeleteHandler) DrainRetired(ctx context.Context) int {
	if !h.drainHere() {
		return 0
	}
	recs, err := h.retiredRecords(ctx)
	if err != nil {
		h.logger.Warn().Err(err).Msg("Could not read the retired-file journal")
		return 0
	}
	if len(recs) == 0 {
		return 0
	}
	if h.coordinator != nil {
		s, ok := h.coordinator.(manifestSyncer)
		if !ok || s.SyncManifest(ctx) != nil {
			return 0
		}
	}
	removed := 0
	for _, r := range recs {
		if ctx.Err() != nil {
			break
		}
		// The lock per record: a DELETE waits for one decision, not a pass.
		h.rewriteMu.Lock()
		var derr error
		act := h.retiredAction(ctx, r)
		switch act {
		case retiredRemoveOld:
			if derr = deleteReplaced(ctx, h.storage, r.Path); derr == nil {
				h.clearRetired(ctx, r.Path)
				removed++
			}
		case retiredRevert:
			if derr = deleteReplaced(ctx, h.storage, r.ReplacedBy); derr == nil {
				h.clearRetired(ctx, r.Path)
			}
		case retiredClear:
			h.clearRetired(ctx, r.Path)
		}
		h.rewriteMu.Unlock()
		switch {
		case derr != nil:
			h.logger.Warn().Err(derr).Str("file", r.Path).Str("replaced_by", r.ReplacedBy).
				Msg("Could not settle a file a DELETE replaced; queries may read its rows twice until it is")
		case act == retiredRemoveOld:
			h.logger.Info().Str("file", r.Path).Str("replaced_by", r.ReplacedBy).
				Msg("Removed a file a DELETE replaced and could not remove at the time")
		case act == retiredRevert:
			h.logger.Warn().Str("file", r.Path).Str("removed", r.ReplacedBy).
				Msg("A DELETE rewrite never reached the manifest; removed the rewrite, the original stays")
		case act == retiredClear:
			h.logger.Info().Str("file", r.Path).Msg("Cleared a retired-file record with nothing left to do")
		}
	}
	if removed > 0 {
		h.filesRemoved()
	}
	return removed
}

// StartRetiredDrainer runs DrainRetired now and every retiredDrainEvery
// until ctx ends (drainHere decides, at every tick, whether this node does).
func (h *DeleteHandler) StartRetiredDrainer(ctx context.Context) {
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

// logLeftovers warns about the leftovers a scan left out.
func (h *DeleteHandler) logLeftovers(left []leftover) {
	if len(left) == 0 {
		return
	}
	sample := make([]string, 0, min(len(left), 5))
	for _, l := range left[:min(len(left), 5)] {
		sample = append(sample, l.path)
	}
	h.logger.Warn().Int("files", len(left)).Strs("sample", sample).
		Msg("Delete leaves out files an earlier DELETE replaced and could not remove; queries read their rows twice until they are removed")
}

// parseRetiredRecord decodes a record body.
func parseRetiredRecord(data []byte, rec *retiredRecord) bool {
	return json.Unmarshal(data, rec) == nil
}
