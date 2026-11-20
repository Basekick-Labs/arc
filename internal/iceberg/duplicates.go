package iceberg

import (
	"context"
	"errors"
	"sort"

	"github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
)

// Paths held twice (#1795).
//
// Two processes exporting one bucket could add a file twice: one's pass
// registered it, and the other then took that published version over and
// appended the file again. Readers count such a file's rows twice. Worse,
// iceberg-go cannot remove such a path: its ReplaceDataFilesWithDataFiles and
// ReplaceFiles refuse when they match more entries than paths ("cannot delete
// files that do not belong to the table"), so once the file left storage every
// pass of the table failed and the table stopped exporting.
//
// appendFiles no longer appends to a table that is not where this process
// left it (errTableMoved). repairDuplicates mends a table that already holds a
// path twice: Delete(AlwaysTrue), which takes every data entry by path with no
// such count, then each live path added back once, merged, in one commit.

// errTableMoved is appendFiles' report that the table is not at the version
// this process last left it at: another process committed to it (it was taken
// over from the warehouse), or its catalog row was recovered. What this
// process remembers of the table's files does not cover that, so a pass
// diffs the table instead.
var errTableMoved = errors.New("iceberg: the table is not at the version this node last left it at")

// noteLeftAt records the metadata location this process left the table at.
func (e *Exporter) noteLeftAt(tkey, loc string) {
	e.chunkMu.Lock()
	defer e.chunkMu.Unlock()
	if e.leftAt == nil {
		e.leftAt = make(map[string]string)
	}
	e.leftAt[tkey] = loc
}

// leftAtLoc returns the location noteLeftAt recorded for the table.
func (e *Exporter) leftAtLoc(tkey string) (string, bool) {
	e.chunkMu.Lock()
	defer e.chunkMu.Unlock()
	loc, ok := e.leftAt[tkey]
	return loc, ok
}

// repairDuplicates rewrites the table's data entries so that each path in
// dups is held once, and returns the committed table. It is best-effort: on any
// failure it logs and returns the table unchanged with ok=false, and the pass
// goes on with the table as it was (a duplicate whose file is still in storage
// blocks nothing; one whose file left storage fails the removal, as before).
//
// The entry kept for a path is the one whose size matches the listing (want),
// else the first; the diff that follows treats a size mismatch as a rewrite.
// The re-add merges the manifests (stageMergedAdd), which drops the DELETED
// entries Delete writes for every file and leaves one manifest.
//
// Refused on a table with delete files (Arc writes none; an adopted table could
// carry some): the re-added entries take the new sequence number, so position or
// equality deletes written before would stop applying and deleted rows would
// come back.
func (e *Exporter) repairDuplicates(ctx context.Context, tbl *icetable.Table, live map[string]iceberg.DataFile,
	dups map[string][]iceberg.DataFile, want map[string]int64, database, measurement string) (*icetable.Table, bool) {
	log := e.logger.With().Str("database", database).Str("measurement", measurement).Int("paths_held_twice", len(dups)).Logger()
	hasDeletes, err := snapshotHasDeleteFiles(ctx, tbl)
	if err != nil {
		log.Error().Err(err).Msg("Iceberg: the table holds data files more than once and could not be checked for delete files; not repaired this pass")
		return tbl, false
	}
	if hasDeletes {
		log.Error().Msg("Iceberg: the table holds data files more than once, and has delete files, which a repair would stop applying; not repaired. Readers count those files' rows more than once")
		return tbl, false
	}
	keep := make([]iceberg.DataFile, 0, len(live))
	for p, df := range live {
		if entries, dup := dups[p]; dup {
			df = entries[0]
			if size, listed := want[p]; listed {
				for _, d := range entries {
					if d.FileSizeBytes() == size {
						df = d
						break
					}
				}
			}
		}
		keep = append(keep, df)
	}
	sort.Slice(keep, func(i, j int) bool { return keep[i].FilePath() < keep[j].FilePath() })
	txn := tbl.NewTransaction()
	if err := txn.Delete(ctx, iceberg.AlwaysTrue{}, nil); err != nil {
		log.Error().Err(err).Msg("Iceberg: could not stage the repair of data files held more than once; the next pass tries again")
		return tbl, false
	}
	if err := e.stageMergedAdd(ctx, txn, keep); err != nil {
		log.Error().Err(err).Msg("Iceberg: could not stage the repair of data files held more than once; the next pass tries again")
		return tbl, false
	}
	repaired, err := txn.Commit(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Iceberg: the repair of data files held more than once did not commit; the next pass tries again")
		return tbl, false
	}
	examples := make([]string, 0, 3)
	for p := range dups {
		if len(examples) == cap(examples) {
			break
		}
		examples = append(examples, p)
	}
	sort.Strings(examples)
	log.Warn().Strs("examples", examples).Int("files", len(keep)).
		Msg("Iceberg: the table held data files more than once, so readers counted their rows more than once; each is held once now. " +
			"This comes from two processes exporting the same bucket (#1795); run one exporter per bucket")
	return repaired, true
}

// snapshotHasDeleteFiles reports whether the current snapshot lists a delete
// manifest.
func snapshotHasDeleteFiles(ctx context.Context, tbl *icetable.Table) (bool, error) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return false, nil
	}
	fio, err := tbl.FS(ctx)
	if err != nil {
		return false, err
	}
	manifests, err := snap.Manifests(fio)
	if err != nil {
		return false, err
	}
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentData {
			return true, nil
		}
	}
	return false, nil
}
