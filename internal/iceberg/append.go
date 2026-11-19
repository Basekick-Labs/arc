package iceberg

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"
)

// The append path (#1751): an ingest flush reports the file it registered,
// and the export adds that file to its table without listing the measurement
// or reading the table's manifests. A reconcile pass costs work proportional
// to every file of the measurement (a listing, PlanFiles, iceberg-go's
// duplicate check, an expiry commit, a prune and a sweep); an append costs a
// footer read per new file, one commit and one publish.
//
// What it gives up, and who covers it:
//   - iceberg-go's duplicate check is off (it scans every manifest). The
//     scheduler never hands a file over twice (scheduler_append.go).
//   - It evolves nothing and heals nothing: a file with a column the table
//     lacks, or a missing table, is errNeedsFullPass.
//   - It does not expire snapshots, prune version files or sweep orphans.
//     The next reconcile pass of the table does (finishPass), and the
//     scheduler makes sure that pass is not skipped.

// errNeedsFullPass is appendFiles' report that these files cannot be added
// without a reconcile pass of the measurement.
var errNeedsFullPass = errors.New("iceberg: these files need a reconcile pass")

const (
	// appendMergeMinCount is the number of small data manifests at which an
	// append also merges them: every append adds one manifest, and a reader
	// opens each. appendMergeTargetBytes is the size a merged manifest grows
	// to before a new one is started, and the size under which a manifest
	// counts as small: about 4,000 files. A manifest over it is never
	// rewritten by an append, so the merge costs the same on a table of any
	// size (the collapse in commitDiff, by contrast, rewrites the table's
	// whole manifest set, and still runs on a reconcile pass that commits
	// once the table has manifestCollapseThreshold data manifests).
	appendMergeMinCount    = 4
	appendMergeTargetBytes = 64 << 10
)

// appendMergeOn makes the append in flight a merge-append that packs the
// small manifests together and leaves the large ones alone. Staged in the
// transaction and switched back off in it, like manifestMergeOn.
func appendMergeOn() iceberg.Properties {
	return iceberg.Properties{
		icetable.ManifestMergeEnabledKey:    "true",
		icetable.ManifestMinMergeCountKey:   strconv.Itoa(appendMergeMinCount),
		icetable.ManifestTargetSizeBytesKey: strconv.Itoa(appendMergeTargetBytes),
	}
}

// appendMergeDue reports whether the table has enough small data manifests
// for this append to merge them (the new one included), and how many it will
// have with the new one.
//
// The count is read from the current snapshot's manifest list the first time
// (any failure means no merge), and carried from one append to the next after
// that: a reconcile pass forgets it (reconcile) and so does an append that
// did not finish (appendFiles). On object storage that read was one request
// of every append, and iceberg-go reads the same list again to commit
// (#1778).
//
// Carried, it is an estimate. It counts the manifest each append adds as
// small and a merge's result as one small manifest, and iceberg-go, which
// packs by size and applies the minimum itself, can decline a staged merge.
// The estimate only moves when a merge is tried: a few more small manifests
// can stand than with a count read every time, until the table's next pass,
// which comes after appendMaxBetweenPasses appends at the latest.
func (e *Exporter) appendMergeDue(ctx context.Context, tkey string, tbl *icetable.Table) (merge bool, small int) {
	if n, ok := e.appendSmall[tkey]; ok {
		return n+1 >= appendMergeMinCount, n + 1
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return false, -1
	}
	fio, err := tbl.FS(ctx)
	if err != nil {
		return false, -1
	}
	manifests, err := snap.Manifests(fio)
	if err != nil {
		return false, -1
	}
	small = 1 // the manifest this append adds
	for _, m := range manifests {
		if m.ManifestContent() == iceberg.ManifestContentData && m.Length() < appendMergeTargetBytes {
			small++
		}
	}
	return small >= appendMergeMinCount, small
}

// appendResult is what appendFiles did with the files it was given.
type appendResult struct {
	added   []string // physical paths now in the table
	skipped []string // physical paths left out: their rows straddle a UTC day
}

// appendFiles adds files to an existing table in one commit and publishes it.
// The caller guarantees none of them is in the table. Any error leaves the
// caller to run a reconcile pass, which re-derives the table from a listing:
// errNeedsFullPass when the table or its schema cannot take the files as they
// are, anything else for a failure (a commit whose outcome is unknown
// included).
func (e *Exporter) appendFiles(ctx context.Context, database, measurement string, files []FileRef,
	readSchemas func(context.Context, []FileRef) (ArcSchema, error), canCommit func() bool) (res appendResult, err error) {
	if len(files) == 0 {
		return res, nil
	}
	ctx = e.withAWS(ctx)
	if canCommit == nil {
		canCommit = func() bool { return true }
	}
	// The same per-pass state a reconcile sets up (batched.go): the flags are
	// the exporter's, not the table's, and would otherwise carry over from
	// the last table it worked on.
	e.passLag = 0
	tkey := database + "\x00" + measurement
	// An append that does not finish leaves the table's manifests unknown to
	// the next one (appendMergeDue).
	defer func() {
		if err != nil {
			delete(e.appendSmall, tkey)
		}
	}()
	e.setPassGate(canCommit)
	defer e.setPassGate(nil)
	defer e.forgetExportLagIfNotPublisher(database, measurement)
	e.resetHintFailure()

	if err := checkNamespaceAddressable(e.nsPrefix, database); err != nil {
		return res, err
	}
	if err := e.adoptBeforeLoad(ctx, database, measurement); err != nil {
		return res, err
	}
	// Load only. EnsureTable would create a missing table, and from these
	// files alone that could be a table without its partition spec.
	tbl, err := e.loadTableRecovering(ctx, e.tableIdent(database, measurement))
	if errors.Is(err, icecatalog.ErrNoSuchTable) {
		return res, errNeedsFullPass
	}
	if err != nil {
		return res, err
	}
	e.notePassStart(ctx, tbl)

	add, err := readSchemas(ctx, files)
	if err != nil {
		return res, err
	}
	current := tbl.Schema()
	for _, f := range add.Fields {
		// A new column, or one of another type: the pass evolves the table or
		// reports the conflict, with the file named. (buildDataFiles would
		// refuse such a file too; this says why.)
		if existing, ok := current.FindFieldByName(f.Name); !ok || !existing.Type.Equals(f.Type) {
			return res, errNeedsFullPass
		}
	}
	paths := make([]string, len(files))
	want := make(map[string]int64, len(files))
	for i, f := range files {
		paths[i] = f.PhysicalPath
		want[f.PhysicalPath] = f.SizeBytes
	}
	built, skipped, err := e.buildDataFiles(ctx, tbl, paths, want)
	if err != nil {
		return res, err
	}
	res.skipped = skipped
	if len(built) == 0 {
		return res, nil
	}
	if !canCommit() {
		return res, errWriterLost
	}
	start := time.Now()
	txn := tbl.NewTransaction()
	merge, small := e.appendMergeDue(ctx, tkey, tbl)
	// Not known again until this append has finished (a panic past this point
	// skips the deferred delete's err check).
	delete(e.appendSmall, tkey)
	if merge {
		if err := txn.SetProperties(appendMergeOn()); err != nil {
			return res, fmt.Errorf("iceberg append: enable manifest merge: %w", err)
		}
	}
	// No duplicate check: it reads every manifest of the table, which is the
	// cost this path exists to avoid. The scheduler is what keeps a file from
	// being handed over twice.
	if err := txn.AddDataFiles(ctx, built, nil, icetable.WithoutDuplicateCheck()); err != nil {
		return res, fmt.Errorf("iceberg append (%d files): %w", len(built), err)
	}
	if merge {
		if err := txn.SetProperties(manifestMergeOff()); err != nil {
			return res, fmt.Errorf("iceberg append: disable manifest merge: %w", err)
		}
	}
	committed, err := txn.Commit(ctx)
	if err != nil {
		return res, fmt.Errorf("iceberg append commit (%d files): %w", len(built), err)
	}
	// The pass that follows expires what this added (finishPass).
	e.setExpiryOwed(database+"\x00"+measurement, true)
	// Before the publish, which is where the lag is observed (export_lag.go).
	e.noteExported(committed.Identifier(), built, nil)
	wrote := e.writeVersionHint(ctx, committed)
	if fe := e.fencedErr(); fe != nil {
		return res, fe
	}
	if !wrote || e.hintFailed() {
		// Committed and not published: the pass that follows publishes it.
		return res, errors.New("iceberg append: committed, but the discovery files were not published")
	}
	for _, df := range built {
		res.added = append(res.added, df.FilePath())
	}
	// What the next append of this table starts from. A merge packs the small
	// manifests into one, which is counted as small: if it is not, the next
	// merge is staged one append early and iceberg-go, which applies the
	// minimum itself, leaves the manifests alone.
	if small > 0 {
		if merge {
			small = 1
		}
		if e.appendSmall == nil {
			e.appendSmall = make(map[string]int)
		}
		e.appendSmall[tkey] = small
	}
	e.logger.Debug().
		Str("database", database).Str("measurement", measurement).
		Int("added", len(built)).Int("skipped", len(skipped)).Bool("merged_manifests", merge).
		Dur("took", time.Since(start)).Dur("oldest_ingest_file_lag", e.passLag).
		Msg("Appended ingest files to the Iceberg table")
	return res, nil
}
