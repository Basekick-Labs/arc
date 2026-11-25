package iceberg

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"sync"
	"time"

	iceberg "github.com/apache/iceberg-go"
	"github.com/basekick-labs/arc/internal/storage"
	"gocloud.dev/gcerrors"
	"golang.org/x/sync/errgroup"
)

// Arc's other writer paths (compaction, the DELETE rewrite, retention, tier moves) delete data
// files while an export pass is running. The listing guard in listOnce catches a file that
// vanishes WHILE the measurement is listed, and refuses that listing: the vanished file's rows may
// be in a file the listing missed. A file can also vanish AFTER a stable listing, before the pass
// reads its footer. Both are races with ordinary Arc work, not failures, and the next pass, which
// lists again, sees the result. This file is how the pass tells such a race apart from a real read
// failure and what it does with the files involved.
//
// Evidence, not error text: a read error that looks like "not found" (isReadNotFound) only makes
// the pass ask the file's own backend whether it still exists (reconcileRequest.exists, which
// routes a cold file to the cold tier). Only a file confirmed absent is treated as vanished; any
// other read error, or a probe that fails, is the pass's error as before.
//
// What is done with a vanished file depends on what the table already holds, at the granularity
// the table changes atomically, the UTC day (splitAnchored):
//
//   - A new file in a day with no removal this pass (the free set) is not in the table, so no
//     reader sees its rows yet. Every new file of that day is left for the next pass (the day's
//     other files are, almost always, the same compaction's inputs, about to be deleted too); the
//     rest of the pass goes on. Except a fresh ingest file of the current UTC day
//     (freshIngestFile): no compaction takes it as an input, so it commits now (#1755).
//   - A file of commit 0 (a day that also loses files this pass: a compaction output, a rewrite, a
//     tier move) defers that whole day: its removals, tier moves and new files stay exactly as the
//     table has them, and the other days commit. A day's removals cannot be split from the files
//     that hold their rows, or a reader between two commits would see its rows missing (#1686).
//     A fresh ingest file holds no removed rows, so it is never in commit 0 (splitAnchoredAt).
//
// A pass that left files or days behind is never settled, so the scheduler does not cache the
// listing it started from, and reports the measurement as busy (nothing committed) or in progress
// (something committed) and comes back on the short interval.

// errSourcesMoving marks a listing that kept changing under every attempt (FilesAndLocal): a
// writer path is rewriting the measurement right now. The scheduler reports it as busy.
var errSourcesMoving = errors.New("source files are changing under the export")

// stopSourcesMoving is the reason a pass stops registering when files kept vanishing.
const stopSourcesMoving = "source files kept changing"

// maxVanishRounds bounds how many times one pass drops vanished files and tries again, for
// commit 0 and for the free chunks each: a measurement being compacted from end to end must not
// keep one pass probing until its deadline.
const maxVanishRounds = 3

// probeConcurrency bounds the existence checks for one chunk, as unionSchemaConcurrency bounds its
// footer reads.
const probeConcurrency = 16

// vanishedErr is a chunk's read failure explained: paths were confirmed absent after the read
// failed with a not-found. cause is that read error.
type vanishedErr struct {
	paths []string
	cause error
}

func (v *vanishedErr) Error() string {
	return fmt.Sprintf("%d source file(s) disappeared during the pass: %v", len(v.paths), v.cause)
}

func (v *vanishedErr) Unwrap() error { return v.cause }

// isReadNotFound reports whether a footer read failed because the object is missing. Arc's
// backends say so through storage.IsNotFound (the S3 NoSuchKey, fs.ErrNotExist); iceberg-go's S3
// FileIO maps a missing object on open to fs.ErrNotExist, but a 404 on a later range read comes
// back as a gocloud error.
func isReadNotFound(err error) bool {
	return storage.IsNotFound(err) || gcerrors.Code(err) == gcerrors.NotFound
}

// probeFunc returns which of paths (table URIs) no longer exist.
type probeFunc func(ctx context.Context, paths []string) ([]string, error)

// newProbe returns the probe for one reconcile: each path is checked through exists with the
// FileRef the listing gave it. nil when the request has no exists (a caller that cannot check),
// which turns vanished-file handling off: every read error is the pass's error, as before.
func newProbe(exists func(context.Context, FileRef) (bool, error), current []FileRef) probeFunc {
	if exists == nil {
		return nil
	}
	refs := make(map[string]FileRef, len(current))
	for _, f := range current {
		refs[f.PhysicalPath] = f
	}
	return func(ctx context.Context, paths []string) ([]string, error) {
		var (
			mu   sync.Mutex
			gone []string
		)
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(probeConcurrency)
		for _, p := range paths {
			f, ok := refs[p]
			if !ok {
				// Not from this listing: nothing to check it against, so it cannot be called gone.
				continue
			}
			g.Go(func() error {
				release := acquireRead(gctx) // one of the batch's storage reads, in a batch (#1786)
				ok, err := exists(gctx, f)
				release()
				if err != nil {
					return fmt.Errorf("check whether %s still exists: %w", p, err)
				}
				if !ok {
					mu.Lock()
					gone = append(gone, p)
					mu.Unlock()
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
		sort.Strings(gone)
		return gone, nil
	}
}

// explainRead turns a footer-read error into a *vanishedErr when it is a not-found and the probe
// confirms at least one of paths is gone. Every other case returns err unchanged.
func explainRead(ctx context.Context, err error, paths []string, probe probeFunc) error {
	if err == nil || probe == nil || !isReadNotFound(err) {
		return err
	}
	gone, perr := probe(ctx, paths)
	if perr != nil || len(gone) == 0 {
		return err
	}
	return &vanishedErr{paths: gone, cause: err}
}

// daysOfPaths returns the UTC days the paths belong to. ok is false when any path's day cannot be
// read from its key, in which case no day-level decision is safe.
func daysOfPaths(paths []string, database, measurement string) (days map[string]struct{}, ok bool) {
	days = make(map[string]struct{}, len(paths))
	for _, p := range paths {
		d := dayOf(p, database, measurement)
		if d == "" {
			return nil, false
		}
		days[d] = struct{}{}
	}
	return days, true
}

// freshIngestAge is how recent an ingest file's name must be for dropVanished to keep it beside a
// vanished file of its day (#1755). Compaction skips a partition holding a file named within its
// minimum age, at least an hour on both tiers (compaction/hourly.go groupHourPartitions,
// compaction/daily.go groupDayPartitions); the daily tier stops checking for days older than
// skip_file_age_check_days (at least one day), so only the current UTC day's files are exempt.
// Such a file is not an input of the compaction whose file vanished, about to go too, and its rows
// are new to the table. A quarter hour short of the hour, for clock skew between the node that
// named the file and the node that compacts.
const freshIngestAge = 45 * time.Minute

// dropVanished removes from paths every path in gone and, when every gone path has a readable day,
// every other path in one of those days, except a fresh ingest file (freshIngestAge). It returns
// what is kept and what was dropped.
func dropVanished(paths, gone []string, database, measurement string) (kept, dropped []string) {
	return dropVanishedAt(paths, gone, database, measurement, time.Now())
}

func dropVanishedAt(paths, gone []string, database, measurement string, now time.Time) (kept, dropped []string) {
	goneSet := make(map[string]struct{}, len(gone))
	for _, p := range gone {
		goneSet[p] = struct{}{}
	}
	days, byDay := daysOfPaths(gone, database, measurement)
	for _, p := range paths {
		_, isGone := goneSet[p]
		inDay := false
		if byDay && !isGone {
			_, inDay = days[dayOf(p, database, measurement)]
			if inDay && freshIngestFile(p, database, measurement, now) {
				inDay = false // not a compaction input: it commits now
			}
		}
		if isGone || inDay {
			dropped = append(dropped, p)
		} else {
			kept = append(kept, p)
		}
	}
	return kept, dropped
}

// freshIngestFile reports whether p is an ingest file of the current UTC day named within
// freshIngestAge of now, and not a DELETE rewrite (which replaces its original), for
// dropVanished and splitAnchoredAt.
func freshIngestFile(p, database, measurement string, now time.Time) bool {
	name := path.Base(p)
	if storage.StripRewriteSuffix(name) != name {
		return false
	}
	t, ok := storage.IngestFileTime(name)
	if !ok || now.Sub(t) >= freshIngestAge {
		return false
	}
	return dayOf(p, database, measurement) == now.UTC().Format("2006/01/02")
}

// dataFilePaths returns the paths of DataFiles.
func dataFilePaths(dfs []iceberg.DataFile) []string {
	out := make([]string, len(dfs))
	for i, df := range dfs {
		out[i] = df.FilePath()
	}
	return out
}

// refPaths returns the table URIs of files.
func refPaths(files []FileRef) []string {
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.PhysicalPath
	}
	return out
}

// keepRefs returns the files whose URI is in kept, in their order.
func keepRefs(files []FileRef, kept []string) []FileRef {
	set := make(map[string]struct{}, len(kept))
	for _, p := range kept {
		set[p] = struct{}{}
	}
	out := files[:0:0]
	for _, f := range files {
		if _, ok := set[f.PhysicalPath]; ok {
			out = append(out, f)
		}
	}
	return out
}

// anyUnknownDay reports whether a path's day cannot be read from its key. A removal like that is
// anchored to every day (splitAnchored), so no single day of commit 0 can be deferred without it.
func anyUnknownDay(paths []string, database, measurement string) bool {
	for _, p := range paths {
		if dayOf(p, database, measurement) == "" {
			return true
		}
	}
	return false
}

// deferDays takes the given UTC days out of commit 0, whole: their removals, rewrites, tier moves
// and new files. nil days defers every day. What remains is returned, with the removals rebuilt
// from the table's own entries, and deferredNew counts the new files (not rewrites) taken out.
func deferDays(days map[string]struct{}, live map[string]iceberg.DataFile, toRemove, rewritten []string, cloned []iceberg.DataFile, anchored []string, database, measurement string) (deferredNew int, keptRemove []string, removes []iceberg.DataFile, keptRewritten []string, keptCloned []iceberg.DataFile, keptAnchored []string) {
	keep := func(p string) bool {
		if days == nil {
			return false
		}
		_, hit := days[dayOf(p, database, measurement)]
		return !hit
	}
	rewrote := make(map[string]struct{}, len(rewritten))
	for _, p := range rewritten {
		rewrote[p] = struct{}{}
	}
	for _, p := range toRemove {
		if keep(p) {
			keptRemove = append(keptRemove, p)
			removes = append(removes, live[p])
		}
	}
	for _, p := range rewritten {
		if keep(p) {
			keptRewritten = append(keptRewritten, p)
		}
	}
	for _, df := range cloned {
		if keep(df.FilePath()) {
			keptCloned = append(keptCloned, df)
		}
	}
	for _, p := range anchored {
		if keep(p) {
			keptAnchored = append(keptAnchored, p)
		} else if _, ok := rewrote[p]; !ok {
			deferredNew++
		}
	}
	return deferredNew, keptRemove, removes, keptRewritten, keptCloned, keptAnchored
}
