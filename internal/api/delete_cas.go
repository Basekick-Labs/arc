package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// The cluster coordinator is what production wires in.
var _ compactionCommitter = (*cluster.Coordinator)(nil)

// Row DELETE under compaction commit (#1207 d2): with the cluster flag on,
// the DELETE's manifest ops apply only to the content it read, and a file a
// compaction replaced while the DELETE ran is scanned again through the
// compaction's output.

// errFileChanged: the manifest refused a DELETE op because its file is no
// longer registered at the content the DELETE read.
var errFileChanged = errors.New("file changed under the delete")

// deleteChangedRounds bounds how many times a DELETE scans again after files
// changed under it.
const deleteChangedRounds = 3

// compactionCommitter is the coordinator's compaction commit side.
type compactionCommitter interface {
	CompactionCommitActive() bool
	WaitLocalFiles(ctx context.Context, database, measurement string, budget time.Duration) ([]string, error)
	// SyncManifest returns once this node applied everything the leader
	// had committed.
	SyncManifest(ctx context.Context) error
	// ManifestHotFiles is what the manifest lists under database/measurement/.
	ManifestHotFiles(database, measurement string) []raft.FileEntry
}

// scanOpts: how a DELETE scan treats files the manifest lists that are not
// in storage here yet.
type scanOpts struct {
	// wait for them, up to deleteLocalWait per scan (not on a dry run).
	wait bool
	// allowUnreachable: scan without the ones still missing then.
	allowUnreachable bool
}

// errManifestNotSynced: this node could not catch up with the cluster
// manifest before a scan; a file registered a moment ago could be missed.
var errManifestNotSynced = errors.New("this node could not catch up with the cluster manifest")

// casActive reports whether DELETE ops carry the content they read.
func casActive(coord any) bool {
	c, ok := coord.(compactionCommitter)
	return ok && c.CompactionCommitActive()
}

// registrationHolder is the registration outbox's view of a path it still
// owns (pending, parked, or confirmed a moment ago).
type registrationHolder interface {
	HoldsRegistration(path string) bool
}

// scanSet is what a DELETE scans once compaction commit is on: the files
// the manifest lists for the measurement, read once after this node caught
// up with the leader, plus the listed files this node's outbox still holds.
// Other listed files are not data (a compaction output before its commit, an
// input after it). The listing alone is not enough: a compaction that
// commits between the listing and the catch-up has an output the listing
// missed, and inputs the catch-up drops; scanning only the listing then finds
// nothing and the output brings the rows back.
//
// A manifest file not here at its size is waited for (opts), the manifest
// read again on each poll so a file it stops listing meanwhile is no longer
// waited for. Still missing, it stops the DELETE with errUnreachableFiles
// unless opts.allowUnreachable; it is returned as unreachable either way.
func (h *DeleteHandler) scanSet(ctx context.Context, database, measurement string, listed []fileInfo, opts scanOpts, leftovers *int) ([]fileInfo, map[string]string, []string, error) {
	c, ok := h.coordinator.(compactionCommitter)
	if !ok {
		return listed, nil, nil, nil
	}
	if err := c.SyncManifest(ctx); err != nil {
		return nil, nil, nil, fmt.Errorf("%w: %v", errManifestNotSynced, err)
	}
	// Leftovers of earlier DELETEs, judged on the manifest just synced
	// (delete_leftovers.go): left out before sharedUnlisted could adopt one.
	listed, left := h.clusterClassify(listed, h.journaledPaths(ctx))
	h.logLeftovers(left)
	if leftovers != nil {
		*leftovers = len(left)
	}
	manifest := hotByPath(c.ManifestHotFiles(database, measurement))
	// A listed file is complete (local storage stages writes under a name
	// List omits; an object appears whole), but on local storage a size check
	// is a cheap guard against reading other content than the registered.
	statListed := h.storage.Type() == "local"
	holder, _ := h.pending.(registrationHolder)
	shas := make(map[string]string)
	inListing := make(map[string]bool, len(listed))
	kept := make([]fileInfo, 0, len(listed))
	var skipped []string
	for _, f := range listed {
		inListing[f.relativePath] = true
		e, ok := manifest[f.relativePath]
		if !ok {
			// Registered but not under the hot entries of the measurement
			// (another tier, or an entry indexed under another database), or
			// held by the outbox.
			if other, reg := h.coordinator.GetFileEntry(f.relativePath); reg {
				shas[f.relativePath] = other.SHA256
				kept = append(kept, f)
				continue
			}
			if holder != nil && holder.HoldsRegistration(f.relativePath) {
				kept = append(kept, f)
				continue
			}
			if so, shared := sharedObjectStore(h.coordinator); shared {
				// What a query reads (#1207): an adopted compaction output,
				// or an object no manifest change covers.
				if h.sharedUnlisted(so, f.relativePath) {
					shas[f.relativePath] = unmanagedSHA
					kept = append(kept, f)
					continue
				}
			}
			skipped = append(skipped, f.relativePath)
			continue
		}
		if statListed {
			size, err := h.storage.StatFile(ctx, f.relativePath)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to check %s: %w", f.relativePath, err)
			}
			if size != e.SizeBytes {
				inListing[f.relativePath] = false
				continue
			}
		}
		shas[f.relativePath] = e.SHA256
		kept = append(kept, f)
	}
	if len(skipped) > 0 {
		h.logger.Info().Int("files", len(skipped)).Strs("sample", skipped[:min(len(skipped), 5)]).
			Msg("Delete skipped files in storage the cluster manifest does not list (compaction output not committed yet, or an input already replaced)")
	}
	// Manifest files the listing did not have (or not at their size): here
	// by now, or waited for.
	deadline := time.Now().Add(deleteLocalWait)
	found := make(map[string]bool)
	so, shared := sharedObjectStore(h.coordinator)
	var absent []string
	for {
		absent = absent[:0]
		for _, path := range sortedPaths(manifest) {
			e := manifest[path]
			if inListing[path] || found[path] || !strings.HasSuffix(strings.ToLower(path), ".parquet") {
				continue
			}
			// Held first: an output copied into place and ended between a
			// stat and the check would read as gone.
			held := shared && so.CompactionOutputHeld(path)
			size, err := h.storage.StatFile(ctx, path)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to check %s: %w", path, err)
			}
			if size < 0 && shared && !held {
				// Gone from the shared store: no node will ever have it.
				found[path] = true
				continue
			}
			if size != e.SizeBytes {
				absent = append(absent, path)
				continue
			}
			queryPath, err := readParquetPath(h.storage, path)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("manifest file %q has no usable storage path: %w", path, err)
			}
			found[path] = true
			kept = append(kept, fileInfo{queryPath: queryPath, relativePath: path})
			shas[path] = e.SHA256
		}
		if len(absent) == 0 || !opts.wait || !time.Now().Before(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
		// The manifest again: a file it no longer lists (replaced, moved to
		// the cold tier, removed) is not waited for.
		manifest = hotByPath(c.ManifestHotFiles(database, measurement))
	}
	unreachable := append([]string(nil), absent...)
	if len(unreachable) > 0 && opts.wait && !opts.allowUnreachable {
		return nil, nil, unreachable, errUnreachableFiles
	}
	return kept, shas, unreachable, nil
}

func hotByPath(files []raft.FileEntry) map[string]raft.FileEntry {
	m := make(map[string]raft.FileEntry, len(files))
	for _, f := range files {
		m[f.Path] = f
	}
	return m
}

func sortedPaths(m map[string]raft.FileEntry) []string {
	out := make([]string, 0, len(m))
	for p := range m {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// rescanAfterChange scans again after files changed under the DELETE (a
// compaction replaced them): the scan set waits for the compaction's output
// (scanSet). Files still missing are returned; unless allowUnreachable, they
// stop the DELETE.
func (h *DeleteHandler) rescanAfterChange(ctx context.Context, database, measurement, where string, opts scanOpts) ([]affectedFile, []string, error) {
	return h.findAffectedFiles(ctx, database, measurement, where, opts)
}

// isContextErr: the request was cancelled or timed out (retryable).
func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// sharedObjectScanner is the coordinator on a shared object store (#1207):
// every node reads one bucket, so what a query reads is the listing, and a
// manifest entry whose object is gone is gone rather than not here yet.
type sharedObjectScanner interface {
	SharedObjectStore() bool
	// ManifestAdoption: objects the manifest does not list may be
	// registered (the rewrite of one a DELETE scanned).
	ManifestAdoption() bool
	CompactionOwns(path string) bool
	CompactionOutputHeld(path string) bool
	AdoptRewrite(ctx context.Context, path string, size int64, sha string) (raft.FileEntry, error)
}

// sharedObjectStore returns the coordinator as a sharedObjectScanner when
// the cluster is on a shared object store with compaction commit on.
func sharedObjectStore(coord any) (sharedObjectScanner, bool) {
	so, ok := coord.(sharedObjectScanner)
	if !ok || !casActive(coord) || !so.SharedObjectStore() {
		return nil, false
	}
	return so, true
}

// unmanagedSHA marks a scanned file no manifest entry covers (on a shared
// object store): its rewrite is registered as a new file, its removal
// leaves the manifest alone.
const unmanagedSHA = "unmanaged"

// sharedUnlisted reports whether a DELETE scans an object of the listing the
// manifest does not list, on a shared object store, where queries read it:
// not a staged compaction's input (its rows are in the output, which the
// DELETE scans, and its finisher removes it), and only where its rewrite can
// be registered (else the rewrite would be an orphan the reconciler removes).
// The repair registers compaction outputs the manifest lost; a DELETE never
// does (it would read them whole before its limits are checked).
func (h *DeleteHandler) sharedUnlisted(so sharedObjectScanner, path string) bool {
	return so.ManifestAdoption() && !so.CompactionOwns(path)
}

// finalizingInputs is the coordinator's view of a committed object-store
// compaction whose output a DELETE just removed (#1207 d2a2b).
type finalizingInputs interface {
	FinalizingObjectInputs(path string) []string
}

// dropFinalizingInputs removes the input objects of the committed compaction
// whose output path this DELETE rewrote or deleted: they hold the same rows,
// unregistered, and reads glob them until the job's finisher removes them.
func (h *DeleteHandler) dropFinalizingInputs(ctx context.Context, path string) {
	f, ok := h.coordinator.(finalizingInputs)
	if !ok || !casActive(h.coordinator) {
		return
	}
	for _, in := range f.FinalizingObjectInputs(path) {
		if err := h.storage.Delete(ctx, in); err != nil {
			h.logger.Warn().Err(err).Str("file", in).Str("output", path).
				Msg("Could not remove an input of the compaction whose output the delete replaced; its finisher removes it")
		}
	}
}
