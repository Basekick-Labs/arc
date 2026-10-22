package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

// The cluster manifest repair (#1207), run by the leader.
//
// Until #1207 a cluster on a shared object store ran no compaction
// completion watcher (only replication did), so compaction never updated the
// cluster manifest: every compaction output stayed unregistered and every
// input it consumed stayed listed after its object was deleted. The watcher
// runs everywhere now; the repair brings the manifest of an upgraded cluster
// back in line with the bucket, which is what every query reads there:
//
//   - a hot entry whose object is gone is dropped (an output a finalizing
//     staged job still copies into place excepted);
//   - a compaction output the manifest does not list is registered
//     (adopted), once it is older than manifestRepairGrace.
//
// Only compaction outputs are adopted: other unlisted objects are leftovers
// of a delete that failed after its manifest change (a DELETE rewrite, a
// tiering flip, retention, a database delete), which the reconciler owns. A
// compaction output a DELETE rewrote is such a leftover too, while the
// manifest lists its rewrite (AdoptableObject).
//
// On every cluster, compaction also registered its outputs in the
// compaction tier ("hourly", "daily") rather than "hot"; the repair moves
// such entries to "hot", so a staged compaction can take them as inputs.
const (
	manifestRepairEvery = time.Hour
	// manifestRepairFirst: the first run after a node becomes leader.
	manifestRepairFirst = 2 * time.Minute
	// manifestRepairRetry: the next run after one that stopped early.
	manifestRepairRetry = 10 * time.Minute
	// manifestRepairGrace: an unlisted compaction output younger than this
	// may be one a legacy compaction's watcher registers in a moment.
	manifestRepairGrace = time.Hour
	// manifestRepairReason is the manifest delete reason of a dropped entry.
	// Not in raft.OutputGoneAccounted: a finalizing job whose output this
	// dropped reverts to its inputs rather than deleting them.
	manifestRepairReason = "repair:missing"
	// manifestRepairBatch: manifest changes per Raft entry.
	manifestRepairBatch = 200
)

// manifestRepairMaxAdopt / manifestRepairMaxBytes bound the objects a run
// reads to adopt (each is read whole for its checksum); the rest waits for
// the next run. Variables for tests.
var (
	manifestRepairMaxAdopt = 1000
	manifestRepairMaxBytes = int64(10) << 30
)

// manifestRepairState is the leader's last repair, for the status view, and
// the cold-tier check main.go wires.
type manifestRepairState struct {
	mu       sync.Mutex
	last     time.Time
	dropped  int
	adopted  int
	retiered int
	pending  int
	err      string
	// coldPath reports a path the tiering metadata records as cold: a hot
	// copy tiering failed to delete, never adopted.
	coldPath func(ctx context.Context, path string) bool
}

// ManifestRepairStatus is the /cluster status view of the last repair this
// node ran as leader.
type ManifestRepairStatus struct {
	LastRun time.Time `json:"last_run"`
	Dropped int       `json:"dropped"`
	Adopted int       `json:"adopted"`
	// Retiered: entries moved from a compaction tier name to "hot".
	Retiered int `json:"retiered,omitempty"`
	// Pending: adoptable objects left for the next run (budget reached).
	Pending int    `json:"pending,omitempty"`
	Error   string `json:"error,omitempty"`
}

// SetColdPathCheck wires the tiering metadata: a path it records as cold is
// never adopted (#1207).
func (c *Coordinator) SetColdPathCheck(fn func(ctx context.Context, path string) bool) {
	c.repair.mu.Lock()
	c.repair.coldPath = fn
	c.repair.mu.Unlock()
}

func (c *Coordinator) manifestRepairStatus() *ManifestRepairStatus {
	c.repair.mu.Lock()
	defer c.repair.mu.Unlock()
	if c.repair.last.IsZero() {
		return nil
	}
	return &ManifestRepairStatus{LastRun: c.repair.last, Dropped: c.repair.dropped, Adopted: c.repair.adopted,
		Retiered: c.repair.retiered, Pending: c.repair.pending, Error: c.repair.err}
}

// SharedObjectStore reports whether the nodes read and write one object
// store (S3, Azure: Arc never replicates files between nodes on one): an
// object is then either in the store for every node or gone, never "not on
// this node yet".
func (c *Coordinator) SharedObjectStore() bool {
	return c != nil && c.storage != nil && c.stagedObject()
}

// ManifestAdoption reports whether objects the manifest does not list may be
// registered (adopted) here: a shared object store with compaction commit on
// (every FSM checks an adoption) and stream claims off (a claim decides what
// an unregistered batch of a gone writer becomes).
func (c *Coordinator) ManifestAdoption() bool {
	return c.SharedObjectStore() && c.CompactionCommitActive() && !c.StreamClaimsEnabled()
}

// compactedOutputName matches a compaction output's file name:
// <measurement>_<YYYYMMDD>_<HHMMSS>_<nanos>_b<batch>_<compacted|daily>.parquet
// (compaction.Job). Group 1 is the measurement.
var compactedOutputName = regexp.MustCompile(`^(.+)_\d{8}_\d{6}_\d+_b\d+_(compacted|daily)\.parquet$`)

// parsePartitionPath parses a parquet path in an hour
// (db/measurement/YYYY/MM/DD/HH/<file>) or day (db/measurement/YYYY/MM/DD/
// <file>) directory. Edge-sync spoke namespaces, reserved roots and Iceberg
// warehouse directories are not.
func parsePartitionPath(path string) (database, measurement string, partition time.Time, hourly, ok bool) {
	parts := strings.Split(path, "/")
	switch len(parts) {
	case 7:
		hourly = true
	case 6:
	default:
		return "", "", time.Time{}, false, false
	}
	database, measurement = parts[0], parts[1]
	if database == "" || measurement == "" || storage.IsReservedRootDir(database) || strings.Contains(database, ".") ||
		!strings.HasSuffix(parts[len(parts)-1], ".parquet") {
		return "", "", time.Time{}, false, false
	}
	num := func(s string, n, lo, hi int) (int, bool) {
		if len(s) != n {
			return 0, false
		}
		v, err := strconv.Atoi(s)
		return v, err == nil && v >= lo && v <= hi
	}
	y, ok1 := num(parts[2], 4, 1, 9999)
	mo, ok2 := num(parts[3], 2, 1, 12)
	d, ok3 := num(parts[4], 2, 1, 31)
	h, ok4 := 0, true
	if hourly {
		h, ok4 = num(parts[5], 2, 0, 23)
	}
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return "", "", time.Time{}, false, false
	}
	partition = time.Date(y, time.Month(mo), d, h, 0, 0, 0, time.UTC)
	if partition.Day() != d {
		return "", "", time.Time{}, false, false
	}
	return database, measurement, partition, hourly, true
}

// parseCompactedOutputPath parses an hourly (_compacted, in an hour
// directory) or daily (_daily, in a day directory) compaction output path.
func parseCompactedOutputPath(path string) (database, measurement string, partition time.Time, ok bool) {
	database, measurement, partition, hourly, ok := parsePartitionPath(path)
	if !ok {
		return "", "", time.Time{}, false
	}
	m := compactedOutputName.FindStringSubmatch(path[strings.LastIndex(path, "/")+1:])
	if m == nil || m[1] != measurement || (m[2] == "compacted") != hourly {
		return "", "", time.Time{}, false
	}
	return database, measurement, partition, true
}

// AdoptableObject reports whether an unlisted object at path may be
// registered as the compaction output it is: ManifestAdoption, owned by no
// staged compaction (a finalizing job's input is out of the manifest on
// purpose), not a row DELETE's leftover (another member of its rewrite
// family is listed), and not one the tiering metadata records as cold. The
// reconciler treats an object this refuses as an orphan.
func (c *Coordinator) AdoptableObject(ctx context.Context, path string) bool {
	if !c.ManifestAdoption() {
		return false
	}
	if _, _, _, ok := parseCompactedOutputPath(path); !ok || c.CompactionOwns(path) {
		return false
	}
	// A row DELETE rewrote it and the manifest lists the rewrite (or the
	// file it was rewritten from), or the DELETE journal names it: it is a
	// leftover the DELETE could not remove (or a rewrite still being
	// settled), and registering it would count its rows twice, for good.
	if journaled, known := c.retiredByDelete(ctx, path); c.HasRewriteSibling(path) || journaled || !known {
		return false
	}
	c.repair.mu.Lock()
	cold := c.repair.coldPath
	c.repair.mu.Unlock()
	return cold == nil || !cold(ctx, path)
}

// HasRewriteSibling reports whether the manifest lists another member of
// path's rewrite family (storage.RewriteFamily).
func (c *Coordinator) HasRewriteSibling(path string) bool {
	if c == nil || c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	return fsm != nil && fsm.HasRewriteSibling(path)
}

// ReconcilerKeeps is the reconciler's filter for unlisted objects: adoptable
// ones are the repair's, and a file the DELETE journal names is the
// drainer's (it may still be the current copy while its rewrite settles);
// neither is an orphan to remove.
func (c *Coordinator) ReconcilerKeeps(ctx context.Context, path string) bool {
	journaled, known := c.retiredByDelete(ctx, path)
	return journaled || !known || c.AdoptableObject(ctx, path)
}

// retiredJournalTTL bounds how stale the cached view of the DELETE journal
// may be.
const retiredJournalTTL = 30 * time.Second

// retiredByDelete reports whether the DELETE journal (storage.ReadRetired)
// names path, from a view refreshed at most every retiredJournalTTL (read
// outside the lock). known is false until a read has succeeded: callers
// then fail closed (nothing is adopted, nothing is an orphan).
func (c *Coordinator) retiredByDelete(ctx context.Context, path string) (journaled, known bool) {
	if c == nil || c.storage == nil {
		return false, true
	}
	c.retiredJournal.mu.Lock()
	stale := time.Since(c.retiredJournal.at) >= retiredJournalTTL
	if stale {
		c.retiredJournal.at = time.Now() // one reader per refresh
	}
	c.retiredJournal.mu.Unlock()
	if stale {
		r, err := storage.ReadRetired(ctx, c.storage)
		c.retiredJournal.mu.Lock()
		if err == nil {
			c.retiredJournal.paths, c.retiredJournal.known = r, true
		} else {
			c.logger.Warn().Err(err).Msg("Could not read the DELETE journal; unlisted objects are neither adopted nor treated as orphans until it can be read")
		}
		c.retiredJournal.mu.Unlock()
	}
	c.retiredJournal.mu.Lock()
	defer c.retiredJournal.mu.Unlock()
	return c.retiredJournal.paths[path], c.retiredJournal.known
}

// RewriteSiblings returns the registered members of path's rewrite family
// other than path.
func (c *Coordinator) RewriteSiblings(path string) []raft.FileEntry {
	if c == nil || c.raftNode == nil {
		return nil
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return nil
	}
	return fsm.RewriteSiblings(path)
}

// ErrNotAdoptable: the object is gone, changed while it was read, or not one
// that may be registered.
var ErrNotAdoptable = errors.New("object cannot be registered in the cluster manifest")

// AdoptObject registers the compaction output at path, which the manifest
// does not list, reading it for its checksum. The FSM refuses it if the path
// was registered or a staged compaction took it meanwhile
// (raft.ManifestChangedError). The returned entry is what was registered.
func (c *Coordinator) AdoptObject(ctx context.Context, path string) (raft.FileEntry, error) {
	if !c.AdoptableObject(ctx, path) {
		return raft.FileEntry{}, ErrNotAdoptable
	}
	size, sha, err := c.objectChecksum(ctx, path)
	if err != nil {
		return raft.FileEntry{}, err
	}
	return c.adopt(ctx, path, size, sha)
}

// AdoptRewrite registers a DELETE's rewrite of an object the manifest did not
// list (on a shared object store, #1207), so the rows the DELETE kept are a
// listed file rather than an orphan the reconciler removes.
func (c *Coordinator) AdoptRewrite(ctx context.Context, path string, size int64, sha string) (raft.FileEntry, error) {
	if !c.ManifestAdoption() {
		return raft.FileEntry{}, ErrNotAdoptable
	}
	return c.adopt(ctx, path, size, sha)
}

func (c *Coordinator) adopt(ctx context.Context, path string, size int64, sha string) (raft.FileEntry, error) {
	database, measurement, partition, _, ok := parsePartitionPath(path)
	if !ok {
		return raft.FileEntry{}, ErrNotAdoptable
	}
	entry := raft.FileEntry{
		Path: path, SHA256: sha, SizeBytes: size,
		Database: database, Measurement: measurement, PartitionTime: partition,
		OriginNodeID: c.LocalNodeID(), Tier: "hot", CreatedAt: time.Now().UTC(),
	}
	payload, err := json.Marshal(raft.RegisterFilePayload{File: entry, Adopt: true})
	if err != nil {
		return raft.FileEntry{}, err
	}
	if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandRegisterFile, Payload: payload}); err != nil {
		return raft.FileEntry{}, err
	}
	return entry, nil
}

// objectChecksum returns the size and SHA-256 of the object at path: the
// checksum its metadata records, or read whole. The size is checked again
// after a read, so an object replaced or removed meanwhile is not adopted.
func (c *Coordinator) objectChecksum(ctx context.Context, path string) (int64, string, error) {
	if oc, ok := c.storage.(storage.ObjectCopier); ok {
		size, sha, err := oc.ObjectSHA(ctx, path)
		if err != nil {
			return 0, "", err
		}
		if size < 0 {
			return 0, "", ErrNotAdoptable
		}
		if sha != "" {
			return size, sha, nil
		}
	}
	h := sha256.New()
	cw := &countingWriter{w: h}
	if err := c.storage.ReadTo(ctx, path, cw); err != nil {
		if storage.IsNotFound(err) {
			return 0, "", ErrNotAdoptable
		}
		return 0, "", err
	}
	size, err := c.storage.StatFile(ctx, path)
	if err != nil {
		return 0, "", err
	}
	if size != cw.n {
		return 0, "", ErrNotAdoptable
	}
	return size, hex.EncodeToString(h.Sum(nil)), nil
}

type countingWriter struct {
	w interface{ Write([]byte) (int, error) }
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

// manifestRepairLoop runs the repair on the leader: manifestRepairFirst
// after it becomes leader, then every manifestRepairEvery
// (manifestRepairRetry after a run that stopped early).
func (c *Coordinator) manifestRepairLoop(ctx context.Context) {
	t := time.NewTicker(manifestRepairFirst)
	defer t.Stop()
	var leaderSince, next time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if !c.raftNode.IsLeader() {
			if !leaderSince.IsZero() {
				c.repair.mu.Lock()
				c.repair.last = time.Time{}
				c.repair.mu.Unlock()
			}
			leaderSince, next = time.Time{}, time.Time{}
			continue
		}
		now := time.Now()
		if leaderSince.IsZero() {
			leaderSince, next = now, now.Add(manifestRepairFirst)
		}
		if now.Before(next) || !c.repairMayAct() {
			continue
		}
		err := c.repairManifest(ctx)
		next = time.Now().Add(manifestRepairEvery)
		if err != nil && ctx.Err() == nil {
			next = time.Now().Add(manifestRepairRetry)
			c.logger.Warn().Err(err).Msg("Cluster manifest repair stopped early; it runs again later")
		}
	}
}

// repairMayAct: this node is the leader, compaction commit is on (every FSM
// checks the repair's conditional changes) and no compaction pause (a
// restore) is in force. Checked before every change.
func (c *Coordinator) repairMayAct() bool {
	return c.raftNode.IsLeader() && c.CompactionCommitActive() && !c.CompactionPaused()
}

var errRepairStopped = errors.New("no longer the leader, or a compaction pause began")

// repairManifest runs one repair: legacy compaction tiers everywhere, then,
// on a shared object store, every database the manifest lists (a database it
// does not list has no entry to repair, and its objects are not ones
// compaction consumed from the manifest).
func (c *Coordinator) repairManifest(ctx context.Context) (err error) {
	if err := c.SyncManifest(ctx); err != nil {
		return err
	}
	var dropped, adopted, retiered, pending int
	defer func() {
		msg := ""
		if err != nil {
			msg = err.Error()
		}
		c.repair.mu.Lock()
		c.repair.last, c.repair.dropped, c.repair.adopted, c.repair.retiered, c.repair.pending, c.repair.err =
			time.Now(), dropped, adopted, retiered, pending, msg
		c.repair.mu.Unlock()
		metrics.Get().SetManifestRepairPending(int64(pending))
		if dropped > 0 || adopted > 0 || retiered > 0 || pending > 0 {
			c.logger.Info().Int("dropped", dropped).Int("adopted", adopted).Int("retiered", retiered).Int("pending", pending).
				Msg("Cluster manifest repaired (entries whose object is gone dropped, unregistered compaction outputs registered, compaction tier entries moved to hot)")
		}
	}()
	if retiered, err = c.retierLegacyEntries(ctx); err != nil {
		return err
	}
	lister, ok := c.storage.(storage.ObjectLister)
	if !c.SharedObjectStore() || !ok {
		return nil
	}
	budget := &repairBudget{}
	for _, db := range c.raftNode.FSM().FileDatabases() {
		if strings.Contains(db, "/") || storage.IsReservedRootDir(db) {
			continue
		}
		measurements, err := listMeasurements(ctx, c.storage, lister, db)
		if err != nil {
			return fmt.Errorf("list %s: %w", db, err)
		}
		for _, m := range measurements {
			m = strings.Trim(strings.TrimPrefix(m, db+"/"), "/")
			if m == "" || strings.Contains(m, "/") {
				continue
			}
			d, a, err := c.repairMeasurement(ctx, lister, db, m, budget)
			dropped, adopted = dropped+d, adopted+a
			if err != nil {
				pending = budget.pending
				return err
			}
		}
	}
	pending = budget.pending
	return nil
}

// listMeasurements lists the measurement directories of db: one listing per
// measurement keeps the repair from holding a whole database's listing.
func listMeasurements(ctx context.Context, store storage.Backend, lister storage.ObjectLister, db string) ([]string, error) {
	if dirs, ok := store.(storage.DirectoryLister); ok {
		return dirs.ListDirectories(ctx, db+"/")
	}
	objects, err := lister.ListObjects(ctx, db+"/")
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	for _, o := range objects {
		rest := strings.TrimPrefix(o.Path, db+"/")
		if m, _, found := strings.Cut(rest, "/"); found && !seen[m] {
			seen[m] = true
			out = append(out, m)
		}
	}
	return out, nil
}

// repairBudget bounds the adoptions of one run.
type repairBudget struct {
	adopted int
	bytes   int64
	pending int
}

// allows reports whether an object of size may be read now; one larger than
// the whole byte budget is read alone, first.
func (b *repairBudget) allows(size int64) bool {
	return b.adopted < manifestRepairMaxAdopt && (b.bytes == 0 || b.bytes+size <= manifestRepairMaxBytes)
}

// repairMeasurement repairs database/measurement/: adoptions first, then
// drops (none while adoptions wait for a later run, so the database stays
// one the manifest lists).
func (c *Coordinator) repairMeasurement(ctx context.Context, lister storage.ObjectLister, db, m string, budget *repairBudget) (dropped, adopted int, err error) {
	prefix := db + "/" + m + "/"
	objects, err := lister.ListObjects(ctx, prefix)
	if err != nil {
		return 0, 0, fmt.Errorf("list %s: %w", prefix, err)
	}
	fsm := c.raftNode.FSM()
	listed := make(map[string]bool, len(objects))
	waiting := 0
	for _, o := range objects {
		listed[o.Path] = true
		if _, reg := fsm.GetFile(o.Path); reg || time.Since(o.LastModified) < manifestRepairGrace || !c.AdoptableObject(ctx, o.Path) {
			continue
		}
		if !budget.allows(o.Size) {
			budget.pending++
			waiting++
			continue
		}
		if !c.repairMayAct() {
			return 0, adopted, errRepairStopped
		}
		if _, err := c.AdoptObject(ctx, o.Path); err != nil {
			if raft.IsManifestChanged(err) || errors.Is(err, ErrNotAdoptable) {
				continue
			}
			return 0, adopted, err
		}
		adopted++
		budget.adopted++
		budget.bytes += o.Size
		metrics.Get().IncManifestRepairAdopted()
	}
	if waiting > 0 {
		return 0, adopted, nil
	}
	var gone []raft.FileEntry
	for _, e := range fsm.GetFilesByDatabase(db) {
		if listed[e.Path] || !raft.HotTier(e.Tier) || !strings.HasPrefix(e.Path, prefix) {
			continue
		}
		// Held first: a finalizing output copied into place and ended
		// between a stat and the check would read as gone.
		if c.CompactionOwns(e.Path) || c.CompactionOutputHeld(e.Path) {
			continue
		}
		size, err := c.storage.StatFile(ctx, e.Path)
		if err != nil {
			return 0, adopted, err
		}
		if size >= 0 {
			continue // written after the listing
		}
		gone = append(gone, *e)
	}
	for len(gone) > 0 {
		n := min(len(gone), manifestRepairBatch)
		if !c.repairMayAct() {
			return dropped, adopted, errRepairStopped
		}
		d, err := c.dropEntries(ctx, gone[:n])
		dropped += d
		if err != nil {
			return dropped, adopted, err
		}
		gone = gone[n:]
	}
	return dropped, adopted, nil
}

// dropEntries removes entries whose object is gone, each only if still
// registered at the checksum checked: in one Raft entry, or one by one when
// that is refused because one of them changed.
func (c *Coordinator) dropEntries(ctx context.Context, entries []raft.FileEntry) (int, error) {
	ops := make([]raft.BatchFileOp, 0, len(entries))
	for _, e := range entries {
		raw, err := json.Marshal(raft.DeleteFilePayload{Path: e.Path, Reason: manifestRepairReason, ExpectSHA: e.SHA256})
		if err != nil {
			return 0, err
		}
		ops = append(ops, raft.BatchFileOp{Type: raft.CommandDeleteFile, Payload: raw})
	}
	err := c.applyBatch(ctx, ops)
	if err == nil {
		for range ops {
			metrics.Get().IncManifestRepairDropped()
		}
		return len(ops), nil
	}
	if !raft.IsManifestChanged(err) {
		return 0, err
	}
	n := 0
	for _, op := range ops {
		if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandDeleteFile, Payload: op.Payload}); err != nil {
			if raft.IsManifestChanged(err) {
				continue
			}
			return n, err
		}
		n++
		metrics.Get().IncManifestRepairDropped()
	}
	return n, nil
}

func (c *Coordinator) applyBatch(ctx context.Context, ops []raft.BatchFileOp) error {
	raw, err := json.Marshal(raft.BatchFileOpsPayload{Ops: ops})
	if err != nil {
		return err
	}
	return c.applyOrForward(ctx, &raft.Command{Type: raft.CommandBatchFileOps, Payload: raw})
}

// retierLegacyEntries moves entries registered in a compaction tier
// ("hourly", "daily": compaction before #1207) to "hot", each only if still
// registered as read, so a staged compaction can take them as inputs.
func (c *Coordinator) retierLegacyEntries(ctx context.Context) (int, error) {
	fsm := c.raftNode.FSM()
	var ops []raft.BatchFileOp
	n := 0
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		if !c.repairMayAct() {
			return errRepairStopped
		}
		err := c.applyBatch(ctx, ops)
		if err == nil {
			n += len(ops)
		} else if raft.IsManifestChanged(err) {
			for _, op := range ops {
				if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandUpdateFile, Payload: op.Payload}); err == nil {
					n++
				} else if !raft.IsManifestChanged(err) {
					return err
				}
			}
		} else {
			return err
		}
		ops = ops[:0]
		return nil
	}
	for _, db := range fsm.FileDatabases() {
		for _, e := range fsm.GetFilesByDatabase(db) {
			if e.Tier != "hourly" && e.Tier != "daily" {
				continue
			}
			up := *e
			up.Tier = "hot"
			raw, err := json.Marshal(raft.UpdateFilePayload{File: up, ExpectSHA: e.SHA256, ExpectTier: e.Tier})
			if err != nil {
				return n, err
			}
			ops = append(ops, raft.BatchFileOp{Type: raft.CommandUpdateFile, Payload: raw})
			if len(ops) == manifestRepairBatch {
				if err := flush(); err != nil {
					return n, err
				}
			}
		}
	}
	return n, flush()
}
