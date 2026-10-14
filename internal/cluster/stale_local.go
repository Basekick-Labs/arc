package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

// The stale-local sweep (#1071). A local replica leaves the disk when the
// FSM delete callback hands its path to the delete workers. That misses:
//
//   - a delete applied while this node was down: after a restart the snapshot
//     Restore diffs against the node's last local snapshot, not its disk, so
//     a file registered after that snapshot and deleted during the outage is
//     in neither manifest;
//   - an unlink queued (in memory) when the node crashed, or one that failed;
//   - a delete replayed at startup before the callbacks were installed.
//
// The sweep removes only what the delete workers would have removed: a file
// this node saw registered, at that exact identity, that the manifest no
// longer lists once it has applied past that registration, on two passes
// confirm apart. Never a file it never saw registered.

// UnlinkReasonStaleLocal is the reason reported to tiering for a copy the
// sweep removed; the manifest delete's own reason is unknown, so tiering
// treats it as possibly a migration.
const UnlinkReasonStaleLocal = "stale-local"

// Defaults; each coordinator copies them at start (tests and the rig knob
// change its own copy).
var (
	staleLocalEvery   = 5 * time.Minute
	staleLocalConfirm = 10 * time.Minute
)

// staleLocalMaxPerPass bounds the removals of one pass (a variable for tests).
var staleLocalMaxPerPass = 10000

const (
	staleLocalBreakerMin = 1000
	staleLocalFeedQueue  = 4096
	// staleLocalAsideDir is the reserved root a copy is moved to before it is
	// removed; startup empties it, except copies kept for an operator.
	staleLocalAsideDir  = "_stale_local"
	staleLocalKeepAside = "keep-"
)

type staleLocalState struct {
	seen    atomic.Pointer[seenLocal]
	base    string
	feed    chan string
	every   time.Duration
	confirm time.Duration
	// pending reports a path waiting to register here (the writer's outbox);
	// ready is set once main has wired it (or said there is none).
	pending atomic.Pointer[func(string) bool]
	ready   atomic.Bool
	// verifyContent asks the puller to re-check a listed copy's content.
	verifyContent atomic.Pointer[func(*raft.FileEntry)]
	wg            sync.WaitGroup

	holdMu    sync.Mutex
	holdCond  *sync.Cond
	held      map[string]int
	unlinking map[string]bool

	// firstUnlisted is when a pass first found each path unlisted; only the
	// sweep goroutine touches it.
	firstUnlisted map[string]time.Time
	statusMu      sync.Mutex
	status        *StaleLocalStatus
}

// StaleLocalStatus is the last pass, for the cluster status.
type StaleLocalStatus struct {
	LastRun  *time.Time `json:"last_run,omitempty"`
	Tracked  int        `json:"tracked"`
	Unlinked int        `json:"unlinked"`
	Waiting  int        `json:"waiting"`
	Blocked  bool       `json:"blocked,omitempty"`
	Error    string     `json:"error,omitempty"`
}

// StaleLocalStatus reports the sweep's last pass (nil when it does not run).
func (c *Coordinator) StaleLocalStatus() *StaleLocalStatus {
	c.stale.statusMu.Lock()
	defer c.stale.statusMu.Unlock()
	if c.stale.status == nil {
		return nil
	}
	st := *c.stale.status
	return &st
}

func (c *Coordinator) setStaleStatus(st StaleLocalStatus) {
	c.stale.statusMu.Lock()
	c.stale.status = &st
	c.stale.statusMu.Unlock()
	metrics.Get().SetStaleLocalTracked(int64(st.Tracked))
	var blocked, failed int64
	if st.Blocked {
		blocked = 1
	}
	if seen := c.stale.seen.Load(); seen == nil || seen.err() != nil {
		failed = 1
	}
	metrics.Get().SetStaleLocalBlocked(blocked)
	metrics.Get().SetStaleLocalTrackingFailed(failed)
}

// SetStaleLocalPending wires what a path waiting to register here looks like
// (nil when this node registers nothing) and lets the sweep start: on a writer
// it must not run before its outbox can answer.
func (c *Coordinator) SetStaleLocalPending(fn func(path string) bool) {
	if fn != nil {
		c.stale.pending.Store(&fn)
	}
	c.stale.ready.Store(true)
}

// HoldLocalPaths keeps the sweep away from paths about to be registered by a
// producer that registers a file it did not write just now (edge sync's
// redelivery re-registers the file it finds at the path). It waits for an
// unlink of any of them in progress, so afterwards each path either still
// holds its file or holds none. Release when the registration has applied or
// failed.
func (c *Coordinator) HoldLocalPaths(paths []string) (release func()) {
	s := &c.stale
	s.holdMu.Lock()
	s.initHoldsLocked()
	for _, p := range paths {
		for s.unlinking[p] {
			s.holdCond.Wait()
		}
		s.held[p]++
	}
	s.holdMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.holdMu.Lock()
			for _, p := range paths {
				if s.held[p]--; s.held[p] <= 0 {
					delete(s.held, p)
				}
			}
			s.holdMu.Unlock()
		})
	}
}

func (s *staleLocalState) initHoldsLocked() {
	if s.holdCond == nil {
		s.holdCond = sync.NewCond(&s.holdMu)
		s.held = map[string]int{}
		s.unlinking = map[string]bool{}
	}
}

// beginUnlink marks path as being unlinked unless a producer holds it.
func (s *staleLocalState) beginUnlink(path string) bool {
	s.holdMu.Lock()
	defer s.holdMu.Unlock()
	s.initHoldsLocked()
	if s.held[path] > 0 {
		return false
	}
	s.unlinking[path] = true
	return true
}

func (s *staleLocalState) endUnlink(path string) {
	s.holdMu.Lock()
	delete(s.unlinking, path)
	s.holdCond.Broadcast()
	s.holdMu.Unlock()
}

// staleLocalEnabled: local storage with replication, where the delete workers
// own local unlinks, and somewhere to keep the journal.
func (c *Coordinator) staleLocalEnabled() bool {
	return c.cfg.StaleLocalSweep && c.cfg.ReplicationEnabled && c.cfg.ReplicationCatchUpEnabled &&
		c.cfg.RaftDataDir != "" && c.storage != nil && c.storage.Type() == "local"
}

// startStaleLocalLocked starts the sweep. Called from startFilePullerLocked
// (c.mu held), before the FSM callbacks: no I/O here, the journal is opened
// by the sweep goroutine.
func (c *Coordinator) startStaleLocalLocked() {
	if !c.staleLocalEnabled() {
		return
	}
	bp, ok := c.storage.(interface{ GetBasePath() string })
	if !ok {
		return
	}
	s := &c.stale
	s.base = bp.GetBasePath()
	s.feed = make(chan string, staleLocalFeedQueue)
	s.firstUnlisted = map[string]time.Time{}
	s.every, s.confirm = staleLocalEvery, staleLocalConfirm
	// A rig knob, as ARC_TEST_FINISHER_DELAY: passes this often, confirmed
	// after two of them.
	if v := os.Getenv("ARC_TEST_STALE_LOCAL_EVERY"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			s.every, s.confirm = d, 2*d
		}
	}
	s.wg.Add(1)
	go c.staleLocalRun(c.ctx)
}

// stopStaleLocal joins the sweep and closes the journal (after the Raft join:
// nothing feeds it any more).
func (c *Coordinator) stopStaleLocal() {
	c.stale.wg.Wait()
	if seen := c.stale.seen.Load(); seen != nil {
		seen.close()
	}
}

// staleLocalRun opens the journal, then feeds the set and runs passes.
func (c *Coordinator) staleLocalRun(ctx context.Context) {
	s := &c.stale
	defer s.wg.Done()
	c.removeStaleAsides()
	seen, err := openSeenLocal(c.cfg.RaftDataDir, c.cfg.ClusterName)
	if err != nil {
		c.logger.Error().Err(err).Msg("Stale local copy tracking not started; copies whose delete this node misses stay on disk")
		c.setStaleStatus(StaleLocalStatus{Error: "tracking not started: " + err.Error()})
		return
	}
	s.seen.Store(seen)
	t := time.NewTicker(s.every)
	defer t.Stop()
	syncT := time.NewTicker(time.Second)
	defer syncT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-syncT.C:
			seen.sync()
		case p := <-s.feed:
			if e, ok := c.raftNode.FSM().GetFile(p); ok {
				c.observeLocal(e, true)
			}
		case <-t.C:
			c.staleLocalPass(ctx)
		}
	}
}

// noteLocalRegistered hands a path the manifest lists to the sweep goroutine.
// Never blocks (FSM callbacks call it): a dropped path is recorded by the next
// manifest walk.
func (c *Coordinator) noteLocalRegistered(path string) {
	if c.stale.feed == nil {
		return
	}
	select {
	case c.stale.feed <- path:
	default:
	}
}

// forgetLocal drops path from the set (its copy was removed).
func (c *Coordinator) forgetLocal(path string) {
	if seen := c.stale.seen.Load(); seen != nil {
		seen.remove(path)
	}
}

// observeListed is the replication walks' hook: it records copies present
// here, and has a copy this node recorded under an earlier registration of
// the path with another checksum re-checked, since its content may be the old
// one (a snapshot install fires no callback for a path registered again while
// the node was away). A new index with the same checksum (a tier update) only
// moves the record.
func (c *Coordinator) observeListed(e *raft.FileEntry) {
	seen := c.stale.seen.Load()
	if seen == nil || !sweepablePath(e.Path) {
		return
	}
	if r, ok := seen.get(e.Path); ok && r.lsn != e.LSN {
		cur, ok := localIdentity(c.stalePath(e.Path))
		if ok && cur.size == r.size && cur.mtime == r.mtime && cur.ino == r.ino {
			sha := shaPrefix(e.SHA256)
			if sha != r.sha {
				if fn := c.stale.verifyContent.Load(); fn != nil {
					(*fn)(e)
				}
			}
			if cur.size == e.SizeBytes {
				cur.lsn, cur.sha = e.LSN, sha
				seen.add(e.Path, cur)
			}
			return
		}
	}
	c.observeLocal(e, false)
}

// observeLocal records entry's local copy if it is there at the manifest
// size. Without force, a record already at the entry's index and size is
// trusted (the walk: no stat per file per pass).
func (c *Coordinator) observeLocal(e *raft.FileEntry, force bool) {
	seen := c.stale.seen.Load()
	if seen == nil || !sweepablePath(e.Path) {
		return
	}
	if !force {
		if r, ok := seen.get(e.Path); ok && r.lsn == e.LSN && r.size == e.SizeBytes {
			return
		}
	}
	r, ok := localIdentity(c.stalePath(e.Path))
	if !ok || r.size != e.SizeBytes {
		return
	}
	r.lsn, r.sha = e.LSN, shaPrefix(e.SHA256)
	seen.add(e.Path, r)
}

func (c *Coordinator) stalePath(path string) string {
	return filepath.Join(c.stale.base, filepath.FromSlash(path))
}

// sweepablePath: a manifest data path, never a reserved root.
func sweepablePath(path string) bool {
	if path == "" || strings.Contains(path, "..") || strings.HasPrefix(path, "/") {
		return false
	}
	first, _, _ := strings.Cut(path, "/")
	return !storage.IsReservedRootDir(first)
}

func localIdentity(full string) (seenRecord, bool) {
	fi, err := os.Lstat(full)
	if err != nil || !fi.Mode().IsRegular() {
		return seenRecord{}, false
	}
	return seenRecord{size: fi.Size(), mtime: fi.ModTime().UnixNano(), ino: fileInode(fi)}, true
}

// staleLocalPass removes what is confirmed stale. A skipped pass forgets
// every first sighting, so confirmation always takes two passes that ran.
func (c *Coordinator) staleLocalPass(ctx context.Context) {
	s := &c.stale
	seen := s.seen.Load()
	if seen == nil {
		return
	}
	now := time.Now().UTC()
	st := StaleLocalStatus{LastRun: &now}
	defer func() {
		st.Tracked = seen.len()
		c.setStaleStatus(st)
	}()
	skip := func(why string) {
		st.Error = why
		clear(s.firstUnlisted)
	}
	if err := seen.err(); err != nil {
		skip("tracking stopped: " + err.Error())
		return
	}
	if !s.ready.Load() {
		skip("waiting for this node's registration outbox")
		return
	}
	if c.CompactionPaused() {
		skip("compaction pause in force")
		return
	}
	// The manifest as of now on the leader, so a path unlisted below is one
	// the cluster deleted, not one this node has yet to apply.
	if err := c.SyncManifest(ctx); err != nil {
		skip(err.Error())
		return
	}
	confirmed := c.staleCandidates(ctx, seen, time.Now())
	tracked := seen.len()
	st.Waiting = len(s.firstUnlisted) - len(confirmed)
	if len(confirmed) == 0 {
		return
	}
	frac := c.cfg.StaleLocalSweepMaxFraction
	if frac <= 0 {
		frac = 0.5
	}
	if len(confirmed) >= staleLocalBreakerMin && float64(len(confirmed)) > frac*float64(tracked) {
		st.Blocked = true
		c.logger.Error().Int("stale", len(confirmed)).Int("tracked", tracked).Float64("max_fraction", frac).
			Msg("Stale local copy sweep refused: it would remove more of this node's files than cluster.stale_local_sweep_max_fraction allows. A manifest rolled back by a recovery looks like this; if the deletes are real (a long outage), raise the setting and restart")
		return
	}
	if len(confirmed) > staleLocalMaxPerPass {
		confirmed = confirmed[:staleLocalMaxPerPass]
	}
	for _, p := range confirmed {
		if ctx.Err() != nil || c.CompactionPaused() {
			break
		}
		if c.unlinkStale(ctx, p) {
			st.Unlinked++
		}
	}
	if st.Unlinked > 0 {
		c.logger.Info().Int("unlinked", st.Unlinked).Int("tracked", tracked).
			Msg("Removed local copies of files the cluster deleted while this node missed the delete")
	}
}

// staleCandidates returns the paths unlisted on a pass at least confirm ago
// and still unlisted, held by nothing. A record whose file is gone is dropped.
func (c *Coordinator) staleCandidates(ctx context.Context, seen *seenLocal, now time.Time) []string {
	s := &c.stale
	fsm := c.raftNode.FSM()
	applied := fsm.AppliedDone()
	unlisted := seen.unlisted(fsm.HasFile)
	var pending func(string) bool
	if fn := s.pending.Load(); fn != nil {
		pending = *fn
	}
	cold := c.coldPathCheck()
	var out []string
	for p, r := range unlisted {
		if _, ok := localIdentity(c.stalePath(p)); !ok {
			seen.remove(p)
			delete(s.firstUnlisted, p)
			continue
		}
		if r.lsn > applied || (pending != nil && pending(p)) || c.CompactionOwns(p) ||
			(cold != nil && cold(ctx, p)) {
			delete(s.firstUnlisted, p)
			continue
		}
		first, ok := s.firstUnlisted[p]
		if !ok {
			s.firstUnlisted[p] = now
			continue
		}
		if now.Sub(first) >= s.confirm {
			out = append(out, p)
		}
	}
	for p := range s.firstUnlisted {
		if _, ok := unlisted[p]; !ok {
			delete(s.firstUnlisted, p)
		}
	}
	return out
}

func (c *Coordinator) coldPathCheck() func(context.Context, string) bool {
	c.repair.mu.Lock()
	defer c.repair.mu.Unlock()
	return c.repair.coldPath
}

// unlinkStale removes path's copy if it is still the file this node saw
// registered and nothing lists, holds or keeps the path. The file is moved
// aside first and checked there, so a file written to the path meanwhile (a
// restore, a pull) is put back, never removed.
func (c *Coordinator) unlinkStale(ctx context.Context, path string) bool {
	s := &c.stale
	seen := s.seen.Load()
	defer delete(s.firstUnlisted, path)
	rec, ok := seen.get(path)
	if !ok {
		return false
	}
	if !s.beginUnlink(path) {
		return false
	}
	unlinking := true
	defer func() {
		if unlinking {
			s.endUnlink(path)
		}
	}()
	if c.raftNode.FSM().HasFile(path) || c.CompactionOwns(path) {
		return false
	}
	if fn := s.pending.Load(); fn != nil && (*fn)(path) {
		return false
	}
	if cold := c.coldPathCheck(); cold != nil && cold(ctx, path) {
		return false
	}
	full := c.stalePath(path)
	cur, ok := localIdentity(full)
	if !ok {
		seen.remove(path)
		return false
	}
	if cur.size != rec.size || cur.mtime != rec.mtime || cur.ino != rec.ino {
		seen.remove(path) // another file now: never this sweep's to remove
		return false
	}
	asideDir := filepath.Join(s.base, staleLocalAsideDir)
	if err := os.MkdirAll(asideDir, 0o700); err != nil {
		c.logger.Warn().Err(err).Str("path", path).Msg("Could not remove a stale local copy; the next pass retries")
		return false
	}
	aside := filepath.Join(asideDir, randomSuffix())
	if err := os.Rename(full, aside); err != nil {
		c.logger.Warn().Err(err).Str("path", path).Msg("Could not remove a stale local copy; the next pass retries")
		return false
	}
	moved, ok := localIdentity(aside)
	if !ok || moved.size != rec.size || moved.mtime != rec.mtime || moved.ino != rec.ino {
		// Replaced between the check and the rename: put it back.
		if err := os.Link(aside, full); err != nil {
			keep := filepath.Join(asideDir, staleLocalKeepAside+randomSuffix())
			_ = os.Rename(aside, keep)
			c.logger.Error().Err(err).Str("path", path).Str("kept_at", keep).
				Msg("A file written to a stale path while it was being removed could not be put back; it is kept aside, move it back by hand")
			return false
		}
		_ = os.Remove(aside)
		seen.remove(path)
		return false
	}
	removeErr := os.Remove(aside)
	s.endUnlink(path)
	unlinking = false
	if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		c.logger.Warn().Err(removeErr).Str("aside", aside).Msg("Could not remove a stale local copy moved aside; the next start removes it")
	}
	seen.remove(path)
	c.recordUnlinkedFileInTiering(path, UnlinkReasonStaleLocal, rec.size)
	metrics.Get().IncStaleLocalUnlinked()
	return true
}

func randomSuffix() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// removeStaleAsides removes copies a crash left moved aside, keeping the ones
// set aside for an operator.
func (c *Coordinator) removeStaleAsides() {
	dir := filepath.Join(c.stale.base, staleLocalAsideDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), staleLocalKeepAside) {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}
