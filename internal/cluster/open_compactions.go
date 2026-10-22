package cluster

import (
	"context"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// Queries count a cluster compaction's rows once (the cluster side of #1337).
//
// A staged compaction (#1207) moves its output to its final key and only then
// are its inputs removed, on every node that reads them:
//
//   - On an object store the finisher copies the output from its staged key
//     and then deletes the input objects (compaction_finisher.go): every node
//     reads the shared bucket, and a dead finisher holds the window until the
//     takeover.
//   - On local storage each node moves its staged copy into place at the
//     commit and unlinks its own input copies later (the delete workers, after
//     deleteGrace and waitStagedAdoption).
//
// In between a read sees the output and its inputs. The coordinator names the
// inputs to leave out (OpenExcludedFor) once their output can be read, from
// two sources: the FSM's committed object jobs (known on every node from the
// commit, before the copy), and, on local storage, the files this node was
// told to delete that are still on its disk. The same set covers retention's
// and DELETE's whole-file removals still waiting for their unlink.
//
// Nothing is kept once an object job leaves the FSM: its inputs are deleted
// by then, and a listing DuckDB cached before the deletes (query.
// enable_s3_cache) fails on a missing file whether or not it is left out.

// openState is the coordinator's view of open compactions.
type openState struct {
	mu sync.Mutex
	// localDeleted: files this node's manifest deleted, still waiting for
	// their local unlink.
	localDeleted map[string]*localDelete
	// seen: committed object jobs whose output has been seen at its final
	// key (the hook runs once per job; the key is not checked again).
	seen map[string]bool
	gen  atomic.Uint64
	// onOutputSeen runs the first time an object job's output is seen (drop
	// DuckDB's object-store listing cache when it is on).
	onOutputSeen atomic.Pointer[func()]
}

// localDelete is one pending local unlink. For a compaction input, output is
// the job's registered output once found (resolved at most every
// localResolveEvery while it is not: the lookup can scan the manifest).
type localDelete struct {
	reason     string
	output     string
	outputSize int64
	resolvedAt time.Time
	// A DELETE rewrite's old file: the registered family member seen in
	// storage at its registered size (only the stat is cached; the
	// registration is checked on every read).
	sibling     string
	siblingSize int64
	// goneCheckedAt rate-limits the "copy already gone" stat.
	goneCheckedAt time.Time
}

// deleteRewriteReason is the manifest delete reason of the file a row
// DELETE rewrote (internal/api); it must stay this exact string, which the
// FSM also matches (raft.OutputGoneAccounted).
const deleteRewriteReason = "delete-rewrite"

// goneCheckEvery bounds how often a pending entry's "already gone" stat runs
// (on an object store it is a request per read).
const goneCheckEvery = 2 * time.Second

const localResolveEvery = 5 * time.Second

func (s *openState) init() {
	if s.localDeleted == nil {
		s.localDeleted = map[string]*localDelete{}
		s.seen = map[string]bool{}
	}
}

// SetOnObjectOutputSeen sets what runs the first time an object compaction's
// output is seen at its final key.
func (c *Coordinator) SetOnObjectOutputSeen(fn func()) {
	if c != nil && fn != nil {
		c.open.onOutputSeen.Store(&fn)
	}
}

// excludableReason: a delete whose local copy reads may leave out before its
// unlink (localDeleteExcludable says when). Other reasons (a snapshot drop, a
// repair) are not tracked: the copy is read until the unlink, as before, and
// a mass drop costs reads nothing.
func excludableReason(reason string) bool {
	return strings.HasPrefix(reason, "compaction:") || strings.HasPrefix(reason, "retention:") || reason == "delete" || reason == deleteRewriteReason
}

// noteLocalDeleted records a manifest delete this node must still unlink.
// Called from the FSM delete callback: map work only. The generation moves
// so a plan cached before cannot be served now; leaving the set needs no
// move, since reads under a pending delete are never cached.
func (c *Coordinator) noteLocalDeleted(path, reason string) {
	if !excludableReason(reason) {
		return
	}
	c.open.mu.Lock()
	c.open.init()
	c.open.localDeleted[path] = &localDelete{reason: reason}
	c.open.mu.Unlock()
	c.open.gen.Add(1)
}

// forgetLocalDeleted drops path: its copy was unlinked, or the manifest lists
// it again.
func (c *Coordinator) forgetLocalDeleted(path string) {
	c.open.mu.Lock()
	delete(c.open.localDeleted, path)
	c.open.mu.Unlock()
}

// localDeleteExcludable reports whether a local copy deleted for d.reason may
// be left out of reads now: a compaction input once the job's output is
// registered and on this node's disk; a DELETE rewrite's old file once a
// registered member of its rewrite family (which holds every row the DELETE
// kept) is in storage at its registered size; a retention or whole-file
// DELETE removal at once (its rows are gone).
func (c *Coordinator) localDeleteExcludable(ctx context.Context, fsm *raft.ClusterFSM, path string, d localDelete) (localDelete, bool) {
	if d.reason == deleteRewriteReason {
		return c.rewriteSiblingReadable(ctx, fsm, path, d)
	}
	job, ok := strings.CutPrefix(d.reason, "compaction:")
	if !ok {
		return d, true
	}
	if d.output == "" && time.Since(d.resolvedAt) >= localResolveEvery {
		d.resolvedAt = time.Now()
		if s, ok := fsm.StagedCompactionByJob(job); ok {
			d.output, d.outputSize = s.Output.Path, s.Output.SizeBytes
		} else if e, ok := fsm.FileByCompactionJob(job); ok {
			d.output, d.outputSize = e.Path, e.SizeBytes
		}
	}
	if d.output == "" {
		return d, false
	}
	// Still the job's output: anything else under that path (a DELETE
	// rewrite, a later compaction) has its own accounting.
	if e, ok := fsm.GetFile(d.output); !ok || e.CompactionJob != job {
		return d, false
	}
	size, err := c.storage.StatFile(ctx, d.output)
	return d, err == nil && size == d.outputSize
}

// OpenExcludedFor returns the storage keys under prefix ("db/measurement/")
// a read on this node must leave out so each compaction's rows count once.
func (c *Coordinator) OpenExcludedFor(ctx context.Context, prefix string) []string {
	if c == nil || c.raftNode == nil || c.storage == nil {
		return nil
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return nil
	}
	var out []string
	out = append(out, c.objectExcluded(ctx, fsm, prefix)...)
	out = append(out, c.localExcluded(ctx, fsm, prefix)...)
	return out
}

// OpenAny reports whether a read under prefix must not be cached: a
// committed object job or a pending local delete there can change what it
// leaves out.
func (c *Coordinator) OpenAny(prefix string) bool {
	if c == nil || c.raftNode == nil {
		return false
	}
	if fsm := c.raftNode.FSM(); fsm != nil && fsm.FinalizingObjectUnder(prefix) {
		return true
	}
	c.open.mu.Lock()
	defer c.open.mu.Unlock()
	for p := range c.open.localDeleted {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

// OpenGeneration changes whenever an object job stages, commits or leaves,
// or a local delete is recorded (a cache of SQL built without them keys on
// it).
func (c *Coordinator) OpenGeneration() uint64 {
	if c == nil || c.raftNode == nil {
		return 0
	}
	h := fnv.New64a()
	if fsm := c.raftNode.FSM(); fsm != nil {
		h.Write([]byte(strconv.FormatUint(fsm.ObjectStagedSignature(), 10)))
	}
	h.Write([]byte{0})
	h.Write([]byte(strconv.FormatUint(c.open.gen.Load(), 10)))
	return h.Sum64()
}

// objectExcluded: the inputs of every committed object job under prefix
// whose output is registered by the job and at its final key. Known from the
// commit on every node, so a reader whose FSM lags the finisher still knows
// the job. A job whose output left the manifest (it is being reverted, or
// its remover deletes the inputs too) leaves nothing out; nor does an input
// the manifest lists again or this node's outbox holds, as the finisher
// keeps those (deleteObjectInputs).
func (c *Coordinator) objectExcluded(ctx context.Context, fsm *raft.ClusterFSM, prefix string) []string {
	jobs := fsm.FinalizingObjectJobsUnder(prefix)
	c.open.mu.Lock()
	c.open.init()
	if len(c.open.seen) > 0 {
		live := make(map[string]bool, len(jobs))
		for _, s := range fsm.FinalizingObjectJobsUnder("") {
			live[s.Job] = true
		}
		for job := range c.open.seen {
			if !live[job] {
				delete(c.open.seen, job)
			}
		}
	}
	c.open.mu.Unlock()
	var out []string
	hold := stagedCommitter{c: c}.HoldsRegistration
	for _, s := range jobs {
		if e, ok := fsm.GetFile(s.Output.Path); !ok || e.CompactionJob != s.Job || e.SHA256 != s.Output.SHA256 {
			continue
		}
		c.open.mu.Lock()
		seen := c.open.seen[s.Job]
		c.open.mu.Unlock()
		if !seen {
			size, err := c.storage.StatFile(ctx, s.Output.Path)
			if err != nil || size != s.Output.SizeBytes {
				continue
			}
			c.open.mu.Lock()
			first := !c.open.seen[s.Job]
			c.open.seen[s.Job] = true
			c.open.mu.Unlock()
			if first {
				c.open.gen.Add(1)
				if fn := c.open.onOutputSeen.Load(); fn != nil {
					(*fn)()
				}
			}
		}
		for _, in := range s.Inputs {
			if fsm.HasFile(in.Path) || hold(in.Path) {
				continue
			}
			out = append(out, in.Path)
		}
	}
	return out
}

// localExcluded: this node's copies the manifest deleted that may be left out
// now (localDeleteExcludable). A path the manifest lists again is dropped
// (a snapshot can re-list it without a register callback). Stops when the
// deletes look implausible (the stale-local breaker): over-counting is the
// safe direction.
func (c *Coordinator) localExcluded(ctx context.Context, fsm *raft.ClusterFSM, prefix string) []string {
	c.open.mu.Lock()
	var cands []string
	var dels []localDelete
	for p, d := range c.open.localDeleted {
		if strings.HasPrefix(p, prefix) {
			cands = append(cands, p)
			dels = append(dels, *d)
		}
	}
	c.open.mu.Unlock()
	if len(cands) == 0 {
		return nil
	}
	if seen := c.stale.seen.Load(); seen != nil && seen.err() == nil && c.deletedCopiesImplausible(seen, fsm) {
		return nil
	}
	var out []string
	for i, p := range cands {
		if fsm.HasFile(p) {
			c.forgetLocalDeleted(p)
			continue
		}
		// A copy already gone (the stale sweep, an operator) needs nothing,
		// and its entry would keep reads uncached.
		d := dels[i]
		if time.Since(d.goneCheckedAt) >= goneCheckEvery {
			d.goneCheckedAt = time.Now()
			if size, err := c.storage.StatFile(ctx, p); err == nil && size < 0 {
				c.forgetLocalDeleted(p)
				continue
			}
		}
		d, ok := c.localDeleteExcludable(ctx, fsm, p, d)
		if d != dels[i] {
			c.open.mu.Lock()
			if cur := c.open.localDeleted[p]; cur != nil && cur.reason == d.reason {
				*cur = d
			}
			c.open.mu.Unlock()
		}
		if ok {
			out = append(out, p)
		}
	}
	return out
}

// rewriteSiblingReadable: a registered member of path's rewrite family is in
// storage at its registered size. The stat of the member seen is cached;
// whether it is still registered is not (a member that left the manifest is
// looked for again). The first sighting drops DuckDB's object-store listing
// cache: a listing cached before the member landed would have path without
// it.
func (c *Coordinator) rewriteSiblingReadable(ctx context.Context, fsm *raft.ClusterFSM, path string, d localDelete) (localDelete, bool) {
	sibs := fsm.RewriteSiblings(path)
	if d.sibling != "" {
		for _, e := range sibs {
			if e.Path == d.sibling && e.SizeBytes == d.siblingSize {
				return d, true
			}
		}
		d.sibling, d.siblingSize = "", 0
	}
	for _, e := range sibs {
		if size, err := c.storage.StatFile(ctx, e.Path); err == nil && size == e.SizeBytes {
			d.sibling, d.siblingSize = e.Path, e.SizeBytes
			if fn := c.open.onOutputSeen.Load(); fn != nil {
				(*fn)()
			}
			return d, true
		}
	}
	return d, false
}
