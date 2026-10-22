package cluster

import (
	"context"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
// two sources: the FSM's staged jobs for object stores (known on every node
// from the staging, long before the copy), and, on local storage, the files
// this node was told to delete that are still on its disk. The same set
// covers retention's and DELETE's whole-file removals still waiting for their
// unlink.

// objectSeenTombstone is how long an object job's inputs stay excluded after
// the job leaves the FSM: longer than DuckDB's object-store glob cache (10 s),
// so a listing cached before the deletes is not read whole afterwards.
const objectSeenTombstone = 15 * time.Second

// openState is the coordinator's view of open compactions.
type openState struct {
	mu sync.Mutex
	// localDeleted: files this node's manifest deleted, still waiting for
	// their local unlink, with the delete reason.
	localDeleted map[string]string
	// seen: object jobs whose output has been seen at its final key, with
	// their inputs (kept for the tombstone after the job leaves the FSM).
	seen map[string]seenObjectJob
	gen  atomic.Uint64
	// onOutputSeen runs the first time an object job's output is seen (drop
	// DuckDB's object-store listing cache when it is on).
	onOutputSeen atomic.Pointer[func()]
}

type seenObjectJob struct {
	output string
	inputs []string
	gone   time.Time // when the job left the FSM; zero while it is there
}

func (s *openState) init() {
	if s.localDeleted == nil {
		s.localDeleted = map[string]string{}
		s.seen = map[string]seenObjectJob{}
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
// unlink (localDeleteExcludable says when). Other reasons (a DELETE rewrite's
// old file, a snapshot drop, a repair) are not tracked: the copy is read until
// the unlink, as before, and a mass drop costs reads nothing.
func excludableReason(reason string) bool {
	return strings.HasPrefix(reason, "compaction:") || strings.HasPrefix(reason, "retention:") || reason == "delete"
}

// noteLocalDeleted records a manifest delete this node must still unlink.
// Called from the FSM delete callback: map work only.
func (c *Coordinator) noteLocalDeleted(path, reason string) {
	if !excludableReason(reason) {
		return
	}
	c.open.mu.Lock()
	c.open.init()
	c.open.localDeleted[path] = reason
	c.open.mu.Unlock()
	c.open.gen.Add(1)
}

// forgetLocalDeleted drops path: its copy was unlinked, or the manifest lists
// it again.
func (c *Coordinator) forgetLocalDeleted(path string) {
	c.open.mu.Lock()
	_, ok := c.open.localDeleted[path]
	if ok {
		delete(c.open.localDeleted, path)
	}
	c.open.mu.Unlock()
	if ok {
		c.open.gen.Add(1)
	}
}

// localDeleteExcludable reports whether a local copy deleted for reason may be
// left out of reads now: a compaction input once its output is on this node's
// disk, a retention or whole-file DELETE removal at once (its rows are gone).
// Anything else (a DELETE rewrite's old file, a snapshot drop) is read until
// its unlink, as before.
func (c *Coordinator) localDeleteExcludable(ctx context.Context, reason string) bool {
	switch {
	case strings.HasPrefix(reason, "compaction:"):
		job := strings.TrimPrefix(reason, "compaction:")
		fsm := c.raftNode.FSM()
		if fsm == nil {
			return false
		}
		out, ok := fsm.FileByCompactionJob(job)
		if !ok {
			return false
		}
		size, err := c.storage.StatFile(ctx, out.Path)
		return err == nil && size >= 0
	case strings.HasPrefix(reason, "retention:"), reason == "delete":
		return true
	}
	return false
}

// OpenExcludedFor returns the storage keys under prefix ("db/measurement/")
// a read on this node must leave out so each compaction's rows count once.
func (c *Coordinator) OpenExcludedFor(ctx context.Context, prefix string) []string {
	if c == nil || c.raftNode == nil || c.storage == nil {
		return nil
	}
	var out []string
	out = append(out, c.objectExcluded(ctx, prefix)...)
	out = append(out, c.localExcluded(ctx, prefix)...)
	return out
}

// OpenAny reports whether a read under prefix must not be cached: an object
// job or a pending local delete there can change what it leaves out.
func (c *Coordinator) OpenAny(prefix string) bool {
	if c == nil || c.raftNode == nil {
		return false
	}
	if fsm := c.raftNode.FSM(); fsm != nil {
		for _, s := range fsm.StagedCompactions() {
			if s.Object && strings.HasPrefix(s.Output.Path, prefix) {
				return true
			}
		}
	}
	c.open.mu.Lock()
	defer c.open.mu.Unlock()
	for p := range c.open.localDeleted {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	for _, j := range c.open.seen {
		if strings.HasPrefix(j.output, prefix) {
			return true
		}
	}
	return false
}

// OpenGeneration changes whenever the set of open compactions or pending
// local deletes changes (a cache of SQL built without them keys on it).
func (c *Coordinator) OpenGeneration() uint64 {
	if c == nil || c.raftNode == nil {
		return 0
	}
	h := fnv.New64a()
	if fsm := c.raftNode.FSM(); fsm != nil {
		for _, s := range fsm.StagedCompactions() {
			if !s.Object {
				continue
			}
			h.Write([]byte(s.Job))
			if s.Finalizing {
				h.Write([]byte{1})
			}
			if s.Finalized {
				h.Write([]byte{2})
			}
		}
	}
	h.Write([]byte(strconv.FormatUint(c.open.gen.Load(), 10)))
	return h.Sum64()
}

// objectExcluded: the inputs of every object job under prefix whose output is
// at its final key. Known from the staging on every node, so a reader whose
// FSM lags the commit still knows the job. An input the manifest lists again
// with other content, or a writer still holds for registration, is kept.
func (c *Coordinator) objectExcluded(ctx context.Context, prefix string) []string {
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return nil
	}
	now := time.Now()
	present := map[string]bool{}
	var out []string
	for _, s := range fsm.StagedCompactions() {
		if !s.Object || !strings.HasPrefix(s.Output.Path, prefix) {
			continue
		}
		present[s.Job] = true
		c.open.mu.Lock()
		c.open.init()
		_, seen := c.open.seen[s.Job]
		c.open.mu.Unlock()
		if !seen {
			size, err := c.storage.StatFile(ctx, s.Output.Path)
			if err != nil || size < 0 {
				continue
			}
			inputs := make([]string, 0, len(s.Inputs))
			for _, in := range s.Inputs {
				inputs = append(inputs, in.Path)
			}
			c.open.mu.Lock()
			c.open.seen[s.Job] = seenObjectJob{output: s.Output.Path, inputs: inputs}
			c.open.mu.Unlock()
			c.open.gen.Add(1)
			if fn := c.open.onOutputSeen.Load(); fn != nil {
				(*fn)()
			}
		}
		holds := c.staged.holds
		for _, in := range s.Inputs {
			if e, ok := fsm.GetFile(in.Path); ok && e.SHA256 != in.SHA256 {
				continue
			}
			if holds != nil && holds(in.Path) {
				continue
			}
			out = append(out, in.Path)
		}
	}
	// Jobs that left the FSM: tombstone their inputs a while, then forget.
	c.open.mu.Lock()
	changed := false
	for job, j := range c.open.seen {
		if present[job] || !strings.HasPrefix(j.output, prefix) {
			continue
		}
		if j.gone.IsZero() {
			j.gone = now
			c.open.seen[job] = j
		}
		if now.Sub(j.gone) > objectSeenTombstone {
			delete(c.open.seen, job)
			changed = true
			continue
		}
		out = append(out, j.inputs...)
	}
	c.open.mu.Unlock()
	if changed {
		c.open.gen.Add(1)
		if fn := c.open.onOutputSeen.Load(); fn != nil {
			(*fn)()
		}
	}
	return out
}

// localExcluded: this node's copies the manifest deleted that may be left out
// now (localDeleteExcludable). Stops when the deletes look implausible (the
// stale-local breaker): over-counting is the safe direction.
func (c *Coordinator) localExcluded(ctx context.Context, prefix string) []string {
	c.open.mu.Lock()
	var cands []string
	var reasons []string
	for p, r := range c.open.localDeleted {
		if strings.HasPrefix(p, prefix) {
			cands = append(cands, p)
			reasons = append(reasons, r)
		}
	}
	c.open.mu.Unlock()
	if len(cands) == 0 {
		return nil
	}
	if seen := c.stale.seen.Load(); seen != nil && seen.err() == nil {
		if fsm := c.raftNode.FSM(); fsm != nil && c.deletedCopiesImplausible(seen, fsm) {
			return nil
		}
	}
	var out []string
	for i, p := range cands {
		// A copy already gone (the stale sweep, an operator) needs nothing,
		// and its entry would keep reads uncached.
		if size, err := c.storage.StatFile(ctx, p); err == nil && size < 0 {
			c.forgetLocalDeleted(p)
			continue
		}
		if c.localDeleteExcludable(ctx, reasons[i]) {
			out = append(out, p)
		}
	}
	return out
}
