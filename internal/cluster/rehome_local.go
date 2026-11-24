package cluster

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// Re-home on a cluster with local storage (#1860): every node holds its own
// copy of a job's files. Each node fetches the outputs into the job's staged
// directory while the job is committed, moves them into place while it is
// published, and unlinks its own copy of the source once its reads from
// before the latch ended (api/rehome_cluster_local.go). The cluster side here:
// fetching a staged output from a peer, and keeping the delete workers off
// the source while the job's participant owns its unlink.

const rehomeDeleteReasonPrefix = "rehome:"

// FetchRehomeStaged fetches a job's staged output into its staged key on this
// node, from the job's origin or any other node that has it. The fetch
// verifies the checksum as it reads; a failed one leaves nothing behind.
func (c *Coordinator) FetchRehomeStaged(ctx context.Context, j raft.RehomeJob, out raft.RehomeOutput) error {
	if c.fetcher == nil || c.registry == nil {
		return errors.New("no fetch client")
	}
	entry := raft.FileEntry{Path: out.StagedKey, SHA256: out.SHA256, SizeBytes: out.SizeBytes}
	order := []string{j.Origin}
	for _, n := range c.registry.GetAll() {
		if n.ID != j.Origin {
			order = append(order, n.ID)
		}
	}
	var lastErr error
	for _, id := range order {
		n, ok := c.registry.Get(id)
		if !ok || n.Address == "" || id == c.localNode.ID {
			continue
		}
		fctx, cancel := context.WithTimeout(ctx, stagedFetchTimeout)
		pr, pw := io.Pipe()
		done := make(chan error, 1)
		go func() {
			err := c.storage.WriteReader(fctx, out.StagedKey, pr, out.SizeBytes)
			// A writer that stopped early must not leave the fetch blocked
			// writing into the pipe (it holds the job's lock on this node).
			pr.CloseWithError(err)
			done <- err
		}()
		_, ferr := c.fetcher.Fetch(fctx, n.Address, &entry, pw, 0, nil)
		pw.CloseWithError(ferr)
		werr := <-done
		cancel()
		if ferr == nil && werr == nil {
			return nil
		}
		if ferr == nil {
			ferr = werr
		}
		lastErr = ferr
		_ = c.storage.Delete(context.WithoutCancel(ctx), out.StagedKey)
	}
	if lastErr == nil {
		lastErr = errors.New("no node to fetch it from")
	}
	return lastErr
}

// deferRehomeUnlink: a job's source deleted from the manifest at its latch
// (reason rehome:<id>) is not unlinked by the delete workers while the job
// exists. This node's participant unlinks it once its reads from before the
// latch ended; a node that does not take part (it joined later) unlinks it
// when the job ends (rehomeEndedUnlink).
func (c *Coordinator) deferRehomeUnlink(item deleteRequest) bool {
	id, ok := strings.CutPrefix(item.reason, rehomeDeleteReasonPrefix)
	if !ok || c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return false
	}
	if _, live := fsm.RehomeJob(id); !live {
		return false
	}
	c.logger.Debug().Str("path", item.path).Str("job", id).
		Msg("Re-home: the source's local copy is unlinked by this node's part of the job, after its reads")
	return true
}

// rehomeEndedUnlink queues the local unlink of an ended job's source. The
// delete workers keep a path the manifest lists again, so a job that
// reverted (its source still registered) keeps it. Local storage only: on a
// shared store the source is one object the job's driver removes. Called on
// the apply goroutine: a queue append only.
func (c *Coordinator) rehomeEndedUnlink(j raft.RehomeJob) {
	// The delete workers run, with their check that the manifest no longer
	// lists the path, only where local copies are kept (replication on).
	if c.storage == nil || c.storage.Type() != "local" || j.Source.Path == "" || c.deleteManifestRelisted == nil {
		return
	}
	if j.State == raft.RehomeReverted {
		return // its source is registered: nothing to unlink
	}
	if p := c.rehomeLocalRecords.Load(); p != nil && (*p)(j.ID) {
		return // this node records the job: SweepLocal settles it, outputs first
	}
	c.enqueueLocalDelete(j.Source.Path, "rehome-ended:"+j.ID)
}

// rehomeOwnsLocal: a re-home job owns path. Its source is unlinked by this
// node's part of the job once its reads from before ended, not by a sweep.
func (c *Coordinator) rehomeOwnsLocal(path string) bool {
	if c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	return fsm != nil && fsm.RehomeOwns(path)
}
