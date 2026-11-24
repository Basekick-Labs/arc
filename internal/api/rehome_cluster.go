package api

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/rehome"
	"github.com/basekick-labs/arc/internal/storage"
)

// The job on a cluster whose nodes share one object store (#1825 slice 4b).
// The record and its rules are the cluster's (cluster/raft/rehome.go): this
// file is the driver, run on the primary writer, and every node's part of a
// state (ParticipateCluster).
//
// The driver:
//
//  1. Owns the source locally while it prepares, checks it as the
//     standalone job does, and splits it in scratch.
//  2. Stages the job in the cluster record (the outputs' keys, sizes and
//     checksums; nothing is written yet). From here the cluster owns every
//     path of the job.
//  3. Committed: every node leaves the outputs out of reads. Once every
//     node has acked (its reads from before ended), the driver uploads each
//     output straight to its final key, still left out everywhere.
//  4. Published: every node drops its caches and acks. The driver checks
//     the outputs again and that no DELETE rewrote the source meanwhile.
//  5. Latched: one entry registers the outputs and removes the source's
//     entry; reads leave the source out. Every node acks (its reads from
//     before ended): the driver deletes the source's object.
//  6. Removed: every node drops its caches and acks. The job ends.
//
// Only the driver that split the file uploads: a job taken up at staged or
// committed by anyone, this node after a restart included, is reverted
// (its outputs deleted, left out of reads for the reverted grace). From
// published on a job only goes forward, and whoever holds its lease can.

// RehomeCluster is the cluster as the driver uses it (cluster.Coordinator).
type RehomeCluster interface {
	LocalNodeID() string
	IsPrimaryWriter() bool
	// IsLeader: this node leads Raft. It takes over a job whose lease
	// lapsed too: the primary-writer designation can stay with a node that
	// died (no writer failover), and the job must not wait for it.
	IsLeader() bool
	// RehomeRunnable is why no job can run from this node now, or "".
	RehomeRunnable() string
	RehomeStage(ctx context.Context, p raft.StageRehomePayload) error
	RehomeAdvance(ctx context.Context, id, to string) error
	RehomeClaim(ctx context.Context, id string) error
	RehomeEnd(ctx context.Context, id string) error
	RehomeJob(id string) (raft.RehomeJob, bool)
	RehomeJobs() []raft.RehomeJob
	RehomeConfigBlockers() ([]string, error)
	// ManifestFile is a file's entry in this node's manifest.
	ManifestFile(path string) (*raft.FileEntry, bool)
}

const (
	// rehomeDroppedWait: how long after a node left the cluster (and the
	// Raft configuration) a step waits before it goes on without its ack.
	// The node stops its reads when it loses its read lease, 5 s after it
	// last heard from the leader; this is that and a margin.
	rehomeDroppedWaitDefault = 30 * time.Second
	// rehomeAckPoll is how often a waiting driver looks at the record.
	rehomeAckPoll = 500 * time.Millisecond
	// rehomeTakeoverEvery is how often the primary writer looks for jobs to
	// take up.
	rehomeTakeoverEvery = 30 * time.Second
)

// Variables for tests.
var (
	rehomeDroppedWait    = rehomeDroppedWaitDefault
	rehomeClusterAckWait = rehomeReadWait
)

// errRehomeNotMine: another node holds the job's lease.
var errRehomeNotMine = errors.New("another node holds this re-home job")

// rehomeClusterState is the driver's memory on this node.
type rehomeClusterState struct {
	mu sync.Mutex
	// running: jobs a driver runs in this process.
	running map[string]bool
	// dropped: when this node first saw each dropped node of a job with the
	// Raft configuration clean.
	dropped map[string]time.Time
	// epochs: the read epoch each (job, state) step flipped to, so a retry
	// waits for the same reads rather than for later ones too.
	epochs map[string]uint64
}

func (s *rehomeClusterState) init() {
	if s.running == nil {
		s.running, s.dropped, s.epochs = map[string]bool{}, map[string]time.Time{}, map[string]uint64{}
	}
}

// clusterMode reports whether jobs run through the cluster record.
func (h *RehomeHandler) clusterMode() bool {
	return h.swap != nil && h.swap.Cluster != nil
}

// clusterRefusal is why a job cannot start on this node of the cluster, or "".
func (h *RehomeHandler) clusterRefusal() string {
	c := h.swap.Cluster
	if _, ok := h.storage.(storage.ObjectCopier); !ok || !h.sharedStore() {
		return "moving a file's rows on a cluster needs a shared object store (S3, Azure or GCS) as primary storage in this version"
	}
	if !c.IsPrimaryWriter() {
		return "moving a file's rows on a cluster runs on the primary writer; send the request there"
	}
	return c.RehomeRunnable()
}

func (h *RehomeHandler) sharedStore() bool {
	switch h.storage.Type() {
	case "s3", "azure", "gcs":
		return true
	}
	return false
}

// runClusterSwap is a job's work for a real swap on a cluster.
func (h *RehomeHandler) runClusterSwap(ctx context.Context, job *rehomeJob) (state, reason string, report *rehomeReport) {
	c := h.swap.Cluster
	reg := h.swap.Registry
	refuse := func(why string) (string, string, *rehomeReport) { return "refused", why, report }
	fail := func(step string, err error) (string, string, *rehomeReport) {
		if why, refused := rehome.IsRefused(err); refused {
			return "refused", why, report
		}
		h.logger.Error().Err(err).Str("job", job.ID).Str("path", job.Path).Str("step", step).Msg("Re-home job failed")
		return "failed", fmt.Sprintf("the job failed while it was %s; nothing was changed. The server log has the error under this job's id", step), report
	}
	if job.Tier == rehome.TierCold {
		return refuse("the file is in cold storage; moving a file's rows on a cluster needs it on the hot tier in this version")
	}
	src, err := rehome.ParseKey(job.Path)
	if err != nil {
		return fail("reading the path", err)
	}
	rec := &rehome.Job{ID: job.ID, Database: src.Database, Measurement: src.Measurement,
		Source: rehome.FileRef{Key: src.Key}, CreatedAt: job.StartedAt}

	// 1. Own the source on this node while preparing: DELETE, retention and
	// database deletes here see it. The cluster owns it once staged.
	if err := reg.Begin(rec); err != nil {
		return refuse(fmt.Sprintf("%s cannot be moved now: %v", src.Key, err))
	}
	defer reg.Drop(job.ID)

	entry, ok := c.ManifestFile(src.Key)
	if !ok {
		return refuse(fmt.Sprintf("%s is not in the cluster's manifest", src.Key))
	}
	if entry.Tier != "" && entry.Tier != "hot" {
		return refuse(fmt.Sprintf("%s is on the %s tier; moving a file's rows on a cluster needs it on the hot tier in this version", src.Key, entry.Tier))
	}
	if entry.SHA256 == "" {
		return refuse(fmt.Sprintf("%s has no checksum in the cluster's manifest, so the job cannot tell whether it changed", src.Key))
	}
	var size int64
	var checkErr error
	check := func() {
		size, checkErr = h.storage.StatFile(ctx, src.Key)
		if checkErr == nil && size != entry.SizeBytes {
			checkErr = rehome.Refusef("%s is not in storage as the manifest records it (size %d, recorded %d)", src.Key, size, entry.SizeBytes)
		}
		if checkErr != nil {
			return
		}
		if sib, err := h.rewriteSibling(ctx, src.Key, src.Dir); err != nil {
			checkErr = err
		} else if sib != "" {
			checkErr = rehome.Refusef("%s was rewritten by a DELETE and the two copies are both in storage (%s); run a DELETE on the measurement first, which removes the stale one", src.Key, sib)
		}
		if checkErr != nil || h.swap.OtherExclusions == nil {
			return
		}
		excluded, err := h.swap.OtherExclusions(ctx, rec.Prefix())
		if err != nil {
			checkErr = err
			return
		}
		for _, k := range excluded {
			if k == src.Key {
				checkErr = rehome.Refusef("%s is already left out of reads by another operation (a DELETE that could not remove it, or a compaction that merged it); moving its rows would bring them back", src.Key)
				return
			}
		}
	}
	if h.swap.RewriteLock != nil {
		h.swap.RewriteLock(check)
	} else {
		check()
	}
	if checkErr != nil {
		return fail("checking the file", checkErr)
	}

	// Split in scratch.
	prep, step, err := h.prepare(ctx, src.Key, h.storage.ReadTo)
	if err != nil {
		return fail(step, err)
	}
	defer prep.close()
	if prep.size != entry.SizeBytes || prep.sha256 != entry.SHA256 {
		return fail("copying the file to local disk", rehome.Refusef("%s changed while it was being copied (it is not what the manifest records)", src.Key))
	}
	plan := prep.plan
	report = &rehomeReport{
		Database: src.Database, Measurement: src.Measurement, Shape: string(src.Shape), Directory: src.Dir,
		TimeType: plan.TimeType, Rows: plan.Rows, Bytes: prep.size,
		Misfiled: plan.Misfiled(), MisfiledRows: plan.MisfiledRows(), RemainderRows: plan.RemainderRows,
		Destinations:      plan.Destinations[:min(len(plan.Destinations), rehomeDestinationsListed)],
		DestinationsTotal: len(plan.Destinations),
	}
	if !plan.Misfiled() {
		return "refused", fmt.Sprintf("every row of %s belongs in the directory it sits in; there is nothing to move", src.Key), report
	}
	outs, err := rehome.Split(ctx, prep.db, plan, prep.local, prep.outDir)
	if err != nil {
		return fail("splitting the file and checking the split", err)
	}
	report.SplitChecked, report.Outputs = true, outs
	if len(outs) > raft.MaxRehomeOutputs {
		return refuse(fmt.Sprintf("%s splits into %d files; a job on a cluster moves at most %d in this version", src.Key, len(outs), raft.MaxRehomeOutputs))
	}

	// 2. Stage. Each final key must be free in storage now; the cluster
	// record owns it from the stage on.
	copier := h.storage.(storage.ObjectCopier)
	payload := raft.StageRehomePayload{Job: job.ID, Database: src.Database, Measurement: src.Measurement,
		Source: raft.CompactionInput{Path: src.Key, SizeBytes: entry.SizeBytes, SHA256: entry.SHA256}, Now: time.Now().UTC()}
	files := map[string]string{}
	for i, o := range outs {
		sum, err := fileSHA256(o.LocalPath)
		if err != nil {
			return fail("staging the split", err)
		}
		key := o.Dir + "/" + outputName(src, storage.NameTime(time.Now()))
		if n, _, err := copier.ObjectSHA(ctx, key); err != nil || n >= 0 {
			return fail("staging the split", fmt.Errorf("the output key %s is taken (size %d, err %v)", key, n, err))
		}
		payload.Outputs = append(payload.Outputs, raft.RehomeOutput{Key: key, StagedKey: raft.RehomeStagedKey(job.ID, i),
			SizeBytes: o.Bytes, SHA256: sum, PartitionTime: o.Start})
		files[key] = o.LocalPath
	}
	// This process runs the job from before its stage: a takeover pass in
	// between would take it for one left by a restart and revert it.
	if !h.claimRunning(job.ID) {
		return fail("running the job", errors.New("the job is already running in this process"))
	}
	defer h.releaseRunning(job.ID)
	if err := c.RehomeStage(ctx, payload); err != nil {
		if r := raft.RehomeConflictReason(err); r != "" {
			return refuse(fmt.Sprintf("the cluster refused the job (%s); nothing was changed", r))
		}
		// An apply that timed out may have committed: the job is in the
		// record then, and is run as any staged job.
		if werr := h.waitLocalJob(ctx, job.ID); werr != nil {
			return fail("recording the job in the cluster", err)
		}
		h.logger.Warn().Err(err).Str("job", job.ID).Msg("Re-home: the stage reported an error but is in the cluster record; going on")
	}
	// The local ownership goes only once this node's record has the job:
	// in between, a DELETE here must still see the file as owned.
	if err := h.waitLocalJob(ctx, job.ID); err != nil {
		return fail("recording the job in the cluster", err)
	}
	reg.Drop(job.ID)
	h.hook("staged")

	err = h.driveCluster(ctx, job.ID, files)
	switch {
	case err == nil:
	case errors.Is(err, errRehomeReverted):
		return "failed", "the job was reverted before it changed anything for readers (the server log says why); the file is as it was, and its outputs are cleaned up in the background (GET /api/v1/cluster shows the job until then). Send the request again once it is gone", report
	case errors.Is(err, errRehomeNotMine):
		return "stopped", "another node took the job over; GET /api/v1/cluster shows it", report
	default:
		h.logger.Error().Err(err).Str("job", job.ID).Str("path", src.Key).Msg("Re-home job stopped part way; the cluster keeps its record and the primary writer takes it up again")
		return "stopped", "the job stopped part way; the cluster keeps its record, and the primary writer takes it up within a minute: before its outputs were published that reverts it, after it finishes it. The server log has the error under this job's id", report
	}
	h.logger.Info().Str("job", job.ID).Str("path", src.Key).Int64("rows", plan.Rows).
		Int64("moved_rows", plan.MisfiledRows()).Int("outputs", len(outs)).
		Msg("Re-homed a misfiled file on the cluster: its rows are in the directories they belong to, and the file is removed")
	return "done", "", report
}

// errRehomeReverted: the job was reverted and has ended.
var errRehomeReverted = errors.New("the job was reverted")

// rewriteSibling returns a file in dir of key's DELETE-rewrite family other
// than key, or "".
func (h *RehomeHandler) rewriteSibling(ctx context.Context, key, dir string) (string, error) {
	siblings, err := h.storage.List(ctx, dir+"/")
	if err != nil {
		return "", err
	}
	family := storage.RewriteFamily(key)
	for _, k := range siblings {
		if k != key && storage.RewriteFamily(k) == family {
			return k, nil
		}
	}
	return "", nil
}

func (h *RehomeHandler) waitLocalJob(ctx context.Context, id string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, ok := h.swap.Cluster.RehomeJob(id); ok {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("this node's copy of the cluster record did not show the job within 30 s")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (h *RehomeHandler) claimRunning(id string) bool {
	s := &h.cluster
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	if s.running[id] {
		return false
	}
	s.running[id] = true
	return true
}

func (h *RehomeHandler) releaseRunning(id string) {
	s := &h.cluster
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.running, id)
}

// driveCluster takes a job this node holds from its state to its end. files
// are the outputs' local copies, by key: nil when this process did not split
// the file (a takeover, a restart), which reverts a job that has not
// published.
func (h *RehomeHandler) driveCluster(ctx context.Context, id string, files map[string]string) error {
	c := h.swap.Cluster
	self := c.LocalNodeID()
	// Storage steps run to their end: each is safe to repeat, none to skip.
	bg := context.WithoutCancel(ctx)
	revert := func(why string) error {
		h.logger.Warn().Str("job", id).Str("why", why).Msg("Re-home: reverting the job before it changes anything for readers")
		if err := c.RehomeAdvance(bg, id, raft.RehomeReverting); err != nil {
			return h.clusterErr(err)
		}
		return nil
	}
	failures := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		j, ok := c.RehomeJob(id)
		if !ok {
			return nil // ended
		}
		if j.FinisherNode != self {
			return errRehomeNotMine
		}
		if err := h.renewLease(bg, j); err != nil {
			return err
		}
		var err error
		switch j.State {
		case raft.RehomeStaged:
			if files == nil {
				err = revert("taken up before its outputs were written; only the node that split the file writes them")
				break
			}
			err = h.clusterErr(c.RehomeAdvance(bg, id, raft.RehomeCommitted))
		case raft.RehomeCommitted:
			if files == nil {
				err = revert("taken up before its outputs were written; only the node that split the file writes them")
				break
			}
			if werr := h.waitClusterAcks(ctx, j, rehomeClusterAckWait); werr != nil {
				if errors.Is(werr, errRehomeAckTimeout) {
					err = revert(werr.Error())
					break
				}
				err = werr
				break
			}
			if uerr := h.uploadOutputs(ctx, j, files); uerr != nil {
				var refused errRehomeRevert
				if errors.As(uerr, &refused) {
					err = revert(refused.why)
					break
				}
				err = uerr
				break
			}
			h.hook("uploaded")
			err = h.clusterErr(c.RehomeAdvance(bg, id, raft.RehomePublished))
		case raft.RehomePublished:
			if werr := h.waitClusterAcks(ctx, j, rehomeClusterAckWait); werr != nil {
				if errors.Is(werr, errRehomeAckTimeout) {
					err = revert(werr.Error())
					break
				}
				err = werr
				break
			}
			if why := h.outputsInPlace(bg, j); why != "" {
				err = revert(why)
				break
			}
			if sib, lerr := h.rewriteSibling(bg, j.Source.Path, dirOf(j.Source.Path)); lerr != nil {
				err = lerr
				break
			} else if sib != "" {
				err = revert("a DELETE rewrote the file while the job ran (" + sib + ")")
				break
			}
			err = h.clusterErr(c.RehomeAdvance(bg, id, raft.RehomeLatched))
			if err == nil {
				h.hook("latched")
			}
		case raft.RehomeLatched:
			if werr := h.waitClusterAcks(ctx, j, 0); werr != nil {
				err = werr
				break
			}
			if derr := storage.DeleteWithRetry(bg, h.storage, j.Source.Path); derr != nil && !storage.IsNotFound(derr) {
				err = fmt.Errorf("remove %s: %w", j.Source.Path, derr)
				break
			}
			h.hook("removed")
			err = h.clusterErr(c.RehomeAdvance(bg, id, raft.RehomeRemoved))
		case raft.RehomeRemoved:
			if werr := h.waitClusterAcks(ctx, j, 0); werr != nil {
				err = werr
				break
			}
			err = h.clusterErr(c.RehomeEnd(bg, id))
			if err == nil {
				h.forgetDropped(id)
				if h.swap.ClearMisfiled != nil {
					h.swap.ClearMisfiled(j.Source.Path)
				}
				return nil
			}
		case raft.RehomeReverting:
			if derr := h.deleteOutputs(bg, j); derr != nil {
				err = derr
				break
			}
			err = h.clusterErr(c.RehomeEnd(bg, id))
		case raft.RehomeReverted:
			if files != nil {
				// The request has its answer: the rest (the grace, every
				// node's ack, the outputs deleted again) runs in the
				// background (takeOverOnce), not in the job slot.
				return errRehomeReverted
			}
			wait := time.Until(j.RevertedAt.Add(raft.RehomeRevertedGrace))
			if wait > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(min(wait, raft.FinisherLease/3)):
				}
				continue
			}
			if werr := h.waitClusterAcks(ctx, j, 0); werr != nil {
				err = werr
				break
			}
			// Again: an upload by a driver that had lost the job may have
			// landed during the grace.
			if derr := h.deleteOutputs(bg, j); derr != nil {
				err = derr
				break
			}
			err = h.clusterErr(c.RehomeEnd(bg, id))
			if err == nil {
				h.forgetDropped(id)
				return errRehomeReverted
			}
		default:
			return fmt.Errorf("job %s in state %q", id, j.State)
		}
		if err == nil || errors.Is(err, errRehomeMoved) {
			failures = 0
			continue
		}
		if errors.Is(err, errRehomeNotMine) || errors.Is(err, errRehomeReverted) || ctx.Err() != nil {
			return err
		}
		// Transient (a leader election, a storage error): the state is in
		// the record, and every step is safe to repeat.
		failures++
		if failures > rehomeDriveRetries {
			return err
		}
		h.logger.Warn().Err(err).Str("job", id).Int("attempt", failures).Msg("Re-home: a step failed; retrying")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(failures) * time.Second):
		}
	}
}

// rehomeDriveRetries bounds the retries of one failing step.
const rehomeDriveRetries = 10

// clusterErr maps a refusal that says another node drives the job.
func (h *RehomeHandler) clusterErr(err error) error {
	switch raft.RehomeConflictReason(err) {
	case raft.RehomeNotFinisher, raft.RehomeClaimed:
		return errRehomeNotMine
	}
	return err
}

// renewLease renews the job's lease once a third of it has passed.
func (h *RehomeHandler) renewLease(ctx context.Context, j raft.RehomeJob) error {
	if time.Since(j.ClaimedAt) < raft.FinisherLease/3 {
		return nil
	}
	return h.clusterErr(h.swap.Cluster.RehomeClaim(ctx, j.ID))
}

var errRehomeAckTimeout = errors.New("not every node acked the job's state in time")

// errRehomeMoved: the job left the state being waited on; the driver looks
// at it again.
var errRehomeMoved = errors.New("the job's state moved")

// waitClusterAcks returns once every node the job's state waits for has
// acked it, no Raft server is outside the node table, and every node removed
// without acking has been gone rehomeDroppedWait (its reads stopped). limit
// 0 waits without end. The lease is renewed while it waits; a job that moved
// on returns at once.
func (h *RehomeHandler) waitClusterAcks(ctx context.Context, j raft.RehomeJob, limit time.Duration) error {
	c := h.swap.Cluster
	start := time.Now()
	logged := start // the first warning after a minute of waiting
	for {
		cur, ok := c.RehomeJob(j.ID)
		if !ok || cur.StateIndex != j.StateIndex {
			return errRehomeMoved
		}
		if cur.FinisherNode != c.LocalNodeID() {
			return errRehomeNotMine
		}
		blockers, err := c.RehomeConfigBlockers()
		missing := cur.MissingAcks()
		droppedWait := h.droppedWait(cur, err == nil && len(blockers) == 0)
		if err == nil && len(missing) == 0 && len(blockers) == 0 && droppedWait <= 0 {
			return nil
		}
		if limit > 0 && time.Since(start) > limit {
			return fmt.Errorf("%w (waiting for %v, Raft servers not in the node table %v)", errRehomeAckTimeout, missing, blockers)
		}
		if time.Since(logged) > time.Minute {
			logged = time.Now()
			h.logger.Warn().Str("job", j.ID).Str("state", j.State).Strs("waiting_for", missing).Strs("raft_only", blockers).
				Msg("Re-home: waiting for every node to ack the job's state; a node that is gone for good can be removed from the cluster, after which the job goes on")
		}
		if err := h.renewLease(context.WithoutCancel(ctx), cur); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(rehomeAckPoll):
		}
	}
}

// droppedWait is how much longer the job must wait for its dropped nodes.
// clean: no Raft server is outside the node table now; the clock for a
// dropped node starts once it is.
func (h *RehomeHandler) droppedWait(j raft.RehomeJob, clean bool) time.Duration {
	if len(j.Dropped) == 0 {
		return 0
	}
	s := &h.cluster
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	var longest time.Duration
	for _, n := range j.Dropped {
		key := j.ID + "/" + n
		seen, ok := s.dropped[key]
		if !ok {
			if !clean {
				longest = rehomeDroppedWait
				continue
			}
			seen = time.Now()
			s.dropped[key] = seen
		}
		if left := rehomeDroppedWait - time.Since(seen); left > longest {
			longest = left
		}
	}
	return longest
}

// forgetDropped drops what droppedWait remembers of an ended job.
func (h *RehomeHandler) forgetDropped(id string) {
	s := &h.cluster
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.dropped {
		if strings.HasPrefix(k, id+"/") {
			delete(s.dropped, k)
		}
	}
}

// errRehomeRevert: the job cannot go on and has not changed anything for
// readers.
type errRehomeRevert struct{ why string }

func (e errRehomeRevert) Error() string { return e.why }

// uploadOutputs writes each output to its final key, which every node leaves
// out of reads. A key that already holds the job's output (an earlier
// attempt) is kept; one that holds anything else reverts the job. The lease
// is renewed before each upload, and each runs under RehomeCopyTimeout: an
// upload of a driver that lost the job lands within the reverted grace.
func (h *RehomeHandler) uploadOutputs(ctx context.Context, j raft.RehomeJob, files map[string]string) error {
	copier := h.storage.(storage.ObjectCopier)
	for _, o := range j.Outputs {
		n, sha, err := copier.ObjectSHA(ctx, o.Key)
		if err != nil {
			return fmt.Errorf("stat %s: %w", o.Key, err)
		}
		if n >= 0 {
			if n == o.SizeBytes && sha == o.SHA256 {
				continue
			}
			return errRehomeRevert{fmt.Sprintf("the output key %s holds another object", o.Key)}
		}
		local, ok := files[o.Key]
		if !ok {
			return errRehomeRevert{"an output's local copy is missing"}
		}
		if err := h.swap.Cluster.RehomeClaim(ctx, j.ID); err != nil {
			return h.clusterErr(err)
		}
		if err := h.uploadOne(ctx, copier, o, local); err != nil {
			return err
		}
		h.hook("upload")
	}
	return nil
}

func (h *RehomeHandler) uploadOne(ctx context.Context, copier storage.ObjectCopier, o raft.RehomeOutput, local string) error {
	uctx, cancel := context.WithTimeout(ctx, raft.RehomeCopyTimeout)
	defer cancel()
	f, err := os.Open(local)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := copier.WriteReaderWithSHA(uctx, o.Key, f, o.SizeBytes, o.SHA256); err != nil {
		return fmt.Errorf("upload %s: %w", o.Key, err)
	}
	n, sha, err := copier.ObjectSHA(ctx, o.Key)
	if err != nil {
		return fmt.Errorf("check %s: %w", o.Key, err)
	}
	if n != o.SizeBytes || sha != o.SHA256 {
		return errRehomeRevert{fmt.Sprintf("the output %s is not in storage as written (size %d, want %d)", o.Key, n, o.SizeBytes)}
	}
	return nil
}

// outputsInPlace returns why an output is not at its final key as the job
// recorded it, or "".
func (h *RehomeHandler) outputsInPlace(ctx context.Context, j raft.RehomeJob) string {
	copier := h.storage.(storage.ObjectCopier)
	for _, o := range j.Outputs {
		n, sha, err := copier.ObjectSHA(ctx, o.Key)
		if err != nil {
			return fmt.Sprintf("could not check the output %s: %v", o.Key, err)
		}
		if n != o.SizeBytes || sha != o.SHA256 {
			return fmt.Sprintf("the output %s is not in storage as the job wrote it", o.Key)
		}
	}
	return ""
}

// deleteOutputs deletes each output key that holds the job's output.
func (h *RehomeHandler) deleteOutputs(ctx context.Context, j raft.RehomeJob) error {
	copier := h.storage.(storage.ObjectCopier)
	for _, o := range j.Outputs {
		n, sha, err := copier.ObjectSHA(ctx, o.Key)
		if err != nil {
			return fmt.Errorf("stat %s: %w", o.Key, err)
		}
		if n < 0 {
			continue
		}
		if n != o.SizeBytes || sha != o.SHA256 {
			// Not the job's: nothing but the job writes an owned key.
			h.logger.Error().Str("job", j.ID).Str("key", o.Key).Msg("Re-home: an output key holds an object the job did not write; it is left alone")
			continue
		}
		if err := storage.DeleteWithRetry(ctx, h.storage, o.Key); err != nil && !storage.IsNotFound(err) {
			return fmt.Errorf("delete %s: %w", o.Key, err)
		}
	}
	return nil
}

func dirOf(key string) string {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			return key[:i]
		}
	}
	return ""
}

// SetCluster runs jobs through the cluster record. Call after SetSwap.
func (h *RehomeHandler) SetCluster(c RehomeCluster, readReady func() (bool, string)) {
	if h.swap == nil {
		return
	}
	h.swap.Cluster, h.swap.ReadReady = c, readReady
}

// StartClusterTakeover runs, on the primary writer and on the Raft leader,
// the jobs this process is not running: its own after a restart, and any
// whose lease lapsed (the record's lease decides between the two). Until the
// handler shuts down.
func (h *RehomeHandler) StartClusterTakeover() {
	if !h.clusterMode() {
		return
	}
	ctx := h.ctx
	h.done.Add(1)
	go func() {
		defer h.done.Done()
		t := time.NewTicker(rehomeTakeoverEvery)
		defer t.Stop()
		for {
			h.takeOverOnce(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

func (h *RehomeHandler) takeOverOnce(ctx context.Context) {
	c := h.swap.Cluster
	if !c.IsPrimaryWriter() && !c.IsLeader() {
		return
	}
	if ok, _ := h.clusterReadReady(); !ok {
		return
	}
	self := c.LocalNodeID()
	for _, j := range c.RehomeJobs() {
		if j.FinisherNode != self {
			if time.Since(j.ClaimedAt) <= raft.FinisherLease {
				continue
			}
			if err := c.RehomeClaim(ctx, j.ID); err != nil {
				continue
			}
			h.logger.Warn().Str("job", j.ID).Str("from", j.FinisherNode).Str("state", j.State).Msg("Re-home: took over a job whose driver's lease lapsed")
		}
		if !h.claimRunning(j.ID) {
			continue
		}
		id := j.ID
		h.done.Add(1)
		go func() {
			defer h.done.Done()
			defer h.releaseRunning(id)
			defer func() {
				if r := recover(); r != nil {
					h.logger.Error().Interface("panic", r).Str("job", id).Msg("Re-home: a job taken up on this node stopped on an internal error; it is tried again")
				}
			}()
			err := h.driveCluster(ctx, id, nil)
			switch {
			case err == nil:
				h.logger.Info().Str("job", id).Msg("Re-home: a job taken up on this node finished")
			case errors.Is(err, errRehomeReverted):
				h.logger.Warn().Str("job", id).Msg("Re-home: a job taken up on this node was reverted; the file is as it was")
			case errors.Is(err, errRehomeNotMine), errors.Is(err, context.Canceled):
			default:
				h.logger.Error().Err(err).Str("job", id).Msg("Re-home: a job taken up on this node stopped; it is tried again")
			}
		}()
	}
}

func (h *RehomeHandler) clusterReadReady() (bool, string) {
	if h.swap.ReadReady == nil {
		return true, ""
	}
	return h.swap.ReadReady()
}

// ParticipateCluster is this node's part of a job's state
// (cluster.RehomeParticipantFunc): wait for the reads from before it, and
// after a state that put files in place or removed them, drop this node's
// caches first and again after the wait.
func (h *RehomeHandler) ParticipateCluster(ctx context.Context, j raft.RehomeJob, acked bool) error {
	caches := j.State == raft.RehomePublished || j.State == raft.RehomeRemoved || j.State == raft.RehomeReverted
	if caches {
		h.dropLocalCaches(j)
	}
	if !acked {
		return nil
	}
	if h.swap.Reads != nil {
		key := fmt.Sprintf("%s@%d", j.ID, j.StateIndex)
		s := &h.cluster
		s.mu.Lock()
		s.init()
		epoch, ok := s.epochs[key]
		if !ok {
			epoch = h.swap.Reads.Flip()
			s.epochs[key] = epoch
			if len(s.epochs) > 1000 {
				s.epochs = map[string]uint64{key: epoch}
			}
		}
		s.mu.Unlock()
		if err := h.swap.Reads.Wait(ctx, epoch); err != nil {
			return err
		}
	}
	if caches {
		// Again: a read that ended during the wait may have filled them
		// from what it saw before the state.
		h.dropLocalCaches(j)
	}
	return nil
}

// dropLocalCaches drops this node's caches that may name a job's files or
// remember their directories as empty. This node only: every node runs its
// own part.
func (h *RehomeHandler) dropLocalCaches(j raft.RehomeJob) {
	if h.swap.FilePublished != nil {
		for _, o := range j.Outputs {
			h.swap.FilePublished(j.Database, j.Measurement, o.PartitionTime)
		}
		h.swap.FilePublished(j.Database, j.Measurement, time.Unix(0, 0).UTC())
	}
	if h.swap.InvalidateLocalCaches != nil {
		h.swap.InvalidateLocalCaches()
	}
}

// The coordinator is the cluster the driver runs against.
var _ RehomeCluster = (*cluster.Coordinator)(nil)
