package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/ingest"
)

// DELETE records, the coordinator's side (#1207 d1): the primary records a
// row DELETE before running it, every writer named in a record acknowledges
// it with the batches it had acknowledged, and the outbox applies live
// records to the files it registers (delete_records_outbox.go).

var errDeleteAckPending error = &ingest.RefusalError{Reason: RefusalDeleteAck, Text: "ingest paused: acknowledging a DELETE to the cluster (stream takeover); retry shortly"}

// deleteAckEvery paces the acknowledgement loop; a record is acknowledged
// within about one tick of applying here.
const deleteAckEvery = 100 * time.Millisecond

// deleteRecordState is wired by StartOriginStream on a writer with stream
// takeover: the buffer whose marks a record or an acknowledgement carries,
// and this process's writer instance.
type deleteRecordState struct {
	buffer   scopeFlusher
	instance func() uint64
}

// scopeFlusher is the ingest buffer's FlushScopeAndWait: the marks of a
// record or an acknowledgement are read where the buffer is handed to a
// flush, so no file holds batches on both sides of one.
type scopeFlusher interface {
	FlushScopeAndWait(ctx context.Context, database, measurement string) (map[string]uint64, error)
}

// deleteAckOwed reports whether this writer must refuse writes until it
// acknowledged a DELETE record naming its instance. Lock-free.
func (c *Coordinator) deleteAckOwed() bool {
	st := c.deleteRecords.Load()
	if st == nil || c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	return fsm != nil && fsm.OwesDeleteAck(st.instance())
}

// startDeleteRecords wires the DELETE records on this writer: the ack loop,
// the flush split and the outbox hooks.
func (c *Coordinator) startDeleteRecords(ctx context.Context, buffer *ingest.ArrowBuffer, instance func() uint64, outbox *RegistrationOutbox) {
	fsm := c.raftNode.FSM()
	c.deleteRecords.Store(&deleteRecordState{buffer: buffer, instance: instance})
	buffer.SetDeleteScope(fsm.DeleteScopeIDs)
	if outbox != nil {
		h := &DeleteRecordHooks{
			Records:  fsm.DeleteRecordsFor,
			Settle:   c.proposeSettleDeleted,
			Instance: instance,
			Remove:   c.storage.Delete,
		}
		if rw := c.deleteRewriter.Load(); rw != nil {
			h.Rewrite = *rw
		}
		outbox.SetDeleteRecordHooks(h)
		c.deleteOutbox.Store(outbox)
	}
	go c.deleteAckLoop(ctx, fsm)
}

// SetDeleteRewriter installs the DELETE handler's rewriter for the outbox.
// Call before StartOriginStream; a file waiting for it is held.
func (c *Coordinator) SetDeleteRewriter(fn DeleteRewriter) {
	if fn == nil {
		return
	}
	c.deleteRewriter.Store(&fn)
	if o := c.deleteOutbox.Load(); o != nil {
		if h := o.deleteHooks.Load(); h != nil {
			cp := *h
			cp.Rewrite = fn
			o.SetDeleteRecordHooks(&cp)
		}
	}
}

// deleteAckLoop acknowledges every live record naming this process's
// instance, with the batches it acknowledged per key of the record. A
// record names it only while it is current (a record applied while an
// earlier process of this node was current names that one, which never
// acknowledges: all of its batches are in scope).
func (c *Coordinator) deleteAckLoop(ctx context.Context, fsm *raft.ClusterFSM) {
	t := time.NewTicker(deleteAckEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		st := c.deleteRecords.Load()
		if st == nil {
			continue
		}
		inst := st.instance()
		if inst == 0 || !fsm.OwesDeleteAck(inst) {
			continue
		}
		for _, r := range fsm.UnackedDeleteRecords(inst) {
			// Writes are refused from the moment the record applied here, so
			// what the flush hands off is everything acknowledged before it.
			actx, cancel := context.WithTimeout(ctx, claimantSyncBudget)
			marks, err := st.buffer.FlushScopeAndWait(actx, r.Database, r.Measurement)
			if err != nil {
				cancel()
				c.logger.Warn().Err(err).Uint64("record", r.ID).Msg("Flushing before acknowledging a DELETE record failed; retrying")
				continue
			}
			raw, err := json.Marshal(raft.RecordDeleteAckPayload{Origin: c.localNode.ID, Record: r.ID, Instance: inst, Marks: marks})
			if err != nil {
				cancel()
				continue
			}
			err = c.applyOrForward(actx, &raft.Command{Type: raft.CommandRecordDeleteAck, Payload: raw})
			cancel()
			if err != nil {
				c.logger.Debug().Err(err).Uint64("record", r.ID).Msg("DELETE record acknowledgement not applied yet; retrying")
			}
		}
	}
}

// DeleteRecordsActive reports whether a DELETE run here must be recorded
// first: stream takeover is on in the cluster. RecordDelete refuses on a
// writer not wired for it.
func (c *Coordinator) DeleteRecordsActive() bool {
	if c == nil || c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	return fsm != nil && fsm.StreamClaimsEnabled()
}

// RecordDelete records a row DELETE (where "" = every row; measurement "" =
// the whole database) with this writer's acknowledged batches, marks as
// FlushScopeAndWait returned them, and returns once this node applied it.
func (c *Coordinator) RecordDelete(ctx context.Context, database, measurement, where string, marks map[string]uint64) (uint64, error) {
	st := c.deleteRecords.Load()
	if st == nil {
		return 0, errors.New("stream takeover is on in the cluster but this writer does not run it (its WAL coverage did not start); a DELETE here could not be recorded")
	}
	now := time.Now().UTC()
	p := raft.RecordDeletePayload{
		Origin: c.localNode.ID, Instance: st.instance(), Database: database, Measurement: measurement,
		Where: where, Marks: marks, Now: now,
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return 0, err
	}
	if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandRecordDelete, Payload: raw}); err != nil {
		return 0, err
	}
	if err := c.SyncManifest(ctx); err != nil {
		return 0, fmt.Errorf("delete record committed but not applied here yet: %w", err)
	}
	for _, r := range c.raftNode.FSM().DeleteRecords() {
		if r.Origin == c.localNode.ID && r.At.Equal(now) && r.Database == database && r.Measurement == measurement {
			return r.ID, nil
		}
	}
	// Collected already: nothing could publish a batch of its scope.
	return 0, nil
}

// MissingLocalFiles returns the hot manifest files under database/
// (database/measurement/ when measurement is set: what a DELETE scans) this
// node does not have at their size: a DELETE run here would not see their
// rows. Sorted. On a shared object store a file whose object is gone is not
// missing, it is gone: no node will ever have it (an output a finalizing
// staged job still copies into place excepted) (#1207).
func (c *Coordinator) MissingLocalFiles(ctx context.Context, database, measurement string) ([]string, error) {
	if c == nil || c.raftNode == nil || c.storage == nil {
		return nil, nil
	}
	prefix := database + "/"
	if measurement != "" {
		prefix += measurement + "/"
	}
	shared := c.SharedObjectStore()
	var missing []string
	for _, f := range c.raftNode.FSM().GetFilesByDatabase(database) {
		if !raft.HotTier(f.Tier) || !strings.HasPrefix(f.Path, prefix) {
			continue
		}
		// Held first: an output copied into place and ended between a stat
		// and the check would read as gone.
		held := shared && c.CompactionOutputHeld(f.Path)
		size, err := c.storage.StatFile(ctx, f.Path)
		if err != nil {
			return nil, err
		}
		if size < 0 && shared && !held {
			continue
		}
		if size != f.SizeBytes {
			missing = append(missing, f.Path)
		}
	}
	sort.Strings(missing)
	return missing, nil
}

// ManifestHotFiles returns the hot manifest files of database (by its
// Database field) whose path is under database/measurement/: what a DELETE of
// that measurement must scan (#1207 d2).
func (c *Coordinator) ManifestHotFiles(database, measurement string) []raft.FileEntry {
	if c == nil || c.raftNode == nil {
		return nil
	}
	prefix := database + "/" + measurement + "/"
	var out []raft.FileEntry
	for _, f := range c.raftNode.FSM().GetFilesByDatabase(database) {
		if raft.HotTier(f.Tier) && strings.HasPrefix(f.Path, prefix) {
			out = append(out, *f)
		}
	}
	return out
}

// WaitLocalFiles waits, up to budget, until MissingLocalFiles is empty, and
// returns what is still missing.
func (c *Coordinator) WaitLocalFiles(ctx context.Context, database, measurement string, budget time.Duration) ([]string, error) {
	deadline := time.Now().Add(budget)
	for {
		missing, err := c.MissingLocalFiles(ctx, database, measurement)
		if err != nil || len(missing) == 0 || time.Now().After(deadline) {
			return missing, err
		}
		select {
		case <-ctx.Done():
			return missing, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
