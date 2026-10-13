package cluster

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/filereplication"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// Registered files nobody pulled (#1207 S4b1c2b).
//
// A file the origin registered counts its batches as published: a claim
// never takes them. If no node pulled the file before the origin died with
// its volume, those rows are in no file anyone can read. So a claim finishes
// only once this node holds every hot file of the instance its standby copy
// could publish again, or the file is found on no node (or nobody answers
// for it during twice the grace): then it is deleted from the manifest as
// vanished, which takes its batches back, and the claim publishes them from
// the standby copy (the copy usually keeps them: a standby trims a key only
// once every current file of it is here).

type probeAnswer int

const (
	probeUnknown probeAnswer = iota
	probeHas
	probeAbsent
)

const (
	maxUnpulledProbesPerPass = 32
	unpulledProbeTimeout     = 5 * time.Second
)

// unpulledEvery throttles the check of one claim while files are missing.
const unpulledEvery = 30 * time.Second

// unpulledSettled reports whether every hot file of k's instance that this
// node could republish is here at its size; it pursues the missing ones.
// A file is removed from the manifest only when this node's standby copy
// holds every batch of it: then its rows are published again whatever
// becomes of other copies (they are removed with it). A file the copy
// cannot republish is left registered and never holds the claim: removing
// it could destroy the last copy, and keeping the claim open would not
// bring its rows back.
func (cl *claimant) unpulledSettled(ctx context.Context, fsm *raft.ClusterFSM, k raft.StreamClaim, w *claimWork) bool {
	probe := cl.probe
	if probe == nil && cl.c.fetcher != nil {
		probe = cl.c.probeFile
	}
	if probe == nil {
		return true // no peer replication: nothing to pull, nobody to ask
	}
	now := cl.now()
	if !w.unpulledAt.IsZero() && now.Sub(w.unpulledAt) < unpulledEvery {
		return false
	}
	w.unpulledAt = now
	files := fsm.HotFilesOfInstance(k.Instance)
	held := cl.store.Held(k.Origin, k.Instance)
	current := make(map[string]bool, len(files))
	var missing []raft.FileEntry
	for _, f := range files {
		current[f.Path] = true
		if size, ok := w.local[f.Path]; ok && size == f.SizeBytes {
			continue
		}
		if got, err := cl.c.storage.StatFile(ctx, f.Path); err == nil && got == f.SizeBytes {
			w.local[f.Path] = got
			delete(w.unknownSince, f.Path)
			continue
		}
		if !heldCovers(held, f, k.Instance) {
			if !w.unrepublishable[f.Path] {
				w.unrepublishable[f.Path] = true
				cl.logger.Warn().Str("path", f.Path).Str("origin", k.Origin).
					Msg("A registered file of a writer that is gone is not here, and the standby copy cannot publish its rows again; it stays registered, for any node that has it to serve")
			}
			continue
		}
		missing = append(missing, f)
	}
	for p := range w.local {
		if !current[p] {
			delete(w.local, p)
		}
	}
	for p := range w.unknownSince {
		if !current[p] {
			delete(w.unknownSince, p)
		}
	}
	w.unpulled = len(missing)
	if len(missing) == 0 {
		w.unpulledAt = time.Time{}
		return true
	}
	for i, f := range missing {
		if i >= maxUnpulledProbesPerPass {
			break
		}
		switch cl.locate(ctx, fsm, f, probe) {
		case probeHas:
			delete(w.unknownSince, f.Path) // the puller fetches it from there
		case probeAbsent:
			cl.vanish(ctx, fsm, k, f, probeAbsent)
		default:
			since, ok := w.unknownSince[f.Path]
			if !ok {
				w.unknownSince[f.Path] = now
			} else if now.Sub(since) >= 2*cl.grace {
				cl.vanish(ctx, fsm, k, f, probeUnknown)
			}
		}
	}
	return false
}

// heldCovers reports whether held (the standby copy's batches per key)
// holds every batch of inst with rows in f.
func heldCovers(held map[string][][2]uint64, f raft.FileEntry, inst uint64) bool {
	for _, c := range f.Coverage {
		if c.Instance != inst {
			continue
		}
		if c.Truncated {
			return false
		}
		var need raft.Intervals
		for _, r := range c.Ranges {
			need = need.Union(raft.Intervals{r})
		}
		for _, p := range c.Partial {
			need = need.Union(raft.Intervals{{p.CSeq, p.CSeq}})
		}
		if len(need.Minus(raft.Intervals(held[c.Key]))) > 0 {
			return false
		}
	}
	return true
}

// locate asks every other member, the file's origin included, whether it
// holds f: has when one does, absent when every one answered it does not,
// unknown otherwise (a member not heard from cannot answer).
func (cl *claimant) locate(ctx context.Context, fsm *raft.ClusterFSM, f raft.FileEntry, probe func(context.Context, string, raft.FileEntry) probeAnswer) probeAnswer {
	self := cl.c.localNode.ID
	unknown := false
	for _, n := range fsm.GetAllNodes() {
		if n.ID == self {
			continue
		}
		reg, ok := cl.c.registry.Get(n.ID)
		if !ok || !reg.IsHealthy() || reg.Address == "" {
			unknown = true
			continue
		}
		switch probe(ctx, reg.Address, f) {
		case probeHas:
			return probeHas
		case probeUnknown:
			unknown = true
		}
	}
	if unknown {
		return probeUnknown
	}
	return probeAbsent
}

// vanish deletes f from the manifest as vanished.
func (cl *claimant) vanish(ctx context.Context, fsm *raft.ClusterFSM, k raft.StreamClaim, f raft.FileEntry, ans probeAnswer) {
	if cur, ok := fsm.GetFile(f.Path); !ok || cur.SHA256 != f.SHA256 || cur.SizeBytes != f.SizeBytes {
		return
	}
	if err := cl.c.DeleteFileFromManifest(ctx, f.Path, raft.DeleteReasonVanished); err != nil {
		cl.logger.Warn().Err(err).Str("path", f.Path).Msg("Removing a file no node holds from the manifest failed; retrying")
		return
	}
	cl.logger.Warn().Str("path", f.Path).Str("origin", k.Origin).Str("instance", fmt.Sprintf("%016x", k.Instance)).
		Bool("unanswered", ans == probeUnknown).
		Msg("A registered file of a writer that is gone is on no node; it was removed from the manifest and its rows are published again from the standby copy")
}

// probeFile asks the peer at addr whether it holds e: a fetch at the end of
// the file is refused as a bad offset by a peer that has it.
func (c *Coordinator) probeFile(ctx context.Context, addr string, e raft.FileEntry) probeAnswer {
	if c.fetcher == nil {
		return probeUnknown
	}
	pctx, cancel := context.WithTimeout(ctx, unpulledProbeTimeout)
	defer cancel()
	off := e.SizeBytes
	var h hash.Hash
	if off > 0 {
		h = sha256.New()
	}
	_, err := c.fetcher.Fetch(pctx, addr, &e, io.Discard, off, h)
	switch {
	case err == nil && off == 0, errors.Is(err, filereplication.ErrBadOffset):
		return probeHas
	case errors.Is(err, filereplication.ErrNotInPeerManifest):
		return probeUnknown // its manifest may trail ours
	case errors.Is(err, filereplication.ErrFileNotOnPeer):
		return probeAbsent
	}
	return probeUnknown
}
