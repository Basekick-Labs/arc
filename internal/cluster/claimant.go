package cluster

// The claimant (#1207 S4b1c2): a writer that holds another writer's
// streamed batches in its standby store takes over the ones nobody
// registered, once that writer is gone, and publishes them.
//
// One pass every claimantEvery, on a writer running the origin stream, with
// stream takeover applied and this node's manifest caught up:
//
//   - trigger: an instance of a dead writer (its current or adopted ones,
//     by the designated primary after the grace) or one its writer let go
//     (closed or orphaned, by any writer holding it) is claimed: every batch
//     stored here received whole, minus those the manifest settled by floor;
//   - extend: batches held here and claimable but not in the claim yet are
//     added (a claim opens with what one command carries);
//   - replay: claimed records are decoded and replayed, per partition hour,
//     minus the rows a registered file already holds (the manifest decides,
//     never this store's floors);
//   - done: once every claimed batch is in a registered file or could not be
//     published, the claim is finished; the store copy goes once every file
//     of its keys is here.
//
// The manifest arbitrates every step: a claim names an epoch, the claimant's
// files carry it, and the index refuses rows registered twice.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/originstream"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

const (
	claimantEvery      = 5 * time.Second
	claimantSyncBudget = 10 * time.Second
	// claimChunkBytes bounds the keys one claim command carries (the FSM
	// refuses a payload over raft.MaxTakeoverPayloadSize).
	claimChunkBytes      = 192 << 10
	claimBackoffMin      = 5 * time.Second
	claimBackoffMax      = 5 * time.Minute
	replayCallRecords    = 256
	replayCallBytes      = 64 << 20
	replayFailedAlarm    = 5
	autoReleaseEvery     = 10 * time.Second
	defaultTakeoverGrace = 60 * time.Second
)

type claimantStore interface {
	Instances() []originstream.InstanceInfo
	Held(origin string, instance uint64) map[string][][2]uint64
	Registered(origin string, instance uint64) map[string][][2]uint64
	Iterate(origin string, instance uint64, from originstream.Cursor, fn func(originstream.Record) bool) (originstream.Cursor, int, error)
	DropInstance(origin string, instance uint64) error
}

type claimantOutbox interface {
	InstanceFiles(inst uint64) []InstanceFile
}

type claimantReplayer interface {
	ReplayClaimed(ctx context.Context, r ingest.ClaimedReplay) ([]ingest.HourOutcome, error)
}

// recID names one stored record.
type recID struct {
	key   string
	cseq  uint64
	chunk uint16
}

// storedRec is one record read from the store for a replay call.
type storedRec struct {
	id      recID
	chunks  uint16
	payload []byte
}

// claimWork is what a claimant knows of one of its claims, for one epoch.
type claimWork struct {
	epoch uint64
	// done: records with every hour in a registered file (or nothing left).
	done map[recID]bool
	// chunks per (key, cseq), as stored.
	chunks map[recID]uint16
	// unreplayable batches, reported at ClaimDone.
	unreplayable map[string]raft.Intervals
	// failed counts consecutive passes a record's flush failed.
	failed map[recID]int
	warned bool
}

type claimBackoff struct {
	until time.Time
	wait  time.Duration
}

type claimant struct {
	c      *Coordinator
	store  claimantStore
	outbox claimantOutbox
	replay claimantReplayer
	// filesLocal reports whether every current hot file of each key's
	// measurement is here (the store copy may go).
	filesLocal func(ctx context.Context, origin string, keys []string) bool
	grace      time.Duration
	now        func() time.Time
	logger     zerolog.Logger

	started         time.Time
	designatedSince time.Time
	designated      bool
	lastHealthy     map[string]time.Time
	currentSeen     map[string]instanceSeen
	backoff         map[uint64]*claimBackoff
	work            map[uint64]*claimWork

	cancel context.CancelFunc
	done   chan struct{}
}

type instanceSeen struct {
	instance uint64
	at       time.Time
}

func newClaimant(c *Coordinator, store claimantStore, outbox claimantOutbox, replay claimantReplayer, filesLocal func(context.Context, string, []string) bool) *claimant {
	grace := c.cfg.StreamTakeoverGrace
	if grace <= 0 {
		grace = defaultTakeoverGrace
	}
	return &claimant{
		c: c, store: store, outbox: outbox, replay: replay, filesLocal: filesLocal, grace: grace, now: time.Now,
		logger:      c.logger.With().Str("component", "stream-claimant").Logger(),
		started:     time.Now(),
		lastHealthy: map[string]time.Time{},
		currentSeen: map[string]instanceSeen{},
		backoff:     map[uint64]*claimBackoff{},
		work:        map[uint64]*claimWork{},
	}
}

// start runs the claimant until stop (or ctx ends).
func (cl *claimant) start(ctx context.Context) {
	ctx, cl.cancel = context.WithCancel(ctx)
	cl.done = make(chan struct{})
	go func() {
		defer close(cl.done)
		cl.run(ctx)
	}()
}

// stop ends the claimant and waits for its pass to return. Called before the
// ingest buffer and the registration outbox shut down: a replay in flight
// writes through both.
func (cl *claimant) stop() {
	if cl.cancel == nil {
		return
	}
	cl.cancel()
	select {
	case <-cl.done:
	case <-time.After(30 * time.Second):
		cl.logger.Warn().Msg("Stream claimant did not stop in time")
	}
}

func (cl *claimant) run(ctx context.Context) {
	t := time.NewTicker(claimantEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cl.pass(ctx)
	}
}

// pass is one claimant pass.
func (cl *claimant) pass(ctx context.Context) {
	c := cl.c
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.StreamClaimsEnabled() || c.claimedExit.Load() {
		return
	}
	sctx, cancel := context.WithTimeout(ctx, claimantSyncBudget)
	err := c.SyncManifest(sctx)
	cancel()
	if err != nil {
		cl.logger.Debug().Err(err).Msg("Manifest not caught up; claimant pass skipped")
		return
	}
	now := cl.now()
	self := c.localNode.ID
	designated := fsm.PrimaryWriterIDFast() == self
	if designated && !cl.designated {
		cl.designatedSince = now
	}
	cl.designated = designated

	instances := fsm.StreamInstances()
	byID := make(map[uint64]raft.StreamInstance, len(instances))
	for _, in := range instances {
		byID[in.Instance] = in
	}
	claims := map[uint64]raft.StreamClaim{}
	for _, k := range fsm.StreamClaims() {
		claims[k.Instance] = k
	}
	cl.trigger(ctx, fsm, instances, claims, now)
	for _, k := range fsm.ClaimsBy(self) {
		if k.Released || k.Done {
			delete(cl.work, k.Instance)
			continue
		}
		// Untracked mid-claim: nothing can be added, and its records come
		// back unreplayable (nothing arbitrates them); it still finishes.
		in, known := byID[k.Instance]
		tracked := known && !in.Untracked
		if tracked {
			cl.extend(ctx, fsm, k)
		}
		cl.replayClaim(ctx, fsm, k, tracked)
	}
	cl.dropStale(ctx, fsm, byID, claims)
}

// liveStatus: an instance its origin still publishes itself, if alive.
func liveStatus(s string) bool { return s == raft.InstanceCurrent || s == raft.InstanceAdopted }

// claimKind is what this node may do with an instance.
type claimKind int

const (
	claimNone claimKind = iota
	// claimFirst: a live instance of a writer that is gone, never claimed:
	// the designated primary claims it (with every such instance of the
	// writer), after the grace.
	claimFirst
	// claimFollowUp: a live instance claimed before (that claim finished or
	// was released) of a writer that is gone: any writer that did not finish
	// a claim of it, after the grace.
	claimFollowUp
	// claimLetGo: closed or orphaned (its writer announced without it): any
	// writer that did not finish a claim of it, at once.
	claimLetGo
)

// claimKindOf classifies in for this node. designated: the designated
// primary's id; healthy: this node hears from the instance's writer. It
// mirrors the manifest's rules (takeover.go followUpOpenLocked).
func claimKindOf(in raft.StreamInstance, claims map[uint64]raft.StreamClaim, self, designated string, healthy bool) claimKind {
	if in.Origin == self || in.Untracked {
		return claimNone
	}
	k, claimed := claims[in.Instance]
	if claimed {
		if !k.Released && !k.Done {
			return claimNone // being published
		}
		if (k.Done && k.By == self) || slices.Contains(k.FinishedBy, self) {
			return claimNone // this node published what it held
		}
	}
	switch {
	case !liveStatus(in.Status):
		return claimLetGo
	case healthy:
		return claimNone // its writer publishes it
	}
	adopter, adopterClaimed := claims[in.AdoptedBy]
	takenOver := claimed || (in.Status == raft.InstanceAdopted && adopterClaimed)
	if designated == self {
		if takenOver {
			return claimFollowUp
		}
		return claimFirst
	}
	// Another writer follows up only while the origin is still taken over:
	// not the designated primary, and not back with a new process.
	switch {
	case designated == in.Origin:
		return claimNone
	case in.Status == raft.InstanceCurrent && claimed:
		return claimFollowUp
	case in.Status == raft.InstanceAdopted && adopterClaimed && !adopter.Released:
		return claimFollowUp
	}
	return claimNone
}

// trigger proposes the claims this node should make now.
func (cl *claimant) trigger(ctx context.Context, fsm *raft.ClusterFSM, instances []raft.StreamInstance, claims map[uint64]raft.StreamClaim, now time.Time) {
	c := cl.c
	self := c.localNode.ID
	designatedID := fsm.PrimaryWriterIDFast()
	byOrigin := map[string][]raft.StreamInstance{}
	for _, in := range instances {
		if in.Origin == self || in.Untracked {
			continue
		}
		byOrigin[in.Origin] = append(byOrigin[in.Origin], in)
	}
	origins := make([]string, 0, len(byOrigin))
	for o := range byOrigin {
		origins = append(origins, o)
	}
	sort.Strings(origins)
	for _, origin := range origins {
		healthy := c.originHealthy(origin)
		if healthy {
			cl.lastHealthy[origin] = now
		}
		for _, in := range byOrigin[origin] {
			if in.Status == raft.InstanceCurrent && cl.currentSeen[origin].instance != in.Instance {
				cl.currentSeen[origin] = instanceSeen{in.Instance, now}
			}
		}
		// The grace runs from the last time this node heard from the writer,
		// saw its current process change, or started; a first claim also
		// from this node becoming the designated primary.
		gone := maxTime(cl.started, cl.lastHealthy[origin], cl.currentSeen[origin].at)
		var first []raft.StreamInstance
		for _, in := range byOrigin[origin] {
			switch claimKindOf(in, claims, self, designatedID, healthy) {
			case claimFirst:
				first = append(first, in)
			case claimFollowUp:
				if now.Sub(gone) >= cl.grace {
					cl.followUp(ctx, fsm, in)
				}
			case claimLetGo:
				cl.triggerLetGo(ctx, fsm, in)
			}
		}
		if len(first) == 0 || c.ingestFenced() || now.Sub(maxTime(gone, cl.designatedSince)) < cl.grace {
			continue
		}
		if sets := cl.claimable(fsm, origin, first); len(sets) > 0 {
			cl.propose(ctx, origin, first, sets)
		}
	}
}

func maxTime(ts ...time.Time) time.Time {
	var m time.Time
	for _, t := range ts {
		if t.After(m) {
			m = t
		}
	}
	return m
}

// claimable returns, per instance of list, the batches held here that a
// claim would take (empty instances left out).
func (cl *claimant) claimable(fsm *raft.ClusterFSM, origin string, list []raft.StreamInstance) map[uint64]map[string]raft.Intervals {
	out := map[uint64]map[string]raft.Intervals{}
	for _, in := range list {
		held := cl.store.Held(origin, in.Instance)
		for key, ivs := range held {
			if s := fsm.ClaimableSet(in.Instance, key, raft.Intervals(ivs)); len(s) > 0 {
				if out[in.Instance] == nil {
					out[in.Instance] = map[string]raft.Intervals{}
				}
				out[in.Instance][key] = s
			}
		}
	}
	return out
}

// triggerLetGo handles an instance its origin announced without (closed or
// orphaned): any writer holding batches of it claims them at once; one
// holding nothing says so, so the instance can be removed once every writer
// has.
func (cl *claimant) triggerLetGo(ctx context.Context, fsm *raft.ClusterFSM, in raft.StreamInstance) {
	cl.followUp(ctx, fsm, in)
}

// followUp claims the batches of in this node holds that are claimable, or
// reports holding none (once): DELETEs of its keys wait for every writer's
// answer.
func (cl *claimant) followUp(ctx context.Context, fsm *raft.ClusterFSM, in raft.StreamInstance) {
	sets := cl.claimable(fsm, in.Origin, []raft.StreamInstance{in})
	if len(sets) == 0 {
		for _, r := range in.ReportedBy {
			if r == cl.c.localNode.ID {
				return
			}
		}
		if cl.backingOff(in.Instance) {
			return
		}
		cl.proposeTake(ctx, raft.TakeOverStreamV2Payload{Origin: in.Origin, By: cl.c.localNode.ID, Now: cl.now().UTC(),
			Claims: []raft.ClaimSet{{Instance: in.Instance}}}, []uint64{in.Instance}, "report")
		return
	}
	cl.propose(ctx, in.Origin, []raft.StreamInstance{in}, sets)
}

// propose sends one TakeOverStreamV2 for list, with as many keys as one
// command carries; the rest are added by Extend on later passes. Every
// instance of list is named (a live chain is claimed together: an adopted
// instance is claimable only with its adopter).
func (cl *claimant) propose(ctx context.Context, origin string, list []raft.StreamInstance, sets map[uint64]map[string]raft.Intervals) {
	ids := make([]uint64, 0, len(list))
	for _, in := range list {
		if cl.backingOff(in.Instance) {
			return
		}
		ids = append(ids, in.Instance)
	}
	// Current first (an adopted instance names it), then by id.
	sort.SliceStable(list, func(i, j int) bool {
		if (list[i].Status == raft.InstanceCurrent) != (list[j].Status == raft.InstanceCurrent) {
			return list[i].Status == raft.InstanceCurrent
		}
		return list[i].Instance < list[j].Instance
	})
	budget := claimChunkBytes
	p := raft.TakeOverStreamV2Payload{Origin: origin, By: cl.c.localNode.ID, Now: cl.now().UTC()}
	for _, in := range list {
		cs := raft.ClaimSet{Instance: in.Instance}
		chunk, _ := takeChunk(sets[in.Instance], &budget)
		if len(chunk) > 0 {
			cs.Keys = chunk
		} else if len(sets[in.Instance]) > 0 {
			// Nothing of it fits this command: listed with no keys it would
			// read as a report. Left out; claimed on a later pass.
			if in.Status != raft.InstanceCurrent {
				continue
			}
		}
		p.Claims = append(p.Claims, cs)
	}
	cl.proposeTake(ctx, p, ids, "claim")
}

// takeChunk returns the keys of set, in key order, that fit in budget
// (always at least one when set is not empty), and the keys left.
func takeChunk(set map[string]raft.Intervals, budget *int) (map[string]raft.Intervals, []string) {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := map[string]raft.Intervals{}
	var left []string
	for i, k := range keys {
		size := len(k) + 8 + 44*len(set[k])
		if size > *budget && (len(out) > 0 || *budget < claimChunkBytes) {
			left = keys[i:]
			break
		}
		out[k] = set[k]
		*budget -= size
	}
	return out, left
}

func (cl *claimant) proposeTake(ctx context.Context, p raft.TakeOverStreamV2Payload, ids []uint64, what string) {
	raw, err := json.Marshal(p)
	if err != nil {
		return
	}
	err = cl.c.applyOrForward(ctx, &raft.Command{Type: raft.CommandTakeOverStreamV2, Payload: raw})
	if err != nil {
		for _, id := range ids {
			cl.backOff(id)
		}
		cl.logger.Warn().Err(err).Str("origin", p.Origin).Str("what", what).Msg("Stream takeover proposal refused; retrying later")
		return
	}
	for _, id := range ids {
		delete(cl.backoff, id)
	}
	if what == "claim" {
		cl.logger.Warn().Str("origin", p.Origin).Int("instances", len(p.Claims)).
			Msg("Claimed the unregistered batches of a writer that is gone (stream takeover); publishing them from the standby copy")
	}
}

func (cl *claimant) backingOff(inst uint64) bool {
	b := cl.backoff[inst]
	return b != nil && cl.now().Before(b.until)
}

func (cl *claimant) backOff(inst uint64) {
	b := cl.backoff[inst]
	if b == nil {
		b = &claimBackoff{}
		cl.backoff[inst] = b
	}
	if b.wait < claimBackoffMin {
		b.wait = claimBackoffMin
	} else if b.wait *= 2; b.wait > claimBackoffMax {
		b.wait = claimBackoffMax
	}
	b.until = cl.now().Add(b.wait)
}

// extend adds to an unfinished claim of this node the batches held here
// that it may claim and does not have yet.
func (cl *claimant) extend(ctx context.Context, fsm *raft.ClusterFSM, k raft.StreamClaim) {
	missing := map[string]raft.Intervals{}
	for key, ivs := range cl.store.Held(k.Origin, k.Instance) {
		s := fsm.ClaimableSet(k.Instance, key, raft.Intervals(ivs)).Minus(k.Set[key])
		if len(s) > 0 {
			missing[key] = s
		}
	}
	for len(missing) > 0 {
		budget := claimChunkBytes
		chunk, left := takeChunk(missing, &budget)
		raw, err := json.Marshal(raft.ExtendStreamClaimPayload{Instance: k.Instance, By: k.By, Epoch: k.Epoch, Keys: chunk})
		if err != nil {
			return
		}
		if err := cl.c.applyOrForward(ctx, &raft.Command{Type: raft.CommandExtendStreamClaim, Payload: raw}); err != nil {
			cl.logger.Warn().Err(err).Str("instance", fmt.Sprintf("%016x", k.Instance)).Msg("Extending a stream claim failed; retrying")
			return
		}
		rest := map[string]raft.Intervals{}
		for _, key := range left {
			rest[key] = missing[key]
		}
		missing = rest
	}
}

func (cl *claimant) workFor(k raft.StreamClaim) *claimWork {
	w := cl.work[k.Instance]
	if w == nil || w.epoch != k.Epoch {
		w = &claimWork{epoch: k.Epoch, done: map[recID]bool{}, chunks: map[recID]uint16{},
			unreplayable: map[string]raft.Intervals{}, failed: map[recID]int{}}
		cl.work[k.Instance] = w
	}
	return w
}

func (w *claimWork) markUnreplayable(key string, cseq uint64) {
	w.unreplayable[key] = w.unreplayable[key].Union(raft.Intervals{{cseq, cseq}})
}

// replayClaim is one replay round of an unfinished claim of this node; it
// finishes the claim when nothing of it is left to publish.
func (cl *claimant) replayClaim(ctx context.Context, fsm *raft.ClusterFSM, k raft.StreamClaim, tracked bool) {
	if k.ExtendedAt > fsm.AppliedDone() || k.Epoch > fsm.AppliedDone() {
		return // not applied here yet
	}
	w := cl.workFor(k)
	inst := fmt.Sprintf("%016x", k.Instance)

	// Files of this claim still on their way: their batches wait.
	inflight := map[string]raft.Intervals{}
	for _, f := range cl.outbox.InstanceFiles(k.Instance) {
		if f.Epoch != k.Epoch {
			continue
		}
		var cseqs raft.Intervals
		for _, r := range f.Ranges {
			cseqs = cseqs.Union(raft.Intervals{r})
		}
		for _, p := range f.Partial {
			cseqs = cseqs.Union(raft.Intervals{{p.CSeq, p.CSeq}})
		}
		if !f.Parked {
			inflight[f.Key] = inflight[f.Key].Union(cseqs)
			continue
		}
		switch f.Kind {
		case raft.RefusedClaimed, raft.RefusedStale, raft.RefusedFull:
			// The manifest refused this claim's own file: its batches cannot
			// be published under this claim.
			for _, iv := range cseqs.Minus(w.unreplayable[f.Key]) {
				for c := iv[0]; c <= iv[1]; c++ {
					w.markUnreplayable(f.Key, c)
				}
			}
			cl.logger.Error().Str("instance", inst).Str("key", f.Key).Str("kind", f.Kind).Str("path", f.Path).
				Msg("A replayed file of a stream claim was refused by the manifest; its batches are left to their origin")
		}
	}

	// What still needs a replay round.
	need := map[string]raft.Intervals{}
	for key, ivs := range cl.outstanding(k, w) {
		if left := ivs.Minus(inflight[key]); len(left) > 0 {
			need[key] = left
		}
	}
	if len(need) > 0 && !cl.replayRound(ctx, fsm, k, w, need) {
		return
	}
	if len(inflight) > 0 || len(cl.outstanding(k, w)) > 0 {
		return // files on their way, or batches written this round
	}
	// Nothing missing either: a batch held here but not claimed yet would be
	// left behind by a finished claim (none can be added to an untracked
	// instance).
	for key, ivs := range cl.store.Held(k.Origin, k.Instance) {
		if !tracked {
			break
		}
		if len(fsm.ClaimableSet(k.Instance, key, raft.Intervals(ivs)).Minus(k.Set[key])) > 0 {
			return
		}
	}
	cl.finish(ctx, k, w)
}

// outstanding returns the claimed batches neither done nor unreplayable.
func (cl *claimant) outstanding(k raft.StreamClaim, w *claimWork) map[string]raft.Intervals {
	out := map[string]raft.Intervals{}
	for key, set := range k.Set {
		for _, iv := range set.Minus(w.unreplayable[key]) {
			for c := iv[0]; c <= iv[1]; c++ {
				if !w.cseqDone(key, c) {
					out[key] = out[key].Union(raft.Intervals{{c, c}})
				}
			}
		}
	}
	return out
}

// cseqDone reports whether every stored record of (key, cseq) is done.
func (w *claimWork) cseqDone(key string, cseq uint64) bool {
	n, ok := w.chunks[recID{key, cseq, 0}]
	if !ok {
		return false
	}
	for ch := uint16(0); ch < max(n, 1); ch++ {
		if !w.done[recID{key, cseq, ch}] {
			return false
		}
	}
	return true
}

// replayRound reads the store once for the records of need and replays
// them. False: the round could not run (the pass ends).
func (cl *claimant) replayRound(ctx context.Context, fsm *raft.ClusterFSM, k raft.StreamClaim, w *claimWork, need map[string]raft.Intervals) bool {
	inst := fmt.Sprintf("%016x", k.Instance)
	seen := map[recID]uint16{} // (key, cseq, 0) -> records yielded
	taken := map[recID]bool{}
	// The store is read in slices of one replay call at most, each replayed
	// before the next is read: a claim may be as large as the store.
	cursor := originstream.Cursor{}
	damaged := 0
	for {
		byKey := map[string][]storedRec{}
		n, bytes, full := 0, 0, false
		next, bad, err := cl.store.Iterate(k.Origin, k.Instance, cursor, func(r originstream.Record) bool {
			if !need[r.Key].Contains(r.CSeq) {
				return true
			}
			id := recID{r.Key, r.CSeq, r.Chunk}
			if w.done[id] || taken[id] {
				seen[recID{r.Key, r.CSeq, 0}]++
				return true
			}
			if n > 0 && (n >= replayCallRecords || bytes+len(r.Payload) > replayCallBytes) {
				full = true
				return false // not taken: the next slice starts with it
			}
			n, bytes = n+1, bytes+len(r.Payload)
			taken[id] = true
			seen[recID{r.Key, r.CSeq, 0}]++
			w.chunks[recID{r.Key, r.CSeq, 0}] = r.Chunks
			byKey[r.Key] = append(byKey[r.Key], storedRec{id, r.Chunks, append([]byte(nil), r.Payload...)})
			return ctx.Err() == nil
		})
		damaged += bad
		if errors.Is(err, originstream.ErrNoInstance) {
			cl.selfRelease(ctx, k)
			return false
		}
		if err != nil || ctx.Err() != nil {
			cl.logger.Warn().Err(err).Str("instance", inst).Msg("Reading the standby copy of a claimed instance failed; retrying")
			return false
		}
		if !cl.replayKeys(ctx, fsm, k, w, byKey) {
			return false
		}
		if !full {
			break
		}
		cursor = next
	}
	if damaged > 0 {
		cl.logger.Error().Str("instance", inst).Int("segments", damaged).Msg("Part of the standby copy of a claimed instance does not read back")
	}
	// Claimed batches this store does not hold whole.
	registered := cl.store.Registered(k.Origin, k.Instance)
	for key, ivs := range need {
		floor := fsm.KeyFloor(k.Instance, key)
		for _, iv := range ivs {
			for c := iv[0]; c <= iv[1]; c++ {
				id := recID{key, c, 0}
				n := w.chunks[id]
				if seen[id] > 0 && seen[id] >= max(n, 1) {
					continue
				}
				switch {
				case len(fsm.ClaimableSet(k.Instance, key, raft.Intervals{{c, c}})) == 0:
					// Settled by its origin's floor meanwhile.
				case c <= floor:
					// At or below the floor and claimable: a file of it vanished
					// in some hour, and this copy cannot restore it.
					w.markUnreplayable(key, c)
					cl.logger.Error().Str("instance", inst).Str("key", key).Uint64("batch", c).
						Msg("A claimed batch lost a registered file and is not in this standby copy; its rows of that hour are lost")
					continue
				case raft.Intervals(registered[key]).Contains(c):
					// Its origin reported it registered in every hour.
					cl.logger.Info().Str("instance", inst).Str("key", key).Uint64("batch", c).
						Msg("A claimed batch is no longer in this standby copy; its origin registered it")
				default:
					w.markUnreplayable(key, c)
					cl.logger.Error().Str("instance", inst).Str("key", key).Uint64("batch", c).
						Msg("A claimed batch is not whole in this standby copy (damaged, or never received whole); it is left to its origin")
					continue
				}
				w.chunks[id] = 1
				w.done[recID{key, c, 0}] = true
			}
		}
	}
	return true
}

// replayKeys replays the records read, per key, in calls within the bounds.
func (cl *claimant) replayKeys(ctx context.Context, fsm *raft.ClusterFSM, k raft.StreamClaim, w *claimWork, byKey map[string][]storedRec) bool {
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		db, m, ok := strings.Cut(key, "/")
		if !ok {
			for _, p := range byKey[key] {
				w.markUnreplayable(key, p.id.cseq)
			}
			continue
		}
		recs := byKey[key]
		for len(recs) > 0 {
			n, bytes := 0, 0
			for n < len(recs) && n < replayCallRecords && (n == 0 || bytes+len(recs[n].payload) <= replayCallBytes) {
				bytes += len(recs[n].payload)
				n++
			}
			if !cl.replayCall(ctx, fsm, k, w, db, m, recs[:n]) {
				return false
			}
			recs = recs[n:]
		}
	}
	return true
}

// replayCall replays one call's records of key db/m; it halves the call on
// ErrReplayTooLarge. False: replay cannot run now (the pass ends).
func (cl *claimant) replayCall(ctx context.Context, fsm *raft.ClusterFSM, k raft.StreamClaim, w *claimWork, db, m string, recs []storedRec) bool {
	key := db + "/" + m
	call := ingest.ClaimedReplay{Database: db, Measurement: m, Instance: k.Instance, Epoch: k.Epoch}
	for _, r := range recs {
		call.Records = append(call.Records, ingest.ClaimedRecord{CSeq: r.id.cseq, Chunk: r.id.chunk, Chunks: r.chunks,
			Entry: wal.DecodeLogical(r.payload), PayloadBytes: len(r.payload),
			Keep: claimKeep(fsm, k.Instance, key, r.id.cseq, r.id.chunk, r.chunks)})
	}
	out, err := cl.replay.ReplayClaimed(ctx, call)
	switch {
	case errors.Is(err, ingest.ErrReplayTooLarge):
		if len(recs) == 1 {
			w.markUnreplayable(key, recs[0].id.cseq)
			cl.logger.Error().Str("key", key).Uint64("batch", recs[0].id.cseq).Msg("A claimed batch is too large to replay; it is left to its origin")
			return true
		}
		half := len(recs) / 2
		return cl.replayCall(ctx, fsm, k, w, db, m, recs[:half]) && cl.replayCall(ctx, fsm, k, w, db, m, recs[half:])
	case errors.Is(err, ingest.ErrReplayCoverageOff), errors.Is(err, ingest.ErrReplayClosed):
		cl.logger.Warn().Err(err).Msg("Claimed batches cannot be replayed now")
		return false
	case err != nil:
		// Retried every pass (never unreplayable); alarmed like a failing flush.
		for _, r := range recs {
			if w.failed[r.id]++; w.failed[r.id] == replayFailedAlarm {
				cl.logger.Error().Err(err).Str("key", key).Uint64("batch", r.id.cseq).Int("passes", replayFailedAlarm).
					Msg("A replay call of claimed batches keeps being refused; the claim stays open")
			}
		}
		return true
	}
	ok := make([]bool, len(recs))
	failed := make([]bool, len(recs))
	for i := range ok {
		ok[i] = true
	}
	for _, o := range out {
		if o.Record < 0 || o.Record >= len(recs) {
			continue
		}
		r := recs[o.Record]
		switch o.Outcome {
		case ingest.ReplayCovered, ingest.ReplayNothingLeft:
		case ingest.ReplayWritten:
			ok[o.Record] = false // done once its file is registered
		case ingest.ReplayUnreplayable:
			ok[o.Record] = false
			if !w.unreplayable[key].Contains(r.id.cseq) {
				cl.logger.Error().Err(o.Err).Str("key", key).Uint64("batch", r.id.cseq).
					Msg("A claimed batch cannot be published; it is left to its origin")
			}
			w.markUnreplayable(key, r.id.cseq)
		default: // ReplayFailed
			ok[o.Record] = false
			failed[o.Record] = true
		}
	}
	for i, r := range recs {
		switch {
		case ok[i]:
			w.done[r.id] = true
			delete(w.failed, r.id)
		case failed[i]:
			// Never unreplayable: a failed flush is retried until it works
			// or an operator releases the claim.
			if w.failed[r.id]++; w.failed[r.id] == replayFailedAlarm {
				cl.logger.Error().Str("key", key).Uint64("batch", r.id.cseq).Int("passes", replayFailedAlarm).
					Msg("Replaying a claimed batch keeps failing; the claim stays open")
			}
		default:
			delete(w.failed, r.id)
		}
	}
	return true
}

// selfRelease releases an unfinished claim of this node whose store copy is
// gone: another writer holding the batches may claim them.
func (cl *claimant) selfRelease(ctx context.Context, k raft.StreamClaim) {
	raw, err := json.Marshal(raft.ReleaseStreamClaimPayload{Instance: k.Instance, Epoch: k.Epoch, By: k.By, Now: cl.now().UTC()})
	if err != nil {
		return
	}
	if err := cl.c.applyOrForward(ctx, &raft.Command{Type: raft.CommandReleaseStreamClaim, Payload: raw}); err != nil {
		cl.logger.Warn().Err(err).Msg("Releasing a stream claim whose standby copy is gone failed; retrying")
		return
	}
	cl.logger.Error().Str("instance", fmt.Sprintf("%016x", k.Instance)).Str("origin", k.Origin).
		Msg("The standby copy of a claimed instance is gone from this writer; the claim is released so another writer can take it over")
}

// finish proposes ClaimDone.
func (cl *claimant) finish(ctx context.Context, k raft.StreamClaim, w *claimWork) {
	p := raft.ClaimDonePayload{Instance: k.Instance, By: k.By, Epoch: k.Epoch, Now: cl.now().UTC()}
	var bad uint64
	for key, ivs := range w.unreplayable {
		if s := ivs.Minus(nil); len(s) > 0 {
			if p.Unreplayable == nil {
				p.Unreplayable = map[string]raft.Intervals{}
			}
			p.Unreplayable[key] = s
			bad += s.Count()
		}
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return
	}
	if err := cl.c.applyOrForward(ctx, &raft.Command{Type: raft.CommandClaimDone, Payload: raw}); err != nil {
		cl.logger.Warn().Err(err).Msg("Finishing a stream claim failed; retrying")
		return
	}
	ev := cl.logger.Info()
	if bad > 0 {
		ev = cl.logger.Error()
	}
	ev.Str("instance", fmt.Sprintf("%016x", k.Instance)).Str("origin", k.Origin).Uint64("unreplayable_batches", bad).
		Msg("Stream claim finished: every claimed batch is in a registered file or reported unreplayable")
}

// dropStale removes store copies this node no longer needs, once every
// current file of their keys is here: an instance this node finished a claim
// of, one removed with its claim, and one of a writer it hears from but no
// longer receives from (not the designated primary) with nothing claimable
// left. A writer this node does not hear from keeps its copy: it may be
// dead, and a file of it may yet vanish.
func (cl *claimant) dropStale(ctx context.Context, fsm *raft.ClusterFSM, byID map[uint64]raft.StreamInstance, claims map[uint64]raft.StreamClaim) {
	self := cl.c.localNode.ID
	designatedID := fsm.PrimaryWriterIDFast()
	for _, info := range cl.store.Instances() {
		if info.Origin == self {
			continue
		}
		k, claimed := claims[info.Instance]
		in, known := byID[info.Instance]
		switch {
		case claimed && !k.Released && !k.Done:
			continue // being published
		case claimed && ((k.Done && k.By == self) || slices.Contains(k.FinishedBy, self)):
		case !known && fsm.Tombstoned(info.Instance):
			// Published or settled, and removed with its claim.
		case known && in.Origin == info.Origin && info.Origin != designatedID && cl.c.originHealthy(info.Origin) &&
			len(cl.claimable(fsm, info.Origin, []raft.StreamInstance{in})) == 0:
			// Frozen: this node no longer receives from that writer.
		default:
			continue
		}
		held := cl.store.Held(info.Origin, info.Instance)
		keys := make([]string, 0, len(held)+len(k.Set))
		for key := range held {
			keys = append(keys, key)
		}
		for key := range k.Set {
			keys = append(keys, key)
		}
		if !cl.filesLocal(ctx, info.Origin, keys) {
			continue
		}
		if err := cl.store.DropInstance(info.Origin, info.Instance); err != nil {
			cl.logger.Warn().Err(err).Msg("Dropping a standby copy no longer needed failed; retrying")
			continue
		}
		cl.logger.Info().Str("origin", info.Origin).Str("instance", fmt.Sprintf("%016x", info.Instance)).
			Msg("Standby copy dropped: its batches are published and every file of them is here")
	}
}

// StreamTakeoverBusy reports whether a DELETE of db (measurement "" = any
// of its measurements) must wait (#1207): a claim is publishing rows of it,
// or this node, the designated primary, holds batches of it that a gone
// writer left and it may claim. A DELETE applied first would be undone by
// the replay: the replayed rows were acknowledged before it. Computed at
// request time from this node's manifest (caught up first; busy when it
// cannot be) and store.
func (c *Coordinator) StreamTakeoverBusy(db, measurement string) bool {
	if c == nil || c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.StreamClaimsEnabled() {
		return false
	}
	// This node's manifest may trail the leader's (a claim or a designation
	// just committed): catch up first, and wait when it cannot.
	ctx, cancel := context.WithTimeout(context.Background(), claimantSyncBudget)
	err := c.SyncManifest(ctx)
	cancel()
	if err != nil {
		return true
	}
	match := func(key string) bool {
		d, m, ok := strings.Cut(key, "/")
		return ok && d == db && (measurement == "" || m == measurement)
	}
	for _, k := range fsm.StreamClaims() {
		if k.Released || k.Done {
			continue
		}
		for key := range k.Set {
			if match(key) {
				return true
			}
		}
	}
	cl := c.claimant.Load()
	if cl == nil {
		return false
	}
	// Another writer may still follow up on an ended claim: until every
	// writer this node hears from answered, shortly after the claim ended.
	self := c.localNode.ID
	now := time.Now()
	for _, fu := range fsm.FollowUps() {
		if now.Sub(fu.EndedAt) > followUpWindow(c) || !slices.ContainsFunc(fu.Keys, match) {
			continue
		}
		for _, n := range c.registry.GetWriters() {
			if n.ID != fu.Origin && n.IsHealthy() && !slices.Contains(fu.Answered, n.ID) {
				return true
			}
		}
	}
	// What this node may claim now or once the grace ends, from its store.
	designated := fsm.PrimaryWriterIDFast()
	byID := map[uint64]raft.StreamInstance{}
	for _, in := range fsm.StreamInstances() {
		byID[in.Instance] = in
	}
	claims := map[uint64]raft.StreamClaim{}
	for _, k := range fsm.StreamClaims() {
		claims[k.Instance] = k
	}
	for _, info := range cl.store.Instances() {
		in, known := byID[info.Instance]
		if !known || in.Origin != info.Origin || claimKindOf(in, claims, self, designated, c.originHealthy(in.Origin)) == claimNone {
			continue
		}
		for key, ivs := range cl.store.Held(info.Origin, info.Instance) {
			if match(key) && len(fsm.ClaimableSet(info.Instance, key, raft.Intervals(ivs))) > 0 {
				return true
			}
		}
	}
	return false
}

// followUpWindow is how long after a claim ended DELETEs wait for the other
// writers to answer it: a writer follows up after the grace at most, on a
// pass. A writer that answers later (it was not heard from, then came back)
// may replay rows a DELETE in between removed.
func followUpWindow(c *Coordinator) time.Duration {
	grace := c.cfg.StreamTakeoverGrace
	if grace <= 0 {
		grace = defaultTakeoverGrace
	}
	return 2*grace + 4*claimantEvery
}

// streamClaimAutoReleaseLoop, on the leader, releases an unfinished claim
// whose claimant has not been a healthy member for the grace: another
// writer holding the batches can then claim them. A claimant that comes back
// in time carries on.
func (c *Coordinator) streamClaimAutoReleaseLoop(ctx context.Context) {
	grace := c.cfg.StreamTakeoverGrace
	if grace <= 0 {
		grace = defaultTakeoverGrace
	}
	gone := map[[2]uint64]time.Time{} // (instance, epoch) -> claimant first seen gone
	t := time.NewTicker(autoReleaseEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.autoReleasePass(time.Now(), grace, gone)
	}
}

func (c *Coordinator) autoReleasePass(now time.Time, grace time.Duration, gone map[[2]uint64]time.Time) {
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.StreamClaimsEnabled() || !c.raftNode.IsLeader() {
		clear(gone) // observations restart with the next leadership
		return
	}
	live := map[[2]uint64]bool{}
	for _, k := range fsm.StreamClaims() {
		if k.Released || k.Done {
			continue
		}
		id := [2]uint64{k.Instance, k.Epoch}
		live[id] = true
		if k.By == c.localNode.ID || c.originHealthy(k.By) {
			delete(gone, id)
			continue
		}
		since, seen := gone[id]
		if !seen {
			gone[id] = now
			continue
		}
		if now.Sub(since) < grace {
			continue
		}
		raw, err := json.Marshal(raft.ReleaseStreamClaimPayload{Instance: k.Instance, Epoch: k.Epoch, Now: now.UTC()})
		if err != nil {
			continue
		}
		if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandReleaseStreamClaim, Payload: raw}, 5*time.Second); err != nil {
			c.logger.Warn().Err(err).Msg("Releasing the stream claim of a gone claimant failed; retrying")
			continue
		}
		delete(gone, id)
		c.logger.Error().Str("instance", fmt.Sprintf("%016x", k.Instance)).Str("claimant", k.By).Dur("gone_for", now.Sub(since)).
			Msg("A stream claimant is gone with its claim unfinished; the claim is released so another writer can take the batches over")
	}
	for id := range gone {
		if !live[id] {
			delete(gone, id)
		}
	}
}
