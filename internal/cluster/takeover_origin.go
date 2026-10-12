package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/ingest"
)

// Stream takeover, origin side (#1207 S4b). A writer with
// cluster.stream_takeover tells the cluster about its processes, so a
// standby can tell a primary that restarted with its WAL (its earlier
// batches are still its own) from one that died or lost its disks (they are
// the standby's to take over):
//
//   - AnnounceInstance once per process, before it acknowledges a write or
//     registers a file, naming the earlier instances it still holds;
//   - SettleCoverage, raising its instance's accounted floors;
//   - InstanceSettled, once an earlier instance has nothing left here.
//
// All of it waits for the cluster to have stream takeover on. Until this
// node knows it is on, writes are admitted as before.

// TakeoverOrigin is what the origin side needs from the rest of the process.
type TakeoverOrigin struct {
	// Instance is this process's writer instance (the WAL's tracked instance).
	Instance uint64
	// The three volumes a writer's unflushed batches live on. Each gets a
	// marker naming the last instance that started there.
	WALDir, JournalDir, StorageBase string
	// Holds returns the instances with batches still pending, parked or in
	// the WAL here (this one included or not). An error stops the
	// announcement: an incomplete answer would declare held batches lost.
	Holds func() ([]uint64, error)
	// Progress returns this instance's accounted progress per key, from the
	// floors given (the last ones the cluster accepted).
	Progress func(from map[string]uint64) (uint64, map[string]ingest.KeySettlement, bool)
	// SentCovered reports whether this instance already sent a covered
	// registration: if so its announcement is not a fresh start.
	SentCovered func() bool
	// OnClaimed runs once when another writer claims this process's
	// instance: the process must exit and restart (a new instance). Writes
	// are already refused when it runs.
	OnClaimed func()
}

const (
	takeoverMarkerName   = "instance.last"
	takeoverOnMarkerName = "takeover.on"
	takeoverInstancesDir = "_instances"
	takeoverAnnounceTick = 2 * time.Second
	takeoverSettleEvery  = 30 * time.Second
	takeoverSettledEvery = time.Minute
	maxAnnouncedHolds    = 256
)

type takeoverOriginState struct {
	cfg        TakeoverOrigin
	prev       raft.Continuity // markers read before this process overwrote them
	knownOn    atomic.Bool     // this node has seen the flag applied (now or before)
	announced  atomic.Bool
	settleMu   sync.Mutex // settleCoverage: the origin loop and the final report
	lastFloors map[string]uint64
	retryAt    map[uint64]time.Time // InstanceSettled backoff per instance
	retryWait  map[uint64]time.Duration
}

// SetTakeoverOrigin installs the origin side. Call before Start, with
// cluster.stream_takeover on a writer.
func (c *Coordinator) SetTakeoverOrigin(o TakeoverOrigin) {
	st := &takeoverOriginState{cfg: o}
	st.prev = raft.Continuity{
		WAL:     readInstanceMarker(filepath.Join(o.WALDir, takeoverMarkerName)),
		Journal: readInstanceMarker(filepath.Join(o.JournalDir, takeoverMarkerName)),
		Storage: readInstanceMarker(c.storageMarkerPath(o)),
	}
	if _, err := os.Stat(filepath.Join(o.JournalDir, takeoverOnMarkerName)); err == nil {
		st.knownOn.Store(true)
	}
	c.takeoverOrigin.Store(st)
}

func (c *Coordinator) storageMarkerPath(o TakeoverOrigin) string {
	if o.StorageBase == "" {
		return ""
	}
	return filepath.Join(o.StorageBase, takeoverInstancesDir, c.localNode.ID+"."+takeoverMarkerName)
}

// takeoverGateClosed reports whether writes and registrations must wait for
// this instance's announcement.
func (c *Coordinator) takeoverGateClosed() bool {
	st := c.takeoverOrigin.Load()
	return st != nil && st.knownOn.Load() && !st.announced.Load()
}

// takeoverDesignationRefuses reports whether another writer is the
// designated primary while stream takeover is on here (#1207): a write
// acknowledged by a standby could be claimed by nobody. Read synchronously
// from the manifest, unlike the ingest fence, which evaluates on a ticker.
func (c *Coordinator) takeoverDesignationRefuses() bool {
	st := c.takeoverOrigin.Load()
	if st == nil || !st.knownOn.Load() || c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return false
	}
	d := fsm.PrimaryWriterIDFast()
	return d != "" && d != c.localNode.ID
}

// TakeoverSendGate is the registration outbox's send gate.
func (c *Coordinator) TakeoverSendGate() func() bool {
	return func() bool { return !c.takeoverGateClosed() }
}

var (
	errTakeoverAnnouncing = errors.New("ingest paused: announcing this writer instance to the cluster (stream takeover); retry shortly")
	errTakeoverClaimed    = errors.New("ingest refused: another writer took over this writer process (stream takeover); it is restarting, write to the primary writer")
	errNotDesignated      = errors.New("ingest refused: this node is not the designated primary writer; write to the primary writer")
)

// readInstanceMarker returns the instance a marker names, 0 when it is
// missing or unreadable (which fails safe: no continuity).
func readInstanceMarker(path string) uint64 {
	if path == "" {
		return 0
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 16, 64)
	if err != nil {
		return 0
	}
	return v
}

// writeInstanceMarker writes a marker atomically: temp file, fsync, rename,
// directory fsync.
func writeInstanceMarker(path string, inst uint64) error {
	if path == "" {
		return nil
	}
	if err := mkdirAllSynced(filepath.Dir(path), filepath.Dir(filepath.Dir(path))); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "%016x\n", inst); err != nil {
		f.Close()
		return err
	}
	if err := fsyncFile(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

// streamTakeoverOriginLoop runs the origin side until ctx ends.
func (c *Coordinator) streamTakeoverOriginLoop(ctx context.Context) {
	st := c.takeoverOrigin.Load()
	if st == nil {
		return
	}
	c.ingestPauseChanged()
	caughtUp := false
	var lastSettle, lastSettled time.Time
	t := time.NewTicker(takeoverAnnounceTick)
	defer t.Stop()
	for {
		if !caughtUp {
			sctx, cancel := context.WithTimeout(ctx, outboxAttemptTimeout)
			caughtUp = c.SyncManifest(sctx) == nil
			cancel()
		}
		if caughtUp {
			c.takeoverOriginPass(ctx, st, &lastSettle, &lastSettled)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (c *Coordinator) takeoverOriginPass(ctx context.Context, st *takeoverOriginState, lastSettle, lastSettled *time.Time) {
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return
	}
	if !fsm.StreamClaimsEnabled() {
		if st.knownOn.Load() {
			// Caught up, and the cluster does not have takeover on (a Raft
			// data directory from another cluster, or restored from before
			// the flag): the marker was wrong. Admit writes.
			_ = os.Remove(filepath.Join(st.cfg.JournalDir, takeoverOnMarkerName))
			st.knownOn.Store(false)
			c.ingestPauseChanged()
			c.logger.Warn().Msg("This node recorded that stream takeover was on, but the cluster does not have it on; writes are admitted")
		}
		return
	}
	if !st.knownOn.Load() {
		// On mid-run: this instance may have registered files already.
		if err := writeOnMarker(st.cfg.JournalDir); err != nil {
			c.logger.Warn().Err(err).Msg("Could not record that stream takeover is on")
		}
		st.knownOn.Store(true)
		c.ingestPauseChanged()
	}
	if !st.announced.Load() {
		if err := c.announceInstance(ctx, st); err != nil {
			c.logger.Warn().Err(err).Msg("Announcing this writer instance failed; writes wait and it is retried")
			return
		}
		st.announced.Store(true)
		c.ingestPauseChanged()
		c.logger.Info().Str("instance", fmt.Sprintf("%016x", st.cfg.Instance)).Msg("Writer instance announced to the cluster (stream takeover)")
	}
	if c.checkInstanceClaimed(st) {
		return
	}
	if time.Since(*lastSettle) >= takeoverSettleEvery {
		*lastSettle = time.Now()
		if err := c.settleCoverage(ctx, st); err != nil {
			c.logger.Warn().Err(err).Msg("Reporting settled batches to the cluster failed; retrying")
		}
	}
	if time.Since(*lastSettled) >= takeoverSettledEvery {
		*lastSettled = time.Now()
		c.settleInstances(ctx, st)
	}
}

// checkInstanceClaimed reports whether another writer claimed this
// process's instance (#1207 E1): its batches are being published by the
// claimant, so nothing more of this instance may be acknowledged. Writes are
// refused at once and the process exits for a restart, which starts a new,
// unclaimed instance. Only this process's instance counts: an earlier one it
// adopted may be claimed (its own copies then park) without stopping it.
func (c *Coordinator) checkInstanceClaimed(st *takeoverOriginState) bool {
	if c.claimedExit.Load() {
		return true
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return false
	}
	state, ok := fsm.TakeoverInstanceStates()[st.cfg.Instance]
	if !ok || !state.Claimed || state.ClaimRelease {
		return false
	}
	var claim raft.StreamClaim
	for _, cl := range fsm.StreamClaims() {
		if cl.Instance == st.cfg.Instance {
			claim = cl
		}
	}
	if claim.Released || claim.By == "" || claim.By == c.localNode.ID {
		return false
	}
	if !c.claimedExit.CompareAndSwap(false, true) {
		return true
	}
	c.ingestPauseChanged()
	c.logger.Error().Str("instance", fmt.Sprintf("%016x", st.cfg.Instance)).Str("claimant", claim.By).Uint64("epoch", claim.Epoch).
		Msg("Another writer took over the batches of this writer process (stream takeover): writes are refused and the process exits for a restart")
	if st.cfg.OnClaimed != nil {
		go st.cfg.OnClaimed()
	}
	return true
}

// finalTakeoverReport runs before a graceful leave, after the final flush
// and the registrar drain (#1207): the cluster and the standbys learn every
// batch this writer registered, so a takeover of what it leaves behind
// claims only what is really unregistered.
func (c *Coordinator) finalTakeoverReport() {
	st := c.takeoverOrigin.Load()
	if st == nil || !st.announced.Load() || c.claimedExit.Load() {
		return
	}
	if fsm := c.raftNode.FSM(); fsm == nil || !fsm.StreamClaimsEnabled() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.settleCoverage(ctx, st); err != nil {
		c.logger.Warn().Err(err).Msg("Final report of settled batches failed; a takeover may claim batches this writer registered (the index refuses their duplicates)")
	}
	c.mu.RLock()
	ost := c.originStream
	c.mu.RUnlock()
	if ost != nil {
		if err := ost.server.FlushFloors(ctx); err != nil {
			c.logger.Warn().Err(err).Msg("Final registration progress not sent to every standby writer")
		}
	}
}

func writeOnMarker(dir string) error {
	path := filepath.Join(dir, takeoverOnMarkerName)
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	return writeInstanceMarker(path, 1)
}

// announcedHolds is the instances of this origin, other than the current
// one, with batches still held here: at most maxAnnouncedHolds.
func (c *Coordinator) announcedHolds(st *takeoverOriginState) ([]uint64, error) {
	var holds []uint64
	if st.cfg.Holds != nil {
		all, err := st.cfg.Holds()
		if err != nil {
			return nil, fmt.Errorf("list the writer instances held here: %w", err)
		}
		// Another writer's instance held here (a claimant's replay files
		// of it) is not this origin's to adopt: the manifest ignores it, and
		// it would crowd this origin's own out of the announcement cap.
		foreign := map[uint64]bool{}
		if fsm := c.raftNode.FSM(); fsm != nil {
			for _, in := range fsm.StreamInstances() {
				if in.Origin != c.localNode.ID {
					foreign[in.Instance] = true
				}
			}
		}
		for _, h := range all {
			if h != st.cfg.Instance && h != 0 && !foreign[h] {
				holds = append(holds, h)
			}
		}
	}
	sort.Slice(holds, func(i, j int) bool { return holds[i] < holds[j] })
	if len(holds) > maxAnnouncedHolds {
		// The rest stay unadopted: orphaned, claimable by a standby. Safe.
		c.logger.Error().Int("held", len(holds)).Int("announced", maxAnnouncedHolds).
			Msg("This writer holds batches of more earlier processes than one announcement carries; the others are treated as lost by this node")
		holds = holds[:maxAnnouncedHolds]
	}
	return holds, nil
}

func (c *Coordinator) announceInstance(ctx context.Context, st *takeoverOriginState) error {
	holds, err := c.announcedHolds(st)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(raft.AnnounceInstancePayload{Origin: c.localNode.ID, Instance: st.cfg.Instance,
		Continuity: st.prev, Holds: holds, FreshStart: st.cfg.SentCovered == nil || !st.cfg.SentCovered()})
	if err != nil {
		return err
	}
	if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandAnnounceInstance, Payload: raw}); err != nil {
		return err
	}
	for _, p := range []string{filepath.Join(st.cfg.WALDir, takeoverMarkerName), filepath.Join(st.cfg.JournalDir, takeoverMarkerName), c.storageMarkerPath(st.cfg)} {
		if err := writeInstanceMarker(p, st.cfg.Instance); err != nil {
			// A missing marker reads as 0 next start: no continuity, so
			// earlier instances are orphaned (claimable). Safe.
			c.logger.Warn().Err(err).Str("marker", p).Msg("Could not record this writer instance on a volume")
		}
	}
	return nil
}

func (c *Coordinator) settleCoverage(ctx context.Context, st *takeoverOriginState) error {
	if st.cfg.Progress == nil {
		return nil
	}
	st.settleMu.Lock()
	defer st.settleMu.Unlock()
	if st.lastFloors == nil {
		st.lastFloors = map[string]uint64{}
	}
	inst, keys, ok := st.cfg.Progress(st.lastFloors)
	if !ok || inst != st.cfg.Instance {
		return nil
	}
	names := make([]string, 0, len(keys))
	for k, v := range keys {
		if v.Floor > st.lastFloors[k] {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	// Several proposals when the report is large: one stuck key must not
	// keep every other key from settling.
	const maxKeysPerProposal = 256
	for len(names) > 0 {
		n := len(names)
		if n > maxKeysPerProposal {
			n = maxKeysPerProposal
		}
		batch := names[:n]
		names = names[n:]
		send := make(map[string]raft.KeySettlement, len(batch))
		for _, k := range batch {
			v := keys[k]
			send[k] = raft.KeySettlement{Floor: v.Floor, Lost: raft.Intervals(v.Lost)}
		}
		raw, err := json.Marshal(raft.SettleCoveragePayload{Origin: c.localNode.ID, Instance: inst, Keys: send})
		if err != nil {
			return err
		}
		if len(raw) > raft.MaxTakeoverPayloadSize {
			c.logger.Warn().Int("bytes", len(raw)).Int("keys", len(batch)).Msg("Settled batches report too large; these keys are skipped this round")
			continue
		}
		if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandSettleCoverage, Payload: raw}); err != nil {
			return err
		}
		for _, k := range batch {
			st.lastFloors[k] = keys[k].Floor
		}
	}
	return nil
}

// settleInstances reports earlier instances this writer adopted and no
// longer holds anything of.
func (c *Coordinator) settleInstances(ctx context.Context, st *takeoverOriginState) {
	fsm := c.raftNode.FSM()
	if fsm == nil || st.cfg.Holds == nil {
		return
	}
	all, err := st.cfg.Holds()
	if err != nil {
		return
	}
	held := map[uint64]bool{}
	for _, h := range all {
		held[h] = true
	}
	for _, in := range fsm.StreamInstances() {
		if in.Origin != c.localNode.ID || in.Status != raft.InstanceAdopted || in.AdoptedBy != st.cfg.Instance || held[in.Instance] {
			continue
		}
		if time.Now().Before(st.retryAt[in.Instance]) {
			continue
		}
		raw, err := json.Marshal(raft.InstanceSettledPayload{Origin: c.localNode.ID, Instance: in.Instance})
		if err != nil {
			continue
		}
		if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandInstanceSettled, Payload: raw}); err != nil {
			// Refused while a claim of it is unfinished: back off.
			if st.retryAt == nil {
				st.retryAt, st.retryWait = map[uint64]time.Time{}, map[uint64]time.Duration{}
			}
			w := st.retryWait[in.Instance]*2 + takeoverSettledEvery
			if w > time.Hour {
				w = time.Hour
			}
			st.retryWait[in.Instance], st.retryAt[in.Instance] = w, time.Now().Add(w)
			c.logger.Debug().Err(err).Str("instance", fmt.Sprintf("%016x", in.Instance)).Msg("Earlier writer instance not settled yet")
		}
	}
}

// ingestPauseChanged tells the ingest fence listeners (MQTT) whether live
// writes are refused now: the fence or the takeover gate. Serialized and
// deduplicated, and each call reads the state afresh, so the last
// notification is always the current state.
func (c *Coordinator) ingestPauseChanged() {
	go func() {
		c.pauseNotifyMu.Lock()
		defer c.pauseNotifyMu.Unlock()
		paused := c.ingestFenced() || c.takeoverGateClosed() || c.claimedExit.Load()
		if c.pauseNotified && c.pauseState == paused {
			return
		}
		c.pauseNotified, c.pauseState = true, paused
		c.mu.Lock()
		ls := append([]func(bool){}, c.ingestFenceListeners...)
		c.mu.Unlock()
		for _, fn := range ls {
			fn(paused)
		}
	}()
}
