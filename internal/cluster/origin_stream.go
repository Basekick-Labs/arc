package cluster

import (
	"context"
	"fmt"
	"math"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/originstream"
	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/wal"
)

// Origin stream wiring (#1207 S3). With cluster.wal_coverage on, every writer
// in a Pattern 1 cluster runs both sides:
//   - the server, which streams this node's committed batches to the standby
//     writers while this node is the designated primary;
//   - the client, which receives from the designated primary while this node
//     is a standby, into the standby store under <wal.directory>/received.
//
// Nothing received is applied to a buffer or the local WAL.
type originStream struct {
	server *originstream.Server
	store  *originstream.Store
	cancel context.CancelFunc
	wal    *wal.Writer
	done   chan struct{}

	presentMu sync.Mutex
	// Paths verified present locally at their size, by the last trim pass
	// and the last drop check.
	trimPresent, dropPresent map[string]int64

	spaceDone chan struct{} // the free-space watcher exited

}

// StartOriginStream starts both sides. Call after Start, on a writer whose
// buffer has coverage enabled. A no-op elsewhere.
//
// With cluster.stream_takeover it also starts the claimant (#1207), which
// publishes a gone writer's unregistered batches from the store; outbox is
// this node's registration outbox (the claimant's files go through it).
func (c *Coordinator) StartOriginStream(walWriter *wal.Writer, buffer *ingest.ArrowBuffer, outbox *RegistrationOutbox) error {
	if outbox != nil {
		pending := outbox.IsPending
		c.pendingRegistration.Store(&pending)
	}
	if !c.cfg.WALCoverage || c.cfg.SharedStorageMode || c.raftNode == nil || walWriter == nil || buffer == nil {
		return nil
	}
	if c.localNode.Role != RoleWriter {
		return nil
	}
	if _, _, ok := buffer.RegistrationProgress(); !ok {
		return nil // coverage did not come up on this buffer
	}
	if c.cfg.StreamTakeover && outbox != nil {
		// Before anything below can fail: this writer announces its process
		// either way, so DELETE records name it, and it must acknowledge them
		// and apply them to the files it registers (#1207 d1).
		c.startDeleteRecords(c.ctx, buffer, walWriter.TrackedInstance, outbox)
	}
	if c.cfg.SharedSecret == "" {
		return fmt.Errorf("the origin stream needs cluster.shared_secret")
	}
	if c.storage == nil {
		return fmt.Errorf("the origin stream needs the storage backend")
	}
	root := filepath.Join(filepath.Dir(walWriter.CurrentFile()), "received")
	maxBytes := c.standbyStoreCap(filepath.Dir(root))
	store, err := originstream.OpenStore(root, maxBytes, c.logger.With().Str("component", "standby-store").Logger())
	if err != nil {
		return err
	}

	if c.cfg.StreamTakeover {
		policy := &standbyEvictPolicy{c: c}
		store.SetEvictable(policy.tier, policy.active)
	}
	fsm := c.raftNode.FSM()
	st := &originStream{wal: walWriter, store: store, done: make(chan struct{})}
	st.server = originstream.NewServer(c.cfg.OriginStreamQueueBytes, func() *originstream.Floors {
		inst, keys, ok := buffer.RegistrationProgress()
		if !ok {
			return nil
		}
		// Read after the progress: every registration it reflects is
		// visible here (the outbox settles a covered file only then), so it
		// is at or below the last command this FSM began applying. Not
		// Raft's AppliedIndex: that also counts barrier and no-op entries a
		// standby's FSM never applies, so a standby could never reach it.
		applied := fsm.AppliedStarted()
		if applied == 0 {
			return nil // just restored from an older snapshot: no bound yet
		}
		f := &originstream.Floors{Instance: inst, AppliedIndex: applied, Keys: make(map[string]originstream.KeyFloor, len(keys))}
		for k, r := range keys {
			f.Keys[k] = originstream.KeyFloor{Floor: r.Floor, Settled: r.Settled, Lost: r.Lost}
		}
		return f
	}, c.logger.With().Str("component", "origin-stream").Logger())
	st.server.Start()
	walWriter.SetReplicationHook(st.server.Hook)

	ctx, cancel := context.WithCancel(c.ctx)
	st.cancel = cancel
	st.spaceDone = make(chan struct{})
	go func() {
		defer close(st.spaceDone)
		c.watchStandbyStoreSpace(ctx, store, root)
	}()
	client := originstream.NewClient(originstream.ClientConfig{
		Store:        store,
		LocalNodeID:  c.localNode.ID,
		ClusterName:  c.cfg.ClusterName,
		SharedSecret: c.cfg.SharedSecret,
		Target:       c.originStreamTarget,
		Dial: func(ctx context.Context, addr string) (net.Conn, error) {
			return security.DialContext(ctx, "tcp", addr, 10*time.Second, c.tlsConfig)
		},
		Ready:  st.trimReady(c),
		Logger: c.logger.With().Str("component", "standby-stream").Logger(),
	})
	go func() {
		defer close(st.done)
		client.Run(ctx)
	}()
	go c.originStreamPrimaryWatch(ctx, st)
	if c.cfg.StreamTakeover {
		buffer.SetClaimedBatch(fsm.ClaimedFromOrigin)
	}
	if c.cfg.StreamTakeover && outbox != nil {
		cl := newClaimant(c, store, outbox, buffer, st.instanceFilesLocal(c))
		if dir := filepath.Dir(walWriter.CurrentFile()); walWriter.CurrentFile() != "" {
			cl.republish = newRepublisher(c, outbox, buffer, dir, walWriter.TrackedInstance)
			cl.republish.current = walWriter.CurrentFile
			outbox.SetParkedCovered(cl.republish.ParkedCovered)
		}
		cl.start(ctx)
		c.claimant.Store(cl)
	}

	c.mu.Lock()
	c.originStream = st
	c.mu.Unlock()
	c.logger.Info().Str("store", root).Msg("Origin stream enabled: the primary streams committed batches to standby writers, which keep them until registered")
	return nil
}

// originStreamTarget is the designated primary to receive from, when this
// node is a standby writer.
func (c *Coordinator) originStreamTarget() (string, string, bool) {
	if c.localNode.Role != RoleWriter {
		return "", "", false
	}
	designated := c.designatedPrimaryWriterID()
	if designated == "" || designated == c.localNode.ID {
		return "", "", false
	}
	if node, ok := c.registry.Get(designated); ok && node.Address != "" {
		return designated, node.Address, true
	}
	if fsm := c.raftNode.FSM(); fsm != nil {
		if info, ok := fsm.GetNode(designated); ok && info.Address != "" {
			return designated, info.Address, true
		}
	}
	return "", "", false
}

// originStreamPrimaryWatch ends every session the moment this node is no
// longer the designated primary: a former primary must not keep feeding
// standbys that are about to follow someone else.
func (c *Coordinator) originStreamPrimaryWatch(ctx context.Context, st *originStream) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if c.designatedPrimaryWriterID() != c.localNode.ID && len(st.server.Sessions()) > 0 {
				st.server.CloseSessions()
			}
		}
	}
}

// trimReady implements the standby trim guard, once per trim pass, with one
// request per (segment, key) to trim. A request is ready when this node's
// FSM has applied at least what the primary had when it published its
// floors, no claim or takeover may need the origin's records, and every
// current hot file holding rows of the request's batches (by the files'
// coverage, see raft.TrimFilesFor) is present here at its size (#1266).
// Files holding other batches do not hold the trim back, so a standby whose
// pulls lag still drops what its files already hold.
func (st *originStream) trimReady(c *Coordinator) originstream.TrimReadiness {
	return func(ctx context.Context, reqs []originstream.TrimRequest) []bool {
		out := make([]bool, len(reqs))
		fsm := c.raftNode.FSM()
		if fsm == nil {
			return out
		}
		// Read before the claims flag: a request this FSM is behind on is
		// refused below, so the flag read after it is at least as new as the
		// registrations the floors count.
		applied := fsm.AppliedDone()
		claims := fsm.StreamClaimsEnabled()
		// An origin with an unfinished claim on one of its instances keeps
		// every record (#1207): a claimant publishes from them, and a vanished
		// file can make batches below the origin's floors claimable again.
		// So does an origin this node does not hear from: it may be dead, and
		// its records claimed once the grace ends; trimming them between the
		// claimant reading what it holds and the claim applying would claim
		// batches it no longer has.
		held := map[string]bool{}
		if claims {
			for _, st := range fsm.TakeoverInstanceStates() {
				if st.Claimed && !st.ClaimDone && !st.ClaimRelease {
					held[st.Origin] = true
				}
			}
		}
		healthy := map[string]bool{}
		var qs []raft.TrimQuery
		var qi []int // request index of each query
		for i, r := range reqs {
			// First: every check below reads this node's FSM, which must
			// include every registration the floors count.
			if applied < r.AppliedIndex {
				continue
			}
			if !claims {
				// Nothing claims from the copy. An instance that streamed
				// before the flag turned on announces afterwards as not a
				// fresh start, so it is untracked and never claimed either.
				out[i] = true
				continue
			}
			ok, seen := healthy[r.Origin]
			if !seen {
				ok = c.originHealthy(r.Origin)
				healthy[r.Origin] = ok
			}
			if held[r.Origin] || !ok {
				continue
			}
			qs = append(qs, raft.TrimQuery{Instance: r.Instance, Key: r.Key, CSeqs: r.CSeqs})
			qi = append(qi, i)
		}
		if len(qs) == 0 {
			return out
		}
		tracked, files := fsm.TrimFilesFor(qs)
		here := st.filesHere(ctx, c, &st.trimPresent, files)
		for j, i := range qi {
			// Untracked, removed or unknown: never claimed, so its batches
			// need no file here.
			out[i] = !tracked[j] || here[j]
		}
		return out
	}
}

// instanceFilesLocal is the claimant's check before it drops a store copy it
// no longer needs: every current hot file holding rows of the instance's
// batches of those keys (by coverage, as for a trim) is here at its size.
func (st *originStream) instanceFilesLocal(c *Coordinator) func(ctx context.Context, instance uint64, keys []string) bool {
	return func(ctx context.Context, instance uint64, keys []string) bool {
		fsm := c.raftNode.FSM()
		if fsm == nil {
			return false
		}
		qs := make([]raft.TrimQuery, len(keys))
		for i, k := range keys {
			qs[i] = raft.TrimQuery{Instance: instance, Key: k, CSeqs: [][2]uint64{{0, math.MaxUint64}}}
		}
		tracked, files := fsm.TrimFilesFor(qs)
		for j, ok := range st.filesHere(ctx, c, &st.dropPresent, files) {
			if tracked[j] && !ok {
				return false
			}
		}
		return true
	}
}

// filesHere reports, per list, whether every file of it is present here at
// its size. A file verified present stays verified while it is current at
// the same size (files are immutable); *cache keeps only the paths of this
// call.
func (st *originStream) filesHere(ctx context.Context, c *Coordinator, cache *map[string]int64, lists [][]raft.TrimFile) []bool {
	st.presentMu.Lock()
	prev := *cache
	st.presentMu.Unlock()
	present := map[string]int64{}
	missing := map[string]bool{}
	local := func(f raft.TrimFile) bool {
		if size, ok := present[f.Path]; ok {
			return size == f.SizeBytes
		}
		if missing[f.Path] {
			return false
		}
		if size, ok := prev[f.Path]; ok && size == f.SizeBytes {
			present[f.Path] = size
			return true
		}
		got, err := c.storage.StatFile(ctx, f.Path)
		if err != nil || got != f.SizeBytes {
			missing[f.Path] = true
			return false
		}
		present[f.Path] = got
		return true
	}
	out := make([]bool, len(lists))
	for i, fs := range lists {
		out[i] = true
		for _, f := range fs {
			if !local(f) {
				out[i] = false
				break
			}
		}
	}
	st.presentMu.Lock()
	*cache = present
	st.presentMu.Unlock()
	return out
}

// handleOriginStreamHello answers a standby writer opening the origin
// stream. Every refusal returns the same generic text; the reason is logged.
func (c *Coordinator) handleOriginStreamHello(conn net.Conn, hello *protocol.OriginStreamHello) bool {
	refuseAt := func(warn bool, why string) bool {
		ev := c.logger.Debug()
		if warn {
			ev = c.logger.Warn()
		}
		ev.Str("peer", conn.RemoteAddr().String()).Str("node_id", hello.NodeID).Msg("Origin stream refused: " + why)
		c.sendOriginStreamAck(conn, hello.Nonce, "", "refused")
		return false
	}
	// Authentication failures are worth a Warn; the expected refusals (not
	// the primary, stream off) repeat at every standby retry and are Debug.
	refuse := func(why string) bool { return refuseAt(true, why) }
	routine := func(why string) bool { return refuseAt(false, why) }
	c.mu.RLock()
	st := c.originStream
	c.mu.RUnlock()
	switch {
	case c.cfg.SharedSecret == "":
		return refuse("cluster.shared_secret not configured")
	case hello.HMAC == "" || hello.Nonce == "" || hello.NodeID == "" || hello.Timestamp == 0:
		return refuse("missing auth fields")
	case hello.NodeID == c.localNode.ID:
		return refuse("self-addressed")
	case hello.ClusterName != c.cfg.ClusterName:
		return refuse("cluster name mismatch")
	}
	if err := security.ValidateOriginStreamHelloHMAC(c.cfg.SharedSecret, hello.Version, hello.Nonce, hello.NodeID, hello.ClusterName, hello.Timestamp, hello.HMAC, security.HMACTimestampTolerance); err != nil {
		return refuse("HMAC validation failed")
	}
	if !c.nonceCache.Track(hello.NodeID, hello.Nonce) {
		return refuse("replay (nonce already seen)")
	}
	if hello.Version != originstream.ProtocolVersion {
		return refuse(fmt.Sprintf("origin stream protocol version %d, this node speaks %d", hello.Version, originstream.ProtocolVersion))
	}
	if st == nil {
		return routine("origin stream not enabled on this node (cluster.wal_coverage)")
	}
	if c.designatedPrimaryWriterID() != c.localNode.ID {
		return routine("this node is not the designated primary writer")
	}
	if info, ok := c.raftNode.FSM().GetNode(hello.NodeID); !ok || info.Role != string(RoleWriter) {
		return refuse("the peer is not a writer in the cluster")
	}
	key, err := security.DeriveOriginStreamSessionKey(c.cfg.SharedSecret, hello.Nonce)
	if err != nil {
		return refuse("session key derivation failed")
	}
	if !c.sendOriginStreamAck(conn, hello.Nonce, fmt.Sprintf("%016x", st.wal.TrackedInstance()), "") {
		return false
	}
	st.server.Accept(hello.NodeID, conn, key)
	return true
}

func (c *Coordinator) sendOriginStreamAck(conn net.Conn, nonce, instance, errText string) bool {
	ts := time.Now().Unix()
	ack := &protocol.OriginStreamHelloAck{
		OriginNodeID: c.localNode.ID, Instance: instance, Error: errText, Timestamp: ts,
		HMAC: security.ComputeOriginStreamHelloAckHMAC(c.cfg.SharedSecret, nonce, c.localNode.ID, instance, errText, ts),
	}
	return protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgOriginStreamHelloAck, Payload: ack}, 10*time.Second) == nil
}

// StopClaimant stops the stream claimant (#1207), if it runs. Call before
// the ingest buffer and the registration outbox shut down: a replay writes
// through both.
func (c *Coordinator) StopClaimant() {
	if cl := c.claimant.Load(); cl != nil {
		cl.stop()
	}
}

// stopOriginStream stops both sides; part of the coordinator shutdown.
func (c *Coordinator) stopOriginStream() {
	c.mu.Lock()
	st := c.originStream
	c.originStream = nil
	c.mu.Unlock()
	if st == nil {
		return
	}
	st.wal.SetReplicationHook(nil)
	st.cancel()
	st.server.Stop()
	select {
	case <-st.done:
	case <-time.After(5 * time.Second):
	}
	if st.spaceDone != nil {
		<-st.spaceDone // never touches a closed store
	}
	_ = st.store.Close()
}

// standbyStoreAutoShare and standbyStoreMinCap size the default standby store
// cap: a quarter of the WAL volume, at least 1 GiB (#1266).
const (
	standbyStoreAutoShare  = 4
	standbyStoreMinCap     = int64(1) << 30
	standbyStoreMaxAutoCap = int64(32) << 30
)

// standbyStoreCap resolves cluster.standby_store_max_bytes: > 0 as given,
// < 0 no cap, 0 (the default) a quarter of the filesystem holding dir (at
// least 1 GiB). On a volume that does not enforce its size (k3s local-path,
// hostPath, docker named volumes) the filesystem is the host's disk.
func (c *Coordinator) standbyStoreCap(dir string) int64 {
	switch v := c.cfg.StandbyStoreMaxBytes; {
	case v > 0:
		c.logger.Info().Int64("max_bytes", v).Msg("Standby store cap set by cluster.standby_store_max_bytes")
		return v
	case v < 0:
		c.logger.Warn().Msg("cluster.standby_store_max_bytes is negative: the standby store has no cap")
		return 0
	}
	total, _, err := diskSpace(dir)
	if err != nil {
		c.logger.Warn().Err(err).Int64("max_bytes", standbyStoreMinCap).Msg("Could not size the standby store cap from its volume; using 1 GiB")
		return standbyStoreMinCap
	}
	// Capped at standbyStoreMaxAutoCap: a store at its cap is read whole at
	// startup, and on a host disk a quarter can be hundreds of GiB.
	capBytes := min(max(int64(total/standbyStoreAutoShare), standbyStoreMinCap), standbyStoreMaxAutoCap)
	if standbyStoreReserve(total) >= total {
		c.logger.Warn().Uint64("volume_bytes", total).
			Msg("The WAL volume is smaller than the standby store's free-space reserve (2 GiB): with WAL coverage on, this standby keeps no copy of the primary's batches")
	}
	c.logger.Info().Uint64("volume_bytes", total).Int64("max_bytes", capBytes).
		Msg("Standby store cap: a quarter of its volume (cluster.standby_store_max_bytes = 0)")
	return capBytes
}

// Free-space reserve of the standby store's volume (#1266): below the
// reserve the store stops keeping batches, so the WAL and the Parquet files
// it shares the volume with keep their room; it resumes once a further
// standbyStoreReserveResume is free. The store must give way BEFORE the WAL's
// own disk gate (#676: off at its 512 MiB reserve plus two WAL files, ~712
// MiB at defaults), or a standby fills the volume and is under disk pressure
// the moment it is promoted: the 2 GiB floor keeps the reserve above it.
const (
	standbyStoreSpaceEvery    = 5 * time.Second
	standbyStoreReserveMin    = uint64(2) << 30
	standbyStoreReserveShare  = 10 // percent of the volume
	standbyStoreReserveResume = uint64(1) << 30
)

// diskSpace is originstream.DiskSpace; a var for tests.
var diskSpace = originstream.DiskSpace

// lowSpaceNext is the reserve's state after a reading, with hysteresis.
func lowSpaceNext(low bool, total, avail uint64) bool {
	reserve := standbyStoreReserve(total)
	switch {
	case !low && avail < reserve:
		return true
	case low && avail >= reserve+standbyStoreReserveResume:
		return false
	}
	return low
}

func standbyStoreReserve(total uint64) uint64 {
	return max(total/100*standbyStoreReserveShare, standbyStoreReserveMin)
}

// watchStandbyStoreSpace checks the store's volume on its own tick.
func (c *Coordinator) watchStandbyStoreSpace(ctx context.Context, store *originstream.Store, root string) {
	t := time.NewTicker(standbyStoreSpaceEvery)
	defer t.Stop()
	low, warned := false, false
	for {
		total, avail, err := diskSpace(root)
		switch {
		case err == nil && total > 0:
			low = lowSpaceNext(low, total, avail)
			store.SetLowSpace(low, avail, standbyStoreReserve(total))
		case !warned:
			warned = true
			c.logger.Warn().Err(err).Msg("Cannot read the standby store volume's free space; its free-space reserve is not enforced")
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
