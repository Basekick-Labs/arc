package cluster

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/originstream"
	"github.com/basekick-labs/arc/internal/cluster/protocol"
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
	present   map[string]int64 // paths verified present locally, at their size
}

// StartOriginStream starts both sides. Call after Start, on a writer whose
// buffer has coverage enabled. A no-op elsewhere.
func (c *Coordinator) StartOriginStream(walWriter *wal.Writer, buffer *ingest.ArrowBuffer) error {
	if !c.cfg.WALCoverage || c.cfg.SharedStorageMode || c.raftNode == nil || walWriter == nil || buffer == nil {
		return nil
	}
	if c.localNode.Role != RoleWriter {
		return nil
	}
	if _, _, ok := buffer.RegistrationProgress(); !ok {
		return nil // coverage did not come up on this buffer
	}
	if c.cfg.SharedSecret == "" {
		return fmt.Errorf("the origin stream needs cluster.shared_secret")
	}
	if c.storage == nil {
		return fmt.Errorf("the origin stream needs the storage backend")
	}
	root := filepath.Join(filepath.Dir(walWriter.CurrentFile()), "received")
	store, err := originstream.OpenStore(root, c.cfg.StandbyStoreMaxBytes, c.logger.With().Str("component", "standby-store").Logger())
	if err != nil {
		return err
	}
	if c.cfg.StandbyStoreMaxBytes <= 0 {
		c.logger.Warn().Msg("cluster.standby_store_max_bytes is not positive: the standby store has no cap")
	}
	if c.cfg.StreamTakeover {
		policy := &standbyEvictPolicy{c: c}
		store.SetEvictable(policy.tier, policy.active)
	}
	fsm := c.raftNode.FSM()
	st := &originStream{wal: walWriter, store: store, done: make(chan struct{}), present: map[string]int64{}}
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

// trimReady implements the standby trim guard, once per trim pass for every
// key with segments to trim: this node's FSM has applied at least what the
// primary had when it published its floors, and every current hot file of
// the key's measurement is present here at its size. Compaction and DELETE
// rewrites replace files, and their outputs are current, so a batch whose
// file was rewritten is trimmed only once the rewrite is here too.
//
// Each database's files are listed once per pass. A file verified present
// stays verified while it is in the manifest at the same size (files are
// immutable); the cache keeps only paths still current.
func (st *originStream) trimReady(c *Coordinator) originstream.TrimReadiness {
	return func(ctx context.Context, keys []originstream.TrimKey) map[originstream.TrimKey]bool {
		out := make(map[originstream.TrimKey]bool, len(keys))
		fsm := c.raftNode.FSM()
		if fsm == nil {
			return out
		}
		applied := fsm.AppliedDone()
		type dbm struct{ db, measurement string }
		wanted := map[dbm][]originstream.TrimKey{}
		dbs := map[string]bool{}
		for _, k := range keys {
			db, m, ok := strings.Cut(k.Key, "/")
			if !ok || applied < k.AppliedIndex {
				continue
			}
			wanted[dbm{db, m}] = append(wanted[dbm{db, m}], k)
			dbs[db] = true
		}
		present := map[string]int64{}
		st.presentMu.Lock()
		prev := st.present
		st.presentMu.Unlock()
		for db := range dbs {
			byMeasurement := map[string]bool{} // measurement -> every hot file present
			for _, f := range fsm.GetFilesByDatabase(db) {
				if f.Tier != "" && f.Tier != "hot" {
					continue // in shared object storage, not on any node's disk
				}
				m := f.Measurement
				if _, seen := byMeasurement[m]; !seen {
					byMeasurement[m] = true
				}
				if _, care := wanted[dbm{db, m}]; !care || !byMeasurement[m] {
					continue
				}
				if size, ok := prev[f.Path]; ok && size == f.SizeBytes {
					present[f.Path] = size
					continue
				}
				got, err := c.storage.StatFile(ctx, f.Path)
				if err != nil || got != f.SizeBytes {
					byMeasurement[m] = false
					continue
				}
				present[f.Path] = got
			}
			for key, ks := range wanted {
				if key.db != db {
					continue
				}
				ok, seen := byMeasurement[key.measurement]
				if !seen {
					ok = true // no current hot file: deleted or tiered, nothing to hold
				}
				for _, k := range ks {
					out[k] = ok
				}
			}
		}
		st.presentMu.Lock()
		st.present = present
		st.presentMu.Unlock()
		return out
	}
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
	_ = st.store.Close()
}
