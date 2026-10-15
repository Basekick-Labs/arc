package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/security"
)

// Peer flush before a DELETE in shared storage (#1265).
//
// In shared storage every writer takes writes, and a DELETE runs on the
// primary only. Rows another node acknowledged and still buffers are in no
// file the DELETE scans; they reach storage after it and survive. So before
// it scans, the primary asks every other node to flush the rows of the
// DELETE's scope it buffers, and waits for the files they flushed to be in
// the cluster manifest. A node that does not answer, or whose files do not
// register in time, is named, and the DELETE is refused unless the caller
// allowed it.
//
// Pattern 1 does not need this: only the primary takes writes there, and
// with stream takeover a DELETE is recorded in the cluster (#1207 d1).

// DeleteScopeFlusher is a node's ingest buffer as a peer flush uses it.
type DeleteScopeFlusher interface {
	FlushScopeAndWait(ctx context.Context, database, measurement string) (map[string]uint64, error)
	HasFlushFailure() bool
}

// Tunables, variables so tests can shrink them.
var (
	// peerFlushTimeout bounds one node's flush and answer. Above the
	// buffer's own flush budget (30s by default), so a flush that takes
	// that long answers as failed rather than as unreachable.
	peerFlushTimeout = 40 * time.Second
	// peerRegisterWait bounds the wait for the files nodes flushed to be in
	// the manifest.
	peerRegisterWait = 30 * time.Second
	// peerRegisterPoll spaces the manifest checks of that wait.
	peerRegisterPoll = 200 * time.Millisecond
)

// SetDeleteScopeFlusher installs this node's ingest buffer, which a peer
// flush request flushes (#1265). Without one the node answers that it has
// nothing buffered.
func (c *Coordinator) SetDeleteScopeFlusher(f DeleteScopeFlusher) {
	if f == nil {
		c.scopeFlusher.Store(nil)
		return
	}
	c.scopeFlusher.Store(&f)
}

// PeerFlushActive reports whether a DELETE run here must flush the other
// nodes first: shared storage with Raft, where every writer takes writes.
func (c *Coordinator) PeerFlushActive() bool {
	return c != nil && c.cfg.SharedStorageMode && c.raftNode != nil
}

// validFlushScope reports a database and measurement a flush request may
// name: one storage path segment each, the measurement optional.
func validFlushScope(database, measurement string) bool {
	ok := func(s string) bool {
		return s != "." && s != ".." && !strings.ContainsAny(s, "/\\\x00")
	}
	return database != "" && ok(database) && (measurement == "" || ok(measurement))
}

// handleFlushScopeRequest flushes this node's buffered rows of the scope and
// names its files of the scope still waiting to register. Every refusal
// answers the same status; the reason is logged.
func (c *Coordinator) handleFlushScopeRequest(conn net.Conn, q *protocol.FlushScopeRequest) {
	refuse := func(why string) {
		c.logger.Warn().Str("peer", conn.RemoteAddr().String()).Str("node_id", q.NodeID).
			Msg("Flush scope request refused: " + why)
		c.sendFlushScopeReply(conn, q, protocol.FlushStatusRefused, nil)
	}
	switch {
	case c.cfg.SharedSecret == "":
		refuse("cluster.shared_secret not configured")
		return
	case q.HMAC == "" || q.Nonce == "" || q.NodeID == "" || q.Timestamp == 0:
		refuse("missing auth fields")
		return
	case q.ClusterName != c.cfg.ClusterName:
		refuse("cluster name mismatch")
		return
	case !validFlushScope(q.Database, q.Measurement):
		refuse("invalid database or measurement")
		return
	}
	if err := security.ValidateFlushScopeRequestHMAC(c.cfg.SharedSecret, q.Nonce, q.NodeID, q.ClusterName, q.Database, q.Measurement, q.Timestamp, q.HMAC, security.HMACTimestampTolerance); err != nil {
		refuse("HMAC validation failed")
		return
	}
	if c.nonceCache == nil || !c.nonceCache.Track(q.NodeID, q.Nonce) {
		refuse("replay (nonce already seen)")
		return
	}
	// Only the primary runs a DELETE, and it is the Raft leader in shared
	// storage: any other node asking would only make this one write small
	// files.
	if c.raftNode == nil {
		refuse("raft not initialized")
		return
	}
	if leader := c.raftNode.LeaderID(); leader == "" || leader != q.NodeID {
		refuse("the peer is not the Raft leader")
		return
	}
	status, paths := c.localFlushAnswer(q.Database, q.Measurement)
	c.sendFlushScopeReply(conn, q, status, paths)
}

// localFlushAnswer flushes this node's buffered rows of the scope and lists
// its files of the scope still waiting to register.
func (c *Coordinator) localFlushAnswer(database, measurement string) (string, []string) {
	if fp := c.scopeFlusher.Load(); fp != nil {
		f := *fp
		if f.HasFlushFailure() {
			c.logger.Warn().Str("database", database).Msg("Flush for a DELETE refused: an earlier flush failed and its rows wait for WAL recovery")
			return protocol.FlushStatusFailed, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), peerFlushTimeout-5*time.Second)
		_, err := f.FlushScopeAndWait(ctx, database, measurement)
		cancel()
		if err != nil {
			c.logger.Warn().Err(err).Str("database", database).Str("measurement", measurement).Msg("Flush for a DELETE failed")
			return protocol.FlushStatusFailed, nil
		}
	}
	fn := c.pendingRegs.Load()
	if fn == nil {
		if c.scopeFlusher.Load() == nil {
			return protocol.FlushStatusOK, nil
		}
		return protocol.FlushStatusUntracked, nil
	}
	all, tooMany := (*fn)(pendingReplyBudget)
	if tooMany {
		return protocol.FlushStatusTooMany, nil
	}
	return protocol.FlushStatusOK, pathsInScope(all, database, measurement)
}

// pathsInScope returns the paths under database/ (database/measurement/).
func pathsInScope(paths []string, database, measurement string) []string {
	prefix := database + "/"
	if measurement != "" {
		prefix += measurement + "/"
	}
	var out []string
	for _, p := range paths {
		if strings.HasPrefix(p, prefix) {
			out = append(out, p)
		}
	}
	return out
}

func (c *Coordinator) sendFlushScopeReply(conn net.Conn, q *protocol.FlushScopeRequest, status string, paths []string) {
	ts := time.Now().Unix()
	reply := &protocol.FlushScopeReply{
		NodeID: c.localNode.ID, Status: status, Paths: paths, Timestamp: ts,
		HMAC: security.ComputeFlushScopeReplyHMAC(c.cfg.SharedSecret, q.Nonce, c.localNode.ID, c.cfg.ClusterName, q.Database, q.Measurement, status, paths, ts),
	}
	if err := protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgFlushScopeReply, Payload: reply}, 10*time.Second); err != nil {
		c.logger.Warn().Err(err).Str("peer", conn.RemoteAddr().String()).Msg("Could not send the flush scope reply")
	}
}

// errNoFlushAnswer is a node that did not answer a flush request usefully.
var errNoFlushAnswer = errors.New("no answer")

// requestPeerFlush asks the node at addr to flush the scope, and returns the
// files of the scope it has waiting to register.
func (c *Coordinator) requestPeerFlush(ctx context.Context, nodeID, addr, database, measurement string) ([]string, error) {
	if c.cfg.SharedSecret == "" {
		return nil, errors.New("cluster.shared_secret not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, peerFlushTimeout)
	defer cancel()
	conn, err := security.DialContext(ctx, "tcp", addr, peerFlushTimeout, c.tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	nonce, err := security.GenerateNonce()
	if err != nil {
		return nil, err
	}
	ts := time.Now().Unix()
	q := &protocol.FlushScopeRequest{
		NodeID: c.localNode.ID, ClusterName: c.cfg.ClusterName, Database: database, Measurement: measurement,
		Nonce: nonce, Timestamp: ts,
	}
	q.HMAC = security.ComputeFlushScopeRequestHMAC(c.cfg.SharedSecret, nonce, q.NodeID, q.ClusterName, database, measurement, ts)
	if err := protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgFlushScopeRequest, Payload: q}, peerFlushTimeout); err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}
	msg, err := protocol.ReceiveMessage(conn, peerFlushTimeout)
	if err != nil {
		return nil, fmt.Errorf("%w (a node before this release closes the connection): %v", errNoFlushAnswer, err)
	}
	reply, ok := msg.Payload.(*protocol.FlushScopeReply)
	if !ok || msg.Type != protocol.MsgFlushScopeReply {
		return nil, fmt.Errorf("%w: unexpected reply type %s", errNoFlushAnswer, msg.Type)
	}
	if reply.NodeID != nodeID {
		return nil, fmt.Errorf("%w: answered as %q", errNoFlushAnswer, reply.NodeID)
	}
	if err := security.ValidateFlushScopeReplyHMAC(c.cfg.SharedSecret, nonce, reply.NodeID, c.cfg.ClusterName, database, measurement, reply.Status, reply.Paths, reply.Timestamp, reply.HMAC, security.HMACTimestampTolerance); err != nil {
		return nil, fmt.Errorf("%w: %v", errNoFlushAnswer, err)
	}
	switch reply.Status {
	case protocol.FlushStatusOK:
		return pathsInScope(reply.Paths, database, measurement), nil
	case protocol.FlushStatusUntracked:
		// Its files are not named: a DELETE refuses to rewrite an
		// unregistered file that young (delete_cas.go), so they cannot be
		// lost to it, and any older one is in the scan.
		return nil, nil
	case protocol.FlushStatusFailed:
		return nil, fmt.Errorf("%w: its flush failed, or an earlier one did and its rows wait for WAL recovery", errNoFlushAnswer)
	case protocol.FlushStatusTooMany:
		return nil, fmt.Errorf("%w: more files waiting to register than one reply carries", errNoFlushAnswer)
	default:
		return nil, fmt.Errorf("%w: status %q", errNoFlushAnswer, reply.Status)
	}
}

// FlushPeersForDelete flushes the rows of the scope every other node of the
// cluster buffers, and waits for the files they flushed to be in this node's
// manifest (#1265). unflushed names each node that did not answer, or whose
// files did not register in time, with why and how to clear it: the rows it
// holds are not in any file a scan now sees. Nothing is asked outside shared
// storage. err is a manifest this node could not catch up with.
func (c *Coordinator) FlushPeersForDelete(ctx context.Context, database, measurement string) (unflushed []string, err error) {
	if !c.PeerFlushActive() {
		return nil, nil
	}
	fsm := c.manifestFSM()
	if fsm == nil {
		return nil, errors.New("no cluster manifest on this node")
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	waitFor := map[string]string{} // path -> node
	fail := func(line string) {
		mu.Lock()
		unflushed = append(unflushed, line)
		mu.Unlock()
	}
	for _, n := range fsm.GetAllNodes() {
		// Every node, not only writers: a reader or compactor can buffer
		// rows too (MQTT, imports), and one with nothing buffered answers
		// at once.
		if c.localNode != nil && n.ID == c.localNode.ID {
			continue
		}
		addr := c.leaderCoordinatorAddress(n.ID)
		if addr == "" {
			fail(c.unflushedLine(n.ID, "no coordinator address"))
			continue
		}
		wg.Add(1)
		go func(id, addr string) {
			defer wg.Done()
			paths, err := c.requestPeerFlush(ctx, id, addr, database, measurement)
			if err != nil {
				fail(c.unflushedLine(id, err.Error()))
				return
			}
			mu.Lock()
			for _, p := range paths {
				waitFor[p] = id
			}
			mu.Unlock()
		}(n.ID, addr)
	}
	wg.Wait()
	late, err := c.waitPeerRegistrations(ctx, waitFor)
	if err != nil {
		return nil, err
	}
	for id, paths := range late {
		fail(c.unflushedLine(id, fmt.Sprintf("%d flushed file(s) not registered in the cluster manifest after %s (first: %s)", len(paths), peerRegisterWait, paths[0])))
	}
	sort.Strings(unflushed)
	return unflushed, nil
}

// waitPeerRegistrations waits until every path is in this node's manifest,
// or gone from storage (the node withdrew it, or settled it as lost). late
// is what is still missing at peerRegisterWait, per node, sorted.
func (c *Coordinator) waitPeerRegistrations(ctx context.Context, waitFor map[string]string) (map[string][]string, error) {
	if len(waitFor) == 0 {
		return nil, nil
	}
	deadline := time.Now().Add(peerRegisterWait)
	for {
		if err := c.SyncManifest(ctx); err != nil {
			return nil, fmt.Errorf("could not catch up with the cluster manifest: %w", err)
		}
		for p := range waitFor {
			if _, ok := c.GetFileEntry(p); ok {
				delete(waitFor, p)
				continue
			}
			if c.storage != nil {
				if size, err := c.storage.StatFile(ctx, p); err == nil && size < 0 {
					delete(waitFor, p)
				}
			}
		}
		if len(waitFor) == 0 {
			return nil, nil
		}
		if time.Now().After(deadline) {
			late := map[string][]string{}
			for p, id := range waitFor {
				late[id] = append(late[id], p)
			}
			for id := range late {
				sort.Strings(late[id])
			}
			return late, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(peerRegisterPoll):
		}
	}
}

// unflushedLine names a node whose buffered rows a DELETE could not see,
// with its health and how to clear it.
func (c *Coordinator) unflushedLine(nodeID, why string) string {
	state := "unknown"
	if c.registry != nil {
		if n, ok := c.registry.Get(nodeID); ok {
			state = string(n.GetState())
		}
	}
	return fmt.Sprintf("node %s (state %s): %s; bring it back, remove it from the cluster if it is gone for good, or set allow_unreachable=true to leave the rows it holds", nodeID, state, why)
}
