package cluster

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/storage"
)

// Pending registrations on a shared object store (#1214).
//
// A writer's flushed file is an unregistered object until its registration
// reaches the cluster manifest. On a shared object store the orphan-storage
// sweep runs on the active compactor, which has no registration outbox, so a
// writer cut off from the leader for longer than the grace window, or one
// that left the cluster with files still pending, would lose them to the
// sweep, and its WAL was checkpointed after the flush. Two sources tell the
// sweep what is pending elsewhere:
//
//   - every node that can ingest answers a pending-registrations query from
//     its outbox, authenticated and bound to the query's nonce; a node that
//     does not answer holds the storage sweep for the run;
//   - every writer publishes its pending set to the store, under
//     PendingRegistrationsRoot, when it stops, when it abandons the cluster,
//     and while files wait longer than outboxAlarmAge; it deletes the object
//     once nothing is pending. That covers a writer that left the cluster
//     (no longer asked) and one that can reach the bucket but not the
//     compactor. A writer whose pending set is not published does not
//     leave the cluster on shutdown (CoverForLeave): it stays a node that
//     is asked, and holds the sweep until it returns or is removed.
//
// The object is keyed by a publisher ID persisted next to the journal, not by
// the node ID: without cluster.node_id that is hostname-PID, a new one every
// start on a VM, and a restarted writer must overwrite or delete the list its
// previous start left.

// PendingRegistrationsRoot is the reserved root (storage.IsReservedRootDir)
// the published pending lists live under, one object per node.
const PendingRegistrationsRoot = "_registration_pending"

// pendingRegistrationsDocVersion is written on every published list.
const pendingRegistrationsDocVersion = 1

// pendingReplyBudget bounds the path bytes one reply carries, under the
// codec's 1 MiB frame with room for the JSON around them.
const pendingReplyBudget = 768 << 10

// PublisherIDFile holds the outbox's publisher ID, next to its journal.
const PublisherIDFile = "registration-publisher-id"

// maxPublishedListBytes caps a published list the sweep reads whole. A
// larger one holds the sweep, named, instead of being read.
var maxPublishedListBytes int64 = 16 << 20

// stopPublishAttempts is how many times Stop tries to publish.
const stopPublishAttempts = 3

// Tunables, variables so tests can shrink them.
var (
	// pendingQueryTimeout bounds one node's answer to a query.
	pendingQueryTimeout = 5 * time.Second
	// outboxPublishEvery rate-limits rewriting the published pending list
	// while files wait. Deleting it once nothing is pending is not limited.
	outboxPublishEvery = 30 * time.Second
	// outboxPublishTimeout bounds one write or delete of the list.
	outboxPublishTimeout = 10 * time.Second
	// outboxPublishRetry spaces Stop's publish attempts.
	outboxPublishRetry = time.Second
)

// PendingRegistrationsDoc is a published pending list.
type PendingRegistrationsDoc struct {
	Version     int                       `json:"v"`
	NodeID      string                    `json:"node_id"`
	ClusterName string                    `json:"cluster_name"`
	WrittenAt   time.Time                 `json:"written_at"`
	Files       []PendingRegistrationFile `json:"files"`
}

// PendingRegistrationFile is one file of a published pending list.
type PendingRegistrationFile struct {
	Path string `json:"path"`
	// FlushedAt is zero for a record from before the flush time was
	// journaled (#1206).
	FlushedAt time.Time `json:"flushed_at,omitempty"`
	// State is "pending" (waiting to register) or "parked" (refused by the
	// stream takeover rules and kept until it is registered elsewhere).
	State string `json:"state"`
}

// PendingStore is what publishing the pending list needs from storage.
type PendingStore interface {
	Write(ctx context.Context, path string, data []byte) error
	Delete(ctx context.Context, path string) error
}

// IsSharedObjectStore reports whether b is an object store every node reads
// and writes (S3, Azure), as opposed to a node's own disk.
func IsSharedObjectStore(b storage.Backend) bool {
	if b == nil {
		return false
	}
	_, object := b.(storage.ObjectCopier)
	return object && b.Type() != "local"
}

// PendingRegistrationsKey is the object a publisher's pending list is
// published at.
func PendingRegistrationsKey(publisherID string) string {
	return PendingRegistrationsRoot + "/" + publisherID + ".json"
}

// loadPublisherID returns the publisher ID persisted in dir, creating it once.
func loadPublisherID(dir string) (string, error) {
	path := filepath.Join(dir, PublisherIDFile)
	if data, err := os.ReadFile(path); err == nil {
		if id := strings.TrimSpace(string(data)); id != "" && storage.ValidateKey(PendingRegistrationsKey(id)) == nil && !strings.Contains(id, "/") {
			return id, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(raw[:])
	tmp, err := os.CreateTemp(dir, PublisherIDFile+".tmp-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(id + "\n"); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := fsyncFile(tmp); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return "", err
	}
	return id, syncDir(dir)
}

// pendingPublisher publishes an outbox's pending set (SetPendingPublisher).
type pendingPublisher struct {
	store       PendingStore
	nodeID      string
	key         string
	clusterName string

	mu        sync.Mutex // serialises publishes
	lastCheck time.Time
	lastSum   [sha256.Size]byte
	// published: an object may exist at key. True at start, so a list a
	// previous start left is deleted once nothing is pending.
	published bool
}

// SetPendingPublisher publishes this outbox's pending set to store (#1214),
// keyed by the publisher ID persisted in the journal directory; nodeID goes
// in the document, for operators. Only on a shared object store; call before
// HandOff or Abandon. Without a publisher ID the list is not published, and
// CoverForLeave reports a pending set as uncovered.
func (r *RegistrationOutbox) SetPendingPublisher(store PendingStore, nodeID string) {
	p := &pendingPublisher{store: store, nodeID: nodeID, clusterName: r.clusterName, published: true}
	id, err := loadPublisherID(filepath.Dir(r.journalPath))
	if err != nil {
		r.logger.Error().Err(err).Str("dir", filepath.Dir(r.journalPath)).
			Msg("Could not load or create the pending-registration publisher ID; this writer's pending files are not published, and it does not leave the cluster at shutdown while any are pending")
		p.store = nil
	} else {
		p.key = PendingRegistrationsKey(id)
	}
	r.mu.Lock()
	r.publisher = p
	r.mu.Unlock()
}

// pendingSnapshot returns every pending and parked file, sorted by path.
func (r *RegistrationOutbox) pendingSnapshot() []PendingRegistrationFile {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]PendingRegistrationFile, 0, r.live+len(r.parked))
	for p, e := range r.byPath {
		if !e.done {
			out = append(out, PendingRegistrationFile{Path: p, FlushedAt: e.rec.FlushedAt, State: "pending"})
		}
	}
	for p, pe := range r.parked {
		if e, ok := r.byPath[p]; ok && !e.done {
			continue
		}
		out = append(out, PendingRegistrationFile{Path: p, FlushedAt: pe.rec.Rec.FlushedAt, State: "parked"})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// maybePublish runs from the drain loop: it deletes the published list once
// nothing is pending, and rewrites it, at most every outboxPublishEvery, when
// a file has waited longer than outboxAlarmAge and the set changed.
func (r *RegistrationOutbox) maybePublish() {
	_ = r.publishPending(false)
}

// pendingSum fingerprints a pending set as published.
func pendingSum(files []PendingRegistrationFile) [sha256.Size]byte {
	h := sha256.New()
	for _, f := range files {
		h.Write([]byte(f.Path))
		h.Write([]byte{0})
		h.Write([]byte(f.State))
		h.Write([]byte{0})
	}
	var sum [sha256.Size]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

// publishPending publishes the pending set, or deletes the published list when
// it is empty. force (Stop, Abandon, a file journaled after Stop) publishes
// whatever the age of the files and whenever the last publish was. The error
// is the failed write; a skipped or unchanged publish is nil.
func (r *RegistrationOutbox) publishPending(force bool) error {
	r.mu.Lock()
	p := r.publisher
	r.mu.Unlock()
	if p == nil || p.store == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	r.mu.Lock()
	empty := r.live == 0 && len(r.parked) == 0
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), outboxPublishTimeout)
	defer cancel()
	if empty {
		if !p.published {
			return nil
		}
		if err := p.store.Delete(ctx, p.key); err != nil && !storage.IsNotFound(err) {
			r.logger.Warn().Err(err).Str("key", p.key).Msg("Could not delete the published pending-registration list; retried later")
			return nil // a stale list only keeps registered files from the sweep
		}
		p.published = false
		p.lastSum = [sha256.Size]byte{}
		return nil
	}
	// The drain loop calls this after every attempt: the snapshot is taken
	// at most every outboxPublishEvery.
	if !force && time.Since(p.lastCheck) < outboxPublishEvery {
		return nil
	}
	p.lastCheck = time.Now()
	files := r.pendingSnapshot()
	if !force {
		old := false
		for _, f := range files {
			if f.FlushedAt.IsZero() || time.Since(f.FlushedAt) >= outboxAlarmAge {
				old = true
				break
			}
		}
		if !old {
			return nil
		}
	}
	sum := pendingSum(files)
	if p.published && sum == p.lastSum {
		return nil // unchanged since the last publish
	}
	doc := PendingRegistrationsDoc{
		Version: pendingRegistrationsDocVersion, NodeID: p.nodeID, ClusterName: p.clusterName,
		WrittenAt: time.Now().UTC(), Files: files,
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	if err := p.store.Write(ctx, p.key, data); err != nil {
		r.logger.Warn().Err(err).Str("key", p.key).Int("files", len(files)).
			Msg("Could not publish the pending-registration list; the orphan sweep learns of these files only by asking this writer")
		return err
	}
	p.published, p.lastSum = true, sum
	return nil
}

// publishForStop publishes with up to stopPublishAttempts tries.
func (r *RegistrationOutbox) publishForStop() {
	for i := 0; i < stopPublishAttempts; i++ {
		if i > 0 {
			time.Sleep(outboxPublishRetry)
		}
		if r.publishPending(true) == nil {
			return
		}
	}
}

// CoverForLeave reports whether this writer may leave the cluster at
// shutdown (#1214): nothing is pending, or what is pending is the set last
// published. A node that left is no longer asked what it has pending, so an
// unpublished set would be left with no cover against the orphan sweep. It
// publishes once more first, for a file journaled since. True off a shared
// object store, where nothing is published.
func (r *RegistrationOutbox) CoverForLeave() bool {
	r.mu.Lock()
	p := r.publisher
	r.mu.Unlock()
	if p == nil {
		return true
	}
	if r.publishPending(true) != nil {
		return false
	}
	files := r.pendingSnapshot()
	if len(files) == 0 {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.store != nil && p.published && pendingSum(files) == p.lastSum
}

// PendingPaths lists every file waiting to register here, parked, or
// confirmed a moment ago (HoldsRegistration's view), whenever it was flushed:
// a flush time is this writer's clock, and the sweep ages its candidates by
// the store's, so filtering by age here could leave out a candidate. tooMany
// reports more path bytes than budget, with no paths.
func (r *RegistrationOutbox) PendingPaths(budget int) (paths []string, tooMany bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := make(map[string]bool)
	size := 0
	add := func(p string) bool {
		if seen[p] {
			return true
		}
		seen[p] = true
		if size += len(p) + 3; size > budget {
			return false
		}
		paths = append(paths, p)
		return true
	}
	for p, e := range r.byPath {
		if !e.done && !add(p) {
			return nil, true
		}
	}
	for p := range r.parked {
		if !add(p) {
			return nil, true
		}
	}
	for p, rc := range r.recent {
		if time.Since(rc.at) < outboxRecentWindow && !add(p) {
			return nil, true
		}
	}
	sort.Strings(paths)
	return paths, false
}

// PendingRegistrationsFunc is a node's own pending answer: the outbox's
// PendingPaths.
type PendingRegistrationsFunc func(budget int) ([]string, bool)

// SetPendingRegistrations sets where this node's answer to a pending
// registrations query comes from (#1214): its registration outbox. A node that
// can ingest without one answers that it does not know.
func (c *Coordinator) SetPendingRegistrations(fn PendingRegistrationsFunc) {
	if fn == nil {
		c.pendingRegs.Store(nil)
		return
	}
	c.pendingRegs.Store(&fn)
}

// localPendingAnswer is this node's answer to a query.
func (c *Coordinator) localPendingAnswer() (string, []string) {
	if fn := c.pendingRegs.Load(); fn != nil {
		paths, tooMany := (*fn)(pendingReplyBudget)
		if tooMany {
			return protocol.PendingStatusTooMany, nil
		}
		return protocol.PendingStatusOK, paths
	}
	if c.localNode != nil && c.localNode.GetCapabilities().CanIngest {
		return protocol.PendingStatusUnknown, nil
	}
	return protocol.PendingStatusOK, nil
}

// handlePendingRegistrationsQuery answers a reconciler's query. Every refusal
// returns the same status; the reason is logged.
func (c *Coordinator) handlePendingRegistrationsQuery(conn net.Conn, q *protocol.PendingRegistrationsQuery) {
	refuse := func(why string) {
		c.logger.Warn().Str("peer", conn.RemoteAddr().String()).Str("node_id", q.NodeID).
			Msg("Pending registrations query refused: " + why)
		c.sendPendingReply(conn, q.Nonce, protocol.PendingStatusRefused, nil)
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
	}
	if err := security.ValidatePendingRegistrationsQueryHMAC(c.cfg.SharedSecret, q.Nonce, q.NodeID, q.ClusterName, q.Timestamp, q.HMAC, security.HMACTimestampTolerance); err != nil {
		refuse("HMAC validation failed")
		return
	}
	if c.nonceCache == nil || !c.nonceCache.Track(q.NodeID, q.Nonce) {
		refuse("replay (nonce already seen)")
		return
	}
	if fsm := c.manifestFSM(); fsm == nil {
		refuse("raft not initialized")
		return
	} else if _, ok := fsm.GetNode(q.NodeID); !ok {
		refuse("the peer is not a node of the cluster")
		return
	}
	status, paths := c.localPendingAnswer()
	c.sendPendingReply(conn, q.Nonce, status, paths)
}

func (c *Coordinator) sendPendingReply(conn net.Conn, nonce, status string, paths []string) {
	ts := time.Now().Unix()
	reply := &protocol.PendingRegistrationsReply{
		NodeID: c.localNode.ID, Status: status, Paths: paths, Timestamp: ts,
		HMAC: security.ComputePendingRegistrationsReplyHMAC(c.cfg.SharedSecret, nonce, c.localNode.ID, c.cfg.ClusterName, status, paths, ts),
	}
	if err := protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgPendingRegistrationsReply, Payload: reply}, 10*time.Second); err != nil {
		c.logger.Warn().Err(err).Str("peer", conn.RemoteAddr().String()).Msg("Could not send the pending registrations reply")
	}
}

// manifestFSM is this node's FSM, nil without Raft.
func (c *Coordinator) manifestFSM() *raft.ClusterFSM {
	if c.raftFSM != nil {
		return c.raftFSM
	}
	if c.raftNode != nil {
		if fsm := c.raftNode.FSM(); fsm != nil {
			return fsm
		}
	}
	return nil
}

// errNoPendingAnswer is a node that did not answer a pending query usefully.
var errNoPendingAnswer = errors.New("no answer")

// queryPendingRegistrations asks the node at addr for its pending files.
func (c *Coordinator) queryPendingRegistrations(ctx context.Context, nodeID, addr string) ([]string, error) {
	if c.cfg.SharedSecret == "" {
		return nil, errors.New("cluster.shared_secret not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, pendingQueryTimeout)
	defer cancel()
	conn, err := security.DialContext(ctx, "tcp", addr, pendingQueryTimeout, c.tlsConfig)
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
	q := &protocol.PendingRegistrationsQuery{
		NodeID: c.localNode.ID, ClusterName: c.cfg.ClusterName, Nonce: nonce, Timestamp: ts,
	}
	q.HMAC = security.ComputePendingRegistrationsQueryHMAC(c.cfg.SharedSecret, nonce, q.NodeID, q.ClusterName, ts)
	if err := protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgPendingRegistrationsQuery, Payload: q}, pendingQueryTimeout); err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}
	// A node before #1214 closes the connection on the unknown type: an
	// error here, so no answer.
	msg, err := protocol.ReceiveMessage(conn, pendingQueryTimeout)
	if err != nil {
		return nil, fmt.Errorf("%w (a node before this release closes the connection): %v", errNoPendingAnswer, err)
	}
	reply, ok := msg.Payload.(*protocol.PendingRegistrationsReply)
	if !ok || msg.Type != protocol.MsgPendingRegistrationsReply {
		return nil, fmt.Errorf("%w: unexpected reply type %s", errNoPendingAnswer, msg.Type)
	}
	if reply.NodeID != nodeID {
		return nil, fmt.Errorf("%w: answered as %q", errNoPendingAnswer, reply.NodeID)
	}
	if err := security.ValidatePendingRegistrationsReplyHMAC(c.cfg.SharedSecret, nonce, reply.NodeID, c.cfg.ClusterName, reply.Status, reply.Paths, reply.Timestamp, reply.HMAC, security.HMACTimestampTolerance); err != nil {
		return nil, fmt.Errorf("%w: %v", errNoPendingAnswer, err)
	}
	switch reply.Status {
	case protocol.PendingStatusOK:
		return reply.Paths, nil
	case protocol.PendingStatusTooMany:
		return nil, fmt.Errorf("%w: more pending files than one reply carries", errNoPendingAnswer)
	case protocol.PendingStatusUnknown:
		return nil, fmt.Errorf("%w: it ingests without a registration journal", errNoPendingAnswer)
	default:
		return nil, fmt.Errorf("%w: status %q", errNoPendingAnswer, reply.Status)
	}
}

// PendingRegistrationsElsewhere is the orphan-storage sweep's view of the
// files pending registration across the cluster
// (#1214): every node of the cluster that can ingest is asked, this one in
// process and the others in parallel, and every pending list published to
// the store is read. held names each source that could not be read; any one
// means the set is not known, and the sweep must not delete.
func (c *Coordinator) PendingRegistrationsElsewhere(ctx context.Context) (paths map[string]struct{}, held []string) {
	paths = make(map[string]struct{})
	fsm := c.manifestFSM()
	if fsm == nil {
		return paths, []string{"no cluster manifest on this node"}
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	addAll := func(ps []string) {
		mu.Lock()
		for _, p := range ps {
			paths[p] = struct{}{}
		}
		mu.Unlock()
	}
	hold := func(why string) {
		mu.Lock()
		held = append(held, why)
		mu.Unlock()
	}
	for _, n := range fsm.GetAllNodes() {
		if !ParseRole(n.Role).GetCapabilities().CanIngest {
			continue
		}
		if c.localNode != nil && n.ID == c.localNode.ID {
			switch status, ps := c.localPendingAnswer(); status {
			case protocol.PendingStatusOK:
				addAll(ps)
			default:
				hold(fmt.Sprintf("node %s (this node): %s", n.ID, status))
			}
			continue
		}
		addr := c.leaderCoordinatorAddress(n.ID)
		if addr == "" {
			hold(fmt.Sprintf("node %s: no coordinator address", n.ID))
			continue
		}
		wg.Add(1)
		go func(id, addr string) {
			defer wg.Done()
			ps, err := c.queryPendingRegistrations(ctx, id, addr)
			if err != nil {
				hold(fmt.Sprintf("node %s: %v", id, err))
				return
			}
			addAll(ps)
		}(n.ID, addr)
	}
	wg.Wait()
	if c.storage != nil && IsSharedObjectStore(c.storage) {
		published, why := ReadPublishedPending(ctx, c.storage, c.cfg.ClusterName)
		addAll(published)
		held = append(held, why...)
	}
	sort.Strings(held)
	return paths, held
}

// ReadPublishedPending reads every pending list published under
// PendingRegistrationsRoot. held names each list, or the listing, that could
// not be read: the files it names are not known. A list deleted between the
// listing and the read is not held: its writer has nothing pending. A list
// over maxPublishedListBytes is held rather than read whole, and one written
// for another cluster is held rather than trusted.
func ReadPublishedPending(ctx context.Context, store storage.Backend, clusterName string) (paths []string, held []string) {
	keys, err := store.List(ctx, PendingRegistrationsRoot+"/")
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, nil
		}
		return nil, []string{fmt.Sprintf("published pending lists: list %s: %v", PendingRegistrationsRoot, err)}
	}
	for _, key := range keys {
		if path.Ext(key) != ".json" {
			continue
		}
		size, err := store.StatFile(ctx, key)
		switch {
		case err != nil:
			held = append(held, fmt.Sprintf("published pending list %s: %v", key, err))
			continue
		case size < 0:
			continue // deleted since the listing
		case size > maxPublishedListBytes:
			held = append(held, fmt.Sprintf("published pending list %s: %d bytes, over the %d the sweep reads", key, size, maxPublishedListBytes))
			continue
		}
		data, err := store.Read(ctx, key)
		if err != nil {
			if storage.IsNotFound(err) {
				continue
			}
			held = append(held, fmt.Sprintf("published pending list %s: %v", key, err))
			continue
		}
		var doc PendingRegistrationsDoc
		if err := json.Unmarshal(data, &doc); err != nil {
			held = append(held, fmt.Sprintf("published pending list %s: does not parse (%v); delete it once its writer has registered its files", key, err))
			continue
		}
		if doc.ClusterName != clusterName {
			held = append(held, fmt.Sprintf("published pending list %s: written for cluster %q, not %q; delete it if the bucket is not shared with that cluster", key, doc.ClusterName, clusterName))
			continue
		}
		for _, f := range doc.Files {
			if f.Path != "" {
				paths = append(paths, f.Path)
			}
		}
	}
	return paths, held
}
