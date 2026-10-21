package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
)

// Policy adoption (#1324): how the cluster takes over each node's own
// policies of a kind, once cluster policies are enabled.
//
// The leader opens a collect (CommandCollectPolicies): from the moment a node
// applies it, that node refuses local writes of the kind. It then asks every
// node in the FSM for its rows, each answering only after it applied the
// collect, under the lock its local writes take, so a write either is in the
// answer or was refused. If every node answered, the leader merges the rows
// and seeds the kind (CommandSeedPolicies, accepted only for the open
// collect); from then on the kind lives in the cluster state. If any node did
// not answer, the leader closes the collect (CommandAwaitAdoption) naming
// them, local writes resume, and it tries again later: a partial view never
// seeds, or a policy held only by the silent node would be dropped.

// LocalPolicyLister returns this node's own rows of a kind, as JSON.
type LocalPolicyLister func(ctx context.Context) (json.RawMessage, error)

// Tunables, variables so tests can shrink them.
var (
	// policyQueryTimeout bounds one node's answer to a collect.
	policyQueryTimeout = 15 * time.Second
	// policyApplyWait bounds how long a node waits to apply the collect
	// before answering.
	policyApplyWait = 10 * time.Second
	// policyAdoptRetry spaces collects of a kind that could not seed.
	policyAdoptRetry = 30 * time.Second
	// policyReplyBudget bounds the rows one answer carries (under the
	// protocol's frame cap).
	policyReplyBudget = 900 * 1024
)

// maxSeedTieringPolicies bounds the tiering policies one seed installs.
const maxSeedTieringPolicies = 10000

// policyKinds are the kinds this build adopts, in order.
var policyKinds = []string{raft.PolicyKindTiering}

// SetLocalPolicyLister installs the reader of this node's own rows of kind.
// Without one the node answers that it has none: it does not run that kind.
func (c *Coordinator) SetLocalPolicyLister(kind string, fn LocalPolicyLister) {
	c.policyListersMu.Lock()
	defer c.policyListersMu.Unlock()
	if c.policyListers == nil {
		c.policyListers = map[string]LocalPolicyLister{}
	}
	if fn == nil {
		delete(c.policyListers, kind)
		return
	}
	c.policyListers[kind] = fn
}

func (c *Coordinator) policyLister(kind string) LocalPolicyLister {
	c.policyListersMu.Lock()
	defer c.policyListersMu.Unlock()
	return c.policyListers[kind]
}

// PolicyCollecting reports whether a collect of kind is open: local writes of
// that kind are refused until it ends.
func (c *Coordinator) PolicyCollecting(kind string) bool {
	fsm := c.fsmOrNil()
	if fsm == nil {
		return false
	}
	_, ok := fsm.PolicyCollecting(kind)
	return ok
}

// localPoliciesAnswer reads this node's rows of kind once its FSM has applied
// the collect at epoch and it is still open.
func (c *Coordinator) localPoliciesAnswer(ctx context.Context, kind string, epoch uint64) (string, json.RawMessage) {
	waitCtx, cancel := context.WithTimeout(ctx, policyApplyWait)
	err := c.waitAppliedHere(waitCtx, epoch)
	cancel()
	if err != nil {
		c.logger.Warn().Err(err).Str("kind", kind).Msg("Policy collect: this node has not applied the collect")
		return protocol.LocalPoliciesFailed, nil
	}
	fsm := c.fsmOrNil()
	if fsm == nil {
		return protocol.LocalPoliciesFailed, nil
	}
	if open, ok := fsm.PolicyCollecting(kind); !ok || open.Epoch != epoch {
		c.logger.Warn().Str("kind", kind).Uint64("epoch", epoch).Msg("Policy collect: that collect is no longer open")
		return protocol.LocalPoliciesFailed, nil
	}
	fn := c.policyLister(kind)
	if fn == nil {
		return protocol.LocalPoliciesOK, json.RawMessage("[]")
	}
	rows, err := fn(ctx)
	if err != nil {
		c.logger.Warn().Err(err).Str("kind", kind).Msg("Policy collect: could not read this node's policies")
		return protocol.LocalPoliciesFailed, nil
	}
	if len(rows) > policyReplyBudget {
		c.logger.Warn().Str("kind", kind).Int("bytes", len(rows)).Msg("Policy collect: this node's policies exceed one answer")
		return protocol.LocalPoliciesFailed, nil
	}
	return protocol.LocalPoliciesOK, rows
}

// handleLocalPoliciesQuery answers the leader's collect with this node's own
// rows of the kind.
func (c *Coordinator) handleLocalPoliciesQuery(conn net.Conn, q *protocol.LocalPoliciesQuery) {
	refuse := func(why string) {
		c.logger.Warn().Str("peer", conn.RemoteAddr().String()).Str("node_id", q.NodeID).
			Msg("Local policies query refused: " + why)
		c.sendLocalPoliciesReply(conn, q, protocol.LocalPoliciesRefused, nil)
	}
	switch {
	case c.cfg.SharedSecret == "":
		refuse("cluster.shared_secret not configured")
		return
	case q.HMAC == "" || q.Nonce == "" || q.NodeID == "" || q.Timestamp == 0 || q.Epoch == 0:
		refuse("missing fields")
		return
	case q.ClusterName != c.cfg.ClusterName:
		refuse("cluster name mismatch")
		return
	}
	if err := security.ValidateLocalPoliciesQueryHMAC(c.cfg.SharedSecret, q.Nonce, q.NodeID, q.ClusterName, q.Kind, q.Epoch, q.Timestamp, q.HMAC, security.HMACTimestampTolerance); err != nil {
		refuse("HMAC validation failed")
		return
	}
	if c.nonceCache == nil || !c.nonceCache.Track(q.NodeID, q.Nonce) {
		refuse("replay (nonce already seen)")
		return
	}
	if c.raftNode == nil {
		refuse("raft not initialized")
		return
	}
	if leader := c.raftNode.LeaderID(); leader == "" || leader != q.NodeID {
		refuse("the peer is not the Raft leader")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), policyQueryTimeout-2*time.Second)
	defer cancel()
	status, rows := c.localPoliciesAnswer(ctx, q.Kind, q.Epoch)
	c.sendLocalPoliciesReply(conn, q, status, rows)
}

func (c *Coordinator) sendLocalPoliciesReply(conn net.Conn, q *protocol.LocalPoliciesQuery, status string, rows json.RawMessage) {
	ts := time.Now().Unix()
	reply := &protocol.LocalPoliciesReply{
		NodeID: c.localNode.ID, Status: status, Policies: rows, Timestamp: ts,
		HMAC: security.ComputeLocalPoliciesReplyHMAC(c.cfg.SharedSecret, q.Nonce, c.localNode.ID, c.cfg.ClusterName, q.Kind, q.Epoch, status, rows, ts),
	}
	if err := protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgLocalPoliciesReply, Payload: reply}, 10*time.Second); err != nil {
		c.logger.Warn().Err(err).Str("peer", conn.RemoteAddr().String()).Msg("Could not send the local policies reply")
	}
}

// requestLocalPolicies asks the node at addr for its rows of kind for the
// collect at epoch.
func (c *Coordinator) requestLocalPolicies(ctx context.Context, nodeID, addr, kind string, epoch uint64) (json.RawMessage, error) {
	if c.cfg.SharedSecret == "" {
		return nil, errors.New("cluster.shared_secret not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, policyQueryTimeout)
	defer cancel()
	conn, err := security.DialContext(ctx, "tcp", addr, peerDialTimeout, c.tlsConfig)
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
	q := &protocol.LocalPoliciesQuery{NodeID: c.localNode.ID, ClusterName: c.cfg.ClusterName, Kind: kind, Epoch: epoch, Nonce: nonce, Timestamp: ts}
	q.HMAC = security.ComputeLocalPoliciesQueryHMAC(c.cfg.SharedSecret, nonce, q.NodeID, q.ClusterName, kind, epoch, ts)
	if err := protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgLocalPoliciesQuery, Payload: q}, policyQueryTimeout); err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}
	wait := policyQueryTimeout
	if dl, ok := ctx.Deadline(); ok {
		wait = min(wait, time.Until(dl))
	}
	msg, err := protocol.ReceiveMessage(conn, wait)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errors.New("it closed the connection")
		}
		return nil, err
	}
	reply, ok := msg.Payload.(*protocol.LocalPoliciesReply)
	if !ok || msg.Type != protocol.MsgLocalPoliciesReply {
		return nil, fmt.Errorf("unexpected reply type %s", msg.Type)
	}
	if reply.NodeID != nodeID {
		return nil, fmt.Errorf("answered as %q", reply.NodeID)
	}
	if err := security.ValidateLocalPoliciesReplyHMAC(c.cfg.SharedSecret, nonce, reply.NodeID, c.cfg.ClusterName, kind, epoch, reply.Status, reply.Policies, reply.Timestamp, reply.HMAC, security.HMACTimestampTolerance); err != nil {
		return nil, err
	}
	if reply.Status != protocol.LocalPoliciesOK {
		return nil, fmt.Errorf("status %q", reply.Status)
	}
	return reply.Policies, nil
}

// policyAnswer is one node's answer to a collect.
type policyAnswer struct {
	rows json.RawMessage
	err  error
}

// maybeSeedPolicies is one pass of the adoption, on the leader: for each kind
// not yet seeded, collect every node's rows and seed them.
func (c *Coordinator) maybeSeedPolicies(ctx context.Context) {
	fsm := c.fsmOrNil()
	if fsm == nil || !fsm.ClusterPoliciesEnabled() || !c.raftNode.IsLeader() || c.localNode == nil {
		return
	}
	for _, kind := range policyKinds {
		if fsm.PoliciesSeeded(kind) {
			continue
		}
		if a, ok := fsm.PolicyAwaitingAdoption(kind); ok && time.Since(a.At) < policyAdoptRetry {
			continue
		}
		if err := c.adoptPolicies(ctx, kind); err != nil {
			c.logger.Warn().Err(err).Str("kind", kind).Msg("Adopting the nodes' policies did not complete; retrying")
		}
	}
}

// adoptPolicies runs one collect of kind and seeds it, or closes it naming
// the nodes that did not answer.
func (c *Coordinator) adoptPolicies(ctx context.Context, kind string) error {
	fsm := c.fsmOrNil()
	epoch, err := c.proposeLeader(raft.CommandCollectPolicies, raft.CollectPoliciesPayload{Kind: kind, Leader: c.localNode.ID, Now: time.Now().UTC()})
	if err != nil {
		return fmt.Errorf("open collect: %w", err)
	}
	answers := map[string]policyAnswer{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, n := range fsm.GetAllNodes() {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			var a policyAnswer
			if id == c.localNode.ID {
				status, rows := c.localPoliciesAnswer(ctx, kind, epoch)
				if status != protocol.LocalPoliciesOK {
					a.err = fmt.Errorf("status %q", status)
				}
				a.rows = rows
			} else if addr := c.leaderCoordinatorAddress(id); addr == "" {
				a.err = errors.New("no coordinator address")
			} else {
				a.rows, a.err = c.requestLocalPolicies(ctx, id, addr, kind, epoch)
			}
			mu.Lock()
			answers[id] = a
			mu.Unlock()
		}(n.ID)
	}
	wg.Wait()

	var missing []string
	for id, a := range answers {
		if a.err != nil {
			c.logger.Warn().Err(a.err).Str("kind", kind).Str("node_id", id).Msg("Policy collect: no answer from node")
			missing = append(missing, id)
		}
	}
	sort.Strings(missing)
	var seed raft.SeedPoliciesPayload
	if len(missing) == 0 {
		rows := make(map[string]json.RawMessage, len(answers))
		for id, a := range answers {
			rows[id] = a.rows
		}
		seed, err = mergePolicyAnswers(kind, rows)
		if err != nil {
			c.logger.Warn().Err(err).Str("kind", kind).Msg("Policy collect: the nodes' policies cannot be merged")
			missing = []string{"(merge: " + err.Error() + ")"}
		}
	}
	if len(missing) > 0 {
		if _, err := c.proposeLeader(raft.CommandAwaitAdoption, raft.AwaitAdoptionPayload{Kind: kind, Epoch: epoch, Missing: missing, Now: time.Now().UTC()}); err != nil {
			return fmt.Errorf("close collect: %w", err)
		}
		return fmt.Errorf("nodes did not answer: %v", missing)
	}
	seed.Kind, seed.Epoch, seed.Now = kind, epoch, time.Now().UTC()
	if _, err := c.proposeLeader(raft.CommandSeedPolicies, seed); err != nil {
		return fmt.Errorf("seed: %w", err)
	}
	c.logger.Info().Str("kind", kind).Int("nodes", len(answers)).Int("tiering_policies", len(seed.Tiering)).
		Msg("Cluster policies seeded from every node's own policies")
	return nil
}

// mergePolicyAnswers merges every node's rows of kind into one seed.
func mergePolicyAnswers(kind string, rows map[string]json.RawMessage) (raft.SeedPoliciesPayload, error) {
	switch kind {
	case raft.PolicyKindTiering:
		perNode := map[string][]raft.TieringPolicyEntry{}
		for id, raw := range rows {
			var entries []raft.TieringPolicyEntry
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &entries); err != nil {
					return raft.SeedPoliciesPayload{}, fmt.Errorf("node %s: %w", id, err)
				}
			}
			perNode[id] = entries
		}
		merged := mergeTieringPolicies(perNode)
		if len(merged) > maxSeedTieringPolicies {
			return raft.SeedPoliciesPayload{}, fmt.Errorf("%d tiering policies exceed the %d one seed installs", len(merged), maxSeedTieringPolicies)
		}
		return raft.SeedPoliciesPayload{Tiering: merged}, nil
	default:
		return raft.SeedPoliciesPayload{}, fmt.Errorf("unknown kind %q", kind)
	}
}

// mergeTieringPolicies merges the nodes' tiering policies by database, in the
// direction that moves less data: hot_only on any node wins, and otherwise
// the longest hot_max_age_days set on any node (unset only when unset
// everywhere). Nothing is deleted by a tiering policy, so the union is safe.
func mergeTieringPolicies(perNode map[string][]raft.TieringPolicyEntry) []raft.TieringPolicyEntry {
	byDB := map[string]*raft.TieringPolicyEntry{}
	for _, entries := range perNode {
		for _, e := range entries {
			if e.Database == "" {
				continue
			}
			m, ok := byDB[e.Database]
			if !ok {
				c := e
				if e.HotMaxAgeDays != nil {
					v := *e.HotMaxAgeDays
					c.HotMaxAgeDays = &v
				}
				byDB[e.Database] = &c
				continue
			}
			m.HotOnly = m.HotOnly || e.HotOnly
			if e.HotMaxAgeDays != nil && (m.HotMaxAgeDays == nil || *e.HotMaxAgeDays > *m.HotMaxAgeDays) {
				v := *e.HotMaxAgeDays
				m.HotMaxAgeDays = &v
			}
			if !e.CreatedAt.IsZero() && (m.CreatedAt.IsZero() || e.CreatedAt.Before(m.CreatedAt)) {
				m.CreatedAt = e.CreatedAt
			}
			if e.UpdatedAt.After(m.UpdatedAt) {
				m.UpdatedAt = e.UpdatedAt
			}
		}
	}
	out := make([]raft.TieringPolicyEntry, 0, len(byDB))
	for _, e := range byDB {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Database < out[j].Database })
	return out
}

// proposeLeader applies a leader-only command here and returns its index.
func (c *Coordinator) proposeLeader(cmdType raft.CommandType, payload interface{}) (uint64, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	return c.raftNode.ApplyIndex(&raft.Command{Type: cmdType, Payload: raw}, policyProposeTimeout)
}
