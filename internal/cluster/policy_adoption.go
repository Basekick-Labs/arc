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
	// policyQueryTimeout bounds one node's answer to a collect, and so how
	// long local writes of the kind are refused when a node hangs.
	policyQueryTimeout = 8 * time.Second
	// policyApplyWait bounds how long a node waits to apply the collect
	// before answering.
	policyApplyWait = 5 * time.Second
	// policyProbeTimeout bounds the reachability check of each node before
	// a collect opens.
	policyProbeTimeout = 2 * time.Second
	// policyAdoptRetry spaces adoption attempts of a kind that could not
	// seed; it doubles with each failed attempt, up to policyAdoptRetryMax.
	policyAdoptRetry    = 30 * time.Second
	policyAdoptRetryMax = 10 * time.Minute
	// policyReplyBudget bounds the rows one answer carries (under the
	// protocol's frame cap).
	policyReplyBudget = 900 * 1024
	// policyMirrorRetry spaces a failed mirror's next attempt.
	policyMirrorRetry = 5 * time.Second
)

// policyKinds are the kinds this build adopts, in order.
var policyKinds = []string{raft.PolicyKindTiering, raft.PolicyKindRetention}

// probesFirst reports kinds whose merge can refuse the rows (retention): the
// leader reads every node's rows with a read-only probe first and opens a
// collect, with its window of refused writes, only when the merge would seed.
func probesFirst(kind string) bool { return kind == raft.PolicyKindRetention }

// EmptyPolicyLister answers that this node holds no policies of a kind: for a
// node that does not run it (tiering disabled).
func EmptyPolicyLister(context.Context) (json.RawMessage, error) { return json.RawMessage("null"), nil }

// SetLocalPolicyLister installs the reader of this node's own rows of kind.
// Until one is installed the node cannot answer a collect, which then waits.
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
// the collect at epoch and it is still open. Epoch zero is a read-only probe:
// the rows as they are now, with no collect.
func (c *Coordinator) localPoliciesAnswer(ctx context.Context, kind string, epoch uint64) (string, json.RawMessage) {
	if epoch == 0 {
		return c.readLocalPolicies(ctx, kind)
	}
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
	return c.readLocalPolicies(ctx, kind)
}

// readLocalPolicies runs kind's lister.
func (c *Coordinator) readLocalPolicies(ctx context.Context, kind string) (string, json.RawMessage) {
	fn := c.policyLister(kind)
	if fn == nil {
		// Not "none": a node that has not finished starting registers its
		// lister late, and an empty answer from it would drop its policies
		// for good. A node that does not run the kind registers an empty
		// lister explicitly (EmptyPolicyLister).
		c.logger.Warn().Str("kind", kind).Msg("Policy collect: this node's policies are not readable yet")
		return protocol.LocalPoliciesFailed, nil
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
	case q.HMAC == "" || q.Nonce == "" || q.NodeID == "" || q.Timestamp == 0:
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
	// One pass at a time: two would replace each other's collects, and one
	// could end the admin's choice the other was about to act on.
	c.adoptMu.Lock()
	defer c.adoptMu.Unlock()
	for _, kind := range policyKinds {
		if !c.policyKindActive(kind) || fsm.PoliciesSeeded(kind) {
			continue
		}
		_, chosen := fsm.PolicyAdoptChoiceFor(kind)
		if a, ok := fsm.PolicyAwaitingAdoption(kind); ok && !chosen && time.Since(a.At) < adoptBackoff(a.Attempts) {
			continue
		}
		if err := c.adoptPolicies(ctx, kind); err != nil {
			c.logger.Warn().Err(err).Str("kind", kind).Msg("Adopting the nodes' policies did not complete; retrying")
		}
	}
}

// adoptBackoff is the wait after attempts consecutive failed adoptions.
func adoptBackoff(attempts int) time.Duration {
	d := policyAdoptRetry
	for i := 1; i < attempts && d < policyAdoptRetryMax; i++ {
		d *= 2
	}
	return min(d, policyAdoptRetryMax)
}

// unreachableNodes returns the FSM nodes, other than this one, this node
// cannot connect to. Checked before a collect opens, so a node that is down
// costs no window of refused writes.
func (c *Coordinator) unreachableNodes(ctx context.Context) []string {
	fsm := c.fsmOrNil()
	var mu sync.Mutex
	var wg sync.WaitGroup
	var out []string
	for _, n := range fsm.GetAllNodes() {
		if n.ID == c.localNode.ID {
			continue
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			addr := c.leaderCoordinatorAddress(id)
			ok := false
			if addr != "" {
				d := net.Dialer{Timeout: policyProbeTimeout}
				if conn, err := d.DialContext(ctx, "tcp", addr); err == nil {
					_ = conn.Close()
					ok = true
				}
			}
			if !ok {
				mu.Lock()
				out = append(out, id)
				mu.Unlock()
			}
		}(n.ID)
	}
	wg.Wait()
	sort.Strings(out)
	return out
}

// adoptPolicies runs one collect of kind and seeds it, or closes it naming
// the nodes that did not answer.
func (c *Coordinator) adoptPolicies(ctx context.Context, kind string) error {
	fsm := c.fsmOrNil()
	if down := c.unreachableNodes(ctx); len(down) > 0 {
		if _, err := c.proposeLeader(raft.CommandAwaitAdoption, raft.AwaitAdoptionPayload{Kind: kind, Missing: down, LastError: "unreachable before the collect", Now: time.Now().UTC()}); err != nil {
			return fmt.Errorf("record unreachable nodes: %w", err)
		}
		return fmt.Errorf("nodes unreachable: %v", down)
	}
	var choice *raft.PolicyAdoptChoice
	if ch, ok := fsm.PolicyAdoptChoiceFor(kind); ok {
		choice = &ch
	}
	_, stale := fsm.PolicyCollecting(kind)
	if probesFirst(kind) && choice == nil && !stale {
		// Probe first: a merge that would refuse the rows (nodes that
		// disagree, a policy that never ran) must not cost a window of
		// refused writes at every retry. A collect left open (a leader that
		// died mid-collect) is replaced by a new one at once instead: only
		// a collect can close it.
		answers := c.queryAllNodes(ctx, kind, 0)
		missing, rows := splitAnswers(answers)
		why := ""
		if len(missing) > 0 {
			why = "no answer to the probe"
		} else if _, err := mergePolicyAnswers(kind, rows, nil); err != nil {
			why = err.Error()
		}
		if why != "" {
			if _, err := c.proposeLeader(raft.CommandAwaitAdoption, raft.AwaitAdoptionPayload{Kind: kind, Missing: missing, LastError: why, Now: time.Now().UTC()}); err != nil {
				return fmt.Errorf("record probe: %w", err)
			}
			return fmt.Errorf("%s %v", why, missing)
		}
	}
	epoch, err := c.proposeLeader(raft.CommandCollectPolicies, raft.CollectPoliciesPayload{Kind: kind, Leader: c.localNode.ID, Now: time.Now().UTC()})
	if err != nil {
		return fmt.Errorf("open collect: %w", err)
	}
	answers := c.queryAllNodes(ctx, kind, epoch)
	missing, rows := splitAnswers(answers)
	for _, id := range missing {
		c.logger.Warn().Err(answers[id].err).Str("kind", kind).Str("node_id", id).Msg("Policy collect: no answer from node")
	}
	var seed raft.SeedPoliciesPayload
	lastError := ""
	if len(missing) > 0 {
		lastError = "no answer to the collect"
	} else {
		seed, err = mergePolicyAnswers(kind, rows, choice)
		if err != nil {
			c.logger.Warn().Err(err).Str("kind", kind).Msg("Policy collect: the nodes' policies cannot be merged")
			lastError = "merge: " + err.Error()
		}
	}
	if lastError != "" {
		await := raft.AwaitAdoptionPayload{Kind: kind, Epoch: epoch, Missing: missing, LastError: lastError, Now: time.Now().UTC()}
		if choice != nil {
			// This attempt acted on the admin's choice: it ends it.
			await.ChoiceNode, await.ChoiceHash = choice.Node, choice.Hash
		}
		if _, err := c.proposeLeader(raft.CommandAwaitAdoption, await); err != nil {
			return fmt.Errorf("close collect: %w", err)
		}
		return fmt.Errorf("%s %v", lastError, missing)
	}
	seed.Kind, seed.Epoch, seed.Now = kind, epoch, time.Now().UTC()
	if _, err := c.proposeLeader(raft.CommandSeedPolicies, seed); err != nil {
		return fmt.Errorf("seed: %w", err)
	}
	ev := c.logger.Info().Str("kind", kind).Int("nodes", len(answers)).Int("policies", len(seed.Tiering)+len(seed.Retention))
	if choice != nil {
		ev = ev.Str("chosen_node", choice.Node).Str("chosen_by", choice.By)
	}
	ev.Msg("Cluster policies seeded from the nodes' own policies")
	return nil
}

// queryAllNodes asks every FSM node, this one included, for its rows of kind:
// for the collect at epoch, or with epoch zero a read-only probe.
func (c *Coordinator) queryAllNodes(ctx context.Context, kind string, epoch uint64) map[string]policyAnswer {
	answers := map[string]policyAnswer{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, n := range c.fsmOrNil().GetAllNodes() {
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
	return answers
}

// splitAnswers returns the nodes that did not answer, sorted, and the rows of
// those that did.
func splitAnswers(answers map[string]policyAnswer) ([]string, map[string]json.RawMessage) {
	var missing []string
	rows := make(map[string]json.RawMessage, len(answers))
	for id, a := range answers {
		if a.err != nil {
			missing = append(missing, id)
			continue
		}
		rows[id] = a.rows
	}
	sort.Strings(missing)
	return missing, rows
}

// nudgeAdoption runs an adoption pass now (an admin's choice) rather than at
// the next tick.
func (c *Coordinator) nudgeAdoption() {
	go c.maybeSeedPolicies(c.ctxOrBackground())
}

// TieringLocalRows is one node's answer for the tiering kind: its rows, and
// the default an unset hot_max_age_days means on that node.
type TieringLocalRows struct {
	DefaultHotMaxAgeDays int                       `json:"default_hot_max_age_days"`
	Policies             []raft.TieringPolicyEntry `json:"policies"`
}

// mergePolicyAnswers merges every node's rows of kind into one seed.
func mergePolicyAnswers(kind string, rows map[string]json.RawMessage, choice *raft.PolicyAdoptChoice) (raft.SeedPoliciesPayload, error) {
	switch kind {
	case raft.PolicyKindRetention:
		perNode := make(map[string]RetentionLocalRows, len(rows))
		for id, raw := range rows {
			r, err := decodeRetentionRows(raw)
			if err != nil {
				return raft.SeedPoliciesPayload{}, fmt.Errorf("node %s: %w", id, err)
			}
			perNode[id] = r
		}
		seed, why := planRetentionSeed(perNode, choice)
		if why != "" {
			return raft.SeedPoliciesPayload{}, errors.New(why)
		}
		return seed, nil
	case raft.PolicyKindTiering:
		perNode := map[string]TieringLocalRows{}
		for id, raw := range rows {
			var r TieringLocalRows
			if len(raw) > 0 && string(raw) != "null" {
				if err := json.Unmarshal(raw, &r); err != nil {
					return raft.SeedPoliciesPayload{}, fmt.Errorf("node %s: %w", id, err)
				}
			}
			perNode[id] = r
		}
		merged := mergeTieringPolicies(perNode)
		if len(merged) > raft.MaxSeedTieringPolicies {
			return raft.SeedPoliciesPayload{}, fmt.Errorf("%d tiering policies exceed the %d one seed installs", len(merged), raft.MaxSeedTieringPolicies)
		}
		return raft.SeedPoliciesPayload{Tiering: merged}, nil
	default:
		return raft.SeedPoliciesPayload{}, fmt.Errorf("unknown kind %q", kind)
	}
}

// mergeTieringPolicies merges the nodes' tiering policies by database, in the
// direction that moves less data. hot_only on any node wins. Otherwise the
// age is the longest any node holding a policy for the database applied: its
// own hot_max_age_days, or, unset, that node's default. A database whose
// policy leaves the age unset on every node stays unset (each node goes on
// using its default). Nothing is deleted by a tiering policy, so the union is
// safe.
func mergeTieringPolicies(perNode map[string]TieringLocalRows) []raft.TieringPolicyEntry {
	type acc struct {
		e        raft.TieringPolicyEntry
		explicit bool
		age      int
	}
	byDB := map[string]*acc{}
	for _, node := range perNode {
		for _, e := range node.Policies {
			if e.Database == "" {
				continue
			}
			age, explicit := node.DefaultHotMaxAgeDays, false
			if e.HotMaxAgeDays != nil {
				age, explicit = *e.HotMaxAgeDays, true
			}
			m, ok := byDB[e.Database]
			if !ok {
				c := e
				c.HotMaxAgeDays = nil
				byDB[e.Database] = &acc{e: c, explicit: explicit, age: age}
				continue
			}
			m.e.HotOnly = m.e.HotOnly || e.HotOnly
			m.explicit = m.explicit || explicit
			if age > m.age {
				m.age = age
			}
			if !e.CreatedAt.IsZero() && (m.e.CreatedAt.IsZero() || e.CreatedAt.Before(m.e.CreatedAt)) {
				m.e.CreatedAt = e.CreatedAt
			}
			if e.UpdatedAt.After(m.e.UpdatedAt) {
				m.e.UpdatedAt = e.UpdatedAt
			}
		}
	}
	out := make([]raft.TieringPolicyEntry, 0, len(byDB))
	for _, m := range byDB {
		e := m.e
		if m.explicit {
			v := m.age
			e.HotMaxAgeDays = &v
		}
		out = append(out, e)
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
