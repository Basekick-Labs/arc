package auth

import (
	"database/sql"
	"errors"
	"fmt"
)

// Node-local tokens on a cluster node (#1630).
//
// Since v26.06.1 a cluster's tokens live in the replicated cluster state, and
// each node keeps a SQLite copy of them marked raft_materialised. A token that
// is in this node's SQLite but not in the cluster state authenticates here and
// nowhere else: a token created before v26.06.1, or one created while this
// node ran without its coordinator. Proposing a write for it changes nothing
// (the FSM drops a write to an ID it does not hold), and on an ID collision —
// legacy IDs are per-node AUTOINCREMENT, cluster IDs are Raft log indexes — it
// changes the cluster token that holds the same ID, on every node.
//
// So each token write on a cluster node first decides which token the ID
// names here, from this node's row and the cluster state's entry together
// (classifyTokenWrite). A cluster token is proposed as before. Any other is
// changed in this node's SQLite only, and only when the request names this
// node (?node=<node_id>): a revoke cannot be undone, and an ID names a
// different token on each node, so a request a load balancer sent to the
// wrong node must not change anything.
//
// Never classify and write inside a SQLite transaction: the auth database has
// one connection, and a proposal on the leader runs apply callbacks that write
// it synchronously.

// ClusterTokenView is the read side of the cluster state that token writes
// consult. The production proposer (cluster.CoordinatorAuthProposer)
// implements it; a proposer that does not is classified by the marker alone.
type ClusterTokenView interface {
	// ClusterToken returns the cluster state's entry for this ID.
	ClusterToken(id int64) (ClusterTokenEntry, bool)
	// ClusterTokenIDByName returns the ID of the cluster token with this name.
	ClusterTokenIDByName(name string) (int64, bool)
	// AppliedDone is the index of the last command this node's FSM finished
	// applying, callbacks included.
	AppliedDone() uint64
	// LocalNodeID is this node's cluster node ID.
	LocalNodeID() string
}

// TokenWriteOptions carries what the request said about where it applies.
type TokenWriteOptions struct {
	// Node is the ?node= value; NodeSet is true when the parameter was
	// present, even empty. Ignored on a standalone node.
	Node    string
	NodeSet bool
}

// TokenWriteResult says where a successful write took effect.
type TokenWriteResult struct {
	// NodeLocal is true when the token is not managed by the cluster and
	// was changed on this node only.
	NodeLocal   bool
	NodeID      string
	TokenID     int64
	Name        string // the token's name before the write
	Fingerprint string // of the hash before the write
}

// Codes of a TokenScopeError, returned to API clients as "code".
const (
	TokenCodeNodeLocal         = "node_local_token"     // node-local token, ?node not given
	TokenCodeNotInClusterState = "not_in_cluster_state" // a cluster row the cluster state no longer holds
	TokenCodeHistoryMismatch   = "history_mismatch"     // a cluster row from another Raft history
	TokenCodeWrongNode         = "wrong_node"           // ?node names another node
	TokenCodeInvalidNode       = "invalid_node"         // ?node present but empty
	TokenCodeClusterToken      = "cluster_token"        // ?node given for a cluster token
	TokenCodeNotCaughtUp       = "not_caught_up"        // this node has not applied the cluster state far enough
	TokenCodeChanged           = "token_changed"        // the row changed between the check and the write
	TokenCodeNameHeldByCluster = "name_held_by_cluster" // a node-local rename to a cluster token's name
	TokenCodeRevokeOrDelete    = "revoke_or_delete_only"
	TokenCodeClusterMembership = "cluster_membership" // the membership belongs to the cluster token at this ID
)

// TokenScopeError refuses a token write because of where the token lives.
// Code is one of the TokenCode constants; Message is written for the operator.
type TokenScopeError struct {
	Code    string
	TokenID int64
	NodeID  string
	Message string
}

func (e *TokenScopeError) Error() string { return e.Message }

type tokenClass int

const (
	classCluster           tokenClass = iota
	classNodeLocal                    // unmarked and not this ID's cluster token
	classNotInClusterState            // marked, the caught-up cluster state lacks it
	classHistoryMismatch              // marked, the cluster state holds another token at its ID
)

// tokenWriteRow is this node's row for a token write.
type tokenWriteRow struct {
	id         int64
	name, hash string
	createdAt  any
	enabled    bool
	marked     bool
}

// tokenWriteTarget is a classified token write.
type tokenWriteTarget struct {
	class  tokenClass
	row    tokenWriteRow
	nodeID string
}

// marker is the raft_materialised value a local write must still find.
func (t *tokenWriteTarget) marker() int {
	if t.row.marked {
		return 1
	}
	return 0
}

func (t *tokenWriteTarget) result() TokenWriteResult {
	return TokenWriteResult{
		NodeLocal:   t.class != classCluster,
		NodeID:      t.nodeID,
		TokenID:     t.row.id,
		Name:        t.row.name,
		Fingerprint: tokenFingerprint(t.row.hash),
	}
}

// getTokenRowForWrite reads this node's row with its marker. Separate from
// GetTokenByID, which the forwarded-request path uses.
func (am *AuthManager) getTokenRowForWrite(id int64) (*tokenWriteRow, error) {
	r := tokenWriteRow{id: id}
	var marked sql.NullInt64
	err := am.db.QueryRow(`SELECT name, token_hash, created_at, enabled, raft_materialised FROM api_tokens WHERE id = ?`, id).
		Scan(&r.name, &r.hash, &r.createdAt, &r.enabled, &marked)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to look up token: %w", err)
	}
	r.marked = marked.Int64 == 1
	return &r, nil
}

// tokenRowIsFSMEntry is the adoption rule for an unmarked row: the same name,
// and the same secret or creation instant. The reconcile adopts by it too.
func tokenRowIsFSMEntry(name, hash string, createdAt any, e ClusterTokenEntry) bool {
	return name == e.Name && (hash == e.TokenHash || sameInstant(createdAt, e.CreatedAtUnixNano))
}

// markedRowIsFSMEntry tells whether a marked row is the cluster token at its
// ID. The creation instant is the cluster's own and never changes; name and
// hash both equal covers a row whose instant failed to round-trip. Either
// holds while a rename or rotate is between the FSM map and the callback.
func markedRowIsFSMEntry(r *tokenWriteRow, e ClusterTokenEntry) bool {
	return sameInstant(r.createdAt, e.CreatedAtUnixNano) || (r.name == e.Name && r.hash == e.TokenHash)
}

// checkNodeParam refuses a request addressed to another node, before anything
// about this node's tokens is read: on the wrong node an ID names a different
// token.
func checkNodeParam(view ClusterTokenView, opts TokenWriteOptions) error {
	if opts.NodeSet && opts.Node == "" {
		return &TokenScopeError{Code: TokenCodeInvalidNode, Message: "The node parameter is empty. Pass ?node=<node_id> of the node that holds the token, or omit it for a cluster token."}
	}
	if view == nil || !opts.NodeSet {
		return nil
	}
	if local := view.LocalNodeID(); local != "" && opts.Node != local {
		return &TokenScopeError{Code: TokenCodeWrongNode, NodeID: local,
			Message: fmt.Sprintf("This request reached node %s, not node %s. Nothing was changed. Send it to node %s directly.", local, opts.Node, opts.Node)}
	}
	return nil
}

// classifyTokenWrite decides which token id names on this cluster node. It
// returns errTokenNotFound when this node has no row. A row that is the
// cluster's but not yet adopted (written by an apply before the marker
// existed) is adopted here, by the reconcile's rule, and classified cluster.
func (am *AuthManager) classifyTokenWrite(p RaftProposer, id int64) (*tokenWriteTarget, error) {
	view, _ := p.(ClusterTokenView)
	var applied uint64
	t := &tokenWriteTarget{}
	if view != nil {
		t.nodeID = view.LocalNodeID()
		// Read before the token map: an ID at or below it that the map
		// lacks was not in the cluster state when the map was read.
		applied = view.AppliedDone()
	}
	row, err := am.getTokenRowForWrite(id)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errTokenNotFound()
	}
	t.row = *row
	if view == nil {
		if row.marked {
			t.class = classCluster
		} else {
			t.class = classNodeLocal
		}
		return t, nil
	}
	e, inFSM := view.ClusterToken(id)
	switch {
	case row.marked && inFSM && markedRowIsFSMEntry(row, e):
		t.class = classCluster
	case row.marked && inFSM:
		t.class = classHistoryMismatch
	case row.marked && uint64(id) > applied:
		return nil, &TokenScopeError{Code: TokenCodeNotCaughtUp, TokenID: id, NodeID: t.nodeID,
			Message: "This node has not caught up with the cluster state yet. Nothing was changed; retry shortly."}
	case row.marked:
		t.class = classNotInClusterState
	case inFSM && tokenRowIsFSMEntry(row.name, row.hash, row.createdAt, e):
		res, err := am.db.Exec(`UPDATE api_tokens SET raft_materialised = 1 WHERE id = ? AND raft_materialised = 0`, id)
		if err != nil {
			return nil, fmt.Errorf("adopt cluster token %d: %w", id, err)
		}
		if n, err := res.RowsAffected(); err != nil || n == 0 {
			return nil, errTokenChanged(id, t.nodeID)
		}
		am.logger.Info().Int64("token_id", id).Msg("Adopted this node's copy of a cluster token before writing it")
		t.row.marked = true
		t.class = classCluster
	default:
		t.class = classNodeLocal
	}
	return t, nil
}

// resolveTokenWrite classifies a token write on a cluster node and applies
// the ?node= interlock: a cluster token must not name a node, any other must
// name this one.
func (am *AuthManager) resolveTokenWrite(p RaftProposer, id int64, opts TokenWriteOptions) (*tokenWriteTarget, error) {
	view, _ := p.(ClusterTokenView)
	if err := checkNodeParam(view, opts); err != nil {
		return nil, err
	}
	t, err := am.classifyTokenWrite(p, id)
	if err != nil {
		return nil, err
	}
	if t.class == classCluster {
		if opts.NodeSet {
			return nil, &TokenScopeError{Code: TokenCodeClusterToken, TokenID: id, NodeID: t.nodeID,
				Message: "This token is managed by the cluster: a change applies on every node. Nothing was changed. Repeat the request without ?node."}
		}
		return t, nil
	}
	if !opts.NodeSet {
		return nil, t.nodeLocalRefusal()
	}
	return t, nil
}

func nodeLabel(nodeID string) (label, param string) {
	if nodeID == "" {
		return "this node", "<node_id>"
	}
	return "node " + nodeID, nodeID
}

// nodeLocalRefusal is the 409 for a write to a token the cluster does not
// manage, sent without ?node=.
func (t *tokenWriteTarget) nodeLocalRefusal() error {
	on, param := nodeLabel(t.nodeID)
	e := &TokenScopeError{TokenID: t.row.id, NodeID: t.nodeID}
	switch t.class {
	case classNotInClusterState:
		e.Code = TokenCodeNotInClusterState
		e.Message = fmt.Sprintf("Token %d (%q) on %s is no longer in the cluster state (deleted while this node was away, or refused by a snapshot); it still authenticates on this node only. Nothing was changed. To revoke or delete it on this node, repeat the request against this node with ?node=%s.", t.row.id, t.row.name, on, param)
	case classHistoryMismatch:
		e.Code = TokenCodeHistoryMismatch
		e.Message = fmt.Sprintf("Token %d (%q) on %s is not the token the cluster state holds at this ID: this node's auth database comes from another cluster history. It authenticates on this node only. Nothing was changed. To revoke or delete it on this node, repeat the request against this node with ?node=%s.", t.row.id, t.row.name, on, param)
	default:
		e.Code = TokenCodeNodeLocal
		e.Message = fmt.Sprintf("Token %d (%q) exists only in the database of %s; the cluster does not manage it (a token created before v26.06.1, or while this node ran without its cluster). Nothing was changed. To change it on this node only, repeat the request against this node with ?node=%s. If the same token exists on other nodes, do the same on each of them.", t.row.id, t.row.name, on, param)
	}
	return e
}

// revokeOrDeleteOnly refuses a rotate or update of a cluster row the cluster
// state does not hold: the reconcile would undo it.
func (t *tokenWriteTarget) revokeOrDeleteOnly() error {
	on, _ := nodeLabel(t.nodeID)
	return &TokenScopeError{Code: TokenCodeRevokeOrDelete, TokenID: t.row.id, NodeID: t.nodeID,
		Message: fmt.Sprintf("Token %d (%q) on %s is a cluster token the cluster state does not hold; it can only be revoked or deleted on this node.", t.row.id, t.row.name, on)}
}

func errTokenChanged(id int64, nodeID string) error {
	return &TokenScopeError{Code: TokenCodeChanged, TokenID: id, NodeID: nodeID,
		Message: "The token changed while this request ran. Nothing was changed; retry."}
}

// localTokenWrite runs a node-local revoke or delete. query takes the ID and
// the marker the row must still carry.
func (am *AuthManager) localTokenWrite(t *tokenWriteTarget, op, query string) (TokenWriteResult, error) {
	res, err := am.db.Exec(query, t.row.id, t.marker())
	if err != nil {
		return TokenWriteResult{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return TokenWriteResult{}, err
	}
	if n == 0 {
		return TokenWriteResult{}, am.localWriteMissed(t)
	}
	am.InvalidateCache()
	am.logLocalWrite(t, op)
	return t.result(), nil
}

// localWriteMissed explains a node-local write that changed no row: the row
// is gone, or an apply or the reconcile changed its marker in between.
func (am *AuthManager) localWriteMissed(t *tokenWriteTarget) error {
	row, err := am.getTokenRowForWrite(t.row.id)
	if err != nil {
		return err
	}
	if row == nil {
		return errTokenNotFound()
	}
	return errTokenChanged(t.row.id, t.nodeID)
}

func (am *AuthManager) logLocalWrite(t *tokenWriteTarget, op string) {
	am.logger.Info().Int64("token_id", t.row.id).Str("name", t.row.name).Str("node_id", t.nodeID).
		Str("op", op).Msg("Changed a token the cluster does not manage, on this node only")
}

// ResolveTokenMembershipWrite classifies a team-membership change for token
// id. On a standalone node it returns a zero result. A cluster token gives
// NodeLocal false and the change is proposed as before. A node-local token
// cannot be added to a cluster team (the cluster state does not know it, and
// on an ID collision the membership would go to the cluster token); its
// membership can be removed on this node, under the same ?node= interlock as
// the token writes.
func (am *AuthManager) ResolveTokenMembershipWrite(id int64, opts TokenWriteOptions, remove bool) (TokenWriteResult, error) {
	p := am.getProposer()
	if p == nil {
		return TokenWriteResult{}, nil
	}
	view, _ := p.(ClusterTokenView)
	if err := checkNodeParam(view, opts); err != nil {
		return TokenWriteResult{}, err
	}
	t, err := am.classifyTokenWrite(p, id)
	if err != nil {
		return TokenWriteResult{}, err
	}
	if t.class == classCluster {
		if opts.NodeSet {
			return TokenWriteResult{}, &TokenScopeError{Code: TokenCodeClusterToken, TokenID: id, NodeID: t.nodeID,
				Message: "This token is managed by the cluster: a membership change applies on every node. Nothing was changed. Repeat the request without ?node."}
		}
		return t.result(), nil
	}
	if !remove {
		on, _ := nodeLabel(t.nodeID)
		return TokenWriteResult{}, &TokenScopeError{Code: TokenCodeNodeLocal, TokenID: id, NodeID: t.nodeID,
			Message: fmt.Sprintf("Token %d (%q) exists only in the database of %s; the cluster does not manage it, so it cannot join a cluster team. Create a cluster token (POST /api/v1/auth/tokens) and add that one.", id, t.row.name, on)}
	}
	if t.class != classNodeLocal {
		return TokenWriteResult{}, t.revokeOrDeleteOnly()
	}
	if !opts.NodeSet {
		return TokenWriteResult{}, t.nodeLocalRefusal()
	}
	return t.result(), nil
}

// clusterNode reports whether this node writes tokens through the cluster,
// and its node ID when the proposer knows it.
func (am *AuthManager) clusterNode() (bool, string) {
	p := am.getProposer()
	if p == nil {
		return false, ""
	}
	if view, ok := p.(ClusterTokenView); ok {
		return true, view.LocalNodeID()
	}
	return true, ""
}

// MarkNodeLocal sets info.NodeLocal and info.NodeID for the admin get on a
// cluster node, by the row's marker as ListTokens does. A no-op standalone or
// when the marker cannot be read.
func (am *AuthManager) MarkNodeLocal(info *TokenInfo) {
	if info == nil {
		return
	}
	clustered, nodeID := am.clusterNode()
	if !clustered {
		return
	}
	var marked sql.NullInt64
	if err := am.db.QueryRow(`SELECT raft_materialised FROM api_tokens WHERE id = ?`, info.ID).Scan(&marked); err != nil {
		return
	}
	if marked.Int64 != 1 {
		info.NodeLocal, info.NodeID = true, nodeID
	}
}
