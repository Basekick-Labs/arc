package cluster

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/security"
)

// CapabilityForwardIdentity is advertised at join by builds whose receiving
// side authenticates a forwarded request by its signed identity (#1305). A
// forwarding node sends the identity, and strips the caller's credential, only
// to peers that advertise it; an older peer still gets the credential, so a
// rolling upgrade keeps forwarding.
const CapabilityForwardIdentity = "forward_identity_v1"

type forwardTokenKey struct{}

// WithForwardTokenID records, on the context of a request about to be
// forwarded, the ID of the token the forwarding node verified for it; 0 when
// auth is off. Carried in the context, not a header, so it cannot reach the
// wire on a path that skips the router.
func WithForwardTokenID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, forwardTokenKey{}, id)
}

func forwardTokenID(ctx context.Context) int64 {
	id, _ := ctx.Value(forwardTokenKey{}).(int64)
	return id
}

// credentialHeaders are the request headers that can carry a caller's
// credential (auth.ExtractTokenFromRequest reads Authorization and
// X-Api-Key; Cookie is never read by Arc but is a credential by nature).
var credentialHeaders = []string{"Authorization", "X-Api-Key", "Cookie", "Proxy-Authorization"}

// credentialQueryParams are the query parameters that can carry one: `p` is
// the InfluxDB 1.x token parameter Arc reads; `u` is its user name.
var credentialQueryParams = map[string]bool{"p": true, "u": true}

// stripCredentialQuery removes the credential parameters from a raw query,
// keeping every other parameter in its original order and encoding.
func stripCredentialQuery(raw string) string {
	if raw == "" {
		return raw
	}
	parts := strings.Split(raw, "&")
	out := parts[:0]
	changed := false
	for _, p := range parts {
		key := p
		if i := strings.IndexByte(p, '='); i >= 0 {
			key = p[:i]
		}
		if k, err := url.QueryUnescape(key); err == nil {
			key = k
		}
		if credentialQueryParams[key] {
			changed = true
			continue
		}
		out = append(out, p)
	}
	if !changed {
		return raw
	}
	return strings.Join(out, "&")
}

// removeForwardIdentityHeaders drops every assertion header from h, whatever
// a client may have sent.
func removeForwardIdentityHeaders(h http.Header) {
	for k := range h {
		if strings.HasPrefix(http.CanonicalHeaderKey(k), security.ForwardIdentityHeaderPfx) {
			delete(h, k)
		}
	}
}

// signForward prepares a request about to be sent to node: assertion headers
// a client sent are always removed; to a peer that authenticates forwarded
// requests by identity, the caller's credential is removed and the signed
// identity added. Called for every attempt, so a retry is signed afresh.
func (r *Router) signForward(req *http.Request, node *Node, tokenID int64) error {
	removeForwardIdentityHeaders(req.Header)
	if r.cfg.PeerForwardIdentity == nil || r.cfg.SharedSecret == "" || !r.cfg.PeerForwardIdentity(node.ID) {
		return nil
	}
	for _, h := range credentialHeaders {
		req.Header.Del(h)
	}
	req.URL.RawQuery = stripCredentialQuery(req.URL.RawQuery)
	nonce, err := security.GenerateNonce()
	if err != nil {
		return err
	}
	f := security.ForwardIdentity{
		NodeID:          r.cfg.LocalNode.ID,
		TargetID:        node.ID,
		Cluster:         r.cfg.ClusterName,
		TokenID:         tokenID,
		Method:          req.Method,
		RequestURI:      req.URL.RequestURI(),
		Database:        req.Header.Get("X-Arc-Database"),
		Measurement:     req.Header.Get("X-Arc-Measurement"),
		ContentType:     req.Header.Get("Content-Type"),
		ContentEncoding: req.Header.Get("Content-Encoding"),
		Timestamp:       time.Now().Unix(),
		Nonce:           nonce,
	}
	req.Header.Set(security.HeaderForwardNode, f.NodeID)
	req.Header.Set(security.HeaderForwardTarget, f.TargetID)
	req.Header.Set(security.HeaderForwardCluster, f.Cluster)
	req.Header.Set(security.HeaderForwardTokenID, strconv.FormatInt(f.TokenID, 10))
	req.Header.Set(security.HeaderForwardTimestamp, strconv.FormatInt(f.Timestamp, 10))
	req.Header.Set(security.HeaderForwardNonce, f.Nonce)
	req.Header.Set(security.HeaderForwardHMAC, security.ComputeForwardIdentityHMAC(r.cfg.SharedSecret, f))
	return nil
}

// peerForwardIdentity reports whether node authenticates forwarded requests
// by identity. Needs Raft: a token's ID is its Raft log index, the same on
// every node, and without Raft each node has its own tokens. A peer missing
// from the FSM is treated as capable: the forward then fails closed (401) on
// an older build instead of handing it the credential.
func (c *Coordinator) peerForwardIdentity(nodeID string) bool {
	if c.raftNode == nil || c.cfg.SharedSecret == "" {
		return false
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return false
	}
	n, ok := fsm.GetNode(nodeID)
	if !ok {
		return true
	}
	return hasCapability(n.Capabilities, CapabilityForwardIdentity)
}

// ForwardIdentityHeaderValues returns every value of a request header.
type ForwardIdentityHeaderValues func(name string) []string

// VerifyForwardIdentity checks the signed identity of a request a peer
// forwarded here and returns the caller's token ID. present is false when the
// request carries no assertion header at all. Any assertion header makes the
// request a forwarded one: every header must then be present exactly once and
// verify, or err is set; the caller answers 401 and never falls back to
// another credential.
func (c *Coordinator) VerifyForwardIdentity(values ForwardIdentityHeaderValues, method, requestURI string) (present bool, tokenID int64, err error) {
	got := make(map[string]string, len(security.ForwardIdentityHeaders))
	for _, h := range security.ForwardIdentityHeaders {
		v := values(h)
		if len(v) > 0 {
			present = true
		}
		if len(v) == 1 && v[0] != "" {
			got[h] = v[0]
		}
	}
	if !present {
		return false, 0, nil
	}
	if len(got) != len(security.ForwardIdentityHeaders) {
		return true, 0, security.ErrForwardIdentity
	}
	if c.raftNode == nil || c.cfg.SharedSecret == "" {
		return true, 0, security.ErrForwardIdentity
	}
	node := got[security.HeaderForwardNode]
	if got[security.HeaderForwardCluster] != c.cfg.ClusterName ||
		got[security.HeaderForwardTarget] != c.localNode.ID || node == c.localNode.ID {
		return true, 0, security.ErrForwardIdentity
	}
	// Only a member of this cluster forwards (the same gate as forward-apply).
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return true, 0, security.ErrForwardIdentity
	}
	if _, ok := fsm.GetNode(node); !ok {
		return true, 0, security.ErrForwardIdentity
	}
	ts, err1 := strconv.ParseInt(got[security.HeaderForwardTimestamp], 10, 64)
	tokenID, err2 := strconv.ParseInt(got[security.HeaderForwardTokenID], 10, 64)
	if err1 != nil || err2 != nil || tokenID < 0 {
		return true, 0, security.ErrForwardIdentity
	}
	single := func(name string) string {
		v := values(name)
		if len(v) == 0 {
			return ""
		}
		return v[0]
	}
	for _, h := range []string{"X-Arc-Database", "X-Arc-Measurement", "Content-Type", "Content-Encoding"} {
		if len(values(h)) > 1 {
			return true, 0, security.ErrForwardIdentity
		}
	}
	f := security.ForwardIdentity{
		NodeID:          node,
		TargetID:        c.localNode.ID,
		Cluster:         c.cfg.ClusterName,
		TokenID:         tokenID,
		Method:          method,
		RequestURI:      requestURI,
		Database:        single("X-Arc-Database"),
		Measurement:     single("X-Arc-Measurement"),
		ContentType:     single("Content-Type"),
		ContentEncoding: single("Content-Encoding"),
		Timestamp:       ts,
		Nonce:           got[security.HeaderForwardNonce],
	}
	if security.ValidateForwardIdentityHMAC(c.cfg.SharedSecret, f, got[security.HeaderForwardHMAC], security.ForwardIdentityTolerance) != nil {
		return true, 0, security.ErrForwardIdentity
	}
	// After the MAC, so an unauthenticated sender cannot fill the cache.
	if !c.forwardNonces.Track(node, f.Nonce) {
		return true, 0, security.ErrForwardIdentity
	}
	return true, tokenID, nil
}

// ForwardTokenID returns the token ID WithForwardTokenID recorded, 0 if none.
func ForwardTokenID(ctx context.Context) int64 { return forwardTokenID(ctx) }
