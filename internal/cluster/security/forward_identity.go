package security

import (
	"encoding/hex"
	"errors"
	"hash/maphash"
	"strconv"
	"sync"
	"time"
)

// Forwarded-request identity (#1305, #504 item 2).
//
// A node that forwards a client's request to a peer (a write from a reader to
// the writer, an import from a standby, a query from a compactor) used to copy
// the client's credential onto the inter-node hop, so every peer it forwarded
// to saw the raw token. The forwarding node has already verified that token;
// it now strips the credential and asserts the caller's token ID instead,
// signed with the cluster's shared secret. The receiving node checks the
// assertion and authorizes the request as that token (its permissions and
// RBAC grants, which Raft replicates to every node under the same ID).
//
// The MAC binds who forwarded it and to whom, the cluster, the token (ID
// and fingerprint), the method and request URI as sent, the database and
// measurement headers, and the content headers that decide how the body is
// decoded. The body itself (a query's SQL, a write's rows) is not bound: imports
// stream multi-gigabyte bodies that would have to be buffered to hash, and
// substituting a body needs an active man-in-the-middle on the hop, which the
// cluster's TLS settings address. A short tolerance with a replay cache keeps
// a captured assertion from being used again.

// MsgTypeForwardRequest labels the forwarded-request MAC; no other cluster
// MAC is computed under it.
const MsgTypeForwardRequest MsgType = "forward-request"

// ForwardIdentityTolerance is how far a forwarded request's timestamp may be
// from the receiver's clock. A forward is signed when it is sent, so this
// only needs to absorb clock skew and the hop itself; short, so the replay
// cache holding every nonce for 2×tolerance+1s stays small at write rates.
const ForwardIdentityTolerance = 30 * time.Second

// Headers carrying the assertion. A request with ANY of them is treated as
// forwarded and must carry all of them, each exactly once.
const (
	HeaderForwardNode        = "X-Arc-Forward-Node"
	HeaderForwardTarget      = "X-Arc-Forward-Target"
	HeaderForwardCluster     = "X-Arc-Forward-Cluster"
	HeaderForwardTokenID     = "X-Arc-Forward-Token-Id"
	HeaderForwardTokenFP     = "X-Arc-Forward-Token-Fp"
	HeaderForwardTimestamp   = "X-Arc-Forward-Timestamp"
	HeaderForwardNonce       = "X-Arc-Forward-Nonce"
	HeaderForwardHMAC        = "X-Arc-Forward-Hmac"
	ForwardIdentityHeaderPfx = "X-Arc-Forward-"
)

// ForwardIdentityHeaders lists every assertion header.
var ForwardIdentityHeaders = []string{
	HeaderForwardNode, HeaderForwardTarget, HeaderForwardCluster, HeaderForwardTokenID, HeaderForwardTokenFP,
	HeaderForwardTimestamp, HeaderForwardNonce, HeaderForwardHMAC,
}

// ForwardIdentity is what a forwarding node asserts about a request.
type ForwardIdentity struct {
	NodeID   string // the forwarding node
	TargetID string // the node it is sent to
	Cluster  string
	TokenID  int64 // the caller's token; 0 when auth is off on the forwarder
	// TokenFP is the token's fingerprint (auth.TokenInfo.Fingerprint): the
	// ID alone can name a different token on a node that kept a pre-26.06.1
	// local row at that ID.
	TokenFP    string
	Method     string
	RequestURI string // path and query exactly as sent
	// Headers the receiver acts on.
	Database        string // x-arc-database
	Measurement     string // x-arc-measurement
	ContentType     string
	ContentEncoding string
	Timestamp       int64
	Nonce           string
}

func (f ForwardIdentity) fields() []string {
	return []string{
		f.NodeID, f.TargetID, f.Cluster, strconv.FormatInt(f.TokenID, 10), f.TokenFP,
		f.Method, f.RequestURI, f.Database, f.Measurement, f.ContentType, f.ContentEncoding,
		strconv.FormatInt(f.Timestamp, 10), f.Nonce,
	}
}

// ComputeForwardIdentityHMAC signs a forwarded request's identity.
func ComputeForwardIdentityHMAC(sharedSecret string, f ForwardIdentity) string {
	return hex.EncodeToString(computeHandshakeMACRaw(sharedSecret, MsgTypeForwardRequest, f.fields()...))
}

// ErrForwardIdentity is returned for any assertion that does not verify; the
// receiver answers 401 without saying which check failed.
var ErrForwardIdentity = errors.New("security: forwarded request identity rejected")

// ValidateForwardIdentityHMAC checks freshness and the MAC. Replay is the
// caller's ForwardNonceCache.
func ValidateForwardIdentityHMAC(sharedSecret string, f ForwardIdentity, receivedMAC string, tolerance time.Duration) error {
	if sharedSecret == "" {
		return ErrForwardIdentity
	}
	if err := validateHandshakeMAC(sharedSecret, MsgTypeForwardRequest, f.Timestamp, tolerance, receivedMAC, f.fields()...); err != nil {
		return ErrForwardIdentity
	}
	return nil
}

// ForwardNonceCache rejects a forwarded request's nonce seen before, for as
// long as its MAC could still be accepted (2×tolerance+1s; see
// NewNonceCache). Built for a nonce per forwarded write: entries live in two
// generations per shard that rotate as a whole, so expiry costs one map
// swap instead of a sweep, and shards keep concurrent writes apart.
type ForwardNonceCache struct {
	shards [forwardNonceShards]forwardNonceShard
	ttl    time.Duration
	seed   maphash.Seed
	now    func() time.Time
}

const forwardNonceShards = 32

type forwardNonceShard struct {
	mu        sync.Mutex
	cur, prev map[forwardNonceKey]struct{}
	rotated   time.Time
}

// forwardNonceKey is a node's nonce: a 64-bit hash of the node ID and the
// nonce's 32 bytes. Fixed size and pointer-free, so a cache of a million
// entries is ~40 MB the garbage collector never scans.
type forwardNonceKey struct {
	node  uint64
	nonce [32]byte
}

// NewForwardNonceCache builds a cache for MACs accepted within tolerance.
func NewForwardNonceCache(tolerance time.Duration) *ForwardNonceCache {
	c := &ForwardNonceCache{ttl: 2*tolerance + time.Second, seed: maphash.MakeSeed(), now: time.Now}
	now := c.now()
	for i := range c.shards {
		c.shards[i].cur = make(map[forwardNonceKey]struct{})
		c.shards[i].rotated = now
	}
	return c
}

// Track records a nonce and reports whether it is new. A nil cache, or a
// nonce that is not 64 hex characters (GenerateNonce), reports false, so
// either fails closed.
//
// Why every entry lives at least ttl: a shard rotates only on a Track at
// least ttl after its previous rotation r, so cur only ever holds entries
// added in [r, r+ttl). At the next rotation r' (>= every such entry) they move
// to prev, where they stay until the rotation after, at least r'+ttl. When a
// shard was idle for 2×ttl, cur's entries are all older than ttl and both
// generations are dropped.
func (c *ForwardNonceCache) Track(nodeID, nonce string) bool {
	if c == nil {
		return false
	}
	var key forwardNonceKey
	if len(nonce) != 64 {
		return false
	}
	if _, err := hex.Decode(key.nonce[:], []byte(nonce)); err != nil {
		return false
	}
	key.node = maphash.String(c.seed, nodeID)
	s := &c.shards[(key.node^uint64(key.nonce[0]))%forwardNonceShards]
	now := c.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if since := now.Sub(s.rotated); since >= c.ttl {
		if since >= 2*c.ttl {
			s.prev = nil // both generations are older than ttl
		} else {
			s.prev = s.cur
		}
		s.cur = make(map[forwardNonceKey]struct{}, len(s.cur))
		s.rotated = now
	}
	if _, ok := s.cur[key]; ok {
		return false
	}
	if _, ok := s.prev[key]; ok {
		return false
	}
	s.cur[key] = struct{}{}
	return true
}
