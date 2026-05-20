package sharding

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/rs/zerolog"
)

// Errors returned by the shard router.
var (
	ErrNoDatabaseHeader   = errors.New("missing x-arc-database header")
	ErrNoShardPrimary     = errors.New("no primary for shard")
	ErrLocalNodeCanHandle = errors.New("local node can handle request")
	ErrShardingDisabled   = errors.New("sharding is not enabled")
)

// ShardRouter routes requests to the appropriate shard primary.
type ShardRouter struct {
	shardMap   *ShardMap
	localNode  *cluster.Node
	httpClient *http.Client
	scheme     string
	logger     zerolog.Logger

	// Stats
	forwardedWrites  atomic.Int64
	forwardedQueries atomic.Int64
	localWrites      atomic.Int64
	localQueries     atomic.Int64
	errors           atomic.Int64
}

// ShardRouterConfig holds configuration for the shard router.
type ShardRouterConfig struct {
	ShardMap  *ShardMap
	LocalNode *cluster.Node
	Timeout   time.Duration
	Logger    zerolog.Logger

	// Transport is the http.Transport to use for forwarded requests.
	// When nil, NewShardRouter builds a default plaintext transport via
	// security.NewClusterHTTPTransport(nil). Callers running with
	// server.tls_enabled OR cluster.tls_enabled MUST pass a TLS-aware
	// transport built via security.NewClusterHTTPTransport(tlsCfg);
	// otherwise inter-shard forwarding fails the TLS handshake at every
	// peer.
	Transport *http.Transport

	// Scheme is "http" or "https" — must match what the receiving
	// peer's Fiber listener serves (driven by server.tls_enabled,
	// identical on every cluster node). When empty, defaults to "http".
	Scheme string
}

// NewShardRouter creates a new shard router.
func NewShardRouter(cfg *ShardRouterConfig) *ShardRouter {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	scheme := cfg.Scheme
	if scheme == "" {
		scheme = "http"
	}

	transport := cfg.Transport
	if transport == nil {
		// Default pool settings (MaxIdleConns/MaxIdleConnsPerHost/
		// IdleConnTimeout) live as shared constants in security/tls.go
		// so any tuning change lands across every cluster-internal HTTP
		// client at once.
		transport = security.NewClusterHTTPTransport(nil)
	}

	return &ShardRouter{
		shardMap:  cfg.ShardMap,
		localNode: cfg.LocalNode,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		scheme: scheme,
		logger: cfg.Logger.With().Str("component", "shard-router").Logger(),
	}
}

// RouteWrite routes a write request to the appropriate shard primary.
// Returns ErrLocalNodeCanHandle if this node is the primary.
func (r *ShardRouter) RouteWrite(ctx context.Context, req *http.Request) (*http.Response, error) {
	// Extract database from header
	database := req.Header.Get("X-Arc-Database")
	if database == "" {
		database = req.Header.Get("x-arc-database")
	}
	if database == "" {
		r.errors.Add(1)
		return nil, ErrNoDatabaseHeader
	}

	// Get shard and primary
	shardID := r.shardMap.GetShard(database)
	primary := r.shardMap.GetPrimary(shardID)

	if primary == nil {
		r.errors.Add(1)
		r.logger.Warn().
			Int("shard_id", shardID).
			Str("database", database).
			Msg("No primary for shard")
		return nil, ErrNoShardPrimary
	}

	// Check if we're the primary
	if primary.ID == r.localNode.ID {
		r.localWrites.Add(1)
		return nil, ErrLocalNodeCanHandle
	}

	// Forward to primary
	r.logger.Debug().
		Int("shard_id", shardID).
		Str("database", database).
		Str("primary", primary.ID).
		Msg("Forwarding write to shard primary")

	resp, err := r.forward(ctx, primary, req)
	if err != nil {
		r.errors.Add(1)
		return nil, err
	}

	r.forwardedWrites.Add(1)
	return resp, nil
}

// RouteQuery routes a query request to an appropriate node in the shard.
// For single-database queries, routes to any node in the shard group.
// Returns ErrLocalNodeCanHandle if this node can serve the query.
func (r *ShardRouter) RouteQuery(ctx context.Context, database string, req *http.Request) (*http.Response, error) {
	if database == "" {
		r.errors.Add(1)
		return nil, ErrNoDatabaseHeader
	}

	// Get shard and select a node
	shardID := r.shardMap.GetShard(database)
	node := r.shardMap.SelectNode(shardID)

	if node == nil {
		// No healthy node, try primary
		node = r.shardMap.GetPrimary(shardID)
	}

	if node == nil {
		r.errors.Add(1)
		return nil, ErrNoShardPrimary
	}

	// Check if we can handle locally
	if node.ID == r.localNode.ID {
		r.localQueries.Add(1)
		return nil, ErrLocalNodeCanHandle
	}

	// Check if we're in the shard group at all
	allNodes := r.shardMap.GetAllNodes(shardID)
	for _, n := range allNodes {
		if n.ID == r.localNode.ID {
			// We're in the shard group, handle locally
			r.localQueries.Add(1)
			return nil, ErrLocalNodeCanHandle
		}
	}

	// Forward to selected node
	r.logger.Debug().
		Int("shard_id", shardID).
		Str("database", database).
		Str("target", node.ID).
		Msg("Forwarding query to shard node")

	resp, err := r.forward(ctx, node, req)
	if err != nil {
		r.errors.Add(1)
		return nil, err
	}

	r.forwardedQueries.Add(1)
	return resp, nil
}

// CanHandleLocally checks if this node can handle a request for a database.
// Returns the shard ID and whether this node is primary for it.
func (r *ShardRouter) CanHandleLocally(database string, isWrite bool) (shardID int, canHandle bool) {
	shardID = r.shardMap.GetShard(database)

	if isWrite {
		// Writes must go to primary
		primary := r.shardMap.GetPrimary(shardID)
		return shardID, primary != nil && primary.ID == r.localNode.ID
	}

	// Reads can go to any node in the shard group
	allNodes := r.shardMap.GetAllNodes(shardID)
	for _, n := range allNodes {
		if n.ID == r.localNode.ID {
			return shardID, true
		}
	}

	return shardID, false
}

// GetShardForDatabase returns the shard ID for a database.
func (r *ShardRouter) GetShardForDatabase(database string) int {
	return r.shardMap.GetShard(database)
}

// forward sends a request to another node.
func (r *ShardRouter) forward(ctx context.Context, node *cluster.Node, originalReq *http.Request) (*http.Response, error) {
	// Build target URL via *url.URL so an IPv6 literal in
	// node.APIAddress round-trips bracketed, and so paths with
	// percent-encoded bytes or non-ASCII characters land intact at
	// the peer (RawPath carries the on-the-wire encoding; Path is the
	// decoded form; url.URL.String() prefers RawPath when set). Scheme
	// is taken from ShardRouter.scheme so inter-shard forwarding hits
	// HTTPS peers when the cluster API serves TLS.
	targetURL := (&url.URL{
		Scheme:   r.scheme,
		Host:     node.APIAddress,
		Path:     originalReq.URL.Path,
		RawPath:  originalReq.URL.RawPath,
		RawQuery: originalReq.URL.RawQuery,
	}).String()

	// Read and buffer body for forwarding
	var bodyReader io.Reader
	if originalReq.Body != nil {
		bodyBytes, err := io.ReadAll(originalReq.Body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(bodyBytes)
		// Reset original body in case caller needs it
		originalReq.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	// Create new request
	req, err := http.NewRequestWithContext(ctx, originalReq.Method, targetURL, bodyReader)
	if err != nil {
		return nil, err
	}

	// Copy headers
	for key, values := range originalReq.Header {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	// Add forwarding header to prevent loops
	req.Header.Set("X-Arc-Forwarded-By", r.localNode.ID)
	req.Header.Set("X-Arc-Shard-Routed", "true")

	// Send request
	return r.httpClient.Do(req)
}

// Stats returns router statistics.
func (r *ShardRouter) Stats() map[string]interface{} {
	return map[string]interface{}{
		"forwarded_writes":  r.forwardedWrites.Load(),
		"forwarded_queries": r.forwardedQueries.Load(),
		"local_writes":      r.localWrites.Load(),
		"local_queries":     r.localQueries.Load(),
		"errors":            r.errors.Load(),
		"shard_map":         r.shardMap.Stats(),
	}
}

// ShardMap returns the underlying shard map.
func (r *ShardRouter) ShardMap() *ShardMap {
	return r.shardMap
}
