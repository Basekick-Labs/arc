package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/rs/zerolog"
)

// LoadBalanceStrategy defines how to select nodes for routing.
type LoadBalanceStrategy string

const (
	// LoadBalanceRoundRobin selects nodes in round-robin order.
	LoadBalanceRoundRobin LoadBalanceStrategy = "round_robin"

	// LoadBalanceLeastConnections selects the node with fewest active connections.
	LoadBalanceLeastConnections LoadBalanceStrategy = "least_connections"

	// LoadBalanceRandom selects a random healthy node.
	LoadBalanceRandom LoadBalanceStrategy = "random"
)

// Router errors.
var (
	// ErrNoWriterAvailable indicates no healthy writer node is available.
	ErrNoWriterAvailable = fmt.Errorf("no healthy writer node available")

	// ErrNoReaderAvailable indicates no healthy reader node is available.
	ErrNoReaderAvailable = fmt.Errorf("no healthy reader node available")

	// ErrRoutingFailed indicates the request could not be routed after all retries.
	ErrRoutingFailed = fmt.Errorf("routing failed after all retries")

	// ErrLocalNodeCanHandle indicates the local node can handle this request directly.
	ErrLocalNodeCanHandle = fmt.Errorf("local node can handle request")

	// ErrNotCoordinator indicates this node is not the cluster coordinator.
	ErrNotCoordinator = fmt.Errorf("this node is not the cluster coordinator")
)

// RouterConfig holds configuration for the request router.
type RouterConfig struct {
	// Timeout for forwarded requests
	Timeout time.Duration

	// Number of retries for failed forwards
	Retries int

	// Load balancing strategy
	Strategy LoadBalanceStrategy

	// Registry for looking up nodes
	Registry *Registry

	// LocalNode is this node's identity
	LocalNode *Node

	// Logger for routing events
	Logger zerolog.Logger

	// Transport is the http.Transport to use for forwarded requests.
	// When nil, NewRouter builds a default plain-HTTP transport
	// (back-compat). Callers that run with server.tls_enabled OR
	// cluster.tls_enabled MUST pass a TLS-aware transport built via
	// security.ClusterHTTPTransport; otherwise inter-node forwarding
	// fails the TLS handshake at every peer.
	Transport *http.Transport

	// Scheme is "http" or "https" — must match what the receiving
	// peer's Fiber listener serves (driven by server.tls_enabled,
	// identical on every cluster node). When empty, defaults to
	// "http" (back-compat).
	Scheme string
}

// Router routes requests to appropriate nodes in the cluster.
// The coordinator node uses this to forward writes to writers and queries to readers.
type Router struct {
	cfg        *RouterConfig
	httpClient *http.Client
	logger     zerolog.Logger

	// Round-robin index for reader selection
	readerIndex atomic.Uint64

	// Round-robin index for writer selection (Pattern 2 multi-writer:
	// when no primary writer is designated, distribute writes across
	// all healthy writers). Separate from readerIndex so reader query
	// rotation doesn't skew writer rotation when both happen
	// interleaved.
	writerIndex atomic.Uint64

	// Active connection counts per node (for least_connections strategy)
	activeConns   map[string]*atomic.Int64
	activeConnsMu sync.RWMutex
}

// NewRouter creates a new request router.
func NewRouter(cfg *RouterConfig) *Router {
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.Retries == 0 {
		cfg.Retries = 3
	}
	if cfg.Strategy == "" {
		cfg.Strategy = LoadBalanceRoundRobin
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "http"
	}

	transport := cfg.Transport
	if transport == nil {
		// Back-compat: tests and callers that don't pre-build a TLS
		// transport get the default plaintext one. Going through
		// NewClusterHTTPTransport keeps the pool defaults in one
		// place (see clusterHTTP* constants in security/tls.go).
		transport = security.NewClusterHTTPTransport(nil)
	}

	r := &Router{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout:   cfg.Timeout,
			Transport: transport,
		},
		logger:      cfg.Logger.With().Str("component", "cluster-router").Logger(),
		activeConns: make(map[string]*atomic.Int64),
	}

	return r
}

// RouteWrite routes a write request to an appropriate writer node.
// Returns ErrLocalNodeCanHandle if this node can process the write directly.
//
// Writer selection (in order of preference):
//  1. GetPrimaryWriter() — the failover-promoted primary in legacy mode
//     (Pattern 1 single-writer + failover). Returns nil in Pattern 2
//     shared-storage multi-writer mode where no node holds WriterStatePrimary.
//  2. Round-robin across all healthy writers via selectWriter (the
//     Pattern 2 path; also a strict improvement in legacy mode during
//     the failover window where GetPrimaryWriter() can return nil).
func (r *Router) RouteWrite(ctx context.Context, req *http.Request) (*http.Response, error) {
	// Check if local node can handle writes
	if r.cfg.LocalNode != nil && r.cfg.LocalNode.Role.GetCapabilities().CanIngest {
		return nil, ErrLocalNodeCanHandle
	}

	// Prefer the designated primary writer (legacy single-writer + failover).
	writer := r.cfg.Registry.GetPrimaryWriter()
	if writer == nil {
		// Fall back to round-robin across healthy writers. In Pattern 2
		// multi-writer mode this is the always-taken path (no primary
		// designation); in legacy mode it covers the transient failover
		// window. Either way, distributing across N writers is correct;
		// the previous behaviour of always picking writers[0] defeated
		// the load-distribution that the rest of the cluster expects.
		writers := r.cfg.Registry.GetWriters()
		if len(writers) == 0 {
			r.logger.Warn().Msg("No healthy writer nodes available for routing")
			return nil, ErrNoWriterAvailable
		}
		writer = r.selectWriter(writers)
	}

	return r.forwardRequest(ctx, writer, req)
}

// selectWriter applies the configured load-balance strategy to a
// list of writer nodes. Mirrors selectNode but uses writerIndex so
// the rotation isn't perturbed by interleaved reader queries.
func (r *Router) selectWriter(nodes []*Node) *Node {
	if len(nodes) == 0 {
		return nil
	}
	if len(nodes) == 1 {
		return nodes[0]
	}
	switch r.cfg.Strategy {
	case LoadBalanceLeastConnections:
		return r.selectLeastConnections(nodes)
	case LoadBalanceRoundRobin:
		fallthrough
	default:
		idx := r.writerIndex.Add(1) - 1
		return nodes[idx%uint64(len(nodes))]
	}
}

// RouteQuery routes a query request to an appropriate reader node.
// Returns ErrLocalNodeCanHandle if this node can process the query directly.
func (r *Router) RouteQuery(ctx context.Context, req *http.Request) (*http.Response, error) {
	// Check if local node can handle queries
	if r.cfg.LocalNode != nil && r.cfg.LocalNode.Role.GetCapabilities().CanQuery {
		return nil, ErrLocalNodeCanHandle
	}

	// Find healthy readers
	readers := r.cfg.Registry.GetReaders()
	if len(readers) == 0 {
		// Fall back to writers if no readers (writers can also query in standalone/small clusters)
		writers := r.cfg.Registry.GetWriters()
		if len(writers) == 0 {
			r.logger.Warn().Msg("No healthy reader or writer nodes available for routing")
			return nil, ErrNoReaderAvailable
		}
		readers = writers
	}

	// Select reader based on strategy
	reader := r.selectNode(readers)
	if reader == nil {
		return nil, ErrNoReaderAvailable
	}

	return r.forwardRequest(ctx, reader, req)
}

// selectNode selects a node from the list based on the configured strategy.
func (r *Router) selectNode(nodes []*Node) *Node {
	if len(nodes) == 0 {
		return nil
	}
	if len(nodes) == 1 {
		return nodes[0]
	}

	switch r.cfg.Strategy {
	case LoadBalanceLeastConnections:
		return r.selectLeastConnections(nodes)
	case LoadBalanceRoundRobin:
		fallthrough
	default:
		return r.selectRoundRobin(nodes)
	}
}

// selectRoundRobin selects nodes in round-robin order.
func (r *Router) selectRoundRobin(nodes []*Node) *Node {
	idx := r.readerIndex.Add(1) - 1
	return nodes[idx%uint64(len(nodes))]
}

// selectLeastConnections selects the node with the fewest active connections.
func (r *Router) selectLeastConnections(nodes []*Node) *Node {
	r.activeConnsMu.RLock()
	defer r.activeConnsMu.RUnlock()

	var selected *Node
	var minConns int64 = -1

	for _, node := range nodes {
		conns, exists := r.activeConns[node.ID]
		var count int64 = 0
		if exists {
			count = conns.Load()
		}

		if minConns < 0 || count < minConns {
			minConns = count
			selected = node
		}
	}

	return selected
}

// forwardRequest forwards an HTTP request to the specified node.
func (r *Router) forwardRequest(ctx context.Context, node *Node, req *http.Request) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= r.cfg.Retries; attempt++ {
		if attempt > 0 {
			r.logger.Debug().
				Int("attempt", attempt).
				Str("node_id", node.ID).
				Msg("Retrying request forward")
		}

		resp, err := r.doForward(ctx, node, req)
		if err == nil {
			return resp, nil
		}

		lastErr = err
		r.logger.Warn().
			Err(err).
			Int("attempt", attempt).
			Str("node_id", node.ID).
			Msg("Forward attempt failed")

		// Check if context is cancelled
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("%w: %v", ErrRoutingFailed, lastErr)
}

// doForward performs a single forward attempt to the specified node.
func (r *Router) doForward(ctx context.Context, node *Node, originalReq *http.Request) (*http.Response, error) {
	// Track active connections for least_connections strategy
	r.incrementConns(node.ID)
	defer r.decrementConns(node.ID)

	// Build target URL via *url.URL so an IPv6 literal in
	// node.APIAddress (e.g. "[::1]:8000") survives unmangled. We don't
	// rely on the upstream contract that ListenAddr always brackets
	// IPv6 — using url.URL is correct by construction and the one
	// allocation per request is dwarfed by the network round-trip.
	// RawPath carries the original on-the-wire encoding (RawPath !=
	// "" only when the path required escaping); Path is the decoded
	// form. url.URL.String() prefers RawPath when set, so paths
	// containing spaces or non-ASCII bytes are forwarded intact.
	targetURL := (&url.URL{
		Scheme:   r.cfg.Scheme,
		Host:     node.APIAddress,
		Path:     originalReq.URL.Path,
		RawPath:  originalReq.URL.RawPath,
		RawQuery: originalReq.URL.RawQuery,
	}).String()

	// Read and buffer the body so it can be retried
	var bodyBytes []byte
	if originalReq.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(originalReq.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body: %w", err)
		}
		// Reset the original request body for potential retries
		originalReq.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	// Create forwarded request
	forwardReq, err := http.NewRequestWithContext(ctx, originalReq.Method, targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create forward request: %w", err)
	}

	// Copy headers
	for key, values := range originalReq.Header {
		for _, value := range values {
			forwardReq.Header.Add(key, value)
		}
	}

	// Add forwarding headers
	forwardReq.Header.Set("X-Forwarded-For", originalReq.RemoteAddr)
	forwardReq.Header.Set("X-Arc-Forwarded-By", r.cfg.LocalNode.ID)
	forwardReq.Header.Set("X-Arc-Original-Host", originalReq.Host)

	// Execute the request
	start := time.Now()
	resp, err := r.httpClient.Do(forwardReq)
	elapsed := time.Since(start)

	if err != nil {
		r.logger.Error().
			Err(err).
			Str("node_id", node.ID).
			Str("target", targetURL).
			Dur("elapsed", elapsed).
			Msg("Forward request failed")
		return nil, err
	}

	r.logger.Debug().
		Str("node_id", node.ID).
		Str("target", targetURL).
		Int("status", resp.StatusCode).
		Dur("elapsed", elapsed).
		Msg("Forward request completed")

	return resp, nil
}

// incrementConns increments the active connection count for a node.
func (r *Router) incrementConns(nodeID string) {
	r.activeConnsMu.Lock()
	counter, exists := r.activeConns[nodeID]
	if !exists {
		counter = &atomic.Int64{}
		r.activeConns[nodeID] = counter
	}
	r.activeConnsMu.Unlock()
	counter.Add(1)
}

// decrementConns decrements the active connection count for a node.
// Prunes the map entry when the count drops to zero to prevent unbounded growth.
func (r *Router) decrementConns(nodeID string) {
	r.activeConnsMu.Lock()
	counter, exists := r.activeConns[nodeID]
	if exists {
		if counter.Add(-1) <= 0 {
			delete(r.activeConns, nodeID)
		}
	}
	r.activeConnsMu.Unlock()
}

// GetActiveConnections returns the number of active connections to a node.
func (r *Router) GetActiveConnections(nodeID string) int64 {
	r.activeConnsMu.RLock()
	defer r.activeConnsMu.RUnlock()
	counter, exists := r.activeConns[nodeID]
	if !exists {
		return 0
	}
	return counter.Load()
}

// CanRouteLocally returns true if the local node can handle the given request type.
func (r *Router) CanRouteLocally(isWrite bool) bool {
	if r.cfg.LocalNode == nil {
		return false
	}
	caps := r.cfg.LocalNode.Role.GetCapabilities()
	if isWrite {
		return caps.CanIngest
	}
	return caps.CanQuery
}

// Stats returns routing statistics.
func (r *Router) Stats() map[string]interface{} {
	r.activeConnsMu.RLock()
	defer r.activeConnsMu.RUnlock()

	connStats := make(map[string]int64)
	for nodeID, counter := range r.activeConns {
		connStats[nodeID] = counter.Load()
	}

	return map[string]interface{}{
		"strategy":           r.cfg.Strategy,
		"timeout_ms":         r.cfg.Timeout.Milliseconds(),
		"retries":            r.cfg.Retries,
		"reader_index":       r.readerIndex.Load(),
		"active_connections": connStats,
	}
}
