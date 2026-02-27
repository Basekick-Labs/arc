package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

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
}

// Router routes requests to appropriate nodes in the cluster.
// The coordinator node uses this to forward writes to writers and queries to readers.
type Router struct {
	cfg        *RouterConfig
	httpClient *http.Client
	logger     zerolog.Logger

	// Round-robin index for reader selection
	readerIndex atomic.Uint64

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

	r := &Router{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		logger:      cfg.Logger.With().Str("component", "cluster-router").Logger(),
		activeConns: make(map[string]*atomic.Int64),
	}

	return r
}

// RouteWrite routes a write request to an appropriate writer node.
// Returns ErrLocalNodeCanHandle if this node can process the write directly.
func (r *Router) RouteWrite(ctx context.Context, req *http.Request) (*http.Response, error) {
	// Check if local node can handle writes
	if r.cfg.LocalNode != nil && r.cfg.LocalNode.Role.GetCapabilities().CanIngest {
		return nil, ErrLocalNodeCanHandle
	}

	// Prefer the designated primary writer
	writer := r.cfg.Registry.GetPrimaryWriter()
	if writer == nil {
		// Fall back to any healthy writer (no failover configured or pre-promotion)
		writers := r.cfg.Registry.GetWriters()
		if len(writers) == 0 {
			r.logger.Warn().Msg("No healthy writer nodes available for routing")
			return nil, ErrNoWriterAvailable
		}
		writer = writers[0]
	}

	return r.forwardRequest(ctx, writer, req)
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

	// Build target URL
	targetURL := fmt.Sprintf("http://%s%s", node.APIAddress, originalReq.URL.Path)
	if originalReq.URL.RawQuery != "" {
		targetURL += "?" + originalReq.URL.RawQuery
	}

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
