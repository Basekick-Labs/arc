package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/basekick-labs/arc/internal/metrics"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
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

	// ErrWriteForwardUnavailable: a forwarded write could not reach the
	// writer (no connection was made), so nothing was written there; the
	// client may retry (#1270).
	ErrWriteForwardUnavailable = errors.New("the primary writer could not be reached; nothing was written, retry")

	// ErrWriteForwardAmbiguous: a forwarded write was sent but no answer came
	// back in time. The writer may have written it; a retry may duplicate
	// rows (#1270).
	ErrWriteForwardAmbiguous = errors.New("the forwarded write got no answer (timed out or the connection broke); it may or may not have been written")

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

	// Designated returns the writer the cluster designates as primary
	// (Pattern 1), "" when there is none (Pattern 2, no Raft). Writes are
	// forwarded to it rather than to the registry's view of writer states,
	// which a join can leave empty (#1267, #1270).
	Designated func() string

	// StandbyWriteTarget, when set, names the primary writer a write that
	// arrives at this standby writer should be forwarded to (#1270).
	StandbyWriteTarget func() (*Node, bool)

	// WriteTimeout bounds one forwarded write, upload and processing
	// included. Default 60 s: forwarded writes are never retried once sent.
	WriteTimeout time.Duration

	// ImportTimeout bounds one forwarded import, upload and processing
	// included; 0 = no bound (#1070). A backstop far above any import's
	// duration (cluster.import_forward_timeout_ms, default 1 h).
	ImportTimeout time.Duration

	// SharedSecret and ClusterName sign a forwarded request's identity
	// (forward_identity.go).
	SharedSecret string
	ClusterName  string

	// PeerForwardIdentity reports whether a peer authenticates forwarded
	// requests by their signed identity; nil, or false for a peer, forwards
	// the caller's credential as before (no Raft, or an older peer).
	PeerForwardIdentity func(nodeID string) bool
}

// Router routes requests to appropriate nodes in the cluster.
// The coordinator node uses this to forward writes to writers and queries to readers.
type Router struct {
	cfg          *RouterConfig
	httpClient   *http.Client
	writeClient  *http.Client
	importClient *http.Client
	logger       zerolog.Logger

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
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 60 * time.Second
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
		writeClient: &http.Client{
			Timeout:   cfg.WriteTimeout,
			Transport: writeTransport(transport),
		},
		// Not the write timeout: a local import has no server-side bound,
		// and a 500 MB import can outlast any write timeout. A 504 while the
		// writer is still importing would invite a re-run that duplicates
		// rows (#1070). ImportTimeout is only a backstop: a forward is not
		// cancelled when its client gives up (fasthttp ends a request's
		// context only at shutdown), so without it a writer that never
		// answers would hold this node's goroutine, connection and upload.
		// Dial and TLS handshake stay bounded; TCP keepalive ends a
		// connection to a writer that vanished, but only after minutes.
		importClient: &http.Client{
			Timeout:   cfg.ImportTimeout,
			Transport: writeTransport(transport),
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
	writer, err := r.writeTarget()
	if err != nil {
		return nil, err
	}
	return r.ForwardWriteTo(ctx, writer, req)
}

// RouteImport is RouteWrite for an import (/api/v1/import/*): the same
// writer, sent once, bounded by ImportTimeout instead of WriteTimeout (#1070).
func (r *Router) RouteImport(ctx context.Context, req *http.Request) (*http.Response, error) {
	writer, err := r.writeTarget()
	if err != nil {
		return nil, err
	}
	return r.ForwardImportTo(ctx, writer, req)
}

// writeTarget picks the writer a write from this node goes to, or
// ErrLocalNodeCanHandle when this node ingests itself.
func (r *Router) writeTarget() (*Node, error) {
	// Check if local node can handle writes
	if r.cfg.LocalNode != nil && r.cfg.LocalNode.Role.GetCapabilities().CanIngest {
		return nil, ErrLocalNodeCanHandle
	}

	// The designated primary (Pattern 1), then the registry's primary, then
	// round robin across healthy writers (Pattern 2, or the failover window).
	writer := r.designatedWriter()
	if writer == nil {
		writer = r.cfg.Registry.GetPrimaryWriter()
	}
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
	return writer, nil
}

// designatedWriter is the designated primary when it is a healthy writer
// this node can address.
func (r *Router) designatedWriter() *Node {
	if r.cfg.Designated == nil {
		return nil
	}
	id := r.cfg.Designated()
	if id == "" {
		return nil
	}
	n, ok := r.cfg.Registry.Get(id)
	if !ok || n.Role != RoleWriter || !n.IsHealthy() || n.APIAddress == "" {
		return nil
	}
	return n
}

// StandbyWriteTarget reports the primary writer a write arriving at this
// standby writer should be forwarded to (#1270). Nil-safe.
func (r *Router) StandbyWriteTarget() (*Node, bool) {
	if r == nil || r.cfg.StandbyWriteTarget == nil {
		return nil, false
	}
	return r.cfg.StandbyWriteTarget()
}

// ForwardWriteTo forwards a write to node once. A write is never retried once
// sent: the writer may have written it before an error or a timeout, and a
// retry would duplicate its rows (#1270). A connection that could not be made
// is ErrWriteForwardUnavailable (nothing was sent); a timeout or broken
// connection after sending is ErrWriteForwardAmbiguous.
func (r *Router) ForwardWriteTo(ctx context.Context, node *Node, req *http.Request) (*http.Response, error) {
	return r.forwardWriteOnce(ctx, r.writeClient, node, req)
}

// ForwardImportTo is ForwardWriteTo for an import, bounded by ImportTimeout
// instead of WriteTimeout (#1070).
func (r *Router) ForwardImportTo(ctx context.Context, node *Node, req *http.Request) (*http.Response, error) {
	return r.forwardWriteOnce(ctx, r.importClient, node, req)
}

func (r *Router) forwardWriteOnce(ctx context.Context, client *http.Client, node *Node, req *http.Request) (*http.Response, error) {
	// Whether any of the request left this node decides what a failure
	// means, not the error's type: a dead host shows up as a timeout, a TLS
	// failure as EOF.
	var sent atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteHeaders: func() { sent.Store(true) },
	})
	resp, err := r.doForwardOnce(ctx, client, node, req)
	switch {
	case err == nil:
		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			metrics.Get().IncWritesForwarded("ok")
		case resp.StatusCode == http.StatusServiceUnavailable && resp.Header.Get(IngestFencedResponseHeader) != "":
			metrics.Get().IncWritesForwarded("refused")
		default:
			metrics.Get().IncWritesForwarded("error")
		}
		return resp, nil
	case !sent.Load():
		metrics.Get().IncWritesForwarded("unavailable")
		r.logger.Debug().Err(err).Str("node_id", node.ID).Msg("Forwarded write could not reach the writer")
		return nil, fmt.Errorf("%w: %v", ErrWriteForwardUnavailable, err)
	default:
		metrics.Get().IncWritesForwarded("ambiguous")
		r.logger.Warn().Err(err).Str("node_id", node.ID).Msg("Forwarded write sent but unanswered; it may have been written")
		return nil, fmt.Errorf("%w: %v", ErrWriteForwardAmbiguous, err)
	}
}

// apiHostPort is the address to reach node's HTTP API at. A node that binds
// every interface advertises its API as ":<port>" (cmd/arc main.go): that
// has no host, and dialing it reaches this node itself. The host is then the
// one of its cluster coordinator address, which the cluster already dials
// (#1270: on the QA rig every forward looped back to the sender).
func apiHostPort(node *Node) string {
	host, port, err := net.SplitHostPort(node.APIAddress)
	if err != nil || (host != "" && host != "0.0.0.0" && host != "::") {
		return node.APIAddress
	}
	if ch, _, err := net.SplitHostPort(node.Address); err == nil && ch != "" {
		return net.JoinHostPort(ch, port)
	}
	return node.APIAddress
}

// IngestFencedResponseHeader is the header a writer sets on a 503 that
// refused a write it may not take (api.IngestFencedHeader; the packages do
// not import each other).
const IngestFencedResponseHeader = "X-Arc-Ingest-Fenced"

// writeForwardDialTimeout bounds opening a connection (and its TLS
// handshake) for a forwarded write: a dead writer must answer "unreachable,
// nothing written" quickly, not after the whole write timeout.
const writeForwardDialTimeout = 5 * time.Second

// writeTransport is base with dial and TLS handshake timeouts.
func writeTransport(base *http.Transport) *http.Transport {
	t := base.Clone()
	t.DialContext = (&net.Dialer{Timeout: writeForwardDialTimeout, KeepAlive: 30 * time.Second}).DialContext
	t.TLSHandshakeTimeout = writeForwardDialTimeout
	return t
}

// doForwardOnce sends req to node without buffering its body: a forwarded
// write is sent once, so the body is streamed as is (no second copy of a
// payload that can be large).
func (r *Router) doForwardOnce(ctx context.Context, client *http.Client, node *Node, originalReq *http.Request) (*http.Response, error) {
	r.incrementConns(node.ID)
	defer r.decrementConns(node.ID)
	targetURL := (&url.URL{
		Scheme:   r.cfg.Scheme,
		Host:     apiHostPort(node),
		Path:     originalReq.URL.Path,
		RawPath:  originalReq.URL.RawPath,
		RawQuery: originalReq.URL.RawQuery,
	}).String()
	forwardReq, err := http.NewRequestWithContext(ctx, originalReq.Method, targetURL, originalReq.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to create forward request: %w", err)
	}
	forwardReq.ContentLength = originalReq.ContentLength
	forwardReq.GetBody = originalReq.GetBody
	for key, values := range originalReq.Header {
		for _, value := range values {
			forwardReq.Header.Add(key, value)
		}
	}
	forwardReq.Header.Set("X-Forwarded-For", originalReq.RemoteAddr)
	forwardReq.Header.Set("X-Arc-Forwarded-By", r.cfg.LocalNode.ID)
	forwardReq.Header.Set("X-Arc-Original-Host", originalReq.Host)
	if err := r.signForward(forwardReq, node, forwardTokenID(originalReq.Context())); err != nil {
		return nil, fmt.Errorf("failed to sign forward request: %w", err)
	}
	return client.Do(forwardReq)
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

		resp, err := r.doForward(ctx, r.httpClient, node, req)
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
func (r *Router) doForward(ctx context.Context, client *http.Client, node *Node, originalReq *http.Request) (*http.Response, error) {
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
		Host:     apiHostPort(node),
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
	if err := r.signForward(forwardReq, node, forwardTokenID(originalReq.Context())); err != nil {
		return nil, fmt.Errorf("failed to sign forward request: %w", err)
	}

	// Execute the request
	start := time.Now()
	resp, err := client.Do(forwardReq)
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
		"writer_index":       r.writerIndex.Load(),
		"active_connections": connStats,
	}
}
