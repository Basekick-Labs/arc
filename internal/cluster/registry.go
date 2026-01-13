package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// DefaultMaxNodes is the default maximum number of nodes allowed in the registry.
const DefaultMaxNodes = 1000

// DefaultCallbackTimeout is the default timeout for callback execution.
const DefaultCallbackTimeout = 5 * time.Second

// NodeEventCallback is called when node events occur.
type NodeEventCallback func(*Node)

// Registry manages cluster node membership.
// It provides thread-safe access to the list of nodes in the cluster.
type Registry struct {
	nodes     map[string]*Node
	localNode *Node
	maxNodes  int
	mu        sync.RWMutex
	logger    zerolog.Logger

	// Event callbacks
	onNodeJoined    NodeEventCallback
	onNodeLeft      NodeEventCallback
	onNodeHealthy   NodeEventCallback
	onNodeUnhealthy NodeEventCallback
}

// RegistryConfig holds configuration for the node registry.
type RegistryConfig struct {
	LocalNode *Node
	MaxNodes  int
	Logger    zerolog.Logger
}

// NewRegistry creates a new node registry.
func NewRegistry(cfg *RegistryConfig) *Registry {
	maxNodes := cfg.MaxNodes
	if maxNodes <= 0 {
		maxNodes = DefaultMaxNodes
	}

	r := &Registry{
		nodes:     make(map[string]*Node),
		localNode: cfg.LocalNode,
		maxNodes:  maxNodes,
		logger:    cfg.Logger.With().Str("component", "cluster-registry").Logger(),
	}

	// Register local node if provided
	if cfg.LocalNode != nil {
		r.nodes[cfg.LocalNode.ID] = cfg.LocalNode
	}

	return r
}

// Register adds or updates a node in the registry.
// Returns ErrTooManyNodes if the registry has reached its maximum capacity.
func (r *Registry) Register(node *Node) error {
	r.mu.Lock()

	existing, exists := r.nodes[node.ID]

	// Check max nodes limit only for new nodes
	if !exists && len(r.nodes) >= r.maxNodes {
		r.mu.Unlock()
		return ErrTooManyNodes
	}

	r.nodes[node.ID] = node
	callback := r.onNodeJoined
	r.mu.Unlock()

	if !exists {
		r.logger.Info().
			Str("node_id", node.ID).
			Str("role", string(node.Role)).
			Str("address", node.Address).
			Msg("Node joined cluster")
		r.invokeCallback(callback, node)
	} else {
		r.logger.Debug().
			Str("node_id", node.ID).
			Str("old_state", string(existing.State)).
			Str("new_state", string(node.State)).
			Msg("Node updated")
	}

	return nil
}

// invokeCallback safely invokes a callback with timeout and panic recovery.
func (r *Registry) invokeCallback(callback NodeEventCallback, node *Node) {
	if callback == nil {
		return
	}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				r.logger.Error().Interface("panic", err).Str("node_id", node.ID).Msg("Callback panicked")
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), DefaultCallbackTimeout)
		defer cancel()

		done := make(chan struct{})
		go func() {
			callback(node)
			close(done)
		}()

		select {
		case <-done:
			// Callback completed successfully
		case <-ctx.Done():
			r.logger.Warn().Str("node_id", node.ID).Msg("Callback timed out")
		}
	}()
}

// Unregister removes a node from the registry.
func (r *Registry) Unregister(nodeID string) {
	r.mu.Lock()
	node, exists := r.nodes[nodeID]
	callback := r.onNodeLeft
	if exists {
		delete(r.nodes, nodeID)
	}
	r.mu.Unlock()

	if exists {
		r.logger.Info().
			Str("node_id", nodeID).
			Str("role", string(node.Role)).
			Msg("Node left cluster")
		r.invokeCallback(callback, node)
	}
}

// Get returns a clone of a node by ID.
// Returns a cloned node to prevent race conditions from concurrent modifications.
func (r *Registry) Get(nodeID string) (*Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, exists := r.nodes[nodeID]
	if !exists {
		return nil, false
	}
	return node.Clone(), true
}

// filterNodes returns cloned nodes that match the predicate.
// This is a helper to reduce code duplication across Get* methods.
func (r *Registry) filterNodes(predicate func(*Node) bool) []*Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var nodes []*Node
	for _, node := range r.nodes {
		if predicate(node) {
			nodes = append(nodes, node.Clone())
		}
	}
	return nodes
}

// countNodes returns the count of nodes that match the predicate.
func (r *Registry) countNodes(predicate func(*Node) bool) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, node := range r.nodes {
		if predicate(node) {
			count++
		}
	}
	return count
}

// GetAll returns cloned copies of all registered nodes.
func (r *Registry) GetAll() []*Node {
	return r.filterNodes(func(n *Node) bool { return true })
}

// GetByRole returns cloned nodes with a specific role.
func (r *Registry) GetByRole(role NodeRole) []*Node {
	return r.filterNodes(func(n *Node) bool { return n.Role == role })
}

// GetByState returns cloned nodes with a specific state.
func (r *Registry) GetByState(state NodeState) []*Node {
	return r.filterNodes(func(n *Node) bool { return n.GetState() == state })
}

// GetHealthy returns cloned copies of all healthy nodes.
func (r *Registry) GetHealthy() []*Node {
	return r.filterNodes(func(n *Node) bool { return n.IsHealthy() })
}

// GetAvailable returns cloned copies of all available nodes (healthy or joining).
func (r *Registry) GetAvailable() []*Node {
	return r.filterNodes(func(n *Node) bool { return n.IsAvailable() })
}

// GetWriters returns cloned copies of all healthy writer nodes.
func (r *Registry) GetWriters() []*Node {
	return r.filterNodes(func(n *Node) bool { return n.Role == RoleWriter && n.IsHealthy() })
}

// GetReaders returns cloned copies of all healthy reader nodes.
func (r *Registry) GetReaders() []*Node {
	return r.filterNodes(func(n *Node) bool { return n.Role == RoleReader && n.IsHealthy() })
}

// GetCompactors returns cloned copies of all healthy compactor nodes.
func (r *Registry) GetCompactors() []*Node {
	return r.filterNodes(func(n *Node) bool { return n.Role == RoleCompactor && n.IsHealthy() })
}

// Local returns the local node.
func (r *Registry) Local() *Node {
	return r.localNode
}

// Count returns the total number of registered nodes.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// CountByRole returns the number of nodes with a specific role.
func (r *Registry) CountByRole(role NodeRole) int {
	return r.countNodes(func(n *Node) bool { return n.Role == role })
}

// CountHealthy returns the number of healthy nodes.
func (r *Registry) CountHealthy() int {
	return r.countNodes(func(n *Node) bool { return n.IsHealthy() })
}

// SetCallbacks sets event callbacks for node lifecycle events.
// Callbacks are invoked asynchronously to prevent blocking the registry.
func (r *Registry) SetCallbacks(
	onJoined NodeEventCallback,
	onLeft NodeEventCallback,
	onHealthy NodeEventCallback,
	onUnhealthy NodeEventCallback,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onNodeJoined = onJoined
	r.onNodeLeft = onLeft
	r.onNodeHealthy = onHealthy
	r.onNodeUnhealthy = onUnhealthy
}

// NotifyHealthy notifies that a node became healthy.
func (r *Registry) NotifyHealthy(node *Node) {
	r.mu.RLock()
	callback := r.onNodeHealthy
	r.mu.RUnlock()

	r.invokeCallback(callback, node)
}

// NotifyUnhealthy notifies that a node became unhealthy.
func (r *Registry) NotifyUnhealthy(node *Node) {
	r.mu.RLock()
	callback := r.onNodeUnhealthy
	r.mu.RUnlock()

	r.invokeCallback(callback, node)
}

// Summary returns a summary of the registry state.
func (r *Registry) Summary() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	summary := map[string]int{
		"total":      len(r.nodes),
		"healthy":    0,
		"unhealthy":  0,
		"writers":    0,
		"readers":    0,
		"compactors": 0,
		"standalone": 0,
	}

	for _, node := range r.nodes {
		if node.IsHealthy() {
			summary["healthy"]++
		} else {
			summary["unhealthy"]++
		}

		switch node.Role {
		case RoleWriter:
			summary["writers"]++
		case RoleReader:
			summary["readers"]++
		case RoleCompactor:
			summary["compactors"]++
		case RoleStandalone:
			summary["standalone"]++
		}
	}

	return summary
}
