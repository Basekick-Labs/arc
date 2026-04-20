package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// CommandType represents the type of FSM command.
type CommandType uint8

const (
	// CommandAddNode adds a node to the cluster.
	CommandAddNode CommandType = iota + 1
	// CommandRemoveNode removes a node from the cluster.
	CommandRemoveNode
	// CommandUpdateNode updates node information.
	CommandUpdateNode
	// CommandUpdateNodeState updates a node's state.
	CommandUpdateNodeState
	// CommandPromoteWriter promotes a writer node to primary.
	CommandPromoteWriter
	// CommandDemoteWriter demotes a writer node to standby.
	CommandDemoteWriter
	// CommandRegisterFile announces a newly written file to the cluster manifest.
	CommandRegisterFile
	// CommandDeleteFile removes a file from the cluster manifest.
	CommandDeleteFile
	// CommandAssignCompactor designates a node as the active compactor.
	// Used by the CompactorFailoverManager for automatic failover.
	CommandAssignCompactor
	// CommandBatchFileOps groups multiple RegisterFile and DeleteFile operations
	// into a single Raft log entry — one apply per compaction manifest instead of O(N).
	CommandBatchFileOps
	// CommandUpdateFile replaces an existing file's metadata in the cluster manifest.
	// Used after partial rewrites that change size/checksum but keep the same path.
	CommandUpdateFile
)

// Command represents a command to be applied to the FSM.
type Command struct {
	Type    CommandType `json:"type"`
	Payload []byte      `json:"payload"`
}

// NodeInfo represents node information stored in the FSM.
type NodeInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Role        string `json:"role"`
	ClusterName string `json:"cluster_name"`
	Address     string `json:"address"`
	APIAddress  string `json:"api_address"`
	State       string `json:"state"`
	Version     string `json:"version"`
	WriterState string `json:"writer_state,omitempty"` // "primary", "standby", or "" for non-writers
	CoreCount   int    `json:"core_count"`             // Number of CPU cores on this node
}

// AddNodePayload is the payload for CommandAddNode.
type AddNodePayload struct {
	Node NodeInfo `json:"node"`
}

// RemoveNodePayload is the payload for CommandRemoveNode.
type RemoveNodePayload struct {
	NodeID string `json:"node_id"`
}

// UpdateNodePayload is the payload for CommandUpdateNode.
type UpdateNodePayload struct {
	Node NodeInfo `json:"node"`
}

// UpdateNodeStatePayload is the payload for CommandUpdateNodeState.
type UpdateNodeStatePayload struct {
	NodeID   string `json:"node_id"`
	NewState string `json:"new_state"`
}

// PromoteWriterPayload is the payload for CommandPromoteWriter.
type PromoteWriterPayload struct {
	NodeID       string `json:"node_id"`        // Node to promote to primary
	OldPrimaryID string `json:"old_primary_id"` // Previous primary (to demote)
}

// DemoteWriterPayload is the payload for CommandDemoteWriter.
type DemoteWriterPayload struct {
	NodeID string `json:"node_id"` // Node to demote to standby
}

// FileEntry represents a single Parquet file in the cluster-wide manifest.
// This is the authoritative record of a file's existence, used by peer
// replication to decide what to pull from other nodes.
type FileEntry struct {
	Path          string    `json:"path"`           // Relative storage path (e.g. "db/measurement/2026/04/11/14/file.parquet")
	SHA256        string    `json:"sha256"`         // Content checksum for verification
	SizeBytes     int64     `json:"size_bytes"`     // File size
	Database      string    `json:"database"`       // Arc database name
	Measurement   string    `json:"measurement"`    // Arc measurement name
	PartitionTime time.Time `json:"partition_time"` // Partition time (for hot/cold routing)
	OriginNodeID  string    `json:"origin_node_id"` // Node that first wrote the file
	Tier          string    `json:"tier"`           // "hot" or "cold"
	CreatedAt     time.Time `json:"created_at"`     // When the file was first registered
	LSN           uint64    `json:"lsn"`            // Raft log index at registration (for ordering)
}

// RegisterFilePayload is the payload for CommandRegisterFile.
type RegisterFilePayload struct {
	File FileEntry `json:"file"`
}

// DeleteFilePayload is the payload for CommandDeleteFile.
type DeleteFilePayload struct {
	Path   string `json:"path"`
	Reason string `json:"reason,omitempty"` // "retention", "compaction", "manual"
}

// UpdateFilePayload is the payload for CommandUpdateFile.
type UpdateFilePayload struct {
	File FileEntry `json:"file"`
}

// AssignCompactorPayload is the payload for CommandAssignCompactor.
type AssignCompactorPayload struct {
	NodeID         string `json:"node_id"`
	OldCompactorID string `json:"old_compactor_id,omitempty"`
}

// BatchFileOp is a single operation within a CommandBatchFileOps command.
// Type must be CommandRegisterFile, CommandDeleteFile, or CommandUpdateFile;
// any other value causes the FSM to return an error when the batch is applied.
type BatchFileOp struct {
	Type    CommandType `json:"type"`    // CommandRegisterFile, CommandDeleteFile, or CommandUpdateFile
	Payload []byte      `json:"payload"` // Same payload shape as the corresponding single command
}

// BatchFileOpsPayload is the payload for CommandBatchFileOps.
type BatchFileOpsPayload struct {
	Ops []BatchFileOp `json:"ops"`
}

// FSMSnapshot represents a snapshot of the FSM state.
type FSMSnapshot struct {
	Nodes              map[string]*NodeInfo  `json:"nodes"`
	PrimaryWriterID    string                `json:"primary_writer_id,omitempty"`
	ActiveCompactorID  string                `json:"active_compactor_id,omitempty"`
	Files              map[string]*FileEntry `json:"files,omitempty"` // File manifest (peer replication)
}

// ClusterFSM implements the raft.FSM interface for cluster state management.
// It maintains the authoritative state of nodes in the cluster.
type ClusterFSM struct {
	mu              sync.RWMutex
	nodes           map[string]*NodeInfo
	primaryWriterID   string                // ID of the current primary writer node
	activeCompactorID string                // ID of the node currently holding the compactor lease
	files             map[string]*FileEntry // File manifest (path → entry) for peer replication
	// Secondary index: database → set of file paths. Maintained alongside
	// the primary `files` map on register/delete to avoid O(N) scans when
	// filtering by database.
	filesByDB map[string]map[string]struct{}
	logger    zerolog.Logger

	// Callbacks for state changes
	onNodeAdded      func(*NodeInfo)
	onNodeRemoved    func(string)
	onNodeUpdated    func(*NodeInfo)
	onWriterPromoted   func(newPrimaryID, oldPrimaryID string)
	onCompactorAssigned func(newCompactorID, oldCompactorID string)
	onFileRegistered   func(*FileEntry)
	onFileDeleted      func(path string, reason string)
}

// NewClusterFSM creates a new cluster FSM.
func NewClusterFSM(logger zerolog.Logger) *ClusterFSM {
	return &ClusterFSM{
		nodes:     make(map[string]*NodeInfo),
		files:     make(map[string]*FileEntry),
		filesByDB: make(map[string]map[string]struct{}),
		logger:    logger.With().Str("component", "cluster-fsm").Logger(),
	}
}

// SetCallbacks sets the FSM callbacks for state changes.
func (f *ClusterFSM) SetCallbacks(onAdded func(*NodeInfo), onRemoved func(string), onUpdated func(*NodeInfo)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onNodeAdded = onAdded
	f.onNodeRemoved = onRemoved
	f.onNodeUpdated = onUpdated
}

// SetWriterPromotedCallback sets the callback for writer promotion events.
func (f *ClusterFSM) SetWriterPromotedCallback(cb func(newPrimaryID, oldPrimaryID string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onWriterPromoted = cb
}

// SetCompactorAssignedCallback sets the callback for compactor assignment events.
func (f *ClusterFSM) SetCompactorAssignedCallback(cb func(newCompactorID, oldCompactorID string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onCompactorAssigned = cb
}

// GetActiveCompactorID returns the node ID currently holding the compactor lease.
func (f *ClusterFSM) GetActiveCompactorID() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.activeCompactorID
}

// SetFileCallbacks sets the callbacks for file manifest events (peer replication).
func (f *ClusterFSM) SetFileCallbacks(onRegistered func(*FileEntry), onDeleted func(path, reason string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onFileRegistered = onRegistered
	f.onFileDeleted = onDeleted
}

// GetPrimaryWriterID returns the current primary writer node ID.
func (f *ClusterFSM) GetPrimaryWriterID() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.primaryWriterID
}

// Apply applies a Raft log entry to the FSM.
// This is called by Raft when a log entry is committed.
func (f *ClusterFSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error().Err(err).Msg("Failed to unmarshal command")
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	switch cmd.Type {
	case CommandAddNode:
		return f.applyAddNode(cmd.Payload)
	case CommandRemoveNode:
		return f.applyRemoveNode(cmd.Payload)
	case CommandUpdateNode:
		return f.applyUpdateNode(cmd.Payload)
	case CommandUpdateNodeState:
		return f.applyUpdateNodeState(cmd.Payload)
	case CommandPromoteWriter:
		return f.applyPromoteWriter(cmd.Payload)
	case CommandDemoteWriter:
		return f.applyDemoteWriter(cmd.Payload)
	case CommandRegisterFile:
		return f.applyRegisterFile(cmd.Payload, log.Index)
	case CommandDeleteFile:
		return f.applyDeleteFile(cmd.Payload)
	case CommandAssignCompactor:
		return f.applyAssignCompactor(cmd.Payload)
	case CommandBatchFileOps:
		return f.applyBatchFileOps(cmd.Payload, log.Index)
	case CommandUpdateFile:
		return f.applyUpdateFile(cmd.Payload)
	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

func (f *ClusterFSM) applyAddNode(payload []byte) interface{} {
	var p AddNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal add node payload: %w", err)
	}

	f.mu.Lock()
	f.nodes[p.Node.ID] = &p.Node
	callback := f.onNodeAdded
	f.mu.Unlock()

	f.logger.Info().
		Str("node_id", p.Node.ID).
		Str("role", p.Node.Role).
		Msg("Node added to cluster state")

	if callback != nil {
		callback(&p.Node)
	}

	return nil
}

func (f *ClusterFSM) applyRemoveNode(payload []byte) interface{} {
	var p RemoveNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal remove node payload: %w", err)
	}

	f.mu.Lock()
	delete(f.nodes, p.NodeID)
	callback := f.onNodeRemoved
	f.mu.Unlock()

	f.logger.Info().
		Str("node_id", p.NodeID).
		Msg("Node removed from cluster state")

	if callback != nil {
		callback(p.NodeID)
	}

	return nil
}

func (f *ClusterFSM) applyUpdateNode(payload []byte) interface{} {
	var p UpdateNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update node payload: %w", err)
	}

	f.mu.Lock()
	f.nodes[p.Node.ID] = &p.Node
	callback := f.onNodeUpdated
	f.mu.Unlock()

	f.logger.Debug().
		Str("node_id", p.Node.ID).
		Msg("Node updated in cluster state")

	if callback != nil {
		callback(&p.Node)
	}

	return nil
}

func (f *ClusterFSM) applyUpdateNodeState(payload []byte) interface{} {
	var p UpdateNodeStatePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update node state payload: %w", err)
	}

	f.mu.Lock()
	node, exists := f.nodes[p.NodeID]
	if exists {
		node.State = p.NewState
	}
	callback := f.onNodeUpdated
	f.mu.Unlock()

	if !exists {
		return fmt.Errorf("node %s not found", p.NodeID)
	}

	f.logger.Debug().
		Str("node_id", p.NodeID).
		Str("new_state", p.NewState).
		Msg("Node state updated")

	if callback != nil {
		callback(node)
	}

	return nil
}

func (f *ClusterFSM) applyPromoteWriter(payload []byte) interface{} {
	var p PromoteWriterPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal promote writer payload: %w", err)
	}

	if p.NodeID == "" {
		return fmt.Errorf("promote writer: node_id is required")
	}

	f.mu.Lock()
	// Validate the node exists and is a writer
	if node, exists := f.nodes[p.NodeID]; exists && node.Role != "writer" {
		f.mu.Unlock()
		return fmt.Errorf("promote writer: node %s has role %s, expected writer", p.NodeID, node.Role)
	}

	// Warn if OldPrimaryID doesn't match actual primary (informational only — FSM uses its own tracking)
	oldPrimaryID := f.primaryWriterID
	if p.OldPrimaryID != "" && oldPrimaryID != "" && p.OldPrimaryID != oldPrimaryID {
		f.logger.Warn().
			Str("expected_old_primary", p.OldPrimaryID).
			Str("actual_old_primary", oldPrimaryID).
			Msg("OldPrimaryID mismatch during promotion")
	}
	if oldPrimaryID != "" && oldPrimaryID != p.NodeID {
		if oldNode, exists := f.nodes[oldPrimaryID]; exists {
			oldNode.WriterState = "standby"
		}
	}

	// Promote new primary
	newNode, exists := f.nodes[p.NodeID]
	if exists {
		newNode.WriterState = "primary"
	}
	f.primaryWriterID = p.NodeID
	callback := f.onWriterPromoted
	f.mu.Unlock()

	if !exists {
		return fmt.Errorf("node %s not found", p.NodeID)
	}

	f.logger.Info().
		Str("new_primary", p.NodeID).
		Str("old_primary", oldPrimaryID).
		Msg("Writer promoted to primary")

	if callback != nil {
		callback(p.NodeID, oldPrimaryID)
	}

	return nil
}

func (f *ClusterFSM) applyDemoteWriter(payload []byte) interface{} {
	var p DemoteWriterPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal demote writer payload: %w", err)
	}

	if p.NodeID == "" {
		return fmt.Errorf("demote writer: node_id is required")
	}

	f.mu.Lock()
	node, exists := f.nodes[p.NodeID]
	if exists {
		node.WriterState = "standby"
	}
	if f.primaryWriterID == p.NodeID {
		f.primaryWriterID = ""
	}
	f.mu.Unlock()

	if !exists {
		return fmt.Errorf("node %s not found", p.NodeID)
	}

	f.logger.Info().
		Str("node_id", p.NodeID).
		Msg("Writer demoted to standby")

	return nil
}

func (f *ClusterFSM) applyRegisterFile(payload []byte, logIndex uint64) interface{} {
	var p RegisterFilePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal register file payload: %w", err)
	}

	if p.File.Path == "" {
		return fmt.Errorf("register file: path is required")
	}
	if p.File.CreatedAt.IsZero() {
		// The creator is responsible for setting CreatedAt before appending
		// to Raft. We do NOT fill it in here because time.Now() inside Apply
		// is non-deterministic — log replay on other nodes would produce
		// different values and diverge FSM state.
		return fmt.Errorf("register file: created_at is required")
	}

	// Stamp the LSN from the Raft log index (deterministic across all nodes)
	p.File.LSN = logIndex

	f.mu.Lock()
	entry := p.File
	// If the file was already registered under a different database (unlikely
	// but possible if an operator moves a file across databases), remove the
	// old index entry first to keep filesByDB consistent.
	if old, existed := f.files[entry.Path]; existed && old.Database != entry.Database {
		if oldIdx, ok := f.filesByDB[old.Database]; ok {
			delete(oldIdx, old.Path)
			if len(oldIdx) == 0 {
				delete(f.filesByDB, old.Database)
			}
		}
	}
	f.files[entry.Path] = &entry
	// Maintain the database → files secondary index
	idx, ok := f.filesByDB[entry.Database]
	if !ok {
		idx = make(map[string]struct{})
		f.filesByDB[entry.Database] = idx
	}
	idx[entry.Path] = struct{}{}
	callback := f.onFileRegistered
	f.mu.Unlock()

	f.logger.Debug().
		Str("path", entry.Path).
		Str("database", entry.Database).
		Str("measurement", entry.Measurement).
		Str("origin", entry.OriginNodeID).
		Int64("size_bytes", entry.SizeBytes).
		Uint64("lsn", entry.LSN).
		Msg("File registered in cluster manifest")

	if callback != nil {
		entryCopy := entry
		callback(&entryCopy)
	}

	return nil
}

func (f *ClusterFSM) applyDeleteFile(payload []byte) interface{} {
	var p DeleteFilePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal delete file payload: %w", err)
	}

	if p.Path == "" {
		return fmt.Errorf("delete file: path is required")
	}

	f.mu.Lock()
	existing, existed := f.files[p.Path]
	delete(f.files, p.Path)
	if existed {
		// Remove from the database → files secondary index
		if idx, ok := f.filesByDB[existing.Database]; ok {
			delete(idx, p.Path)
			if len(idx) == 0 {
				delete(f.filesByDB, existing.Database)
			}
		}
	}
	callback := f.onFileDeleted
	f.mu.Unlock()

	if !existed {
		// Idempotent — deletion of a non-existent file is a no-op
		return nil
	}

	f.logger.Debug().
		Str("path", p.Path).
		Str("reason", p.Reason).
		Msg("File removed from cluster manifest")

	if callback != nil {
		callback(p.Path, p.Reason)
	}

	return nil
}

func (f *ClusterFSM) applyUpdateFile(payload []byte) interface{} {
	var p UpdateFilePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update file payload: %w", err)
	}
	if p.File.Path == "" {
		return fmt.Errorf("update file: path is required")
	}

	f.mu.Lock()
	entry := p.File
	// If the database changed (defensive), remove the old secondary index entry first.
	if old, existed := f.files[entry.Path]; existed && old.Database != entry.Database {
		if oldIdx, ok := f.filesByDB[old.Database]; ok {
			delete(oldIdx, old.Path)
			if len(oldIdx) == 0 {
				delete(f.filesByDB, old.Database)
			}
		}
	}
	f.files[entry.Path] = &entry
	if entry.Database != "" {
		idx, ok := f.filesByDB[entry.Database]
		if !ok {
			idx = make(map[string]struct{})
			f.filesByDB[entry.Database] = idx
		}
		idx[entry.Path] = struct{}{}
	}
	callback := f.onFileRegistered
	f.mu.Unlock()

	f.logger.Debug().
		Str("path", entry.Path).
		Int64("size_bytes", entry.SizeBytes).
		Str("sha256", entry.SHA256).
		Msg("File updated in cluster manifest")

	// Trigger onFileRegistered so reader nodes detect the content change and
	// pull the updated file from the writer. The file path is the same but the
	// content (and checksum) changed, so readers must re-fetch it.
	if callback != nil {
		entryCopy := entry
		callback(&entryCopy)
	}

	return nil
}

func (f *ClusterFSM) applyAssignCompactor(payload []byte) interface{} {
	var p AssignCompactorPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal assign compactor payload: %w", err)
	}

	if p.NodeID == "" {
		return fmt.Errorf("assign compactor: node_id is required")
	}

	f.mu.Lock()
	oldCompactorID := f.activeCompactorID
	f.activeCompactorID = p.NodeID
	callback := f.onCompactorAssigned
	f.mu.Unlock()

	f.logger.Info().
		Str("new_compactor", p.NodeID).
		Str("old_compactor", oldCompactorID).
		Msg("Compactor lease assigned")

	if callback != nil {
		callback(p.NodeID, oldCompactorID)
	}

	return nil
}

func (f *ClusterFSM) applyBatchFileOps(payload []byte, logIndex uint64) interface{} {
	var p BatchFileOpsPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal batch file ops payload: %w", err)
	}

	for i, op := range p.Ops {
		var result interface{}
		switch op.Type {
		case CommandRegisterFile:
			result = f.applyRegisterFile(op.Payload, logIndex)
		case CommandDeleteFile:
			result = f.applyDeleteFile(op.Payload)
		case CommandUpdateFile:
			result = f.applyUpdateFile(op.Payload)
		default:
			return fmt.Errorf("batch file ops: op[%d] unsupported type: %d", i, op.Type)
		}
		// applyRegisterFile and applyDeleteFile return nil on success or an
		// error on failure — they never return a non-nil non-error value.
		// The type-assert is defensive: if either handler is ever refactored
		// to return something unexpected, we propagate it as an error rather
		// than silently ignoring it.
		if result != nil {
			if err, ok := result.(error); ok {
				return fmt.Errorf("batch file ops: op[%d] (type=%d): %w", i, op.Type, err)
			}
			return fmt.Errorf("batch file ops: op[%d] (type=%d): unexpected non-error result: %v", i, op.Type, result)
		}
	}
	return nil
}

// Snapshot returns a snapshot of the FSM state.
func (f *ClusterFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Deep copy nodes
	nodes := make(map[string]*NodeInfo, len(f.nodes))
	for id, node := range f.nodes {
		nodeCopy := *node
		nodes[id] = &nodeCopy
	}

	// Deep copy files
	files := make(map[string]*FileEntry, len(f.files))
	for path, file := range f.files {
		fileCopy := *file
		files[path] = &fileCopy
	}

	return &fsmSnapshot{nodes: nodes, primaryWriterID: f.primaryWriterID, activeCompactorID: f.activeCompactorID, files: files}, nil
}

// Restore restores the FSM from a snapshot.
func (f *ClusterFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshot FSMSnapshot
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	f.mu.Lock()
	f.nodes = snapshot.Nodes
	f.primaryWriterID = snapshot.PrimaryWriterID
	f.activeCompactorID = snapshot.ActiveCompactorID
	if snapshot.Files != nil {
		f.files = snapshot.Files
	} else {
		f.files = make(map[string]*FileEntry)
	}
	// Rebuild the database → files secondary index from the restored files
	f.filesByDB = make(map[string]map[string]struct{}, len(f.files))
	for path, entry := range f.files {
		idx, ok := f.filesByDB[entry.Database]
		if !ok {
			idx = make(map[string]struct{})
			f.filesByDB[entry.Database] = idx
		}
		idx[path] = struct{}{}
	}
	f.mu.Unlock()

	f.logger.Info().
		Int("node_count", len(snapshot.Nodes)).
		Int("file_count", len(snapshot.Files)).
		Str("primary_writer", snapshot.PrimaryWriterID).
		Str("active_compactor", snapshot.ActiveCompactorID).
		Msg("FSM restored from snapshot")

	return nil
}

// GetNode returns a copy of the node info for the given ID.
func (f *ClusterFSM) GetNode(nodeID string) (*NodeInfo, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	node, exists := f.nodes[nodeID]
	if !exists {
		return nil, false
	}
	nodeCopy := *node
	return &nodeCopy, true
}

// GetAllNodes returns copies of all nodes.
func (f *ClusterFSM) GetAllNodes() []*NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	nodes := make([]*NodeInfo, 0, len(f.nodes))
	for _, node := range f.nodes {
		nodeCopy := *node
		nodes = append(nodes, &nodeCopy)
	}
	return nodes
}

// GetNodesByRole returns copies of all nodes with the given role.
func (f *ClusterFSM) GetNodesByRole(role string) []*NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var nodes []*NodeInfo
	for _, node := range f.nodes {
		if node.Role == role {
			nodeCopy := *node
			nodes = append(nodes, &nodeCopy)
		}
	}
	return nodes
}

// NodeCount returns the number of nodes in the FSM.
func (f *ClusterFSM) NodeCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.nodes)
}

// TotalCores returns the sum of CoreCount across all nodes in the FSM.
func (f *ClusterFSM) TotalCores() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	total := 0
	for _, node := range f.nodes {
		total += node.CoreCount
	}
	return total
}

// GetFile returns a copy of the file entry for the given path, or nil if not found.
func (f *ClusterFSM) GetFile(path string) (*FileEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	entry, exists := f.files[path]
	if !exists {
		return nil, false
	}
	entryCopy := *entry
	return &entryCopy, true
}

// GetAllFiles returns copies of all files in the manifest.
//
// WARNING: This is O(N) in the number of files and allocates a full copy.
// For large clusters with millions of files this can cause memory pressure
// and GC pauses. It is suitable for small-to-medium clusters (< ~100k files)
// and administrative/debugging use. A future phase will add a paginated or
// streaming variant for the API handler when scale demands it — tracked as
// a follow-up to Phase 1.
func (f *ClusterFSM) GetAllFiles() []*FileEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	files := make([]*FileEntry, 0, len(f.files))
	for _, file := range f.files {
		fileCopy := *file
		files = append(files, &fileCopy)
	}
	return files
}

// GetFilesByDatabase returns copies of all files for the given database.
// Uses the filesByDB secondary index for O(k) lookup where k is the number
// of files for the database — no longer scans the full manifest.
func (f *ClusterFSM) GetFilesByDatabase(database string) []*FileEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	idx, ok := f.filesByDB[database]
	if !ok {
		return nil
	}
	files := make([]*FileEntry, 0, len(idx))
	for path := range idx {
		if entry, exists := f.files[path]; exists {
			entryCopy := *entry
			files = append(files, &entryCopy)
		}
	}
	return files
}

// FileCount returns the number of files in the manifest.
func (f *ClusterFSM) FileCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.files)
}

// fsmSnapshot implements raft.FSMSnapshot.
type fsmSnapshot struct {
	nodes             map[string]*NodeInfo
	primaryWriterID   string
	activeCompactorID string
	files             map[string]*FileEntry
}

// Persist writes the snapshot to the given sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	snapshot := FSMSnapshot{
		Nodes:             s.nodes,
		PrimaryWriterID:   s.primaryWriterID,
		ActiveCompactorID: s.activeCompactorID,
		Files:             s.files,
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	return sink.Close()
}

// Release is called when the snapshot is no longer needed.
func (s *fsmSnapshot) Release() {}
