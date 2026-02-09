package queryregistry

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// QueryStatus represents the lifecycle state of a tracked query.
type QueryStatus string

const (
	StatusRunning   QueryStatus = "running"
	StatusCompleted QueryStatus = "completed"
	StatusCancelled QueryStatus = "cancelled"
	StatusFailed    QueryStatus = "failed"
	StatusTimedOut  QueryStatus = "timed_out"
)

// TrackedQuery holds all metadata about a tracked query.
type TrackedQuery struct {
	ID             string      `json:"id"`
	SQL            string      `json:"sql"`
	TokenID        int64       `json:"token_id,omitempty"`
	TokenName      string      `json:"token_name,omitempty"`
	RemoteAddr     string      `json:"remote_addr,omitempty"`
	Status         QueryStatus `json:"status"`
	StartTime      time.Time   `json:"start_time"`
	EndTime        *time.Time  `json:"end_time,omitempty"`
	DurationMs     float64     `json:"duration_ms,omitempty"`
	RowCount       int         `json:"row_count,omitempty"`
	Error          string      `json:"error,omitempty"`
	IsParallel     bool        `json:"is_parallel"`
	PartitionCount int         `json:"partition_count,omitempty"`
}

// activeEntry stores the tracked query plus its cancel func.
type activeEntry struct {
	query  *TrackedQuery
	cancel context.CancelFunc
}

// RegistryConfig holds configuration for the query registry.
type RegistryConfig struct {
	HistorySize int // Ring buffer size for completed queries (default: 100)
}

// Registry tracks active and recently completed queries.
type Registry struct {
	mu       sync.RWMutex
	active   map[string]*activeEntry
	history  []*TrackedQuery // Ring buffer
	histSize int             // Configured capacity
	histHead int             // Next write position
	histLen  int             // Current count (up to histSize)
	logger   zerolog.Logger
}

// NewRegistry creates a new query registry.
func NewRegistry(cfg *RegistryConfig, logger zerolog.Logger) *Registry {
	histSize := 100
	if cfg != nil && cfg.HistorySize > 0 {
		histSize = cfg.HistorySize
	}
	return &Registry{
		active:   make(map[string]*activeEntry),
		history:  make([]*TrackedQuery, histSize),
		histSize: histSize,
		logger:   logger.With().Str("component", "query-registry").Logger(),
	}
}

// Register registers a new active query. Returns the query ID and a
// context derived from parentCtx that can be cancelled via Cancel().
func (r *Registry) Register(parentCtx context.Context, sql string, tokenID int64, tokenName string, remoteAddr string, isParallel bool, partitionCount int) (string, context.Context) {
	queryID := uuid.New().String()[:12]
	ctx, cancel := context.WithCancel(parentCtx)

	query := &TrackedQuery{
		ID:             queryID,
		SQL:            sql,
		TokenID:        tokenID,
		TokenName:      tokenName,
		RemoteAddr:     remoteAddr,
		Status:         StatusRunning,
		StartTime:      time.Now(),
		IsParallel:     isParallel,
		PartitionCount: partitionCount,
	}

	r.mu.Lock()
	r.active[queryID] = &activeEntry{query: query, cancel: cancel}
	r.mu.Unlock()

	r.logger.Debug().
		Str("query_id", queryID).
		Int64("token_id", tokenID).
		Bool("parallel", isParallel).
		Msg("Query registered")

	return queryID, ctx
}

// Complete marks a query as completed and moves it to history.
func (r *Registry) Complete(queryID string, rowCount int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.active[queryID]
	if !ok {
		return
	}

	now := time.Now()
	entry.query.Status = StatusCompleted
	entry.query.EndTime = &now
	entry.query.DurationMs = float64(now.Sub(entry.query.StartTime).Milliseconds())
	entry.query.RowCount = rowCount

	r.addToHistory(entry.query)
	delete(r.active, queryID)
}

// Fail marks a query as failed and moves it to history.
func (r *Registry) Fail(queryID string, errMsg string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.active[queryID]
	if !ok {
		return
	}

	now := time.Now()
	entry.query.Status = StatusFailed
	entry.query.EndTime = &now
	entry.query.DurationMs = float64(now.Sub(entry.query.StartTime).Milliseconds())
	entry.query.Error = errMsg

	r.addToHistory(entry.query)
	delete(r.active, queryID)
}

// TimedOut marks a query as timed out and moves it to history.
func (r *Registry) TimedOut(queryID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.active[queryID]
	if !ok {
		return
	}

	now := time.Now()
	entry.query.Status = StatusTimedOut
	entry.query.EndTime = &now
	entry.query.DurationMs = float64(now.Sub(entry.query.StartTime).Milliseconds())
	entry.query.Error = "Query timed out"

	r.addToHistory(entry.query)
	delete(r.active, queryID)
}

// Cancel cancels a running query by ID. Returns true if the query was found and cancelled.
func (r *Registry) Cancel(queryID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.active[queryID]
	if !ok {
		return false
	}

	now := time.Now()
	entry.query.Status = StatusCancelled
	entry.query.EndTime = &now
	entry.query.DurationMs = float64(now.Sub(entry.query.StartTime).Milliseconds())

	// Cancel the context — this propagates to DuckDB QueryContext
	entry.cancel()

	r.logger.Info().
		Str("query_id", queryID).
		Int64("token_id", entry.query.TokenID).
		Float64("duration_ms", entry.query.DurationMs).
		Msg("Query cancelled via API")

	r.addToHistory(entry.query)
	delete(r.active, queryID)
	return true
}

// GetActive returns a snapshot of all active queries.
func (r *Registry) GetActive() []*TrackedQuery {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*TrackedQuery, 0, len(r.active))
	now := time.Now()
	for _, entry := range r.active {
		q := *entry.query // copy
		q.DurationMs = float64(now.Sub(q.StartTime).Milliseconds())
		result = append(result, &q)
	}
	return result
}

// GetHistory returns a snapshot of the most recent completed queries (newest first).
func (r *Registry) GetHistory(limit int) []*TrackedQuery {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := r.histLen
	if limit > 0 && limit < count {
		count = limit
	}

	result := make([]*TrackedQuery, 0, count)
	for i := 0; i < count; i++ {
		// Walk backwards from most recent
		idx := (r.histHead - 1 - i + r.histSize) % r.histSize
		if r.history[idx] != nil {
			q := *r.history[idx] // copy
			result = append(result, &q)
		}
	}
	return result
}

// GetQuery returns a specific query by ID (checks active then history).
func (r *Registry) GetQuery(queryID string) *TrackedQuery {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check active first
	if entry, ok := r.active[queryID]; ok {
		q := *entry.query
		q.DurationMs = float64(time.Since(q.StartTime).Milliseconds())
		return &q
	}

	// Check history
	for i := 0; i < r.histLen; i++ {
		idx := (r.histHead - 1 - i + r.histSize) % r.histSize
		if r.history[idx] != nil && r.history[idx].ID == queryID {
			q := *r.history[idx]
			return &q
		}
	}
	return nil
}

// ActiveCount returns the number of currently running queries.
func (r *Registry) ActiveCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.active)
}

// HistoryLen returns the number of queries in the history buffer.
func (r *Registry) HistoryLen() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.histLen
}

// addToHistory appends a query to the ring buffer. Must be called with mu held.
func (r *Registry) addToHistory(q *TrackedQuery) {
	r.history[r.histHead] = q
	r.histHead = (r.histHead + 1) % r.histSize
	if r.histLen < r.histSize {
		r.histLen++
	}
}
