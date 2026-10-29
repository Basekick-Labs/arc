package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/gofiber/fiber/v2"
)

// Continuous queries in a cluster (#1324 stage 3), as retention_cluster.go for
// retention policies, plus the watermark.
//
// Once the cluster adopts them, the definitions and each CQ's watermark live in
// the cluster state: every read and every run takes them from there, writes are
// proposed to it, and this node's own tables are a mirror (backups, the
// execution history's foreign key). Execution history stays per node.
//
// The watermark is moved by the primary after every run with a compare-and-set
// from the value the run read. A run whose watermark the cluster may not have
// taken (the propose timed out) leaves a pending entry; the next run of that CQ
// settles it first and is skipped if it cannot, so a primary cut off from the
// leader does not keep widening the window a new primary would re-run.

// CQClusterSource is the cluster state continuous queries read and write once
// adopted. *cluster.Coordinator satisfies it.
type CQClusterSource interface {
	CQPoliciesSeeded() bool
	CQPoliciesCollecting() bool
	ClusterCQ(id int64) (raft.CQEntry, bool)
	ClusterCQByName(name string) (raft.CQEntry, bool)
	ClusterCQs() []raft.CQEntry
	CreateCQ(ctx context.Context, p raft.CreateCQPayload) error
	UpdateCQ(ctx context.Context, p raft.UpdateCQPayload) error
	DeleteCQ(ctx context.Context, id int64) error
	SetCQWatermark(ctx context.Context, id int64, from *time.Time, to time.Time) error
	CQReadBarrier(ctx context.Context) error
	CQMirrorState() raft.CQMirror
	LocalNodeID() string
}

// cqCluster is the handler's cluster-mode state.
type cqCluster struct {
	mu       sync.RWMutex
	src      CQClusterSource
	localMu  sync.Mutex // this node's own writes vs the cluster's read of them
	mirrorMu sync.Mutex
	owned    bool // guarded by mu; set once the mirror has run here

	runsMu  sync.Mutex
	runs    map[int64]chan struct{} // one run of a CQ at a time (a 1-slot semaphore)
	pending map[int64]cqPending     // watermarks the cluster may not have taken

	scheduler CQSchedulerReloader // guarded by mu: the mirror worker reads it
}

// cqPending is a watermark compare-and-set whose outcome is unknown.
type cqPending struct {
	from *time.Time
	to   time.Time
	// database, measurement: the run's output, flushed before the
	// watermark is proposed.
	database, measurement string
}

// cqOutputFlusher flushes a run's buffered output to storage
// (*ingest.ArrowBuffer).
type cqOutputFlusher interface {
	FlushScopeAndWait(ctx context.Context, database, measurement string) (map[string]uint64, error)
}

// flushOutput writes a run's buffered output to storage before its watermark
// is proposed (taken from #1328). Once the cluster holds the watermark no
// run covers that window again, so output still in this node's buffer (or
// only in its WAL, off by default) would be lost with the node.
func (h *ContinuousQueryHandler) flushOutput(ctx context.Context, database, measurement string) error {
	if h.outputFlusher == nil {
		return nil
	}
	_, err := h.outputFlusher.FlushScopeAndWait(ctx, database, measurement)
	return err
}

// SetClusterSource makes continuous queries cluster-wide once the cluster
// adopts them. Called after construction in cluster mode.
func (h *ContinuousQueryHandler) SetClusterSource(src CQClusterSource) {
	h.cl.mu.Lock()
	defer h.cl.mu.Unlock()
	h.cl.src = src
}

func (h *ContinuousQueryHandler) clusterSource() CQClusterSource {
	h.cl.mu.RLock()
	defer h.cl.mu.RUnlock()
	return h.cl.src
}

func (h *ContinuousQueryHandler) adoptedSource() CQClusterSource {
	if src := h.clusterSource(); src != nil && src.CQPoliciesSeeded() {
		return src
	}
	return nil
}

func (h *ContinuousQueryHandler) ownedFlag() bool {
	h.cl.mu.RLock()
	defer h.cl.mu.RUnlock()
	return h.cl.owned
}

func (h *ContinuousQueryHandler) setOwned() {
	h.cl.mu.Lock()
	h.cl.owned = true
	h.cl.mu.Unlock()
}

// ErrCQNotCaughtUp: this node's tables are the cluster's (a mirror ran here),
// but its cluster state has not caught up since it started. The scheduler
// starts with no jobs and fills them in once it has.
var ErrCQNotCaughtUp = errors.New("this node has not caught up with the cluster's continuous queries yet; retry in a few seconds")

var errCQBeingAdopted = errors.New("continuous queries are being adopted by the cluster; retry in a few seconds")

// readSource: the cluster state (src), this node's own tables (nil, nil), or
// neither yet (ErrCQNotCaughtUp).
func (h *ContinuousQueryHandler) readSource() (CQClusterSource, error) {
	src := h.clusterSource()
	if src == nil {
		return nil, nil
	}
	if src.CQPoliciesSeeded() {
		return src, nil
	}
	if h.ownedFlag() {
		return nil, ErrCQNotCaughtUp
	}
	return nil, nil
}

// localWrite is RetentionHandler.localWrite for continuous queries.
func (h *ContinuousQueryHandler) localWrite(fn func() error) (CQClusterSource, error) {
	h.cl.localMu.Lock()
	defer h.cl.localMu.Unlock()
	if src := h.clusterSource(); src != nil {
		if src.CQPoliciesCollecting() {
			return nil, errCQBeingAdopted
		}
		if src.CQPoliciesSeeded() {
			return src, nil
		}
		if h.ownedFlag() {
			return nil, ErrCQNotCaughtUp
		}
	}
	return nil, fn()
}

func isCQWait(err error) bool {
	return errors.Is(err, errCQBeingAdopted) || errors.Is(err, ErrCQNotCaughtUp)
}

// cqFromEntry is a cluster CQ in the API's shape, with this node's last
// execution of it.
func cqFromEntry(e raft.CQEntry, last *cqLastExecution) ContinuousQuery {
	q := ContinuousQuery{
		ID: e.ID, Name: e.Name, Description: e.Description, Database: e.Database,
		SourceMeasurement: e.SourceMeasurement, DestinationMeasurement: e.DestinationMeasurement,
		Query: e.Query, Interval: e.Interval, TagColumns: e.TagColumns, RetentionDays: e.RetentionDays,
		DeleteSourceAfterDays: e.DeleteSourceAfterDays, IsActive: e.IsActive,
		CreatedAt: retentionTimestamp(e.CreatedAt), UpdatedAt: retentionTimestamp(e.UpdatedAt),
	}
	if e.LastProcessed != nil {
		s := e.LastProcessed.UTC().Format(time.RFC3339)
		q.LastProcessedTime = &s
	}
	if last != nil {
		q.LastExecutionTime, q.LastExecutionStatus, q.LastRecordsWritten = last.time, last.status, last.written
	}
	return q
}

type cqLastExecution struct {
	time, status *string
	written      *int64
}

func (h *ContinuousQueryHandler) cqLastExecutions() map[int64]*cqLastExecution {
	out := map[int64]*cqLastExecution{}
	rows, err := h.sqliteDB.Query(`SELECT query_id, execution_time, status, records_written FROM continuous_query_executions
		WHERE id IN (SELECT MAX(id) FROM continuous_query_executions GROUP BY query_id)`)
	if err != nil {
		h.logger.Warn().Err(err).Msg("Could not read this node's continuous query execution history")
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		l := &cqLastExecution{}
		if err := rows.Scan(&id, &l.time, &l.status, &l.written); err == nil {
			out[id] = l
		}
	}
	return out
}

// clusterQuery is getQuery once adopted; a CQ the cluster does not hold is not
// found, never a row of this node's own.
func (h *ContinuousQueryHandler) clusterQuery(src CQClusterSource, id int64) (*ContinuousQuery, error) {
	e, ok := src.ClusterCQ(id)
	if !ok {
		return nil, sql.ErrNoRows
	}
	q := cqFromEntry(e, h.cqLastExecutions()[id])
	return &q, nil
}

// clusterQueries is getQueries once adopted, with the same filters and order.
func (h *ContinuousQueryHandler) clusterQueries(src CQClusterSource, database, isActiveStr string) []ContinuousQuery {
	entries := src.ClusterCQs()
	last := h.cqLastExecutions()
	sort.SliceStable(entries, func(i, j int) bool {
		if !entries[i].CreatedAt.Equal(entries[j].CreatedAt) {
			return entries[i].CreatedAt.After(entries[j].CreatedAt)
		}
		return entries[i].ID > entries[j].ID
	})
	var out []ContinuousQuery
	for _, e := range entries {
		if database != "" && e.Database != database {
			continue
		}
		if isActiveStr != "" && e.IsActive != (isActiveStr == "true") {
			continue
		}
		out = append(out, cqFromEntry(e, last[e.ID]))
	}
	return out
}

// CQJobDef is what a scheduler job needs of a continuous query.
type CQJobDef struct {
	ID       int64
	Name     string
	Interval string
	IsActive bool
}

// syncJobs hands the scheduler the cluster's continuous queries once adopted,
// so every node's jobs follow them (a node's ticks fire only while it is the
// primary).
func (h *ContinuousQueryHandler) syncJobs(src CQClusterSource) {
	h.cl.mu.RLock()
	sched := h.cl.scheduler
	h.cl.mu.RUnlock()
	s, ok := sched.(interface{ SyncJobs([]CQJobDef) })
	if !ok || s == nil {
		return
	}
	entries := src.ClusterCQs()
	defs := make([]CQJobDef, 0, len(entries))
	for _, e := range entries {
		defs = append(defs, CQJobDef{ID: e.ID, Name: e.Name, Interval: e.Interval, IsActive: e.IsActive})
	}
	s.SyncJobs(defs)
}

// clusterWriteError answers a cluster write's error, as retention's.
func (h *ContinuousQueryHandler) clusterWriteError(c *fiber.Ctx, err error, nameTaken string) error {
	switch {
	case raft.IsPolicyNotFound(err):
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Continuous query not found"})
	case raft.IsPolicyNameTaken(err):
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": nameTaken})
	case errors.Is(err, raft.ErrInvalidCQ), err != nil && strings.Contains(err.Error(), raft.ErrInvalidCQ.Error()):
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	h.logger.Warn().Err(err).Msg("The cluster did not take the continuous query write")
	return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
		"error": "the cluster could not take the continuous query write right now; retry",
	})
}

func (h *ContinuousQueryHandler) clusterCreate(c *fiber.Ctx, src CQClusterSource, req *ContinuousQueryRequest, isActive bool) error {
	err := src.CreateCQ(c.UserContext(), raft.CreateCQPayload{
		Name: req.Name.Value, Description: req.Description.ptr(), Database: req.Database.Value,
		SourceMeasurement: req.SourceMeasurement.Value, DestinationMeasurement: req.DestinationMeasurement.Value,
		Query: req.Query.Value, Interval: req.Interval.Value, TagColumns: req.TagColumns.Value,
		RetentionDays: req.RetentionDays.ptr(), DeleteSourceAfterDays: req.DeleteSourceAfterDays.ptr(), IsActive: isActive,
	})
	if err != nil {
		return h.clusterWriteError(c, err, fmt.Sprintf("Continuous query with name '%s' already exists", req.Name.Value))
	}
	e, ok := src.ClusterCQByName(req.Name.Value)
	if !ok {
		return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
			"message": "Continuous query created; this node has not applied it yet, list the continuous queries to see it",
		})
	}
	h.logger.Info().Int64("query_id", e.ID).Str("name", e.Name).Msg("Created continuous query in the cluster")
	q, _ := h.clusterQuery(src, e.ID)
	return c.Status(fiber.StatusCreated).JSON(q)
}

func (h *ContinuousQueryHandler) clusterUpdate(c *fiber.Ctx, src CQClusterSource, id int64, req *ContinuousQueryRequest) error {
	p := raft.UpdateCQPayload{ID: id}
	for _, f := range []struct {
		field optionalField[string]
		dst   **string
	}{
		{req.Name, &p.Name}, {req.Database, &p.Database}, {req.SourceMeasurement, &p.SourceMeasurement},
		{req.DestinationMeasurement, &p.DestinationMeasurement}, {req.Query, &p.Query}, {req.Interval, &p.Interval},
	} {
		if f.field.Set {
			v := f.field.Value
			*f.dst = &v
		}
	}
	if req.IsActive.Set {
		p.IsActive = &req.IsActive.Value
	}
	if req.Description.Set {
		p.SetDescription, p.Description = true, req.Description.ptr()
	}
	if req.TagColumns.Set {
		p.SetTagColumns, p.TagColumns = true, req.TagColumns.Value
	}
	if req.RetentionDays.Set {
		p.SetRetentionDays, p.RetentionDays = true, req.RetentionDays.ptr()
	}
	if req.DeleteSourceAfterDays.Set {
		p.SetDeleteSourceAfterDays, p.DeleteSourceAfterDays = true, req.DeleteSourceAfterDays.ptr()
	}
	if err := src.UpdateCQ(c.UserContext(), p); err != nil {
		return h.clusterWriteError(c, err, fmt.Sprintf("Continuous query with name %q already exists", req.Name.Value))
	}
	h.logger.Info().Int64("query_id", id).Msg("Updated continuous query in the cluster")
	q, err := h.clusterQuery(src, id)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Continuous query not found"})
	}
	return c.JSON(q)
}

func (h *ContinuousQueryHandler) clusterDelete(c *fiber.Ctx, src CQClusterSource, id int64) error {
	if err := src.DeleteCQ(c.UserContext(), id); err != nil {
		return h.clusterWriteError(c, err, "")
	}
	h.logger.Info().Int64("query_id", id).Msg("Deleted continuous query in the cluster")
	return c.JSON(fiber.Map{"message": "Continuous query deleted successfully"})
}

// acquireRun waits for the CQ's run slot (a scheduled run and a manual
// execute take turns), or for ctx: a scheduled run whose job was stopped
// gives up instead of running late with a cancelled context.
func (h *ContinuousQueryHandler) acquireRun(ctx context.Context, id int64) (release func(), err error) {
	h.cl.runsMu.Lock()
	if h.cl.runs == nil {
		h.cl.runs = map[int64]chan struct{}{}
	}
	slot, ok := h.cl.runs[id]
	if !ok {
		slot = make(chan struct{}, 1)
		h.cl.runs[id] = slot
	}
	h.cl.runsMu.Unlock()
	select {
	case slot <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-slot
		return nil, err
	}
	return func() { <-slot }, nil
}

// ForgetPendingWatermarks drops every unsettled watermark: called when this
// node stops being the primary, so a later promotion never re-proposes one
// over whatever the cluster did meanwhile (another primary's runs, a reset
// that happened to land on the same value).
func (h *ContinuousQueryHandler) ForgetPendingWatermarks() {
	h.cl.runsMu.Lock()
	defer h.cl.runsMu.Unlock()
	if len(h.cl.pending) > 0 {
		h.cl.pending = map[int64]cqPending{}
	}
}

// errCQSkippedDuringCollect: a run while the cluster reads the nodes' own
// continuous queries; its watermark would land after the read and be lost.
var errCQSkippedDuringCollect = errors.New("continuous queries are being adopted by the cluster; this run is skipped and the next one covers its window")

// errCQReadBarrier: this node could not confirm it holds the cluster's latest
// watermark.
var errCQReadBarrier = errors.New("this node could not confirm it holds the cluster's latest watermark; this run is skipped")

// prepareClusterRun is what a run does in a cluster before it reads its CQ:
// while the nodes' CQs are being collected, skip; once adopted, catch up with
// the leader (so the watermark read is not stale) and settle an unsettled
// watermark. It returns the adopted source, or nil for a node-local run.
func (h *ContinuousQueryHandler) prepareClusterRun(ctx context.Context, id int64) (CQClusterSource, error) {
	src, err := h.readSource()
	if err != nil {
		return nil, err
	}
	if src == nil {
		if c := h.clusterSource(); c != nil && c.CQPoliciesCollecting() {
			return nil, errCQSkippedDuringCollect
		}
		return nil, nil
	}
	if err := src.CQReadBarrier(ctx); err != nil {
		h.logger.Warn().Err(err).Int64("query_id", id).Msg("Continuous query run skipped: could not catch up with the cluster")
		return nil, fmt.Errorf("%w: %v", errCQReadBarrier, err)
	}
	if err := h.settlePending(ctx, src, id); err != nil {
		return nil, err
	}
	return src, nil
}

// errCQWatermarkPending: the last run's watermark is still not taken by the
// cluster, so this run is skipped.
var errCQWatermarkPending = errors.New("the previous run's watermark has not reached the cluster yet; this run is skipped so a new primary would not re-run more than one window")

func watermarkEqual(a *time.Time, b time.Time) bool {
	return a != nil && raft.CQWatermarkTime(*a).Equal(raft.CQWatermarkTime(b))
}

func watermarkSame(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return watermarkEqual(a, *b)
}

// settlePending resolves a watermark the cluster may not have taken: done if
// the cluster holds it, proposed again if the cluster still holds what it
// replaced, dropped if anything else moved the watermark since (a manual
// reset, another primary). The run holds the CQ's run lock.
func (h *ContinuousQueryHandler) settlePending(ctx context.Context, src CQClusterSource, id int64) error {
	h.cl.runsMu.Lock()
	p, ok := h.cl.pending[id]
	h.cl.runsMu.Unlock()
	if !ok {
		return nil
	}
	drop := func() {
		h.cl.runsMu.Lock()
		delete(h.cl.pending, id)
		h.cl.runsMu.Unlock()
	}
	// Taken (the cluster holds p.to) or moved by anything else since (a
	// manual reset, another primary): settled. Only a watermark still at
	// p.from is proposed again.
	if e, ok := src.ClusterCQ(id); !ok || !watermarkSame(e.LastProcessed, p.from) {
		drop()
		return nil
	}
	if err := h.flushOutput(ctx, p.database, p.measurement); err != nil {
		h.logger.Warn().Err(err).Int64("query_id", id).Msg("The previous run's output is still not flushed; this run is skipped")
		return errCQWatermarkPending
	}
	_ = src.SetCQWatermark(ctx, id, p.from, p.to)
	// The outcome is read here, not from the propose's error: a forwarded
	// propose returns nil when the wait for this node's apply timed out.
	if e, ok := src.ClusterCQ(id); !ok || !watermarkSame(e.LastProcessed, p.from) {
		drop()
		return nil
	}
	return errCQWatermarkPending
}

// Watermark outcomes of a run, as a manual execute reports them.
const (
	cqWatermarkSet     = "set"
	cqWatermarkMoved   = "moved"
	cqWatermarkPending = "pending"
)

// commitWatermark moves the CQ's watermark from the value the run read to the
// run's end, once the run's output is flushed to storage. The pending entry is
// set first and cleared only when this node's cluster state shows the
// outcome: a flush or a propose that fails leaves it for the next run to
// settle (skipping runs until it can), rather than recording a window whose
// output could still be lost or writing the window again.
func (h *ContinuousQueryHandler) commitWatermark(ctx context.Context, src CQClusterSource, id int64, from *time.Time, to time.Time, database, measurement string) string {
	h.cl.runsMu.Lock()
	if h.cl.pending == nil {
		h.cl.pending = map[int64]cqPending{}
	}
	h.cl.pending[id] = cqPending{from: from, to: to, database: database, measurement: measurement}
	h.cl.runsMu.Unlock()
	if err := h.flushOutput(ctx, database, measurement); err != nil {
		h.logger.Warn().Err(err).Int64("query_id", id).
			Msg("The continuous query's output is not flushed yet; its watermark waits, and the next run settles it first")
		return cqWatermarkPending
	}
	err := src.SetCQWatermark(ctx, id, from, to)
	e, ok := src.ClusterCQ(id)
	if !ok || watermarkEqual(e.LastProcessed, to) || !watermarkSame(e.LastProcessed, from) {
		// Taken, or moved by someone else (a reset wins): settled either way.
		h.cl.runsMu.Lock()
		delete(h.cl.pending, id)
		h.cl.runsMu.Unlock()
		if ok && !watermarkEqual(e.LastProcessed, to) {
			h.logger.Warn().Int64("query_id", id).Msg("The continuous query's watermark moved while it ran (a manual execute or another primary); keeping that one")
			return cqWatermarkMoved
		}
		return cqWatermarkSet
	}
	h.logger.Warn().Err(err).Int64("query_id", id).
		Msg("The cluster has not taken the continuous query's watermark yet; the next run settles it first")
	return cqWatermarkPending
}

// lastProcessedOf parses a CQ's watermark as the API carries it.
func lastProcessedOf(q *ContinuousQuery) *time.Time {
	if q.LastProcessedTime == nil {
		return nil
	}
	t, err := time.Parse(time.RFC3339, *q.LastProcessedTime)
	if err != nil {
		return nil
	}
	t = raft.CQWatermarkTime(t)
	return &t
}

// LocalPolicyLister reads this node's own continuous queries for the
// cluster's adoption, under localMu.
func (h *ContinuousQueryHandler) LocalPolicyLister() cluster.LocalPolicyLister {
	return func(ctx context.Context) (json.RawMessage, error) {
		h.cl.localMu.Lock()
		defer h.cl.localMu.Unlock()
		return listLocalCQs(ctx, h.sqliteDB)
	}
}

// CQTableLister is RetentionTableLister for continuous queries: a node where
// they are disabled still reports the ones its database holds.
func CQTableLister(authManager *auth.AuthManager, dbPath string) cluster.LocalPolicyLister {
	return func(ctx context.Context) (json.RawMessage, error) {
		empty := func() (json.RawMessage, error) {
			return json.Marshal(cluster.CQLocalRows{Policies: []cluster.CQLocalRow{}})
		}
		var db *sql.DB
		if authManager != nil && authManager.SharesDBPath(dbPath) {
			db = authManager.GetDB()
		} else {
			if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
				return empty()
			}
			opened, err := sql.Open("sqlite3", dbPath+dsnSep(dbPath)+"mode=ro&_busy_timeout=5000")
			if err != nil {
				return nil, err
			}
			defer opened.Close()
			db = opened
		}
		var n int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'continuous_queries'`).Scan(&n); err != nil {
			return nil, err
		}
		if n == 0 {
			return empty()
		}
		return listLocalCQs(ctx, db)
	}
}

// listLocalCQs reads a node's own continuous queries, with how many runs of
// each completed here (at any time: see cluster.planCQSeed).
func listLocalCQs(ctx context.Context, db *sql.DB) (json.RawMessage, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT cq.id, cq.name, cq.description, cq.database, cq.source_measurement, cq.destination_measurement,
			cq.query, cq.interval, cq.tag_columns, cq.retention_days, cq.delete_source_after_days, cq.is_active,
			cq.last_processed_time, cq.created_at, cq.updated_at,
			(SELECT COUNT(*) FROM continuous_query_executions e WHERE e.query_id = cq.id AND e.status = 'completed')
		FROM continuous_queries cq ORDER BY cq.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := cluster.CQLocalRows{Policies: []cluster.CQLocalRow{}}
	for rows.Next() {
		var r cluster.CQLocalRow
		var tags, lpt sql.NullString
		var active sql.NullBool
		var created, updated sql.NullTime
		if err := rows.Scan(&r.ID, &r.Name, &r.Description, &r.Database, &r.SourceMeasurement, &r.DestinationMeasurement,
			&r.Query, &r.Interval, &tags, &r.RetentionDays, &r.DeleteSourceAfterDays, &active,
			&lpt, &created, &updated, &r.Executions); err != nil {
			return nil, fmt.Errorf("read continuous query row: %w", err)
		}
		if tags.Valid {
			s := tags.String
			if t, err := decodeTagColumns(&s); err == nil {
				r.TagColumns = t
			}
		}
		if lpt.Valid && lpt.String != "" {
			if t, err := time.Parse(time.RFC3339, lpt.String); err == nil {
				t = raft.CQWatermarkTime(t)
				r.LastProcessed = &t
			}
		}
		r.IsActive = active.Valid && active.Bool
		r.CreatedAt, r.UpdatedAt = created.Time.UTC(), updated.Time.UTC()
		out.Policies = append(out.Policies, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var seq sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT seq FROM sqlite_sequence WHERE name = 'continuous_queries'`).Scan(&seq); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	out.MaxIssuedID = seq.Int64
	if len(out.Policies) > raft.MaxSeedCQs {
		return nil, fmt.Errorf("%d continuous queries exceed the %d the cluster adopts", len(out.Policies), raft.MaxSeedCQs)
	}
	return json.Marshal(out)
}

// MirrorClusterPolicies makes this node's own tables equal the cluster's
// continuous queries, watermarks included, once adopted (a no-op before), and
// then hands the scheduler the cluster's CQs.
func (h *ContinuousQueryHandler) MirrorClusterPolicies(ctx context.Context) error {
	src := h.adoptedSource()
	if src == nil {
		return nil
	}
	h.cl.mirrorMu.Lock()
	err := h.reconcileCQMirror(ctx, src.CQMirrorState(), src.LocalNodeID())
	h.cl.mirrorMu.Unlock()
	h.syncJobs(src)
	return err
}

func (h *ContinuousQueryHandler) ensureCQMirrored(ctx context.Context, id int64) {
	src := h.adoptedSource()
	if src == nil {
		return
	}
	h.cl.mirrorMu.Lock()
	defer h.cl.mirrorMu.Unlock()
	// The history row needs this CQ's row as the mirror wrote it; a full
	// reconcile only when it is missing (the watermark in the row may lag,
	// which the periodic pass catches up).
	var n int
	if err := h.sqliteDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM continuous_queries cq JOIN continuous_query_cluster_ids o ON o.id = cq.id WHERE cq.id = ?`, id).Scan(&n); err == nil && n == 1 {
		return
	}
	if err := h.reconcileCQMirror(ctx, src.CQMirrorState(), src.LocalNodeID()); err != nil {
		h.logger.Warn().Err(err).Msg("Could not mirror the cluster's continuous queries before recording a run")
	}
}

// cqRowKey is a mirror row as written, for comparison with the cluster's.
func cqRowKey(e raft.CQEntry) string {
	tags, _ := encodeTagColumns(e.TagColumns)
	lpt := ""
	if e.LastProcessed != nil {
		lpt = e.LastProcessed.UTC().Format(time.RFC3339)
	}
	raw, _ := json.Marshal([]interface{}{e.ID, e.Name, e.Description, e.Database, e.SourceMeasurement, e.DestinationMeasurement,
		e.Query, e.Interval, tags, e.RetentionDays, e.DeleteSourceAfterDays, e.IsActive, lpt,
		e.CreatedAt.UTC().Format(sqliteTimestamp), e.UpdatedAt.UTC().Format(sqliteTimestamp)})
	return string(raw)
}

// reconcileCQMirror is reconcileMirror for continuous queries: the same rules
// (owned IDs, sources rekeyed, everything else dormant with its history), in
// one transaction on one connection, writing each CQ's watermark as the
// cluster holds it.
func (h *ContinuousQueryHandler) reconcileCQMirror(ctx context.Context, st raft.CQMirror, self string) error {
	cqs := st.CQs
	conn, err := h.sqliteDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id, name, description, database, source_measurement, destination_measurement,
		query, interval, tag_columns, retention_days, delete_source_after_days, COALESCE(is_active, 0), last_processed_time,
		created_at, updated_at FROM continuous_queries`)
	if err != nil {
		return err
	}
	local := map[int64]string{}
	localName := map[int64]string{}
	for rows.Next() {
		var e raft.CQEntry
		var tags, lpt sql.NullString
		var created, updated sql.NullTime
		if err := rows.Scan(&e.ID, &e.Name, &e.Description, &e.Database, &e.SourceMeasurement, &e.DestinationMeasurement,
			&e.Query, &e.Interval, &tags, &e.RetentionDays, &e.DeleteSourceAfterDays, &e.IsActive, &lpt, &created, &updated); err != nil {
			rows.Close()
			return fmt.Errorf("read continuous query row: %w", err)
		}
		if tags.Valid {
			s := tags.String
			e.TagColumns, _ = decodeTagColumns(&s)
		}
		if lpt.Valid && lpt.String != "" {
			if t, err := time.Parse(time.RFC3339, lpt.String); err == nil {
				t = raft.CQWatermarkTime(t)
				e.LastProcessed = &t
			}
		}
		e.CreatedAt, e.UpdatedAt = created.Time, updated.Time
		local[e.ID] = cqRowKey(e)
		localName[e.ID] = e.Name
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	owned := map[int64]bool{}
	idRows, err := tx.QueryContext(ctx, `SELECT id FROM continuous_query_cluster_ids`)
	if err != nil {
		return err
	}
	for idRows.Next() {
		var id int64
		if err := idRows.Scan(&id); err != nil {
			idRows.Close()
			return err
		}
		owned[id] = true
	}
	idRows.Close()
	if err := idRows.Err(); err != nil {
		return err
	}
	var marked int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_cluster_state WHERE name = 'owned'`).Scan(&marked); err != nil {
		return err
	}

	want := make(map[int64]raft.CQEntry, len(cqs))
	sourced := map[int64]int64{}
	for _, e := range cqs {
		want[e.ID] = e
		for _, s := range e.Sources {
			if s.Node == self {
				sourced[s.ID] = e.ID
			}
		}
	}
	changed := marked == 0
	exec := func(q string, args ...interface{}) error {
		changed = true
		_, err := tx.ExecContext(ctx, q, args...)
		return err
	}
	ids := make([]int64, 0, len(local))
	for id := range local {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	dormant := 0
	for _, id := range ids {
		to, isSource := sourced[id]
		if _, ok := want[id]; ok && (owned[id] || (isSource && to == id)) {
			continue
		}
		if owned[id] && id > st.Seq {
			// A CQ this node's state has not caught up with yet (a restart
			// from an older snapshot): left as it is, history included.
			continue
		}
		switch {
		case isSource && to != id:
			if err := exec(`UPDATE continuous_query_executions SET query_id = ? WHERE query_id = ?`, to, id); err != nil {
				return err
			}
		case owned[id]:
			if err := exec(`DELETE FROM continuous_query_executions WHERE query_id = ?`, id); err != nil {
				return err
			}
		default:
			dormant++
			if err := exec(`INSERT INTO continuous_queries_dormant (query_id, name, description, database, source_measurement, destination_measurement,
				query, interval, tag_columns, retention_days, delete_source_after_days, is_active, last_processed_time, created_at, updated_at)
				SELECT id, name, description, database, source_measurement, destination_measurement, query, interval, tag_columns,
				retention_days, delete_source_after_days, is_active, last_processed_time, created_at, updated_at FROM continuous_queries WHERE id = ?`, id); err != nil {
				return err
			}
			if err := exec(`INSERT INTO continuous_query_executions_dormant (execution_row_id, query_id, execution_id, execution_time, status,
				start_time, end_time, records_read, records_written, execution_duration_seconds, error_message)
				SELECT id, query_id, execution_id, execution_time, status, start_time, end_time, records_read, records_written,
				execution_duration_seconds, error_message FROM continuous_query_executions WHERE query_id = ?`, id); err != nil {
				return err
			}
			if err := exec(`DELETE FROM continuous_query_executions WHERE query_id = ?`, id); err != nil {
				return err
			}
		}
		if err := exec(`DELETE FROM continuous_queries WHERE id = ?`, id); err != nil {
			return err
		}
		delete(local, id)
	}
	// A row whose name is unchanged is updated in place (the watermark moves
	// at every run): the foreign key is not touched. A renamed row is deleted
	// and inserted again, all deletes first, so two CQs that swapped names
	// never collide on the unique name.
	var rewrite []raft.CQEntry
	for _, e := range cqs {
		k, ok := local[e.ID]
		if ok && k == cqRowKey(e) {
			continue
		}
		if ok && localName[e.ID] == e.Name {
			tags, _ := encodeTagColumns(e.TagColumns)
			var lpt *string
			if e.LastProcessed != nil {
				s := e.LastProcessed.UTC().Format(time.RFC3339)
				lpt = &s
			}
			if err := exec(`UPDATE continuous_queries SET description = ?, database = ?, source_measurement = ?, destination_measurement = ?,
				query = ?, interval = ?, tag_columns = ?, retention_days = ?, delete_source_after_days = ?, is_active = ?,
				last_processed_time = ?, created_at = ?, updated_at = ? WHERE id = ?`,
				e.Description, e.Database, e.SourceMeasurement, e.DestinationMeasurement, e.Query, e.Interval, tags,
				e.RetentionDays, e.DeleteSourceAfterDays, e.IsActive, lpt,
				e.CreatedAt.UTC().Format(sqliteTimestamp), e.UpdatedAt.UTC().Format(sqliteTimestamp), e.ID); err != nil {
				return err
			}
			continue
		}
		rewrite = append(rewrite, e)
		if ok {
			if err := exec(`DELETE FROM continuous_queries WHERE id = ?`, e.ID); err != nil {
				return err
			}
		}
	}
	for _, e := range rewrite {
		tags, _ := encodeTagColumns(e.TagColumns)
		var lpt *string
		if e.LastProcessed != nil {
			s := e.LastProcessed.UTC().Format(time.RFC3339)
			lpt = &s
		}
		if err := exec(`INSERT INTO continuous_queries (id, name, description, database, source_measurement, destination_measurement,
			query, interval, tag_columns, retention_days, delete_source_after_days, is_active, last_processed_time, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			e.ID, e.Name, e.Description, e.Database, e.SourceMeasurement, e.DestinationMeasurement, e.Query, e.Interval, tags,
			e.RetentionDays, e.DeleteSourceAfterDays, e.IsActive, lpt,
			e.CreatedAt.UTC().Format(sqliteTimestamp), e.UpdatedAt.UTC().Format(sqliteTimestamp)); err != nil {
			return err
		}
	}
	for id := range owned {
		if _, ok := want[id]; !ok && id <= st.Seq {
			if err := exec(`DELETE FROM continuous_query_cluster_ids WHERE id = ?`, id); err != nil {
				return err
			}
		}
	}
	for id := range want {
		if !owned[id] {
			if err := exec(`INSERT INTO continuous_query_cluster_ids (id) VALUES (?)`, id); err != nil {
				return err
			}
		}
	}
	if !changed {
		return nil
	}
	if err := exec(`INSERT OR IGNORE INTO cq_cluster_state (name, value) VALUES ('owned', '1')`); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	h.setOwned()
	if dormant > 0 {
		h.logger.Warn().Int("dormant", dormant).
			Msg("Continuous queries this node held that the cluster did not adopt were moved to continuous_queries_dormant; they no longer run")
	}
	return nil
}

// initClusterTables creates the cluster mirror's tables and reads the marker.
func (h *ContinuousQueryHandler) initClusterTables() error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS continuous_queries_dormant (
			dormant_id INTEGER PRIMARY KEY AUTOINCREMENT,
			query_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			description TEXT,
			database TEXT,
			source_measurement TEXT,
			destination_measurement TEXT,
			query TEXT,
			interval TEXT,
			tag_columns TEXT,
			retention_days INTEGER,
			delete_source_after_days INTEGER,
			is_active BOOLEAN,
			last_processed_time TIMESTAMP,
			created_at TIMESTAMP,
			updated_at TIMESTAMP,
			dormant_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS continuous_query_executions_dormant (
			dormant_id INTEGER PRIMARY KEY AUTOINCREMENT,
			execution_row_id INTEGER NOT NULL,
			query_id INTEGER NOT NULL,
			execution_id TEXT,
			execution_time TIMESTAMP,
			status TEXT,
			start_time TIMESTAMP,
			end_time TIMESTAMP,
			records_read INTEGER,
			records_written INTEGER,
			execution_duration_seconds FLOAT,
			error_message TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS continuous_query_cluster_ids (id INTEGER PRIMARY KEY)`,
		`CREATE TABLE IF NOT EXISTS cq_cluster_state (name TEXT PRIMARY KEY, value TEXT)`,
		// The history is looked up by CQ (and the foreign key checked by it)
		// whenever a CQ row is deleted.
		`CREATE INDEX IF NOT EXISTS idx_cq_executions_query_id ON continuous_query_executions (query_id)`,
	} {
		if _, err := h.sqliteDB.Exec(q); err != nil {
			return fmt.Errorf("failed to create continuous query cluster table: %w", err)
		}
	}
	var owned int
	if err := h.sqliteDB.QueryRow(`SELECT COUNT(*) FROM cq_cluster_state WHERE name = 'owned'`).Scan(&owned); err != nil {
		return fmt.Errorf("failed to read cq_cluster_state: %w", err)
	}
	h.cl.owned = owned > 0
	return nil
}
