package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/gofiber/fiber/v2"
)

// Retention policies in a cluster (#1324 stage 2).
//
// Until the cluster adopts the nodes' retention policies, they stay in each
// node's own SQLite as before. Once it has, the definitions live in the
// cluster state: every read and every run takes them from there, writes are
// proposed to it, and this node's own table is a mirror kept for backups and
// for the execution history's foreign key. Execution history stays per node.

// RetentionClusterSource is the cluster state retention reads and writes once
// adopted. *cluster.Coordinator satisfies it.
type RetentionClusterSource interface {
	RetentionPoliciesSeeded() bool
	RetentionPoliciesCollecting() bool
	RetentionPolicy(id int64) (raft.RetentionPolicyEntry, bool)
	RetentionPolicyByName(name string) (raft.RetentionPolicyEntry, bool)
	RetentionPolicies() []raft.RetentionPolicyEntry
	CreateRetentionPolicy(ctx context.Context, p raft.CreateRetentionPolicyPayload) error
	UpdateRetentionPolicy(ctx context.Context, p raft.UpdateRetentionPolicyPayload) error
	DeleteRetentionPolicy(ctx context.Context, id int64) error
	RetentionMirrorState() raft.RetentionMirror
	LocalNodeID() string
}

// SetClusterSource makes retention cluster-wide once the cluster adopts it.
// Called after construction in cluster mode.
func (h *RetentionHandler) SetClusterSource(src RetentionClusterSource) {
	h.clusterMu.Lock()
	defer h.clusterMu.Unlock()
	h.cluster = src
}

func (h *RetentionHandler) clusterSource() RetentionClusterSource {
	h.clusterMu.RLock()
	defer h.clusterMu.RUnlock()
	return h.cluster
}

// adoptedSource returns the cluster source when retention policies live in
// the cluster state, nil while they are this node's own.
func (h *RetentionHandler) adoptedSource() RetentionClusterSource {
	if src := h.clusterSource(); src != nil && src.RetentionPoliciesSeeded() {
		return src
	}
	return nil
}

// errRetentionBeingAdopted: a local write during the cluster's collect.
var errRetentionBeingAdopted = errors.New("retention policies are being adopted by the cluster; retry in a few seconds")

// localWrite runs fn on this node's own table while retention is not adopted,
// under localMu so the collect's read of the table sees it or refuses it. It
// returns adopted when the cluster holds retention instead (fn not run), and
// errRetentionBeingAdopted during a collect. Collecting is read before
// seeded: the seed ends the collect, so the other order could see neither.
func (h *RetentionHandler) localWrite(fn func() error) (adopted RetentionClusterSource, err error) {
	h.localMu.Lock()
	defer h.localMu.Unlock()
	if src := h.clusterSource(); src != nil {
		if src.RetentionPoliciesCollecting() {
			return nil, errRetentionBeingAdopted
		}
		if src.RetentionPoliciesSeeded() {
			return src, nil
		}
	}
	return nil, fn()
}

// retentionTimestamp is the API form of a timestamp, as SQLite's TIMESTAMP
// columns have always been returned.
func retentionTimestamp(t time.Time) string {
	return t.UTC().Truncate(time.Second).Format(time.RFC3339Nano)
}

// policyFromEntry is a cluster policy in the API's shape, with this node's
// last execution of it.
func policyFromEntry(e raft.RetentionPolicyEntry, last *lastExecution) RetentionPolicy {
	p := RetentionPolicy{
		ID: e.ID, Name: e.Name, Database: e.Database, Measurement: e.Measurement,
		RetentionDays: e.RetentionDays, BufferDays: e.BufferDays, IsActive: e.IsActive,
		CreatedAt: retentionTimestamp(e.CreatedAt), UpdatedAt: retentionTimestamp(e.UpdatedAt),
	}
	if last != nil {
		p.LastExecutionTime, p.LastExecutionStatus, p.LastDeletedCount = last.time, last.status, last.deleted
	}
	return p
}

type lastExecution struct {
	time, status *string
	deleted      *int64
}

// lastExecutions returns this node's latest execution of each policy.
func (h *RetentionHandler) lastExecutions(ids ...int64) (map[int64]*lastExecution, error) {
	q := `SELECT policy_id, execution_time, status, deleted_count FROM retention_executions
		WHERE id IN (SELECT MAX(id) FROM retention_executions GROUP BY policy_id)`
	var args []interface{}
	if len(ids) == 1 {
		q = `SELECT policy_id, execution_time, status, deleted_count FROM retention_executions
			WHERE id = (SELECT MAX(id) FROM retention_executions WHERE policy_id = ?)`
		args = append(args, ids[0])
	}
	rows, err := h.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]*lastExecution{}
	for rows.Next() {
		var id int64
		l := &lastExecution{}
		if err := rows.Scan(&id, &l.time, &l.status, &l.deleted); err != nil {
			continue
		}
		out[id] = l
	}
	return out, rows.Err()
}

// clusterPolicy is getPolicy once adopted. A policy the cluster does not hold
// is not found: it never falls back to a row of this node's own.
func (h *RetentionHandler) clusterPolicy(src RetentionClusterSource, id int64) (*RetentionPolicy, error) {
	e, ok := src.RetentionPolicy(id)
	if !ok {
		return nil, sql.ErrNoRows
	}
	last, err := h.lastExecutions(id)
	if err != nil {
		h.logger.Warn().Err(err).Msg("Could not read this node's retention execution history")
	}
	p := policyFromEntry(e, last[id])
	return &p, nil
}

// clusterPolicies is getPolicies once adopted, newest first as before.
func (h *RetentionHandler) clusterPolicies(src RetentionClusterSource, activeOnly bool) ([]RetentionPolicy, error) {
	entries := src.RetentionPolicies()
	last, err := h.lastExecutions()
	if err != nil {
		h.logger.Warn().Err(err).Msg("Could not read this node's retention execution history")
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if !entries[i].CreatedAt.Equal(entries[j].CreatedAt) {
			return entries[i].CreatedAt.After(entries[j].CreatedAt)
		}
		return entries[i].ID > entries[j].ID
	})
	var out []RetentionPolicy
	for _, e := range entries {
		if activeOnly && !e.IsActive {
			continue
		}
		out = append(out, policyFromEntry(e, last[e.ID]))
	}
	return out, nil
}

// clusterWriteError answers a cluster write's error. A refusal reaches a
// follower as the leader's text, so the FSM's errors are recognised by text.
func (h *RetentionHandler) clusterWriteError(c *fiber.Ctx, err error, nameTaken string) error {
	switch {
	case raft.IsPolicyNotFound(err):
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Retention policy not found"})
	case raft.IsPolicyNameTaken(err):
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": nameTaken})
	case raft.IsRetentionNotAboveBuffer(err):
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": raft.ErrRetentionNotAboveBuffer.Error()})
	case errors.Is(err, raft.ErrInvalidRetentionPolicy), err != nil && strings.Contains(err.Error(), raft.ErrInvalidRetentionPolicy.Error()):
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	h.logger.Warn().Err(err).Msg("The cluster did not take the retention policy write")
	return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
		"error": "the cluster could not take the retention policy write right now; retry",
	})
}

// clusterCreate creates a policy in the cluster state and answers with it.
func (h *RetentionHandler) clusterCreate(c *fiber.Ctx, src RetentionClusterSource, req *RetentionPolicyRequest) error {
	err := src.CreateRetentionPolicy(c.UserContext(), raft.CreateRetentionPolicyPayload{
		Name: req.Name.Value, Database: req.Database.Value, Measurement: measurementColumn(req.Measurement),
		RetentionDays: req.RetentionDays.Value, BufferDays: req.BufferDays.Value, IsActive: req.IsActive.Value,
	})
	if err != nil {
		return h.clusterWriteError(c, err, fmt.Sprintf("Retention policy with name '%s' already exists", req.Name.Value))
	}
	// Names are unique in the cluster state, and the write has been applied
	// here by now (a forwarded write waits for this node to apply it).
	e, ok := src.RetentionPolicyByName(req.Name.Value)
	if !ok {
		return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
			"message": "Retention policy created; this node has not applied it yet, list the policies to see it",
		})
	}
	h.logger.Info().Int64("policy_id", e.ID).Str("name", e.Name).Msg("Created retention policy in the cluster")
	p, err := h.clusterPolicy(src, e.ID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to retrieve created policy"})
	}
	return c.Status(fiber.StatusCreated).JSON(p)
}

// clusterUpdate applies a partial update in the cluster state: the fields the
// body carries are merged with the policy as it is when the update applies.
func (h *RetentionHandler) clusterUpdate(c *fiber.Ctx, src RetentionClusterSource, id int64, req *RetentionPolicyRequest) error {
	p := raft.UpdateRetentionPolicyPayload{ID: id}
	if req.Name.Set {
		p.Name = &req.Name.Value
	}
	if req.Database.Set {
		p.Database = &req.Database.Value
	}
	if req.Measurement.Set {
		p.SetMeasurement = true
		p.Measurement = measurementColumn(req.Measurement)
	}
	if req.RetentionDays.Set {
		p.RetentionDays = &req.RetentionDays.Value
	}
	if req.BufferDays.Set {
		p.BufferDays = &req.BufferDays.Value
	}
	if req.IsActive.Set {
		p.IsActive = &req.IsActive.Value
	}
	if err := src.UpdateRetentionPolicy(c.UserContext(), p); err != nil {
		return h.clusterWriteError(c, err, fmt.Sprintf("Retention policy with name %q already exists", req.Name.Value))
	}
	h.logger.Info().Int64("policy_id", id).Msg("Updated retention policy in the cluster")
	policy, err := h.clusterPolicy(src, id)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Retention policy not found"})
	}
	return c.JSON(policy)
}

// clusterDelete removes a policy from the cluster state. This node's mirror
// then removes its row and execution history, as a local delete does.
func (h *RetentionHandler) clusterDelete(c *fiber.Ctx, src RetentionClusterSource, id int64) error {
	if err := src.DeleteRetentionPolicy(c.UserContext(), id); err != nil {
		return h.clusterWriteError(c, err, "")
	}
	h.logger.Info().Int64("policy_id", id).Msg("Deleted retention policy in the cluster")
	return c.JSON(fiber.Map{"message": "Retention policy deleted successfully"})
}

// LocalPolicyLister reads this node's own retention policies for the
// cluster's adoption, with how often this node ran each, under localMu so a
// local write is either in the answer or refused by the collect.
func (h *RetentionHandler) LocalPolicyLister() cluster.LocalPolicyLister {
	return func(ctx context.Context) (json.RawMessage, error) {
		h.localMu.Lock()
		defer h.localMu.Unlock()
		rows, err := h.db.QueryContext(ctx, `
			SELECT rp.id, rp.name, rp.database, rp.measurement, rp.retention_days, rp.buffer_days, rp.is_active,
				rp.created_at, rp.updated_at, (SELECT COUNT(*) FROM retention_executions re WHERE re.policy_id = rp.id)
			FROM retention_policies rp ORDER BY rp.id`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := cluster.RetentionLocalRows{Policies: []cluster.RetentionLocalRow{}}
		for rows.Next() {
			var r cluster.RetentionLocalRow
			var measurement sql.NullString
			var buffer sql.NullInt64
			var active sql.NullBool
			var created, updated sql.NullTime
			if err := rows.Scan(&r.ID, &r.Name, &r.Database, &measurement, &r.RetentionDays, &buffer, &active,
				&created, &updated, &r.Executions); err != nil {
				// Not skipped: a row missing from the answer would be dropped
				// by the adoption.
				return nil, fmt.Errorf("read retention policy row: %w", err)
			}
			if measurement.Valid && measurement.String != "" {
				m := measurement.String
				r.Measurement = &m
			}
			r.BufferDays = int(buffer.Int64)
			r.IsActive = active.Valid && active.Bool
			r.CreatedAt, r.UpdatedAt = created.Time.UTC(), updated.Time.UTC()
			out.Policies = append(out.Policies, r)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(out.Policies) > raft.MaxSeedRetentionPolicies {
			return nil, fmt.Errorf("%d retention policies exceed the %d the cluster adopts", len(out.Policies), raft.MaxSeedRetentionPolicies)
		}
		return json.Marshal(out)
	}
}

// MirrorClusterPolicies makes this node's own table equal the cluster's
// retention policies, once adopted (a no-op before). See reconcileMirror.
func (h *RetentionHandler) MirrorClusterPolicies(ctx context.Context) error {
	src := h.adoptedSource()
	if src == nil {
		return nil
	}
	h.mirrorMu.Lock()
	defer h.mirrorMu.Unlock()
	return h.reconcileMirror(ctx, src.RetentionMirrorState(), src.LocalNodeID())
}

// localRow is a row of this node's retention_policies, for the mirror.
type localRow struct {
	id                        int64
	name, database            string
	measurement               sql.NullString
	retentionDays, bufferDays int64
	isActive                  bool
	createdAt, updatedAt      sql.NullTime
}

func (r localRow) equals(e raft.RetentionPolicyEntry) bool {
	m, lm := "", ""
	if e.Measurement != nil {
		m = *e.Measurement
	}
	if r.measurement.Valid {
		lm = r.measurement.String
	}
	return r.id == e.ID && r.name == e.Name && r.database == e.Database && lm == m &&
		r.retentionDays == int64(e.RetentionDays) && r.bufferDays == int64(e.BufferDays) && r.isActive == e.IsActive &&
		r.createdAt.Time.Equal(e.CreatedAt.UTC().Truncate(time.Second)) && r.updatedAt.Time.Equal(e.UpdatedAt.UTC().Truncate(time.Second))
}

// sqliteTimestamp is the layout SQLite's CURRENT_TIMESTAMP writes.
const sqliteTimestamp = "2006-01-02 15:04:05"

// reconcileMirror rewrites this node's retention_policies to the cluster's,
// in one transaction on one connection (the deferred foreign keys are per
// connection; on the shared auth database, a second statement outside it
// would wait for this one's connection forever).
//
// A row the cluster does not hold is, by its ID:
//   - a source of a seeded policy on this node: its execution history moves
//     to the policy's new ID, and the row goes;
//   - a policy the cluster held and deleted (seeded, or at or above the
//     seed's floor): it goes with its history, as a delete always did;
//   - otherwise a row from before the seed that the cluster did not adopt:
//     it moves, with its history, to the dormant tables. It never runs again
//     and nothing is lost.
func (h *RetentionHandler) reconcileMirror(ctx context.Context, st raft.RetentionMirror, self string) error {
	conn, err := h.db.Conn(ctx)
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
	rows, err := tx.QueryContext(ctx, `SELECT id, name, database, measurement, retention_days, COALESCE(buffer_days, 0),
		COALESCE(is_active, 0), created_at, updated_at FROM retention_policies`)
	if err != nil {
		return err
	}
	local := map[int64]localRow{}
	for rows.Next() {
		var r localRow
		if err := rows.Scan(&r.id, &r.name, &r.database, &r.measurement, &r.retentionDays, &r.bufferDays, &r.isActive, &r.createdAt, &r.updatedAt); err != nil {
			rows.Close()
			return fmt.Errorf("read retention policy row: %w", err)
		}
		local[r.id] = r
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	want := make(map[int64]raft.RetentionPolicyEntry, len(st.Policies))
	movedTo := map[int64]int64{} // this node's source row -> the seeded policy
	for _, e := range st.Policies {
		want[e.ID] = e
		for _, s := range e.Sources {
			if s.Node == self && s.ID != e.ID {
				movedTo[s.ID] = e.ID
			}
		}
	}

	changed := false
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
		if _, ok := want[id]; ok {
			continue
		}
		switch to, moved := movedTo[id]; {
		case moved:
			if err := exec(`UPDATE retention_executions SET policy_id = ? WHERE policy_id = ?`, to, id); err != nil {
				return err
			}
		case st.SeededIDs[id] || id >= st.Floor:
			if err := exec(`DELETE FROM retention_executions WHERE policy_id = ?`, id); err != nil {
				return err
			}
		default:
			dormant++
			if err := exec(`INSERT INTO retention_policies_dormant (policy_id, name, database, measurement, retention_days, buffer_days, is_active, created_at, updated_at)
				SELECT id, name, database, measurement, retention_days, buffer_days, is_active, created_at, updated_at FROM retention_policies WHERE id = ?`, id); err != nil {
				return err
			}
			if err := exec(`INSERT INTO retention_executions_dormant (execution_id, policy_id, execution_time, status, deleted_count, cutoff_date, execution_duration_ms, error_message)
				SELECT id, policy_id, execution_time, status, deleted_count, cutoff_date, execution_duration_ms, error_message FROM retention_executions WHERE policy_id = ?`, id); err != nil {
				return err
			}
			if err := exec(`DELETE FROM retention_executions WHERE policy_id = ?`, id); err != nil {
				return err
			}
		}
		if err := exec(`DELETE FROM retention_policies WHERE id = ?`, id); err != nil {
			return err
		}
	}
	// Rewrite every row that differs: deleted first, then inserted, so two
	// policies that swapped names never collide on the unique name.
	var rewrite []raft.RetentionPolicyEntry
	for _, e := range st.Policies {
		if r, ok := local[e.ID]; ok && r.equals(e) {
			continue
		}
		rewrite = append(rewrite, e)
		if _, ok := local[e.ID]; ok {
			if err := exec(`DELETE FROM retention_policies WHERE id = ?`, e.ID); err != nil {
				return err
			}
		}
	}
	for _, e := range rewrite {
		if err := exec(`INSERT INTO retention_policies (id, name, database, measurement, retention_days, buffer_days, is_active, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			e.ID, e.Name, e.Database, e.Measurement, e.RetentionDays, e.BufferDays, e.IsActive,
			e.CreatedAt.UTC().Format(sqliteTimestamp), e.UpdatedAt.UTC().Format(sqliteTimestamp)); err != nil {
			return err
		}
	}
	if !changed {
		return nil
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if dormant > 0 {
		h.logger.Warn().Int("dormant", dormant).
			Msg("Retention policies this node held that the cluster did not adopt were moved to retention_policies_dormant; they no longer run")
	}
	return nil
}

// ensureMirrored makes this node's table hold the cluster's policies before an
// execution is recorded against one: the history's foreign key needs the row,
// and the mirror worker may not have run yet.
func (h *RetentionHandler) ensureMirrored(ctx context.Context) {
	if err := h.MirrorClusterPolicies(ctx); err != nil {
		h.logger.Warn().Err(err).Msg("Could not mirror the cluster's retention policies before recording a run")
	}
}
