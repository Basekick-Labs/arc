package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/gofiber/fiber/v2"
)

var errCQAdopting = errors.New("continuous queries are being adopted by the cluster; retry shortly")
var errCQNotCaughtUp = errors.New("this node has not caught up with the cluster's continuous queries yet; retry")

func (h *ContinuousQueryHandler) adoptedCQSource() CQClusterSource {
	s := h.clusterSource()
	if s != nil && s.CQsSeeded() {
		return s
	}
	return nil
}

func (h *ContinuousQueryHandler) localCQWriteSource() (CQClusterSource, error) {
	s := h.clusterSource()
	if s == nil {
		return nil, nil
	}
	if s.CQCollecting() {
		return nil, errCQAdopting
	}
	if s.CQsSeeded() {
		return s, nil
	}
	if s.CQEnabled() {
		return nil, errCQNotCaughtUp
	}
	if h.cqClusterOwned.Load() {
		return nil, errCQNotCaughtUp
	}
	return nil, nil
}

func cqEntryToAPI(e raft.CQPolicyEntry) ContinuousQuery {
	q := ContinuousQuery{ID: e.ID, Version: e.Version, Name: e.Name, Description: e.Description, Database: e.Database, SourceMeasurement: e.SourceMeasurement, DestinationMeasurement: e.DestinationMeasurement, Query: e.Query, Interval: e.Interval, TagColumns: append([]string(nil), e.TagColumns...), RetentionDays: e.RetentionDays, DeleteSourceAfterDays: e.DeleteSourceAfterDays, IsActive: e.IsActive, CreatedAt: e.CreatedAt.UTC().Format(time.RFC3339Nano), UpdatedAt: e.UpdatedAt.UTC().Format(time.RFC3339Nano)}
	if e.LastProcessedTime != nil {
		v := e.LastProcessedTime.UTC().Format(time.RFC3339Nano)
		q.LastProcessedTime = &v
	}
	return q
}

func cqEntryFromAPI(q ContinuousQuery) raft.CQPolicyEntry {
	var last *time.Time
	if q.LastProcessedTime != nil {
		if t, err := time.Parse(time.RFC3339Nano, *q.LastProcessedTime); err == nil {
			v := t.UTC()
			last = &v
		}
	}
	return raft.CQPolicyEntry{ID: q.ID, Version: q.Version, Name: q.Name, Description: q.Description, Database: q.Database, SourceMeasurement: q.SourceMeasurement, DestinationMeasurement: q.DestinationMeasurement, Query: q.Query, Interval: q.Interval, TagColumns: append([]string(nil), q.TagColumns...), RetentionDays: q.RetentionDays, DeleteSourceAfterDays: q.DeleteSourceAfterDays, IsActive: q.IsActive, LastProcessedTime: last}
}

// LocalPolicyLister returns legacy CQ definitions and whether each actually
// completed on this node. Completed execution history is local by design.
func (h *ContinuousQueryHandler) LocalPolicyLister(ctx context.Context) (json.RawMessage, error) {
	h.cqLocalMu.Lock()
	defer h.cqLocalMu.Unlock()
	rows, err := h.sqliteDB.QueryContext(ctx, `SELECT id,name,description,database,source_measurement,destination_measurement,query,interval,tag_columns,retention_days,delete_source_after_days,is_active,last_processed_time,created_at,updated_at FROM continuous_queries`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type local struct {
		Policies    []raft.CQPolicyEntry `json:"policies"`
		MaxIssuedID int64                `json:"max_issued_id,omitempty"`
		Completed   map[int64]bool       `json:"completed,omitempty"`
	}
	out := local{Completed: map[int64]bool{}}
	for rows.Next() {
		var q ContinuousQuery
		var tags *string
		var lp *string
		if err := rows.Scan(&q.ID, &q.Name, &q.Description, &q.Database, &q.SourceMeasurement, &q.DestinationMeasurement, &q.Query, &q.Interval, &tags, &q.RetentionDays, &q.DeleteSourceAfterDays, &q.IsActive, &lp, &q.CreatedAt, &q.UpdatedAt); err != nil {
			return nil, err
		}
		if tags != nil {
			_ = json.Unmarshal([]byte(*tags), &q.TagColumns)
		}
		q.LastProcessedTime = lp
		e := cqEntryFromAPI(q)
		e.CreatedAt = parseCQTime(q.CreatedAt)
		e.UpdatedAt = parseCQTime(q.UpdatedAt)
		out.MaxIssuedID = max(out.MaxIssuedID, q.ID)
		out.Policies = append(out.Policies, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var seq sql.NullInt64
	if err := h.sqliteDB.QueryRowContext(ctx, `SELECT seq FROM sqlite_sequence WHERE name='continuous_queries'`).Scan(&seq); err == nil && seq.Valid {
		out.MaxIssuedID = max(out.MaxIssuedID, seq.Int64)
	}
	rows, err = h.sqliteDB.QueryContext(ctx, `SELECT query_id FROM continuous_query_executions WHERE status='completed' GROUP BY query_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out.Completed[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(out)
	return b, err
}

func parseCQTime(v string) time.Time {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05", "2006-01-02 15:04:05.999999999-07:00"} {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

// MirrorClusterCQs projects committed Raft state into the local SQLite table.
// It keeps excluded legacy definitions and their history in dormant tables.
func (h *ContinuousQueryHandler) MirrorClusterCQs(ctx context.Context) error {
	h.cqLocalMu.Lock()
	defer h.cqLocalMu.Unlock()
	return h.mirrorClusterCQs(ctx)
}

// mirrorClusterCQs is used by the CRUD handlers that already hold cqLocalMu.
func (h *ContinuousQueryHandler) mirrorClusterCQs(ctx context.Context) error {
	s := h.adoptedCQSource()
	if s == nil {
		return nil
	}
	h.cqMirrorMu.Lock()
	defer h.cqMirrorMu.Unlock()
	st := s.CQMirrorState()
	conn, err := h.sqliteDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS continuous_queries_dormant AS SELECT * FROM continuous_queries WHERE 0`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS continuous_query_executions_dormant AS SELECT * FROM continuous_query_executions WHERE 0`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cq_cluster_mirror_state (id INTEGER PRIMARY KEY CHECK(id=1), owned BOOLEAN NOT NULL, epoch INTEGER NOT NULL)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cq_cluster_owned_ids (id INTEGER PRIMARY KEY)`); err != nil {
		return err
	}
	previouslyOwned := map[int64]bool{}
	ownedRows, err := tx.QueryContext(ctx, `SELECT id FROM cq_cluster_owned_ids`)
	if err != nil {
		return err
	}
	for ownedRows.Next() {
		var id int64
		if err := ownedRows.Scan(&id); err != nil {
			ownedRows.Close()
			return err
		}
		previouslyOwned[id] = true
	}
	ownedRows.Close()
	if err := ownedRows.Err(); err != nil {
		return err
	}
	keep := map[int64]bool{}
	localSources := map[int64]int64{}
	localNode := s.LocalNodeID()
	for _, e := range st.Entries {
		keep[e.ID] = true
		for _, source := range e.Sources {
			if source.Node == localNode {
				localSources[e.ID] = source.ID
				keep[source.ID] = true
				break
			}
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM continuous_queries`)
	if err != nil {
		return err
	}
	var stale []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		if !keep[id] {
			stale = append(stale, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range stale {
		if !previouslyOwned[id] {
			if _, err = tx.ExecContext(ctx, `INSERT INTO continuous_queries_dormant SELECT * FROM continuous_queries WHERE id=?`, id); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO continuous_query_executions_dormant SELECT * FROM continuous_query_executions WHERE query_id=?`, id); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM continuous_query_executions WHERE query_id=?`, id); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM continuous_queries WHERE id=?`, id); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM cq_cluster_owned_ids`); err != nil {
		return err
	}
	for _, e := range st.Entries {
		if _, err = tx.ExecContext(ctx, `INSERT INTO cq_cluster_owned_ids(id) VALUES(?)`, e.ID); err != nil {
			return err
		}
	}
	for _, e := range st.Entries {
		sourceID := localSources[e.ID]
		migrateSource := false
		if sourceID > 0 && sourceID != e.ID {
			var sourceName string
			sourceErr := tx.QueryRowContext(ctx, `SELECT name FROM continuous_queries WHERE id=?`, sourceID).Scan(&sourceName)
			if sourceErr == nil && sourceName == e.Name {
				// The old local name is unique. Temporarily rename it so the new
				// canonical row can be inserted before its history is re-keyed.
				if _, err := tx.ExecContext(ctx, `UPDATE continuous_queries SET name=? WHERE id=?`, fmt.Sprintf("%s.__cq-source-%d", sourceName, e.ID), sourceID); err != nil {
					return err
				}
				migrateSource = true
			} else if sourceErr != nil && sourceErr != sql.ErrNoRows {
				return sourceErr
			}
		}
		// A local row can share the cluster ID while representing another
		// definition. Preserve that row and its history before replacing it.
		localTarget, targetErr := localCQEntryByID(ctx, tx, e.ID)
		if targetErr == nil && !sameCQDefinitionEntries(localTarget, e) {
			if err := moveCQToDormant(ctx, tx, e.ID); err != nil {
				return err
			}
		} else if targetErr != nil && targetErr != sql.ErrNoRows {
			return targetErr
		}
		tagJSON, _ := json.Marshal(e.TagColumns)
		var tag any
		if len(e.TagColumns) > 0 {
			tag = string(tagJSON)
		}
		var desc any
		if e.Description != nil {
			desc = *e.Description
		}
		var retention, deleteAfter any
		if e.RetentionDays != nil {
			retention = *e.RetentionDays
		}
		if e.DeleteSourceAfterDays != nil {
			deleteAfter = *e.DeleteSourceAfterDays
		}
		var last any
		if e.LastProcessedTime != nil {
			last = e.LastProcessedTime.UTC().Format(time.RFC3339Nano)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO continuous_queries (id,name,description,database,source_measurement,destination_measurement,query,interval,tag_columns,retention_days,delete_source_after_days,is_active,last_processed_time,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,description=excluded.description,database=excluded.database,source_measurement=excluded.source_measurement,destination_measurement=excluded.destination_measurement,query=excluded.query,interval=excluded.interval,tag_columns=excluded.tag_columns,retention_days=excluded.retention_days,delete_source_after_days=excluded.delete_source_after_days,is_active=excluded.is_active,last_processed_time=excluded.last_processed_time,created_at=excluded.created_at,updated_at=excluded.updated_at`, e.ID, e.Name, desc, e.Database, e.SourceMeasurement, e.DestinationMeasurement, e.Query, e.Interval, tag, retention, deleteAfter, e.IsActive, last, e.CreatedAt.UTC().Format("2006-01-02 15:04:05"), e.UpdatedAt.UTC().Format("2006-01-02 15:04:05"))
		if err != nil {
			return err
		}
		// When adoption assigned a collision-free cluster ID, carry this
		// node's execution history to that ID if it belongs to this definition.
		if migrateSource {
			if _, err := tx.ExecContext(ctx, `INSERT INTO continuous_query_executions (query_id,execution_id,execution_time,status,start_time,end_time,records_read,records_written,execution_duration_seconds,error_message) SELECT ?,execution_id,execution_time,status,start_time,end_time,records_read,records_written,execution_duration_seconds,error_message FROM continuous_query_executions WHERE query_id=?`, e.ID, sourceID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM continuous_query_executions WHERE query_id=?`, sourceID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM continuous_queries WHERE id=?`, sourceID); err != nil {
				return err
			}
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO cq_cluster_mirror_state(id,owned,epoch) VALUES(1,1,?) ON CONFLICT(id) DO UPDATE SET owned=1,epoch=excluded.epoch`, st.Epoch); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	h.cqClusterOwned.Store(true)
	var scheduleHash strings.Builder
	for _, e := range st.Entries {
		fmt.Fprintf(&scheduleHash, "%d:%d;", e.ID, e.Version)
	}
	newHash := scheduleHash.String()
	changed := newHash != h.cqMirrorHash
	h.cqMirrorHash = newHash
	if changed && h.scheduler != nil {
		if all, ok := h.scheduler.(interface{ ReloadAll() error }); ok {
			go func() {
				if err := all.ReloadAll(); err != nil {
					h.logger.Warn().Err(err).Msg("Failed to reload CQ schedules after cluster mirror")
				}
			}()
		}
	}
	return nil
}

func localCQEntryByID(ctx context.Context, tx *sql.Tx, id int64) (raft.CQPolicyEntry, error) {
	var q ContinuousQuery
	var tagColumnsRaw *string
	err := tx.QueryRowContext(ctx, `SELECT name,description,database,source_measurement,destination_measurement,query,interval,tag_columns,retention_days,delete_source_after_days,is_active,last_processed_time,created_at,updated_at FROM continuous_queries WHERE id=?`, id).Scan(&q.Name, &q.Description, &q.Database, &q.SourceMeasurement, &q.DestinationMeasurement, &q.Query, &q.Interval, &tagColumnsRaw, &q.RetentionDays, &q.DeleteSourceAfterDays, &q.IsActive, &q.LastProcessedTime, &q.CreatedAt, &q.UpdatedAt)
	if err != nil {
		return raft.CQPolicyEntry{}, err
	}
	q.TagColumns, _ = decodeTagColumns(tagColumnsRaw)
	return cqEntryFromAPI(q), nil
}

func sameCQDefinitionEntries(a, b raft.CQPolicyEntry) bool {
	a.ID, b.ID = 0, 0
	a.Version, b.Version = 0, 0
	a.CreatedAt, b.CreatedAt = time.Time{}, time.Time{}
	a.UpdatedAt, b.UpdatedAt = time.Time{}, time.Time{}
	a.LastProcessedTime, b.LastProcessedTime = nil, nil
	a.Sources, b.Sources = nil, nil
	return reflect.DeepEqual(a, b)
}

func (h *ContinuousQueryHandler) clusterQueries(src CQClusterSource, database, isActiveStr string) ([]ContinuousQuery, error) {
	if err := src.CQReadBarrier(context.Background()); err != nil {
		return nil, fmt.Errorf("continuous query read barrier: %w", err)
	}
	entries := src.CQs()
	sort.Slice(entries, func(i, j int) bool { return entries[i].CreatedAt.After(entries[j].CreatedAt) })
	out := make([]ContinuousQuery, 0, len(entries))
	for _, e := range entries {
		if database != "" && e.Database != database {
			continue
		}
		if isActiveStr != "" && e.IsActive != (isActiveStr == "true") {
			continue
		}
		q := cqEntryToAPI(e)
		// Execution history remains local; it enriches the cluster definition
		// but never participates in its authority or watermark decisions.
		var executionTime, status sql.NullString
		var recordsWritten sql.NullInt64
		if err := h.sqliteDB.QueryRow(`SELECT execution_time,status,records_written FROM continuous_query_executions WHERE id=(SELECT MAX(id) FROM continuous_query_executions WHERE query_id=?)`, e.ID).Scan(&executionTime, &status, &recordsWritten); err == nil {
			if executionTime.Valid {
				q.LastExecutionTime = &executionTime.String
			}
			if status.Valid {
				q.LastExecutionStatus = &status.String
			}
			if recordsWritten.Valid {
				q.LastRecordsWritten = &recordsWritten.Int64
			}
		}
		out = append(out, q)
	}
	return out, nil
}

func cqUpdatedEntry(current *ContinuousQuery, req *ContinuousQueryRequest) raft.CQPolicyEntry {
	q := *current
	if req.Name.Set {
		q.Name = req.Name.Value
	}
	if req.Description.Set {
		q.Description = req.Description.ptr()
	}
	if req.Database.Set {
		q.Database = req.Database.Value
	}
	if req.SourceMeasurement.Set {
		q.SourceMeasurement = req.SourceMeasurement.Value
	}
	if req.DestinationMeasurement.Set {
		q.DestinationMeasurement = req.DestinationMeasurement.Value
	}
	if req.Query.Set {
		q.Query = req.Query.Value
	}
	if req.Interval.Set {
		q.Interval = req.Interval.Value
	}
	if req.TagColumns.Set {
		q.TagColumns = req.TagColumns.Value
	}
	if req.RetentionDays.Set {
		q.RetentionDays = req.RetentionDays.ptr()
	}
	if req.DeleteSourceAfterDays.Set {
		q.DeleteSourceAfterDays = req.DeleteSourceAfterDays.ptr()
	}
	if req.IsActive.Set {
		q.IsActive = req.IsActive.Value
	}
	return cqEntryFromAPI(q)
}

func moveCQToDormant(ctx context.Context, tx *sql.Tx, id int64) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO continuous_queries_dormant SELECT * FROM continuous_queries WHERE id=?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO continuous_query_executions_dormant SELECT * FROM continuous_query_executions WHERE query_id=?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM continuous_query_executions WHERE query_id=?`, id); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM continuous_queries WHERE id=?`, id)
	return err
}

func (h *ContinuousQueryHandler) cqClusterWriteError(c *fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, raft.ErrCQNotFound):
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
	case errors.Is(err, raft.ErrCQNameTaken):
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	case errors.Is(err, raft.ErrCQConflict):
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": err.Error()})
	case errors.Is(err, raft.ErrCQTooLarge):
		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{"error": err.Error()})
	default:
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": err.Error()})
	}
}
