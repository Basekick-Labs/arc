package auth

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Reconciling the local auth and RBAC tables with the cluster FSM (#1336).
//
// Every node authenticates against its own SQLite, which follows the Raft FSM
// only through the per-command apply callbacks. Two paths change the FSM
// without firing them: a snapshot install (ClusterFSM.Restore) and everything
// the FSM applies at boot before cmd/arc wires the callbacks. A node that took
// either path kept revoked, deleted or rotated credentials, and RBAC grants
// the cluster had withdrawn. ReconcileClusterState rewrites the tables from a
// copy of the FSM, under rules that never touch a row the cluster does not own.
//
// Ownership is the raft_materialised marker. A marked row is the cluster's: it
// follows the FSM, and is deleted when the FSM no longer has it. An unmarked
// row is local — a pre-26.06.1 token, or one created while the node ran
// standalone after its coordinator failed to start — and is only reported. An
// unmarked row is adopted (marked) when it is evidently the FSM entry with the
// same id: same name and same hash or creation instant for a token, the same
// identity the create applies compare for RBAC.
//
// Deletes and every narrowing change run in a first transaction, inserts and
// renames in a second, so a constraint failure while converging can never undo
// a revoke. SQLite's default ON CONFLICT ABORT rolls back only the failing
// statement: each error is reported and the run continues.
//
// All I/O goes through the transaction. The auth DB has one connection, so a
// call through *sql.DB from inside a transaction would wait forever.

// ClusterAuthState is a copy of the FSM's auth and RBAC maps.
type ClusterAuthState struct {
	Tokens                 []ClusterTokenEntry
	Organizations          []ClusterOrganizationEntry
	Teams                  []ClusterTeamEntry
	Roles                  []ClusterRoleEntry
	MeasurementPermissions []ClusterMeasurementPermissionEntry
	Memberships            []ClusterTokenMembershipEntry
	// IncludeRBAC is false where this node has no RBAC manager; the RBAC
	// tables are then left as they are.
	IncludeRBAC bool
	// Quarantined holds, per table, the ids a snapshot restore refused. Their
	// rows are kept: the FSM lost them through validation, not a delete.
	Quarantined map[string]map[int64]bool
	// AppliedIndex is the FSM's applied index. Token and RBAC ids are Raft
	// log indexes, so a marked row with an id above it cannot come from this
	// history and is kept. 0 means unknown: nothing is deleted.
	AppliedIndex uint64
	// Term is the Raft term. 0 (a snapshot install) skips the rebuilt-history
	// check, which only a boot can trip.
	Term uint64
}

// Table names, used as report keys and in Quarantined.
const (
	TableTokens                 = "api_tokens"
	TableOrganizations          = "rbac_organizations"
	TableTeams                  = "rbac_teams"
	TableRoles                  = "rbac_roles"
	TableMeasurementPermissions = "rbac_measurement_permissions"
	TableMemberships            = "rbac_token_memberships"
)

// ReconcileTableReport counts what one table's reconcile did.
type ReconcileTableReport struct {
	Inserted     int
	Updated      int
	Deleted      int
	Adopted      int
	Collisions   int // an FSM entry blocked by a local row it cannot take over
	Unreplicated int // local rows the cluster does not know (left in place)
	KeptDisabled int // FSM enabled, local copy disabled: kept disabled (#1336 D4)
	Protected    int // marked rows the FSM lacks but which were kept
	Errors       int
}

// ReconcileReport is the outcome of one ReconcileClusterState.
type ReconcileReport struct {
	Tables         map[string]*ReconcileTableReport
	HistoryRebuilt bool
	BreakerTripped []string
	Duration       time.Duration
}

func (r *ReconcileReport) table(name string) *ReconcileTableReport {
	if r.Tables[name] == nil {
		r.Tables[name] = &ReconcileTableReport{}
	}
	return r.Tables[name]
}

// Errors totals statement errors across tables.
func (r *ReconcileReport) Errors() int {
	n := 0
	for _, t := range r.Tables {
		n += t.Errors
	}
	return n
}

// reconcileOp is one planned statement.
type reconcileOp struct {
	table string
	kind  string // counted on success: "inserted", "updated", "deleted", "adopted"
	query string
	args  []any
	// onErr runs when the statement fails. It returns true when it resolved
	// the failure (the op then counts as done), false when it reported it.
	onErr func(tx *sql.Tx, err error) bool
}

// deleteBreakerFraction: a run that would delete more than this share of a
// table's marked rows (with more than deleteBreakerMinMarked of them) deletes
// none and reports instead. The rebuilt-history check catches a wiped Raft;
// this is the second net.
const (
	deleteBreakerFraction  = 0.5
	deleteBreakerMinMarked = 2
)

type reconciler struct {
	st      ClusterAuthState
	log     zerolog.Logger
	report  *ReconcileReport
	tx1     []reconcileOp // deletes, adoptions, narrowing and other in-place updates
	renames []reconcileOp // tx2, first: every renamed row to a temporary name
	final   []reconcileOp // tx2, second: final names, then inserts parent to child
	nonce   string
	// deletes are planned per table, then checked against the breaker.
	deletes map[string][]reconcileOp
	marked  map[string]int
}

// ReconcileClusterState rewrites the local auth (and, with IncludeRBAC, RBAC)
// tables from st. The caller drops the token and RBAC caches afterwards.
func ReconcileClusterState(ctx context.Context, db *sql.DB, st ClusterAuthState, logger zerolog.Logger) (*ReconcileReport, error) {
	start := time.Now()
	nonce := make([]byte, 6)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("reconcile nonce: %w", err)
	}
	r := &reconciler{
		st:      st,
		log:     logger,
		report:  &ReconcileReport{Tables: map[string]*ReconcileTableReport{}},
		nonce:   hex.EncodeToString(nonce),
		deletes: map[string][]reconcileOp{},
		marked:  map[string]int{},
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("reconcile: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// A boot on a rebuilt Raft (data wiped and re-bootstrapped, or this DB
	// restored into another cluster's history) sees a history below the one
	// recorded last time. Every marked row then belongs to a history this FSM
	// never had: unmark them all so nothing is deleted, and let the adoption
	// rules take back the ones the new FSM really holds.
	if st.Term > 0 {
		rebuilt, err := historyRegressed(tx, st.Term, st.AppliedIndex)
		if err != nil {
			return nil, err
		}
		if rebuilt {
			r.report.HistoryRebuilt = true
			for _, t := range allReconcileTables {
				if _, err := tx.Exec(`UPDATE ` + t + ` SET raft_materialised = 0 WHERE raft_materialised = 1`); err != nil {
					return nil, fmt.Errorf("reconcile: unmark %s after a rebuilt history: %w", t, err)
				}
			}
		}
	}

	if err := r.planTokens(tx); err != nil {
		return nil, err
	}
	if st.IncludeRBAC {
		if err := r.planRBAC(tx); err != nil {
			return nil, err
		}
	}
	r.applyBreaker()

	// Transaction 1: deletes, children first, then adoptions and updates.
	for _, t := range deleteOrder {
		r.exec(tx, r.deletes[t])
	}
	r.exec(tx, r.tx1)
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("reconcile: commit deletes and updates: %w", err)
	}

	// Transaction 2: renames through temporary names, then inserts.
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		return r.report, fmt.Errorf("reconcile: begin inserts: %w", err)
	}
	defer func() { _ = tx2.Rollback() }()
	r.exec(tx2, r.renames)
	r.exec(tx2, r.final)
	if err := recordHistory(tx2, st.Term, st.AppliedIndex); err != nil {
		r.log.Error().Err(err).Msg("Cluster auth reconcile: could not record the Raft history it ran at")
	}
	if reconcileBeforeSecondCommit != nil {
		reconcileBeforeSecondCommit()
	}
	if err := tx2.Commit(); err != nil {
		return r.report, fmt.Errorf("reconcile: commit inserts: %w", err)
	}
	r.report.Duration = time.Since(start)
	return r.report, nil
}

// reconcileBeforeSecondCommit is a test seam: it runs just before the second
// transaction (renames, inserts) commits.
var reconcileBeforeSecondCommit func()

var allReconcileTables = []string{TableMemberships, TableMeasurementPermissions, TableRoles, TableTeams, TableOrganizations, TableTokens}

// deleteOrder is children before parents. A cascade may already have removed
// a child; a delete of an absent row is not an error.
var deleteOrder = allReconcileTables

func (r *reconciler) exec(tx *sql.Tx, ops []reconcileOp) {
	for _, op := range ops {
		if _, err := tx.Exec(op.query, op.args...); err != nil {
			if op.onErr == nil || !op.onErr(tx, err) {
				r.report.table(op.table).Errors++
				if op.onErr == nil {
					r.log.Error().Err(err).Str("table", op.table).Str("op", op.kind).Msg("Cluster auth reconcile: statement failed; the run continues")
				}
				continue
			}
		}
		t := r.report.table(op.table)
		switch op.kind {
		case "inserted":
			t.Inserted++
		case "updated":
			t.Updated++
		case "deleted":
			t.Deleted++
		case "adopted":
			t.Adopted++
		}
	}
}

// keepOrDelete decides a marked row the FSM no longer has.
func (r *reconciler) keepOrDelete(table string, id int64, op reconcileOp) {
	if r.st.Quarantined[table][id] || r.st.AppliedIndex == 0 || uint64(id) > r.st.AppliedIndex {
		r.report.table(table).Protected++
		r.log.Warn().Str("table", table).Int64("id", id).
			Msg("Cluster auth reconcile: kept a cluster row the FSM does not hold (refused by the snapshot, or from another Raft history)")
		return
	}
	r.deletes[table] = append(r.deletes[table], op)
}

func (r *reconciler) applyBreaker() {
	for table, ops := range r.deletes {
		m := r.marked[table]
		if m > deleteBreakerMinMarked && float64(len(ops)) > deleteBreakerFraction*float64(m) {
			r.report.BreakerTripped = append(r.report.BreakerTripped, table)
			r.report.table(table).Protected += len(ops)
			r.log.Error().Str("table", table).Int("would_delete", len(ops)).Int("cluster_rows", m).
				Msg("Cluster auth reconcile: refused to delete most of this table's cluster rows; none deleted. Check that this node belongs to the cluster its auth database came from")
			delete(r.deletes, table)
		}
	}
}

func (r *reconciler) tempName(id int64) string {
	return fmt.Sprintf("__arc_reconcile_%s_%d", r.nonce, id)
}

// rename plans a two-step rename of a cluster row: a temporary name first,
// so two rows that swap names never collide, then the final one. If the final
// name is held by a row the cluster does not own, the old name is restored
// and the collision reported.
func (r *reconciler) rename(table string, id int64, oldName, newName string) {
	r.renames = append(r.renames, reconcileOp{table: table,
		query: `UPDATE ` + table + ` SET name = ? WHERE id = ?`, args: []any{r.tempName(id), id}})
	r.final = append(r.final, reconcileOp{table: table, kind: "updated",
		query: `UPDATE ` + table + ` SET name = ? WHERE id = ?`, args: []any{newName, id},
		onErr: func(tx *sql.Tx, err error) bool {
			r.collision(table, id, newName)
			r.log.Error().Err(err).Str("table", table).Int64("id", id).Msg("Cluster auth reconcile: rename failed")
			if _, rerr := tx.Exec(`UPDATE `+table+` SET name = ? WHERE id = ?`, oldName, id); rerr != nil {
				r.log.Error().Err(rerr).Str("table", table).Int64("id", id).Msg("Cluster auth reconcile: could not restore the old name either; the row keeps a temporary name")
			}
			return false
		}})
}

func (r *reconciler) collision(table string, id int64, what string) {
	r.report.table(table).Collisions++
	r.log.Error().Str("table", table).Int64("id", id).Str("cluster", what).
		Msg("Cluster auth reconcile: a local row holds this cluster entry's id or name; left in place, the cluster entry is not usable on this node. See operations/pre-26.06.1-token-upgrade.md")
}

func (r *reconciler) unreplicated(table string, id int64, name string) {
	r.report.table(table).Unreplicated++
	r.log.Warn().Str("table", table).Int64("id", id).Str("name", name).
		Msg("Cluster auth reconcile: local row the cluster does not know; left in place. It is valid only on this node (operations/pre-26.06.1-token-upgrade.md)")
}

// --- time helpers ----------------------------------------------------------

func utcNano(n int64) time.Time { return time.Unix(0, n).UTC() }

// nullableNano is the stored form of an optional instant (0 = NULL).
func nullableNano(n int64) any {
	if n == 0 {
		return nil
	}
	return utcNano(n)
}

// scannedTime reads a TIMESTAMP column, which go-sqlite3 returns as
// time.Time when it parses it and as text otherwise.
func scannedTime(v any) (time.Time, bool) {
	switch t := v.(type) {
	case time.Time:
		return t, true
	case string:
		return parseStoredTime(t)
	case []byte:
		return parseStoredTime(string(t))
	}
	return time.Time{}, false
}

func parseStoredTime(s string) (time.Time, bool) {
	for _, layout := range []string{"2006-01-02 15:04:05.999999999-07:00", "2006-01-02T15:04:05.999999999-07:00", "2006-01-02 15:04:05", time.RFC3339Nano} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// sameInstant compares a stored optional instant with an FSM one (0 = none),
// as instants, never as text (SQLite checklist #1).
func sameInstant(stored any, n int64) bool {
	t, ok := scannedTime(stored)
	if n == 0 {
		return !ok
	}
	return ok && t.Equal(utcNano(n))
}

// --- history watermark -----------------------------------------------------

func historyRegressed(tx *sql.Tx, term, index uint64) (bool, error) {
	var wTerm, wIndex uint64
	err := tx.QueryRow(`SELECT raft_term, applied_index FROM cluster_auth_reconcile_state WHERE id = 1`).Scan(&wTerm, &wIndex)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reconcile: read history watermark: %w", err)
	}
	return term < wTerm || (term == wTerm && index < wIndex), nil
}

// recordHistory stores where this reconcile ran. A snapshot install (term 0)
// keeps the recorded term and only moves the index forward.
func recordHistory(tx *sql.Tx, term, index uint64) error {
	if index == 0 {
		return nil
	}
	if term == 0 {
		_, err := tx.Exec(`UPDATE cluster_auth_reconcile_state SET applied_index = ? WHERE id = 1 AND applied_index < ?`, index, index)
		return err
	}
	_, err := tx.Exec(`INSERT INTO cluster_auth_reconcile_state (id, raft_term, applied_index) VALUES (1, ?, ?)
		ON CONFLICT(id) DO UPDATE SET raft_term = excluded.raft_term, applied_index = excluded.applied_index`, term, index)
	return err
}

// --- tokens ----------------------------------------------------------------

type localToken struct {
	id                   int64
	name, hash, prefix   string
	description, perms   sql.NullString
	createdAt, expiresAt any
	enabled, marked      bool
}

func (r *reconciler) planTokens(tx *sql.Tx) error {
	rows, err := tx.Query(`SELECT id, name, token_hash, COALESCE(token_prefix, ''), description, permissions, created_at, expires_at, enabled, raft_materialised FROM api_tokens`)
	if err != nil {
		return fmt.Errorf("reconcile: read api_tokens: %w", err)
	}
	var local []localToken
	for rows.Next() {
		var l localToken
		var enabled, marked sql.NullInt64
		if err := rows.Scan(&l.id, &l.name, &l.hash, &l.prefix, &l.description, &l.perms, &l.createdAt, &l.expiresAt, &enabled, &marked); err != nil {
			rows.Close()
			return fmt.Errorf("reconcile: scan api_tokens: %w", err)
		}
		l.enabled = enabled.Int64 != 0
		l.marked = marked.Int64 == 1
		local = append(local, l)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reconcile: read api_tokens: %w", err)
	}

	fsm := make(map[int64]ClusterTokenEntry, len(r.st.Tokens))
	for _, e := range r.st.Tokens {
		fsm[e.ID] = e
	}
	present := make(map[int64]bool, len(local))
	for _, l := range local {
		present[l.id] = true
		if l.marked {
			r.marked[TableTokens]++
		}
	}

	for _, l := range local {
		e, ok := fsm[l.id]
		if !ok {
			if l.marked {
				r.keepOrDelete(TableTokens, l.id, reconcileOp{table: TableTokens, kind: "deleted",
					query: `DELETE FROM api_tokens WHERE id = ? AND raft_materialised = 1`, args: []any{l.id}})
			} else {
				r.unreplicated(TableTokens, l.id, l.name)
			}
			continue
		}
		if !l.marked {
			// A row written by a cluster apply before the marker existed:
			// the same name and the same secret or creation instant.
			if l.name != e.Name || (l.hash != e.TokenHash && !sameInstant(l.createdAt, e.CreatedAtUnixNano)) {
				r.collision(TableTokens, l.id, e.Name)
				continue
			}
			r.tx1 = append(r.tx1, reconcileOp{table: TableTokens, kind: "adopted",
				query: `UPDATE api_tokens SET raft_materialised = 1 WHERE id = ?`, args: []any{l.id}})
		}
		if l.hash != e.TokenHash || l.prefix != e.TokenPrefix || l.description.String != e.Description ||
			l.perms.String != e.Permissions || !sameInstant(l.expiresAt, e.ExpiresAtUnixNano) {
			r.tx1 = append(r.tx1, reconcileOp{table: TableTokens, kind: "updated",
				query: `UPDATE api_tokens SET token_hash = ?, token_prefix = ?, description = ?, permissions = ?, expires_at = ? WHERE id = ?`,
				args:  []any{e.TokenHash, e.TokenPrefix, e.Description, e.Permissions, nullableNano(e.ExpiresAtUnixNano), l.id}})
		}
		r.planEnabled(TableTokens, "api_tokens", l.id, l.name, e.Enabled, l.enabled)
		if l.name != e.Name {
			r.rename(TableTokens, l.id, l.name, e.Name)
		}
	}

	for _, e := range r.st.Tokens {
		if present[e.ID] {
			continue
		}
		enabled := 0
		if e.Enabled {
			enabled = 1
		}
		e := e
		r.final = append(r.final, reconcileOp{table: TableTokens, kind: "inserted",
			query: `INSERT INTO api_tokens (id, name, token_hash, token_prefix, description, permissions, created_at, expires_at, enabled, raft_materialised)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)`,
			args: []any{e.ID, e.Name, e.TokenHash, e.TokenPrefix, e.Description, e.Permissions, utcNano(e.CreatedAtUnixNano), nullableNano(e.ExpiresAtUnixNano), enabled},
			onErr: func(_ *sql.Tx, err error) bool {
				r.collision(TableTokens, e.ID, e.Name)
				r.log.Error().Err(err).Int64("token_id", e.ID).Msg("Cluster auth reconcile: could not add a cluster token on this node")
				return false
			}})
	}
	return nil
}

// planEnabled narrows a cluster row the FSM has disabled, and never widens
// one: the cluster has no command that re-enables, so a disabled local copy
// of an enabled entry was disabled here while the node ran standalone. Local
// may be stricter than the cluster, never looser (#1336 D4).
func (r *reconciler) planEnabled(table, sqlTable string, id int64, name string, want, have bool) {
	switch {
	case !want && have:
		r.tx1 = append(r.tx1, reconcileOp{table: table, kind: "updated",
			query: `UPDATE ` + sqlTable + ` SET enabled = 0 WHERE id = ?`, args: []any{id}})
	case want && !have:
		r.report.table(table).KeptDisabled++
		r.log.Warn().Str("table", table).Int64("id", id).Str("name", name).
			Msg("Cluster auth reconcile: the cluster has this entry enabled but it was disabled on this node; kept disabled here")
	}
}

// --- RBAC ------------------------------------------------------------------

// rbacLocal is one RBAC row as read; rbacWant is the FSM's version of it.
// key is the identity a create apply compares (and the UNIQUE key, where the
// table has one); vals are the remaining columns, compared as text.
type rbacLocal struct {
	id      int64
	marked  bool
	enabled bool
	name    string
	key     string
	vals    []string
}

type rbacWant struct {
	id      int64
	enabled bool
	name    string
	key     string
	vals    []string
	update  []any // values for spec.updateCols, in order
	insert  []any // values for spec.insertSQL
}

type rbacSpec struct {
	table      string
	selectSQL  string // id, raft_materialised, enabled-or-1, name-or-'', then the key and value columns scanned by scan
	scan       func(*sql.Rows) (rbacLocal, error)
	want       []rbacWant
	updateCols []string // SET list for an in-place update (never the name)
	hasName    bool     // renamed through rename()
	hasEnabled bool
	insertSQL  string
	// adoptByKey: a local row holding the FSM entry's key under another id is
	// that entry (memberships, whose id nothing references).
	adoptByKey bool
	// realignByName: an insert blocked by a local row of the same name
	// replaces that row (#1336 D3; the live ApplyCreateOrganization does the
	// same, cascading the local row's children).
	realignByName bool
}

func (r *reconciler) planRBAC(tx *sql.Tx) error {
	for _, spec := range r.rbacSpecs() {
		if err := r.planRBACTable(tx, spec); err != nil {
			return err
		}
	}
	return nil
}

func nullStr(s sql.NullString) string { return s.String }

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (r *reconciler) rbacSpecs() []rbacSpec {
	var orgs, teams, roles, perms, members []rbacWant
	for _, e := range r.st.Organizations {
		orgs = append(orgs, rbacWant{id: e.ID, enabled: e.Enabled, name: e.Name, key: e.Name,
			vals:   []string{e.Description},
			update: []any{e.Description, utcNano(e.UpdatedAtUnixNano)},
			insert: []any{e.ID, e.Name, e.Description, utcNano(e.CreatedAtUnixNano), utcNano(e.UpdatedAtUnixNano), boolInt(e.Enabled)}})
	}
	for _, e := range r.st.Teams {
		key := fmt.Sprintf("%d|%s", e.OrganizationID, e.Name)
		teams = append(teams, rbacWant{id: e.ID, enabled: e.Enabled, name: e.Name, key: key,
			vals:   []string{e.Description},
			update: []any{e.Description, utcNano(e.UpdatedAtUnixNano)},
			insert: []any{e.ID, e.OrganizationID, e.Name, e.Description, utcNano(e.CreatedAtUnixNano), utcNano(e.UpdatedAtUnixNano), boolInt(e.Enabled)}})
	}
	for _, e := range r.st.Roles {
		key := fmt.Sprintf("%d|%s", e.TeamID, e.DatabasePattern)
		roles = append(roles, rbacWant{id: e.ID, enabled: true, key: key,
			vals:   []string{e.DatabasePattern, e.Permissions},
			update: []any{e.DatabasePattern, e.Permissions},
			insert: []any{e.ID, e.TeamID, e.DatabasePattern, e.Permissions, utcNano(e.CreatedAtUnixNano)}})
	}
	for _, e := range r.st.MeasurementPermissions {
		key := fmt.Sprintf("%d|%s", e.RoleID, e.MeasurementPattern)
		perms = append(perms, rbacWant{id: e.ID, enabled: true, key: key,
			vals:   []string{e.Permissions},
			update: []any{e.Permissions},
			insert: []any{e.ID, e.RoleID, e.MeasurementPattern, e.Permissions, utcNano(e.CreatedAtUnixNano)}})
	}
	for _, e := range r.st.Memberships {
		key := fmt.Sprintf("%d|%d", e.TokenID, e.TeamID)
		members = append(members, rbacWant{id: e.ID, enabled: true, key: key,
			insert: []any{e.ID, e.TokenID, e.TeamID, utcNano(e.CreatedAtUnixNano)}})
	}

	return []rbacSpec{
		{
			table:     TableOrganizations,
			selectSQL: `SELECT id, raft_materialised, COALESCE(enabled, 1), name, description FROM rbac_organizations`,
			scan: func(rows *sql.Rows) (rbacLocal, error) {
				var l rbacLocal
				var marked, enabled int64
				var desc sql.NullString
				err := rows.Scan(&l.id, &marked, &enabled, &l.name, &desc)
				l.marked, l.enabled, l.key, l.vals = marked == 1, enabled != 0, l.name, []string{nullStr(desc)}
				return l, err
			},
			want: orgs, updateCols: []string{"description", "updated_at"}, hasName: true, hasEnabled: true, realignByName: true,
			insertSQL: `INSERT INTO rbac_organizations (id, name, description, created_at, updated_at, enabled, raft_materialised) VALUES (?, ?, ?, ?, ?, ?, 1)`,
		},
		{
			table:     TableTeams,
			selectSQL: `SELECT id, raft_materialised, COALESCE(enabled, 1), name, organization_id, description FROM rbac_teams`,
			scan: func(rows *sql.Rows) (rbacLocal, error) {
				var l rbacLocal
				var marked, enabled, org int64
				var desc sql.NullString
				err := rows.Scan(&l.id, &marked, &enabled, &l.name, &org, &desc)
				l.marked, l.enabled, l.key, l.vals = marked == 1, enabled != 0, fmt.Sprintf("%d|%s", org, l.name), []string{nullStr(desc)}
				return l, err
			},
			want: teams, updateCols: []string{"description", "updated_at"}, hasName: true, hasEnabled: true,
			insertSQL: `INSERT INTO rbac_teams (id, organization_id, name, description, created_at, updated_at, enabled, raft_materialised) VALUES (?, ?, ?, ?, ?, ?, ?, 1)`,
		},
		{
			table:     TableRoles,
			selectSQL: `SELECT id, raft_materialised, 1, '', team_id, database_pattern, permissions FROM rbac_roles`,
			scan: func(rows *sql.Rows) (rbacLocal, error) {
				var l rbacLocal
				var marked, enabled, team int64
				var pattern, perms string
				err := rows.Scan(&l.id, &marked, &enabled, &l.name, &team, &pattern, &perms)
				l.marked, l.enabled, l.key, l.vals = marked == 1, true, fmt.Sprintf("%d|%s", team, pattern), []string{pattern, perms}
				return l, err
			},
			want: roles, updateCols: []string{"database_pattern", "permissions"},
			insertSQL: `INSERT INTO rbac_roles (id, team_id, database_pattern, permissions, created_at, raft_materialised) VALUES (?, ?, ?, ?, ?, 1)`,
		},
		{
			table:     TableMeasurementPermissions,
			selectSQL: `SELECT id, raft_materialised, 1, '', role_id, measurement_pattern, permissions FROM rbac_measurement_permissions`,
			scan: func(rows *sql.Rows) (rbacLocal, error) {
				var l rbacLocal
				var marked, enabled, role int64
				var pattern, perms string
				err := rows.Scan(&l.id, &marked, &enabled, &l.name, &role, &pattern, &perms)
				l.marked, l.enabled, l.key, l.vals = marked == 1, true, fmt.Sprintf("%d|%s", role, pattern), []string{perms}
				return l, err
			},
			want: perms, updateCols: []string{"permissions"},
			insertSQL: `INSERT INTO rbac_measurement_permissions (id, role_id, measurement_pattern, permissions, created_at, raft_materialised) VALUES (?, ?, ?, ?, ?, 1)`,
		},
		{
			table:     TableMemberships,
			selectSQL: `SELECT id, raft_materialised, 1, '', token_id, team_id FROM rbac_token_memberships`,
			scan: func(rows *sql.Rows) (rbacLocal, error) {
				var l rbacLocal
				var marked, enabled, tok, team int64
				err := rows.Scan(&l.id, &marked, &enabled, &l.name, &tok, &team)
				l.marked, l.enabled, l.key = marked == 1, true, fmt.Sprintf("%d|%d", tok, team)
				return l, err
			},
			want: members, adoptByKey: true,
			insertSQL: `INSERT INTO rbac_token_memberships (id, token_id, team_id, created_at, raft_materialised) VALUES (?, ?, ?, ?, 1)`,
		},
	}
}

func (r *reconciler) planRBACTable(tx *sql.Tx, spec rbacSpec) error {
	rows, err := tx.Query(spec.selectSQL)
	if err != nil {
		return fmt.Errorf("reconcile: read %s: %w", spec.table, err)
	}
	var local []rbacLocal
	for rows.Next() {
		l, err := spec.scan(rows)
		if err != nil {
			rows.Close()
			return fmt.Errorf("reconcile: scan %s: %w", spec.table, err)
		}
		local = append(local, l)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reconcile: read %s: %w", spec.table, err)
	}

	wantByID := make(map[int64]rbacWant, len(spec.want))
	wantKeys := make(map[string]bool, len(spec.want))
	for _, w := range spec.want {
		wantByID[w.id] = w
		wantKeys[w.key] = true
	}
	present := make(map[int64]bool, len(local))
	localByKey := make(map[string]rbacLocal, len(local))
	for _, l := range local {
		present[l.id] = true
		localByKey[l.key] = l
		if l.marked {
			r.marked[spec.table]++
		}
	}

	for _, l := range local {
		w, ok := wantByID[l.id]
		if !ok {
			switch {
			case spec.adoptByKey && wantKeys[l.key]:
				// Handled below, from the FSM side.
			case l.marked:
				r.keepOrDelete(spec.table, l.id, reconcileOp{table: spec.table, kind: "deleted",
					query: `DELETE FROM ` + spec.table + ` WHERE id = ? AND raft_materialised = 1`, args: []any{l.id}})
			default:
				r.unreplicated(spec.table, l.id, l.name)
			}
			continue
		}
		if !l.marked {
			if l.key != w.key {
				r.collision(spec.table, l.id, w.key)
				continue
			}
			r.tx1 = append(r.tx1, reconcileOp{table: spec.table, kind: "adopted",
				query: `UPDATE ` + spec.table + ` SET raft_materialised = 1 WHERE id = ?`, args: []any{l.id}})
		}
		if len(spec.updateCols) > 0 && strings.Join(l.vals, "\x00") != strings.Join(w.vals, "\x00") {
			set := make([]string, len(spec.updateCols))
			for i, c := range spec.updateCols {
				set[i] = c + " = ?"
			}
			r.tx1 = append(r.tx1, reconcileOp{table: spec.table, kind: "updated",
				query: `UPDATE ` + spec.table + ` SET ` + strings.Join(set, ", ") + ` WHERE id = ?`,
				args:  append(append([]any{}, w.update...), l.id)})
		}
		if spec.hasEnabled {
			r.planEnabled(spec.table, spec.table, l.id, l.name, w.enabled, l.enabled)
		}
		if spec.hasName && l.name != w.name {
			r.rename(spec.table, l.id, l.name, w.name)
		}
	}

	for _, w := range spec.want {
		if present[w.id] {
			continue
		}
		if spec.adoptByKey {
			if l, ok := localByKey[w.key]; ok {
				if !l.marked {
					r.tx1 = append(r.tx1, reconcileOp{table: spec.table, kind: "adopted",
						query: `UPDATE ` + spec.table + ` SET raft_materialised = 1 WHERE id = ?`, args: []any{l.id}})
				}
				continue
			}
		}
		w := w
		r.final = append(r.final, reconcileOp{table: spec.table, kind: "inserted", query: spec.insertSQL, args: w.insert,
			onErr: func(tx *sql.Tx, err error) bool {
				if spec.realignByName && strings.Contains(err.Error(), "UNIQUE constraint failed") && r.realign(tx, spec, w) {
					return true
				}
				r.collision(spec.table, w.id, w.key)
				r.log.Error().Err(err).Str("table", spec.table).Int64("id", w.id).Msg("Cluster auth reconcile: could not add a cluster row on this node")
				return false
			}})
	}
	return nil
}

// realign replaces a local, unowned row holding the cluster entry's name with
// the cluster entry, as ApplyCreateOrganization does live. The local row's
// children cascade away; they were never replicated.
func (r *reconciler) realign(tx *sql.Tx, spec rbacSpec, w rbacWant) bool {
	if _, err := tx.Exec(`DELETE FROM `+spec.table+` WHERE name = ? AND raft_materialised = 0`, w.name); err != nil {
		return false
	}
	if _, err := tx.Exec(spec.insertSQL, w.insert...); err != nil {
		return false
	}
	r.log.Info().Str("table", spec.table).Int64("id", w.id).Str("name", w.name).
		Msg("Cluster auth reconcile: replaced a local row of the same name with the cluster's; its local children were removed")
	return true
}
