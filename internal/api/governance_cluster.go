package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/governance"
)

// GovernanceTableLister answers the cluster's adoption of governance policies
// (#1627) on a node where governance does not run (disabled or unlicensed):
// it enforces nothing, but its database may hold the policies of when it
// did, and those count, their IDs above all. Without it that node would
// stall the adoption. The table is read from the auth database; no file or
// no table is no policies.
func GovernanceTableLister(authManager *auth.AuthManager, dbPath string) cluster.LocalPolicyLister {
	return func(ctx context.Context) (json.RawMessage, error) {
		var db *sql.DB
		if authManager != nil && authManager.SharesDBPath(dbPath) {
			db = authManager.GetDB()
		} else {
			if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
				return json.Marshal(governance.LocalRows{Policies: []governance.LocalRow{}})
			}
			opened, err := sql.Open("sqlite3", dbPath+dsnSep(dbPath)+"mode=ro&_busy_timeout=5000")
			if err != nil {
				return nil, err
			}
			defer opened.Close()
			db = opened
		}
		rows, err := governance.ReadLocalRows(ctx, db)
		if err != nil {
			return nil, err
		}
		return json.Marshal(rows)
	}
}

// GovernanceTableMirror keeps the governance table of a node where governance
// does not run equal to the cluster's policies once adopted, and marks it as
// the cluster's backup copy (#1627): if governance is turned on there later,
// that node never enforces policies the cluster dropped or an admin's
// adoption choice discarded. The database is opened for each pass (a pass
// with nothing to change writes nothing); a node with no database file is
// left without one.
func GovernanceTableMirror(authManager *auth.AuthManager, dbPath string, src governance.ClusterViewSource) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if seeded, _ := src.GovernanceView(); !seeded {
			return nil
		}
		db := (*sql.DB)(nil)
		if authManager != nil && authManager.SharesDBPath(dbPath) {
			db = authManager.GetDB()
		} else {
			if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
				return nil // no database, no policies to be enforced later
			}
			opened, err := sql.Open("sqlite3", dbPath+dsnSep(dbPath)+"_busy_timeout=5000")
			if err != nil {
				return err
			}
			defer opened.Close()
			db = opened
		}
		if err := governance.EnsureSchema(db); err != nil {
			return err
		}
		_, err := governance.MirrorTable(ctx, db, src)
		return err
	}
}
