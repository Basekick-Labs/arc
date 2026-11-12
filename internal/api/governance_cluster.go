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
