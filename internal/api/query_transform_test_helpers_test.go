package api

import "context"

// getTransformedSQL is getTransformedSQLWithRefs without the scopes, for tests
// that look only at the SQL. Production callers must take the scopes and check
// them (checkEmittedQueryPermissions, #764), so this lives in a test file.
func (h *QueryHandler) getTransformedSQL(ctx context.Context, sql string, headerDB string) (string, bool, error) {
	transformed, cached, _, err := h.getTransformedSQLWithRefs(ctx, sql, headerDB)
	return transformed, cached, err
}

// getTransformedSQLForQuery is getTransformedSQLForQueryWithRefs without the
// scopes; see getTransformedSQL.
func (h *QueryHandler) getTransformedSQLForQuery(ctx context.Context, sql string, headerDB string) (string, bool, error) {
	transformed, cached, _, err := h.getTransformedSQLForQueryWithRefs(ctx, sql, headerDB)
	return transformed, cached, err
}
