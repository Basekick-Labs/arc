package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/basekick-labs/arc/internal/replicaview"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
)

// replicationRewriteOptions preserves the full source coverage, including
// identities whose rows the predicate removes. Shrinking coverage to surviving
// rows would allow a late replica WAL replay to resurrect deleted rows.
// DuckDB reads only the footer and supports both local and object-store paths.
func (h *DeleteHandler) replicationRewriteOptions(ctx context.Context, source, storageKey string, rowsBefore int64) (string, error) {
	var encoded string
	err := h.db.DB().QueryRowContext(ctx, "SELECT value FROM parquet_kv_metadata(?) WHERE key = ?", source, replicaview.FileMetadataKey).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read delete source replication metadata: %w", err)
	}
	metadata, err := replicaview.DecodeFileMetadata(encoded, rowsBefore)
	if err != nil {
		return "", err
	}
	if metadata.IsReplica() {
		return "", fmt.Errorf("cannot rewrite a replica materialization as originating data")
	}
	metadata.Replaces = []string{storageKey}
	return ", KV_METADATA {" + sqlutil.QuoteStringLiteral(replicaview.FileMetadataKey) + ": " + sqlutil.QuoteStringLiteral(metadata.Encode()) + ", " + sqlutil.QuoteStringLiteral(replicaview.MetadataKey) + ": " + sqlutil.QuoteStringLiteral(metadata.Coverage.Encode()) + "}", nil
}
