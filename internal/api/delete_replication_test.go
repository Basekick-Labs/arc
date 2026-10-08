package api

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/replicaview"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestDeleteRewritePreservesReplicationCoverage(t *testing.T) {
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	require.NoError(t, err)
	defer backend.Close()
	db, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 1, LocalStorageRoot: root}, zerolog.Nop())
	require.NoError(t, err)
	defer db.Close()
	const key = "db/cpu/2026/10/01/01/source.parquet"
	full := filepath.Join(root, key)
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0700))
	coverage := replicaview.Coverage{{Instance: 7, First: 1, Last: 2}}
	metadata := replicaview.FileMetadata{Database: "db", Measurement: "cpu", Coverage: coverage, Partitions: []replicaview.PartitionCoverage{{Hour: 1, Coverage: coverage}, {Hour: 2, Coverage: coverage}}}
	_, err = db.DB().Exec("COPY (SELECT * FROM (VALUES (1, 'remove'), (2, 'keep')) t(v, host)) TO " + sqlutil.QuoteStringLiteral(full) + " (FORMAT PARQUET, KV_METADATA {" + sqlutil.QuoteStringLiteral(replicaview.FileMetadataKey) + ": " + sqlutil.QuoteStringLiteral(metadata.Encode()) + "})")
	require.NoError(t, err)
	coord := &rewriteManifestCoordinator{entry: &raft.FileEntry{Path: key, Database: "db", Measurement: "cpu", WALCoverage: metadata.PartitionCoverages()}}
	handler := &DeleteHandler{db: db, storage: backend, coordinator: coord, logger: zerolog.Nop()}
	deleted, err := handler.rewriteFileWithoutDeletedRows(context.Background(), full, key, "v = 1")
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
	require.Len(t, coord.ops, 2)
	var registration raft.RegisterFilePayload
	require.NoError(t, json.Unmarshal(coord.ops[0].Payload, &registration))
	require.Equal(t, []string{key}, registration.File.Replaces)
	require.Equal(t, metadata.PartitionCoverages(), registration.File.WALCoverage)
	output := filepath.Join(root, registration.File.Path)
	f, err := os.Open(output)
	require.NoError(t, err)
	defer f.Close()
	stat, err := f.Stat()
	require.NoError(t, err)
	actual, err := replicaview.ReadFileMetadata(f, stat.Size())
	require.NoError(t, err)
	metadata.Replaces = []string{key}
	require.Equal(t, metadata, *actual, "deletion must preserve identities of removed rows and every source hour")
	var count, value int
	require.NoError(t, db.DB().QueryRow("SELECT count(*), max(v) FROM read_parquet(?)", output).Scan(&count, &value))
	require.Equal(t, 1, count)
	require.Equal(t, 2, value)
}
