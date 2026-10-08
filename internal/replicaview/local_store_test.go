package replicaview_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/replicaview"
	"github.com/basekick-labs/arc/internal/storage"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestLocalStoreHandoffRecoveryAndDelete(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	primary, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	require.NoError(t, err)
	defer primary.Close()
	replica, err := storage.NewLocalBackend(root, zerolog.Nop())
	require.NoError(t, err)
	defer replica.Close()
	store, err := replicaview.OpenLocalStore(ctx, root)
	require.NoError(t, err)
	defer func() { store.Close() }()
	initial := store.View().Snapshot("db", "cpu")
	require.ErrorIs(t, initial.Err, replicaview.ErrManifestNotReady)
	initial.Close()
	require.NoError(t, store.Reconcile(ctx, nil, nil))
	cfg := &config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 60000, FlushWorkers: 1, FlushQueueSize: 10, ShardCount: 1, Compression: "snappy"}
	writer := ingest.NewArrowBuffer(cfg, primary, zerolog.Nop())
	defer writer.Close()
	reader := ingest.NewArrowBuffer(cfg, replica, zerolog.Nop())
	defer reader.Close()
	writer.SetReplicationPublisher(replicaview.NewView())
	reader.SetReplicationPublisher(store)
	const identity = "00000000000000070000000000000001"
	columns := map[string][]interface{}{"time": {int64(1700000000000000), int64(1700000000000001)}, "v": {int64(1), int64(2)}}
	require.NoError(t, writer.WriteColumnarDirectReplay(ctx, "db", "cpu", columns, identity))
	require.NoError(t, reader.WriteReplicatedColumnar(ctx, "db", "cpu", columns, identity))
	require.NoError(t, reader.FlushAll(ctx))
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()
	count := func(snapshot *replicaview.Snapshot) int {
		t.Helper()
		query, err := snapshot.SQL(store.Resolve, "", "union_by_name=true")
		require.NoError(t, err)
		var n int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM "+query).Scan(&n))
		return n
	}
	snapshot := store.View().Snapshot("db", "cpu")
	require.Equal(t, 2, count(snapshot))
	snapshot.Close()
	require.NoError(t, writer.FlushAll(ctx))
	keys, err := primary.List(ctx, "db/cpu")
	require.NoError(t, err)
	require.Len(t, keys, 1)
	body, err := primary.Read(ctx, keys[0])
	require.NoError(t, err)
	digest := sha256.Sum256(body)
	coverage, err := replicaview.FromIdentities([]string{identity})
	require.NoError(t, err)
	hour := int64(1700000000000000) / 1000000 / 3600
	origin := replicaview.Canonical{Path: keys[0], SHA256: hex.EncodeToString(digest[:]), SizeBytes: int64(len(body)), Database: "db", Measurement: "cpu", Hour: hour, Partitions: []replicaview.PartitionCoverage{{Hour: hour, Coverage: coverage}}}
	require.NoError(t, store.Reconcile(ctx, []replicaview.Canonical{origin}, nil))
	snapshot = store.View().Snapshot("db", "cpu")
	require.Equal(t, 2, count(snapshot), "advertisement cannot withdraw rows")
	snapshot.Close()
	require.NoError(t, replica.Write(ctx, origin.Path, body))
	require.NoError(t, store.PublishCanonical(ctx, origin))
	snapshot = store.View().Snapshot("db", "cpu")
	require.Equal(t, 2, count(snapshot))
	snapshot.Close()
	// Recovery must reuse the pinned version even after an external compactor
	// unlinks the ordinary path. Before reconciliation the view remains closed.
	require.NoError(t, replica.Delete(ctx, origin.Path))
	require.NoError(t, store.Close())
	store, err = replicaview.OpenLocalStore(ctx, root)
	require.NoError(t, err)
	require.NoError(t, store.Reconcile(ctx, []replicaview.Canonical{origin}, nil))
	oldQuery := store.View().Snapshot("db", "cpu")
	defer oldQuery.Close()
	require.Equal(t, 2, count(oldQuery))
	// A partial DELETE preserves the original identity even though one row is
	// removed. Until its new bytes arrive, queries must not silently return zero.
	replacement := origin
	replacement.Path = filepath.ToSlash(filepath.Join(filepath.Dir(origin.Path), "rewritten.parquet"))
	replacement.Replaces = []string{origin.Path}
	out := filepath.Join(primary.GetBasePath(), replacement.Path)
	metadata := replicaview.FileMetadata{Database: "db", Measurement: "cpu", Hour: hour, Coverage: coverage, Replaces: replacement.Replaces}
	_, err = db.Exec("COPY (SELECT * FROM read_parquet(?) WHERE v = 2) TO '"+out+"' (FORMAT PARQUET, KV_METADATA {'"+replicaview.FileMetadataKey+"': '"+metadata.Encode()+"'})", filepath.Join(primary.GetBasePath(), origin.Path))
	require.NoError(t, err)
	body, err = os.ReadFile(out)
	require.NoError(t, err)
	digest = sha256.Sum256(body)
	replacement.SHA256 = hex.EncodeToString(digest[:])
	replacement.SizeBytes = int64(len(body))
	retirements := []replicaview.Retirement{{Database: "db", Measurement: "cpu", Hour: hour, Coverage: coverage}}
	require.NoError(t, store.Reconcile(ctx, []replicaview.Canonical{replacement}, retirements))
	pending := store.View().Snapshot("db", "cpu")
	require.Error(t, pending.Err)
	pending.Close()
	require.Equal(t, 2, count(oldQuery), "in-flight query keeps its immutable source")
	require.NoError(t, replica.Write(ctx, replacement.Path, body))
	require.NoError(t, store.PublishCanonical(ctx, replacement))
	snapshot = store.View().Snapshot("db", "cpu")
	require.Equal(t, 1, count(snapshot))
	snapshot.Close()
	require.NoError(t, store.Collect(ctx))
	require.Equal(t, 2, count(oldQuery), "collection preserves a leased canonical pin")
	oldPin := store.Resolve(oldQuery.Sources[0].File.ReadPath)
	require.FileExists(t, oldPin)
	require.NoError(t, oldQuery.Close())
	require.NoError(t, store.Collect(ctx))
	require.NoFileExists(t, oldPin, "superseded canonical pins are reclaimed after the last lease")
	require.FileExists(t, store.Resolve(replacement.Path), "collection must not unlink canonical storage keys")
	require.NoError(t, store.Close())
	store, err = replicaview.OpenLocalStore(ctx, root)
	require.NoError(t, err)
	require.NoError(t, store.Reconcile(ctx, []replicaview.Canonical{replacement}, retirements))
	snapshot = store.View().Snapshot("db", "cpu")
	require.Equal(t, 1, count(snapshot))
	snapshot.Close()
}
