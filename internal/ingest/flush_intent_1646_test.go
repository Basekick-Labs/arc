package ingest

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// crashBeforeCheckpointWAL is a real WAL writer whose flush checkpoints never
// land: the process "dies" after a flush stored its file and before the
// checkpoint was durable (#1646).
type crashBeforeCheckpointWAL struct{ *wal.Writer }

func (w crashBeforeCheckpointWAL) MarkFlushed([]string) error         { return nil }
func (w crashBeforeCheckpointWAL) MarkFlushedRetained([]string) error { return nil }
func (w crashBeforeCheckpointWAL) MarkFlushedContext(context.Context, []string) error {
	return nil
}

type intentFixture struct {
	walDir  string
	dataDir string
	writer  *wal.Writer
	local   storage.Backend
	buffer  *ArrowBuffer
}

func newIntentFixture(t *testing.T, cluster bool) *intentFixture {
	t.Helper()
	tmp := t.TempDir()
	f := &intentFixture{walDir: filepath.Join(tmp, "wal"), dataDir: filepath.Join(tmp, "data")}
	var err error
	f.writer, err = wal.NewWriter(&wal.WriterConfig{WALDir: f.walDir, SyncMode: wal.SyncModeFsync, MaxSizeBytes: 100 << 20, Logger: zerolog.Nop()})
	require.NoError(t, err)
	f.local, err = storage.NewLocalBackend(f.dataDir, zerolog.Nop())
	require.NoError(t, err)
	f.buffer = newReplayTestBuffer(t, filepath.Join(tmp, "unused"))
	f.buffer.storage = f.local
	f.buffer.SetClusterMode(cluster)
	f.buffer.SetWAL(crashBeforeCheckpointWAL{f.writer})
	t.Cleanup(func() { _ = f.local.Close() })
	return f
}

func (f *intentFixture) write(t *testing.T, base int64, n int) {
	t.Helper()
	times := make([]interface{}, n)
	vals := make([]interface{}, n)
	for i := range times {
		times[i] = base + int64(i)
		vals[i] = float64(i)
	}
	payload, err := msgpack.Marshal(map[string]interface{}{"m": "probe", "columns": map[string]interface{}{"time": times, "v": vals}})
	require.NoError(t, err)
	decoded, err := NewMessagePackDecoder(zerolog.Nop()).Decode(payload)
	require.NoError(t, err)
	require.NoError(t, f.buffer.Write(context.Background(), "db", decoded))
}

// crash flushes (files stored, checkpoints lost), then stops the process.
func (f *intentFixture) crash(t *testing.T) {
	t.Helper()
	require.NoError(t, f.buffer.FlushAll(context.Background()))
	_ = f.buffer.Close()
	require.NoError(t, f.writer.Close())
}

// restart runs the startup recovery of a new process into a fresh buffer and
// returns how many rows it replayed.
func (f *intentFixture) restart(t *testing.T, cluster bool) (*wal.RecoveryStats, int) {
	t.Helper()
	w2, err := wal.NewWriter(&wal.WriterConfig{WALDir: f.walDir, SyncMode: wal.SyncModeFsync, MaxSizeBytes: 100 << 20, Logger: zerolog.Nop()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })
	b2 := newReplayTestBuffer(t, filepath.Join(t.TempDir(), "unused"))
	b2.storage = f.local
	b2.SetClusterMode(cluster)
	b2.SetWAL(w2)
	t.Cleanup(func() { _ = b2.Close() })
	rows := 0
	stats, err := wal.NewRecovery(f.walDir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &wal.RecoveryOptions{
		SkipActiveFile: w2.CurrentFile(),
		ColumnarCallbackWithTagColumns: func(ctx context.Context, db, m string, cols map[string][]interface{}, id string, tags []string) error {
			rows += len(cols["time"])
			return b2.WriteColumnarDirectReplayWithTagColumns(ctx, db, m, cols, id, tags)
		},
		BeforeDelete:        func(ctx context.Context) error { return b2.FlushAll(ctx) },
		CheckpointRecovered: func(ctx context.Context, ids []string) error { return w2.MarkFlushedContext(ctx, ids) },
		AdoptIntent:         b2.AdoptFlushedFile,
	})
	require.NoError(t, err)
	return stats, rows
}

// #1646: the crash lands between a flush's storage write and its checkpoint.
// The restart finds the file by its intent, checkpoints the entries and
// replays nothing: one file, not two.
func TestCrashBeforeCheckpointIsAdoptedNotReplayed(t *testing.T) {
	f := newIntentFixture(t, false)
	base := time.Now().UTC().Truncate(time.Hour).UnixMicro()
	f.write(t, base, 30)
	f.write(t, base+100, 20)
	f.crash(t)
	require.Len(t, parquetPaths(t, f.dataDir), 1)

	before := metrics.Get().GetWALFlushIntentsAdopted()
	stats, rows := f.restart(t, false)
	require.Zero(t, rows, "stored rows were replayed")
	require.Equal(t, 1, stats.AdoptedFiles)
	require.Len(t, parquetPaths(t, f.dataDir), 1, "a second file was written")
	require.Equal(t, int64(1), metrics.Get().GetWALFlushIntentsAdopted()-before)

	// The checkpoint the crash prevented now exists: a second restart has
	// nothing to replay or adopt.
	stats, rows = f.restart(t, false)
	require.Zero(t, rows)
	require.Zero(t, stats.AdoptedFiles)
}

// A multi-hour flush with a row map: one intent per hour file, each naming
// its own rows; all are adopted.
func TestCrashBeforeCheckpointOfAMultiHourFlushIsAdopted(t *testing.T) {
	f := newIntentFixture(t, false)
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC).UnixMicro()
	f.write(t, base, 10)
	f.write(t, base+int64(time.Hour/time.Microsecond), 10)
	// One buffer spanning two hours: a single flush, two hour files.
	f.crash(t)
	files := parquetPaths(t, f.dataDir)
	require.Len(t, files, 2, "one file per hour")
	stats, rows := f.restart(t, false)
	require.Zero(t, rows)
	require.Equal(t, len(files), stats.AdoptedFiles)
	require.Len(t, parquetPaths(t, f.dataDir), len(files))
}

// A stored file that is gone (compacted, deleted) is not adopted: replay.
func TestMissingFileIsReplayed(t *testing.T) {
	f := newIntentFixture(t, false)
	f.write(t, time.Now().UTC().Truncate(time.Hour).UnixMicro(), 10)
	f.crash(t)
	for _, p := range parquetPaths(t, f.dataDir) {
		require.NoError(t, f.local.Delete(context.Background(), filepath.ToSlash(p)))
	}
	stats, rows := f.restart(t, false)
	require.Equal(t, 10, rows)
	require.Zero(t, stats.AdoptedFiles)
}

// A cluster node writes no intents and adopts none.
func TestClusterModeWritesAndAdoptsNoIntent(t *testing.T) {
	f := newIntentFixture(t, true)
	f.write(t, time.Now().UTC().Truncate(time.Hour).UnixMicro(), 10)
	f.crash(t)
	stats, rows := f.restart(t, false) // even a standalone restart: no intent to find
	require.Equal(t, 10, rows)
	require.Zero(t, stats.AdoptedFiles)

	b := newReplayTestBuffer(t, t.TempDir())
	b.SetClusterMode(true)
	require.False(t, b.AdoptFlushedFile(context.Background(), wal.FlushIntent{Path: "db/m/x.parquet", Size: 1, IDs: []string{"x"}}))
}

// Size must match: another object at the path is not the flush's file.
func TestSizeMismatchIsNotAdopted(t *testing.T) {
	dir := t.TempDir()
	local, err := storage.NewLocalBackend(dir, zerolog.Nop())
	require.NoError(t, err)
	defer local.Close()
	require.NoError(t, local.Write(context.Background(), "db/m/x.parquet", []byte("1234")))
	b := newReplayTestBuffer(t, t.TempDir())
	b.storage = local
	require.True(t, b.AdoptFlushedFile(context.Background(), wal.FlushIntent{Path: "db/m/x.parquet", Size: 4, IDs: []string{"x"}}))
	require.False(t, b.AdoptFlushedFile(context.Background(), wal.FlushIntent{Path: "db/m/x.parquet", Size: 5, IDs: []string{"x"}}))
	require.False(t, b.AdoptFlushedFile(context.Background(), wal.FlushIntent{Path: "db/m/y.parquet", Size: 4, IDs: []string{"x"}}))
}

type schemaRecorder struct {
	mu    sync.Mutex
	files map[string][]string // storage key -> field names
}

func (r *schemaRecorder) EnsureFile(_ context.Context, _, _ string, schema *arrow.Schema, _ []string, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var names []string
	for _, f := range schema.Fields() {
		names = append(names, f.Name)
	}
	r.files[key] = names
	return nil
}

// Adoption registers the adopted file's field schema, read from its footer
// alone: the crash may have cut the flush's own registration short.
func TestAdoptionRegistersTheFileSchemaFromItsFooter(t *testing.T) {
	f := newIntentFixture(t, false)
	f.write(t, time.Now().UTC().Truncate(time.Hour).UnixMicro(), 10)
	f.crash(t)
	paths := parquetPaths(t, f.dataDir)
	require.Len(t, paths, 1)
	key := filepath.ToSlash(paths[0])
	size, err := f.local.StatFile(context.Background(), key)
	require.NoError(t, err)

	b := newReplayTestBuffer(t, t.TempDir())
	b.storage = f.local
	rec := &schemaRecorder{files: map[string][]string{}}
	b.SetFieldSchema(rec)
	require.True(t, b.AdoptFlushedFile(context.Background(), wal.FlushIntent{Path: key, Size: size, IDs: []string{"x"}}))
	require.ElementsMatch(t, []string{"time", "v"}, rec.files[key])
}

// An adopted file a #1635 ledger entry was waiting on is the stored copy:
// the entry is dropped, so the sweeper can never remove the file.
func TestAdoptionDropsTheLedgerEntry(t *testing.T) {
	dir := t.TempDir()
	local, err := storage.NewLocalBackend(dir, zerolog.Nop())
	require.NoError(t, err)
	defer local.Close()
	require.NoError(t, local.Write(context.Background(), "db/m/2026/10/01/10/x.parquet", []byte("12345678")))
	b := newReplayTestBuffer(t, t.TempDir())
	b.storage = local
	require.True(t, b.ambiguous.record("db/m/2026/10/01/10/x.parquet", 8, []string{"00000000000000010000000000000001"}))
	require.True(t, b.AdoptFlushedFile(context.Background(), wal.FlushIntent{Path: "db/m/2026/10/01/10/x.parquet", Size: 8, IDs: []string{"x"}}))
	require.Zero(t, b.ambiguous.count.Load())
}

// One hour file of a multi-hour flush is gone: its rows are replayed, the
// other hour's file is adopted.
func TestPartialMultiHourAdoption(t *testing.T) {
	f := newIntentFixture(t, false)
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC).UnixMicro()
	f.write(t, base, 10)
	f.write(t, base+int64(time.Hour/time.Microsecond), 6)
	f.crash(t)
	paths := parquetPaths(t, f.dataDir)
	require.Len(t, paths, 2)
	var gone string
	for _, p := range paths {
		if strings.Contains(filepath.ToSlash(p), "/11/") {
			gone = filepath.ToSlash(p)
		}
	}
	require.NotEmpty(t, gone)
	require.NoError(t, f.local.Delete(context.Background(), gone))
	stats, rows := f.restart(t, false)
	require.Equal(t, 1, stats.AdoptedFiles)
	require.Equal(t, 6, rows, "only the missing hour's rows replay")
}

func TestPartitionOfPath(t *testing.T) {
	db, m, h, ok := partitionOfPath("db/cpu/2026/10/01/10/cpu_x.parquet")
	require.True(t, ok)
	require.Equal(t, "db", db)
	require.Equal(t, "cpu", m)
	require.Equal(t, time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC), h)
	_, _, _, ok = partitionOfPath("db/cpu/x.parquet")
	require.False(t, ok)
}
