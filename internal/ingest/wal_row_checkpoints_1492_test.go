package ingest

import (
	"context"
	"errors"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// failNthStorage is a local backend whose n-th Write (1-based) fails; 0 never
// fails. It records the paths it stored.
type failNthStorage struct {
	storage.Backend
	mu     sync.Mutex
	writes int
	failAt int
	stored []string
}

func (s *failNthStorage) Write(ctx context.Context, path string, data []byte) error {
	s.mu.Lock()
	s.writes++
	n := s.writes
	s.mu.Unlock()
	if s.failAt > 0 && n == s.failAt {
		return errors.New("injected storage failure")
	}
	if err := s.Backend.Write(ctx, path, data); err != nil {
		return err
	}
	s.mu.Lock()
	s.stored = append(s.stored, path)
	s.mu.Unlock()
	return nil
}

var hourInPath = regexp.MustCompile(`/(\d{4})/(\d{2})/(\d{2})/(\d{2})/[^/]+\.parquet$`)

// storedHours is the set of hour ids (micros / hour) of the files stored.
func (s *failNthStorage) storedHours(t *testing.T) map[int64]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[int64]bool{}
	for _, p := range s.stored {
		m := hourInPath.FindStringSubmatch(p)
		require.NotNil(t, m, p)
		y, _ := strconv.Atoi(m[1])
		mo, _ := strconv.Atoi(m[2])
		d, _ := strconv.Atoi(m[3])
		h, _ := strconv.Atoi(m[4])
		out[hourIDOf(y, mo, d, h)] = true
	}
	return out
}

func hourIDOf(y, mo, d, h int) int64 {
	return hourBucketOfMicros(timeMicros(y, mo, d, h, 0))
}

const microsPerHour = int64(3_600_000_000)

func hourBucketOfMicros(us int64) int64 { return us / microsPerHour }

func timeMicros(y, mo, d, h, min int) int64 {
	return time.Date(y, time.Month(mo), d, h, min, 0, 0, time.UTC).UnixMicro()
}

// rowCheckpointFixture: three columnar writes, each with rows in every one of
// four hours, so every WAL entry straddles every hour file of the flush.
type rowCheckpointFixture struct {
	writer  *wal.Writer
	storage *failNthStorage
	buffer  *ArrowBuffer
	// token -> the time of each row, in entry order
	rows   map[string][]int64
	closed bool
}

// closeWriter closes the WAL once, like the process exiting; the buffer is
// closed first so its own Close does not write to a closed WAL.
func (f *rowCheckpointFixture) closeWriter(t *testing.T) {
	t.Helper()
	if f.closed {
		return
	}
	f.closed = true
	_ = f.buffer.Close()
	require.NoError(t, f.writer.Close())
}

func newRowCheckpointFixture(t *testing.T, failAt int) *rowCheckpointFixture {
	return newRowCheckpointFixtureCov(t, failAt, nil)
}

// newRowCheckpointFixtureCov enables coverage (#1207) with reg before the
// writes, so the batches carry coverage and their flush has a group (#1498).
func newRowCheckpointFixtureCov(t *testing.T, failAt int, reg *coverageRegistrar) *rowCheckpointFixture {
	t.Helper()
	tmp := t.TempDir()
	writer, err := wal.NewWriter(&wal.WriterConfig{
		WALDir: filepath.Join(tmp, "wal"), SyncMode: wal.SyncModeFsync, MaxSizeBytes: 100 << 20, Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	local, err := storage.NewLocalBackend(filepath.Join(tmp, "data"), zerolog.Nop())
	require.NoError(t, err)
	st := &failNthStorage{Backend: local, failAt: failAt}
	b := newReplayTestBuffer(t, filepath.Join(tmp, "unused"))
	b.storage = st
	b.SetWAL(writer)
	if reg != nil {
		b.SetFileRegistrar(reg)
		require.NoError(t, b.EnableCoverage())
	}
	f := &rowCheckpointFixture{writer: writer, storage: st, buffer: b, rows: map[string][]int64{}}
	t.Cleanup(func() { f.closeWriter(t); _ = local.Close() })
	base := timeMicros(2026, 10, 1, 10, 0)
	for i := 0; i < 3; i++ {
		var times []interface{}
		var vals []interface{}
		for h := 0; h < 4; h++ {
			for r := 0; r < 2+i; r++ { // 2, 3, 4 rows per hour: 8, 12, 16 rows per entry
				times = append(times, base+int64(h)*microsPerHour+int64(i*100+r))
				vals = append(vals, float64(i*1000+h*10+r))
			}
		}
		payload, err := msgpack.Marshal(map[string]interface{}{"m": "ticks", "columns": map[string]interface{}{"time": times, "v": vals}})
		require.NoError(t, err)
		decoded, err := NewMessagePackDecoder(zerolog.Nop()).Decode(payload)
		require.NoError(t, err)
		require.NoError(t, b.Write(context.Background(), "fx", decoded))
	}
	// Read each batch's token and rows from the buffer before the flush.
	key := "fx/ticks"
	shard := b.getShard(key)
	shard.mu.Lock()
	for _, item := range shard.buffers[key] {
		batch := item.(*TypedColumnBatch)
		require.Len(t, batch.WALHashes, 1)
		require.True(t, batch.hasWALCopy)
		f.rows[batch.WALHashes[0]] = append([]int64(nil), batch.Data["time"].([]int64)...)
	}
	shard.mu.Unlock()
	require.Len(t, f.rows, 3)
	return f
}

// coveredRows reads the row-range checkpoints in the WAL: token -> set of rows.
func (f *rowCheckpointFixture) coveredRows(t *testing.T) (map[string]map[int]bool, map[string]bool) {
	t.Helper()
	ids, err := f.writer.CurrentCheckpointHashes()
	require.NoError(t, err)
	ranges := map[string]map[int]bool{}
	whole := map[string]bool{}
	for _, id := range ids {
		if parent, s, e, ok := wal.ParseRecoveryRowIdentity(id); ok {
			if ranges[parent] == nil {
				ranges[parent] = map[int]bool{}
			}
			for r := s; r < e; r++ {
				require.False(t, ranges[parent][r], "row %d of %s checkpointed twice", r, parent)
				ranges[parent][r] = true
			}
			continue
		}
		whole[id] = true
	}
	return ranges, whole
}

// #1492: a multi-hour flush that fails part-way has checkpointed exactly the
// rows of the hour files it stored, and recovery replays exactly the others.
func TestMultiHourFlushCheckpointsStoredHoursAndRecoveryReplaysTheRest(t *testing.T) {
	f := newRowCheckpointFixture(t, 3) // the third hour file fails
	err := f.buffer.FlushAll(context.Background())
	require.Error(t, err)
	var partial *flushPartialError
	require.True(t, errors.As(err, &partial), "%v", err)
	require.Equal(t, 2, partial.StoredFiles)
	require.Equal(t, 0, partial.LostWithoutWAL)

	stored := f.storage.storedHours(t)
	require.Len(t, stored, 2)
	ranges, whole := f.coveredRows(t)
	for tok, times := range f.rows {
		require.False(t, whole[tok], "the parent must not be checkpointed by a failed flush")
		storedRows := 0
		for r, ts := range times {
			inStored := stored[hourBucketOfMicros(ts)]
			require.Equal(t, inStored, ranges[tok][r], "token %s row %d (hour stored=%v)", tok, r, inStored)
			if inStored {
				storedRows++
			}
		}
		require.Len(t, ranges[tok], storedRows)
	}
	require.Equal(t, partial.StoredRows, func() int {
		n := 0
		for _, m := range ranges {
			n += len(m)
		}
		return n
	}())

	// Recovery into a fresh buffer replays only the rows of the two hours that
	// were not stored, each range under its own identity.
	f.closeWriter(t)
	replayed := map[string]map[int]bool{}
	replayedValues := map[float64]int{}
	_, err = wal.NewRecovery(filepath.Dir(f.writer.CurrentFile()), zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error { return nil },
		&wal.RecoveryOptions{
			ColumnarCallbackWithTagColumns: func(_ context.Context, db, m string, cols map[string][]interface{}, id string, _ []string) error {
				parent, s, e, ok := wal.ParseRecoveryRowIdentity(id)
				require.True(t, ok, "whole-entry replay of %s", id)
				require.Len(t, cols["time"], e-s)
				if replayed[parent] == nil {
					replayed[parent] = map[int]bool{}
				}
				for r := s; r < e; r++ {
					require.False(t, replayed[parent][r])
					replayed[parent][r] = true
				}
				for i, v := range cols["v"] {
					replayedValues[v.(float64)]++
					require.Equal(t, f.rows[parent][s+i], cols["time"][i].(int64))
				}
				return nil
			},
			CheckpointRecovered: func(context.Context, []string) error { return nil },
		})
	require.NoError(t, err)
	for tok, times := range f.rows {
		for r, ts := range times {
			require.Equal(t, !stored[hourBucketOfMicros(ts)], replayed[tok][r], "token %s row %d", tok, r)
		}
	}
	for _, n := range replayedValues {
		require.Equal(t, 1, n)
	}
}

// A flush that completes checkpoints every hour file's rows and then the
// parents; a recovery pass has nothing to replay.
func TestMultiHourFlushCheckpointsEveryHourThenTheParents(t *testing.T) {
	f := newRowCheckpointFixture(t, 0)
	require.NoError(t, f.buffer.FlushAll(context.Background()))
	require.Len(t, f.storage.storedHours(t), 4)
	ranges, whole := f.coveredRows(t)
	for tok, times := range f.rows {
		require.True(t, whole[tok])
		require.Len(t, ranges[tok], len(times))
	}
	f.closeWriter(t)
	calls := 0
	_, err := wal.NewRecovery(filepath.Dir(f.writer.CurrentFile()), zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error { return nil },
		&wal.RecoveryOptions{ColumnarCallbackWithTagColumns: func(context.Context, string, string, map[string][]interface{}, string, []string) error {
			calls++
			return nil
		}})
	require.NoError(t, err)
	require.Equal(t, 0, calls)
}
