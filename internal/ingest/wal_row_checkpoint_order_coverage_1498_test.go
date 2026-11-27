package ingest

import (
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// orderedCoveredWAL is a real writer (coverage needs one) whose checkpoints
// are logged: row ranges apart from the flush's own.
type orderedCoveredWAL struct {
	*wal.Writer
	log *orderLog
}

func (w *orderedCoveredWAL) MarkFlushedContext(ctx context.Context, ids []string) error {
	kind := "checkpoint"
	if _, _, _, ok := wal.ParseRecoveryRowIdentity(ids[0]); ok {
		kind = "rowcheckpoint"
	}
	w.log.add(kind)
	return w.Writer.MarkFlushedContext(ctx, ids)
}

func (w *orderedCoveredWAL) MarkFlushedRetained(ids []string) error {
	w.log.add("checkpoint")
	return w.Writer.MarkFlushedRetained(ids)
}

type orderedCoverageRegistrar struct {
	coverageRegistrar
	log *orderLog
}

func (r *orderedCoverageRegistrar) RegisterCoveredFile(db, m, path string, pt time.Time, size int64, sum string, cov *GroupCoverage) {
	r.log.add("register")
	r.coverageRegistrar.RegisterCoveredFile(db, m, path, pt, size, sum, cov)
}

func (r *orderedCoverageRegistrar) SyncRegistrations() error {
	r.log.add("sync")
	return nil
}

// #1498 keeps #1209 per hour file on a covered writer too: register, make the
// registration durable, then checkpoint the file's rows; the flush's own sync
// and checkpoint follow the last file.
func TestCoveredMultiHourFlushRegistersAndSyncsEachHourBeforeItsRowCheckpoint(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: filepath.Join(dir, "wal"), SyncMode: wal.SyncModeAsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	log := &orderLog{}
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy", ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 16},
		&mockStorageBackend{}, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close(); _ = w.Close() })
	b.SetWAL(&orderedCoveredWAL{Writer: w, log: log})
	reg := &orderedCoverageRegistrar{log: log}
	b.SetFileRegistrar(reg)
	require.NoError(t, b.EnableCoverage())

	hour := int64(3_600_000_000)
	base := int64(1_790_856_000_000_000)
	for i := 0; i < 2; i++ {
		rec := &models.ColumnarRecord{
			Measurement: "ticks", Columnar: true, TimeUnit: "us", RawPayload: []byte{0x01, byte(i)},
			Columns: map[string][]interface{}{
				"time":  {base + int64(i), base + hour + int64(i), base + 2*hour + int64(i)},
				"value": {1.0, 2.0, 3.0},
			},
		}
		require.NoError(t, b.writeColumnarInternal(context.Background(), "default", rec, false, ""))
	}
	require.NoError(t, b.FlushAll(context.Background()))

	var want []string
	for i := 0; i < 3; i++ {
		want = append(want, "register", "sync", "rowcheckpoint")
	}
	want = append(want, "sync", "checkpoint")
	require.Equal(t, want, log.snapshot())
	require.Len(t, reg.snapshot(), 3)
}
