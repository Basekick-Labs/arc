package ingest

// #1475: buffer, coverage and tier-cache keys are database + "/" +
// measurement, so the buffer refuses a database or measurement containing "/"
// before any WAL work, on every path, as a content refusal. Entry points
// validate names first; this is the backstop for one that does not.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/basekick-labs/arc/pkg/models"
	"github.com/stretchr/testify/require"
)

const slashMeasurement = "cpu/load"

func TestBufferRefusesASlashInEitherKeyNameOnEveryPath(t *testing.T) {
	ctx := context.Background()
	cols := func() map[string][]interface{} {
		return map[string][]interface{}{"time": {time.Now().UnixMicro()}, "value": {1.0}}
	}
	for _, names := range []struct {
		label, database, measurement, quoted string
		want                                 error
	}{
		{"measurement", "db", slashMeasurement, `"cpu/load"`, ErrUnstorableMeasurementName},
		{"database", "spoke/db", "cpu", `"spoke/db"`, ErrUnstorableDatabaseName},
	} {
		db, m := names.database, names.measurement
		for name, write := range map[string]func(*ArrowBuffer) error{
			"WriteColumnarRecord": func(b *ArrowBuffer) error {
				return b.WriteColumnarRecord(ctx, db, &models.ColumnarRecord{Measurement: m, Columnar: true, Columns: cols()})
			},
			"WriteColumnarDirect": func(b *ArrowBuffer) error { return b.WriteColumnarDirect(ctx, db, m, cols()) },
			"WriteTypedColumnarDirect": func(b *ArrowBuffer) error {
				batch := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{time.Now().UnixMicro()}, "value": []float64{1}}}
				return b.WriteTypedColumnarDirect(ctx, db, m, batch, 1)
			},
			"WriteColumnarDirectReplay": func(b *ArrowBuffer) error {
				return b.WriteColumnarDirectReplay(ctx, db, m, cols(), "")
			},
		} {
			t.Run(names.label+"/"+name, func(t *testing.T) {
				b, w := newCheckpointBuffer(1000)
				defer b.Close()
				err := write(b)
				require.ErrorIs(t, err, names.want)
				require.True(t, IsContentRefusal(err))
				require.Contains(t, err.Error(), names.quoted)
				if handedOut, _ := w.counts(); handedOut != 0 {
					t.Fatalf("a refused write reached the WAL (%d entries)", handedOut)
				}
				require.Zero(t, b.BufferedBytes(), "a refused write was buffered")
			})
		}
	}
}

// In a batch the record is dropped and the others are written, as for the
// other content refusals; the WAL holds only what was written.
func TestWriteDropsASlashMeasurementAndWritesTheRest(t *testing.T) {
	for _, mode := range walModes {
		t.Run(string(mode), func(t *testing.T) {
			buf, w, dir := newWALRefusalRig(t, mode, nil)
			ok := &models.ColumnarRecord{Measurement: "aaa", Columnar: true, Columns: refusalTestColumns()}
			bad := &models.ColumnarRecord{Measurement: slashMeasurement, Columnar: true, Columns: refusalTestColumns()}
			err := buf.Write(context.Background(), "d", []interface{}{ok, bad})
			var pd *PointsDroppedError
			require.True(t, errors.As(err, &pd), "err = %v (%T), want PointsDroppedError", err, err)
			require.Len(t, pd.Drops, 1)
			require.Equal(t, slashMeasurement, pd.Drops[0].Measurement)
			require.Equal(t, 1, replayedWALEntries(t, w, dir), "only the written record is in the WAL")
			_ = buf.Close()
		})
	}
}

func TestKeyBackstopAcceptsOrdinaryNames(t *testing.T) {
	for _, n := range []string{"cpu", "cpu-load", "cpu_load", "CPU.load", "x", "rocket.01"} {
		require.NoError(t, unstorableKeyError(n, n), n)
	}
}

// Row records reach the same backstop through rowsToColumnar.
func TestWriteRefusesASlashMeasurementInRowRecords(t *testing.T) {
	b, w := newCheckpointBuffer(1000)
	defer b.Close()
	err := b.Write(context.Background(), "db", []interface{}{
		&models.Record{Measurement: slashMeasurement, Time: time.Now(), Fields: map[string]interface{}{"v": 1.0}},
	})
	// The batch's only record: the refusal itself, as for the other content
	// refusals (a partial write needs something written).
	require.ErrorIs(t, err, ErrUnstorableMeasurementName)
	if handedOut, _ := w.counts(); handedOut != 0 {
		t.Fatalf("a refused row write reached the WAL (%d entries)", handedOut)
	}
}
