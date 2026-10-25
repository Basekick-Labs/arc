package ingest

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// textBinaryRegistrar answers KnownTextBinaryColumns from a fixed map.
type textBinaryRegistrar map[string]map[string]bool

func (textBinaryRegistrar) EnsureFile(context.Context, string, string, *arrow.Schema, []string, string) error {
	return nil
}

func (r textBinaryRegistrar) KnownTextBinaryColumns(database, measurement string) map[string]bool {
	return r[database+"/"+measurement]
}

func decodeMsgpack(t *testing.T, v interface{}) interface{} {
	t.Helper()
	payload, err := msgpack.Marshal(v)
	require.NoError(t, err)
	decoded, err := NewMessagePackDecoder(zerolog.Nop()).Decode(payload)
	require.NoError(t, err)
	return decoded
}

const ts440 = int64(1_760_000_000_000_000)

// A live write that would change a column between string and binary is
// refused with ErrColumnTypeChange, on every write shape, in both directions.
func TestColumnTypeChangeRefused(t *testing.T) {
	ctx := context.Background()
	r := newFlushRig(t, nil)
	r.buf.SetFieldSchema(textBinaryRegistrar{"d/m": {"name": false, "geom": true}})

	// columnar msgpack: bytes into a string column
	err := r.buf.Write(ctx, "d", decodeMsgpack(t, map[string]interface{}{"m": "m", "columns": map[string]interface{}{
		"time": []interface{}{ts440, ts440 + 1}, "name": []interface{}{nil, []byte{0x01}}}}))
	require.True(t, errors.Is(err, ErrColumnTypeChange), "columnar bytes->string column: %v", err)

	// row msgpack: a string into a binary column
	err = r.buf.Write(ctx, "d", decodeMsgpack(t, []interface{}{map[string]interface{}{
		"m": "m", "t": ts440, "fields": map[string]interface{}{"geom": "POINT(1 2)"}}}))
	require.True(t, errors.Is(err, ErrColumnTypeChange), "row string->binary column: %v", err)

	// typed fast path: a string column into a binary column
	batch := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{ts440}, "geom": []string{"x"}}}
	err = r.buf.WriteTypedColumnarDirect(ctx, "d", "m", batch, 1)
	require.True(t, errors.Is(err, ErrColumnTypeChange), "typed string->binary column: %v", err)

	// direct columnar (continuous queries, line protocol)
	err = r.buf.WriteColumnarDirect(ctx, "d", "m", map[string][]interface{}{"time": {ts440}, "name": {[]byte{0x02}}})
	require.True(t, errors.Is(err, ErrColumnTypeChange), "direct bytes->string column: %v", err)

	// the same kinds, another measurement and an untracked column: accepted
	require.NoError(t, r.buf.Write(ctx, "d", decodeMsgpack(t, map[string]interface{}{"m": "m", "columns": map[string]interface{}{
		"time": []interface{}{ts440}, "name": []interface{}{"a"}, "geom": []interface{}{[]byte{0x03}}, "other": []interface{}{[]byte{0x04}}}})))
	require.NoError(t, r.buf.WriteColumnarDirect(ctx, "d", "n", map[string][]interface{}{"time": {ts440}, "name": {[]byte{0x05}}}))
	files := r.files(t)
	require.Len(t, files, 2)
}

// A refused batch writes nothing, not the records before the refused one.
func TestColumnTypeChangeRefusesTheWholeBatch(t *testing.T) {
	ctx := context.Background()
	r := newFlushRig(t, nil)
	r.buf.SetFieldSchema(textBinaryRegistrar{"d/bad": {"name": false}})
	batch := []interface{}{
		&models.ColumnarRecord{Measurement: "good", Columnar: true, Columns: map[string][]interface{}{"time": {ts440}, "v": {1.0}}},
		&models.ColumnarRecord{Measurement: "bad", Columnar: true, Columns: map[string][]interface{}{"time": {ts440}, "name": {[]byte{1}}}},
	}
	err := r.buf.Write(ctx, "d", batch)
	require.True(t, errors.Is(err, ErrColumnTypeChange), "err = %v", err)
	require.Empty(t, r.files(t), "a refused batch wrote some of its records")

	// Columnar records are written before row records, so without the
	// pre-check the good one would be written before the bad row is refused.
	good := &models.ColumnarRecord{Measurement: "good", Columnar: true, Columns: map[string][]interface{}{"time": {ts440}, "v": {1.0}}}
	rows := []interface{}{
		good,
		&models.Record{Measurement: "bad", Timestamp: ts440, Fields: map[string]interface{}{"name": []byte{1}}},
	}
	err = r.buf.Write(ctx, "d", rows)
	require.True(t, errors.Is(err, ErrColumnTypeChange), "rows: err = %v", err)
	require.Empty(t, r.files(t), "a refused row batch wrote some of its records")
	r.buf.SetFieldSchema(textBinaryRegistrar{"d/bad": {"tagged": true}})
	err = r.buf.Write(ctx, "d", []interface{}{good,
		&models.Record{Measurement: "bad", Timestamp: ts440, Fields: map[string]interface{}{"v": 1.0}, Tags: map[string]string{"tagged": "x"}}})
	require.True(t, errors.Is(err, ErrColumnTypeChange), "string tag into a binary column: err = %v", err)
	require.Empty(t, r.files(t))
}

// A refused write never reaches the WAL, so a crash before the purge does not
// replay a write the client was told was refused. A replayed entry is never
// refused: it was accepted when it was written.
func TestColumnTypeChangeRefusedBeforeTheWAL(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	walDir := filepath.Join(tmp, "wal")
	writer, err := wal.NewWriter(&wal.WriterConfig{WALDir: walDir, SyncMode: wal.SyncModeFsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	live := newReplayTestBuffer(t, filepath.Join(tmp, "live"))
	live.SetWAL(writer)
	t.Cleanup(func() { _ = live.Close() })
	reg := textBinaryRegistrar{"d/m": {"name": false}}
	live.SetFieldSchema(reg)

	err = live.Write(ctx, "d", decodeMsgpack(t, map[string]interface{}{"m": "m", "columns": map[string]interface{}{
		"time": []interface{}{ts440}, "name": []interface{}{[]byte{0xff}}}}))
	require.True(t, errors.Is(err, ErrColumnTypeChange), "err = %v", err)
	// The direct path (continuous queries) is refused by the buffer's
	// backstop, which must also run before the WAL append.
	err = live.WriteColumnarDirect(ctx, "d", "m", map[string][]interface{}{"time": {ts440}, "name": {[]byte{0xfe}}})
	require.True(t, errors.Is(err, ErrColumnTypeChange), "direct: err = %v", err)
	require.NoError(t, writer.Close())

	entries := 0
	recovery := wal.NewRecovery(walDir, zerolog.Nop())
	_, err = recovery.RecoverWithOptions(ctx, func(context.Context, []map[string]interface{}) error { entries++; return nil },
		&wal.RecoveryOptions{ColumnarCallbackWithTagColumns: func(context.Context, string, string, map[string][]interface{}, string, []string) error {
			entries++
			return nil
		}})
	require.NoError(t, err)
	require.Zero(t, entries, "the refused write is in the WAL")

	// Replay of the same columns into a buffer that knows the column as a
	// string: written, not refused.
	replay := newFlushRig(t, nil)
	replay.buf.SetFieldSchema(reg)
	require.NoError(t, replay.buf.WriteColumnarDirectReplay(ctx, "d", "m",
		map[string][]interface{}{"time": {ts440}, "name": {[]byte{0xff}}}, ""))
	require.Len(t, replay.files(t), 1)
}

// Binary values come back from the WAL as the same bytes: values with every
// byte value, an empty value distinct from NULL (#440).
func TestBinaryColumnSurvivesWALRecovery(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	walDir := filepath.Join(tmp, "wal")
	writer, err := wal.NewWriter(&wal.WriterConfig{WALDir: walDir, SyncMode: wal.SyncModeFsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	live := newReplayTestBuffer(t, filepath.Join(tmp, "live"))
	live.SetWAL(writer)
	t.Cleanup(func() { _ = live.Close() })
	want := []interface{}{[]byte{0x00, 0xff, 0x80}, nil, []byte{}}
	shapes := map[string]interface{}{
		// columnar with time: the raw client payload is the WAL entry
		"columnar": map[string]interface{}{"m": "a", "columns": map[string]interface{}{
			"time": []interface{}{ts440, ts440 + 1, ts440 + 2}, "geom": want}},
		// row format: re-encoded through columnarToWALRecords
		"rows": []interface{}{
			map[string]interface{}{"m": "b", "t": ts440, "fields": map[string]interface{}{"geom": want[0]}},
			map[string]interface{}{"m": "b", "t": ts440 + 1, "fields": map[string]interface{}{"v": 1.0}},
			map[string]interface{}{"m": "b", "t": ts440 + 2, "fields": map[string]interface{}{"geom": want[2]}},
		},
	}
	for _, name := range []string{"columnar", "rows"} {
		require.NoError(t, live.Write(ctx, "d", decodeMsgpack(t, shapes[name])), name)
	}
	require.NoError(t, writer.Close())

	got := map[string][]interface{}{}
	recovery := wal.NewRecovery(walDir, zerolog.Nop())
	_, err = recovery.RecoverWithOptions(ctx, func(_ context.Context, rows []map[string]interface{}) error {
		for _, row := range rows {
			m, _ := row["_measurement"].(string)
			got[m] = append(got[m], row["geom"])
		}
		return nil
	}, &wal.RecoveryOptions{ColumnarCallbackWithTagColumns: func(_ context.Context, _, m string, cols map[string][]interface{}, _ string, _ []string) error {
		got[m] = append(got[m], cols["geom"]...)
		return nil
	}})
	require.NoError(t, err)
	for _, m := range []string{"a", "b"} {
		vals := got[m]
		require.Len(t, vals, 3, "measurement %s: %v", m, vals)
		require.True(t, bytes.Equal(vals[0].([]byte), want[0].([]byte)), "%s[0] = %#v", m, vals[0])
		require.Nil(t, vals[1], "%s[1]: NULL came back as %#v", m, vals[1])
		b, ok := vals[2].([]byte)
		require.True(t, ok && len(b) == 0, "%s[2]: empty value came back as %#v", m, vals[2])
	}
}

// The typed WAL encoder writes a binary column as msgpack bin, an empty value
// as a zero-length bin and a NULL as an absent cell.
func TestEncodeTypedWALRowsBinary(t *testing.T) {
	batch := &TypedColumnBatch{
		// Row 3 is a valid value held as a nil slice, which merging and
		// slicing can produce: still an empty value, not a NULL.
		Data:     map[string]interface{}{"time": []int64{1, 2, 3, 4}, "geom": [][]byte{{0xff}, nil, {}, nil}},
		Validity: map[string][]bool{"geom": {true, false, true, true}},
	}
	payload, err := encodeTypedWALRows("d", "m", batch, 4, nil)
	require.NoError(t, err)
	var rows []map[string]interface{}
	require.NoError(t, msgpack.Unmarshal(payload, &rows))
	require.Len(t, rows, 4)
	b3, ok3 := rows[3]["geom"].([]byte)
	require.True(t, ok3 && len(b3) == 0, "valid nil slice = %#v, want an empty value", rows[3]["geom"])
	require.Equal(t, []byte{0xff}, rows[0]["geom"])
	_, present := rows[1]["geom"]
	require.False(t, present, "a NULL cell was written")
	b, ok := rows[2]["geom"].([]byte)
	require.True(t, ok && len(b) == 0, "empty value = %#v", rows[2]["geom"])
}

// Buffered-bytes accounting counts a binary column's bytes.
func TestEstimateTypedColumnBatchBytesCountsBinary(t *testing.T) {
	small := estimateTypedColumnBatchBytes(&TypedColumnBatch{Data: map[string]interface{}{"geom": [][]byte{{}}}})
	big := estimateTypedColumnBatchBytes(&TypedColumnBatch{Data: map[string]interface{}{"geom": [][]byte{make([]byte, 1<<20)}}})
	require.GreaterOrEqual(t, big-small, int64(1<<20))
}

// Batches of a binary measurement keep the sorted-merge fast path, and the
// bytes and NULLs follow their rows.
func TestSortedMergeKeepsBinaryColumns(t *testing.T) {
	buf := createTestArrowBuffer(t)
	if keys := buf.getSortKeys("geo"); len(keys) != 1 || keys[0] != "time" {
		t.Skipf("sort keys for the test buffer are %v; the fast path needs [time]", keys)
	}
	a := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1, 3}, "geom": [][]byte{{1}, nil}},
		Validity: map[string][]bool{"geom": {true, false}}}
	b := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{2}, "geom": [][]byte{{2}}}}
	merged, ok := buf.mergeSortedByTime("geo", []interface{}{a, b})
	require.True(t, ok, "binary column turned the sorted-merge fast path off")
	require.Equal(t, []int64{1, 2, 3}, merged.Data["time"])
	require.Equal(t, [][]byte{{1}, {2}, nil}, merged.Data["geom"])
	require.Equal(t, []bool{true, true, false}, merged.Validity["geom"])
}

// A row field named like a tag is stored as "<name>_value": the check reads
// the column the field lands in, not the field's name. Wrongly refusing the
// first case and missing the second (after writing the batch's first record)
// were both possible when the raw rows were checked.
func TestColumnTypeChangeChecksTheRenamedRowField(t *testing.T) {
	ctx := context.Background()
	r := newFlushRig(t, nil)
	r.buf.SetFieldSchema(textBinaryRegistrar{"d/m": {"host": false, "host_value": true}})
	tagged := func(v interface{}) *models.Record {
		return &models.Record{Measurement: "m", Timestamp: ts440, Tags: map[string]string{"host": "a"}, Fields: map[string]interface{}{"host": v}}
	}
	good := &models.ColumnarRecord{Measurement: "good", Columnar: true, Columns: map[string][]interface{}{"time": {ts440}, "v": {1.0}}}

	// Bytes in field "host" go to binary "host_value": accepted.
	require.NoError(t, r.buf.Write(ctx, "d", []interface{}{tagged([]byte{1})}))
	// A string in field "host" would go to binary "host_value": refused, and
	// the batch's first record is not written either.
	err := r.buf.Write(ctx, "d", []interface{}{good, tagged("x")})
	require.True(t, errors.Is(err, ErrColumnTypeChange), "err = %v", err)
	for _, f := range r.files(t) {
		require.NotContains(t, f.path, "/good/", "a refused batch wrote its first record")
	}
}

// A refused typed write never reaches the WAL either.
func TestTypedColumnTypeChangeRefusedBeforeTheWAL(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	walDir := filepath.Join(tmp, "wal")
	writer, err := wal.NewWriter(&wal.WriterConfig{WALDir: walDir, SyncMode: wal.SyncModeFsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	live := newReplayTestBuffer(t, filepath.Join(tmp, "live"))
	live.SetWAL(writer)
	t.Cleanup(func() { _ = live.Close() })
	live.SetFieldSchema(textBinaryRegistrar{"d/m": {"geom": true}})
	batch := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{ts440}, "geom": []string{"x"}}}
	err = live.WriteTypedColumnarDirect(ctx, "d", "m", batch, 1)
	require.True(t, errors.Is(err, ErrColumnTypeChange), "err = %v", err)
	require.NoError(t, writer.Close())
	entries := 0
	_, err = wal.NewRecovery(walDir, zerolog.Nop()).RecoverWithOptions(ctx, func(context.Context, []map[string]interface{}) error { entries++; return nil },
		&wal.RecoveryOptions{ColumnarCallbackWithTagColumns: func(context.Context, string, string, map[string][]interface{}, string, []string) error {
			entries++
			return nil
		}})
	require.NoError(t, err)
	require.Zero(t, entries, "the refused typed write is in the WAL")
}

// Bytes refused for a string column say how to recover data that is binary
// stored as text (what Parquet import made of binary columns before
// 27.01.1); a string refused for a binary column does not.
func TestColumnTypeChangeMessageNamesTheRecovery(t *testing.T) {
	require.Contains(t, columnTypeChangeError("m", "geom", false).Error(), "write the binary values to a new column")
	require.NotContains(t, columnTypeChangeError("m", "geom", true).Error(), "new column")
	require.True(t, errors.Is(columnTypeChangeError("m", "geom", false), ErrColumnTypeChange))
}
