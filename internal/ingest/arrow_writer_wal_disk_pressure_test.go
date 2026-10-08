package ingest

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

type diskPressureTestWAL struct{}

func (diskPressureTestWAL) Append([]map[string]interface{}) error  { return nil }
func (diskPressureTestWAL) AppendRaw([]byte) error                 { return nil }
func (diskPressureTestWAL) AppendRawWithMeta(string, []byte) error { return nil }
func (diskPressureTestWAL) Stats() map[string]interface{}          { return nil }
func (diskPressureTestWAL) Close() error                           { return nil }
func (diskPressureTestWAL) AppendTracked([]map[string]interface{}) ([]string, error) {
	return nil, wal.ErrWALDiskPressure
}
func (diskPressureTestWAL) AppendRawWithMetaTracked(string, []byte) ([]string, error) {
	return nil, wal.ErrWALDiskPressure
}
func (diskPressureTestWAL) MarkFlushed([]string) error { return nil }
func (diskPressureTestWAL) ForgetTracked([]string)     {}

func TestTypedColumnarPropagatesWALDiskPressure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rawPayload []byte
	}{
		{name: "typed fallback"},
		{name: "raw MessagePack", rawPayload: []byte{0x81}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.IngestConfig{
				MaxBufferSize:  1000,
				MaxBufferAgeMS: 60000,
				Compression:    "snappy",
				ShardCount:     4,
				FlushWorkers:   1,
				FlushQueueSize: 16,
			}
			buffer := NewArrowBuffer(cfg, &mockStorageBackend{}, zerolog.New(io.Discard))
			t.Cleanup(func() { _ = buffer.Close() })
			buffer.SetWAL(diskPressureTestWAL{})

			batch := &TypedColumnBatch{Data: map[string]interface{}{
				"value": []float64{1},
			}}
			err := buffer.writeTypedColumnarRaw(context.Background(), "testdb", "pressure_probe", batch, 1, tc.rawPayload, false, nil)
			if !errors.Is(err, wal.ErrWALDiskPressure) {
				t.Fatalf("writeTypedColumnarRaw() error = %v, want ErrWALDiskPressure", err)
			}
		})
	}
}

type batchRejectingWAL struct {
	reservations int
	payloads     int
	appendCalls  int
}

func (w *batchRejectingWAL) Append([]map[string]interface{}) error  { w.appendCalls++; return nil }
func (w *batchRejectingWAL) AppendRaw([]byte) error                 { w.appendCalls++; return nil }
func (w *batchRejectingWAL) AppendRawWithMeta(string, []byte) error { w.appendCalls++; return nil }
func (w *batchRejectingWAL) Stats() map[string]interface{}          { return nil }
func (w *batchRejectingWAL) Close() error                           { return nil }
func (w *batchRejectingWAL) AppendTracked([]map[string]interface{}) ([]string, error) {
	w.appendCalls++
	return nil, nil
}
func (w *batchRejectingWAL) AppendRawWithMetaTracked(string, []byte) ([]string, error) {
	w.appendCalls++
	return nil, nil
}
func (w *batchRejectingWAL) AppendRawWithMetaTrackedReserved(string, []byte, *wal.DiskReservation) ([]string, error) {
	w.appendCalls++
	return nil, nil
}
func (w *batchRejectingWAL) ReserveRawWithMetaBatch(_ string, payloads [][]byte) (*wal.DiskReservation, error) {
	w.reservations++
	w.payloads += len(payloads)
	return nil, wal.ErrWALDiskPressure
}
func (*batchRejectingWAL) MarkFlushed([]string) error { return nil }
func (*batchRejectingWAL) ForgetTracked([]string)     {}

func TestLineProtocolMeasurementBatchRejectsBeforeBufferMutation(t *testing.T) {
	walWriter := &batchRejectingWAL{}
	buffer := newDiskPressureAdmissionTestBuffer(t, walWriter)

	records := map[string]*models.ColumnarRecord{
		"first": {
			Measurement: "first",
			Columns: map[string][]interface{}{
				"time":  []interface{}{int64(1)},
				"value": []interface{}{float64(1)},
			},
		},
		"second": {
			Measurement: "second",
			Columns: map[string][]interface{}{
				"time":  []interface{}{int64(2)},
				"value": []interface{}{float64(2)},
			},
		},
	}

	err := buffer.WriteColumnarBatch(context.Background(), "testdb", records)
	assertBatchAdmissionRejectedBeforeMutation(t, buffer, walWriter, err)
}

func TestMessagePackMeasurementBatchRejectsBeforeBufferMutation(t *testing.T) {
	walWriter := &batchRejectingWAL{}
	buffer := newDiskPressureAdmissionTestBuffer(t, walWriter)

	records := []interface{}{
		&models.ColumnarRecord{Measurement: "first", RawPayload: []byte{0x91, 0x01}},
		&models.ColumnarRecord{Measurement: "second", RawPayload: []byte{0x91, 0x02}},
	}
	err := buffer.Write(context.Background(), "testdb", records)
	assertBatchAdmissionRejectedBeforeMutation(t, buffer, walWriter, err)
}

func newDiskPressureAdmissionTestBuffer(t *testing.T, walWriter WALWriter) *ArrowBuffer {
	t.Helper()
	cfg := &config.IngestConfig{
		MaxBufferSize:  1000,
		MaxBufferAgeMS: 60000,
		Compression:    "snappy",
		ShardCount:     4,
		FlushWorkers:   1,
		FlushQueueSize: 16,
	}
	buffer := NewArrowBuffer(cfg, &mockStorageBackend{}, zerolog.New(io.Discard))
	buffer.SetWAL(walWriter)
	t.Cleanup(func() { _ = buffer.Close() })
	return buffer
}

func assertBatchAdmissionRejectedBeforeMutation(t *testing.T, buffer *ArrowBuffer, walWriter *batchRejectingWAL, err error) {
	t.Helper()
	if !errors.Is(err, wal.ErrWALDiskPressure) {
		t.Fatalf("batch write error = %v, want ErrWALDiskPressure", err)
	}
	if walWriter.reservations != 1 || walWriter.payloads != 2 {
		t.Fatalf("batch reservations/payloads = %d/%d, want 1/2", walWriter.reservations, walWriter.payloads)
	}
	if walWriter.appendCalls != 0 {
		t.Fatalf("batch rejection attempted %d WAL appends", walWriter.appendCalls)
	}
	if got := buffer.totalRecordsBuffered.Load(); got != 0 {
		t.Fatalf("batch rejection buffered %d records", got)
	}
	for _, shard := range buffer.shards {
		shard.mu.Lock()
		bufferCount := len(shard.buffers)
		shard.mu.Unlock()
		if bufferCount != 0 {
			t.Fatalf("batch rejection left %d measurement buffers populated", bufferCount)
		}
	}
}
