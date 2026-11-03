package ingest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/basekick-labs/arc/internal/wal"
)

// notDurableWAL refuses every tracked append the way the real writer does in
// wal.ack_mode = "durable" when the covering sync fails.
type notDurableWAL struct{ checkpointRecordingWAL }

func (w *notDurableWAL) AppendRawWithMetaTracked(database string, payload []byte) ([]string, error) {
	return nil, fmt.Errorf("%w: injected sync failure", wal.ErrNotDurable)
}

// A live write whose WAL entry is not durable is refused with a retryable
// not-durable refusal, and nothing is buffered.
func TestWALNotDurableRefusesLiveWrite(t *testing.T) {
	for name, write := range liveWrites(context.Background()) {
		t.Run(name, func(t *testing.T) {
			buffer, _ := newCheckpointBuffer(1000)
			defer buffer.Close()
			buffer.SetWAL(&notDurableWAL{})
			before := buffer.totalRecordsBuffered.Load()
			err := write(buffer)
			if !errors.Is(err, ErrWALNotDurable) || !IsWriteRefusal(err) || RefusalReason(err) != RefusalWALNotDurable {
				t.Fatalf("err = %v, want ErrWALNotDurable refusal", err)
			}
			if errors.Is(err, ErrIngestFenced) {
				t.Fatal("a not-durable refusal reads as a fence: clients would retry elsewhere")
			}
			if after := buffer.totalRecordsBuffered.Load(); after != before {
				t.Fatalf("records buffered %d -> %d: a not-durable write must buffer nothing", before, after)
			}
		})
	}
}

// Queued-mode WAL errors keep the old behaviour: logged, the write is accepted.
func TestWALQueuedErrorStillAcceptsWrite(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	buffer.SetWAL(&droppingWAL{})
	if err := buffer.WriteColumnarRecord(context.Background(), "db", checkpointTestRecord()); err != nil {
		t.Fatalf("queued-mode WAL drop failed the write: %v", err)
	}
}

type droppingWAL struct{ checkpointRecordingWAL }

func (w *droppingWAL) AppendRawWithMetaTracked(database string, payload []byte) ([]string, error) {
	return nil, wal.ErrWALDropped
}

// durableDroppingWAL reports durable mode but fails with a non-durability
// error: ingest must still refuse, never buffer and acknowledge.
type durableDroppingWAL struct{ checkpointRecordingWAL }

func (w *durableDroppingWAL) AppendRawWithMetaTracked(database string, payload []byte) ([]string, error) {
	return nil, errors.New("payload too large")
}
func (w *durableDroppingWAL) DurableAck() bool { return true }

func TestWALDurableAnyErrorRefuses(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	buffer.SetWAL(&durableDroppingWAL{})
	before := buffer.totalRecordsBuffered.Load()
	err := buffer.WriteColumnarRecord(context.Background(), "db", checkpointTestRecord())
	if !errors.Is(err, ErrWALNotDurable) {
		t.Fatalf("err = %v, want a not-durable refusal", err)
	}
	if buffer.totalRecordsBuffered.Load() != before {
		t.Fatal("a refused durable write was buffered")
	}
}
