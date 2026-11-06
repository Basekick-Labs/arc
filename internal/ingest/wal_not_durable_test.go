package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
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

type walCtxKey struct{}

// contextWAL refuses like the real writer, after recording whether the append
// was given the write's own context.
type contextWAL struct {
	checkpointRecordingWAL
	sawWriteContext atomic.Bool
}

func (w *contextWAL) AppendRawWithMetaTrackedContext(ctx context.Context, _ string, _ []byte) ([]string, error) {
	w.sawWriteContext.Store(ctx.Value(walCtxKey{}) == true)
	return nil, fmt.Errorf("%w: %w", wal.ErrNotDurable, context.DeadlineExceeded)
}

func (w *contextWAL) DurableAck() bool { return true }

// #1501: every live write hands its own context to the WAL's durable wait, and
// a wait that ended is an ordinary not-durable refusal with nothing buffered.
func TestWALDurableWaitGetsTheWriteContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), walCtxKey{}, true)
	for name, write := range liveWrites(ctx) {
		t.Run(name, func(t *testing.T) {
			buffer, _ := newCheckpointBuffer(1000)
			defer buffer.Close()
			w := &contextWAL{}
			buffer.SetWAL(w)
			before := buffer.totalRecordsBuffered.Load()
			if err := write(buffer); !errors.Is(err, ErrWALNotDurable) {
				t.Fatalf("err = %v, want ErrWALNotDurable", err)
			}
			if !w.sawWriteContext.Load() {
				t.Fatal("the WAL append did not get the write's context")
			}
			if after := buffer.totalRecordsBuffered.Load(); after != before {
				t.Fatalf("records buffered %d -> %d: a refused write must buffer nothing", before, after)
			}
		})
	}
}

// A write whose own context ended gets the context's error: the caller gave up,
// which is not a refusal by the WAL and must not count as one.
func TestWALDurableWaitEndedByTheWriteContextIsNotARefusal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for name, write := range liveWrites(ctx) {
		t.Run(name, func(t *testing.T) {
			buffer, _ := newCheckpointBuffer(1000)
			defer buffer.Close()
			buffer.SetWAL(&contextWAL{})
			before := metrics.Get().Snapshot()["wal_not_durable_rejections"]
			err := write(buffer)
			if !errors.Is(err, context.Canceled) || IsWriteRefusal(err) {
				t.Fatalf("err = %v, want context.Canceled and no refusal", err)
			}
			if after := metrics.Get().Snapshot()["wal_not_durable_rejections"]; after != before {
				t.Fatalf("arc_wal_not_durable_rejections_total moved %v -> %v", before, after)
			}
		})
	}
}
