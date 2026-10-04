package ingest

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

// checkpointRecordingWAL is a trackedWALWriter that records which identities it
// handed out and which were later checkpointed. The gap between the two is the
// bug under test.
type checkpointRecordingWAL struct {
	mu        sync.Mutex
	handedOut []string
	flushed   []string
	n         int
}

func (w *checkpointRecordingWAL) Append(records []map[string]interface{}) error           { return nil }
func (w *checkpointRecordingWAL) AppendRaw(payload []byte) error                          { return nil }
func (w *checkpointRecordingWAL) AppendRawWithMeta(database string, payload []byte) error { return nil }
func (w *checkpointRecordingWAL) Stats() map[string]interface{}                           { return nil }
func (w *checkpointRecordingWAL) Close() error                                            { return nil }

func (w *checkpointRecordingWAL) AppendTracked(records []map[string]interface{}) ([]string, error) {
	return w.issue(), nil
}

func (w *checkpointRecordingWAL) AppendRawWithMetaTracked(database string, payload []byte) ([]string, error) {
	return w.issue(), nil
}

func (w *checkpointRecordingWAL) issue() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.n++
	token := "identity-" + string(rune('A'+(w.n-1)%26))
	w.handedOut = append(w.handedOut, token)
	return []string{token}
}

func (w *checkpointRecordingWAL) MarkFlushed(hashes []string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushed = append(w.flushed, hashes...)
	return nil
}

func (w *checkpointRecordingWAL) counts() (handedOut, flushed int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.handedOut), len(w.flushed)
}

func newCheckpointBuffer(maxBufferSize int) (*ArrowBuffer, *checkpointRecordingWAL) {
	cfg := &config.IngestConfig{
		MaxBufferSize: maxBufferSize,
		// Long enough that nothing flushes by age: the async tests must flush
		// because the buffer is FULL, which is the path that was broken.
		MaxBufferAgeMS: 600000,
		Compression:    "snappy",
		ShardCount:     4,
		FlushWorkers:   2,
		FlushQueueSize: 16,
	}
	buffer := NewArrowBuffer(cfg, &mockStorageBackend{}, zerolog.New(io.Discard))
	walWriter := &checkpointRecordingWAL{}
	buffer.SetWAL(walWriter)
	return buffer, walWriter
}

func checkpointTestRecord() *models.ColumnarRecord {
	return &models.ColumnarRecord{
		Measurement: "checkpoint_probe",
		Columnar:    true,
		Columns: map[string][]interface{}{
			"time":  {time.Now().UTC().UnixMicro()},
			"value": {1.0},
		},
		TimeUnit:   "us",
		RawPayload: []byte{0x01, 0x02, 0x03},
	}
}

// TestAsyncFlush_RecordsWALCheckpoints is the regression test for the gap that
// shipped with #948's fix: the flush task was constructed without walHashes, so
// markWALFlushed received nil on every ASYNCHRONOUS flush and no checkpoint was
// ever written for it. Recovery then replayed those entries after a crash —
// permanent duplicate rows for tagless measurements, which is the bug #948
// closed, still open for the one path that carries production ingest.
//
// MaxBufferSize=1 forces the size-triggered async path (enqueueOrDeferLocked)
// on every write, with MaxBufferAgeMS set high so nothing can reach the
// synchronous age path instead and mask the result.
func TestAsyncFlush_RecordsWALCheckpoints(t *testing.T) {
	buffer, walWriter := newCheckpointBuffer(1)
	defer buffer.Close()

	const writes = 3
	for i := 0; i < writes; i++ {
		if err := buffer.writeColumnarInternal(context.Background(), "default", checkpointTestRecord(), false); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, flushed := walWriter.counts(); flushed >= writes {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Guard the probe itself: a failed flush must not be read as a missing
	// checkpoint, because a failed flush is SUPPOSED to skip the checkpoint.
	if buffer.HasFlushFailure() {
		t.Fatal("async flush failed; this test cannot distinguish that from the bug")
	}
	if buffer.totalFlushes.Load() == 0 {
		t.Fatal("no async flush completed; this test cannot observe anything")
	}

	handedOut, flushed := walWriter.counts()
	if handedOut != writes {
		t.Fatalf("WAL handed out %d identities, want %d — the write path is not tracking", handedOut, writes)
	}
	if flushed != handedOut {
		t.Errorf("async flush checkpointed %d of %d WAL identities; every identity whose batch reached storage must be checkpointed, or recovery replays it after a crash (#948)", flushed, handedOut)
	}
}

// TestSyncFlush_RecordsWALCheckpoints covers the path that always worked, so a
// regression there is caught too. flushBufferLocked collects the identities
// itself; this pins that it keeps doing so.
func TestSyncFlush_RecordsWALCheckpoints(t *testing.T) {
	buffer, walWriter := newCheckpointBuffer(100000)
	defer buffer.Close()

	const writes = 3
	for i := 0; i < writes; i++ {
		if err := buffer.writeColumnarInternal(context.Background(), "default", checkpointTestRecord(), false); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	if err := buffer.FlushAll(context.Background()); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	handedOut, flushed := walWriter.counts()
	if flushed != handedOut {
		t.Errorf("sync flush checkpointed %d of %d WAL identities", flushed, handedOut)
	}
}
