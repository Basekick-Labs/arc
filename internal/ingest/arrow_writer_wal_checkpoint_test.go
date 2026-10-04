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
	forgotten []string
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

// ForgetTracked completes trackedWALWriter. Without it this double no longer
// satisfies the interface, the write path's type assertion falls through, and
// tracking is silently skipped — which is how the checkpoint gap in #948
// looked from the outside.
func (w *checkpointRecordingWAL) ForgetTracked(hashes []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.forgotten = append(w.forgotten, hashes...)
}

// forgottenIdentities reports what the write path abandoned.
func (w *checkpointRecordingWAL) forgottenIdentities() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.forgotten...)
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
		if err := buffer.writeColumnarInternal(context.Background(), "default", checkpointTestRecord(), false, ""); err != nil {
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
		if err := buffer.writeColumnarInternal(context.Background(), "default", checkpointTestRecord(), false, ""); err != nil {
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

// TestReplay_InheritsTrackedIdentity pins that a replayed batch carries the
// identity of the WAL entry it came from, so the flush that persists it
// checkpoints the ORIGINAL entry.
//
// Without this, a replayed batch writes nothing to the WAL (correct — the copy
// being replayed is already on disk) and therefore produced no checkpoint
// either, so every later recovery pass replayed the same entry again. Any file
// recovery keeps — one poisoned entry is enough (#590) — re-applied all of its
// healthy entries on every pass, and for a tagless measurement compaction can
// never remove those duplicates.
func TestReplay_InheritsTrackedIdentity(t *testing.T) {
	buffer, walWriter := newCheckpointBuffer(1)
	defer buffer.Close()

	// 32 hex characters: the shape the writer issues, "%016x%016x" of
	// (instance, sequence).
	const identity = "00000000000000ff000000000000002a"
	if err := buffer.WriteColumnarDirectReplay(context.Background(), "default", "replayed",
		map[string][]interface{}{
			"time":  {time.Now().UTC().UnixMicro()},
			"value": {1.0},
		}, identity); err != nil {
		t.Fatalf("replay write: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, flushed := walWriter.counts(); flushed > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if buffer.HasFlushFailure() {
		t.Fatal("flush failed; this test cannot distinguish that from a missing checkpoint")
	}

	walWriter.mu.Lock()
	got := append([]string(nil), walWriter.flushed...)
	handedOut := len(walWriter.handedOut)
	walWriter.mu.Unlock()

	// A replay must not append to the WAL: the entry is already on disk.
	if handedOut != 0 {
		t.Errorf("replay appended %d WAL entries, want 0 — the copy being replayed is already durable", handedOut)
	}
	if len(got) != 1 || got[0] != identity {
		t.Errorf("checkpointed %v, want exactly [%s] — the replayed batch must checkpoint the original entry", got, identity)
	}
}

// TestReplay_DoesNotInheritContentHash pins the other half: an UNtracked entry's
// identity is a 64-hex SHA-256 of its payload, and it must never be
// checkpointed. Content hashes collide for legitimately identical payloads — two
// tagless rows with the same values and timestamp are two real events — so
// checkpointing one would make recovery skip the other. #998 moved off content
// hashes for exactly this reason, and inheriting one here would reintroduce it
// as data loss rather than duplication.
func TestReplay_DoesNotInheritContentHash(t *testing.T) {
	buffer, walWriter := newCheckpointBuffer(1)
	defer buffer.Close()

	const contentHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if len(contentHash) != 64 {
		t.Fatalf("fixture is %d chars, want a 64-hex content hash", len(contentHash))
	}
	if err := buffer.WriteColumnarDirectReplay(context.Background(), "default", "replayed",
		map[string][]interface{}{
			"time":  {time.Now().UTC().UnixMicro()},
			"value": {1.0},
		}, contentHash); err != nil {
		t.Fatalf("replay write: %v", err)
	}

	// Give a checkpoint every chance to appear before concluding it did not.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, flushed := walWriter.counts(); flushed > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if buffer.totalFlushes.Load() == 0 {
		t.Fatal("no flush completed; this test cannot observe anything")
	}

	if _, flushed := walWriter.counts(); flushed != 0 {
		t.Errorf("checkpointed %d identities for an untracked (content-hash) entry, want 0", flushed)
	}
}

// A write rejected AFTER its WAL append must release its identity, or the WAL
// purge floor waits forever for data no buffer holds. Because PurgeFlushed
// stops at the first retained file instead of skipping it, one such identity
// stops the WAL reclaiming anything at all for the life of the process (#676).
//
// A string time column is converted after the WAL append and rejected there
// ("time column must be int64 microseconds"), which is one of several error
// returns in that window — a type mismatch, a client that disconnects during a
// schema-change flush, the schema-churn guard, a closing shard. The release is
// deferred over the whole window rather than written at each return, so a
// return added later cannot reintroduce the leak.
func TestAbandonedWrite_ReleasesItsWALIdentity(t *testing.T) {
	buffer, walWriter := newCheckpointBuffer(1)
	defer buffer.Close()

	rejected := checkpointTestRecord()
	rejected.Columns = map[string][]interface{}{
		"time":  {"not-an-int64"},
		"value": {1.0},
	}

	err := buffer.writeColumnarInternal(context.Background(), "default", rejected, false, "")
	if err == nil {
		t.Fatal("expected the write to be rejected after its WAL append")
	}

	handedOut, _ := walWriter.counts()
	if handedOut == 0 {
		t.Fatal("the WAL handed out no identity, so this test proves nothing")
	}
	forgotten := walWriter.forgottenIdentities()
	if len(forgotten) != handedOut {
		t.Fatalf("WAL handed out %d identities and %d were released: %v — an unreleased identity pins the purge floor",
			handedOut, len(forgotten), forgotten)
	}
}

// The mirror image: a write that IS accepted must keep its identity pinned, so
// the floor protects its WAL copy until a flush checkpoints it.
func TestAcceptedWrite_KeepsItsWALIdentity(t *testing.T) {
	// MaxBufferSize high so the write stays buffered rather than flushing.
	buffer, walWriter := newCheckpointBuffer(1000)
	defer buffer.Close()

	if err := buffer.writeColumnarInternal(context.Background(), "default", checkpointTestRecord(), false, ""); err != nil {
		t.Fatalf("write: %v", err)
	}

	if forgotten := walWriter.forgottenIdentities(); len(forgotten) != 0 {
		t.Fatalf("released %v for a buffered write; its WAL copy is now unprotected", forgotten)
	}
}

// A REJECTED REPLAY must not release the identity it inherited. On the replay
// path walHashes is the identity of the entry being replayed (#1048), and that
// identity belongs to a WAL file whose keep-or-delete decision is recovery's.
// Releasing it drops the purge floor below a sequence that is still unflushed,
// and the periodic flush-failure recovery replays THIS process's own files, so
// the instance prefix matches and the release lands.
//
// Sequence it would break: seq 5 is appended, its flush fails during an object
// store outage, so it stays pending and its file is correctly retained.
// Recovery replays that file; the replay write is rejected (a conversion
// failure, or the schema-churn guard); the release drops the floor past seq 5;
// recovery keeps the file; the next purge deletes it. Unflushed acknowledged
// write, gone.
func TestRejectedReplay_DoesNotReleaseTheInheritedIdentity(t *testing.T) {
	buffer, walWriter := newCheckpointBuffer(1)
	defer buffer.Close()

	const inherited = "abababababababababababababababab"
	rejected := checkpointTestRecord()
	rejected.Columns = map[string][]interface{}{
		"time":  {"not-an-int64"},
		"value": {1.0},
	}

	// skipWAL=true with an inherited identity is the replay path.
	err := buffer.writeColumnarInternal(context.Background(), "default", rejected, true, inherited)
	if err == nil {
		t.Fatal("expected the replay write to be rejected")
	}

	if forgotten := walWriter.forgottenIdentities(); len(forgotten) != 0 {
		t.Fatalf("a rejected replay released %v; that identity's file is recovery's to keep or delete", forgotten)
	}
}
