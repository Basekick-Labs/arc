package wal

import (
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/rs/zerolog"
)

func newDurableWriter(t *testing.T, syncMode SyncMode, bufferSize int) *Writer {
	t.Helper()
	dir := t.TempDir()
	w, err := NewWriter(&WriterConfig{
		WALDir:       dir,
		SyncMode:     syncMode,
		MaxSizeBytes: 64 << 20,
		MaxAge:       time.Hour,
		SyncInterval: time.Hour, // only durable waiters trigger syncs in these tests
		SyncBytes:    1 << 30,
		BufferSize:   bufferSize,
		AckMode:      AckModeDurable,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return w
}

func durablePayload(t *testing.T) []byte {
	t.Helper()
	b, err := msgpack.Marshal([]map[string]interface{}{{"value": int64(1)}})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return b
}

type appendResult struct {
	hashes []string
	err    error
}

func appendAsync(w *Writer, payload []byte) chan appendResult {
	ch := make(chan appendResult, 1)
	go func() {
		h, err := w.AppendRawWithMetaTracked("iot", payload)
		ch <- appendResult{h, err}
	}()
	return ch
}

// In durable mode the append returns only after a sync covering the entry has
// completed, and async sync mode still syncs because a waiter forces it.
func TestDurableAckWaitsForCoveringSync(t *testing.T) {
	for _, mode := range []SyncMode{SyncModeAsync, SyncModeFsync} {
		t.Run(string(mode), func(t *testing.T) {
			rec := installSyncRecorder(t, 1)
			w := newDurableWriter(t, mode, 16)
			defer w.Close()

			res := appendAsync(w, durablePayload(t))
			select {
			case <-rec.started:
			case <-time.After(5 * time.Second):
				t.Fatal("no sync started for a durable entry")
			}
			select {
			case r := <-res:
				t.Fatalf("append returned before its sync completed: %+v", r)
			case <-time.After(100 * time.Millisecond):
			}
			close(rec.release)
			select {
			case r := <-res:
				if r.err != nil || len(r.hashes) != 1 {
					t.Fatalf("append = (%v, %v), want one token and nil", r.hashes, r.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("append did not return after the sync completed")
			}
			if got := w.PendingUnflushedCount(); got != 1 {
				t.Fatalf("PendingUnflushedCount = %d, want 1 (the durable entry awaits its flush checkpoint)", got)
			}
		})
	}
}

// A failed sync answers the append with ErrNotDurable and releases its pending
// sequence, so it cannot pin the purge floor (#676).
func TestDurableAckSyncFailureReleasesPending(t *testing.T) {
	rec := installSyncRecorder(t, 1)
	rec.errOnce = errors.New("injected sync failure")
	w := newDurableWriter(t, SyncModeFsync, 16)
	defer w.Close()

	res := appendAsync(w, durablePayload(t))
	<-rec.started
	close(rec.release)
	r := <-res
	if !errors.Is(r.err, ErrNotDurable) {
		t.Fatalf("err = %v, want ErrNotDurable", r.err)
	}
	if r.hashes != nil {
		t.Fatalf("hashes = %v, want nil on failure", r.hashes)
	}
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("PendingUnflushedCount = %d, want 0: a failed durable append must not pin the floor", got)
	}
}

// A full queue applies backpressure in durable mode instead of dropping.
func TestDurableAckBlocksInsteadOfDropping(t *testing.T) {
	// NewWriter writes the file header through writeFileFn, so the stall is
	// installed only after it.
	w := newDurableWriter(t, SyncModeAsync, 1)
	defer w.Close()
	release := make(chan struct{})
	var held atomic.Bool
	prevWrite := writeFileFn
	writeFileFn = func(f *os.File, b []byte) (int, error) {
		if held.CompareAndSwap(false, true) {
			<-release // stall the writer loop on its first data write
		}
		return prevWrite(f, b)
	}
	t.Cleanup(func() { writeFileFn = prevWrite })
	payload := durablePayload(t)

	first := appendAsync(w, payload)
	for !held.Load() {
		time.Sleep(time.Millisecond)
	}
	second := appendAsync(w, payload) // fills the 1-slot queue
	third := appendAsync(w, payload)  // must block, not drop
	time.Sleep(100 * time.Millisecond)
	if d := atomic.LoadInt64(&w.DroppedEntries); d != 0 {
		t.Fatalf("DroppedEntries = %d, want 0 in durable mode", d)
	}
	close(release)
	for i, ch := range []chan appendResult{first, second, third} {
		select {
		case r := <-ch:
			if r.err != nil || len(r.hashes) != 1 {
				t.Fatalf("append %d = (%v, %v)", i, r.hashes, r.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("append %d never returned", i)
		}
	}
}

// After Close, a durable append is refused with ErrNotDurable rather than
// hanging, and releases nothing it does not own.
func TestDurableAckAfterClose(t *testing.T) {
	w := newDurableWriter(t, SyncModeAsync, 16)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := w.AppendRawWithMetaTracked("iot", durablePayload(t))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNotDurable) {
			t.Fatalf("err = %v, want ErrNotDurable", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("durable append after Close hung")
	}
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("PendingUnflushedCount = %d, want 0", got)
	}
}

// Appends racing Close either complete durably or fail with ErrNotDurable;
// none hangs (an entry admitted after the drain is answered by exited).
func TestDurableAckRacingCloseNeverHangs(t *testing.T) {
	for i := 0; i < 20; i++ {
		w := newDurableWriter(t, SyncModeAsync, 4)
		payload := durablePayload(t)
		results := make([]chan appendResult, 8)
		for j := range results {
			results[j] = appendAsync(w, payload)
		}
		_ = w.Close()
		for j, ch := range results {
			select {
			case r := <-ch:
				if r.err != nil && !errors.Is(r.err, ErrNotDurable) {
					t.Fatalf("iteration %d append %d: unexpected error %v", i, j, r.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("iteration %d append %d hung across Close", i, j)
			}
		}
	}
}

func TestNewWriterRejectsUnknownAckMode(t *testing.T) {
	_, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), AckMode: "sometimes", Logger: zerolog.Nop()})
	if err == nil {
		t.Fatal("NewWriter accepted an unknown ack mode")
	}
}

// A flush checkpoint competing with durable writers parked on a full queue is
// admitted, not dropped: under saturation the runtime hands every freed slot to
// a parked sender, so a non-blocking admission would see a full queue.
func TestDurableAckCheckpointNotDroppedUnderSaturation(t *testing.T) {
	w := newDurableWriter(t, SyncModeAsync, 1)
	defer w.Close()
	payload := durablePayload(t)
	tokens, err := w.AppendRawWithMetaTracked("iot", payload)
	if err != nil {
		t.Fatalf("seed append: %v", err)
	}

	release := make(chan struct{})
	var held atomic.Bool
	prevWrite := writeFileFn
	writeFileFn = func(f *os.File, b []byte) (int, error) {
		if held.CompareAndSwap(false, true) {
			<-release
		}
		return prevWrite(f, b)
	}
	t.Cleanup(func() { writeFileFn = prevWrite })

	parked := make([]chan appendResult, 4)
	for i := range parked {
		parked[i] = appendAsync(w, payload)
	}
	for !held.Load() {
		time.Sleep(time.Millisecond)
	}
	ckpt := make(chan error, 1)
	go func() { ckpt <- w.MarkFlushed(tokens) }()
	time.Sleep(100 * time.Millisecond)
	close(release)
	select {
	case err := <-ckpt:
		if err != nil {
			t.Fatalf("MarkFlushed under durable saturation = %v, want nil (not dropped)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("MarkFlushed never returned")
	}
	for i, ch := range parked {
		if r := <-ch; r.err != nil {
			t.Fatalf("parked append %d: %v", i, r.err)
		}
	}
}

// In durable mode every tracked-append failure is ErrNotDurable, so no error
// path can leave a write acknowledged without durability.
func TestDurableAckWrapsEveryAppendError(t *testing.T) {
	w := newDurableWriter(t, SyncModeAsync, 16)
	defer w.Close()
	long := make([]byte, 300)
	for i := range long {
		long[i] = 'd'
	}
	_, err := w.AppendRawWithMetaTracked(string(long), durablePayload(t))
	if !errors.Is(err, ErrNotDurable) {
		t.Fatalf("err = %v, want ErrNotDurable for a database name over the limit", err)
	}
	if !w.DurableAck() {
		t.Fatal("DurableAck() = false on a durable writer")
	}
}
