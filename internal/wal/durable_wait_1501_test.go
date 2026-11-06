package wal

// #1501: a durable append's wait ends with its context and with AckTimeout, on
// a WAL device that hangs instead of failing: in the sync, in the write that
// holds w.mu, and with the queue full. Its retraction must not hang there either.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// shortRetraction shortens retractTimeout. Call it before creating the writer,
// so the restore runs after the writer's Close.
func shortRetraction(t *testing.T) {
	t.Helper()
	prev := retractTimeout
	retractTimeout = 50 * time.Millisecond
	t.Cleanup(func() { retractTimeout = prev })
}

func newAckTimeoutWriter(t *testing.T, ackTimeout time.Duration, bufferSize int) (*Writer, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := NewWriter(&WriterConfig{
		WALDir:       dir,
		SyncMode:     SyncModeFsync,
		MaxSizeBytes: 64 << 20,
		MaxAge:       time.Hour,
		SyncInterval: time.Hour,
		SyncBytes:    1 << 30,
		BufferSize:   bufferSize,
		AckMode:      AckModeDurable,
		AckTimeout:   ackTimeout,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return w, dir
}

func appendContextAsync(ctx context.Context, w *Writer, payload []byte) chan appendResult {
	ch := make(chan appendResult, 1)
	go func() {
		h, err := w.AppendRawWithMetaTrackedContext(ctx, "iot", payload)
		ch <- appendResult{h, err}
	}()
	return ch
}

func requireRefused(t *testing.T, ch chan appendResult) {
	t.Helper()
	select {
	case r := <-ch:
		if !errors.Is(r.err, ErrNotDurable) {
			t.Fatalf("err = %v, want ErrNotDurable", r.err)
		}
		if r.hashes != nil {
			t.Fatalf("hashes = %v, want nil on a refusal", r.hashes)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the append did not return")
	}
}

// stallWrites makes the writer loop hang in its next file write, holding w.mu.
// The returned func releases it.
func stallWrites(t *testing.T) (held *atomic.Bool, release func()) {
	t.Helper()
	ch := make(chan struct{})
	held = &atomic.Bool{}
	prev := writeFileFn
	writeFileFn = func(f *os.File, b []byte) (int, error) {
		if held.CompareAndSwap(false, true) {
			<-ch
		}
		return prev(f, b)
	}
	t.Cleanup(func() { writeFileFn = prev })
	return held, func() { close(ch) }
}

// Cancelling the context refuses an append whose sync hangs, and frees its
// sequence. The retraction cannot complete while the sync hangs, so it is
// counted; its checkpoint is still queued, and once the device recovers the
// entry does not replay.
func TestDurableWaitEndsWithTheContextWhileASyncHangs(t *testing.T) {
	shortRetraction(t)
	rec := installSyncRecorder(t, 1)
	w, dir := newAckTimeoutWriter(t, 0, 16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	res := appendContextAsync(ctx, w, durablePayload(t))
	select {
	case <-rec.started:
	case <-time.After(5 * time.Second):
		t.Fatal("no sync started")
	}
	select {
	case r := <-res:
		t.Fatalf("append returned with its context live: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	requireRefused(t, res)
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("PendingUnflushedCount = %d, want 0: a refused append must not pin the floor", got)
	}
	if got := atomic.LoadInt64(&w.RetractionsFailed); got != 1 {
		t.Fatalf("RetractionsFailed = %d, want 1", got)
	}

	close(rec.release)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if n := replayedEntries(t, dir); n != 0 {
		t.Fatalf("replayed %d entries: the retraction written after the device recovered must cover the refused one", n)
	}
}

// AckTimeout bounds the wait with no context to do it: an HTTP write's context
// (a fasthttp.RequestCtx) has no deadline and ends only at server shutdown.
func TestDurableWaitEndsAtAckTimeout(t *testing.T) {
	shortRetraction(t)
	rec := installSyncRecorder(t, 1)
	w, _ := newAckTimeoutWriter(t, 20*time.Millisecond, 16)
	defer func() {
		close(rec.release)
		_ = w.Close()
	}()

	requireRefused(t, appendAsync(w, durablePayload(t)))
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("PendingUnflushedCount = %d, want 0", got)
	}
}

// A write that hangs holds w.mu. Neither the refusal nor its retraction may wait
// for that lock.
func TestDurableWaitAndRetractionEndWhileAWriteHangs(t *testing.T) {
	shortRetraction(t)
	w, _ := newAckTimeoutWriter(t, 20*time.Millisecond, 16)
	held, release := stallWrites(t)
	defer func() {
		release()
		_ = w.Close()
	}()

	res := appendAsync(w, durablePayload(t))
	waitFor(t, "the write to hang", held.Load)
	requireRefused(t, res)
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("PendingUnflushedCount = %d, want 0", got)
	}
}

// With the queue full an append waits for admission; that wait ends with its
// context too, and what never reached the queue pins nothing.
func TestDurableAdmissionEndsWithTheContext(t *testing.T) {
	shortRetraction(t)
	w, _ := newAckTimeoutWriter(t, 0, 1)
	held, release := stallWrites(t)
	payload := durablePayload(t)

	first := appendAsync(w, payload)
	waitFor(t, "the write to hang", held.Load)
	second := appendAsync(w, payload)
	waitFor(t, "the queue to fill", func() bool { return len(w.entryChan) == 1 })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	third := appendContextAsync(ctx, w, payload)
	select {
	case r := <-third:
		t.Fatalf("append returned with no room in the queue: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	requireRefused(t, third)
	if got := w.PendingUnflushedCount(); got != 2 {
		t.Fatalf("PendingUnflushedCount = %d, want the 2 entries still waiting", got)
	}

	release()
	for i, ch := range []chan appendResult{first, second} {
		select {
		case r := <-ch:
			if r.err != nil {
				t.Fatalf("append %d: %v", i, r.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("append %d never returned", i)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// A context already done refuses the append without writing anything.
func TestDurableAppendWithADoneContextWritesNothing(t *testing.T) {
	w, dir := newAckTimeoutWriter(t, 0, 16)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	requireRefused(t, appendContextAsync(ctx, w, durablePayload(t)))
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("PendingUnflushedCount = %d, want 0", got)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt64(&w.TotalEntries); got != 0 {
		t.Fatalf("TotalEntries = %d, want 0", got)
	}
	if n := replayedEntries(t, dir); n != 0 {
		t.Fatalf("replayed %d entries", n)
	}
}

// An answer already there wins over a context that ended: the entry is durable,
// and refusing the write would make the client send it again.
func TestAwaitDonePrefersAnAnswerOverTheContext(t *testing.T) {
	w, _ := newAckTimeoutWriter(t, 0, 16)
	defer w.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 200 { // select picks among ready cases at random
		d := make(chan error, 1)
		d <- nil
		if err := w.awaitDone(ctx, d); err != nil {
			t.Fatalf("awaitDone = %v, want the answer", err)
		}
	}
	if err := w.awaitDone(ctx, make(chan error, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("awaitDone with no answer = %v, want context.Canceled", err)
	}
}

// Queued acknowledgement has no wait to bound: neither a done context nor
// AckTimeout refuses the append.
func TestQueuedAppendIgnoresTheContext(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := NewWriter(&WriterConfig{WALDir: dir, SyncMode: SyncModeFsync, AckMode: AckModeQueued, AckTimeout: time.Nanosecond, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	hashes, err := w.AppendRawWithMetaTrackedContext(ctx, "iot", durablePayload(t))
	if err != nil || len(hashes) != 1 {
		t.Fatalf("append = (%v, %v), want one token and nil", hashes, err)
	}
}
