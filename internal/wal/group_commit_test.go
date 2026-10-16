package wal

// Group commit (#1246): the writer loop never waits for a sync. These hold a
// sync in flight through syncFileFn and watch what the writer does meanwhile.

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// syncRecorder replaces syncFileFn. The first `hold` calls block until
// release is closed; every call records the file's size when it started.
type syncRecorder struct {
	mu         sync.Mutex
	calls      int
	fullCalls  int
	startSizes []int64
	hold       int
	started    chan struct{}
	release    chan struct{}
	errOnce    error // returned by the first held call, after release
}

func installSyncRecorder(t *testing.T, hold int) *syncRecorder {
	t.Helper()
	r := &syncRecorder{hold: hold, started: make(chan struct{}, 64), release: make(chan struct{})}
	prev := syncFileFn
	syncFileFn = func(f *os.File, full bool) error {
		r.mu.Lock()
		r.calls++
		if full {
			r.fullCalls++
		}
		n := r.calls
		if st, err := f.Stat(); err == nil {
			r.startSizes = append(r.startSizes, st.Size())
		}
		held := n <= r.hold
		r.mu.Unlock()
		if held {
			r.started <- struct{}{}
			<-r.release
			if n == 1 && r.errOnce != nil {
				return r.errOnce
			}
		}
		return nil
	}
	t.Cleanup(func() { syncFileFn = prev })
	return r
}

func (r *syncRecorder) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func groupCommitWriter(t *testing.T, mode SyncMode, maxSize int64) *Writer {
	t.Helper()
	w, err := NewWriter(&WriterConfig{
		WALDir:       t.TempDir(),
		SyncMode:     mode,
		MaxSizeBytes: maxSize,
		MaxAge:       time.Hour,
		SyncInterval: time.Hour, // only the byte threshold and durable entries request syncs
		SyncBytes:    4096,
		BufferSize:   10000,
		Logger:       zerolog.Nop(),
	})
	require.NoError(t, err)
	// Close before the sync hook is restored (cleanups run last-in first-out),
	// so no syncer of this writer can read syncFileFn while it is reassigned.
	t.Cleanup(func() {
		w.mu.Lock()
		closed := w.closed
		w.mu.Unlock()
		if !closed {
			_ = w.Close()
		}
	})
	return w
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func waitStarted(t *testing.T, r *syncRecorder) {
	t.Helper()
	select {
	case <-r.started:
	case <-time.After(5 * time.Second):
		t.Fatal("no sync started")
	}
}

// The mechanism #1246 is about: on main the writer loop ran fdatasync inline,
// so while one sync was in flight nothing else reached the file, and WAL
// throughput was bounded by sync latency. Now the writer keeps writing.
func TestWriterKeepsWritingWhileASyncIsInFlight(t *testing.T) {
	r := installSyncRecorder(t, 1)
	w := groupCommitWriter(t, SyncModeFdatasync, 100*1024*1024)
	payload := make([]byte, 1024)

	for i := 0; i < 8; i++ { // past the 4 KiB threshold: a sync starts and is held
		require.NoError(t, w.AppendRaw(payload))
	}
	waitStarted(t, r)
	before := atomic.LoadInt64(&w.TotalEntries)
	for i := 0; i < 500; i++ {
		require.NoError(t, w.AppendRaw(payload))
	}
	waitFor(t, "the writer to keep writing during a sync", func() bool {
		return atomic.LoadInt64(&w.TotalEntries) >= before+500
	})
	close(r.release)
	require.NoError(t, w.Close())
}

// A durable entry (a flush checkpoint) is answered only by a sync that started
// after it was written, never at write time.
func TestADurableEntryIsAnsweredOnlyByASyncThatCoversIt(t *testing.T) {
	r := installSyncRecorder(t, 1)
	w := groupCommitWriter(t, SyncModeFdatasync, 100*1024*1024)
	tokens, err := w.AppendTracked([]map[string]interface{}{{"v": 1}})
	require.NoError(t, err)

	marked := make(chan error, 1)
	go func() { marked <- w.MarkFlushed(tokens) }()
	waitStarted(t, r)
	select {
	case err := <-marked:
		t.Fatalf("MarkFlushed returned (%v) while the sync covering its checkpoint was still in flight", err)
	case <-time.After(50 * time.Millisecond):
	}
	st, err := os.Stat(w.CurrentFile())
	require.NoError(t, err)
	close(r.release)
	select {
	case err := <-marked:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("MarkFlushed never returned after its sync completed")
	}
	r.mu.Lock()
	first := r.startSizes[0]
	r.mu.Unlock()
	require.Equal(t, st.Size(), first, "the sync that answered the checkpoint started before the checkpoint was in the file")
	require.NoError(t, w.Close())
}

// A failed sync is reported to the durable entries it covered, counted, and
// the writer rotates away from the file; later checkpoints succeed.
func TestAFailedSyncFailsItsCheckpointAndRotates(t *testing.T) {
	r := installSyncRecorder(t, 1)
	r.errOnce = errors.New("simulated writeback error")
	w := groupCommitWriter(t, SyncModeFdatasync, 100*1024*1024)
	oldFile := w.CurrentFile()
	tokens, err := w.AppendTracked([]map[string]interface{}{{"v": 1}})
	require.NoError(t, err)

	marked := make(chan error, 1)
	go func() { marked <- w.MarkFlushed(tokens) }()
	waitStarted(t, r)
	close(r.release)
	require.Error(t, <-marked)
	require.EqualValues(t, 1, atomic.LoadInt64(&w.FailedSyncs))

	// The next entry through the writer loop triggers the rotation.
	more, err := w.AppendTracked([]map[string]interface{}{{"v": 2}})
	require.NoError(t, err)
	waitFor(t, "a rotation after the failed sync", func() bool { return w.CurrentFile() != oldFile })
	require.NoError(t, w.MarkFlushed(more))
	require.NoError(t, w.Close())
}

// In async mode data appends never sync, but a durable checkpoint still does:
// it is never answered without one.
func TestAsyncModeStillSyncsDurableCheckpoints(t *testing.T) {
	r := installSyncRecorder(t, 0)
	w := groupCommitWriter(t, SyncModeAsync, 100*1024*1024)
	payload := make([]byte, 1024)
	for i := 0; i < 64; i++ {
		require.NoError(t, w.AppendRaw(payload))
	}
	waitFor(t, "data entries written", func() bool { return atomic.LoadInt64(&w.TotalEntries) >= 64 })
	require.Zero(t, r.callCount(), "async mode synced data entries")

	tokens, err := w.AppendTracked([]map[string]interface{}{{"v": 1}})
	require.NoError(t, err)
	require.NoError(t, w.MarkFlushed(tokens))
	require.GreaterOrEqual(t, r.callCount(), 1, "a durable checkpoint was answered without a sync")
	r.mu.Lock()
	require.Zero(t, r.fullCalls)
	r.mu.Unlock()
	require.NoError(t, w.Close())
}

// A rotation waits for a sync already in flight on the old file (syncs run one
// at a time, so a failure is recorded before another sync can mask it), then
// answers the checkpoints written before it.
func TestRotationWaitsForAnInFlightSyncAndAnswersItsCheckpoint(t *testing.T) {
	r := installSyncRecorder(t, 1)
	w := groupCommitWriter(t, SyncModeFdatasync, 100*1024*1024)
	oldFile := w.CurrentFile()
	tokens, err := w.AppendTracked([]map[string]interface{}{{"v": 1}})
	require.NoError(t, err)

	marked := make(chan error, 1)
	go func() { marked <- w.MarkFlushed(tokens) }()
	waitStarted(t, r)
	rotated := make(chan error, 1)
	go func() { rotated <- w.Rotate() }()
	select {
	case err := <-rotated:
		t.Fatalf("the rotation finished (%v) while a sync of the old file was still in flight", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(r.release)
	require.NoError(t, <-rotated)
	require.NoError(t, <-marked)
	require.NotEqual(t, oldFile, w.CurrentFile())
	require.Zero(t, atomic.LoadInt64(&w.FailedSyncs))
	require.NoError(t, w.Close())
}

// Close answers a checkpoint whose sync is in flight, and returns.
func TestCloseAnswersAPendingCheckpoint(t *testing.T) {
	r := installSyncRecorder(t, 1)
	w := groupCommitWriter(t, SyncModeFdatasync, 100*1024*1024)
	tokens, err := w.AppendTracked([]map[string]interface{}{{"v": 1}})
	require.NoError(t, err)
	marked := make(chan error, 1)
	go func() { marked <- w.MarkFlushed(tokens) }()
	waitStarted(t, r)

	closed := make(chan error, 1)
	go func() { closed <- w.Close() }()
	close(r.release)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
	select {
	case err := <-marked:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("a checkpoint pending at Close was never answered")
	}
}

// The real sync path under contention: many rotations racing syncs on the
// syncer, with appends and checkpoints from several goroutines. Under -race on
// Linux this exercises dataSync's reference-counted descriptor (#1246).
func TestGroupCommitUnderRotationAndCheckpointLoad(t *testing.T) {
	w := groupCommitWriter(t, SyncModeFdatasync, 64*1024)
	payload := make([]byte, 2048)
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = w.AppendRaw(payload)
				tokens, err := w.AppendTracked([]map[string]interface{}{{"g": i}})
				if err != nil {
					continue // a full queue drops; not what this test is about
				}
				if err := w.MarkFlushed(tokens); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("checkpoint failed under rotation load: %v", err)
	}
	require.Greater(t, atomic.LoadInt64(&w.TotalRotations), int64(2))
	require.Zero(t, atomic.LoadInt64(&w.FailedSyncs))
	require.NoError(t, w.Close())
}

// dataSync on a closed file reports an error rather than syncing whatever
// descriptor now carries the old number.
func TestDataSyncOnAClosedFileFails(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "closed-*.wal")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Error(t, dataSync(f))
}

// wal.sync_bytes: a negative value is refused rather than turned into a
// threshold that never triggers.
func TestNewWriterRefusesNegativeSyncBytes(t *testing.T) {
	_, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), SyncBytes: -1, Logger: zerolog.Nop()})
	require.Error(t, err)
}

// A rotation in async mode still syncs before it answers a checkpoint that is
// waiting: async skips data syncs, never a checkpoint's. Driven on a stopped
// writer loop, so the rotation and not the syncer is what answers it.
func TestAsyncModeRotationSyncsAWaitingCheckpoint(t *testing.T) {
	r := installSyncRecorder(t, 0)
	w := groupCommitWriter(t, SyncModeAsync, 100*1024*1024)
	close(w.done)
	w.wg.Wait() // writer loop and its syncer stopped
	done := make(chan error, 1)
	deferred, err := w.writeEntry(walEntry{data: make([]byte, 64), durable: true, done: done})
	require.NoError(t, err)
	require.True(t, deferred)
	before := r.callCount()
	w.mu.Lock()
	err = w.rotate()
	w.mu.Unlock()
	require.NoError(t, err)
	require.Greater(t, r.callCount(), before, "the rotation answered an async-mode checkpoint without a sync")
	require.NoError(t, <-done)
	w.done = make(chan struct{})
	w.wg.Add(1)
	go w.writerLoop()
}

// Bytes below the threshold are still synced within the sync interval.
func TestUnsyncedBytesAreSyncedWithinTheInterval(t *testing.T) {
	r := installSyncRecorder(t, 0)
	w, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), SyncMode: SyncModeFdatasync, MaxSizeBytes: 100 << 20,
		MaxAge: time.Hour, SyncInterval: 20 * time.Millisecond, SyncBytes: 1 << 30, BufferSize: 100, Logger: zerolog.Nop()})
	require.NoError(t, err)
	require.NoError(t, w.AppendRaw(make([]byte, 100)))
	waitFor(t, "an interval sync", func() bool { return r.callCount() >= 1 })
	require.NoError(t, w.Close())
}

// A sync answers only the checkpoints written before it started: one written
// while it runs waits for the next sync.
func TestASyncDoesNotAnswerACheckpointWrittenAfterItStarted(t *testing.T) {
	rel := []chan struct{}{make(chan struct{}), make(chan struct{})}
	started := make(chan int, 4)
	var calls atomic.Int32
	prev := syncFileFn
	syncFileFn = func(f *os.File, full bool) error {
		n := int(calls.Add(1))
		if n <= len(rel) {
			started <- n
			<-rel[n-1]
		}
		return nil
	}
	t.Cleanup(func() { syncFileFn = prev })
	w := groupCommitWriter(t, SyncModeFdatasync, 100*1024*1024)

	first, err := w.AppendTracked([]map[string]interface{}{{"v": 1}})
	require.NoError(t, err)
	mark1 := make(chan error, 1)
	go func() { mark1 <- w.MarkFlushed(first) }()
	require.Equal(t, 1, <-started)

	second, err := w.AppendTracked([]map[string]interface{}{{"v": 2}})
	require.NoError(t, err)
	entries := atomic.LoadInt64(&w.TotalEntries)
	mark2 := make(chan error, 1)
	go func() { mark2 <- w.MarkFlushed(second) }()
	waitFor(t, "the second checkpoint written", func() bool { return atomic.LoadInt64(&w.TotalEntries) >= entries+2 })

	close(rel[0])
	require.NoError(t, <-mark1)
	select {
	case err := <-mark2:
		t.Fatalf("a checkpoint written after a sync started was answered by it (%v)", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, 2, <-started)
	close(rel[1])
	require.NoError(t, <-mark2)
	require.NoError(t, w.Close())
}

// Linux reports a lost write-back once: after a failed sync, a later sync of
// the same file can succeed over pages that were never written. A checkpoint
// written to that file must get the error too, not a success from that later
// sync.
func TestAFailedSyncFailsEveryLaterCheckpointInTheSameFile(t *testing.T) {
	r := installSyncRecorder(t, 1)
	r.errOnce = errors.New("simulated writeback error")
	w := groupCommitWriter(t, SyncModeFdatasync, 100*1024*1024)
	first, err := w.AppendTracked([]map[string]interface{}{{"v": 1}})
	require.NoError(t, err)
	mark1 := make(chan error, 1)
	go func() { mark1 <- w.MarkFlushed(first) }()
	waitStarted(t, r)

	second, err := w.AppendTracked([]map[string]interface{}{{"v": 2}})
	require.NoError(t, err)
	entries := atomic.LoadInt64(&w.TotalEntries)
	mark2 := make(chan error, 1)
	go func() { mark2 <- w.MarkFlushed(second) }()
	waitFor(t, "the second checkpoint written to the same file", func() bool {
		return atomic.LoadInt64(&w.TotalEntries) >= entries+2
	})
	close(r.release) // the held sync fails; the next one succeeds
	require.Error(t, <-mark1)
	require.Error(t, <-mark2, "a checkpoint in a file whose write-back failed was answered as durable")

	third, err := w.AppendTracked([]map[string]interface{}{{"v": 3}})
	require.NoError(t, err)
	require.NoError(t, w.MarkFlushed(third), "the writer rotated away, so a new file's checkpoints succeed")
}

// A rotation that keeps failing after a failed sync (a full disk) is retried
// about once a second, not on every entry.
func TestAFailingRotationAfterASyncFailureBacksOff(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	var logs syncLogBuffer
	dir := t.TempDir()
	w, err := NewWriter(&WriterConfig{WALDir: dir, SyncMode: SyncModeFdatasync, MaxSizeBytes: 100 << 20,
		MaxAge: time.Hour, SyncInterval: time.Hour, SyncBytes: 1 << 30, BufferSize: 1000, Logger: zerolog.New(&logs)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700); _ = w.Close() })
	require.NoError(t, os.Chmod(dir, 0o500)) // a new WAL file cannot be created
	w.syncFailed.Store(true)
	for i := 0; i < 200; i++ {
		require.NoError(t, w.AppendRaw(make([]byte, 64)))
	}
	waitFor(t, "the entries written", func() bool { return atomic.LoadInt64(&w.TotalEntries) >= 200 })
	if n := logs.count("Failed to rotate WAL after a sync failure"); n > 2 {
		t.Fatalf("rotation retried %d times within a burst of 200 entries", n)
	}
}

type syncLogBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *syncLogBuffer) count(s string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for i := 0; i+len(s) <= len(b.buf); i++ {
		if string(b.buf[i:i+len(s)]) == s {
			n++
		}
	}
	return n
}
