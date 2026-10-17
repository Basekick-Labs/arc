package main

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #676 part A: the WAL maintenance goroutine purges on the writer's hints, at
// most once per walPromptPurgeInterval, with a trailing pass.

// servePromptPurge drives l from hint the way the maintenance goroutine in
// main.go does: its two select arms, on one goroutine.
func servePromptPurge(t *testing.T, hint <-chan struct{}, l *walPurgeLimiter) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer l.stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-hint:
				l.hint()
			case <-l.trailing():
				l.fire()
			}
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
}

type purgeRecorder struct {
	mu    sync.Mutex
	times []time.Time
}

func (r *purgeRecorder) purge() {
	r.mu.Lock()
	r.times = append(r.times, time.Now())
	r.mu.Unlock()
}

func (r *purgeRecorder) snapshot() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.times...)
}

func sendHint(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

// A hint purges at once; a hint inside the interval is not lost but purges
// once when the interval ends, with no further hint.
func TestPromptPurgeLeadingAndTrailingEdge(t *testing.T) {
	const interval = 300 * time.Millisecond
	rec := &purgeRecorder{}
	hint := make(chan struct{}, 1)
	servePromptPurge(t, hint, newWALPurgeLimiter(interval, rec.purge))

	start := time.Now()
	sendHint(hint)
	require.Eventually(t, func() bool { return len(rec.snapshot()) == 1 }, time.Second, 5*time.Millisecond)
	require.Less(t, rec.snapshot()[0].Sub(start), interval/2, "the first hint purges at once")

	sendHint(hint) // inside the interval
	time.Sleep(interval / 3)
	require.Len(t, rec.snapshot(), 1, "a hint inside the interval waits for it to end")
	require.Eventually(t, func() bool { return len(rec.snapshot()) == 2 }, 2*interval, 5*time.Millisecond,
		"the hint inside the interval is not lost: one trailing purge follows")
	got := rec.snapshot()
	require.GreaterOrEqual(t, got[1].Sub(got[0]), interval)

	time.Sleep(2 * interval)
	require.Len(t, rec.snapshot(), 2, "no purge without a hint")
}

// A stream of hints purges at most once per interval.
func TestPromptPurgeRateLimit(t *testing.T) {
	const interval = 200 * time.Millisecond
	rec := &purgeRecorder{}
	hint := make(chan struct{}, 1)
	servePromptPurge(t, hint, newWALPurgeLimiter(interval, rec.purge))

	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		sendHint(hint)
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(2 * interval) // the trailing purge
	got := rec.snapshot()
	require.GreaterOrEqual(t, len(got), 4)
	require.LessOrEqual(t, len(got), 7)
	for i := 1; i < len(got); i++ {
		require.GreaterOrEqual(t, got[i].Sub(got[i-1]), interval, "purges %d and %d are closer than the interval", i-1, i)
	}
}

// promptPurgeRig is a real writer whose hints drive the production purge
// pass through the limiter, at the production interval.
type promptPurgeRig struct {
	w      *wal.Writer
	dir    string
	passes atomic.Int64
}

func newPromptPurgeRig(t *testing.T, dir string) *promptPurgeRig {
	t.Helper()
	w, err := wal.NewWriter(&wal.WriterConfig{
		WALDir: dir, SyncMode: wal.SyncModeFsync, MaxSizeBytes: 1 << 40, MaxAge: time.Hour,
		BufferSize: 1024, Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	rig := &promptPurgeRig{w: w, dir: dir}
	pass := &walPurgePass{writer: w, logger: zerolog.Nop()}
	servePromptPurge(t, w.PurgeHint(), newWALPurgeLimiter(walPromptPurgeInterval, func() {
		pass.prompt()
		rig.passes.Add(1)
	}))
	return rig
}

func (r *promptPurgeRig) append(t *testing.T) []string {
	t.Helper()
	ids, err := r.w.AppendTracked([]map[string]interface{}{{"m": "t", "v": 1}})
	require.NoError(t, err)
	return ids
}

// purgeRunsAfter waits for a purge pass that starts after this call, which a
// rotation hints.
func (r *promptPurgeRig) purgeRunsAfter(t *testing.T) {
	t.Helper()
	n := r.passes.Load()
	require.NoError(t, r.w.Rotate())
	require.Eventually(t, func() bool { return r.passes.Load() > n }, 3*time.Second, 10*time.Millisecond)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// The production cadence: a rotated file is gone about a second after the
// checkpoint that flushes it, with no periodic pass.
func TestPromptPurgeDeletesAFlushedFileWithinASecond(t *testing.T) {
	r := newPromptPurgeRig(t, t.TempDir())
	ids := r.append(t)
	old := r.w.CurrentFile()
	require.NoError(t, r.w.Rotate()) // purges now: old is not flushed yet
	require.Eventually(t, func() bool { return r.passes.Load() >= 1 }, time.Second, 5*time.Millisecond)
	require.True(t, exists(old))

	flushed := time.Now()
	require.NoError(t, r.w.MarkFlushed(ids)) // inside the interval: the trailing pass purges
	require.Eventually(t, func() bool { return !exists(old) }, 3*time.Second, 10*time.Millisecond)
	require.Less(t, time.Since(flushed), walPromptPurgeInterval+time.Second)
}

// heldFlag is a registration hold that holds everything while set.
type heldFlag struct{ on atomic.Bool }

func (h *heldFlag) HoldsAny(uint64, string, uint64, uint64) bool { return h.on.Load() }

// Files the purge must keep stay through prompt purges, and go once the
// reason is gone.
func TestPromptPurgeKeepsPinnedFiles(t *testing.T) {
	t.Run("unflushed", func(t *testing.T) {
		r := newPromptPurgeRig(t, t.TempDir())
		ids := r.append(t)
		old := r.w.CurrentFile()
		r.purgeRunsAfter(t)
		r.purgeRunsAfter(t)
		require.True(t, exists(old), "a file holding an unflushed entry was purged")

		require.NoError(t, r.w.MarkFlushed(ids))
		require.Eventually(t, func() bool { return !exists(old) }, 3*time.Second, 10*time.Millisecond)
	})

	t.Run("held for registration", func(t *testing.T) {
		r := newPromptPurgeRig(t, t.TempDir())
		hold := &heldFlag{}
		hold.on.Store(true)
		r.w.SetCoverageHold(func() wal.CoverageHold { return hold })
		p, err := r.w.PrepareCovered("db", "k", []byte{0x90})
		require.NoError(t, err)
		ids, err := p.Commit(1)
		require.NoError(t, err)
		old := r.w.CurrentFile()
		require.NoError(t, r.w.MarkFlushed(ids)) // the floor is released; the hold is not
		r.purgeRunsAfter(t)
		r.purgeRunsAfter(t)
		require.True(t, exists(old), "a file whose batch waits to register was purged")

		hold.on.Store(false)
		require.NoError(t, r.w.Rotate())
		require.Eventually(t, func() bool { return !exists(old) }, 3*time.Second, 10*time.Millisecond)
	})

	t.Run("flush proof for an earlier process file (#1222)", func(t *testing.T) {
		dir := t.TempDir()
		earlier := filepath.Join(dir, "arc-19700101_000000.000000000.wal")
		require.NoError(t, os.WriteFile(earlier, []byte("ARCW"), 0600))
		r := newPromptPurgeRig(t, dir)
		first := r.w.CurrentFile()
		require.NoError(t, r.w.MarkFlushed([]string{"ffffffffffffffff0000000000000007"}))
		r.purgeRunsAfter(t)
		r.purgeRunsAfter(t)
		require.True(t, exists(first), "flush proof for an earlier process file was purged while that file remains")

		require.NoError(t, os.Remove(earlier))
		require.NoError(t, r.w.Rotate())
		require.Eventually(t, func() bool { return !exists(first) }, 3*time.Second, 10*time.Millisecond)
	})
}

// The prompt pass also deletes the files recovery retained for the
// registration hold once nothing in them is held, without the periodic pass.
func TestPromptPurgeReleasesRetainedFiles(t *testing.T) {
	dir := t.TempDir()
	earlier := filepath.Join(dir, "arc-19700101_000000.000000000.wal")
	require.NoError(t, os.WriteFile(earlier, []byte("ARCW"), 0600))
	r := newPromptPurgeRig(t, dir)
	r.w.AdoptRetained([]wal.RetainedFile{{Path: earlier}})
	require.NoError(t, r.w.Rotate())
	require.Eventually(t, func() bool { return !exists(earlier) }, 3*time.Second, 10*time.Millisecond)
}
