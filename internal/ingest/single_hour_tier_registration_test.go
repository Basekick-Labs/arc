package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// A single-hour flush whose PUT outlasts the file's flush timeout, but
// completes (the backend ignores its context, as LocalBackend.Write does): the
// file is stored, so its tiering registration must run under a live context,
// as the multi-hour path's does (#1698/#1700). Under the write's spent
// context, RecordFile fails, only a warning is logged, and on a measurement
// whose tier rows are all cold the new file is left out of reads until the
// next tier scan.
func TestSingleHourRegistrationOutlivesASlowWrite(t *testing.T) {
	store := &sleepingBackend1692{countingBackend: countingBackend{delay: 1300 * time.Millisecond}}
	b := NewArrowBuffer(cfg1692(1), store, zerolog.Nop()) // flush timeout 1 s
	t.Cleanup(func() { _ = b.Close() })
	rec := &tierRecorder1693{}
	b.tierRecorder = rec

	require.NoError(t, b.writeColumnarInternal(context.Background(), "db", hoursRecord1692(1, 9), true, ""))
	err, _ := flushWithin(t, b, 10*time.Second)
	require.NoError(t, err)
	require.Equal(t, int64(1), store.writes.Load(), "the slow PUT completed")

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Len(t, rec.errs, 1, "the stored file reached tiering registration")
	require.NoError(t, rec.errs[0], "registration ran under the write's spent context")
}

// The same at shutdown: a single-hour PUT that completes as Close's budget runs
// out is stored, and its registration must not run under the cancelled flush
// context (the multi-hour form is TestNoWALRegistrationOutlivesCloseCancel_1693).
func TestSingleHourRegistrationOutlivesCloseCancel(t *testing.T) {
	store := &sleepingBackend1692{countingBackend: countingBackend{delay: 800 * time.Millisecond}}
	cfg := cfg1692(30)
	cfg.MaxBufferSize = 1 // the 1-row write below goes to a worker
	b := NewArrowBuffer(cfg, store, zerolog.Nop())
	rec := &tierRecorder1693{}
	b.tierRecorder = rec
	b.SetCloseBudget(time.Second) // flushParent cancelled at budget/2, mid-upload

	require.NoError(t, b.writeColumnarInternal(context.Background(), "db", hoursRecord1692(1, 8), true, ""))
	require.Eventually(t, func() bool { return store.started.Load() >= 1 }, 5*time.Second, 10*time.Millisecond)
	_ = b.Close()

	require.Equal(t, int64(1), store.writes.Load(), "the upload completed after the cancel (the test needs the straddle)")
	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Len(t, rec.errs, 1, "the stored file reached tiering registration")
	require.NoError(t, rec.errs[0], "registration ran under the cancelled flush context")
}
