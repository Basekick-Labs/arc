package ingest

import (
	"context"
	"sync"
	"testing"
	"time"
)

// registeredRecorder notes what the file-registered hook was told.
type registeredRecorder struct {
	mu    sync.Mutex
	paths []string
	sizes []int64
}

func (r *registeredRecorder) hook(_, _, storagePath string, size int64) {
	r.mu.Lock()
	r.paths = append(r.paths, storagePath)
	r.sizes = append(r.sizes, size)
	r.mu.Unlock()
}

func (r *registeredRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.paths)
}

// A stored file is reported once, with its storage key and its size; a file
// whose write failed is not reported at all (#1751: the Iceberg export adds
// what is reported without checking the listing).
func TestFileRegisteredIsToldOfStoredFilesOnly(t *testing.T) {
	t.Run("stored", func(t *testing.T) {
		b, backend, _ := newPublishDelayBuffer(t, 100000)
		rec := &registeredRecorder{}
		b.SetOnFileRegistered(rec.hook)
		writePublishDelayRows(t, b, inHour(t))
		if err := b.FlushAll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if rec.count() != 1 {
			t.Fatalf("%d reports for one stored file, want 1", rec.count())
		}
		size, err := backend.StatFile(context.Background(), rec.paths[0])
		if err != nil {
			t.Fatalf("the reported key %q is not in storage: %v", rec.paths[0], err)
		}
		if size != rec.sizes[0] {
			t.Errorf("reported %d bytes, the file has %d", rec.sizes[0], size)
		}
	})
	t.Run("write failed", func(t *testing.T) {
		b, backend, _ := newPublishDelayBuffer(t, 100000)
		backend.failFrom = 1
		rec := &registeredRecorder{}
		b.SetOnFileRegistered(rec.hook)
		writePublishDelayRows(t, b, inHour(t))
		_ = b.FlushAllAndWait(context.Background())
		if backend.writes.Load() == 0 {
			t.Fatal("the flush never reached storage")
		}
		if rec.count() != 0 {
			t.Errorf("%d reports for a flush whose write failed, want 0", rec.count())
		}
	})
	t.Run("one of two hour files stored", func(t *testing.T) {
		b, backend, _ := newPublishDelayBuffer(t, 100000)
		backend.failFrom = 2
		rec := &registeredRecorder{}
		b.SetOnFileRegistered(rec.hook)
		now := inHour(t)
		writePublishDelayRows(t, b, now, now.Add(-3*time.Hour))
		_ = b.FlushAllAndWait(context.Background())
		if rec.count() != 1 {
			t.Errorf("%d reports with one of two hour files stored, want 1", rec.count())
		}
	})
	t.Run("removed", func(t *testing.T) {
		b, _, _ := newPublishDelayBuffer(t, 100000)
		rec := &registeredRecorder{}
		b.SetOnFileRegistered(rec.hook)
		b.SetOnFileRegistered(nil)
		writePublishDelayRows(t, b, inHour(t))
		if err := b.FlushAll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if rec.count() != 0 {
			t.Errorf("%d reports after the hook was removed, want 0", rec.count())
		}
	})
}

// Hour files that wait for the rest of their flush are reported when the
// flush keeps them, and not at all when a later hour fails and it keeps none:
// such a file is in storage and can still be taken away.
func TestFileRegisteredWaitsForDeferredHourFilesToBeKept(t *testing.T) {
	t.Run("every hour stored", func(t *testing.T) {
		store := newMemStore1697(0)
		b := walBuffer1697(t, store)
		b.SetFileRegistrar(&recordingRegistrar1693{})
		rec := &registeredRecorder{}
		b.SetOnFileRegistered(rec.hook)
		writeChunked(t, b, hoursRecord1692(4, 2))
		if err, _ := flushWithin(t, b, 20*time.Second); err != nil {
			t.Fatal(err)
		}
		if rec.count() != 4 {
			t.Errorf("%d reports for four deferred hour files, want 4", rec.count())
		}
	})
	t.Run("a later hour fails", func(t *testing.T) {
		store := newMemStore1697(4) // hours 1-3 stored, the 4th fails
		b := walBuffer1697(t, store)
		b.SetFileRegistrar(&recordingRegistrar1693{})
		rec := &registeredRecorder{}
		b.SetOnFileRegistered(rec.hook)
		writeChunked(t, b, hoursRecord1692(6, 1))
		if err, _ := flushWithin(t, b, 20*time.Second); err == nil {
			t.Fatal("the flush succeeded; the 4th write was meant to fail")
		}
		if store.count() != 3 {
			t.Fatalf("%d files stored, want 3", store.count())
		}
		if rec.count() != 0 {
			t.Errorf("%d reports for stored files the flush did not keep, want 0", rec.count())
		}
	})
}
