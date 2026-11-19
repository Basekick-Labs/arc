package ingest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

type publishedFile struct {
	database, measurement string
	hour                  time.Time
	stored                int64 // files in storage when the report was made
}

// failingAfterBackend is a local backend that fails every Write from the
// n-th on (0: never) and counts the ones that stored.
type failingAfterBackend struct {
	*storage.LocalBackend
	failFrom, writes, stored atomic.Int64
}

func (f *failingAfterBackend) Write(ctx context.Context, path string, data []byte) error {
	if n := f.writes.Add(1); f.failFrom.Load() != 0 && n >= f.failFrom.Load() {
		return errors.New("injected storage failure")
	}
	if err := f.LocalBackend.Write(ctx, path, data); err != nil {
		return err
	}
	f.stored.Add(1)
	return nil
}

func newFilePublishedBuffer(t *testing.T) (*ArrowBuffer, *failingAfterBackend, func() []publishedFile) {
	t.Helper()
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	backend := &failingAfterBackend{LocalBackend: local}
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0", FlushTimeoutSeconds: 10}, backend, zerolog.Nop())
	t.Cleanup(func() { b.Close() })
	var mu sync.Mutex
	var got []publishedFile
	b.SetOnFilePublished(func(database, measurement string, partitionTime time.Time) {
		mu.Lock()
		got = append(got, publishedFile{database, measurement, partitionTime.UTC().Truncate(time.Hour), backend.stored.Load()})
		mu.Unlock()
	})
	return b, backend, func() []publishedFile {
		mu.Lock()
		defer mu.Unlock()
		return append([]publishedFile(nil), got...)
	}
}

func writeAt(t *testing.T, b *ArrowBuffer, times ...time.Time) {
	t.Helper()
	ts := make([]int64, len(times))
	vs := make([]float64, len(times))
	for i, tm := range times {
		ts[i] = tm.UnixMicro()
		vs[i] = float64(i)
	}
	if err := b.WriteTypedColumnarDirect(context.Background(), "db", "cpu",
		&TypedColumnBatch{Data: map[string]interface{}{"time": ts, "v": vs}}, len(times)); err != nil {
		t.Fatal(err)
	}
}

// midHour is a time well inside the current hour.
func midHour(t *testing.T) time.Time {
	t.Helper()
	now := time.Now().UTC()
	if now.Truncate(time.Hour).Add(time.Hour).Sub(now) < 30*time.Second {
		t.Skip("too close to the end of the hour")
	}
	return now
}

// Each file a flush writes is reported once, with its measurement and the
// hour its rows are in, and only after the write has returned: a report made
// before it would let a query list, miss the file, and cache that. A write
// that returned an error is reported too, because it can still have landed.
func TestFilePublishedReportsEachStoredFile(t *testing.T) {
	now := midHour(t)
	late := now.Add(-3 * time.Hour)

	t.Run("one hour", func(t *testing.T) {
		b, _, reported := newFilePublishedBuffer(t)
		writeAt(t, b, late)
		if err := b.FlushAllAndWait(context.Background()); err != nil {
			t.Fatal(err)
		}
		got := reported()
		if len(got) != 1 || got[0].database != "db" || got[0].measurement != "cpu" || !got[0].hour.Equal(late.Truncate(time.Hour)) {
			t.Fatalf("reported %+v, want one file of db/cpu in hour %s", got, late.Truncate(time.Hour))
		}
		if got[0].stored != 1 {
			t.Errorf("reported with %d files in storage, want 1: the report must follow the write", got[0].stored)
		}
	})

	t.Run("two hours, synchronous flush", func(t *testing.T) {
		b, _, reported := newFilePublishedBuffer(t)
		writeAt(t, b, now, late)
		if err := b.FlushAll(context.Background()); err != nil {
			t.Fatal(err)
		}
		got := reported()
		if len(got) != 2 {
			t.Fatalf("reported %+v, want one per hour file", got)
		}
		hours := map[time.Time]bool{got[0].hour: true, got[1].hour: true}
		if !hours[now.Truncate(time.Hour)] || !hours[late.Truncate(time.Hour)] {
			t.Errorf("hours reported %v, want %s and %s", hours, now.Truncate(time.Hour), late.Truncate(time.Hour))
		}
		for i, f := range got {
			if f.stored != int64(i+1) {
				t.Errorf("file %d reported with %d files in storage, want %d", i, f.stored, i+1)
			}
		}
	})

	t.Run("a write that fails is reported: it may have landed", func(t *testing.T) {
		for name, hours := range map[string][]time.Time{"one hour": {late}, "two hours": {now, late}} {
			t.Run(name, func(t *testing.T) {
				b, backend, reported := newFilePublishedBuffer(t)
				backend.failFrom.Store(int64(len(hours))) // the last file's write fails
				writeAt(t, b, hours...)
				_ = b.FlushAllAndWait(context.Background())
				if got := backend.writes.Load(); got < int64(len(hours)) {
					t.Fatalf("%d writes, want %d attempted", got, len(hours))
				}
				got := reported()
				if len(got) != len(hours) {
					t.Fatalf("reported %+v, want one report per attempted file", got)
				}
				// The failed one was reported after its write returned, with
				// nothing more in storage than before it.
				if last := got[len(got)-1]; last.stored != int64(len(hours)-1) {
					t.Errorf("the failed write was reported with %d files in storage, want %d", last.stored, len(hours)-1)
				}
			})
		}
	})

	t.Run("no function installed", func(t *testing.T) {
		b, _, reported := newFilePublishedBuffer(t)
		b.SetOnFilePublished(nil)
		writeAt(t, b, late)
		if err := b.FlushAllAndWait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := reported(); len(got) != 0 {
			t.Errorf("reported %+v after the function was removed", got)
		}
	})
}

// Several receivers are each told about every file, in the order they were
// added; Set replaces them all.
func TestAddOnFilePublishedCallsEveryReceiverInOrder(t *testing.T) {
	b, _, _ := newFilePublishedBuffer(t)
	b.SetOnFilePublished(nil)
	var mu sync.Mutex
	var calls []string
	note := func(name string) FilePublishedFunc {
		return func(database, measurement string, _ time.Time) {
			mu.Lock()
			calls = append(calls, name+":"+database+"/"+measurement)
			mu.Unlock()
		}
	}
	b.AddOnFilePublished(note("first"))
	b.AddOnFilePublished(nil) // ignored
	b.AddOnFilePublished(note("second"))
	writeAt(t, b, midHour(t).Add(-3*time.Hour))
	if err := b.FlushAllAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([]string(nil), calls...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "first:db/cpu" || got[1] != "second:db/cpu" {
		t.Fatalf("calls = %v, want first then second, once each", got)
	}

	b.SetOnFilePublished(note("only"))
	writeAt(t, b, midHour(t).Add(-3*time.Hour))
	if err := b.FlushAllAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 3 || calls[2] != "only:db/cpu" {
		t.Errorf("calls = %v, want Set to replace both receivers", calls)
	}
}
