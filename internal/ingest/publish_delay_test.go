package ingest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/rs/zerolog"
)

func publishDelayCount() int64 {
	return metrics.Get().Snapshot()["ingest_publish_delay_count"].(int64)
}

func publishDelaySum() float64 {
	return metrics.Get().Snapshot()["ingest_publish_delay_sum_seconds"].(float64)
}

// publishDelayBackend is a local backend that notes how many observations
// existed while each Write was still running, and can fail a chosen Write.
type publishDelayBackend struct {
	*storage.LocalBackend
	mu       sync.Mutex
	inWrite  []int64 // publishDelayCount() seen inside each Write, before it stores
	writes   atomic.Int64
	failFrom int64 // fail every Write from this one on (1-based); 0 = never
}

func (p *publishDelayBackend) Write(ctx context.Context, path string, data []byte) error {
	n := p.writes.Add(1)
	p.mu.Lock()
	p.inWrite = append(p.inWrite, publishDelayCount())
	p.mu.Unlock()
	if p.failFrom != 0 && n >= p.failFrom {
		return errors.New("injected storage failure")
	}
	return p.LocalBackend.Write(ctx, path, data)
}

// publishDelayRecorder stands where the tiering metadata does: production
// calls RecordFile for a file right after it is stored.
type publishDelayRecorder struct {
	mu       sync.Mutex
	inRecord []int64 // publishDelayCount() seen inside each RecordFile
}

func (r *publishDelayRecorder) RecordFile(context.Context, *tiering.FileMetadata) error {
	r.mu.Lock()
	r.inRecord = append(r.inRecord, publishDelayCount())
	r.mu.Unlock()
	return nil
}

func newPublishDelayBuffer(t *testing.T, maxBufferSize int) (*ArrowBuffer, *publishDelayBackend, *publishDelayRecorder) {
	t.Helper()
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	backend := &publishDelayBackend{LocalBackend: local}
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: maxBufferSize, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0", FlushTimeoutSeconds: 10}, backend, zerolog.Nop())
	t.Cleanup(func() { b.Close() })
	rec := &publishDelayRecorder{}
	b.tierRecorder = rec
	return b, backend, rec
}

func writePublishDelayRows(t *testing.T, b *ArrowBuffer, times ...time.Time) {
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

// backdateBuffer moves the buffer's start back, as if its oldest row had
// arrived that long ago.
func backdateBuffer(t *testing.T, b *ArrowBuffer, key string, by time.Duration) {
	t.Helper()
	shard := b.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if _, ok := shard.bufferStartTimes[key]; !ok {
		t.Fatalf("no buffer for %s", key)
	}
	shard.bufferStartTimes[key] = time.Now().UTC().Add(-by)
}

// inHour returns a time safely inside the current hour, so a batch stamped
// with it is one hour file.
func inHour(t *testing.T) time.Time {
	t.Helper()
	now := time.Now().UTC()
	if now.Truncate(time.Hour).Add(time.Hour).Sub(now) < 30*time.Second {
		t.Skip("too close to the end of the hour")
	}
	return now
}

// One file, one observation, of the buffer's age (not the flush's duration),
// and made between the file being stored and the next thing the flush does
// with it: not before the write, and not left to the end of the flush.
func TestPublishDelayObservedWhenTheFileIsStored(t *testing.T) {
	b, backend, rec := newPublishDelayBuffer(t, 100000)
	writePublishDelayRows(t, b, inHour(t))
	backdateBuffer(t, b, "db/cpu", 2*time.Hour)

	count, sum := publishDelayCount(), publishDelaySum()
	if err := b.FlushAllAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := publishDelayCount() - count; got != 1 {
		t.Fatalf("%d observations for one flushed file, want 1", got)
	}
	if got := publishDelaySum() - sum; got < 7200 || got > 7260 {
		t.Errorf("observed %.1f s for a buffer started 2 h ago, want about 7200: the delay must be measured from the buffer's start, not the flush's", got)
	}
	if len(backend.inWrite) != 1 || backend.inWrite[0] != count {
		t.Errorf("observations seen inside storage.Write: %v, want [%d]: observed before the file was stored", backend.inWrite, count)
	}
	if len(rec.inRecord) != 1 || rec.inRecord[0] != count+1 {
		t.Errorf("observations seen inside RecordFile: %v, want [%d]: not observed by the time the stored file is registered", rec.inRecord, count+1)
	}
}

// The size trigger takes the asynchronous path (queue, worker), which carries
// the buffer start separately from the synchronous one.
func TestPublishDelayObservedOnTheSizeTriggeredFlush(t *testing.T) {
	b, _, _ := newPublishDelayBuffer(t, 4)
	now := inHour(t)
	count, sum := publishDelayCount(), publishDelaySum()
	writePublishDelayRows(t, b, now, now, now)
	backdateBuffer(t, b, "db/cpu", time.Hour)
	writePublishDelayRows(t, b, now) // the 4th row reaches max_buffer_size

	deadline := time.Now().Add(10 * time.Second)
	for publishDelayCount() == count && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := publishDelayCount() - count; got != 1 {
		t.Fatalf("%d observations for the size-triggered flush, want 1", got)
	}
	if got := publishDelaySum() - sum; got < 3600 || got > 3660 {
		t.Errorf("observed %.1f s for a buffer started 1 h ago, want about 3600", got)
	}
}

// FlushAll writes in the caller (the path the age sweep, the schema-change
// flush and Close also take), which carries the buffer start on its own
// struct: the flush's own start time must not stand in for it.
func TestPublishDelayObservedOnTheSynchronousFlush(t *testing.T) {
	b, _, _ := newPublishDelayBuffer(t, 100000)
	writePublishDelayRows(t, b, inHour(t))
	backdateBuffer(t, b, "db/cpu", time.Hour)
	count, sum := publishDelayCount(), publishDelaySum()
	if err := b.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := publishDelayCount() - count; got != 1 {
		t.Fatalf("%d observations for the synchronous flush, want 1", got)
	}
	if got := publishDelaySum() - sum; got < 3600 || got > 3660 {
		t.Errorf("observed %.1f s for a buffer started 1 h ago, want about 3600", got)
	}
}

// A flush that splits into hour files publishes each one.
func TestPublishDelayObservedPerHourFile(t *testing.T) {
	b, backend, _ := newPublishDelayBuffer(t, 100000)
	now := inHour(t)
	writePublishDelayRows(t, b, now, now.Add(-3*time.Hour))
	count := publishDelayCount()
	if err := b.FlushAllAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := backend.writes.Load(); got != 2 {
		t.Fatalf("%d files written for rows in two hours, want 2", got)
	}
	if got := publishDelayCount() - count; got != 2 {
		t.Errorf("%d observations for two hour files, want 2", got)
	}
}

// A file that was not stored was not published.
func TestPublishDelayNotObservedWhenTheWriteFails(t *testing.T) {
	b, backend, _ := newPublishDelayBuffer(t, 100000)
	backend.failFrom = 1
	writePublishDelayRows(t, b, inHour(t))
	count := publishDelayCount()
	_ = b.FlushAllAndWait(context.Background())
	if got := backend.writes.Load(); got == 0 {
		t.Fatal("the flush never reached storage")
	}
	if got := publishDelayCount() - count; got != 0 {
		t.Errorf("%d observations for a flush whose write failed, want 0", got)
	}
}

// Of two hour files, the one that was stored is observed and the one whose
// write failed is not.
func TestPublishDelayObservedOnlyForTheStoredHourFiles(t *testing.T) {
	b, backend, _ := newPublishDelayBuffer(t, 100000)
	backend.failFrom = 2
	now := inHour(t)
	writePublishDelayRows(t, b, now, now.Add(-3*time.Hour))
	count := publishDelayCount()
	_ = b.FlushAllAndWait(context.Background())
	if got := backend.writes.Load(); got < 2 {
		t.Fatalf("%d writes, want the second hour file attempted", got)
	}
	if got := publishDelayCount() - count; got != 1 {
		t.Errorf("%d observations with one of two hour files stored, want 1", got)
	}
}

// A batch that never sat in a buffer has no start to measure from.
func TestPublishDelaySkipsABatchWithNoBufferStart(t *testing.T) {
	count := publishDelayCount()
	observePublishDelay(time.Time{})
	if got := publishDelayCount() - count; got != 0 {
		t.Errorf("%d observations for a zero start, want 0", got)
	}
}

// With a WAL, an hour file whose rows the WAL can name is registered as soon
// as it is stored, and observed there.
func TestPublishDelayObservedPerHourFileWithAWAL(t *testing.T) {
	now := inHour(t)
	b, _ := newCoverageBuffer(t, 1000000, nil)
	rec := coverageRecordFrom("cpu",
		[]interface{}{now.UnixMicro(), now.Add(-3 * time.Hour).UnixMicro()}, []interface{}{1.0, 2.0})
	if err := b.WriteColumnarRecord(context.Background(), "db", rec); err != nil {
		t.Fatal(err)
	}
	backdateBuffer(t, b, "db/cpu", time.Hour)
	count, sum := publishDelayCount(), publishDelaySum()
	if err := b.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := publishDelayCount() - count; got != 2 {
		t.Fatalf("%d observations for two hour files, want 2", got)
	}
	if got := publishDelaySum() - sum; got < 7200 || got > 7320 {
		t.Errorf("observed %.1f s in total for two files of a buffer started 1 h ago, want about 7200", got)
	}
}

// Hour files that wait for the rest of their flush (deferred, #1697) are
// observed once each, when the flush registers them after its last hour: not
// at their write as well, and not at all when a later hour fails and the
// flush registers none of them.
func TestPublishDelayObservedForDeferredHourFilesWhenRegistered(t *testing.T) {
	t.Run("every hour stored", func(t *testing.T) {
		store := newMemStore1697(0)
		b := walBuffer1697(t, store)
		reg := &recordingRegistrar1693{}
		b.SetFileRegistrar(reg)
		writeChunked(t, b, hoursRecord1692(4, 2))
		count := publishDelayCount()
		if err, _ := flushWithin(t, b, 20*time.Second); err != nil {
			t.Fatal(err)
		}
		if reg.count() != 4 {
			t.Fatalf("%d files registered, want 4 deferred files", reg.count())
		}
		if got := publishDelayCount() - count; got != 4 {
			t.Errorf("%d observations for four deferred hour files, want 4 (one each, at registration)", got)
		}
	})
	t.Run("a later hour fails", func(t *testing.T) {
		store := newMemStore1697(4) // hours 1-3 stored, the 4th fails
		b := walBuffer1697(t, store)
		reg := &recordingRegistrar1693{}
		b.SetFileRegistrar(reg)
		writeChunked(t, b, hoursRecord1692(6, 1))
		count := publishDelayCount()
		if err, _ := flushWithin(t, b, 20*time.Second); err == nil {
			t.Fatal("the flush succeeded; the 4th write was meant to fail")
		}
		if store.count() != 3 || reg.count() != 0 {
			t.Fatalf("%d files stored and %d registered, want 3 and 0", store.count(), reg.count())
		}
		if got := publishDelayCount() - count; got != 0 {
			t.Errorf("%d observations for stored files the flush did not register, want 0", got)
		}
	})
}

// A flush split by coverage writes one file per class; each is a file of the
// same buffer and carries its start, on the synchronous path and on the
// size-triggered one.
func TestPublishDelayObservedForEachCoverageClass(t *testing.T) {
	for name, tc := range map[string]struct {
		maxBufferSize int
		flush         func(*ArrowBuffer) error
	}{
		"synchronous":    {1000000, func(b *ArrowBuffer) error { return b.FlushAll(context.Background()) }},
		"size-triggered": {3, func(b *ArrowBuffer) error { return b.FlushAllAndWait(context.Background()) }},
	} {
		t.Run(name, func(t *testing.T) {
			reg := &coverageRegistrar{}
			b, _ := newCoverageBuffer(t, tc.maxBufferSize, reg)
			b.SetClaimedBatch(func(_ uint64, _ string, cseq uint64) bool { return cseq == 2 })
			ctx := context.Background()
			count, sum := publishDelayCount(), publishDelaySum()
			for i := 0; i < 3; i++ {
				if i == 2 {
					backdateBuffer(t, b, "db/cpu", time.Hour) // before the write that fills the buffer
				}
				if err := b.WriteColumnarRecord(ctx, "db", coverageRecord("cpu", float64(i))); err != nil {
					t.Fatal(err)
				}
			}
			if err := tc.flush(b); err != nil {
				t.Fatal(err)
			}
			if files := len(reg.snapshot()); files != 2 {
				t.Fatalf("%d files, want the flush split in 2", files)
			}
			if got := publishDelayCount() - count; got != 2 {
				t.Fatalf("%d observations for two files, want 2", got)
			}
			if got := publishDelaySum() - sum; got < 7200 || got > 7320 {
				t.Errorf("observed %.1f s in total for two files of a buffer started 1 h ago, want about 7200: a class lost the buffer's start", got)
			}
		})
	}
}
