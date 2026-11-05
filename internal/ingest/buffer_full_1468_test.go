package ingest

// #1468: once the rows held for flushing reach ingest.max_buffered_bytes, new
// requests are refused with a retryable ErrBufferFull at admission, before
// anything of them is written; imports from 90% of it. Replay is never
// refused. The buffer-full state MQTT suspends on sets at the limit and clears
// below 90% of it.

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

func bufferFullRejections() int64 {
	return metrics.Get().Snapshot()["ingest_buffer_full_rejections"].(int64)
}

func TestResolveMaxBufferedBytes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cfg   *config.IngestConfig
		limit uint64
		want  int64
	}{
		{"no config", nil, 8 << 30, 0},
		{"off", &config.IngestConfig{}, 8 << 30, 0},
		{"explicit", &config.IngestConfig{MaxBufferedBytes: 256 << 20}, 8 << 30, 256 << 20},
		{"explicit ignores the memory limit", &config.IngestConfig{MaxBufferedBytes: 256 << 20}, 0, 256 << 20},
		{"auto is half the memory limit", &config.IngestConfig{MaxBufferedBytesAuto: true}, 8 << 30, 4 << 30},
		{"auto without a memory limit is off", &config.IngestConfig{MaxBufferedBytesAuto: true}, 0, 0},
		{"auto does not overflow", &config.IngestConfig{MaxBufferedBytesAuto: true}, math.MaxUint64, math.MaxUint64 / 100 * 50},
		{"auto of an odd limit is exact", &config.IngestConfig{MaxBufferedBytesAuto: true}, 1001, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveMaxBufferedBytes(tc.cfg, tc.limit); got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// limitedBuffer is a buffer built with limit (so the sampler, which runs from
// NewArrowBuffer, never races a later assignment) and buffered bytes stored.
func limitedBuffer(t *testing.T, limit, buffered int64) (*ArrowBuffer, *checkpointRecordingWAL) {
	t.Helper()
	return limitedBufferLogging(t, limit, buffered, zerolog.Nop())
}

func limitedBufferLogging(t *testing.T, limit, buffered int64, lg zerolog.Logger) (*ArrowBuffer, *checkpointRecordingWAL) {
	t.Helper()
	b := NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 16,
		MaxBufferedBytes: limit,
	}, &mockStorageBackend{}, lg)
	w := &checkpointRecordingWAL{}
	b.SetWAL(w)
	t.Cleanup(func() { b.Close() })
	b.bufferBytesBuffered.Store(buffered)
	return b, w
}

// observe stores n as the buffered bytes before observing it: the 1 Hz
// sampler observes the stored value too, and then sees the same thing.
func observe(b *ArrowBuffer, n int64) {
	b.bufferBytesBuffered.Store(n)
	b.observeBufferedBytes(n)
}

func TestAdmitRequestRefusesAtTheLimit(t *testing.T) {
	const limit = 1 << 20
	for _, tc := range []struct {
		name     string
		buffered int64
		refused  bool
	}{
		{"below", limit - 1, false},
		{"at", limit, true},
		{"over", 2 * limit, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := limitedBuffer(t, limit, tc.buffered)
			before := bufferFullRejections()
			_, release, err := b.AdmitRequest(context.Background(), 100)
			release()
			if !tc.refused {
				if err != nil {
					t.Fatalf("refused below the limit: %v", err)
				}
				return
			}
			if !errors.Is(err, ErrBufferFull) || !IsWriteRefusal(err) || RefusalReason(err) != RefusalBufferFull {
				t.Fatalf("err = %v, want ErrBufferFull", err)
			}
			if errors.Is(err, ErrIngestFenced) {
				t.Fatal("a buffer-full refusal reads as a fence: clients would retry elsewhere")
			}
			if after := bufferFullRejections(); after != before+1 {
				t.Fatalf("rejections %d -> %d, want +1", before, after)
			}
			if !b.bufferFull.Load() {
				t.Fatal("a refusal at the limit did not mark the buffer full")
			}
		})
	}
}

func TestAdmitRequestWithTheLimitOffNeverRefuses(t *testing.T) {
	b, _ := limitedBuffer(t, 0, 1<<40)
	if _, release, err := b.AdmitRequest(context.Background(), 100); err != nil {
		t.Fatal(err)
	} else {
		release()
	}
}

// The check comes before the disk gate's reservation: a refused request holds
// no WAL bytes.
func TestBufferFullRefusalReservesNoWALBytes(t *testing.T) {
	b, _ := limitedBuffer(t, 1<<20, 1<<20)
	gate := &reserveCountingGate{}
	b.SetWALDiskGate(gate)
	if _, release, err := b.AdmitRequest(context.Background(), 4096); !errors.Is(err, ErrBufferFull) {
		t.Fatalf("err = %v", err)
	} else {
		release()
	}
	if n := gate.reserved.Load(); n != 0 {
		t.Fatalf("a refused request reserved %d WAL bytes", n)
	}
}

type reserveCountingGate struct{ reserved atomic.Int64 }

func (g *reserveCountingGate) Pressure() bool { return false }
func (g *reserveCountingGate) Reserve(n int64) bool {
	g.reserved.Add(n)
	return true
}
func (g *reserveCountingGate) Release(n int64) { g.reserved.Add(-n) }

func TestBufferFullRefusalCountedOnceAcrossRetries(t *testing.T) {
	b, _ := limitedBuffer(t, 1<<20, 1<<20)
	ctx := WithRefusalCounted(context.Background())
	before := bufferFullRejections()
	for range 3 {
		if _, _, err := b.AdmitRequest(ctx, 1); !errors.Is(err, ErrBufferFull) {
			t.Fatalf("err = %v", err)
		}
	}
	if after := bufferFullRejections(); after != before {
		t.Fatalf("retries under a counted context counted %d more", after-before)
	}
}

func TestAdmitImportRefusesFromNinetyPercent(t *testing.T) {
	const limit = 100 << 20
	for _, tc := range []struct {
		name     string
		buffered int64
		refused  bool
	}{
		{"below 90%", limit/100*90 - 1, false},
		{"at 90%", limit / 100 * 90, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := limitedBuffer(t, limit, tc.buffered)
			_, release, err := b.AdmitImport(context.Background(), 100)
			release()
			if tc.refused != errors.Is(err, ErrBufferFull) {
				t.Fatalf("err = %v, refused want %v", err, tc.refused)
			}
			if b.bufferFull.Load() {
				t.Fatal("an import refused below the limit marked the buffer full (MQTT would suspend)")
			}
		})
	}
	// Other writes are still admitted in the import band.
	b, _ := limitedBuffer(t, limit, limit/100*95)
	if _, release, err := b.AdmitRequest(context.Background(), 100); err != nil {
		t.Fatalf("a write below the limit was refused: %v", err)
	} else {
		release()
	}
}

// WAL replay never calls AdmitRequest: a node over the limit still recovers.
func TestReplayIsNeverRefusedForBufferedBytes(t *testing.T) {
	b, _ := limitedBuffer(t, 1<<20, 1<<30)
	if err := b.WriteColumnarDirectReplay(context.Background(), "db", "m", checkpointTestRecord().Columns, ""); err != nil {
		t.Fatalf("replay refused: %v", err)
	}
}

// bufferFullEvents records the listener's calls.
type bufferFullEvents struct {
	mu  sync.Mutex
	got []bool
}

func (e *bufferFullEvents) fn(full bool) {
	e.mu.Lock()
	e.got = append(e.got, full)
	e.mu.Unlock()
}

func (e *bufferFullEvents) snapshot() []bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]bool(nil), e.got...)
}

func TestBufferFullStateSetsAtTheLimitAndClearsBelowNinetyPercent(t *testing.T) {
	const limit = 100 << 20
	b, _ := limitedBuffer(t, limit, 0)
	ev := &bufferFullEvents{}
	b.OnBufferFullChange(ev.fn)
	steps := []struct {
		bytes int64
		full  bool
	}{
		{limit / 2, false},        // below: nothing
		{limit, true},             // at: on
		{limit * 2, true},         // still on: no second event
		{limit / 100 * 95, true},  // between 90% and the limit: stays on
		{limit / 100 * 90, true},  // at 90%: stays on
		{limit/100*90 - 1, false}, // below 90%: off
		{limit / 100 * 95, false}, // back between 90% and the limit: stays off
		{limit / 2, false},        // still off
	}
	for _, st := range steps {
		observe(b, st.bytes)
		if b.bufferFull.Load() != st.full {
			t.Fatalf("at %d bytes full = %v, want %v", st.bytes, !st.full, st.full)
		}
	}
	got := ev.snapshot()
	want := []bool{false, true, false} // the registration's current state, then one edge each way
	if len(got) != len(want) {
		t.Fatalf("events %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events %v, want %v", got, want)
		}
	}
}

// A listener registered after the state set (the sampler runs from
// NewArrowBuffer, before main.go wires MQTT) is told at once.
func TestBufferFullListenerRegisteredLateGetsTheState(t *testing.T) {
	b, _ := limitedBuffer(t, 1<<20, 0)
	observe(b, 1<<20)
	ev := &bufferFullEvents{}
	b.OnBufferFullChange(ev.fn)
	if got := ev.snapshot(); len(got) != 1 || !got[0] {
		t.Fatalf("events %v, want [true]", got)
	}
}

func TestAdmitRequestRefusalNotifiesTheListener(t *testing.T) {
	b, _ := limitedBuffer(t, 1<<20, 1<<20)
	ev := &bufferFullEvents{}
	b.OnBufferFullChange(ev.fn)
	_, _, _ = b.AdmitRequest(context.Background(), 1)
	_, _, _ = b.AdmitRequest(context.Background(), 1)
	// [false true]: registered empty, then the refusal set it. [true]: the
	// sampler saw the stored bytes first; either way one "full", no clear.
	got := ev.snapshot()
	full := 0
	for _, f := range got {
		if f {
			full++
		}
	}
	if full != 1 || !got[len(got)-1] {
		t.Fatalf("events %v, want one transition to full", got)
	}
}

func TestBufferFullWarningOncePerEpisode(t *testing.T) {
	const limit = 100 << 20
	logs := &syncLogBuffer{}
	b, _ := limitedBufferLogging(t, limit, 0, zerolog.New(logs))
	count := func() int { return strings.Count(logs.String(), "near ingest.max_buffered_bytes") }

	observe(b, limit/100*80-1)
	if count() != 0 {
		t.Fatal("warned below 80%")
	}
	observe(b, limit/100*80)
	observe(b, limit)
	if count() != 1 {
		t.Fatalf("warned %d times in one episode, want 1", count())
	}
	observe(b, limit/100*70) // above the re-arm level
	observe(b, limit/100*85)
	if count() != 1 {
		t.Fatalf("re-armed above 64%%: %d warnings", count())
	}
	observe(b, limit/100*64-1)
	observe(b, limit/100*85)
	if count() != 2 {
		t.Fatalf("did not re-arm below 64%%: %d warnings", count())
	}
}

// syncLogBuffer is a log sink safe for the sampler's goroutine to write to.
type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *syncLogBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *syncLogBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}
