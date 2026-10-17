package ingest

// #676 part B: under WAL volume disk pressure every live write is refused with
// a retryable ErrWALDiskPressure before anything is written, the WAL included.
// Replay, recovery and replication paths are never gated. A handler that
// admitted a request as a whole is not refused part-way through it.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

func liveWrites(ctx context.Context) map[string]func(*ArrowBuffer) error {
	cols := func() map[string][]interface{} {
		return map[string][]interface{}{"time": {time.Now().UnixMicro()}, "value": {1.0}}
	}
	return map[string]func(*ArrowBuffer) error{
		"WriteColumnarRecord": func(b *ArrowBuffer) error { return b.WriteColumnarRecord(ctx, "db", checkpointTestRecord()) },
		"WriteColumnarDirect": func(b *ArrowBuffer) error { return b.WriteColumnarDirect(ctx, "db", "m", cols()) },
		"Write (rows)": func(b *ArrowBuffer) error {
			return b.Write(ctx, "db", []interface{}{&models.Record{Measurement: "m", Time: time.Now(), Fields: map[string]interface{}{"v": 1.0}}})
		},
		"WriteTypedColumnarDirect": func(b *ArrowBuffer) error {
			batch := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{time.Now().UnixMicro()}, "value": []float64{1}}}
			return b.WriteTypedColumnarDirect(ctx, "db", "m", batch, 1)
		},
	}
}

func rejections() int64 {
	return metrics.Get().Snapshot()["wal_disk_pressure_rejections"].(int64)
}

func TestWALDiskPressureRefusesEveryLiveWrite(t *testing.T) {
	for name, write := range liveWrites(context.Background()) {
		t.Run(name, func(t *testing.T) {
			buffer, wal := newCheckpointBuffer(1000)
			defer buffer.Close()
			buffer.SetWALDiskGate(PressureFunc(func() bool { return true }))
			before := rejections()
			err := write(buffer)
			if !errors.Is(err, ErrWALDiskPressure) || !IsWriteRefusal(err) || RefusalReason(err) != RefusalWALDisk {
				t.Fatalf("err = %v, want ErrWALDiskPressure", err)
			}
			if errors.Is(err, ErrIngestFenced) {
				t.Fatal("a disk pressure refusal reads as a fence: clients would retry elsewhere")
			}
			if handedOut, _ := wal.counts(); handedOut != 0 {
				t.Fatalf("a refused write reached the WAL (%d entries)", handedOut)
			}
			if after := rejections(); after != before+1 {
				t.Fatalf("rejections %d -> %d, want +1", before, after)
			}
		})
	}
}

func TestWALDiskGateAdmitsWithoutPressure(t *testing.T) {
	for name, write := range liveWrites(context.Background()) {
		t.Run(name, func(t *testing.T) {
			buffer, _ := newCheckpointBuffer(1000)
			defer buffer.Close()
			buffer.SetWALDiskGate(PressureFunc(func() bool { return false }))
			if err := write(buffer); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Replay and replication carry rows already accepted; refusing them would
// strand them, and they hold no new client data to back off.
func TestWALDiskPressureNeverGatesReplay(t *testing.T) {
	ctx := context.Background()
	cols := func() map[string][]interface{} {
		return map[string][]interface{}{"time": {time.Now().UnixMicro()}, "value": {1.0}}
	}
	paths := map[string]func(*ArrowBuffer) error{
		"WriteColumnarDirectReplay": func(b *ArrowBuffer) error { return b.WriteColumnarDirectReplay(ctx, "db", "m", cols(), "") },
		"WriteColumnarDirectReplayWithTagColumns": func(b *ArrowBuffer) error {
			return b.WriteColumnarDirectReplayWithTagColumns(ctx, "db", "m", cols(), "", nil)
		},
		"WriteColumnarDirectNoWAL": func(b *ArrowBuffer) error { return b.WriteColumnarDirectNoWAL(ctx, "db", "m", cols()) },
	}
	for name, write := range paths {
		t.Run(name, func(t *testing.T) {
			buffer, _ := newCheckpointBuffer(1000)
			defer buffer.Close()
			buffer.SetWALDiskGate(PressureFunc(func() bool { return true }))
			if err := write(buffer); err != nil {
				t.Fatalf("refused under disk pressure: %v", err)
			}
		})
	}
}

// A request admitted as a whole is not refused part-way through when pressure
// starts; an unmarked caller (MQTT, CQ, a direct call) still is.
func TestAdmitRequestHoldsForTheWholeRequest(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	var pressure atomic.Bool
	buffer.SetWALDiskGate(PressureFunc(pressure.Load))
	ctx, release, err := buffer.AdmitRequest(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	pressure.Store(true) // between the admission and the measurements
	for name, write := range liveWrites(ctx) {
		if err := write(buffer); err != nil {
			t.Fatalf("%s: an admitted request was refused part-way: %v", name, err)
		}
	}
	for name, write := range liveWrites(context.Background()) {
		if err := write(buffer); !errors.Is(err, ErrWALDiskPressure) {
			t.Fatalf("%s: an unadmitted write passed under pressure: %v", name, err)
		}
	}
	if _, _, err := buffer.AdmitRequest(context.Background(), 0); !errors.Is(err, ErrWALDiskPressure) {
		t.Fatalf("AdmitRequest under pressure = %v", err)
	}
}

// The admission marker skips only the disk check: a fence that closes
// mid-request is still enforced per call.
func TestAdmitRequestDoesNotSkipTheFence(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	buffer.SetWALDiskGate(PressureFunc(func() bool { return false }))
	var fenced atomic.Bool
	buffer.SetIngestGate(func() error {
		if fenced.Load() {
			return fmt.Errorf("%w: test", ErrIngestFenced)
		}
		return nil
	})
	ctx, release, err := buffer.AdmitRequest(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	fenced.Store(true)
	if err := buffer.WriteColumnarRecord(ctx, "db", checkpointTestRecord()); !errors.Is(err, ErrIngestFenced) {
		t.Fatalf("err = %v, want the fence", err)
	}
}

// With no gate (WAL off, or wal.disk_pressure_enabled off) admission is free:
// the context comes back unchanged, no allocation.
func TestAdmitRequestWithoutGateReturnsTheSameContext(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	ctx := context.WithValue(context.Background(), struct{ k int }{1}, 1)
	got, release, err := buffer.AdmitRequest(ctx, 100)
	release()
	if err != nil || got != ctx {
		t.Fatalf("AdmitRequest wrapped the context with no gate (%v)", err)
	}
	if n := testing.AllocsPerRun(100, func() { _, r, _ := buffer.AdmitRequest(ctx, 100); r() }); n != 0 {
		t.Fatalf("%v allocations per admission with no gate", n)
	}
}

// The gate is a slot of its own: the cluster wiring replacing the ingest gate
// must not remove it.
func TestWALDiskGateSurvivesSetIngestGate(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	buffer.SetWALDiskGate(PressureFunc(func() bool { return true }))
	buffer.SetIngestGate(func() error { return nil })
	buffer.SetIngestGate(nil)
	if err := buffer.WriteColumnarRecord(context.Background(), "db", checkpointTestRecord()); !errors.Is(err, ErrWALDiskPressure) {
		t.Fatalf("err = %v: replacing the ingest gate removed the disk gate", err)
	}
	buffer.SetWALDiskGate(nil)
	if err := buffer.WriteColumnarRecord(context.Background(), "db", checkpointTestRecord()); err != nil {
		t.Fatal(err)
	}
}

func TestWALDiskRefusalLogNamesItsOwnMetric(t *testing.T) {
	var buf lockedBuf
	defer CaptureRefusalLog(zerolog.New(&buf), time.Now)()
	LogRefusal("/api/v1/write", ErrWALDiskPressure)
	out := buf.String()
	if !strings.Contains(out, `"reason":"wal_disk"`) || !strings.Contains(out, "arc_wal_disk_pressure_rejections_total") {
		t.Fatalf("log line:\n%s", out)
	}
	if strings.Contains(out, "arc_cluster_ingest_fenced_writes_total") {
		t.Fatalf("a disk refusal points at the fence metric:\n%s", out)
	}
}

func TestIsWriteRefusal(t *testing.T) {
	cases := map[error]bool{
		nil:                                  false,
		errors.New("x"):                      false,
		ErrBufferClosing:                     false,
		ErrIngestFenced:                      true,
		fmt.Errorf("%w: x", ErrIngestFenced): true,
		ErrWALDiskPressure:                   true,
		fmt.Errorf("wrap: %w", ErrWALDiskPressure):    true,
		&WriteRefusal{Reason: "wal_queue", Text: "q"}: true,
	}
	for err, want := range cases {
		if got := IsWriteRefusal(err); got != want {
			t.Errorf("IsWriteRefusal(%v) = %v, want %v", err, got, want)
		}
	}
	if errors.Is(&WriteRefusal{Reason: "wal_queue"}, ErrWALDiskPressure) {
		t.Fatal("refusals of different reasons match")
	}
}

// BenchmarkLPWriteGates measures the per-call gate cost of a 1000-row write
// with no disk gate installed (WAL off): compare with main for the #1115
// regression.
func BenchmarkLPWriteGates(b *testing.B) {
	buffer, _ := newCheckpointBuffer(1 << 30)
	buffer.SetWAL(nil)
	defer buffer.Close()
	times := make([]interface{}, 1000)
	vals := make([]interface{}, 1000)
	now := time.Now().UnixMicro()
	for i := range times {
		times[i], vals[i] = now+int64(i), float64(i)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := &models.ColumnarRecord{Measurement: "cpu", Columnar: true, Columns: map[string][]interface{}{"time": times, "value": vals}}
		if err := buffer.WriteColumnarRecord(ctx, "db", rec); err != nil {
			b.Fatal(err)
		}
	}
}

// budgetGate is a WALDiskGate with a byte budget, the contract of
// wal.DiskMonitor.Reserve: concurrent reservations see each other.
type budgetGate struct {
	mu       sync.Mutex
	budget   int64
	reserved int64
	released int64
}

func (g *budgetGate) Pressure() bool { return false }
func (g *budgetGate) Reserve(n int64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.reserved+n > g.budget {
		return false
	}
	g.reserved += n
	return true
}
func (g *budgetGate) Release(n int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reserved -= n
	g.released += n
}
func (g *budgetGate) outstanding() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reserved
}

// A request larger than what is left is refused at admission, and counted.
func TestAdmitRequestRefusesWhatDoesNotFit(t *testing.T) {
	buffer, wal := newCheckpointBuffer(1000)
	defer buffer.Close()
	buffer.SetWALDiskGate(&budgetGate{budget: 100})
	before := rejections()
	if _, _, err := buffer.AdmitRequest(context.Background(), 101); !errors.Is(err, ErrWALDiskPressure) {
		t.Fatalf("err = %v, want a refusal", err)
	}
	if after := rejections(); after != before+1 {
		t.Fatalf("rejections %d -> %d", before, after)
	}
	if handedOut, _ := wal.counts(); handedOut != 0 {
		t.Fatal("a refused request reached the WAL")
	}
	_, release, err := buffer.AdmitRequest(context.Background(), 100)
	if err != nil {
		t.Fatalf("a request that fits was refused: %v", err)
	}
	release()
}

// Appends under an admitted context consume its reservation, so a byte is
// charged once; release returns the rest, once.
func TestAdmittedAppendsConsumeTheReservation(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	g := &budgetGate{budget: 1 << 20}
	buffer.SetWALDiskGate(g)
	ctx, release, err := buffer.AdmitRequest(context.Background(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	rec := checkpointTestRecord() // a 3-byte raw payload reaches the WAL
	if err := buffer.WriteColumnarRecord(ctx, "db", rec); err != nil {
		t.Fatal(err)
	}
	if got := g.outstanding(); got != 1000-int64(len(rec.RawPayload)) {
		t.Fatalf("outstanding %d after a %d-byte append, want %d", got, len(rec.RawPayload), 1000-len(rec.RawPayload))
	}
	release()
	release()
	if got := g.outstanding(); got != 0 {
		t.Fatalf("outstanding %d after release, want 0", got)
	}
}

// A retry of a write already refused and counted is not counted again (MQTT).
func TestRefusalCountedOnceAcrossRetries(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	buffer.SetWALDiskGate(PressureFunc(func() bool { return true }))
	before := rejections()
	_, _, _ = buffer.AdmitRequest(context.Background(), 1)
	retry := WithRefusalCounted(context.Background())
	_, _, _ = buffer.AdmitRequest(retry, 1)
	if err := buffer.WriteColumnarRecord(retry, "db", checkpointTestRecord()); !errors.Is(err, ErrWALDiskPressure) {
		t.Fatal(err)
	}
	if after := rejections(); after != before+1 {
		t.Fatalf("rejections %d -> %d, want one", before, after)
	}
}
