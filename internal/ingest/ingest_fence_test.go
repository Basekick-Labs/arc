package ingest

// #1207 S2: a fenced node refuses live writes before anything is written, the
// WAL included, so a rejected write never resurfaces on recovery. Replay paths
// are never gated.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/pkg/models"
)

func fencedGate() error { return fmt.Errorf("%w: test", ErrIngestFenced) }

func TestFencedWritesReachNeitherTheWALNorTheBuffer(t *testing.T) {
	ctx := context.Background()
	cols := func() map[string][]interface{} {
		return map[string][]interface{}{"time": {time.Now().UnixMicro()}, "value": {1.0}}
	}
	entries := map[string]func(*ArrowBuffer) error{
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
	for gateName, install := range map[string]func(*ArrowBuffer){
		"cluster": func(b *ArrowBuffer) { b.SetIngestGate(fencedGate) },
		"license": func(b *ArrowBuffer) {
			b.SetLicenseIngestGate(fencedGate)
			b.SetIngestGate(nil) // cluster startup cannot clear the license gate
		},
	} {
		for name, write := range entries {
			t.Run(gateName+"/"+name, func(t *testing.T) {
				buffer, wal := newCheckpointBuffer(1000)
				defer buffer.Close()
				install(buffer)
				if err := write(buffer); !errors.Is(err, ErrIngestFenced) {
					t.Fatalf("err = %v, want ErrIngestFenced", err)
				}
				if handedOut, _ := wal.counts(); handedOut != 0 {
					t.Fatalf("a fenced write reached the WAL (%d entries): it would resurface on recovery", handedOut)
				}
			})
		}
	}
}

// Replay carries rows the cluster already accepted; fencing them would strand
// recovered data.
func TestReplayIsNotFenced(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	buffer.SetIngestGate(fencedGate)
	buffer.SetLicenseIngestGate(fencedGate)
	cols := map[string][]interface{}{"time": {time.Now().UnixMicro()}, "value": {1.0}}
	if err := buffer.WriteColumnarDirectReplay(context.Background(), "db", "m", cols, ""); err != nil {
		t.Fatalf("replay refused by the ingest fence: %v", err)
	}
}

func TestOpenGateAdmits(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	buffer.SetIngestGate(func() error { return nil })
	if err := buffer.WriteColumnarRecord(context.Background(), "db", checkpointTestRecord()); err != nil {
		t.Fatal(err)
	}
}

// #1422: a request admitted whole passes the fence once. Its later buffer
// calls are written even when the fence closes part-way, as a MessagePack
// batch is; a request admitted with AdmitRequest (an import) is still
// refused per call, and a fence already closed still refuses admission.
func TestAdmitWholeRequestChecksTheFenceOnce(t *testing.T) {
	buffer, wal := newCheckpointBuffer(1000)
	defer buffer.Close()
	fenced := false
	buffer.SetIngestGate(func() error {
		if fenced {
			return fencedGate()
		}
		return nil
	})
	ctx := context.Background()
	whole, release, err := buffer.AdmitWholeRequest(ctx, 0)
	if err != nil {
		t.Fatalf("AdmitWholeRequest with the fence open: %v", err)
	}
	defer release()
	perCall, releasePerCall, err := buffer.AdmitRequest(ctx, 0)
	if err != nil {
		t.Fatalf("AdmitRequest with the fence open: %v", err)
	}
	defer releasePerCall()

	fenced = true // closes after both requests were admitted
	if err := buffer.WriteColumnarRecord(whole, "db", checkpointTestRecord()); err != nil {
		t.Fatalf("first measurement of a request admitted whole: %v", err)
	}
	batch := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{time.Now().UnixMicro()}, "value": []float64{1}}}
	if err := buffer.WriteTypedColumnarDirect(whole, "db", "m", batch, 1); err != nil {
		t.Fatalf("second measurement of a request admitted whole was refused after the first was written: %v", err)
	}
	if err := buffer.WriteColumnarRecord(perCall, "db", checkpointTestRecord()); !errors.Is(err, ErrIngestFenced) {
		t.Fatalf("write under AdmitRequest after the fence closed: err = %v, want ErrIngestFenced", err)
	}
	if _, _, err := buffer.AdmitWholeRequest(ctx, 0); !errors.Is(err, ErrIngestFenced) {
		t.Fatalf("AdmitWholeRequest with the fence closed: err = %v, want ErrIngestFenced", err)
	}
	if handedOut, _ := wal.counts(); handedOut != 2 {
		t.Fatalf("%d entries reached the WAL, want the 2 of the request admitted whole", handedOut)
	}
}

// With no ingest gate (a standalone node) there is no gate check to skip:
// AdmitWholeRequest returns the context as AdmitRequest does, unchanged
// without a disk gate, so a line-protocol request allocates nothing for it.
func TestAdmitWholeRequestWithoutAGateLeavesTheContextUnchanged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for name, install := range map[string]func(*ArrowBuffer){
		"no gate installed": func(*ArrowBuffer) {},
		"nil gate":          func(b *ArrowBuffer) { b.SetIngestGate(nil) },
	} {
		t.Run(name, func(t *testing.T) {
			buffer, _ := newCheckpointBuffer(1000)
			defer buffer.Close()
			install(buffer)
			got, release, err := buffer.AdmitWholeRequest(ctx, 0)
			if err != nil {
				t.Fatalf("AdmitWholeRequest: %v", err)
			}
			defer release()
			if got != ctx {
				t.Fatal("AdmitWholeRequest wrapped the context with no ingest gate to skip")
			}
		})
	}
}

// pressureGate is a countingGate whose disk pressure a test turns on.
type pressureGate struct {
	countingGate
	pressure atomic.Bool
}

func (g *pressureGate) Pressure() bool { return g.pressure.Load() }

func (g *pressureGate) outstanding() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reserved
}

// A request admitted whole under a WAL disk gate carries both marks: its
// writes skip the ingest gate and the disk pressure check, and its disk
// reservation is still charged and released.
func TestAdmitWholeRequestWithADiskGate(t *testing.T) {
	buffer, wal := newCheckpointBuffer(1000)
	defer buffer.Close()
	disk := &pressureGate{}
	buffer.SetWALDiskGate(disk)
	var fenced atomic.Bool
	buffer.SetIngestGate(func() error {
		if fenced.Load() {
			return fencedGate()
		}
		return nil
	})
	ctx, release, err := buffer.AdmitWholeRequest(context.Background(), 1<<20)
	if err != nil {
		t.Fatalf("AdmitWholeRequest: %v", err)
	}
	if n := disk.outstanding(); n != 1<<20 {
		t.Fatalf("reserved %d bytes, want %d", n, 1<<20)
	}

	fenced.Store(true)
	disk.pressure.Store(true)
	if err := buffer.WriteColumnarRecord(ctx, "db", checkpointTestRecord()); err != nil {
		t.Fatalf("write after the fence closed and pressure started: %v", err)
	}
	batch := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{time.Now().UnixMicro()}, "value": []float64{1}}}
	if err := buffer.WriteTypedColumnarDirect(ctx, "db", "m", batch, 1); err != nil {
		t.Fatalf("second write after the fence closed and pressure started: %v", err)
	}
	if handedOut, _ := wal.counts(); handedOut != 2 {
		t.Fatalf("%d entries reached the WAL, want 2", handedOut)
	}
	release()
	if n := disk.outstanding(); n != 0 {
		t.Fatalf("%d bytes still reserved after release", n)
	}
}
