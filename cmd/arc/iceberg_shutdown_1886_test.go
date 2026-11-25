package main

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/rs/zerolog"
)

// fakeDrainer records what the shutdown steps do to the scheduler.
type fakeDrainer struct {
	mu        sync.Mutex
	order     *[]string
	stopOK    bool
	queued    []string // files reported to the export
	drained   []string // files the drain exported
	drainDead time.Time
}

func (f *fakeDrainer) StopWithin(time.Duration) bool {
	*f.order = append(*f.order, "iceberg-stop")
	return f.stopOK
}

func (f *fakeDrainer) DrainStopped(ctx context.Context) int {
	*f.order = append(*f.order, "iceberg-drain")
	f.drainDead, _ = ctx.Deadline()
	f.drained = append(f.drained, f.queued...)
	return len(f.queued)
}

// #1886: the export's loop stops before the buffer's final flush (a hook),
// and the files that flush writes are exported after the WAL closes, then
// Glue syncs, then the export's SQLite handle closes, all while the cluster
// coordinator (the export's gate), auth (a borrowed handle) and storage are
// still open. Registered through the helpers main.go uses, so a priority
// change there fails this test.
func TestIcebergShutdownExportsTheFinalFlush(t *testing.T) {
	const timeout = 3 * time.Second
	coordinator := shutdown.New(timeout, zerolog.Nop())
	var order []string
	d := &fakeDrainer{order: &order, stopOK: true}

	coordinator.Register("arrow-buffer", shutdownFunc(func() error {
		order = append(order, "arrow-buffer")
		d.queued = append(d.queued, "final.parquet") // SetOnFileRegistered → RequestIngestAppend
		return nil
	}), shutdown.PriorityBuffer)
	registerFileRegistrarShutdown(coordinator, func() { order = append(order, "file-registrar") })
	coordinator.Register("wal", shutdownFunc(func() error { order = append(order, "wal"); return nil }), shutdown.PriorityWAL)
	registerClusterCoordinatorShutdown(coordinator, func() error { order = append(order, "cluster-coordinator"); return nil })
	registerTieringShutdown(coordinator, func() error { order = append(order, "tiering"); return nil })
	coordinator.Register("auth", shutdownFunc(func() error { order = append(order, "auth"); return nil }), shutdown.PriorityAuth)
	coordinator.Register("storage", shutdownFunc(func() error { order = append(order, "storage"); return nil }), shutdown.PriorityStorage)

	ice := &icebergShutdown{sched: d, timeout: timeout, glue: true, logger: zerolog.Nop()}
	registerIcebergShutdown(coordinator, ice, func() error { order = append(order, "iceberg-catalog-db"); return nil })
	registerIcebergGlueShutdown(coordinator, &icebergGlueShutdown{timeout: timeout, flush: func(context.Context) {
		order = append(order, "iceberg-glue")
	}})

	start := time.Now()
	if err := coordinator.Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	want := []string{"iceberg-stop", "arrow-buffer", "file-registrar", "wal", "iceberg-drain", "iceberg-glue",
		"iceberg-catalog-db", "cluster-coordinator", "tiering", "auth", "storage"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("shutdown order\n got %v\nwant %v", order, want)
	}
	if !reflect.DeepEqual(d.drained, []string{"final.parquet"}) {
		t.Fatalf("drain exported %v, want the final flush's file", d.drained)
	}
	// The drain ends a tenth plus a twentieth of the budget before the
	// shutdown's deadline: the closes after it and Glue keep their time.
	wantEnd := start.Add(timeout - icebergDrainMargin(timeout, true))
	if diff := d.drainDead.Sub(wantEnd); diff < -100*time.Millisecond || diff > 100*time.Millisecond {
		t.Fatalf("drain deadline %v after start, want about %v", d.drainDead.Sub(start), wantEnd.Sub(start))
	}
}

// When the loop did not stop in time it may still hold the scheduler's state
// and the catalog handle: no drain, no close.
func TestIcebergShutdownSkipsWhenTheLoopDidNotStop(t *testing.T) {
	coordinator := shutdown.New(time.Second, zerolog.Nop())
	var order []string
	d := &fakeDrainer{order: &order, stopOK: false}
	registerIcebergShutdown(coordinator, &icebergShutdown{sched: d, timeout: time.Second, logger: zerolog.Nop()},
		func() error { order = append(order, "iceberg-catalog-db"); return nil })
	if err := coordinator.Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if !reflect.DeepEqual(order, []string{"iceberg-stop"}) {
		t.Fatalf("order = %v, want only the stop", order)
	}
}

// The drain's margins at a small, the default and a large
// server.shutdown_timeout.
func TestIcebergShutdownMargins(t *testing.T) {
	for _, tc := range []struct {
		timeout          time.Duration
		noGlue, withGlue time.Duration
	}{
		{5 * time.Second, 500 * time.Millisecond, 750 * time.Millisecond},
		{30 * time.Second, 3 * time.Second, 4500 * time.Millisecond},
		{120 * time.Second, 12 * time.Second, 18 * time.Second},
	} {
		if got := icebergDrainMargin(tc.timeout, false); got != tc.noGlue {
			t.Errorf("timeout %v without Glue: margin %v, want %v", tc.timeout, got, tc.noGlue)
		}
		if got := icebergDrainMargin(tc.timeout, true); got != tc.withGlue {
			t.Errorf("timeout %v with Glue: margin %v, want %v", tc.timeout, got, tc.withGlue)
		}
	}
}

// With no shutdown time left the drain does not start.
func TestIcebergShutdownNoTimeLeft(t *testing.T) {
	var order []string
	d := &fakeDrainer{order: &order, stopOK: true}
	s := &icebergShutdown{sched: d, timeout: 10 * time.Second, logger: zerolog.Nop(), stopped: true}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond) // less than the 1 s margin
	defer cancel()
	if err := s.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	if len(order) != 0 {
		t.Fatalf("drain ran with no time left: %v", order)
	}
}
