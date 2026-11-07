package ingest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
)

// Grace days 7-14 of an expired license: live writes are refused at
// admission with license_grace, counted once per request. The gate has its
// own slot, so the cluster replacing or clearing the ingest gate leaves it in
// place, and the per-call check CQ output uses never consults it.
func TestLicenseWriteGate(t *testing.T) {
	b := createTestArrowBuffer(t)
	var stopped atomic.Bool
	b.SetLicenseWriteGate(stopped.Load)
	if _, release, err := b.AdmitRequest(context.Background(), 10); err != nil {
		t.Fatalf("writes not stopped: %v", err)
	} else {
		release()
	}
	stopped.Store(true)
	b.SetIngestGate(nil) // the cluster clearing its gate
	before := metrics.Get().Snapshot()["license_write_refusals"]
	_, _, err := b.AdmitRequest(context.Background(), 10)
	if !errors.Is(err, ErrLicenseWritesStopped) || RefusalReason(err) != RefusalLicenseGrace || !IsWriteRefusal(err) {
		t.Fatalf("admission: %v", err)
	}
	if got := metrics.Get().Snapshot()["license_write_refusals"]; got != before.(int64)+1 {
		t.Fatalf("counted %v, want %v", got, before.(int64)+1)
	}
	// A retry already counted (MQTT) is not counted again.
	_, _, _ = b.AdmitRequest(WithRefusalCounted(context.Background()), 10)
	if got := metrics.Get().Snapshot()["license_write_refusals"]; got != before.(int64)+1 {
		t.Fatalf("a counted retry was counted again: %v", got)
	}
	if err := b.CheckIngestGate(); err != nil {
		t.Fatalf("the per-call gate (CQ output) refused: %v", err)
	}
	b.SetLicenseWriteGate(nil)
	if _, release, err := b.AdmitRequest(context.Background(), 10); err != nil {
		t.Fatalf("gate removed: %v", err)
	} else {
		release()
	}
}
