package ingest

import (
	"context"
	"errors"
	"testing"
)

func TestLicenseStopPreservesAdmittedBatchesAndStopsNewRequests(t *testing.T) {
	b, wal := newCheckpointBuffer(1000)
	defer b.Close()
	stopped := false
	b.SetLicenseIngestGate(func() error {
		if stopped {
			return fencedGate()
		}
		return nil
	})
	ctx := context.Background()
	whole, release, err := b.AdmitWholeRequest(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	stream, releaseStream, err := b.AdmitRequest(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseStream()
	stopped = true
	if err := b.WriteColumnarRecord(whole, "db", checkpointTestRecord()); err != nil {
		t.Fatalf("already admitted finite batch interrupted: %v", err)
	}
	before, _ := wal.counts()
	if err := b.WriteColumnarRecord(stream, "db", checkpointTestRecord()); !errors.Is(err, ErrIngestFenced) {
		t.Fatalf("stream continued after writer stop: %v", err)
	}
	if _, _, err := b.AdmitWholeRequest(ctx, 0); !errors.Is(err, ErrIngestFenced) {
		t.Fatalf("new batch admitted after writer stop: %v", err)
	}
	if after, _ := wal.counts(); after != before {
		t.Fatal("refused writes reached WAL")
	}
	stopped = false
	if _, release, err := b.AdmitWholeRequest(ctx, 0); err != nil {
		t.Fatalf("renewal did not reopen admission: %v", err)
	} else {
		release()
	}
}
