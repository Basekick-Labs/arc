package ingest

import (
	"errors"
	"testing"
)

func TestLicenseGateNilContextStillFences(t *testing.T) {
	b := &ArrowBuffer{}
	b.SetLicenseIngestGate(func() error { return ErrIngestFenced })
	if err := b.Write(nil, "db", []interface{}{struct{}{}}); !errors.Is(err, ErrIngestFenced) {
		t.Fatalf("nil context bypassed license fence: %v", err)
	}
	b.SetLicenseIngestGate(nil)
	if err := b.Write(nil, "db", []interface{}{struct{}{}}); err == nil {
		t.Fatal("unwired payload lost its existing dispatch error")
	}
}
