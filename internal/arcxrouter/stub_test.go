// Stock-build isolation guarantee: without the arcx_engine tag, Decide must always
// decline and Run must never handle, so the handleQuery hook is inert and stock
// Arc behaves exactly as before. This test only compiles/runs in the stub build.

//go:build !cgo || !arcx_engine

package arcxrouter

import (
	"context"
	"testing"
	"time"
)

func TestStubDecideAlwaysDeclines(t *testing.T) {
	// Even for a query that WOULD be eligible in the tagged build, the stub
	// declines — the whole point of the isolation seam.
	d := Decide("SELECT count(*) FROM cpu", "prod", nil)
	if d.Eligible {
		t.Fatal("stub Decide returned Eligible=true; stock Arc must be inert")
	}
}

func TestStubRunArrowNeverServes(t *testing.T) {
	// RunArrow was added to the stub in the signature-drift fix; pin it here so the
	// untagged build keeps a compile-time check on it (the drift this test file itself
	// failed to catch was that nothing referenced the stub's signature).
	if r, served := RunArrow(context.Background(), Decision{Eligible: true}, nil, ModeServe); r != nil || served {
		t.Fatal("stub RunArrow served; stock Arc must fall through to DuckDB")
	}
}

func TestStubRunNeverHandles(t *testing.T) {
	if Run(nil, Decision{Eligible: true}, nil, ModeServe, time.Now()) {
		t.Fatal("stub Run returned handled=true; stock Arc must fall through to DuckDB")
	}
}
