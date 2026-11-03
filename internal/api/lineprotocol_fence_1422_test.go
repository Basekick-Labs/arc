package api

// #1422: a line-protocol body of several measurements is written as one
// buffer call per measurement. The ingest gate (the fence and stream-takeover
// pauses) used to be checked on every call, so a gate closing part-way
// stopped the request after its first measurements were buffered, and a
// client resending it duplicated them. The gate is now checked once for the
// request, as for a MessagePack batch.

import (
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// lpFenceRequest is two measurements: two buffer calls on either path.
const lpFenceRequest = "cpu,host=a value=1 1700000000000000000\nmem,host=a value=2 1700000000000000000\n"

func TestLineProtocolFenceClosingMidRequestDoesNotSplitIt(t *testing.T) {
	for _, path := range lpPaths {
		t.Run(path.name, func(t *testing.T) {
			r := newLPWriteRig(t, path.cfg())
			// Open for the request's admission and its first measurement,
			// then closed. A per-call check that comes back passes the
			// first measurement and refuses the second: the split this
			// test is for, so do not lower the 2.
			var mu sync.Mutex
			calls := 0
			r.buf.SetIngestGate(func() error {
				mu.Lock()
				defer mu.Unlock()
				calls++
				if calls <= 2 {
					return nil
				}
				return fencedErr(t)
			})

			status, body := r.post(t, lpFenceRequest)
			require.Equal(t, http.StatusNoContent, status, body)
			mu.Lock()
			require.Equal(t, 1, calls, "the ingest gate is checked once per request")
			mu.Unlock()
			rows, _ := r.stored(t)
			require.Equal(t, map[string]int64{"cpu": 1, "mem": 1}, rows)
		})
	}
}

// A gate already closed refuses the whole request at admission, on both
// paths: the fenced 503, and nothing written.
func TestLineProtocolFenceClosedRefusesTheRequestUpFront(t *testing.T) {
	for _, path := range lpPaths {
		t.Run(path.name, func(t *testing.T) {
			r := newLPWriteRig(t, path.cfg())
			var mu sync.Mutex
			calls := 0
			r.buf.SetIngestGate(func() error {
				mu.Lock()
				defer mu.Unlock()
				calls++
				return fencedErr(t)
			})

			a := postLP(t, r, lpFenceRequest)
			require.Equal(t, http.StatusServiceUnavailable, a.status, a.rawBody)
			require.NotEmpty(t, a.header.Get(IngestFencedHeader))
			mu.Lock()
			require.Equal(t, 1, calls, "refused at admission, before any measurement")
			mu.Unlock()
			rows, _ := r.stored(t)
			require.Empty(t, rows)
		})
	}
}
