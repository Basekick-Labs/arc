package api

// #1422: a line-protocol body of several measurements is written as one
// buffer call per measurement. The ingest gate used to be checked on every
// call, so a fence closing part-way refused the request with the "nothing was
// written, retry" 503 after its first measurements were buffered, and a
// client retrying it as told duplicated them. The gate is now checked once
// for the request, as for a MessagePack batch.

import (
	"net/http"
	"sync"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/stretchr/testify/require"
)

func TestLineProtocolFenceClosingMidRequestDoesNotSplitIt(t *testing.T) {
	cases := []struct {
		name string
		cfg  *config.IngestConfig
	}{
		{"typed path", nil},
		// Decimal columns route the body through the record path.
		{"record path", &config.IngestConfig{DecimalColumns: []string{"trades:price=18,8"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newLPWriteRig(t, tc.cfg)
			// Open for the request's admission and its first measurement,
			// then closed: per-call checks refused the second measurement
			// after the first was buffered.
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

			status, body := r.post(t, "cpu,host=a value=1 1700000000000000000\nmem,host=a value=2 1700000000000000000\n")
			require.Equal(t, http.StatusNoContent, status, body)
			mu.Lock()
			require.Equal(t, 1, calls, "the ingest gate is checked once per request")
			mu.Unlock()
			rows, _ := r.stored(t)
			require.Equal(t, map[string]int64{"cpu": 1, "mem": 1}, rows)
		})
	}
}
