package api

// #1759 (b): with a forcer set, a visible write flushes its measurement early
// instead of waiting for the age flush.

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/stretchr/testify/require"
)

func TestWaitVisibleForcesThroughSetForcer1759(t *testing.T) {
	old := waitVisibleBudget
	waitVisibleBudget = 5 * time.Second
	t.Cleanup(func() { waitVisibleBudget = old })

	// No age flush within the test (600 s): only a forced flush answers 204.
	r := newVisRig(t, visOpts{})
	r.buf.EnableKeyFlushLedger(ingest.ForceBudget{})
	r.wv.SetForcer(r.buf, time.Second)

	now := time.Now().UTC()
	for _, tc := range []struct{ name, path string }{
		{"msgpack", "/api/v1/write/msgpack"},
		{"line protocol", "/api/v1/write/line-protocol"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := mpCPU(t, now)
			if tc.path == "/api/v1/write/line-protocol" {
				body = lpCPU(now)
				time.Sleep(time.Second) // the msgpack case's force: d/cpu is due again
			}
			start := time.Now()
			resp, out := r.post(t, tc.path, body, WaitVisibleHeader, "visible")
			require.Equal(t, http.StatusNoContent, resp.StatusCode, out)
			v, reason := visible(resp.Header)
			require.Equal(t, "true", v, reason)
			require.Less(t, time.Since(start), 3*time.Second, "answered by the forced flush, not the age")
		})
	}
	require.Equal(t, int64(2), r.count(t, "SELECT COUNT(*) FROM d.cpu"))

	// Without the forcer the same write waits out its budget.
	waitVisibleBudget = 500 * time.Millisecond
	r.wv.SetForcer(nil, 0)
	resp, out := r.post(t, "/api/v1/write/msgpack", mpCPU(t, now), WaitVisibleHeader, "visible")
	require.Equal(t, http.StatusAccepted, resp.StatusCode, out)
	_, reason := visible(resp.Header)
	require.Equal(t, string(ingest.VisibleReasonTimeout), reason)

	var nilWV *WaitVisible
	nilWV.SetForcer(r.buf, time.Second) // a no-op, no panic
}

// The first visible write to an idle measurement (never flushed) is forced on
// its first ask and answered within a flush time, not an interval or an age:
// the real buffer, local storage, line protocol through the handler.
func TestWaitVisibleFirstWriteToAnIdleMeasurementAnswersAtOnce1759(t *testing.T) {
	r := newVisRig(t, visOpts{ageMS: 25000})
	r.buf.EnableKeyFlushLedger(ingest.ForceBudget{})
	r.wv.SetForcer(r.buf, 5*time.Second)
	for _, m := range []string{"idle1", "idle2", "idle3"} {
		body := []byte(m + ",host=h v=1i " + strconv.FormatInt(time.Now().UnixNano(), 10) + "\n")
		start := time.Now()
		resp, out := r.post(t, "/api/v1/write/line-protocol", body, WaitVisibleHeader, "visible")
		took := time.Since(start)
		require.Equal(t, http.StatusNoContent, resp.StatusCode, out)
		v, reason := visible(resp.Header)
		require.Equal(t, "true", v, reason)
		// 3 s, not a flush time: a wall-clock bound in the race job must leave
		// room for a loaded runner, and still sits below the 5 s interval and
		// the 25 s age a write that was not forced at its first ask would wait.
		require.Less(t, took, 3*time.Second, "%s: the first visible write to an idle measurement took %v; it is forced at its first ask", m, took)
	}
}
