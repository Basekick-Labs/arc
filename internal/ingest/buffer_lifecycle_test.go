package ingest

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Every ArrowBuffer runs a once-a-second sampler that stores its own counts
// into the package-global metrics (publishBufferMetrics). Production has one
// buffer; a test that leaves one running keeps overwriting the gauges every
// later test in the package reads (#1735).

// stopAtEnd stops b's background loops when the test ends, whether or not
// the test closed it: cancelling is idempotent and safe after Close, unlike a
// second Close. Registered right after the buffer is made, it runs after the
// test's own cleanups.
func stopAtEnd(t *testing.T, b *ArrowBuffer) {
	t.Helper()
	t.Cleanup(func() {
		b.cancel()
		done := make(chan struct{})
		go func() { b.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Log("buffer goroutines still running 5s after the test ended")
		}
	})
}

// liveSamplers returns the stacks of the running buffer samplers.
func liveSamplers() []string {
	buf := make([]byte, 16<<20)
	n := runtime.Stack(buf, true)
	var out []string
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(g, "(*ArrowBuffer).metricsSampler") {
			out = append(out, g)
		}
	}
	return out
}

// describeLiveBuffers says how many buffers publish into the gauges, and
// where each was created.
func describeLiveBuffers() string {
	samplers := liveSamplers()
	var b strings.Builder
	fmt.Fprintf(&b, "%d buffer(s) publishing", len(samplers))
	for _, g := range samplers {
		for _, line := range strings.Split(g, "\n") {
			if strings.Contains(line, "_test.go:") && !strings.Contains(line, "buffer_lifecycle_test.go") {
				fmt.Fprintf(&b, "; created at %s", strings.TrimSpace(line))
				break
			}
		}
	}
	if !strings.Contains(b.String(), "created at") && len(samplers) > 0 {
		b.WriteString("; run with GODEBUG=tracebackancestors=10 to see which tests made them")
	}
	return b.String()
}

// requireSoleBuffer fails unless the buffer the test just made is the only
// one publishing into the gauges: another one, left running by an earlier
// test, overwrites them every second and the test reads its numbers, not
// this buffer's. Stopping buffers is asynchronous, and a sampler that has not
// run yet shows only its launching closure, so it waits for exactly one.
// Run with GODEBUG=tracebackancestors=10 to see which test made each one.
func requireSoleBuffer(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for len(liveSamplers()) != 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := len(liveSamplers()); n != 1 {
		t.Fatalf("this test reads the package-global buffer gauges, but %s; an earlier test left a buffer running (stop it with stopAtEnd)", describeLiveBuffers())
	}
}
