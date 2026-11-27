package api

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

func TestQueryStreamOutcomePanicAccounting(t *testing.T) {
	for _, terminal := range []string{"none", "success", "error"} {
		t.Run(terminal, func(t *testing.T) {
			metrics.Init(zerolog.Nop())
			o := newQueryStreamOutcome(metrics.QueryEngineDuckDB, time.Now())
			o.m = &metrics.Metrics{}
			h := &QueryHandler{logger: zerolog.Nop()}
			cleanup := 0
			h.safeStream("test", func() { cleanup++ }, func(*bufio.Writer) {
				defer func() { cleanup++ }()
				if terminal == "success" {
					o.succeed(3)
				} else if terminal == "error" {
					o.fail()
				}
				panic("after optional terminal outcome")
			}, o.fail)(bufio.NewWriter(&bytes.Buffer{}))
			s := o.m.Snapshot()
			e := s["query_engine_executions"].(map[string]map[string]int64)[metrics.QueryEngineDuckDB]
			successes := int64(0)
			if terminal == "success" {
				successes = 1
			}
			if e["success"] != successes || e["error"] != 1-successes || s["query_errors_total"].(int64) != 1-successes || s["query_success_total"].(int64) != successes {
				t.Fatalf("duplicate or missing outcome: %v", s)
			}
			if cleanup != 2 {
				t.Fatalf("defer and onPanic must each run once: %d", cleanup)
			}
		})
	}
}

func TestQueryStreamOutcomeConcurrentTerminal(t *testing.T) {
	metrics.Init(zerolog.Nop())
	o := newQueryStreamOutcome(metrics.QueryEngineDuckDB, time.Now())
	o.m = &metrics.Metrics{}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				o.fail()
			} else {
				o.succeed(1)
			}
		}(i)
	}
	wg.Wait()
	s := o.m.Snapshot()
	e := s["query_engine_executions"].(map[string]map[string]int64)[metrics.QueryEngineDuckDB]
	if e["success"]+e["error"] != 1 || s["query_success_total"].(int64)+s["query_errors_total"].(int64) != 1 {
		t.Fatalf("terminal recorded more than once: %v", s)
	}
}

func TestFirstStreamWriteError(t *testing.T) {
	original := errors.New("engine failure")
	if got := firstStreamWriteError(original, io.ErrClosedPipe); got != original {
		t.Fatal("first engine error lost")
	}
	got := firstStreamWriteError(nil, io.ErrClosedPipe)
	if !errors.Is(got, io.ErrClosedPipe) || !isClientError(got) {
		t.Fatalf("lost write error/disconnect classification: %v", got)
	}
}

func TestSafeStreamDefaultPanicMetricUnchanged(t *testing.T) {
	h := &QueryHandler{logger: zerolog.Nop()}
	before := metrics.Get().Snapshot()["query_errors_total"].(int64)
	h.safeStream("ordinary", nil, func(*bufio.Writer) { panic("ordinary stream") })(bufio.NewWriter(&bytes.Buffer{}))
	if got := metrics.Get().Snapshot()["query_errors_total"].(int64); got != before+1 {
		t.Fatalf("default panic metric=%d, want %d", got, before+1)
	}
}
