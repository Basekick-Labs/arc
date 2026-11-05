package api

import (
	"fmt"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// queryStreamOutcome records exactly one terminal outcome, including a panic
// before normal disposition. A panic in logging or cleanup after disposition
// must not turn one execution into two outcomes.
type queryStreamOutcome struct {
	once   sync.Once
	m      *metrics.Metrics
	engine string
	start  time.Time
}

func newQueryStreamOutcome(engine string, start time.Time) *queryStreamOutcome {
	return &queryStreamOutcome{m: metrics.Get(), engine: engine, start: start}
}

func (o *queryStreamOutcome) fail() {
	o.once.Do(func() { o.m.RecordQueryFailure(o.engine, time.Since(o.start).Microseconds()) })
}

func (o *queryStreamOutcome) succeed(rows int64) {
	o.once.Do(func() {
		o.m.IncQuerySuccess()
		o.m.IncQueryRows(rows)
		o.m.RecordQueryLatencyForEngine(o.engine, time.Since(o.start).Microseconds())
	})
}

// firstStreamWriteError preserves an earlier engine/encoding error while
// classifying a final response write failure as a client disconnect.
func firstStreamWriteError(current, writeErr error) error {
	if current != nil {
		return current
	}
	if writeErr != nil {
		return fmt.Errorf("%w: %w", errClientDisconnected, writeErr)
	}
	return nil
}
