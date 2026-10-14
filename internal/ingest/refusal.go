package ingest

import (
	"errors"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/logger"
	"github.com/rs/zerolog"
)

// Reasons a live write is refused (#1268): the label of
// arc_cluster_ingest_fenced_writes_total and the key refusals are logged by.
const (
	RefusalContact       = "contact"        // this writer lost steady contact with the Raft leader
	RefusalStandby       = "standby"        // another writer is the designated primary
	RefusalClaimed       = "claimed"        // a stream takeover claimed this writer process
	RefusalAnnouncing    = "announcing"     // announcing this writer instance (stream takeover)
	RefusalNotDesignated = "not_designated" // stream takeover: not the designated primary
	RefusalDeleteAck     = "delete_ack"     // acknowledging a DELETE record
	RefusalOther         = "other"          // an ErrIngestFenced without a reason
)

// RefusalError is the reason inside an ErrIngestFenced error: the cluster gate
// wraps one, so callers can count and log refusals by reason.
type RefusalError struct {
	Reason string
	Text   string
}

func (e *RefusalError) Error() string { return e.Text }

// RefusalReason returns the reason of a refused live write: a Refusal
// constant, RefusalOther for an ErrIngestFenced without one, and "" for an
// error that is not an ingest refusal.
func RefusalReason(err error) string {
	var r *RefusalError
	if errors.As(err, &r) {
		return r.Reason
	}
	if errors.Is(err, ErrIngestFenced) {
		return RefusalOther
	}
	return ""
}

// RefusalLogEvery is how often a refusal reason is logged again while writes
// keep being refused for it (#1268).
const RefusalLogEvery = 30 * time.Second

// refusalLog throttles the ingest refusal log per reason: the first refusal of
// a reason not logged within RefusalLogEvery logs at once, later ones are
// counted and reported on the next line for that reason. Per reason, so a
// standby's routine refusals never hide a primary that fences itself. During
// a failover every client write is refused; one line per request rotated away
// the logs that explain the failover.
type refusalLog struct {
	mu     sync.Mutex
	now    func() time.Time
	last   map[string]time.Time
	quiet  map[string]int64
	logger func() zerolog.Logger
}

func newRefusalLog(now func() time.Time, lg func() zerolog.Logger) *refusalLog {
	return &refusalLog{now: now, last: map[string]time.Time{}, quiet: map[string]int64{}, logger: lg}
}

var (
	refusalsMu sync.Mutex
	refusals   = newRefusalLog(time.Now, func() zerolog.Logger { return logger.Get("ingest") })
)

// LogRefusal records one refused write. source names where it arrived (an
// HTTP path, an MQTT topic, "forwarded").
func LogRefusal(source string, err error) {
	refusalsMu.Lock()
	l := refusals
	refusalsMu.Unlock()
	l.record(RefusalReason(err), source, err)
}

// CaptureRefusalLog points the refusal log at lg and the clock at now until
// the returned restore runs. For tests.
func CaptureRefusalLog(lg zerolog.Logger, now func() time.Time) (restore func()) {
	refusalsMu.Lock()
	prev := refusals
	refusals = newRefusalLog(now, func() zerolog.Logger { return lg })
	refusalsMu.Unlock()
	return func() {
		refusalsMu.Lock()
		refusals = prev
		refusalsMu.Unlock()
	}
}

func (l *refusalLog) record(reason, source string, err error) {
	l.mu.Lock()
	now := l.now()
	if last, ok := l.last[reason]; ok && now.Sub(last) < RefusalLogEvery {
		l.quiet[reason]++
		l.mu.Unlock()
		return
	}
	suppressed := l.quiet[reason]
	l.last[reason] = now
	l.quiet[reason] = 0
	l.mu.Unlock()
	lg := l.logger()
	lg.Warn().
		Str("reason", reason).
		Str("source", source).
		Int64("suppressed", suppressed).
		Err(err).
		Msg("Writes refused on this node (retryable); logged once per reason every 30s, see arc_cluster_ingest_fenced_writes_total")
}
