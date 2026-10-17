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
	// RefusalForwardUnreachable: a standby writer could not reach the primary
	// it forwards writes to (#1270); counted in
	// arc_cluster_writes_forwarded_total, not in the fenced-writes counter.
	RefusalForwardUnreachable = "forward_unreachable"
	// RefusalWALDisk: the WAL volume is low on free space, or the WAL
	// directory is at wal.max_directory_mb (#676); counted in
	// arc_wal_disk_pressure_rejections_total.
	RefusalWALDisk = "wal_disk"
)

// WriteRefusal is a live write this node refuses for a local, transient
// condition: nothing was written, the WAL included, and the client should
// retry (#676). Unlike ErrIngestFenced, retrying against this same node is
// right. Reason is a Refusal constant: the HTTP answer carries it in the
// X-Arc-Ingest-Refused header, and refusals are logged by it. Compare with
// errors.Is against the package's Err values (same reason matches).
type WriteRefusal struct {
	Reason string
	Text   string
}

func (e *WriteRefusal) Error() string { return e.Text }

// Is matches another WriteRefusal with the same reason.
func (e *WriteRefusal) Is(target error) bool {
	t, ok := target.(*WriteRefusal)
	return ok && t.Reason == e.Reason
}

// ErrWALDiskPressure refuses a live write while the WAL volume is under disk
// pressure (#676). Retryable: the gate clears once free space returns.
var ErrWALDiskPressure error = &WriteRefusal{
	Reason: RefusalWALDisk,
	Text:   "write refused: the WAL volume is low on free space (WAL volume disk pressure); nothing was written, retry",
}

// IsWriteRefusal reports any retryable refusal of a live write: a fence
// (ErrIngestFenced) or a WriteRefusal. Every place that treats a refused
// write as expected rather than as an ingest error tests this, so a new
// refusal reason needs no sweep of the call sites.
func IsWriteRefusal(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrIngestFenced) {
		return true
	}
	var r *WriteRefusal
	return errors.As(err, &r)
}

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
	var w *WriteRefusal
	if errors.As(err, &w) {
		return w.Reason
	}
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
		Msg(refusalLogMessage(reason))
}

// refusalLogMessage names the metric that counts refusals of reason.
func refusalLogMessage(reason string) string {
	if reason == RefusalWALDisk || reason == "forwarded_"+RefusalWALDisk {
		return "Writes refused on this node (retryable): WAL volume disk pressure; logged once per reason every 30s, see arc_wal_disk_pressure and arc_wal_disk_pressure_rejections_total"
	}
	return "Writes refused on this node (retryable); logged once per reason every 30s, see arc_cluster_ingest_fenced_writes_total"
}
