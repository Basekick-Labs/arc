package api

import (
	"fmt"
	"strings"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/rs/zerolog"
)

// Points a write drops (line protocol, msgpack row format; see ingest.LPDrop)
// are answered with InfluxDB's "partial write" and logged loudly.

// maxDropLogLines bounds the Error lines one request's drops log: a body of
// many measurements could otherwise log a line per measurement.
const maxDropLogLines = 10

// logPointDrops logs a write's dropped points loudly (they are data a client
// sent and Arc did not store): one Error line per measurement, column and
// reason, up to maxDropLogLines, then one line counting the rest. Called once
// the request's outcome is known, so a refused request logs no drops.
// protocol names the format in the message ("Line protocol", "MessagePack").
func logPointDrops(logger zerolog.Logger, protocol, database string, drops []ingest.LPDrop) {
	for i, d := range drops {
		if i == maxDropLogLines {
			rest := 0
			for _, r := range drops[i:] {
				rest += r.Points
			}
			logger.Error().
				Str("database", database).
				Int("points_dropped", rest).
				Int("reasons", len(drops)-i).
				Msg(protocol + " points dropped (partial write): more measurements and columns")
			return
		}
		ev := logger.Error().
			Str("database", database).
			Str("measurement", d.Measurement).
			Int("points_dropped", d.Points).
			Str("reason", d.Reason)
		if d.Column != "" {
			ev = ev.Str("column", ingest.QuoteColumnName(d.Column))
		}
		if d.Reason == ingest.LPDropKind {
			ev = ev.Str("column_type", d.Kind.String()).Str("value_type", d.ValueType)
		}
		ev.Msg(protocol + " points dropped (partial write)")
	}
}

// partialWriteMessage is InfluxDB's "partial write" answer: how many points
// were dropped and why.
func partialWriteMessage(drops []ingest.LPDrop, dropped int, wroteRest bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "partial write: %d points dropped:", dropped)
	for i, d := range drops {
		if i == 5 {
			fmt.Fprintf(&b, " (and %d more)", len(drops)-i)
			break
		}
		if d.Column != "" {
			fmt.Fprintf(&b, " %d in %q column %q: %s;", d.Points, d.Measurement, ingest.TruncateColumnName(d.Column), d.Reason)
		} else {
			fmt.Fprintf(&b, " %d in %q: %s;", d.Points, d.Measurement, d.Reason)
		}
	}
	if wroteRest {
		b.WriteString(" the other points were written")
	}
	return b.String()
}
