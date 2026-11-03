package api

import (
	"fmt"
	"strings"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
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
		} else if d.Measurement != "" {
			fmt.Fprintf(&b, " %d in %q: %s;", d.Points, d.Measurement, d.Reason)
		} else {
			fmt.Fprintf(&b, " %d: %s;", d.Points, d.Reason)
		}
	}
	if wroteRest {
		b.WriteString(" the other points were written")
	}
	return b.String()
}

// respondWriteStopped answers a multi-measurement write that failed after
// part of it was buffered (#1437): 500, with what was written, and without the
// refusal headers or Retry-After, which promise that nothing was. The fields
// are the ones an import that stops part-way answers with (#1070).
//
// Not 503: 503 with Retry-After is Arc's "nothing written, resend" answer.
// Not 400: InfluxDB clients and Telegraf drop a 4xx batch for good, losing the
// measurements that were not written, while a resend's duplicates of the
// written ones are collapsed by compaction when they are identical rows
// (timestamped lines). Clients that resend any 5xx still duplicate the written
// part until then; the answer says so, so a client that reads it can resend
// only the rest.
func respondWriteStopped(c *fiber.Ctx, logger zerolog.Logger, protocol, database string, ws *ingest.WriteStoppedError) error {
	metrics.Get().IncIngestErrors()
	logger.Error().Err(ws.Err).
		Str("database", database).
		Str("stopped_at", ws.StoppedAt).
		Int64("rows_written", ws.RowsWritten).
		Strs("written", ws.Written).
		Msg(protocol + " write stopped part-way: the measurements before it are stored")
	body := fiber.Map{
		"error":                  writeStoppedMessage(ws),
		"rows_written":           ws.RowsWritten,
		"written_measurements":   ws.Written,
		"stopped_at_measurement": ws.StoppedAt,
	}
	// Points left out for good (their content, or rows that could not be
	// converted): a client resending the unwritten part must not resend
	// these as if they were merely not reached.
	if n := droppedPoints(ws.Drops); n > 0 {
		body["points_dropped"] = n
		body["dropped"] = partialWriteMessage(ws.Drops, n, false)
	}
	return c.Status(fiber.StatusInternalServerError).JSON(body)
}

func droppedPoints(drops []ingest.LPDrop) int {
	n := 0
	for _, d := range drops {
		n += d.Points
	}
	return n
}

// writeStoppedMessage is the error text of respondWriteStopped. It never says
// "partial write" first: clients read that phrase as InfluxDB's 400, points
// dropped for their content.
func writeStoppedMessage(ws *ingest.WriteStoppedError) string {
	return ws.Error() + ". Resending the whole request writes the written rows again"
}
