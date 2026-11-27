package api

// No build tag, and no import of internal/arcxrouter: the stock binary does
// not link the arcx router, and this file is compiled and tested by the
// default build all the same.

import (
	"context"
	"errors"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
)

// vanishedPathError is an engine error caused by a planned file removed while
// the engine read it. arcxrouter.FileVanishedError implements it (#1357); its
// Error text is vanishedPartitionMessage, and the path is for logs only.
type vanishedPathError interface {
	error
	VanishedPath() string
}

// vanishedEnginePath returns the removed file an error is about, if any,
// through any %w wrapping (drainArrowBatches wraps the reader's error).
func vanishedEnginePath(err error) (string, bool) {
	var v vanishedPathError
	if errors.As(err, &v) {
		return v.VanishedPath(), true
	}
	return "", false
}

// respondArcxMsgPackDrainError answers an arcx msgpack drain that failed
// before any header was sent. The caller has already released the batches,
// the reader and the request context.
//
//   - A planned file removed while arcx read it: 503 with Retry-After: 1 and
//     vanishedPartitionMessage, what DuckDB answers when a file it planned
//     is gone at execution (#1356). A retry plans from a fresh listing.
//   - The query deadline: 504, as DuckDB's msgpack drain answers.
//   - Anything else: 500 with the error, as before.
//
// Every branch counts a query error and gives the registry its disposition.
func (h *QueryHandler) respondArcxMsgPackDrainError(c *fiber.Ctx, derr error, timestamp string, start time.Time, onFail func(string)) error {
	m := metrics.Get()
	m.IncQueryErrors()
	if path, ok := vanishedEnginePath(derr); ok {
		if onFail != nil {
			onFail(vanishedPartitionMessage)
		}
		h.logger.Warn().Str("path", path).
			Msg("arcx serve: msgpack drain hit a file removed while the query ran; 503, the client retries")
		c.Set(fiber.HeaderRetryAfter, "1")
		return respondError(c, fiber.StatusServiceUnavailable, vanishedPartitionMessage, timestamp, start)
	}
	if errors.Is(derr, context.DeadlineExceeded) {
		m.IncQueryTimeouts()
		if onFail != nil {
			onFail("Query timed out")
		}
		h.logger.Warn().Err(derr).Msg("arcx serve: msgpack drain hit the query deadline; 504")
		return respondError(c, fiber.StatusGatewayTimeout, "Query timed out", timestamp, start)
	}
	if onFail != nil {
		onFail(derr.Error())
	}
	h.logger.Error().Err(derr).Msg("arcx serve: msgpack drain error before headers; 500")
	return respondError(c, fiber.StatusInternalServerError, clientErrText(h.logger, derr), timestamp, start)
}
