//go:build duckdb_arrow

package api

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/metrics"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/gofiber/fiber/v2"
)

// Forwarding an Arrow query from a node that cannot serve queries (#1567).
//
// POST /api/v1/query and /query/msgpack have always been forwarded from such a
// node (a compactor); the Arrow endpoint ran locally. It is now forwarded too,
// with two differences from the buffered forward of the other routes
// (CopyResponse):
//
//   - The result is streamed, not buffered, because its trailers
//     (Arc-Stream-Truncated, Arc-Rows-Capped, Arc-Execution-Time-Ms) arrive
//     only after the body, and fasthttp sends trailers only on a streamed
//     (chunked) response. The router's streaming client has no whole-response
//     timeout; the request is bounded by the query timeout instead.
//   - An upstream failure partway through the body ends this response as a
//     failed transfer (streamAbort): no terminating chunk, connection closed.
//     The Arrow endpoint's in-band truncation marker (poisonArrowStream) is
//     written only at a message boundary, and a relayed stream breaks at any
//     byte, where the marker could complete a half message and decode as a
//     clean end. A broken transfer cannot be mistaken for a complete result.
//
// Only peers that advertise cluster.CapabilityForwardArrowQuery are used; with
// none (an older reader in a rolling upgrade, no Raft) the query runs locally,
// as before.

// arrowForwardSlack is added to the query timeout to bound a forwarded Arrow
// query: the answering node enforces the query timeout itself, so the forward
// only has to outlast it by the time to deliver the end of the stream.
const arrowForwardSlack = 30 * time.Second

// arrowTrailerNames are the trailers the Arrow endpoint declares; a forward
// declares and relays exactly these.
var arrowTrailerNames = []string{arrowStreamTruncatedTrailer, arrowExecutionTimeTrailer, arrowRowsCappedTrailer}

// errForwardedArrowPanicked aborts a forwarded stream whose relay panicked.
var errForwardedArrowPanicked = errors.New("forwarded Arrow stream relay panicked")

// forwardArrowQuery forwards the request to a peer that can serve it. handled
// is false when no such peer accepts a forwarded Arrow query: the caller then
// runs the query locally.
func (h *QueryHandler) forwardArrowQuery(c *fiber.Ctx) (handled bool, err error) {
	m := metrics.Get()
	httpReq, err := BuildHTTPRequest(c)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to build HTTP request for forwarding an Arrow query")
		m.IncQueryErrors()
		return true, c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to prepare request for forwarding",
		})
	}

	// The request's context carries the forwarded identity
	// (BuildHTTPRequest); the timeout is layered on it, not substituted.
	parent := httpReq.Context()
	var ctx context.Context
	var cancel context.CancelFunc
	if h.queryTimeout > 0 {
		ctx, cancel = context.WithTimeout(parent, h.queryTimeout+arrowForwardSlack)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}

	resp, err := h.router.RouteArrowQuery(ctx, httpReq.WithContext(ctx))
	if errors.Is(err, cluster.ErrLocalNodeCanHandle) || errors.Is(err, cluster.ErrNoArrowQueryPeer) {
		cancel()
		return false, nil
	}
	if err != nil {
		cancel()
		h.logger.Error().Err(err).Msg("Failed to route an Arrow query request")
		m.IncQueryErrors()
		return true, HandleRoutingError(c, err)
	}

	// An error answer (JSON, small) is relayed as every other forward is.
	if resp.StatusCode != http.StatusOK {
		defer cancel()
		return true, CopyResponse(c, resp)
	}

	c.Status(resp.StatusCode)
	copyForwardedHeaders(c, resp)
	fctx := c.Context()
	// Declared before the body, as executeQueryArrow does; the upstream's
	// own Trailer header is hop-by-hop and not copied.
	for _, name := range arrowTrailerNames {
		if err := fctx.Response.Header.AddTrailer(name); err != nil {
			arrowTrailerWarnOnce.Do(func() {
				h.logger.Warn().Err(err).Str("trailer", name).
					Msg("Failed to register an Arrow trailer on a forwarded response; clients will not see it")
			})
		}
	}

	trailers := newResponseTrailers()
	abort := &streamAbort{}
	closeUpstream := func() {
		_ = resp.Body.Close()
		cancel()
	}
	h.setAbortableBodyStream(fctx, closeUpstream, trailers, abort, "forwarded_arrow_ipc", func() {
		abort.set(errForwardedArrowPanicked)
	}, func(w *bufio.Writer) {
		defer closeUpstream()
		if err := relayArrowBody(w, resp.Body); err != nil {
			var clientGone *clientWriteError
			if errors.As(err, &clientGone) {
				// The client left; closing the upstream body stops its query.
				return
			}
			m.IncQueryErrors()
			h.logger.Warn().Err(err).Msg("Forwarded Arrow stream failed partway; ending the response as a failed transfer")
			abort.set(fmt.Errorf("forwarded Arrow stream failed: %s", sqlutil.SanitizeErrText(err.Error())))
			return
		}
		// Upstream trailer values exist only after a clean end of body.
		for _, name := range arrowTrailerNames {
			if v := resp.Trailer.Get(name); v != "" {
				trailers.set(name, v)
			}
		}
	})
	return true, nil
}

// clientWriteError wraps a failure to write to this node's client.
type clientWriteError struct{ err error }

func (e *clientWriteError) Error() string { return "client write: " + e.err.Error() }
func (e *clientWriteError) Unwrap() error { return e.err }

// relayArrowBody copies the upstream body to w, flushing after every read so
// the stream keeps flowing (and a client that left is noticed). A read error
// other than EOF is the upstream failing; a write or flush error is wrapped in
// clientWriteError.
func relayArrowBody(w *bufio.Writer, body io.Reader) error {
	buf := make([]byte, 64<<10)
	for {
		n, rerr := body.Read(buf)
		if n > 0 {
			if _, err := w.Write(buf[:n]); err != nil {
				return &clientWriteError{err}
			}
			if err := w.Flush(); err != nil {
				return &clientWriteError{err}
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return rerr
		}
	}
}
