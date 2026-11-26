package api

import (
	"bytes"

	"github.com/gofiber/fiber/v2"
)

// A query carrying "x-arc-freshness: flush" asks for an answer that includes
// every row this node acknowledged before the request arrived (#1760). This
// build recognises the header and refuses it on every query route, before
// anything is forwarded, parsed or run: the feature is not available yet. The
// refusal is the one a node with the feature off will give, so a client that
// handles it now keeps working when the feature arrives.
//
//   - any value other than "flush" (case-insensitive, trimmed): 400,
//     X-Arc-Freshness-Reason: invalid_value;
//   - "flush": 403, X-Arc-Freshness-Reason: disabled.
//
// Both carry X-Arc-Freshness: false. A request without the header is
// untouched; the absence of X-Arc-Freshness never means fresh. The check runs
// in the handler, so the route's auth middleware has already admitted the
// request: an unauthenticated client cannot probe for the feature.

const (
	// FreshnessHeader is the request header; its one value is "flush".
	FreshnessHeader = "x-arc-freshness"
	// FreshnessAnswerHeader and FreshnessReasonHeader carry the answer.
	FreshnessAnswerHeader = "X-Arc-Freshness"
	FreshnessReasonHeader = "X-Arc-Freshness-Reason"

	freshnessReasonInvalidValue = "invalid_value"
	freshnessReasonDisabled     = "disabled"
)

// freshnessRefusal reads x-arc-freshness. Without it, status is 0 and nothing
// is touched. With it, the answer headers are set and status and msg are the
// refusal the caller writes in its route's error shape.
//
// Every value of a repeated header is read, as on Flight SQL: empty (or
// whitespace-only) values count as absent; any other value than "flush" is
// 400; otherwise 403. A request without the header pays one Peek.
func freshnessRefusal(c *fiber.Ctx) (status int, msg string) {
	h := &c.Request().Header
	if h.Peek(FreshnessHeader) == nil {
		return 0, "" // no such header line at all
	}
	present, invalid := false, false
	for _, v := range h.PeekAll(FreshnessHeader) {
		v = bytes.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		present = true
		if !bytes.EqualFold(v, []byte("flush")) {
			invalid = true
		}
	}
	if !present {
		return 0, ""
	}
	c.Set(FreshnessAnswerHeader, "false")
	if invalid {
		c.Set(FreshnessReasonHeader, freshnessReasonInvalidValue)
		return fiber.StatusBadRequest, `invalid x-arc-freshness value: the only value is "flush"; the query was not run`
	}
	c.Set(FreshnessReasonHeader, freshnessReasonDisabled)
	return fiber.StatusForbidden, "x-arc-freshness is not available on this server; the query was not run"
}
