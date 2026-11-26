package api

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// decimalsHeader is x-arc-decimals, the HTTP request header and the
	// Flight SQL call header alike (#1784).
	decimalsHeader     = "x-arc-decimals"
	decimalModeDefault = "default"
	decimalModeExact   = "exact"
)

// errDecimalsRepeated is the refusal of a header sent more than once.
var errDecimalsRepeated = errors.New("the x-arc-decimals header was sent more than once")

// decimalModeOf is the one x-arc-decimals rule, for HTTP and Flight SQL, over
// every value a request carried. Each value is trimmed and an empty one is
// dropped. None left is the header absent: default, present false. More than
// one is refused, identical values included: the mode decides the column
// types, so a request must name it once (ADBC joins the database- and
// connection-level call headers, so a client sets it at one level). The one
// value left must be exactly "exact" or "default". The result is one of the
// constants, never the request's own string, which on HTTP is a copy of
// fasthttp's buffer.
func decimalModeOf(values []string) (mode string, present bool, err error) {
	value := ""
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if present {
			return "", true, errDecimalsRepeated
		}
		value, present = v, true
	}
	switch {
	case !present:
		return decimalModeDefault, false, nil
	case value == decimalModeExact:
		return decimalModeExact, true, nil
	case value == decimalModeDefault:
		return decimalModeDefault, true, nil
	default:
		return "", true, fmt.Errorf("invalid x-arc-decimals header %q: use %q or %q", value, decimalModeExact, decimalModeDefault)
	}
}

// requestDecimalMode reads the HTTP form of x-arc-decimals (decimalModeOf).
// Without the header it costs one Peek; the values are copied out of
// fasthttp's scratch slice only when it is there.
func requestDecimalMode(c *fiber.Ctx) (string, error) {
	hd := &c.Request().Header
	if hd.Peek(decimalsHeader) == nil {
		return decimalModeDefault, nil
	}
	all := hd.PeekAll(decimalsHeader)
	values := make([]string, len(all))
	for i, v := range all {
		values[i] = string(v)
	}
	mode, _, err := decimalModeOf(values)
	return mode, err
}

// flightSQLDecimalMode reads x-arc-decimals from an incoming Flight SQL call
// (decimalModeOf). present distinguishes an omitted header from an explicit
// "default", so the bridge forwards what the client sent.
func flightSQLDecimalMode(ctx context.Context) (mode string, present bool, err error) {
	md, _ := metadata.FromIncomingContext(ctx)
	mode, present, err = decimalModeOf(md.Get(decimalsHeader))
	if err != nil {
		return "", true, status.Error(codes.InvalidArgument, err.Error())
	}
	return mode, present, nil
}

// flightStatementDecimalMode checks that the mode used for a Flight SQL
// statement is unchanged from its prepare or GetFlightInfo call. A call
// without the header is default, so it is refused for a statement prepared
// with exact.
func flightStatementDecimalMode(ctx context.Context, stored string) (string, error) {
	if stored == "" {
		stored = decimalModeDefault
	}
	if stored != decimalModeDefault && stored != decimalModeExact {
		return "", status.Error(codes.InvalidArgument, "invalid decimal mode in statement handle")
	}
	current, _, err := flightSQLDecimalMode(ctx)
	if err != nil {
		return "", err
	}
	if current != stored {
		return "", status.Error(codes.InvalidArgument, "the x-arc-decimals header differs from the mode the statement was prepared with")
	}
	return current, nil
}
