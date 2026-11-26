package api

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	flightSQLDecimalsHeader = "x-arc-decimals"
	decimalModeDefault      = "default"
	decimalModeExact        = "exact"
)

func parseDecimalMode(value string) (string, error) {
	switch value {
	case decimalModeExact, decimalModeDefault:
		return value, nil
	default:
		return "", fmt.Errorf("invalid x-arc-decimals header %q: use %q or %q", value, decimalModeExact, decimalModeDefault)
	}
}

// requestDecimalMode reads the HTTP form of x-arc-decimals. An absent or
// empty header preserves the historical response types.
func requestDecimalMode(c *fiber.Ctx) (string, error) {
	value := strings.TrimSpace(c.Get(flightSQLDecimalsHeader))
	if value == "" {
		return decimalModeDefault, nil
	}
	return parseDecimalMode(value)
}

// flightSQLDecimalMode reads x-arc-decimals from an incoming Flight SQL call.
// present distinguishes an omitted header from an explicit "default" so the
// bridge preserves the client's request exactly.
func flightSQLDecimalMode(ctx context.Context) (mode string, present bool, err error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get(flightSQLDecimalsHeader)
	if len(values) == 0 {
		return decimalModeDefault, false, nil
	}
	if len(values) > 1 {
		return "", true, status.Error(codes.InvalidArgument, "the x-arc-decimals header was sent more than once")
	}
	mode, err = parseDecimalMode(values[0])
	if err != nil {
		return "", true, status.Error(codes.InvalidArgument, err.Error())
	}
	return mode, true, nil
}

// flightStatementDecimalMode checks that the mode used for a Flight SQL
// statement is unchanged from its prepare or GetFlightInfo call.
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
