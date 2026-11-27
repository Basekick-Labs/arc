package api

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// decimalOverflowPrefix starts every decimalOverflowError's text. It holds no
// quotes, so it survives the masking Arc applies to error texts it logs or
// sends as a trailer (sqlutil.SanitizeErrText), and the Flight SQL bridge can
// tell this error from any other truncation by its start (#1784). A user's
// own error() text cannot start a truncation reason: those arrive wrapped.
const decimalOverflowPrefix = "decimal value beyond the int64 range"

// flightTruncationError is the error a Flight SQL DoGet ends with when the
// Arrow response it relays was cut, given the reason in the truncation
// trailer. A decimal value outside int64 is the query's to fix, so it is
// InvalidArgument with the reason; any other cut stays a generic Internal
// error, as before, so no other reason text reaches the client.
func flightTruncationError(reason string) error {
	if strings.HasPrefix(reason, decimalOverflowPrefix) {
		return status.Error(codes.InvalidArgument, reason)
	}
	return status.Error(codes.Internal, "Arc query stream was truncated")
}
