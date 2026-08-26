package logger

import (
	"github.com/rs/zerolog"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
)

// installErrSanitizer sets the process-wide zerolog error marshaller so that
// EVERY `Err(err)` log field is sanitized before it reaches a log line.
//
// Why this exists: the DuckDB driver's error strings echo user data back —
// every Parser/Binder/Conversion error quotes the offending literal in its
// message AND appends the full query text verbatim in a "LINE 1: …" context
// block. There are ~77 `Err(err)` sites on the query paths alone, so masking
// the `sql=` log fields while logging errors unmasked would achieve nothing on
// exactly the paths (failures) where those logs fire. One hook here covers all
// of them, plus every future `Err(err)` site, plus arcx-engine errors crossing
// the FFI (whose messages echo user-supplied paths).
//
// What it does, in order:
//  1. Cut everything from the first "\nLINE " onward — that block is a verbatim
//     copy of the query, pure duplication of the (masked) `sql=` field.
//  2. Keep only the first line of what remains (drops "Candidate bindings" and
//     raw-value echo lines some error classes append).
//  3. Mask quoted spans in BOTH styles ('…' and "…"): engine messages quote
//     offending values with either. The message CLASS ("Could not convert
//     string '…' to INT64") survives; the value does not.
//
// Trade-off, accepted deliberately: the hook is global, so quoted spans in
// non-query errors (config values, filenames) are masked too. Uniform posture
// — "every logged error is sanitized" — is a one-sentence claim a reviewer can
// verify; a per-site wrapper would be a convention that erodes.
func installErrSanitizer() {
	zerolog.ErrorMarshalFunc = func(err error) interface{} {
		if err == nil {
			return nil
		}
		return sanitizeErrText(err.Error())
	}
}

func sanitizeErrText(s string) string { return sqlutil.SanitizeErrText(s) }
