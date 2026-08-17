//go:build duckdb_arrow

package api

import (
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/rs/zerolog/log"
)

// arrowTypeName maps an Arrow data type to the name Arc publishes in the
// msgpack response's "types" array.
//
// This is Arc's wire contract, deliberately NOT arrow.DataType.String().
// That method is Go debug output and upstream changes its format in patch
// releases: `list<item: int32>` gained `, nullable` in 2021 (ARROW-8452),
// the hardcoded `item` became the element's name a week later (ARROW-8453),
// and struct gained an (un-comma'd) ` nullable` in arrow-go 1a28af3 — which
// shipped in v18.6.0, the version this module pins. Upstream classes these
// as bug fixes because String() was never meant to be a contract. Clients
// binding to it would break on a routine dependency bump.
//
// Dispatch is on arrow.Type IDs instead. Those are the Arrow wire-format
// enum: a renumbering is invisible here (Go resolves the constant names at
// compile time) and a removal is a compile error rather than silent drift.
//
// Rules:
//   - Scalars keep the exact spelling they have always had, so no existing
//     client changes.
//   - Parameterized types keep their information — decimal precision/scale
//     and timestamp unit are semantically load-bearing — but the format
//     string is Arc's, so upstream cannot move it.
//   - Nested types collapse to a bare token. encodeColumn has no List or
//     Struct case, so nested values already go out as ValueStr strings; a
//     bare `list` honestly says "opaque, rendered as string" where
//     `list<int32>` would promise element typing the encoder never delivers.
//   - Anything unrecognized gets an `unknown:` sentinel rather than raw
//     String(), so a client cannot accidentally bind to a spelling Arc does
//     not control. The query still succeeds — a new Arrow type must never
//     fail a request.
//
// Keep in sync with the golden test in query_msgpack_types_test.go, which
// pins every string this function can return.
func arrowTypeName(dt arrow.DataType) string {
	// A nil type cannot come from a well-formed Arrow schema, but the
	// design rule is that a type name must never fail a query — so
	// degrade to the sentinel rather than panicking on dt.ID().
	if dt == nil {
		return wireTypeUnknownPrefix + "nil"
	}
	switch dt.ID() {
	case arrow.BOOL:
		return wireTypeBool
	case arrow.INT8:
		return wireTypeInt8
	case arrow.INT16:
		return wireTypeInt16
	case arrow.INT32:
		return wireTypeInt32
	case arrow.INT64:
		return wireTypeInt64
	case arrow.UINT8:
		return wireTypeUint8
	case arrow.UINT16:
		return wireTypeUint16
	case arrow.UINT32:
		return wireTypeUint32
	case arrow.UINT64:
		return wireTypeUint64
	case arrow.FLOAT32:
		return wireTypeFloat32
	case arrow.FLOAT64:
		return wireTypeFloat64
	case arrow.STRING:
		return wireTypeUTF8
	case arrow.LARGE_STRING:
		return "large_utf8"
	case arrow.BINARY:
		return wireTypeBinary
	case arrow.LARGE_BINARY:
		return "large_binary"
	case arrow.DATE32:
		return "date32"
	case arrow.TIMESTAMP:
		// Unit is load-bearing: a client must distinguish us from ns to
		// scale the integer correctly. Timezone is deliberately omitted —
		// Arc normalizes to UTC.
		if t, ok := dt.(*arrow.TimestampType); ok {
			return "timestamp[" + arrowTimeUnitName(t.Unit) + "]"
		}
		return "timestamp[us]"
	case arrow.DECIMAL32, arrow.DECIMAL64, arrow.DECIMAL128, arrow.DECIMAL256:
		// Reachable only if a decimal escapes normalizeDecimalSchema
		// (which rewrites every arrow.DecimalType to int64/float64 before
		// this runs). All four widths are listed so a decimal can never
		// fall to the unknown: sentinel — that would be the same
		// types-disagree-with-values bug this contract exists to prevent.
		if t, ok := dt.(arrow.DecimalType); ok {
			return fmt.Sprintf("decimal(%d, %d)", t.GetPrecision(), t.GetScale())
		}
		return "decimal"
	case arrow.DATE64, arrow.TIME32, arrow.TIME64, arrow.DURATION,
		arrow.INTERVAL_MONTHS, arrow.INTERVAL_DAY_TIME, arrow.INTERVAL_MONTH_DAY_NANO,
		arrow.FLOAT16, arrow.FIXED_SIZE_BINARY:
		// encodeColumn has no case for any of these, so their values go
		// out as encodeFallbackColumn strings (e.g. a DURATION renders as
		// "1h0m0s"). Naming them `duration[ns]` or `date64` would repeat
		// the mistake this contract exists to fix — a precise numeric-
		// looking name over a string payload. Same rule as the nested
		// types below: the name describes what is actually on the wire.
		// If an encoder case is added later, the type graduates to its
		// own name and that is a deliberate contract change.
		return wireTypeStringEncoded
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		return wireTypeList
	case arrow.STRUCT:
		return wireTypeStruct
	case arrow.MAP:
		return wireTypeMap
	case arrow.NULL:
		return wireTypeNull
	default:
		return unknownArrowTypeName(dt)
	}
}

// arrowTimeUnitName spells an Arrow time unit. Arc owns these four strings
// rather than using arrow.TimeUnit.String() for the same reason
// arrowTypeName exists.
func arrowTimeUnitName(u arrow.TimeUnit) string {
	switch u {
	case arrow.Second:
		return "s"
	case arrow.Millisecond:
		return "ms"
	case arrow.Microsecond:
		return "us"
	case arrow.Nanosecond:
		return "ns"
	default:
		return "us"
	}
}

// unknownArrowTypeLogged bounds the log volume for unmapped types: one
// line per distinct Arrow type ID for the process lifetime, not one per
// query (a wide result set would otherwise log per column, per request).
var unknownArrowTypeLogged sync.Map

// unknownArrowTypeName renders an unmapped type behind an `unknown:`
// prefix so no client mistakes it for a stable Arc type name, and logs it
// once so the gap is noticed and mapped in a later release.
//
// DuckDB ENUM columns land here: DuckDB's Arrow export maps them to
// dictionary<...>, and Arc does not normalize that. Their values already
// go out as encodeFallbackColumn strings, so the sentinel is honest about
// what is on the wire.
func unknownArrowTypeName(dt arrow.DataType) string {
	if _, seen := unknownArrowTypeLogged.LoadOrStore(dt.ID(), struct{}{}); !seen {
		log.Warn().
			Str("component", "query-handler").
			Str("arrow_type", dt.String()).
			Int("arrow_type_id", int(dt.ID())).
			Msg("msgpack response: Arrow type has no Arc wire-type name; emitting unknown: sentinel")
	}
	return wireTypeUnknownPrefix + dt.String()
}
