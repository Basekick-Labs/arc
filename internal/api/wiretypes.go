package api

// Arc's msgpack wire-type names.
//
// These are the strings the msgpack response's "types" array carries, and
// they are a published contract: BI drivers and other clients switch on
// them to decode each column. They live here, in an untagged file with no
// Arrow dependency, because two independent code paths emit them and must
// not drift apart:
//
//   - the streaming query path, via arrowTypeName (query_msgpack_types.go,
//     build-tagged duckdb_arrow) — derives the name from the Arrow schema
//   - the SHOW handlers (query.go), which know their schema by construction
//     and cannot import Arrow, since query.go builds without the tag
//
// Defining the strings once and having both paths reference them makes
// divergence a compile-time impossibility rather than something a test has
// to notice after the fact.
//
// Any change here is a wire-contract change. See query_msgpack_types.go for
// why these are Arc's own names rather than arrow.DataType.String().
const (
	wireTypeBool    = "bool"
	wireTypeInt8    = "int8"
	wireTypeInt16   = "int16"
	wireTypeInt32   = "int32"
	wireTypeInt64   = "int64"
	wireTypeUint8   = "uint8"
	wireTypeUint16  = "uint16"
	wireTypeUint32  = "uint32"
	wireTypeUint64  = "uint64"
	wireTypeFloat32 = "float32"
	wireTypeFloat64 = "float64"
	wireTypeUTF8    = "utf8"
	wireTypeBinary  = "binary"
	wireTypeStruct  = "struct"
	wireTypeList    = "list"
	wireTypeMap     = "map"
	wireTypeNull    = "null"

	// wireTypeStringEncoded is the honest name for a column whose Arrow
	// type Arc recognizes but whose values the msgpack encoder has no
	// typed case for, so they are rendered with ValueStr and transmitted
	// as msgpack strings (DATE64, TIME32/64, DURATION, the INTERVALs,
	// FLOAT16, FIXED_SIZE_BINARY). Naming these by their logical Arrow
	// type would advertise a numeric payload over a string one — the
	// exact defect this contract was introduced to eliminate.
	wireTypeStringEncoded = "string_encoded"

	// wireTypeUnknownPrefix marks a type Arc has no published name for.
	// Prefixed rather than bare so a client cannot mistake the trailing
	// debug text for a stable Arc type name.
	wireTypeUnknownPrefix = "unknown:"
)
