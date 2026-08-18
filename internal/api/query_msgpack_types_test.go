//go:build duckdb_arrow

package api

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// TestArrowTypeName_Golden pins every string arrowTypeName can return.
//
// This is the durable artifact of the wire-contract freeze: these strings
// are published to clients, so a change here is a breaking change and must
// be a deliberate edit to this table, not a side effect of bumping
// arrow-go. See query_msgpack_types.go for the three historical cases where
// arrow.DataType.String() changed format in a patch release.
func TestArrowTypeName_Golden(t *testing.T) {
	tests := []struct {
		name string
		dt   arrow.DataType
		want string
	}{
		// Scalars — these spellings match what Arc shipped before the
		// freeze, so no existing client sees a change.
		{"bool", arrow.FixedWidthTypes.Boolean, "bool"},
		{"int8", arrow.PrimitiveTypes.Int8, "int8"},
		{"int16", arrow.PrimitiveTypes.Int16, "int16"},
		{"int32", arrow.PrimitiveTypes.Int32, "int32"},
		{"int64", arrow.PrimitiveTypes.Int64, "int64"},
		{"uint8", arrow.PrimitiveTypes.Uint8, "uint8"},
		{"uint16", arrow.PrimitiveTypes.Uint16, "uint16"},
		{"uint32", arrow.PrimitiveTypes.Uint32, "uint32"},
		{"uint64", arrow.PrimitiveTypes.Uint64, "uint64"},
		{"float32", arrow.PrimitiveTypes.Float32, "float32"},
		{"float64", arrow.PrimitiveTypes.Float64, "float64"},
		{"utf8", arrow.BinaryTypes.String, "utf8"},
		{"large_utf8", arrow.BinaryTypes.LargeString, "large_utf8"},
		{"binary", arrow.BinaryTypes.Binary, "binary"},
		{"large_binary", arrow.BinaryTypes.LargeBinary, "large_binary"},
		{"date32", arrow.FixedWidthTypes.Date32, "date32"},
		{"null", arrow.Null, "null"},

		// Parameterized — the unit and precision/scale are load-bearing,
		// so they survive, but in a format string Arc owns.
		{"timestamp_us", arrow.FixedWidthTypes.Timestamp_us, "timestamp[us]"},
		{"timestamp_ns", arrow.FixedWidthTypes.Timestamp_ns, "timestamp[ns]"},
		{"timestamp_ms", arrow.FixedWidthTypes.Timestamp_ms, "timestamp[ms]"},
		{"timestamp_s", arrow.FixedWidthTypes.Timestamp_s, "timestamp[s]"},
		{"timestamp_tz", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "America/Denver"}, "timestamp[us]"},
		// All four decimal widths must be named — a decimal that reached
		// the unknown: sentinel would be the types-lie-about-values bug
		// again, in miniature.
		{"decimal128", &arrow.Decimal128Type{Precision: 10, Scale: 2}, "decimal(10, 2)"},
		{"decimal128_hugeint", &arrow.Decimal128Type{Precision: 38, Scale: 0}, "decimal(38, 0)"},
		{"decimal256", &arrow.Decimal256Type{Precision: 40, Scale: 4}, "decimal(40, 4)"},
		{"decimal32", &arrow.Decimal32Type{Precision: 8, Scale: 2}, "decimal(8, 2)"},
		{"decimal64", &arrow.Decimal64Type{Precision: 16, Scale: 2}, "decimal(16, 2)"},

		// Recognized types the encoder has no typed case for: their
		// values go out as ValueStr strings, so the name says so rather
		// than advertising a numeric payload.
		{"date64", arrow.FixedWidthTypes.Date64, "string_encoded"},
		{"time32_s", arrow.FixedWidthTypes.Time32s, "string_encoded"},
		{"time64_ns", arrow.FixedWidthTypes.Time64ns, "string_encoded"},
		{"duration_ns", arrow.FixedWidthTypes.Duration_ns, "string_encoded"},
		{"month_interval", arrow.FixedWidthTypes.MonthInterval, "string_encoded"},
		{"day_time_interval", arrow.FixedWidthTypes.DayTimeInterval, "string_encoded"},
		{"month_day_nano", arrow.FixedWidthTypes.MonthDayNanoInterval, "string_encoded"},
		{"float16", arrow.FixedWidthTypes.Float16, "string_encoded"},
		{"fixed_size_binary", &arrow.FixedSizeBinaryType{ByteWidth: 16}, "string_encoded"},

		// Nested — deliberately collapsed to a bare token. encodeColumn
		// has no List/Struct case, so the values go out as strings; the
		// token says "opaque" instead of promising element typing.
		{"list", arrow.ListOf(arrow.PrimitiveTypes.Int32), "list"},
		{"large_list", arrow.LargeListOf(arrow.PrimitiveTypes.Int32), "list"},
		{"fixed_size_list", arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int32), "list"},
		{"struct", arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}), "struct"},
		{"map", arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32), "map"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := arrowTypeName(tt.dt); got != tt.want {
				t.Errorf("arrowTypeName(%s) = %q, want %q (this is a WIRE CONTRACT change — update clients, not just this test)", tt.name, got, tt.want)
			}
		})
	}
}

// TestArrowTypeName_NestedIsNotArrowString is the regression guard for the
// bug that motivated this work: nested types must NOT carry through
// arrow.DataType.String(), whose format upstream changes in patch releases.
//
// Reverting arrowTypeName's LIST/STRUCT cases to f.Type.String() fails this
// test — the assertions below are exactly the debug spellings that leaked
// before the freeze.
func TestArrowTypeName_NestedIsNotArrowString(t *testing.T) {
	list := arrow.ListOf(arrow.PrimitiveTypes.Int32)
	strct := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true})

	// Guard the premise: if upstream ever makes String() match our token,
	// this test would pass for the wrong reason.
	if list.String() == "list" || strct.String() == "struct" {
		t.Fatalf("premise broken: arrow String() now matches the Arc token (list=%q struct=%q)", list.String(), strct.String())
	}

	for _, tc := range []struct {
		name string
		dt   arrow.DataType
		want string
	}{
		{"list", list, "list"},
		{"struct", strct, "struct"},
	} {
		got := arrowTypeName(tc.dt)
		if got != tc.want {
			t.Errorf("arrowTypeName(%s) = %q, want %q", tc.name, got, tc.want)
		}
		if got == tc.dt.String() {
			t.Errorf("arrowTypeName(%s) returned arrow's String() output %q — the wire contract must not be arrow-go debug output", tc.name, got)
		}
		if strings.ContainsAny(got, "<>:") {
			t.Errorf("arrowTypeName(%s) = %q contains structural punctuation; nested types must be a bare token", tc.name, got)
		}
	}
}

// TestArrowTypeName_NilIsSentinelNotPanic guards the design rule that a
// type name must never fail a query.
func TestArrowTypeName_NilIsSentinelNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("arrowTypeName(nil) panicked: %v", r)
		}
	}()
	if got := arrowTypeName(nil); !strings.HasPrefix(got, "unknown:") {
		t.Errorf("arrowTypeName(nil) = %q, want an unknown: sentinel", got)
	}
}

// TestArrowTypeName_NoTypedNameWithoutEncoderCase is the structural guard
// for the class of bug this contract exists to prevent: a column must
// never be advertised with a precise, typed-looking name while its values
// are transmitted as msgpack strings.
//
// encodeColumn (query_msgpack.go) has typed cases only for the Arrow types
// listed here. Every other type must resolve to "string_encoded", a bare
// nested token, or the unknown: sentinel — all of which tell a client the
// payload is opaque. If someone adds a typed name without a matching
// encoder case, this fails.
func TestArrowTypeName_NoTypedNameWithoutEncoderCase(t *testing.T) {
	// Types encodeColumn handles with a typed (non-string) encoding.
	typedOK := map[string]bool{
		"int8": true, "int16": true, "int32": true, "int64": true,
		"uint8": true, "uint16": true, "uint32": true, "uint64": true,
		"float32": true, "float64": true, "bool": true,
		"timestamp[s]": true, "timestamp[ms]": true, "timestamp[us]": true, "timestamp[ns]": true,
		"date32": true,
		// String-family types are genuinely strings on the wire, so a
		// string-shaped name is honest for them.
		"utf8": true, "large_utf8": true, "binary": true, "large_binary": true,
		// null carries no values at all.
		"null": true,
	}
	// Names that self-describe as opaque.
	opaque := func(s string) bool {
		return s == "string_encoded" || s == "list" || s == "struct" || s == "map" ||
			strings.HasPrefix(s, "unknown:") || strings.HasPrefix(s, "decimal(")
	}

	// Every Arrow type Arc could plausibly meet.
	all := []arrow.DataType{
		arrow.FixedWidthTypes.Boolean, arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64,
		arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64, arrow.FixedWidthTypes.Float16,
		arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString, arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.LargeBinary, &arrow.FixedSizeBinaryType{ByteWidth: 16},
		arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Date64,
		arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Timestamp_ns,
		arrow.FixedWidthTypes.Time32s, arrow.FixedWidthTypes.Time64ns,
		arrow.FixedWidthTypes.Duration_ns, arrow.FixedWidthTypes.MonthInterval,
		arrow.FixedWidthTypes.DayTimeInterval, arrow.FixedWidthTypes.MonthDayNanoInterval,
		&arrow.Decimal128Type{Precision: 38, Scale: 0}, &arrow.Decimal256Type{Precision: 40, Scale: 4},
		&arrow.Decimal32Type{Precision: 8, Scale: 2}, &arrow.Decimal64Type{Precision: 16, Scale: 2},
		arrow.ListOf(arrow.PrimitiveTypes.Int32),
		arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32}),
		arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32),
		arrow.Null,
		&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String},
	}

	for _, dt := range all {
		name := arrowTypeName(dt)
		if !typedOK[name] && !opaque(name) {
			t.Errorf("arrowTypeName(%s) = %q: a typed-looking name with no encodeColumn case — "+
				"values would ship as strings under a numeric-looking type. Use string_encoded, "+
				"or add the encoder case.", dt, name)
		}
	}
}

// TestArrowTypeName_UnknownSentinel covers the fallback path. This is the
// one place arrow.DataType.String() still runs, so it is the residual
// exposure to an upstream format change — pinned here deliberately.
//
// DuckDB ENUM columns reach this path: DuckDB's Arrow export maps ENUM to
// dictionary<...> and Arc does not normalize it.
func TestArrowTypeName_UnknownSentinel(t *testing.T) {
	dict := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.BinaryTypes.String,
	}

	got := arrowTypeName(dict)

	if !strings.HasPrefix(got, "unknown:") {
		t.Errorf("arrowTypeName(dictionary) = %q, want an %q-prefixed sentinel so clients cannot bind to an unowned spelling", got, "unknown:")
	}
	if got == dict.String() {
		t.Errorf("arrowTypeName(dictionary) = %q — the raw String() must not be emitted unprefixed", got)
	}
	// The suffix is arrow-go's debug text by design (it is diagnostic, not
	// contractual); the prefix is what makes that safe.
	if !strings.Contains(got, dict.String()) {
		t.Errorf("arrowTypeName(dictionary) = %q, want it to carry %q for diagnosis", got, dict.String())
	}
}

// TestArrowTypeName_MatchesSHOWLiterals proves the two independent emitters
// of the "types" array agree. The SHOW handlers (query.go) cannot import
// Arrow — query.go builds without the duckdb_arrow tag — so both sides
// reference the shared constants in wiretypes.go instead. This asserts the
// constants really are what arrowTypeName produces for the same logical
// type, which is the property that makes the sharing meaningful.
func TestArrowTypeName_MatchesSHOWLiterals(t *testing.T) {
	cases := []struct {
		constant string
		dt       arrow.DataType
	}{
		{wireTypeUTF8, arrow.BinaryTypes.String},
		{wireTypeInt64, arrow.PrimitiveTypes.Int64},
		{wireTypeFloat64, arrow.PrimitiveTypes.Float64},
	}
	for _, c := range cases {
		if got := arrowTypeName(c.dt); got != c.constant {
			t.Errorf("SHOW handlers emit %q but the streaming path emits %q for the same logical type", c.constant, got)
		}
	}
}
