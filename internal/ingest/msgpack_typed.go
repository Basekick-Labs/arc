package ingest

import (
	"fmt"
	"math"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/Basekick-Labs/msgpack/v6/msgpcode"
)

// TypedColumnarRecord is the typed-decode counterpart of models.ColumnarRecord:
// the msgpack columnar payload decoded straight into typed column slices,
// skipping the per-value interface boxing of the generic decode path.
// Produced only by tryDecodeColumnarTyped; consumed by ArrowBuffer.Write.
type TypedColumnarRecord struct {
	Measurement string
	Batch       *TypedColumnBatch
	NumRecords  int
	// RawPayload holds the original msgpack bytes for the zero-copy WAL path,
	// exactly like ColumnarRecord.RawPayload.
	RawPayload []byte
}

// maxTypedPreallocElems mirrors the fork's sliceAllocLimit: a forged msgpack
// length header must not be able to force a huge upfront allocation. Columns
// claiming more elements than this fall back to the generic decode path,
// which carries its own alloc-bomb guard.
const maxTypedPreallocElems = 1 << 20

// column type classes for the per-element dispatch
type colClass uint8

const (
	clsUnknown colClass = iota
	clsInt
	clsFloat
	clsStr
	clsBool
)

func isMapCode(c byte) bool {
	return msgpcode.IsFixedMap(c) || c == msgpcode.Map16 || c == msgpcode.Map32
}

func isArrayCode(c byte) bool {
	return msgpcode.IsFixedArray(c) || c == msgpcode.Array16 || c == msgpcode.Array32
}

func isIntCode(c byte) bool {
	if msgpcode.IsFixedNum(c) {
		return true
	}
	switch c {
	case msgpcode.Int8, msgpcode.Int16, msgpcode.Int32, msgpcode.Int64,
		msgpcode.Uint8, msgpcode.Uint16, msgpcode.Uint32, msgpcode.Uint64:
		return true
	}
	return false
}

func isFloatCode(c byte) bool {
	return c == msgpcode.Float || c == msgpcode.Double
}

func isStrCode(c byte) bool {
	// Deliberately excludes Bin8/16/32: the generic path boxes bin as []byte
	// and convertColumnsToTyped rejects it, so the typed path must not
	// accept bin where today's path errors. Bin falls back to the generic
	// decoder, which produces today's rejection.
	return msgpcode.IsFixedString(c) || c == msgpcode.Str8 || c == msgpcode.Str16 || c == msgpcode.Str32
}

func isBoolCode(c byte) bool {
	return c == msgpcode.True || c == msgpcode.False
}

// tryDecodeColumnarTyped attempts to decode a top-level single-map columnar
// payload ({m: "...", columns: {...}}) directly into typed column slices.
//
// Contract: it either fully succeeds — producing a record whose typed data,
// validity, and inferred types are IDENTICAL to what the generic decode +
// convertColumnsToTyped would have produced — or it returns ok=false having
// caused no side effects, and the caller runs the generic path. It never
// surfaces an error to the user: every reject decision (mixed types, bin
// columns, string time, uint64 overflow, nested values, batch format, ...)
// is delegated to the generic path by falling back, which guarantees the
// user-visible accept/reject behavior is byte-identical to the pre-typed
// decoder. The cost of double-decoding malformed payloads is irrelevant:
// they are rejected requests, not the hot path.
//
// Scope (deliberate, per the validated design): top-level single-map payloads
// only. Batch format, array format, row format, and payloads for deployments
// with configured decimal columns take the generic path unchanged.
func (d *MessagePackDecoder) tryDecodeColumnarTyped(data []byte) (*TypedColumnarRecord, bool) {
	dec := msgpack.GetDecoder()
	defer msgpack.PutDecoder(dec)
	dec.ResetBytes(data)

	c, err := dec.PeekCode()
	if err != nil || !isMapCode(c) {
		return nil, false
	}
	nkeys, err := dec.DecodeMapLen()
	if err != nil || nkeys <= 0 {
		return nil, false
	}

	var (
		measurement    string
		haveM, haveCol bool
		typed          map[string]interface{}
		validity       map[string][]bool
		numRecords     int
		sanitized      int
	)

	for k := 0; k < nkeys; k++ {
		kc, err := dec.PeekCode()
		if err != nil || !isStrCode(kc) {
			return nil, false
		}
		key, err := dec.DecodeString()
		if err != nil {
			return nil, false
		}
		switch key {
		case "batch":
			// Batch format takes precedence over columns in the generic
			// decoder regardless of key order — defer to it entirely.
			return nil, false
		case "m":
			if haveM {
				return nil, false // duplicate key — let the generic path decide
			}
			m, ok := decodeMeasurementTyped(dec)
			if !ok {
				return nil, false
			}
			measurement = m
			haveM = true
		case "columns":
			if haveCol {
				return nil, false
			}
			t, v, n, s, ok := d.decodeTypedColumns(dec)
			if !ok {
				return nil, false
			}
			typed, validity, numRecords, sanitized = t, v, n, s
			haveCol = true
		default:
			if err := dec.Skip(); err != nil {
				return nil, false
			}
		}
	}

	if !haveM || !haveCol {
		return nil, false
	}

	// Missing/omitted time column: generate now-µs for every row, matching
	// decodeColumnar's behavior (including the Warn so users know their data
	// lacked timestamps).
	if _, ok := typed["time"]; !ok {
		d.logger.Warn().
			Str("measurement", measurement).
			Int("row_count", numRecords).
			Msg("Data missing 'time' column - generating UTC timestamps")
		nowMicros := time.Now().UTC().UnixMicro()
		arr := make([]int64, numRecords)
		for i := range arr {
			arr[i] = nowMicros
		}
		typed["time"] = arr
	}

	if sanitized > 0 {
		d.logger.Warn().
			Str("measurement", measurement).
			Int("sanitized_fields", sanitized).
			Msg("Sanitized non-UTF8 characters in string columns")
	}

	batch := &TypedColumnBatch{
		Data:      typed,
		Validity:  validity,
		Signature: getColumnSignature(typed),
	}
	return &TypedColumnarRecord{
		Measurement: measurement,
		Batch:       batch,
		NumRecords:  numRecords,
		RawPayload:  data,
	}, true
}

// decodeMeasurementTyped decodes the "m" value with extractMeasurement's
// semantics: string as-is; integer becomes "measurement_<v>"; anything else
// is not eligible (the generic path produces the rejection).
func decodeMeasurementTyped(dec *msgpack.Decoder) (string, bool) {
	c, err := dec.PeekCode()
	if err != nil {
		return "", false
	}
	switch {
	case isStrCode(c):
		s, err := dec.DecodeString()
		if err != nil {
			return "", false
		}
		return s, true
	case c == msgpcode.Uint64:
		v, err := dec.DecodeUint64()
		if err != nil {
			return "", false
		}
		return fmt.Sprintf("measurement_%d", v), true
	case isIntCode(c):
		v, err := dec.DecodeInt64()
		if err != nil {
			return "", false
		}
		return fmt.Sprintf("measurement_%d", v), true
	default:
		return "", false
	}
}

// decodeTypedColumns decodes the columns map into typed slices.
// Returns (data, validity, numRecords, sanitizedCount, ok).
func (d *MessagePackDecoder) decodeTypedColumns(dec *msgpack.Decoder) (map[string]interface{}, map[string][]bool, int, int, bool) {
	c, err := dec.PeekCode()
	if err != nil || !isMapCode(c) {
		return nil, nil, 0, 0, false
	}
	ncols, err := dec.DecodeMapLen()
	if err != nil || ncols <= 0 {
		return nil, nil, 0, 0, false
	}

	colsHint := ncols
	if colsHint > 256 {
		colsHint = 256
	}
	typed := make(map[string]interface{}, colsHint)
	// Always non-nil, matching convertColumnsToTyped's return shape exactly
	// (per-column PRESENCE still only when nulls exist).
	validity := make(map[string][]bool)
	expected := -1
	sanitized := 0

	for i := 0; i < ncols; i++ {
		kc, err := dec.PeekCode()
		if err != nil || !isStrCode(kc) {
			return nil, nil, 0, 0, false
		}
		name, err := dec.DecodeString()
		if err != nil {
			return nil, nil, 0, 0, false
		}
		vc, err := dec.PeekCode()
		if err != nil {
			return nil, nil, 0, 0, false
		}
		if !isArrayCode(vc) {
			// The generic path silently drops non-array column values
			// (mapToPayload keeps only arrays). Match it: skip the value.
			if err := dec.Skip(); err != nil {
				return nil, nil, 0, 0, false
			}
			continue
		}
		n, err := dec.DecodeArrayLen()
		if err != nil || n <= 0 || n > maxTypedPreallocElems {
			// Empty columns and oversized length claims go to the generic
			// path (which errors on mismatch / guards allocation).
			return nil, nil, 0, 0, false
		}
		if expected == -1 {
			expected = n
		} else if n != expected {
			// Length mismatch — the generic path produces the error.
			return nil, nil, 0, 0, false
		}
		if _, dup := typed[name]; dup {
			return nil, nil, 0, 0, false // duplicate column key
		}

		if name == "time" {
			arr, ok := decodeTimeColumnTyped(dec, n)
			if !ok {
				return nil, nil, 0, 0, false
			}
			typed[name] = arr
			continue
		}

		colData, colValid, sanCount, ok := d.decodeValueColumnTyped(dec, n)
		if !ok {
			return nil, nil, 0, 0, false
		}
		typed[name] = colData
		if colValid != nil {
			validity[name] = colValid
		}
		sanitized += sanCount
	}

	if len(typed) == 0 || expected <= 0 {
		return nil, nil, 0, 0, false
	}
	return typed, validity, expected, sanitized, true
}

// decodeTimeColumnTyped decodes the time column with the combined semantics
// of normalizeTimestamps + convertColumnsToTyped's time chokepoint:
//   - unit auto-detected from element 0 (NOT first-non-nil — matches
//     normalizeTimestamps, which reads timeCol[0])
//   - any nil element rejects (groupByHour reads times with no validity)
//   - string time rejects (VARCHAR time would wedge compaction)
//   - floats truncate via toInt64 semantics
//
// All rejects are expressed as ok=false → generic path produces the error.
func decodeTimeColumnTyped(dec *msgpack.Decoder, n int) ([]int64, bool) {
	arr := make([]int64, n)
	var multiplier int64
	for i := 0; i < n; i++ {
		c, err := dec.PeekCode()
		if err != nil {
			return nil, false
		}
		var ts int64
		switch {
		case c == msgpcode.Uint64:
			v, err := dec.DecodeUint64()
			if err != nil {
				return nil, false
			}
			// toInt64Timestamp casts uint64 to int64 unconditionally; values
			// above MaxInt64 wrap there too. Match exactly.
			ts = int64(v)
		case isIntCode(c):
			v, err := dec.DecodeInt64()
			if err != nil {
				return nil, false
			}
			ts = v
		case isFloatCode(c):
			f, err := dec.DecodeFloat64()
			if err != nil {
				return nil, false
			}
			ts = int64(f)
		default:
			// nil, string, bool, bin, nested — the generic path rejects all
			// of these for time.
			return nil, false
		}
		if i == 0 {
			// Unit detection from element 0, mirroring normalizeTimestamps.
			switch {
			case ts < 1e10:
				multiplier = 1_000_000 // seconds
			case ts < 1e13:
				multiplier = 1000 // milliseconds
			case ts < 1e16:
				multiplier = 1 // microseconds
			default:
				multiplier = -1000 // nanoseconds (divide)
			}
		}
		if multiplier < 0 {
			arr[i] = ts / -multiplier
		} else {
			arr[i] = ts * multiplier
		}
	}
	return arr, true
}

// decodeValueColumnTyped decodes one non-time column. The type class is
// decided by the first non-nil element (matching firstNonNil +
// convertColumnsToTyped), with the same coercion rules:
//   - int class: later ints/floats coerce via toInt64 (uint64 > MaxInt64
//     rejects, floats truncate after bounds check); strings/bools/bin reject
//   - float class: later ints coerce via toFloat64; strings/bools/bin reject
//   - string class: strings only (UTF-8 sanitized inline); everything else
//     rejects
//   - bool class: bools only
//   - all-nil column: []string with all-false validity (#337 semantics)
//
// Validity is returned non-nil ONLY when at least one nil was seen,
// preserving convertColumnsToTyped's presence contract.
func (d *MessagePackDecoder) decodeValueColumnTyped(dec *msgpack.Decoder, n int) (interface{}, []bool, int, bool) {
	class := clsUnknown
	var (
		i64s  []int64
		f64s  []float64
		strs  []string
		bools []bool
		valid []bool
	)
	sanitized := 0

	// valid is allocated lazily on the first nil, so valid != nil doubles as
	// the "column has nulls" marker (validity presence contract).
	markNil := func(i int) {
		if valid == nil {
			valid = make([]bool, n)
			// entries before i were valid (nil marks skipped setting them)
			for j := 0; j < i; j++ {
				valid[j] = true
			}
		}
	}
	markValid := func(i int) {
		if valid != nil {
			valid[i] = true
		}
	}

	for i := 0; i < n; i++ {
		c, err := dec.PeekCode()
		if err != nil {
			return nil, nil, 0, false
		}
		if c == msgpcode.Nil {
			if err := dec.DecodeNil(); err != nil {
				return nil, nil, 0, false
			}
			markNil(i)
			continue
		}

		if class == clsUnknown {
			switch {
			case isIntCode(c):
				class = clsInt
				i64s = make([]int64, n)
			case isFloatCode(c):
				class = clsFloat
				f64s = make([]float64, n)
			case isStrCode(c):
				class = clsStr
				strs = make([]string, n)
			case isBoolCode(c):
				class = clsBool
				bools = make([]bool, n)
			default:
				// bin, nested array/map, ext — generic path rejects.
				return nil, nil, 0, false
			}
		}

		switch class {
		case clsInt:
			v, ok := decodeIntElemAsInt64(dec, c)
			if !ok {
				return nil, nil, 0, false
			}
			i64s[i] = v
		case clsFloat:
			v, ok := decodeElemAsFloat64(dec, c)
			if !ok {
				return nil, nil, 0, false
			}
			f64s[i] = v
		case clsStr:
			if !isStrCode(c) {
				return nil, nil, 0, false
			}
			s, err := dec.DecodeString()
			if err != nil {
				return nil, nil, 0, false
			}
			if sane, modified := SanitizeUTF8(s); modified {
				s = sane
				sanitized++
			}
			strs[i] = s
		case clsBool:
			if !isBoolCode(c) {
				return nil, nil, 0, false
			}
			b, err := dec.DecodeBool()
			if err != nil {
				return nil, nil, 0, false
			}
			bools[i] = b
		}
		markValid(i)
	}

	if class == clsUnknown {
		// Every element was nil: all-null string column, all-false validity
		// (#337 — the column must exist so queries bind).
		return make([]string, n), make([]bool, n), 0, true
	}

	var data interface{}
	switch class {
	case clsInt:
		data = i64s
	case clsFloat:
		data = f64s
	case clsStr:
		data = strs
	case clsBool:
		data = bools
	}
	return data, valid, sanitized, true
}

// decodeIntElemAsInt64 decodes one element into an int-class column with
// toInt64's exact coercion semantics.
func decodeIntElemAsInt64(dec *msgpack.Decoder, c byte) (int64, bool) {
	switch {
	case c == msgpcode.Uint64:
		v, err := dec.DecodeUint64()
		if err != nil || v > math.MaxInt64 {
			// toInt64 rejects uint64 values above MaxInt64.
			return 0, false
		}
		return int64(v), true
	case isIntCode(c):
		v, err := dec.DecodeInt64()
		if err != nil {
			return 0, false
		}
		return v, true
	case isFloatCode(c):
		f, err := dec.DecodeFloat64()
		if err != nil {
			return 0, false
		}
		// toInt64 float semantics: bounds check, then truncate.
		if f > float64(math.MaxInt64) || f < float64(math.MinInt64) {
			return 0, false
		}
		return int64(f), true
	default:
		return 0, false
	}
}

// decodeElemAsFloat64 decodes one element into a float-class column with
// toFloat64's coercion semantics (all numeric kinds accepted, no bounds).
func decodeElemAsFloat64(dec *msgpack.Decoder, c byte) (float64, bool) {
	switch {
	case c == msgpcode.Uint64:
		v, err := dec.DecodeUint64()
		if err != nil {
			return 0, false
		}
		return float64(v), true
	case isIntCode(c):
		v, err := dec.DecodeInt64()
		if err != nil {
			return 0, false
		}
		return float64(v), true
	case isFloatCode(c):
		f, err := dec.DecodeFloat64()
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}
