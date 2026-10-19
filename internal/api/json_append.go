package api

import (
	"math"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// jsonValueEncoder appends JSON values read from typed Arrow arrays to a byte
// slice. It is the only encoder of the Arrow-JSON query path, serial or
// parallel, so every path produces the same bytes. An encoder is not safe for
// concurrent use: each worker has its own (it caches the last timestamp
// second).
type jsonValueEncoder struct {
	// The "YYYY-MM-DDTHH:MM:SS" of the last two timestamp seconds formatted:
	// rows of a time-series result mostly share or step through seconds, and
	// the date/time part is the expensive half of RFC 3339 formatting. Two
	// slots, so a row with two timestamp columns does not evict one with the
	// other on every value.
	ts   [2]tsPrefixSlot
	tsLR int // the least recently used slot, overwritten on a miss
}

type tsPrefixSlot struct {
	sec    int64
	prefix []byte
	valid  bool
}

// appendRow appends one row as a JSON array.
func (e *jsonValueEncoder) appendRow(dst []byte, cols []arrow.Array, row int) []byte {
	dst = append(dst, '[')
	for c, col := range cols {
		if c > 0 {
			dst = append(dst, ',')
		}
		dst = e.appendValue(dst, col, row)
	}
	return append(dst, ']')
}

// appendValue appends one value. NaN and ±Inf are null (JSON has no literal
// for them); timestamps and dates are RFC 3339 strings in UTC; types without
// a case go through ValueStr as a JSON string.
func (e *jsonValueEncoder) appendValue(dst []byte, col arrow.Array, row int) []byte {
	if col.IsNull(row) {
		return append(dst, "null"...)
	}

	switch c := col.(type) {
	case *array.Int64:
		return strconv.AppendInt(dst, c.Value(row), 10)
	case *array.Int32:
		return strconv.AppendInt(dst, int64(c.Value(row)), 10)
	case *array.Int16:
		return strconv.AppendInt(dst, int64(c.Value(row)), 10)
	case *array.Int8:
		return strconv.AppendInt(dst, int64(c.Value(row)), 10)
	case *array.Uint64:
		return strconv.AppendUint(dst, c.Value(row), 10)
	case *array.Uint32:
		return strconv.AppendUint(dst, uint64(c.Value(row)), 10)
	case *array.Uint16:
		return strconv.AppendUint(dst, uint64(c.Value(row)), 10)
	case *array.Uint8:
		return strconv.AppendUint(dst, uint64(c.Value(row)), 10)
	case *array.Float64:
		return appendJSONFloat(dst, c.Value(row))
	case *array.Float32:
		return appendJSONFloat(dst, float64(c.Value(row)))
	case *array.Boolean:
		if c.Value(row) {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)
	case *array.Timestamp:
		unit := c.DataType().(*arrow.TimestampType).Unit
		dst = append(dst, '"')
		dst = e.appendTime(dst, c.Value(row).ToTime(unit))
		return append(dst, '"')
	case *array.Date32:
		dst = append(dst, '"')
		dst = e.appendTime(dst, c.Value(row).ToTime())
		return append(dst, '"')
	case *array.String:
		return appendJSONString(dst, c.Value(row))
	case *array.LargeString:
		return appendJSONString(dst, c.Value(row))
	case *array.Binary:
		return appendJSONString(dst, string(c.Value(row)))
	default:
		return appendJSONString(dst, col.ValueStr(row))
	}
}

func appendJSONFloat(dst []byte, v float64) []byte {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return append(dst, "null"...)
	}
	return strconv.AppendFloat(dst, v, 'f', -1, 64)
}

// appendTime appends t in UTC exactly as t.UTC().AppendFormat(dst,
// time.RFC3339Nano) does: the fraction has its trailing zeros dropped and is
// left out when zero, and the zone is "Z". Years outside 0-9999 take
// AppendFormat itself.
func (e *jsonValueEncoder) appendTime(dst []byte, t time.Time) []byte {
	t = t.UTC()
	if y := t.Year(); y < 0 || y > 9999 {
		return t.AppendFormat(dst, time.RFC3339Nano)
	}
	sec := t.Unix()
	slot := -1
	for i := range e.ts {
		if e.ts[i].valid && e.ts[i].sec == sec {
			slot = i
			break
		}
	}
	if slot < 0 {
		slot = e.tsLR
		s := &e.ts[slot]
		s.prefix = t.AppendFormat(s.prefix[:0], "2006-01-02T15:04:05")
		s.sec, s.valid = sec, true
	}
	e.tsLR = 1 - slot
	dst = append(dst, e.ts[slot].prefix...)
	if ns := t.Nanosecond(); ns != 0 {
		var frac [10]byte
		frac[0] = '.'
		for i := 9; i >= 1; i-- {
			frac[i] = byte('0' + ns%10)
			ns /= 10
		}
		n := 10
		for frac[n-1] == '0' {
			n--
		}
		dst = append(dst, frac[:n]...)
	}
	return append(dst, 'Z')
}

// appendJSONString appends s as a JSON string. Bytes that need no escaping
// are copied as they are (invalid UTF-8 included), like writeJSONString.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '"' && c != '\\' && c >= 0x20 {
			continue
		}
		dst = append(dst, s[start:i]...)
		switch c {
		case '"':
			dst = append(dst, `\"`...)
		case '\\':
			dst = append(dst, `\\`...)
		case '\n':
			dst = append(dst, `\n`...)
		case '\r':
			dst = append(dst, `\r`...)
		case '\t':
			dst = append(dst, `\t`...)
		case '\b':
			dst = append(dst, `\b`...)
		case '\f':
			dst = append(dst, `\f`...)
		default:
			dst = append(dst, `\u00`...)
			dst = append(dst, hexDigit(c>>4), hexDigit(c&0x0f))
		}
		start = i + 1
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}
