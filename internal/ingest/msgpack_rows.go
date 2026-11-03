package ingest

import (
	"fmt"
	"math"
	"sort"

	"github.com/Basekick-Labs/msgpack/v6"
)

// Row-format msgpack ({m, t, h, fields, tags}, alone, in a top-level array,
// or in {"batch": [...]}: the Python client's write_records) decoded straight
// into typed columns per measurement, without a map per row and field.
//
// The result is what the record path makes of the same rows (decodeRow,
// rowsToColumnar, the buffer's typing), with line protocol's point-level
// rules for what it cannot write: a point with a tag or field named "time",
// one whose keys land in one column, or one with a value that does not fit
// its column's type (the first value in the request decides) is dropped and
// counted, and the rest are written. All or nothing: a row this decoder
// does not handle exactly (an unusual type anywhere, a missing "m" or
// "fields", compact "f") sends the whole payload to the generic path.

// msgRow is one decoded row.
type msgRow struct {
	measurement string
	ts          int64 // µs
	timed       bool  // false: no "t", ts is the decode time
	tags        []LPTag
	fields      []LPField // a nil Value is a NULL
}

// tryDecodeRowsTyped decodes a row-format payload; ok false sends it to the
// generic path.
func (d *MessagePackDecoder) tryDecodeRowsTyped(data []byte, known KnownColumns) (recs []interface{}, rows int, drops []LPDrop, ok bool) {
	var items [][]byte
	switch c := data[0]; {
	case isArrayCode(c):
		n, pos, ok := msgpackHeader(data, 0, true)
		if !ok || n == 0 || n > len(data)-pos {
			return nil, 0, nil, false
		}
		items = make([][]byte, 0, n)
		for i := 0; i < n; i++ {
			end, ok := msgpackValueEnd(data, pos)
			if !ok {
				return nil, 0, nil, false
			}
			items = append(items, data[pos:end])
			pos = end
		}
		if pos != len(data) {
			return nil, 0, nil, false
		}
	case isMapCode(c):
		if spans, ok := batchItemSpans(data); ok {
			if len(spans) == 0 {
				return nil, 0, nil, false
			}
			for _, sp := range spans {
				items = append(items, data[sp[0]:sp[1]])
			}
		} else {
			if end, ok := msgpackValueEnd(data, 0); !ok || end != len(data) {
				return nil, 0, nil, false
			}
			items = [][]byte{data}
		}
	default:
		return nil, 0, nil, false
	}

	decoded := make([]msgRow, 0, len(items))
	sanitized, untimed := 0, 0
	for _, item := range items {
		row, s, ok := d.decodeRowTyped(item)
		if ok && !row.timed {
			untimed++
		}
		if !ok {
			return nil, 0, nil, false
		}
		sanitized += s
		decoded = append(decoded, row)
	}
	if sanitized > 0 {
		d.logger.Warn().Int("sanitized_fields", sanitized).Msg("Sanitized non-UTF8 characters in row tags and fields")
	}
	if untimed > 0 {
		// One line per request (the record path logged one per row).
		d.logger.Warn().Int("rows", untimed).Msg("Row data missing timestamp - generating UTC timestamps")
	}

	// rowsToColumnar's naming, per measurement: a field named like any tag of
	// any row of the measurement is written to "<name>_value".
	tagNames := map[string]map[string]struct{}{}
	for i := range decoded {
		r := &decoded[i]
		set := tagNames[r.measurement]
		if set == nil {
			set = map[string]struct{}{}
			tagNames[r.measurement] = set
		}
		for _, t := range r.tags {
			set[t.Key] = struct{}{}
		}
	}
	b := newLPBuilder(nil, min(len(decoded), lpMaxColumnHint))
	b.known = known
	var pt lpPoint
	for i := range decoded {
		r := &decoded[i]
		set := tagNames[r.measurement]
		pt.measurement, pt.ts, pt.tags, pt.fields = r.measurement, r.ts, r.tags, r.fields
		pt.cols = pt.cols[:0]
		pt.renamed = false
		for _, f := range r.fields {
			if _, isTag := set[f.Key]; isTag {
				pt.renamed = true
				pt.cols = append(pt.cols, f.Key+"_value")
			} else {
				pt.cols = append(pt.cols, f.Key)
			}
		}
		if err := b.add(&pt); err != nil {
			return nil, 0, nil, false // cannot happen in infer mode; the generic path decides
		}
	}
	batches, counts, drops := b.finish()
	names := make([]string, 0, len(batches))
	for name := range batches {
		names = append(names, name)
	}
	sort.Strings(names)
	recs = make([]interface{}, 0, len(names))
	for _, name := range names {
		recs = append(recs, &TypedColumnarRecord{Measurement: name, Batch: batches[name], NumRecords: counts[name]})
	}
	return recs, len(decoded), drops, true
}

// decodeRowTyped decodes one row map as decodeRow reads it. sanitized counts
// the strings made valid UTF-8.
func (d *MessagePackDecoder) decodeRowTyped(item []byte) (row msgRow, sanitized int, ok bool) {
	dec := msgpack.GetDecoder()
	defer msgpack.PutDecoder(dec)
	dec.ResetBytes(item)
	nkeys, err := dec.DecodeMapLen()
	if err != nil || nkeys <= 0 || nkeys > len(item) {
		return row, 0, false
	}
	var haveM, haveT, haveH, haveFields bool
	var host string
	var seen keySet
	for k := 0; k < nkeys; k++ {
		c, err := dec.PeekCode()
		if err != nil || !isStrCode(c) {
			return row, 0, false
		}
		key, err := dec.DecodeString()
		if err != nil {
			return row, 0, false
		}
		if !seen.add(key) {
			return row, 0, false // a repeated key: the generic map keeps the last
		}
		switch key {
		case "m":
			m, ok := decodeMeasurementTyped(dec)
			if !ok {
				return row, 0, false
			}
			row.measurement, haveM = m, true
		case "t":
			ts, present, ok := decodeRowTime(dec)
			if !ok {
				return row, 0, false
			}
			row.ts, haveT = ts, present
		case "h":
			h, present, ok := d.decodeRowHost(dec)
			if !ok {
				return row, 0, false
			}
			host, haveH = h, present
		case "fields":
			fields, s, ok := decodeRowFields(dec, len(item))
			if !ok {
				return row, 0, false
			}
			row.fields, haveFields = fields, true
			sanitized += s
		case "tags":
			tags, s, ok := decodeRowTags(dec, len(item))
			if !ok {
				return row, 0, false
			}
			row.tags = tags
			sanitized += s
		case "batch", "f":
			return row, 0, false // batch format / compact fields: the generic path
		case "columns":
			if c, err := dec.PeekCode(); err != nil || isMapCode(c) {
				return row, 0, false // columnar: the generic path decides
			}
			if !skipScalar(dec) {
				return row, 0, false
			}
		default:
			if !skipScalar(dec) {
				return row, 0, false
			}
		}
	}
	if !haveM || !haveFields {
		return row, 0, false // the generic path's error
	}
	row.timed = haveT
	if !haveT {
		row.ts = lpNow() // per row, as the record path does
	}
	// Host, as decodeRow sets it: h when sent (an empty h sets none);
	// otherwise the row's own host tag; otherwise "unknown".
	switch {
	case haveH && host != "":
		row.tags = setTag(row.tags, "host", host)
	case haveH:
	case !hasTag(row.tags, "host"):
		row.tags = append(row.tags, LPTag{Key: "host", Value: "unknown"})
	}
	return row, sanitized, true
}

// decodeRowTime is a row's "t" in µs (unit by magnitude, as extractTimestamp
// reads it); present false for nil. Only positive integers are taken.
func decodeRowTime(dec *msgpack.Decoder) (ts int64, present, ok bool) {
	c, err := dec.PeekCode()
	if err != nil {
		return 0, false, false
	}
	if c == 0xc0 {
		return 0, false, dec.Skip() == nil
	}
	if !isIntCode(c) {
		return 0, false, false
	}
	var v int64
	if c == 0xcf { // uint64
		u, err := dec.DecodeUint64()
		if err != nil || u > math.MaxInt64 {
			return 0, false, false
		}
		v = int64(u)
	} else {
		v, err = dec.DecodeInt64()
		if err != nil {
			return 0, false, false
		}
	}
	if v <= 0 {
		return 0, false, false
	}
	switch {
	case v < 1e10:
		return v * 1_000_000, true, true
	case v < 1e13:
		return v * 1_000, true, true
	case v < 1e16:
		return v, true, true
	default:
		return v / 1000, true, true
	}
}

// decodeRowHost is a row's "h" as extractHost reads it (a string, or an
// integer as "host_<v>"); present false for nil; other types: not ok.
func (d *MessagePackDecoder) decodeRowHost(dec *msgpack.Decoder) (string, bool, bool) {
	c, err := dec.PeekCode()
	if err != nil {
		return "", false, false
	}
	switch {
	case c == 0xc0:
		return "", false, dec.Skip() == nil
	case isStrCode(c):
		s, err := dec.DecodeString()
		s, _ = SanitizeUTF8(s) // a tag value, made valid UTF-8 as the others
		return s, true, err == nil
	case isIntCode(c):
		if c == 0xcf {
			u, err := dec.DecodeUint64()
			return fmt.Sprintf("host_%d", u), true, err == nil
		}
		v, err := dec.DecodeInt64()
		return fmt.Sprintf("host_%d", v), true, err == nil
	}
	return "", false, false
}

// decodeRowFields is a row's "fields" map: integer (int64, or uint64 above
// int64), float64, string (made valid UTF-8), bool, or nil (a NULL) values.
func decodeRowFields(dec *msgpack.Decoder, size int) ([]LPField, int, bool) {
	c, err := dec.PeekCode()
	if err != nil || !isMapCode(c) {
		return nil, 0, false
	}
	n, err := dec.DecodeMapLen()
	if err != nil || n < 0 || n > size { // each entry takes at least two bytes
		return nil, 0, false
	}
	fields := make([]LPField, 0, n)
	var seen keySet
	sanitized := 0
	for i := 0; i < n; i++ {
		kc, err := dec.PeekCode()
		if err != nil || !isStrCode(kc) {
			return nil, 0, false
		}
		key, err := dec.DecodeString()
		if err != nil {
			return nil, 0, false
		}
		if !seen.add(key) {
			return nil, 0, false // repeated key
		}
		vc, err := dec.PeekCode()
		if err != nil {
			return nil, 0, false
		}
		var v interface{}
		switch {
		case vc == 0xc0:
			if dec.Skip() != nil {
				return nil, 0, false
			}
		case vc == 0xcf: // uint64
			u, err := dec.DecodeUint64()
			if err != nil {
				return nil, 0, false
			}
			if u <= math.MaxInt64 {
				v = int64(u)
			} else {
				v = u
			}
		case isIntCode(vc):
			x, err := dec.DecodeInt64()
			if err != nil {
				return nil, 0, false
			}
			v = x
		case vc == 0xcb: // float64 (float32 goes to the generic path)
			x, err := dec.DecodeFloat64()
			if err != nil {
				return nil, 0, false
			}
			v = x
		case isStrCode(vc):
			s, err := dec.DecodeString()
			if err != nil {
				return nil, 0, false
			}
			if clean, modified := SanitizeUTF8(s); modified {
				s = clean
				sanitized++
			}
			v = s
		case isBoolCode(vc):
			x, err := dec.DecodeBool()
			if err != nil {
				return nil, 0, false
			}
			v = x
		default:
			return nil, 0, false
		}
		fields = append(fields, LPField{Key: key, Value: v})
	}
	return fields, sanitized, true
}

// decodeRowTags is a row's "tags" map of strings (made valid UTF-8).
func decodeRowTags(dec *msgpack.Decoder, size int) ([]LPTag, int, bool) {
	c, err := dec.PeekCode()
	if err != nil || !isMapCode(c) {
		return nil, 0, false
	}
	n, err := dec.DecodeMapLen()
	if err != nil || n < 0 || n > size {
		return nil, 0, false
	}
	tags := make([]LPTag, 0, n+1) // +1: the host tag
	var seen keySet
	sanitized := 0
	for i := 0; i < n; i++ {
		kc, err := dec.PeekCode()
		if err != nil || !isStrCode(kc) {
			return nil, 0, false
		}
		key, err := dec.DecodeString()
		if err != nil {
			return nil, 0, false
		}
		if !seen.add(key) {
			return nil, 0, false
		}
		vc, err := dec.PeekCode()
		if err != nil || !isStrCode(vc) {
			return nil, 0, false // the record path writes other types with %v
		}
		v, err := dec.DecodeString()
		if err != nil {
			return nil, 0, false
		}
		if clean, modified := SanitizeUTF8(v); modified {
			v = clean
			sanitized++
		}
		tags = append(tags, LPTag{Key: key, Value: v})
	}
	return tags, sanitized, true
}

// keySet tells repeated keys of one map apart: a linear scan while the map
// is small (rows have a handful of keys), a hash set past keySetLinear keys
// (a map of 100,000 keys would otherwise cost 10^10 comparisons).
type keySet struct {
	keys []string
	set  map[string]struct{}
}

const keySetLinear = 16

// add reports false if key was already added.
func (k *keySet) add(key string) bool {
	if k.set != nil {
		if _, dup := k.set[key]; dup {
			return false
		}
		k.set[key] = struct{}{}
		return true
	}
	for _, s := range k.keys {
		if s == key {
			return false
		}
	}
	k.keys = append(k.keys, key)
	if len(k.keys) > keySetLinear {
		k.set = make(map[string]struct{}, 2*len(k.keys))
		for _, s := range k.keys {
			k.set[s] = struct{}{}
		}
	}
	return true
}

func hasTag(tags []LPTag, key string) bool {
	for _, t := range tags {
		if t.Key == key {
			return true
		}
	}
	return false
}

func setTag(tags []LPTag, key, value string) []LPTag {
	for i := range tags {
		if tags[i].Key == key {
			tags[i].Value = value
			return tags
		}
	}
	return append(tags, LPTag{Key: key, Value: value})
}
