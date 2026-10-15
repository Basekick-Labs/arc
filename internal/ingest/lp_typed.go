package ingest

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"sort"

	"github.com/basekick-labs/arc/pkg/models"
)

// LPKind is the type of a line-protocol column: a field's column is typed by
// its first value in the file (as a write of the whole file typed it), a tag's
// column is a string.
type LPKind uint8

const (
	LPKindNone LPKind = iota
	LPKindInt
	LPKindFloat
	LPKindString
	LPKindBool
)

func (k LPKind) String() string {
	switch k {
	case LPKindInt:
		return "integer"
	case LPKindFloat:
		return "float"
	case LPKindString:
		return "string"
	case LPKindBool:
		return "boolean"
	}
	return "none"
}

// LPKindOf is the kind of a field value (int64, uint64, float64, string,
// bool).
func LPKindOf(v interface{}) LPKind {
	switch v.(type) {
	case int64, uint64:
		return LPKindInt
	case float64:
		return LPKindFloat
	case string:
		return LPKindString
	case bool:
		return LPKindBool
	}
	return LPKindNone
}

// LPValueFits reports whether v can be stored in a column of kind k, with the
// conversions a write applies (a float in an integer column is truncated, an
// integer in a float column widened).
func LPValueFits(k LPKind, v interface{}) bool {
	switch k {
	case LPKindInt:
		switch x := v.(type) {
		case int64:
			return true
		case uint64:
			return x <= math.MaxInt64
		case float64:
			return !math.IsNaN(x) && !math.IsInf(x, 0) && x < 1<<63 && x >= -1<<63
		}
	case LPKindFloat:
		switch v.(type) {
		case int64, uint64, float64:
			return true
		}
	case LPKindString:
		_, ok := v.(string)
		return ok
	case LPKindBool:
		_, ok := v.(bool)
		return ok
	}
	return false
}

// LPFieldColumn is the column a field of l is written to: a field named like
// one of the line's tags goes to "<name>_value" (BatchToColumnar's rule).
func LPFieldColumn(l *LPLine, field string) string {
	for _, t := range l.Tags {
		if t.Key == field {
			return field + "_value"
		}
	}
	return field
}

// LexBatch parses data line by line and calls fn with each point, its tags
// and fields resolved as a Record's maps resolve them: one entry per key, the
// line's last value for it. The line passed to fn is reused: fn must not keep
// it.
func (p *LineProtocolParser) LexBatch(data []byte, precision string, fn func(l *LPLine) error) error {
	if precision == "" {
		precision = "ns"
	}
	validUTF8 := ValidateUTF8Bytes(data)
	var l LPLine
	seen := map[string]int{}
	for len(data) > 0 {
		var line []byte
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			line, data = data[:i], data[i+1:]
		} else {
			line, data = data, nil
		}
		if !p.lexLine(line, precision, validUTF8, &l) {
			continue
		}
		l.Tags = lastPerKeyTags(l.Tags, seen)
		l.Fields = lastPerKeyFields(l.Fields, seen)
		if err := fn(&l); err != nil {
			return err
		}
	}
	return nil
}

// lastPerKeyTags keeps one tag per key, holding the key's last value, at the
// position of its first occurrence.
func lastPerKeyTags(tags []LPTag, seen map[string]int) []LPTag {
	if len(tags) < 2 {
		return tags
	}
	if len(tags) <= 8 { // a short line: no map
		out := tags[:0]
	next:
		for _, t := range tags {
			for i := range out {
				if out[i].Key == t.Key {
					out[i].Value = t.Value
					continue next
				}
			}
			out = append(out, t)
		}
		return out
	}
	clear(seen)
	out := tags[:0]
	for _, t := range tags {
		if i, ok := seen[t.Key]; ok {
			out[i].Value = t.Value
			continue
		}
		seen[t.Key] = len(out)
		out = append(out, t)
	}
	return out
}

func lastPerKeyFields(fields []LPField, seen map[string]int) []LPField {
	if len(fields) < 2 {
		return fields
	}
	if len(fields) <= 8 { // a short line: no map
		out := fields[:0]
	next:
		for _, f := range fields {
			for i := range out {
				if out[i].Key == f.Key {
					out[i].Value = f.Value
					continue next
				}
			}
			out = append(out, f)
		}
		return out
	}
	clear(seen)
	out := fields[:0]
	for _, f := range fields {
		if i, ok := seen[f.Key]; ok {
			out[i].Value = f.Value
			continue
		}
		seen[f.Key] = len(out)
		out = append(out, f)
	}
	return out
}

// lpColumn builds one typed column of a measurement, row by row.
type lpColumn struct {
	kind  LPKind
	tag   bool
	ints  []int64
	flts  []float64
	strs  []string
	bools []bool
	valid []bool // nil while every row so far has a value
	n     int
}

// pad fills the rows before row with NULL.
func (c *lpColumn) pad(row int) {
	for c.n < row {
		if c.valid == nil {
			c.valid = make([]bool, c.n, max(row, 16))
			for i := range c.valid {
				c.valid[i] = true
			}
		}
		switch c.kind {
		case LPKindInt:
			c.ints = append(c.ints, 0)
		case LPKindFloat:
			c.flts = append(c.flts, 0)
		case LPKindString:
			c.strs = append(c.strs, "")
		case LPKindBool:
			c.bools = append(c.bools, false)
		}
		c.valid = append(c.valid, false)
		c.n++
	}
}

// set writes v at row (rows are added in order). Two keys of a line that
// land in one column (a field "x" next to a tag "x" goes to "x_value", where
// a literal field "x_value" also goes) give one row two values: refused, as
// the validation pass refuses the line.
func (c *lpColumn) set(row int, v interface{}) error {
	if c.n > row {
		return fmt.Errorf("two keys of one line write this column")
	}
	c.pad(row)
	switch c.kind {
	case LPKindInt:
		switch x := v.(type) {
		case int64:
			c.ints = append(c.ints, x)
		case uint64:
			if x <= math.MaxInt64 {
				c.ints = append(c.ints, int64(x))
			} else {
				return fmt.Errorf("unsigned value %d does not fit an integer column", x)
			}
		case float64:
			// float64(MaxInt64) is 2^63, so "<" is the exact bound; NaN
			// fails both comparisons.
			if !(x < math.MaxInt64 && x >= math.MinInt64) {
				return fmt.Errorf("value %v does not fit an integer column", x)
			}
			// big.Float.Int64 truncates toward zero as int64(x) does in range.
			i, _ := new(big.Float).SetFloat64(x).Int64()
			c.ints = append(c.ints, i)
		default:
			return fmt.Errorf("a %T value in an integer column", v)
		}
	case LPKindFloat:
		switch x := v.(type) {
		case float64:
			c.flts = append(c.flts, x)
		case int64:
			c.flts = append(c.flts, float64(x))
		case uint64:
			c.flts = append(c.flts, float64(x))
		default:
			return fmt.Errorf("a %T value in a float column", v)
		}
	case LPKindString:
		x, ok := v.(string)
		if !ok {
			return fmt.Errorf("a %T value in a string column", v)
		}
		c.strs = append(c.strs, x)
	case LPKindBool:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("a %T value in a boolean column", v)
		}
		c.bools = append(c.bools, x)
	default:
		return fmt.Errorf("a column of unknown kind")
	}
	if c.valid != nil {
		c.valid = append(c.valid, true)
	}
	c.n++
	return nil
}

func (c *lpColumn) data() interface{} {
	switch c.kind {
	case LPKindInt:
		return c.ints
	case LPKindFloat:
		return c.flts
	case LPKindString:
		return c.strs
	default:
		return c.bools
	}
}

// lpMeasurement builds the typed batch of one measurement of a chunk.
type lpMeasurement struct {
	time []int64
	cols map[string]*lpColumn
}

// ParseLPTyped parses a line-protocol chunk straight into one typed batch per
// measurement: the batch a write of the chunk's records (BatchToColumnar,
// then the buffer's typing) would make, with each field column typed by kinds
// (measurement -> column -> kind, from a validation pass over the whole file)
// and no per-line maps or boxed values. A measurement other than filter
// (when set) is skipped. A value that does not fit its column's kind is an
// error.
func (p *LineProtocolParser) ParseLPTyped(data []byte, precision, filter string, kinds map[string]map[string]LPKind) (map[string]*TypedColumnBatch, map[string]int, error) {
	ms := map[string]*lpMeasurement{}
	err := p.LexBatch(data, precision, func(l *LPLine) error {
		if filter != "" && l.Measurement != filter {
			return nil
		}
		mk := kinds[l.Measurement]
		m := ms[l.Measurement]
		if m == nil {
			m = &lpMeasurement{cols: map[string]*lpColumn{}}
			ms[l.Measurement] = m
		}
		row := len(m.time)
		m.time = append(m.time, l.Timestamp)
		col := func(name string, tag bool) (*lpColumn, error) {
			c := m.cols[name]
			if c == nil {
				k := LPKindString
				if !tag {
					k = mk[name]
				}
				if k == LPKindNone {
					return nil, fmt.Errorf("measurement %q column %q has no kind", l.Measurement, name)
				}
				c = &lpColumn{kind: k, tag: tag}
				m.cols[name] = c
			}
			c.tag = c.tag || tag
			return c, nil
		}
		for _, t := range l.Tags {
			c, err := col(t.Key, true)
			if err != nil {
				return err
			}
			if err := c.set(row, t.Value); err != nil {
				return fmt.Errorf("measurement %q column %q: %w", l.Measurement, t.Key, err)
			}
		}
		for _, f := range l.Fields {
			name := LPFieldColumn(l, f.Key)
			c, err := col(name, false)
			if err != nil {
				return err
			}
			if err := c.set(row, f.Value); err != nil {
				return fmt.Errorf("measurement %q column %q: %w", l.Measurement, name, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	out := make(map[string]*TypedColumnBatch, len(ms))
	rows := make(map[string]int, len(ms))
	for name, m := range ms {
		n := len(m.time)
		b := &TypedColumnBatch{Data: make(map[string]interface{}, len(m.cols)+1), Validity: map[string][]bool{}}
		b.Data["time"] = m.time
		for cname, c := range m.cols {
			c.pad(n)
			b.Data[cname] = c.data()
			if c.valid != nil {
				b.Validity[cname] = c.valid
			}
			if c.tag {
				b.TagColumns = append(b.TagColumns, cname)
			}
		}
		sort.Strings(b.TagColumns)
		out[name] = b
		rows[name] = n
	}
	return out, rows, nil
}

// LPLineTimeKey reports a tag or field of l named "time" (see
// LPRecordTimeKey). kind is "tag" or "field".
func LPLineTimeKey(l *LPLine) (kind string, found bool) {
	for _, t := range l.Tags {
		if t.Key == "time" {
			return "tag", true
		}
	}
	for _, f := range l.Fields {
		if f.Key == "time" {
			return "field", true
		}
	}
	return "", false
}

// CoerceFirstValues is for the record path (decimal deployments); the
// typed path types every value by kinds.
// CoerceFirstValues converts the first value of each field column to the kind
// the file's first value of that field had: the buffer types a column from
// its first value, and a chunk must type it as the whole file would.
func CoerceFirstValues(rec *models.ColumnarRecord, kinds map[string]LPKind) {
	for name, k := range kinds {
		col := rec.Columns[name]
	first:
		for i, v := range col {
			if v == nil {
				continue
			}
			switch k {
			case LPKindInt:
				// The validation pass checked the values fit; the bounds are
				// checked again at the conversion. A value that does not fit is
				// left as is, and the buffer refuses it.
				switch x := v.(type) {
				case float64:
					// float64(MaxInt64) is 2^63, so "<" is the exact bound;
					// NaN fails both comparisons. big.Float.Int64 truncates
					// toward zero as int64(x) does in range.
					if x < math.MaxInt64 && x >= math.MinInt64 {
						col[i], _ = new(big.Float).SetFloat64(x).Int64()
					}
				case uint64:
					if x <= math.MaxInt64 {
						col[i] = int64(x)
					}
				}
			case LPKindFloat:
				switch x := v.(type) {
				case int64:
					col[i] = float64(x)
				case uint64:
					col[i] = float64(x)
				}
			}
			break first
		}
	}
}

// LPLineColumnClash reports a column two keys of l would both write: a tag
// and a field named "<tag>_value", or two fields where one is renamed to
// "<tag>_value" (LPFieldColumn). Keys are resolved per line, so a plain
// repeated key never clashes.
func LPLineColumnClash(l *LPLine) (string, bool) {
	if len(l.Tags) == 0 {
		return "", false
	}
	cols := make(map[string]struct{}, len(l.Tags)+len(l.Fields))
	for _, t := range l.Tags {
		cols[t.Key] = struct{}{}
	}
	for _, f := range l.Fields {
		c := LPFieldColumn(l, f.Key)
		if _, dup := cols[c]; dup {
			return c, true
		}
		cols[c] = struct{}{}
	}
	return "", false
}
