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
		return fmt.Errorf("two keys of one point write this column")
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
	hint int // capacity of a new column
	// folds maps each column's FoldColumnName to its spelling (#1342).
	folds map[string]string
}

// ParseLPTyped parses a line-protocol chunk straight into one typed batch per
// measurement: the batch a write of the chunk's records (BatchToColumnar,
// then the buffer's typing) would make, with each field column typed by kinds
// (measurement -> column -> kind, from a validation pass over the whole file)
// and no per-line maps or boxed values. A measurement other than filter
// (when set) is skipped. A value that does not fit its column's kind is an
// error.
func (p *LineProtocolParser) ParseLPTyped(data []byte, precision, filter string, kinds map[string]map[string]LPKind) (map[string]*TypedColumnBatch, map[string]int, error) {
	if kinds == nil {
		kinds = map[string]map[string]LPKind{} // every column is then "has no kind"
	}
	batches, rows, _, err := p.parseLPTyped(data, precision, filter, kinds, nil)
	return batches, rows, err
}

// LPDrop counts the points of a write dropped for one reason, measurement
// and column.
type LPDrop struct {
	Measurement string
	Column      string // the offending tag or field name or column ("" for a key named exactly "time")
	Reason      string // LPDropTime, LPDropName, LPDropCase, LPDropSameColumn, LPDropKind
	Kind        LPKind // the column's kind (LPDropKind)
	ValueType   string // the dropped value's Go type (LPDropKind)
	Points      int
}

// Reasons a write drops a point.
const (
	LPDropTime       = ColumnNameTimeAlike
	LPDropName       = ColumnNameNotUTF8
	LPDropCase       = ColumnNameCaseTwin
	LPDropSameColumn = "two keys of one point write one column (a field named like a tag is stored as \"<name>_value\")"
	LPDropKind       = "a value that does not fit its column's type (the column's first value decides it)"
)

// ParseLPWrite is ParseLPTyped for a write: each column is typed by its first
// value in data, as a write of the body's records typed it, and a point that
// cannot be written as one row is dropped (counted in drops) while the rest
// are written, as InfluxDB does: a point with a `time` key, one whose keys
// land in one column, one with a value that does not fit its column's type.
// A dropped point sets no column's type.
func (p *LineProtocolParser) ParseLPWrite(data []byte, precision string) (map[string]*TypedColumnBatch, map[string]int, []LPDrop, error) {
	return p.parseLPTyped(data, precision, "", nil, nil)
}

// ParseLPWriteKnown is ParseLPWrite that also drops a point whose new column
// differs only in letter case from a column known gives for its measurement
// (one the measurement was written with before, #1342). known may be nil.
func (p *LineProtocolParser) ParseLPWriteKnown(data []byte, precision string, known KnownColumns) (map[string]*TypedColumnBatch, map[string]int, []LPDrop, error) {
	return p.parseLPTyped(data, precision, "", nil, known)
}

// lpMaxColumnHint bounds the rows a new column reserves up front.
const lpMaxColumnHint = 1 << 16

type lpDropKey struct{ measurement, column, reason string }

// lpPoint is one point for lpBuilder: its tags, and its fields with the
// column each is written to (cols[i] for fields[i]; a nil Value is a NULL,
// which only msgpack rows send), resolved by the caller:
// per line for line protocol (LPFieldColumn), per measurement for msgpack
// rows (rowsToColumnar's rule).
type lpPoint struct {
	measurement string
	ts          int64
	tags        []LPTag
	fields      []LPField
	cols        []string
	// renamed: a field was written under "<name>_value" for a tag of
	// another point (msgpack rows); only then can a point with no tags of
	// its own have two keys in one column.
	renamed bool
}

// lpBuilder builds one typed batch per measurement from points. In infer
// mode (kinds nil) a column's kind is its first value's, and a point that
// cannot be written as one row is dropped and counted (LPDrop); otherwise
// kinds gives every column's kind and a value that does not fit is an error.
type lpBuilder struct {
	infer bool
	kinds map[string]map[string]LPKind
	hint  int
	ms    map[string]*lpMeasurement
	drops map[lpDropKey]*LPDrop
	// nullCols are, per measurement, columns that only ever had NULL values
	// (msgpack rows: a field sent as nil); see finish.
	nullCols map[string]map[string]struct{}
	// known gives each measurement's stored column spellings (#1342), asked
	// once per measurement; knownBy caches the answer.
	known    KnownColumns
	knownBy  map[string]map[string]string
	newCols  []string          // fits' scratch: the point's new columns
	newFolds map[string]string // fits' scratch: their folds, cleared per point
}

func newLPBuilder(kinds map[string]map[string]LPKind, hint int) *lpBuilder {
	return &lpBuilder{infer: kinds == nil, kinds: kinds, hint: hint, ms: map[string]*lpMeasurement{}}
}

func (b *lpBuilder) drop(measurement, column, reason string, kind LPKind, v interface{}) {
	if b.drops == nil {
		b.drops = map[lpDropKey]*LPDrop{}
	}
	key := lpDropKey{measurement, column, reason}
	d := b.drops[key]
	if d == nil {
		d = &LPDrop{Measurement: measurement, Column: column, Reason: reason, Kind: kind}
		if v != nil {
			d.ValueType = fmt.Sprintf("%T", v)
		}
		b.drops[key] = d
	}
	d.Points++
}

// add adds p as one row of its measurement, or (infer mode) drops it.
func (b *lpBuilder) add(p *lpPoint) error {
	m := b.ms[p.measurement]
	if b.infer && !b.fits(p, m) {
		return nil
	}
	mk := b.kinds[p.measurement]
	if m == nil {
		m = &lpMeasurement{cols: map[string]*lpColumn{}, folds: map[string]string{}, time: make([]int64, 0, b.hint), hint: b.hint}
		b.ms[p.measurement] = m
		b.hint = 0
	}
	row := len(m.time)
	m.time = append(m.time, p.ts)
	col := func(name string, tag bool, v interface{}) (*lpColumn, error) {
		c := m.cols[name]
		if c == nil {
			k := LPKindString
			switch {
			case tag:
			case b.infer:
				k = LPKindOf(v)
			default:
				k = mk[name]
			}
			if k == LPKindNone {
				return nil, fmt.Errorf("measurement %q column %q has no kind", p.measurement, name)
			}
			c = newLPColumn(k, tag, m.hint)
			m.cols[name] = c
			m.folds[FoldColumnName(name)] = name
		}
		c.tag = c.tag || tag
		return c, nil
	}
	for _, t := range p.tags {
		c, err := col(t.Key, true, t.Value)
		if err != nil {
			return err
		}
		if err := c.set(row, t.Value); err != nil {
			return fmt.Errorf("measurement %q column %q: %w", p.measurement, t.Key, err)
		}
	}
	for i, f := range p.fields {
		name := p.cols[i]
		if f.Value == nil {
			b.nullColumn(p.measurement, name) // a NULL: the row's cell stays empty
			if k := FoldColumnName(name); m.folds[k] == "" {
				m.folds[k] = name // a later twin of a NULL-only column is one too (#1342)
			}
			continue
		}
		c, err := col(name, false, f.Value)
		if err != nil {
			return err
		}
		if err := c.set(row, f.Value); err != nil {
			return fmt.Errorf("measurement %q column %q: %w", p.measurement, name, err)
		}
	}
	return nil
}

// fits reports whether p can be written as one row of m (nil: the
// measurement has no rows yet); if not, it records why. It checks the whole
// point before any of it is written, so a dropped point leaves no partial
// row and sets no column's type.
func (b *lpBuilder) fits(p *lpPoint, m *lpMeasurement) bool {
	if column, clash := pointColumnClash(p); clash {
		b.drop(p.measurement, column, LPDropSameColumn, LPKindNone, nil)
		return false
	}
	kindOf := func(column string) (LPKind, bool) {
		if m == nil {
			return LPKindNone, false
		}
		if c := m.cols[column]; c != nil {
			return c.kind, true
		}
		return LPKindNone, false
	}
	// A column the measurement does not have yet must have a name Arc can
	// read back (#1342). One already in m passed when it was added, so the
	// steady state pays nothing beyond the kind lookup it already does.
	b.newCols = b.newCols[:0]
	for _, t := range p.tags {
		k, ok := kindOf(t.Key)
		if !ok {
			if !b.newColumnName(p, t.Key, t.Key) {
				return false
			}
			continue
		}
		if k != LPKindString {
			b.drop(p.measurement, t.Key, LPDropKind, k, t.Value)
			return false
		}
	}
	for i, f := range p.fields {
		column := p.cols[i]
		k, ok := kindOf(column)
		if !ok && !b.newColumnName(p, f.Key, column) {
			return false
		}
		if f.Value == nil {
			continue // a NULL fits any column
		}
		if !ok {
			k = LPKindOf(f.Value) // a new column: its first value must fit it too
		}
		if !LPValueFits(k, f.Value) {
			b.drop(p.measurement, column, LPDropKind, k, f.Value)
			return false
		}
	}
	return len(b.newCols) == 0 || b.newColumnsFit(p, m)
}

// newColumnName checks the name of a column the measurement does not have
// yet (key is the tag or field key it comes from): not valid UTF-8, or "time"
// in any letter case, drops the point. An acceptable one is queued for
// newColumnsFit.
func (b *lpBuilder) newColumnName(p *lpPoint, key, column string) bool {
	switch ColumnNameProblem(key) {
	case "":
		b.newCols = append(b.newCols, column)
		return true
	case ColumnNameTimeAlike:
		col := key
		if key == "time" {
			col = ""
		}
		b.drop(p.measurement, col, LPDropTime, LPKindNone, nil)
	default:
		b.drop(p.measurement, key, LPDropName, LPKindNone, nil)
	}
	return false
}

// newColumnsFit drops the point when one of its new columns differs only in
// letter case from another column: of the measurement in this write, of the
// point itself, or (via known) of the measurement's earlier writes.
func (b *lpBuilder) newColumnsFit(p *lpPoint, m *lpMeasurement) bool {
	known := b.knownColumns(p.measurement)
	var batch map[string]string
	if m != nil {
		batch = m.folds
	}
	if b.newFolds == nil {
		b.newFolds = map[string]string{}
	}
	clear(b.newFolds)
	for _, c := range b.newCols {
		if _, twin := ColumnCaseTwin(c, batch, known); twin {
			b.drop(p.measurement, c, LPDropCase, LPKindNone, nil)
			return false
		}
		f := FoldColumnName(c)
		if o, ok := b.newFolds[f]; ok && o != c {
			b.drop(p.measurement, c, LPDropCase, LPKindNone, nil)
			return false
		}
		b.newFolds[f] = c
	}
	return true
}

// knownColumns is known's answer for measurement, asked once per write.
func (b *lpBuilder) knownColumns(measurement string) map[string]string {
	if b.known == nil {
		return nil
	}
	if k, ok := b.knownBy[measurement]; ok {
		return k
	}
	if b.knownBy == nil {
		b.knownBy = map[string]map[string]string{}
	}
	k := b.known(measurement)
	b.knownBy[measurement] = k
	return k
}

// pointColumnClash reports a column two keys of p resolve to.
func pointColumnClash(p *lpPoint) (string, bool) {
	if len(p.tags)+len(p.fields) < 2 || (len(p.tags) == 0 && !p.renamed) {
		return "", false
	}
	if len(p.tags) == 0 && len(p.fields) <= 8 {
		for i := range p.cols {
			for j := 0; j < i; j++ {
				if p.cols[i] == p.cols[j] {
					return p.cols[i], true
				}
			}
		}
		return "", false
	}
	cols := make(map[string]struct{}, len(p.tags)+len(p.fields))
	for _, t := range p.tags {
		cols[t.Key] = struct{}{}
	}
	for _, c := range p.cols {
		if _, dup := cols[c]; dup {
			return c, true
		}
		cols[c] = struct{}{}
	}
	return "", false
}

// nullColumn records a column of measurement that got a NULL (a msgpack row
// field sent as nil): if no point gives it a value, finish writes it as an
// all-NULL string column, as the record path does (#337).
func (b *lpBuilder) nullColumn(measurement, column string) {
	if b.nullCols == nil {
		b.nullCols = map[string]map[string]struct{}{}
	}
	if b.nullCols[measurement] == nil {
		b.nullCols[measurement] = map[string]struct{}{}
	}
	b.nullCols[measurement][column] = struct{}{}
}

// finish is the built batches, their row counts, and the drops (sorted).
func (b *lpBuilder) finish() (map[string]*TypedColumnBatch, map[string]int, []LPDrop) {
	out := make(map[string]*TypedColumnBatch, len(b.ms))
	rows := make(map[string]int, len(b.ms))
	for name, m := range b.ms {
		n := len(m.time)
		for cname := range b.nullCols[name] {
			if m.cols[cname] == nil {
				c := newLPColumn(LPKindString, false, 0)
				m.cols[cname] = c
			}
		}
		batch := &TypedColumnBatch{Data: make(map[string]interface{}, len(m.cols)+1), Validity: map[string][]bool{}}
		batch.Data["time"] = exactSlice(m.time)
		for cname, c := range m.cols {
			c.pad(n)
			batch.Data[cname] = c.exactData()
			if c.valid != nil {
				batch.Validity[cname] = c.valid
			}
			if c.tag {
				batch.TagColumns = append(batch.TagColumns, cname)
			}
		}
		if len(batch.TagColumns) > 1 {
			sort.Strings(batch.TagColumns)
		}
		out[name] = batch
		rows[name] = n
	}
	var dropped []LPDrop
	for _, d := range b.drops {
		dropped = append(dropped, *d)
	}
	sort.Slice(dropped, func(i, j int) bool {
		if dropped[i].Measurement != dropped[j].Measurement {
			return dropped[i].Measurement < dropped[j].Measurement
		}
		if dropped[i].Column != dropped[j].Column {
			return dropped[i].Column < dropped[j].Column
		}
		return dropped[i].Reason < dropped[j].Reason
	})
	return out, rows, dropped
}

func (p *LineProtocolParser) parseLPTyped(data []byte, precision, filter string, kinds map[string]map[string]LPKind, known KnownColumns) (map[string]*TypedColumnBatch, map[string]int, []LPDrop, error) {
	// The first measurement's columns are sized for the whole body: exact for
	// a body of one measurement (the common write), at most the body's line
	// count per column otherwise. Later measurements grow by append: sizing
	// each of them for the body would reserve measurements x lines x columns.
	// Capped: blank, comment and invalid lines count but add no rows, and a
	// larger body grows by append from the cap.
	b := newLPBuilder(kinds, min(bytes.Count(data, []byte{'\n'})+1, lpMaxColumnHint))
	b.known = known
	var pt lpPoint
	err := p.LexBatch(data, precision, func(l *LPLine) error {
		if filter != "" && l.Measurement != filter {
			return nil
		}
		pt.measurement, pt.ts, pt.tags, pt.fields = l.Measurement, l.Timestamp, l.Tags, l.Fields
		pt.cols = pt.cols[:0]
		for _, f := range l.Fields {
			pt.cols = append(pt.cols, LPFieldColumn(l, f.Key))
		}
		return b.add(&pt)
	})
	if err != nil {
		return nil, nil, nil, err
	}
	out, rows, dropped := b.finish()
	return out, rows, dropped, nil
}

func newLPColumn(k LPKind, tag bool, hint int) *lpColumn {
	c := &lpColumn{kind: k, tag: tag}
	switch k {
	case LPKindInt:
		c.ints = make([]int64, 0, hint)
	case LPKindFloat:
		c.flts = make([]float64, 0, hint)
	case LPKindString:
		c.strs = make([]string, 0, hint)
	case LPKindBool:
		c.bools = make([]bool, 0, hint)
	}
	return c
}

// exactData is data with no more than 1/8 unused capacity: the buffer counts
// a batch's bytes by length, so slack would be held unseen until the flush.
func (c *lpColumn) exactData() interface{} {
	switch c.kind {
	case LPKindInt:
		return exactSlice(c.ints)
	case LPKindFloat:
		return exactSlice(c.flts)
	case LPKindString:
		return exactSlice(c.strs)
	default:
		return exactSlice(c.bools)
	}
}

func exactSlice[T any](v []T) []T {
	if cap(v) > len(v)+len(v)/8 {
		out := make([]T, len(v))
		copy(out, v)
		return out
	}
	return v
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
