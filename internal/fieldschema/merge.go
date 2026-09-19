package fieldschema

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// TimeColumn is the column every Arc measurement carries and that leads the
// anchor's column order.
const TimeColumn = "time"

// Normalize turns a file schema into anchor form: nullable fields, no
// metadata, and a deterministic order. Order matters because the anchor
// fixes SELECT * for the measurement: time first, then tag columns sorted,
// then every other column sorted. Ingest infers file schemas from a Go map
// and so writes columns in a random order per process; two nodes creating
// the same anchor concurrently must still agree.
func Normalize(schema *arrow.Schema, tagColumns []string) *arrow.Schema {
	if schema == nil {
		return nil
	}
	tags := make(map[string]bool, len(tagColumns))
	for _, t := range tagColumns {
		tags[t] = true
	}
	if schema.Metadata().Len() > 0 {
		// Files written by ingest carry "arc:tags"; honor it when the caller
		// passed nothing so a schema read back from a file orders the same.
		if v, ok := schema.Metadata().GetValue("arc:tags"); ok && len(tagColumns) == 0 {
			for _, t := range strings.Split(v, ",") {
				if t != "" {
					tags[t] = true
				}
			}
		}
	}
	var timeField *arrow.Field
	var tagFields, otherFields []arrow.Field
	for _, f := range schema.Fields() {
		nf := arrow.Field{Name: f.Name, Type: f.Type, Nullable: true}
		switch {
		case f.Name == TimeColumn:
			c := nf
			timeField = &c
		case tags[f.Name]:
			tagFields = append(tagFields, nf)
		default:
			otherFields = append(otherFields, nf)
		}
	}
	byName := func(fs []arrow.Field) { sort.Slice(fs, func(i, j int) bool { return fs[i].Name < fs[j].Name }) }
	byName(tagFields)
	byName(otherFields)
	out := make([]arrow.Field, 0, schema.NumFields())
	if timeField != nil {
		out = append(out, *timeField)
	}
	out = append(out, tagFields...)
	out = append(out, otherFields...)
	return arrow.NewSchema(out, nil)
}

func stripMetadata(s *arrow.Schema) *arrow.Schema {
	if s == nil {
		return nil
	}
	fields := make([]arrow.Field, s.NumFields())
	for i, f := range s.Fields() {
		fields[i] = arrow.Field{Name: f.Name, Type: f.Type, Nullable: true}
	}
	return arrow.NewSchema(fields, nil)
}

// Conflict records a field whose incoming type could not be ordered against
// the stored one; the stored type was kept.
type Conflict struct {
	Field    string
	Stored   arrow.DataType
	Incoming arrow.DataType
}

func (c Conflict) String() string {
	return fmt.Sprintf("%s: stored %s, incoming %s", c.Field, c.Stored, c.Incoming)
}

// Merge folds incoming into stored. Fields new to stored are appended in
// incoming's (already normalized) order; stored order is kept, which is what
// lets every node converge on one order once the stored anchor is re-read
// as the base. A field present in both keeps the NARROWER of the two types,
// so the anchor always sits at or below every file's type in DuckDB's
// promotion order and can never widen what a query binds. Types that cannot
// be ordered fall to Bottom and are reported. changed is true when the
// result differs from stored. A nil stored returns incoming as is.
func Merge(stored, incoming *arrow.Schema) (merged *arrow.Schema, changed bool, conflicts []Conflict) {
	if incoming == nil {
		return stored, false, nil
	}
	if stored == nil {
		return incoming, true, nil
	}
	fields := make([]arrow.Field, 0, stored.NumFields()+incoming.NumFields())
	index := make(map[string]int, stored.NumFields())
	for i, f := range stored.Fields() {
		fields = append(fields, arrow.Field{Name: f.Name, Type: f.Type, Nullable: true})
		index[f.Name] = i
	}
	for _, f := range incoming.Fields() {
		i, ok := index[f.Name]
		if !ok {
			fields = append(fields, arrow.Field{Name: f.Name, Type: f.Type, Nullable: true})
			index[f.Name] = len(fields) - 1
			changed = true
			continue
		}
		if arrow.TypeEqual(fields[i].Type, f.Type) {
			continue
		}
		narrow, comparable := narrower(fields[i].Type, f.Type)
		if !comparable {
			conflicts = append(conflicts, Conflict{Field: f.Name, Stored: fields[i].Type, Incoming: f.Type})
			narrow = Bottom
		}
		if !arrow.TypeEqual(narrow, fields[i].Type) {
			fields[i].Type = narrow
			changed = true
		}
	}
	return arrow.NewSchema(fields, nil), changed, conflicts
}

// Covers reports whether every field of incoming is present in stored with a
// type that is the stored type or wider than it, i.e. whether Merge would
// change nothing. It is the ingest fast path and must not allocate a schema.
func Covers(stored, incoming *arrow.Schema) bool {
	if stored == nil {
		return incoming == nil || incoming.NumFields() == 0
	}
	if incoming == nil {
		return true
	}
	for _, f := range incoming.Fields() {
		idx := stored.FieldIndices(f.Name)
		if len(idx) == 0 {
			return false
		}
		st := stored.Field(idx[0]).Type
		if arrow.TypeEqual(st, f.Type) {
			continue
		}
		narrow, comparable := narrower(st, f.Type)
		if !comparable {
			narrow = Bottom
		}
		if !arrow.TypeEqual(narrow, st) {
			return false
		}
	}
	return true
}

// Bottom is the type DuckDB promotes to ANY other type when union_by_name
// unifies files (verified for every numeric width, DECIMAL, DATE, TIMESTAMP
// with and without zone, VARCHAR, BLOB, UUID, INTERVAL, LIST and STRUCT). It
// is the anchor type for a field whose observed types cannot be ordered: an
// anchor at Bottom never changes what a range whose files carry the column
// binds, and a range whose files lack it binds a BOOLEAN NULL column.
var Bottom arrow.DataType = arrow.FixedWidthTypes.Boolean

// rank places a type on the ladder DuckDB promotes along when union_by_name
// unifies files: BOOLEAN < TINYINT < SMALLINT < INTEGER < BIGINT < FLOAT <
// DOUBLE < VARCHAR. Types off the ladder return ok=false and are ordered by
// narrower's special cases or fall to Bottom.
func rank(t arrow.DataType) (int, bool) {
	switch t.ID() {
	case arrow.BOOL:
		return 0, true
	case arrow.INT8:
		return 1, true
	case arrow.INT16:
		return 2, true
	case arrow.INT32:
		return 3, true
	case arrow.INT64:
		return 4, true
	case arrow.FLOAT32:
		return 5, true
	case arrow.FLOAT64:
		return 6, true
	case arrow.STRING, arrow.LARGE_STRING:
		return 7, true
	}
	return 0, false
}

// narrower returns the narrower of a and b in DuckDB's promotion order and
// whether the two are comparable at all. Verified against DuckDB: an anchor
// at the narrower type never changes the bound type of a range whose files
// carry the column (BOOLEAN+BIGINT binds BIGINT, DECIMAL+DOUBLE binds DOUBLE,
// TIMESTAMP+TIMESTAMPTZ binds TIMESTAMPTZ, INT32+BIGINT binds BIGINT).
func narrower(a, b arrow.DataType) (arrow.DataType, bool) {
	if arrow.TypeEqual(a, b) {
		return a, true
	}
	// Bottom is below everything by construction.
	if a.ID() == arrow.BOOL {
		return a, true
	}
	if b.ID() == arrow.BOOL {
		return b, true
	}
	ra, oka := rank(a)
	rb, okb := rank(b)
	if oka && okb {
		if ra <= rb {
			return a, true
		}
		return b, true
	}
	isDecimal := func(t arrow.DataType) bool { return t.ID() == arrow.DECIMAL128 || t.ID() == arrow.DECIMAL256 }
	isFloat := func(t arrow.DataType) bool { return t.ID() == arrow.FLOAT32 || t.ID() == arrow.FLOAT64 }
	switch {
	case isDecimal(a) && isFloat(b):
		return a, true
	case isFloat(a) && isDecimal(b):
		return b, true
	case isDecimal(a) && isDecimal(b):
		// DECIMAL(p1,s1) is below DECIMAL(p2,s2) when both bounds are:
		// DuckDB binds the wider of the two for the pair.
		da, db := a.(arrow.DecimalType), b.(arrow.DecimalType)
		if da.GetPrecision() <= db.GetPrecision() && da.GetScale() <= db.GetScale() {
			return a, true
		}
		if db.GetPrecision() <= da.GetPrecision() && db.GetScale() <= da.GetScale() {
			return b, true
		}
		return nil, false
	case a.ID() == arrow.TIMESTAMP && b.ID() == arrow.TIMESTAMP:
		ta, tb := a.(*arrow.TimestampType), b.(*arrow.TimestampType)
		if ta.Unit != tb.Unit {
			return nil, false
		}
		// A naive timestamp is below the zoned one: DuckDB binds TIMESTAMPTZ
		// for the pair, so the naive anchor changes nothing.
		if ta.TimeZone == "" {
			return a, true
		}
		if tb.TimeZone == "" {
			return b, true
		}
		return nil, false
	}
	// BIGINT vs DECIMAL, DATE vs TIMESTAMP, VARCHAR vs BLOB and everything
	// else: DuckDB has a promotion for some of these but neither side is
	// below the other, so the caller falls to Bottom and logs it.
	return nil, false
}

// Fingerprint identifies a schema by its ordered names and types, for the
// change checks that decide whether a stored or materialized anchor is stale.
func Fingerprint(s *arrow.Schema) string {
	if s == nil {
		return ""
	}
	h := sha256.New()
	for _, f := range s.Fields() {
		h.Write([]byte(f.Name))
		h.Write([]byte{0})
		h.Write([]byte(f.Type.String()))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
