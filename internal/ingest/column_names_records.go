package ingest

import (
	"sort"

	"github.com/basekick-labs/arc/pkg/models"
)

// FilterRecordNames drops, from a decoded write (msgpack: a record, a
// columnar record, a typed columnar record, or a []interface{} of them), every
// record whose tag or field names Arc refuses (#1342): ColumnNameProblem, or a
// name that differs only in letter case from another of the same measurement
// in this write or in known. A columnar record is dropped whole: its names
// belong to all its rows. The earliest spelling in the write wins against a
// later one. It returns what is left, the drops by measurement, name and
// reason (Points counts rows, for the partial-write answer), and how many
// records it removed; records with acceptable names are returned untouched,
// in order.
func FilterRecordNames(records interface{}, known KnownColumns) (interface{}, []LPDrop, int) {
	f := NewRecordNameFilter(known)
	switch r := records.(type) {
	case []interface{}:
		kept := r[:0:0]
		for _, item := range r {
			if f.Keep(item) {
				kept = append(kept, item)
			}
		}
		if len(f.Drops()) == 0 {
			return records, nil, 0
		}
		return kept, f.Drops(), len(r) - len(kept)
	default:
		if f.Keep(records) {
			return records, nil, 0
		}
		return []interface{}{}, f.Drops(), 1
	}
}

// NewRecordNameFilter is FilterRecordNames one record at a time: Keep
// reports whether a record's names are acceptable, Drops lists the rest.
func NewRecordNameFilter(known KnownColumns) *RecordNameFilter {
	return &RecordNameFilter{recordNameFilter{known: known, folds: map[string]map[string]string{}, knownBy: map[string]map[string]string{}}}
}

// RecordNameFilter: see NewRecordNameFilter.
type RecordNameFilter struct{ f recordNameFilter }

// Keep reports whether item's names are acceptable (see FilterRecordNames).
func (r *RecordNameFilter) Keep(item interface{}) bool { return r.f.ok(item) }

// Drops are the records Keep refused, by measurement, name and reason.
func (r *RecordNameFilter) Drops() []LPDrop { return r.f.drops }

type recordNameFilter struct {
	known   KnownColumns
	knownBy map[string]map[string]string
	folds   map[string]map[string]string // measurement -> fold -> spelling, of records kept
	drops   []LPDrop
}

func (f *recordNameFilter) knownFor(measurement string) map[string]string {
	if f.known == nil {
		return nil
	}
	k, ok := f.knownBy[measurement]
	if !ok {
		k = f.known(measurement)
		f.knownBy[measurement] = k
	}
	return k
}

// ok checks one record and, if it is kept, remembers its spellings.
func (f *recordNameFilter) ok(item interface{}) bool {
	var measurement string
	var names []string
	rows := 1
	switch r := item.(type) {
	case *models.ColumnarRecord:
		measurement = r.Measurement
		for name, col := range r.Columns {
			if name == "time" {
				rows = len(col)
				continue
			}
			names = append(names, name)
		}
		if rows == 1 {
			for _, col := range r.Columns {
				rows = len(col)
				break
			}
		}
	case *TypedColumnarRecord:
		measurement, rows = r.Measurement, r.NumRecords
		if r.Batch != nil {
			for name := range r.Batch.Data {
				if name != "time" {
					names = append(names, name)
				}
			}
		}
	case *models.Record:
		measurement = r.Measurement
		for name := range r.Tags {
			names = append(names, name)
		}
		for name := range r.Fields {
			if name == "time" {
				continue // the record path's own drop (rowsToColumnar) answers it
			}
			names = append(names, name)
		}
	default:
		return true
	}
	// Deterministic: a record's names come from maps.
	sort.Strings(names)
	seen := f.folds[measurement]
	known := f.knownFor(measurement)
	mine := make(map[string]string, len(names))
	for _, name := range names {
		if p := ColumnNameProblem(name); p != "" {
			f.drop(measurement, name, p, rows)
			return false
		}
		fold := FoldColumnName(name)
		if other, ok := mine[fold]; ok && other != name {
			f.drop(measurement, name, ColumnNameCaseTwin, rows)
			return false
		}
		if _, twin := ColumnCaseTwin(name, seen, known); twin {
			f.drop(measurement, name, ColumnNameCaseTwin, rows)
			return false
		}
		mine[fold] = name
	}
	if seen == nil {
		seen = map[string]string{}
		f.folds[measurement] = seen
	}
	for fold, name := range mine {
		if _, ok := seen[fold]; !ok {
			seen[fold] = name
		}
	}
	return true
}

func (f *recordNameFilter) drop(measurement, column, reason string, rows int) {
	for i := range f.drops {
		d := &f.drops[i]
		if d.Measurement == measurement && d.Column == column && d.Reason == reason {
			d.Points += rows
			return
		}
	}
	f.drops = append(f.drops, LPDrop{Measurement: measurement, Column: column, Reason: reason, Points: rows})
}

// StripRecordNames removes, in place, the tags and fields of row records
// (*models.Record, as MQTT builds them) whose names Arc refuses (#1342),
// keeping the record and its other values. For a source that has no client to
// answer, a payload key like Tasmota's "Time" must not cost the whole reading.
// Of two names differing only in letter case, the one seen first (in known,
// then earlier records, then in byte order within a record) stays. It returns
// the names removed, by measurement and reason; Points counts records.
func StripRecordNames(records []interface{}, known KnownColumns) []LPDrop {
	f := NewRecordNameFilter(known)
	for _, item := range records {
		r, ok := item.(*models.Record)
		if !ok {
			continue
		}
		seen := f.f.folds[r.Measurement]
		if seen == nil {
			seen = map[string]string{}
			f.f.folds[r.Measurement] = seen
		}
		k := f.f.knownFor(r.Measurement)
		strip := func(name string) (string, bool) {
			if name == "time" {
				return "", false // the record path's own rule answers it
			}
			if p := ColumnNameProblem(name); p != "" {
				return p, true
			}
			if _, twin := ColumnCaseTwin(name, seen, k); twin {
				return ColumnNameCaseTwin, true
			}
			seen[FoldColumnName(name)] = name
			return "", false
		}
		names := make([]string, 0, len(r.Tags)+len(r.Fields))
		for name := range r.Tags {
			names = append(names, name)
		}
		for name := range r.Fields {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if reason, bad := strip(name); bad {
				delete(r.Tags, name)
				delete(r.Fields, name)
				f.f.drop(r.Measurement, name, reason, 1)
			}
		}
	}
	return f.Drops()
}
