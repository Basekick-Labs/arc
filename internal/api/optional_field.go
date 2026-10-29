package api

import (
	"bytes"
	"encoding/json"
	"strings"
)

// optionalField is a request field whose absence means something different
// from its being null. Update handlers use it for the fields a body may leave
// out: an absent key keeps the stored value, a present key writes it, and a
// literal null clears it where clearing is meaningful (#1023).
//
// A plain pointer cannot carry that distinction — encoding/json leaves it nil
// for both an absent key and a null one. This type relies on encoding/json's
// documented behaviour of calling UnmarshalJSON for a present key, including
// for a literal null, and not at all for an absent one. Fiber's BodyParser
// uses encoding/json by default (no JSONDecoder is configured in server.go);
// a decoder swap must keep that behaviour or these fields silently become
// "always absent".
type optionalField[T any] struct {
	Set   bool // the key was present in the body
	Null  bool // the key was present with a literal null
	Value T    // the decoded value when Set && !Null
}

func (o *optionalField[T]) UnmarshalJSON(b []byte) error {
	var zero T
	o.Set = true
	o.Value = zero
	if bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
		o.Null = true
		return nil
	}
	o.Null = false
	return json.Unmarshal(b, &o.Value)
}

// hasValue reports whether the body carried a non-null value for the field.
func (o optionalField[T]) hasValue() bool { return o.Set && !o.Null }

// ptr returns the value as a pointer for a nullable column: nil when the field
// is absent or null.
func (o optionalField[T]) ptr() *T {
	if !o.hasValue() {
		return nil
	}
	v := o.Value
	return &v
}

// updateColumn is one column of a partial UPDATE: written only when set.
// Column names come from the handler's own literals, never from the request.
type updateColumn struct {
	col   string
	set   bool
	value any
}

// buildUpdateSet renders the SET assignments for the columns that are set, and
// their arguments in the same order. Column and argument come from the same
// element, so they cannot drift apart.
func buildUpdateSet(cols []updateColumn) (string, []any) {
	var b strings.Builder
	args := make([]any, 0, len(cols))
	for _, c := range cols {
		if !c.set {
			continue
		}
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c.col)
		b.WriteString(" = ?")
		args = append(args, c.value)
	}
	return b.String(), args
}
