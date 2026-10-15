package ingest

// The typed LP parse (ParseLPTyped) must produce exactly what the record path
// produced for the same chunk: records -> BatchToColumnar -> CoerceFirstValues
// -> the buffer's typing. Kinds come from a validation pass over the chunk,
// as the import's does.

import (
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// scanKinds is the import validation pass's kind rule: first value per
// (measurement, column) decides; a value that does not fit makes the chunk
// invalid (the import refuses it).
func scanKinds(p *LineProtocolParser, data []byte, precision string) (map[string]map[string]LPKind, bool) {
	kinds := map[string]map[string]LPKind{}
	ok := true
	_ = p.LexBatch(data, precision, func(l *LPLine) error {
		if _, found := LPLineTimeKey(l); found {
			ok = false
		}
		if _, clash := LPLineColumnClash(l); clash {
			ok = false
		}
		cols := kinds[l.Measurement]
		if cols == nil {
			cols = map[string]LPKind{}
			kinds[l.Measurement] = cols
		}
		observe := func(c string, v interface{}) {
			k, seen := cols[c]
			if !seen {
				k = LPKindOf(v)
				cols[c] = k
			}
			if !LPValueFits(k, v) {
				ok = false
			}
		}
		for _, t := range l.Tags {
			observe(t.Key, t.Value)
		}
		for _, f := range l.Fields {
			observe(LPFieldColumn(l, f.Key), f.Value)
		}
		return nil
	})
	return kinds, ok
}

type normCol struct {
	typ  string
	vals []interface{}
}

type normBatch struct {
	cols map[string]normCol
	tags []string
}

func normalize(b *TypedColumnBatch, rows int) normBatch {
	out := normBatch{cols: map[string]normCol{}}
	for name, data := range b.Data {
		valid := b.Validity[name]
		c := normCol{typ: fmt.Sprintf("%T", data)}
		at := func(i int) bool { return valid == nil || valid[i] }
		switch d := data.(type) {
		case []int64:
			for i := 0; i < rows; i++ {
				if at(i) {
					c.vals = append(c.vals, d[i])
				} else {
					c.vals = append(c.vals, nil)
				}
			}
		case []float64:
			for i := 0; i < rows; i++ {
				if at(i) && math.IsNaN(d[i]) {
					c.vals = append(c.vals, "NaN") // NaN != NaN
				} else if at(i) {
					c.vals = append(c.vals, d[i])
				} else {
					c.vals = append(c.vals, nil)
				}
			}
		case []string:
			for i := 0; i < rows; i++ {
				if at(i) {
					c.vals = append(c.vals, d[i])
				} else {
					c.vals = append(c.vals, nil)
				}
			}
		case []bool:
			for i := 0; i < rows; i++ {
				if at(i) {
					c.vals = append(c.vals, d[i])
				} else {
					c.vals = append(c.vals, nil)
				}
			}
		}
		out.cols[name] = c
	}
	out.tags = append([]string(nil), b.TagColumns...)
	sort.Strings(out.tags)
	return out
}

func recordPath(t *testing.T, buf *ArrowBuffer, p *LineProtocolParser, data []byte, precision string, kinds map[string]map[string]LPKind) (map[string]normBatch, error) {
	out := map[string]normBatch{}
	for m, rec := range BatchToColumnar(p.ParseBatchWithPrecision(data, precision)) {
		CoerceFirstValues(rec, kinds[m])
		b, n, err := buf.TypeColumnarRecord(rec)
		if err != nil {
			return nil, err
		}
		out[m] = normalize(b, n)
	}
	return out, nil
}

func typedPath(p *LineProtocolParser, data []byte, precision string, kinds map[string]map[string]LPKind) (map[string]normBatch, error) {
	batches, rows, err := p.ParseLPTyped(data, precision, "", kinds)
	if err != nil {
		return nil, err
	}
	out := map[string]normBatch{}
	for m, b := range batches {
		for name, data := range b.Data {
			if n := reflect.ValueOf(data).Len(); n != rows[m] {
				return nil, fmt.Errorf("column %q of %q has %d values for %d rows", name, m, n, rows[m])
			}
		}
		out[m] = normalize(b, rows[m])
	}
	return out, nil
}

func diffBuffer(t *testing.T) *ArrowBuffer {
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 10, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 1, ShardCount: 1, DataPageVersion: "2.0"}, &mockStorageBackend{}, zerolog.New(io.Discard))
	t.Cleanup(func() { b.Close() })
	return b
}

// checkSamePath asserts both paths agree on data; it reports whether the
// chunk was valid (validation pass accepted it).
func checkSamePath(t *testing.T, buf *ArrowBuffer, data []byte, precision string) bool {
	t.Helper()
	old := lpNow
	lpNow = func() int64 { return 42 }
	defer func() { lpNow = old }()
	p := NewLineProtocolParser()
	kinds, ok := scanKinds(p, data, precision)
	if !ok {
		return false
	}
	want, werr := recordPath(t, buf, p, data, precision, kinds)
	got, gerr := typedPath(p, data, precision, kinds)
	if werr != nil || gerr != nil {
		require.Equal(t, werr != nil, gerr != nil, "error mismatch for %q: record %v, typed %v", data, werr, gerr)
		return true
	}
	require.Equal(t, want, got, "for %q", data)
	return true
}

var lpDiffCorpus = []string{
	"cpu,host=a v=1 1700000000000000000",
	"cpu,host=a,region=us v=1i,w=2.5,s=\"x\",b=t 1700000000000000000\ncpu,host=b v=2i 1700000001000000000",
	"cpu h=1_000",
	"cpu k=0x10",
	"cpu f=NaN\ncpu f=-Inf",
	"cpu f=1,f=2i",
	"cpu f=1,f=1.5i",
	"cpu,t=a,t=b v=1",
	"m,t=a\"b f=1",
	"cpu g=\"unterminated 1000",
	"cpu v=1 1000 extra junk",
	"cpu    v=1     1000",
	"cpu v=1 -1999",
	"cpu v=1u\ncpu v=2.5",
	"cpu v=-1u",
	",t=a f=1",
	"cpu =x,v=1",
	"cpu,t= v=1",
	"cpu f=1,=2,g",
	"cpu b=TrUe\ncpu b=T\ncpu b=false",
	"c\\,p\\ u,t\\=k=v\\,1 f\\ x=1",
	"cpu v=1\r\ncpu v=2\r\n",
	"cpu s=\"caf\xc3\xa9\"\ncpu s=\"bad\xff\"",
	"cpu k=1i,k=",
	"cpu k=,k=1i",
	"cpu a=abc",
	"cpu v=99999999999999999999i",
	"cpu,host=x host=1i\ncpu,host=y v=2",
	"cpu,host=x v=1\ncpu host_value=2i",
	"cpu,host=x host=1i,host_value=2i\ncpu,host=y host=3i\ncpu,host=z host=5i",
	"cpu,a=t,a_value=u a=\"p\"\ncpu,a=t2 a=\"q\"",
	"cpu,a=1 v=1\ncpu,b=2 v=2\ncpu,a=3,b=4 v=3",
	"cpu v=1i\ncpu v=2.9\ncpu v=-3.9",
	"cpu v=1.5\ncpu v=2i\ncpu v=3u",
	"cpu f=1.5\ncpu f=18446744073709551615u",
	"cpu v=1 1700000000\nmem v=2 1700000001",
	"# comment\n\ncpu v=1\n  \n",
	"cpu v=\"a,b c=d\"",
}

func TestLPTypedMatchesRecordPath(t *testing.T) {
	buf := diffBuffer(t)
	for _, c := range lpDiffCorpus {
		for _, prec := range []string{"", "ns", "us", "ms", "s"} {
			checkSamePath(t, buf, []byte(c), prec)
		}
	}
	// Wide random chunks.
	var b strings.Builder
	for i := 0; i < 2000; i++ {
		fmt.Fprintf(&b, "m%d,host=h%d", i%3, i%7)
		if i%5 == 0 {
			fmt.Fprintf(&b, ",dc=d%d", i%2)
		}
		fmt.Fprintf(&b, " v=%di", i)
		if i%4 == 0 {
			fmt.Fprintf(&b, ",f=%d.5", i)
		}
		if i%6 == 0 {
			fmt.Fprintf(&b, ",s=\"x%d\"", i)
		}
		fmt.Fprintf(&b, " %d\n", 1700000000000000000+int64(i)*1e9)
	}
	require.True(t, checkSamePath(t, buf, []byte(b.String()), "ns"))
}

func FuzzLPTypedMatchesRecordPath(f *testing.F) {
	for _, c := range lpDiffCorpus {
		f.Add(c)
	}
	buf := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 10, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 1, ShardCount: 1, DataPageVersion: "2.0"}, &mockStorageBackend{}, zerolog.New(io.Discard))
	defer buf.Close()
	f.Fuzz(func(t *testing.T, data string) {
		checkSamePath(t, buf, []byte(data), "ns")
	})
}

// A value that does not fit its column's kind is an error, never a wrapped
// or zero value (the validation pass refuses it first; this is the guard).
func TestLPTypedRefusesValuesOutsideTheirKind(t *testing.T) {
	p := NewLineProtocolParser()
	for _, line := range []string{"cpu v=18446744073709551615u", "cpu v=1e300", "cpu v=\"x\""} {
		_, _, err := p.ParseLPTyped([]byte(line), "ns", "", map[string]map[string]LPKind{"cpu": {"v": LPKindInt}})
		require.Error(t, err, line)
	}
}

// Two keys of one line landing in one column never give a row two values
// (the validation pass refuses the line; this is the builder's own guard).
func TestLPTypedRefusesTwoValuesForOneRow(t *testing.T) {
	p := NewLineProtocolParser()
	_, _, err := p.ParseLPTyped([]byte("cpu,host=x host=1i,host_value=2i\ncpu,host=y host=3i"), "ns", "",
		map[string]map[string]LPKind{"cpu": {"host": LPKindString, "host_value": LPKindInt}})
	require.ErrorContains(t, err, "two keys of one line")
}

// TypeColumnarRecord types a columnar record's columns as a write of it
// would (decimal columns included), for a caller that groups the batches
// before writing them (HourGrouper).
func (b *ArrowBuffer) TypeColumnarRecord(record *models.ColumnarRecord) (*TypedColumnBatch, int, error) {
	batch, n, err := b.convertColumnsToTyped(record.Measurement, record.Columns)
	if err != nil {
		return nil, 0, err
	}
	batch.TagColumns = append([]string(nil), record.TagColumns...)
	return batch, n, nil
}
