package ingest

// ParseLPWrite (the typed /write parse) must produce what the record path a
// write took produced: records -> `time` key drop -> BatchToColumnar -> the
// buffer's typing, each column typed by its first value in the body. Where
// the record path failed a whole measurement (a value that does not fit its
// column), ParseLPWrite drops only the points that do not fit: the record
// path over the body WITHOUT those points must then succeed and match.

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// writeRecordPath is the record path of a write: no kind coercion, points
// with a `time` key dropped.
func writeRecordPath(buf *ArrowBuffer, p *LineProtocolParser, data []byte, precision string) (map[string]normBatch, error) {
	records := p.ParseBatchWithPrecision(data, precision)
	kept := records[:0]
	for _, r := range records {
		if _, found := LPRecordTimeKey(r); !found {
			kept = append(kept, r)
		}
	}
	out := map[string]normBatch{}
	for m, rec := range BatchToColumnar(kept) {
		b, n, err := buf.TypeColumnarRecord(rec)
		if err != nil {
			return nil, err
		}
		out[m] = normalize(b, n)
	}
	return out, nil
}

func writeTypedPath(p *LineProtocolParser, data []byte, precision string) (map[string]normBatch, int, []LPDrop, error) {
	batches, rows, drops, err := p.ParseLPWrite(data, precision)
	if err != nil {
		return nil, 0, nil, err
	}
	out := map[string]normBatch{}
	total := 0
	for m, b := range batches {
		for name, data := range b.Data {
			if n := reflect.ValueOf(data).Len(); n != rows[m] {
				return nil, 0, nil, fmt.Errorf("column %q of %q has %d values for %d rows", name, m, n, rows[m])
			}
		}
		for name, v := range b.Validity {
			if len(v) != rows[m] {
				return nil, 0, nil, fmt.Errorf("validity %q of %q has %d entries for %d rows", name, m, len(v), rows[m])
			}
		}
		if rows[m] == 0 {
			return nil, 0, nil, fmt.Errorf("measurement %q has no rows", m)
		}
		out[m] = normalize(b, rows[m])
		total += rows[m]
	}
	return out, total, drops, nil
}

func lpRows(p *LineProtocolParser, data []byte) int {
	_, rows, _, err := p.ParseLPWrite(data, "ns")
	if err != nil {
		panic(err)
	}
	n := 0
	for _, r := range rows {
		n += r
	}
	return n
}

// checkWritePath asserts ParseLPWrite matches the record path over the
// points it kept, and that every point it dropped is one the record path
// could not write as a row of its own.
func checkWritePath(t *testing.T, buf *ArrowBuffer, data []byte, precision string) {
	t.Helper()
	old := lpNow
	lpNow = func() int64 { return 42 }
	defer func() { lpNow = old }()
	p := NewLineProtocolParser()

	got, total, drops, err := writeTypedPath(p, data, precision)
	require.NoError(t, err, "for %q", data)
	dropped := 0
	for _, d := range drops {
		require.Positive(t, d.Points)
		dropped += d.Points
	}

	// Split the body into the lines ParseLPWrite kept and dropped (a line
	// it kept adds a row to the prefix before it).
	lines := bytes.Split(data, []byte{'\n'})
	var keptBody, prefix []byte
	var droppedLines [][]byte
	rows := 0
	for i, line := range lines {
		if i > 0 {
			prefix = append(prefix, '\n')
		}
		prefix = append(prefix, line...)
		n := lpRows(p, prefix)
		require.GreaterOrEqual(t, n, rows, "rows went down at line %q", line)
		switch {
		case n > rows:
			require.Equal(t, rows+1, n, "line %q added more than one row", line)
			keptBody = append(append(keptBody, line...), '\n')
		case p.lineParses(line):
			droppedLines = append(droppedLines, line)
		}
		rows = n
	}
	require.Equal(t, total, rows, "for %q", data)
	require.Equal(t, dropped, len(droppedLines), "for %q: drops %+v", data, drops)

	want, werr := writeRecordPath(buf, p, keptBody, precision)
	require.NoError(t, werr, "record path over the kept points of %q", data)
	require.Equal(t, want, got, "for %q", data)

	if dropped == 0 {
		// Nothing dropped: the record path over the whole body is the same.
		want, werr = writeRecordPath(buf, p, data, precision)
		require.NoError(t, werr, "for %q", data)
		require.Equal(t, want, got, "for %q", data)
		return
	}
	// Each dropped point is one the record path could not write next to
	// the kept points before it (or one with a `time` key / a column clash).
	for _, line := range droppedLines {
		var l LPLine
		if !p.lexLine(line, "ns", ValidateUTF8Bytes(line), &l) {
			t.Fatalf("dropped line %q does not parse", line)
		}
		l.Tags = lastPerKeyTags(l.Tags, map[string]int{})
		l.Fields = lastPerKeyFields(l.Fields, map[string]int{})
		if _, found := LPLineTimeKey(&l); found {
			continue
		}
		if _, clash := LPLineColumnClash(&l); clash {
			continue
		}
		body := append(append([]byte(nil), keptBody...), line...)
		_, err := writeRecordPath(buf, p, body, precision)
		if err == nil {
			// The record path may have converted it silently (NaN/Inf or
			// an out-of-range value in an integer column): then the column
			// typing differs; the point does not fit an int64 exactly.
			require.True(t, lpLineHasUnfitInt(&l), "dropped line %q was writable by the record path after %q", line, keptBody)
		}
	}
}

func (p *LineProtocolParser) lineParses(line []byte) bool {
	var l LPLine
	return p.lexLine(line, "ns", ValidateUTF8Bytes(line), &l)
}

func lpLineHasUnfitInt(l *LPLine) bool {
	for _, f := range l.Fields {
		switch x := f.Value.(type) {
		case uint64:
			if x > math.MaxInt64 {
				return true
			}
		case float64:
			if !LPValueFits(LPKindInt, x) {
				return true
			}
		}
	}
	return false
}

var lpWriteCorpus = []string{
	// kind conflicts within one measurement: only the point is dropped
	"cpu v=1\ncpu v=\"x\"\ncpu v=2",
	"cpu v=\"x\"\ncpu v=1\ncpu v=\"y\"",
	"cpu b=t\ncpu b=1i\ncpu b=f",
	"cpu,host=a v=1\ncpu host=2\ncpu,host=b v=3",
	"cpu host=2\ncpu,host=a v=1",
	// a dropped first point sets no kind
	"cpu time=1,v=\"x\"\ncpu v=1",
	"cpu,host=x host=1i,host_value=2i\ncpu host_value=\"s\"",
	// interleaved measurements, other measurement unaffected
	"cpu v=1\nmem v=\"a\"\ncpu v=\"b\"\nmem v=2\ncpu v=3",
	// uint64 past MaxInt64 first and later
	"cpu v=18446744073709551615u\ncpu v=1i",
	"cpu v=1i\ncpu v=18446744073709551615u",
	"cpu v=1u\ncpu v=9223372036854775808u",
	// NaN/Inf in an int column
	"cpu v=1i\ncpu v=NaN\ncpu v=Inf\ncpu v=-Inf\ncpu v=2.5",
	"cpu v=1i\ncpu v=1e300",
	// >8 tags, NULL padding
	"cpu,a=1,b=2,c=3,d=4,e=5,f=6,g=7,h=8,i=9,j=10 v=1\ncpu,k=1 w=2\ncpu x=\"s\"",
	// tag and field with the same name across lines
	"cpu,t=a v=1\ncpu t=\"b\"\ncpu t=2",
	"cpu t=2\ncpu,t=a v=1",
	// all points dropped
	"cpu time=1\ncpu,time=x v=1",
	"cpu,h=x h=1i,h_value=2i",
}

func TestLPWriteMatchesRecordPath(t *testing.T) {
	buf := diffBuffer(t)
	for _, c := range append(append([]string(nil), lpDiffCorpus...), lpWriteCorpus...) {
		for _, prec := range []string{"", "ns", "s"} {
			checkWritePath(t, buf, []byte(c), prec)
		}
	}
	var b strings.Builder
	for i := 0; i < 2000; i++ {
		fmt.Fprintf(&b, "m%d,host=h%d", i%3, i%7)
		if i%5 == 0 {
			fmt.Fprintf(&b, ",dc=d%d", i%2)
		}
		if i%97 == 0 {
			fmt.Fprintf(&b, " v=\"conflict%d\"", i)
		} else {
			fmt.Fprintf(&b, " v=%di", i)
		}
		if i%4 == 0 {
			fmt.Fprintf(&b, ",f=%d.5", i)
		}
		fmt.Fprintf(&b, " %d\n", 1700000000000000000+int64(i)*1e9)
	}
	checkWritePath(t, buf, []byte(b.String()), "ns")
}

func FuzzLPWriteMatchesRecordPath(f *testing.F) {
	for _, c := range append(append([]string(nil), lpDiffCorpus...), lpWriteCorpus...) {
		f.Add(c)
	}
	buf := newDiffBufferForFuzz()
	defer buf.Close()
	f.Fuzz(func(t *testing.T, data string) {
		checkWritePath(t, buf, []byte(data), "ns")
	})
}

// The buffer counts a batch's bytes by length: columns sized for the whole
// body are trimmed to their rows, so a measurement with few rows of a long
// body holds no hidden capacity.
func TestLPWriteColumnsHoldNoSlack(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 1000; i++ {
		m := "big"
		if i%100 == 0 {
			m = "small"
		}
		fmt.Fprintf(&b, "%s,host=h v=%d.5,n=%di,s=\"x\",ok=t %d\n", m, i, i, 1700000000000000000+int64(i))
	}
	batches, rows, _, err := NewLineProtocolParser().ParseLPWrite([]byte(b.String()), "ns")
	require.NoError(t, err)
	require.Equal(t, 10, rows["small"])
	for m, batch := range batches {
		for name, data := range batch.Data {
			v := reflect.ValueOf(data)
			require.LessOrEqual(t, v.Cap(), v.Len()+v.Len()/8, "%s.%s: len %d cap %d", m, name, v.Len(), v.Cap())
		}
	}
}

// A body of many measurements allocates in proportion to its rows, not to
// measurements x lines: sizing every measurement's columns for the whole
// body made a 5,000-line body of distinct measurements allocate 397 MB, and
// an import chunk of 200 interleaved measurements 925 MB.
func TestLPParseMemoryIsLinearInMeasurements(t *testing.T) {
	alloc := func(f func()) uint64 {
		var a, b runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&a)
		f()
		runtime.ReadMemStats(&b)
		return b.TotalAlloc - a.TotalAlloc
	}
	p := NewLineProtocolParser()

	var distinct strings.Builder
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&distinct, "m%d,host=h v=1,n=2i,s=\"x\" %d\n", i, 1700000000000000000+int64(i))
	}
	got := alloc(func() {
		_, _, _, err := p.ParseLPWrite([]byte(distinct.String()), "ns")
		require.NoError(t, err)
	})
	require.Less(t, got, uint64(32<<20), "5000 measurements allocated %d bytes", got)

	var interleaved strings.Builder
	kinds := map[string]map[string]LPKind{}
	for i := 0; i < 40000; i++ {
		m := fmt.Sprintf("m%d", i%200)
		kinds[m] = map[string]LPKind{"host": LPKindString, "v": LPKindFloat, "n": LPKindInt}
		fmt.Fprintf(&interleaved, "%s,host=h v=1,n=2i %d\n", m, 1700000000000000000+int64(i))
	}
	got = alloc(func() {
		_, _, err := p.ParseLPTyped([]byte(interleaved.String()), "ns", "", kinds)
		require.NoError(t, err)
	})
	require.Less(t, got, uint64(64<<20), "200 interleaved measurements allocated %d bytes", got)

	// Lines that add no rows reserve nothing: one point and a million blank
	// or comment lines.
	for name, filler := range map[string]string{"blank": "\n", "comment": "#\n"} {
		body := "cpu,host=a v=1,n=2i,s=\"x\" 1700000000000000000\n" + strings.Repeat(filler, 1_000_000)
		got = alloc(func() {
			_, rows, _, err := p.ParseLPWrite([]byte(body), "ns")
			require.NoError(t, err)
			require.Equal(t, 1, rows["cpu"])
		})
		require.Less(t, got, uint64(8<<20), "%s filler allocated %d bytes", name, got)
	}
}
