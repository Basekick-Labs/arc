package api

import (
	"strconv"
	"testing"
)

func benchIntColumn(n int) []string {
	raw := make([]string, n)
	for i := range raw {
		raw[i] = strconv.Itoa(i)
	}
	return raw
}

func benchFloatColumn(n int) []string {
	raw := make([]string, n)
	for i := range raw {
		raw[i] = strconv.FormatFloat(float64(i)*1.5, 'f', 3, 64)
	}
	return raw
}

func benchStringColumn(n int) []string {
	raw := make([]string, n)
	for i := range raw {
		raw[i] = "host-" + strconv.Itoa(i%1000)
	}
	return raw
}

const perfRows = 1_000_000

func BenchmarkInferAndConvert_Int(b *testing.B) {
	raw := benchIntColumn(perfRows)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_, _ = inferAndConvertColumn(raw)
	}
}

func BenchmarkInferAndConvert_Float(b *testing.B) {
	raw := benchFloatColumn(perfRows)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_, _ = inferAndConvertColumn(raw)
	}
}

// BenchmarkInferAndConvert_StringEarlyBreak measures the common string-column
// path: the first value is non-numeric/non-bool, so type inference breaks on
// row 0 and the function falls straight to the string copy. This is the typical
// case (a text column is recognized immediately), NOT a full-column scan.
func BenchmarkInferAndConvert_StringEarlyBreak(b *testing.B) {
	raw := benchStringColumn(perfRows)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_, _ = inferAndConvertColumn(raw)
	}
}

// benchLateStringColumn is numeric for all but the last row, so inference scans
// (and parses) the whole column before the final value forces a demotion to
// string and the parsed buffer is abandoned — the worst case for the
// single-parse optimization.
func benchLateStringColumn(n int) []string {
	raw := make([]string, n)
	for i := range raw {
		raw[i] = strconv.Itoa(i)
	}
	raw[n-1] = "not-a-number"
	return raw
}

// BenchmarkInferAndConvert_LateStringDemotion measures the worst case: a column
// that looks numeric until the final row, so the full scan + parse happens and
// the accumulated int buffer is thrown away.
func BenchmarkInferAndConvert_LateStringDemotion(b *testing.B) {
	raw := benchLateStringColumn(perfRows)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_, _ = inferAndConvertColumn(raw)
	}
}
