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

func BenchmarkInferAndConvert_String(b *testing.B) {
	raw := benchStringColumn(perfRows)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_, _ = inferAndConvertColumn(raw)
	}
}
