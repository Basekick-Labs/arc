package ingest

import (
	"math"
	"testing"
)

func TestToInt64RejectsOutOfRangeAndNonFiniteFloats(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{name: "float32 upper bound", value: float32(math.MaxInt64)},
		{name: "float32 above upper bound", value: math.Nextafter32(float32(math.MaxInt64), float32(math.Inf(1)))},
		{name: "float32 below lower bound", value: math.Nextafter32(float32(math.MinInt64), float32(math.Inf(-1)))},
		{name: "float32 NaN", value: float32(math.NaN())},
		{name: "float32 positive infinity", value: float32(math.Inf(1))},
		{name: "float32 negative infinity", value: float32(math.Inf(-1))},
		{name: "float64 upper bound", value: float64(math.MaxInt64)},
		{name: "float64 above upper bound", value: math.Nextafter(float64(math.MaxInt64), math.Inf(1))},
		{name: "float64 below lower bound", value: math.Nextafter(float64(math.MinInt64), math.Inf(-1))},
		{name: "float64 NaN", value: math.NaN()},
		{name: "float64 positive infinity", value: math.Inf(1)},
		{name: "float64 negative infinity", value: math.Inf(-1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, ok := toInt64(tt.value); ok {
				t.Fatalf("toInt64(%v) accepted an invalid float", tt.value)
			}
		})
	}
}

func TestToInt64AcceptsInt64FloatBounds(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  int64
	}{
		{name: "lower bound", value: float64(math.MinInt64), want: math.MinInt64},
		{name: "zero", value: 0, want: 0},
		{name: "upper in-range integer", value: float64(math.MaxInt64 - 1023), want: math.MaxInt64 - 1023},
		{name: "float32 lower bound", value: float32(math.MinInt64), want: math.MinInt64},
		{name: "float32 upper in-range integer", value: math.Nextafter32(float32(1<<63), 0), want: (1 << 63) - (1 << 39)},
		{name: "float32 positive fraction", value: float32(1.75), want: 1},
		{name: "float32 negative fraction", value: float32(-1.75), want: -1},
		{name: "float64 positive fraction", value: 1.75, want: 1},
		{name: "float64 negative fraction", value: -1.75, want: -1},
		{name: "negative zero", value: math.Copysign(0, -1), want: 0},
		{name: "float32 subnormal", value: float32(math.SmallestNonzeroFloat32), want: 0},
		{name: "float64 subnormal", value: math.SmallestNonzeroFloat64, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toInt64(tt.value)
			if !ok {
				t.Fatalf("toInt64(%v) rejected a valid float", tt.value)
			}
			if got != tt.want {
				t.Fatalf("toInt64(%v) = %d, want %d", tt.value, got, tt.want)
			}
		})
	}
}
