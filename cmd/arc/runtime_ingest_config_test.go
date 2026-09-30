package main

import (
	"testing"
	"time"
)

func TestWALSafeAgeUsesBufferAgeWithFloorAndMargin(t *testing.T) {
	for _, tc := range []struct {
		name string
		age  int
		want time.Duration
	}{
		{name: "floor", age: 1, want: 30 * time.Second},
		{name: "three times age", age: 20_000, want: 60 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := walSafeAge(tc.age); got != tc.want {
				t.Fatalf("walSafeAge(%d) = %s, want %s", tc.age, got, tc.want)
			}
		})
	}
}

func TestWALSafeAgeSaturatesInsteadOfOverflowing(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	maxDuration := time.Duration(int64(^uint64(0) >> 1))
	if got := walSafeAge(maxInt); got != maxDuration {
		t.Fatalf("walSafeAge(math.MaxInt) = %s, want MaxInt64 duration", got)
	}
}
