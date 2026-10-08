package replicaview

import (
	"fmt"
	"math"
	"reflect"
	"testing"
)

func TestCoveragePreservesDistinctWritesAcrossFlushBoundaries(t *testing.T) {
	id := func(instance, seq uint64) string { return fmt.Sprintf("%016x%016x", instance, seq) }
	primary, err := FromIdentities([]string{id(5, 1), id(5, 2), id(5, 4), id(9, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if len(primary) != 3 {
		t.Fatalf("expected three compact intervals: %v", primary)
	}
	for _, identity := range []string{id(5, 1), id(5, 2), id(5, 4), id(9, 1)} {
		if !primary.Contains(identity) {
			t.Fatalf("missing covered identity %s", identity)
		}
	}
	for _, identity := range []string{id(5, 3), id(9, 2), id(7, 1)} {
		if primary.Contains(identity) {
			t.Fatalf("hid a distinct write: %s", identity)
		}
	}
	a, _ := FromIdentities([]string{id(5, 1), id(5, 4)})
	b, _ := FromIdentities([]string{id(5, 2), id(9, 1)})
	if got := Union(a, b); !reflect.DeepEqual(got, primary) {
		t.Fatalf("flush boundaries changed coverage: %v != %v", got, primary)
	}
	decoded, err := Decode(primary.Encode())
	if err != nil || !reflect.DeepEqual(primary, decoded) {
		t.Fatalf("round trip: %v %v", decoded, err)
	}
}

func TestCoverageCompactsMillionContiguousEntries(t *testing.T) {
	spans := make(Coverage, 1000000)
	for i := range spans {
		seq := uint64(i + 1)
		spans[len(spans)-1-i] = Span{123, seq, seq}
	}
	compact, err := Normalize(spans)
	if err != nil || !reflect.DeepEqual(compact, Coverage{{123, 1, 1000000}}) {
		t.Fatalf("compact=%v err=%v", compact, err)
	}
	if spans[0].First != 1000000 {
		t.Fatal("normalization mutated caller snapshot")
	}
}

func TestCoverageExtremeSequenceAndInvalidMetadata(t *testing.T) {
	result, err := Normalize(Coverage{{1, math.MaxUint64, math.MaxUint64}, {1, 1, 2}, {1, 2, 3}})
	if err != nil || len(result) != 2 {
		t.Fatalf("overflow merged unrelated sequences: %v %v", result, err)
	}
	for _, input := range []string{`[{"instance":"1","first":"0","last":"3"}]`, `[{"instance":"1","first":"3","last":"2"}]`, `[{"instance":"1","first":"1","last":"18446744073709551616"}]`, `{}`} {
		if _, err := Decode(input); err == nil {
			t.Fatalf("accepted invalid coverage: %s", input)
		}
	}
}
