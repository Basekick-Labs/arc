// compareScan tests: build a synthetic arcx-shaped arrow.Record and compare it to
// a synthetic oracle row set. Proves the record-batch comparator matches on equal
// value multisets (order-insensitive) and flags real differences.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// mkRecord builds a 2-column (host string, code int64) record from rows.
func mkRecord(t *testing.T, rows []struct {
	host *string
	code *int64
}) arrow.Record {
	t.Helper()
	pool := memory.NewGoAllocator()
	hostB := array.NewStringBuilder(pool)
	codeB := array.NewInt64Builder(pool)
	for _, r := range rows {
		if r.host == nil {
			hostB.AppendNull()
		} else {
			hostB.Append(*r.host)
		}
		if r.code == nil {
			codeB.AppendNull()
		} else {
			codeB.Append(*r.code)
		}
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "code", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	rec := array.NewRecord(schema, []arrow.Array{hostB.NewArray(), codeB.NewArray()}, int64(len(rows)))
	return rec
}

func s(v string) *string { return &v }
func n(v int64) *int64   { return &v }

func TestCompareScan_MatchesRegardlessOfRowOrder(t *testing.T) {
	type row = struct {
		host *string
		code *int64
	}
	rec := mkRecord(t, []row{{s("a"), n(1)}, {s("b"), n(2)}})
	defer rec.Release()
	// Oracle rows in the OPPOSITE order — multiset match must still hold.
	oracle := []scanRow{
		{"host=b", "code=2"},
		{"host=a", "code=1"},
	}
	if diff := compareScan(rec, oracle); diff != "" {
		t.Fatalf("expected match, got diff: %s", diff)
	}
}

func TestCompareScan_FlagsValueMismatch(t *testing.T) {
	type row = struct {
		host *string
		code *int64
	}
	rec := mkRecord(t, []row{{s("a"), n(1)}})
	defer rec.Release()
	oracle := []scanRow{{"host=a", "code=999"}}
	if diff := compareScan(rec, oracle); diff == "" {
		t.Fatal("expected mismatch, got match")
	}
}

func TestCompareScan_HandlesNull(t *testing.T) {
	type row = struct {
		host *string
		code *int64
	}
	rec := mkRecord(t, []row{{s("a"), nil}})
	defer rec.Release()
	// NULL code renders as the sentinel on both sides.
	oracle := []scanRow{{"host=a", "code=" + scanNull}}
	if diff := compareScan(rec, oracle); diff != "" {
		t.Fatalf("expected NULL match, got diff: %s", diff)
	}
}
