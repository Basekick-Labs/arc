//go:build !duckdb_arrow

package api

import "testing"

// TestArrowMsgPackQueryFunc_NilWithoutTag pins the contract that the
// msgpack dispatch symbol is unset when Arc is built without the
// duckdb_arrow tag. The dispatch site in executeQuery relies on this
// to short-circuit to 501 — a future refactor that accidentally
// registers a non-Arrow fallback would silently regress the
// "msgpack requires duckdb_arrow" contract.
func TestArrowMsgPackQueryFunc_NilWithoutTag(t *testing.T) {
	if arrowMsgPackQueryFunc != nil {
		t.Fatal("arrowMsgPackQueryFunc must be nil when built WITHOUT -tags=duckdb_arrow")
	}
}
