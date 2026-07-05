// Package arcxengine is the Go side of the in-process (v0) bridge to the
// standalone arcx query engine (Rust), over the Arrow C Data Interface.
//
// It calls the C ABI exported by arcx (see arcx/include/arcx.h and
// arcx/docs/2026-07-05-ffi-bridge-design.md) and adapts the Arrow result into a
// Go arrow.Record. The memory-ownership contract is the load-bearing part:
//   - arcx allocates the Arrow result; Go releases it via the callback the Arrow
//     C Data Interface embeds (cdata does this when the imported array is
//     released). Go must NOT free those buffers directly.
//   - error strings are arcx-owned; freed via C.arcx_string_free.
//   - the status code — not string parsing — tells us "fall back to DuckDB"
//     (StatusUnsupported) vs "real failure" (StatusError).
//
// Build: requires cgo and a statically linked libarcx.a (the Makefile builds it
// via cargo). Guarded by the `arcx_engine` build tag so a build without the Rust
// toolchain still compiles (the stub in arcxengine_stub.go takes over).

//go:build cgo && arcx_engine

package arcxengine

/*
#cgo CFLAGS: -I${SRCDIR}/../../../arcx/include
#cgo LDFLAGS: -L${SRCDIR}/../../../arcx/target/release -larcx -lm -ldl
#include <stdlib.h>
#include "arcx.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

// Available reports whether this build was compiled with the arcx engine linked
// in. Always true in this tagged file; the stub build returns false.
func Available() bool { return true }

// Context carries the out-of-band identity the footer-agg fast path needs. Empty
// fields are passed as NULL (the engine declines shapes that require them).
type Context struct {
	Database    string
	Measurement string
	TimeColumn  string
}

// ErrUnsupported signals the engine declined the query shape — the caller should
// fall back to DuckDB. It is NOT a failure.
type ErrUnsupported struct{ msg string }

func (e ErrUnsupported) Error() string { return "arcx: unsupported: " + e.msg }

// Query runs sql through the arcx engine. On success it returns a single
// arrow.Record (Phase 0/1 results are one batch); the caller owns it and must
// Release() it. A declined shape returns ErrUnsupported (fall back to DuckDB); a
// real failure returns a plain error.
func Query(sql string, ctx Context) (arrow.Record, error) {
	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))

	// Build the C context. Keep the C strings alive until after the call.
	cctx, freeCtx := buildCCtx(ctx)
	defer freeCtx()

	// Caller-allocated Arrow C Data Interface structs (our arcx.h definitions).
	var cSchema C.struct_ArrowSchema
	var cArray C.struct_ArrowArray
	var cErr *C.char

	status := C.arcx_query(cSQL, cctx, &cSchema, &cArray, &cErr)

	switch status {
	case C.ARCX_OK:
		return importRecord(&cSchema, &cArray)
	case C.ARCX_UNSUPPORTED:
		// No Arrow output produced, no error string per the contract.
		return nil, ErrUnsupported{msg: takeErr(cErr)}
	default: // C.ARCX_ERROR
		return nil, fmt.Errorf("arcx: %s", takeErr(cErr))
	}
}

// Version returns the engine's proof-of-life version string.
func Version() string {
	p := C.arcx_version()
	return takeErr(p) // same ownership: arcx-owned, freed via arcx_string_free
}

// buildCCtx builds a *C.ArcxCtx from ctx. Returns the pointer and a cleanup func
// that frees the C strings (call after the C call returns). Empty Go strings are
// passed as NULL, which the engine reads as "not provided".
func buildCCtx(ctx Context) (*C.ArcxCtx, func()) {
	var cctx C.ArcxCtx
	var toFree []unsafe.Pointer
	set := func(s string) *C.char {
		if s == "" {
			return nil
		}
		p := C.CString(s)
		toFree = append(toFree, unsafe.Pointer(p))
		return p
	}
	cctx.database = set(ctx.Database)
	cctx.measurement = set(ctx.Measurement)
	cctx.time_column = set(ctx.TimeColumn)
	return &cctx, func() {
		for _, p := range toFree {
			C.free(p)
		}
	}
}

// importRecord adapts the arcx-exported struct array into a Go arrow.Record.
// arcx exports the result RecordBatch as a single struct array (per the Arrow C
// Data Interface); we import it, then unwrap the struct's children into a Record.
//
// The two C struct types (ours from arcx.h and cdata's from its own cgo package)
// are layout-identical standard Arrow C Data Interface structs, so we hand cdata
// the address reinterpreted via unsafe.Pointer — the canonical interop pattern.
func importRecord(cSchema *C.struct_ArrowSchema, cArray *C.struct_ArrowArray) (arrow.Record, error) {
	_, arr, err := cdata.ImportCArray(
		(*cdata.CArrowArray)(unsafe.Pointer(cArray)),
		(*cdata.CArrowSchema)(unsafe.Pointer(cSchema)),
	)
	if err != nil {
		return nil, fmt.Errorf("arcx: importing arrow result: %w", err)
	}
	// The imported array is a struct array whose columns are the result columns.
	// Release it when we're done unwrapping — its release drives the arcx-side
	// free of the underlying buffers.
	sa, ok := arr.(*array.Struct)
	if !ok {
		arr.Release()
		return nil, fmt.Errorf("arcx: expected struct array result, got %T", arr)
	}
	rec := structToRecord(sa)
	// structToRecord retains the columns it keeps, so we can release our reference.
	sa.Release()
	return rec, nil
}

// structToRecord turns a struct array into a Record over its child columns,
// preserving the field schema (names/types arcx set to match DuckDB).
func structToRecord(sa *array.Struct) arrow.Record {
	st := sa.DataType().(*arrow.StructType)
	fields := st.Fields()
	cols := make([]arrow.Array, sa.NumField())
	for i := 0; i < sa.NumField(); i++ {
		cols[i] = sa.Field(i)
	}
	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, cols, int64(sa.Len()))
}

// takeErr copies an arcx-owned C string to a Go string and frees it via
// arcx_string_free (never C.free — it's a Rust CString, not malloc'd). Safe on NULL.
func takeErr(p *C.char) string {
	if p == nil {
		return ""
	}
	s := C.GoString(p)
	C.arcx_string_free(p)
	return s
}
