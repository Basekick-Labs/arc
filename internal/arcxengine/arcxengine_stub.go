// Stub for builds WITHOUT the arcx engine linked in (no `arcx_engine` tag, or no
// cgo). Keeps Arc compiling without the Rust toolchain / libarcx.a. Every entry
// point reports "unavailable" so callers transparently fall back to DuckDB.

//go:build !cgo || !arcx_engine

package arcxengine

import "github.com/apache/arrow-go/v18/arrow"

// Available reports whether the arcx engine is linked in. False in this build.
func Available() bool { return false }

// Context mirrors the real build's type so callers compile identically.
type Context struct {
	Database    string
	Measurement string
	TimeColumn  string
	AllowedDirs []string
}

// ErrUnsupported mirrors the real build's type.
type ErrUnsupported struct{ msg string }

func (e ErrUnsupported) Error() string { return "arcx: unsupported: " + e.msg }

// Query always declines in the stub build — the caller falls back to DuckDB.
func Query(sql string, ctx Context) (arrow.Record, error) {
	return nil, ErrUnsupported{msg: "arcx engine not built into this binary"}
}

// Version returns empty in the stub build.
func Version() string { return "" }
