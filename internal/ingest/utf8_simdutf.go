//go:build simdutf

package ingest

// #cgo CFLAGS: -I/opt/homebrew/include
// #cgo LDFLAGS: -L/opt/homebrew/lib -lsimdutf
// #include <simdutf_c.h>
// #include <stdbool.h>
import "C"

import (
	"unicode/utf8"
	"unsafe"
)

// simdutfThreshold is the minimum string length where simdutf outperforms
// Go's utf8.ValidString. Below this, cgo call overhead (~25ns) dominates.
// Measured on Apple M3 Max: crossover at ~4KB, clear win at 16KB+.
const simdutfThreshold = 4096

// ValidateUTF8 uses simdutf's SIMD-accelerated UTF-8 validation for large strings.
// On ARM (Apple Silicon, Graviton) this uses NEON, on x86 it uses AVX2/SSE4.
// Falls back to Go stdlib for strings under 4KB where cgo overhead dominates.
func ValidateUTF8(s string) bool {
	if len(s) < simdutfThreshold {
		return utf8.ValidString(s)
	}
	return bool(C.simdutf_validate_utf8((*C.char)(unsafe.Pointer(unsafe.StringData(s))), C.size_t(len(s))))
}

// ValidateUTF8Bytes validates a byte slice as UTF-8 using SIMD acceleration for large buffers.
// This is useful for validating entire incoming payloads before parsing individual fields.
func ValidateUTF8Bytes(b []byte) bool {
	if len(b) == 0 {
		return true
	}
	if len(b) < simdutfThreshold {
		return utf8.Valid(b)
	}
	return bool(C.simdutf_validate_utf8((*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b))))
}

// SanitizeUTF8 replaces invalid UTF-8 sequences with U+FFFD.
// Uses the fastest available validation for the fast path.
func SanitizeUTF8(s string) (string, bool) {
	if ValidateUTF8(s) {
		return s, false
	}

	return sanitizeUTF8SlowPath(s), true
}
