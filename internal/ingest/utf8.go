package ingest

import "unicode/utf8"

// SanitizeUTF8 replaces invalid UTF-8 sequences with the Unicode replacement character (U+FFFD).
// This function is optimized for the common case where strings are already valid UTF-8:
// - Fast path: utf8.ValidString() check (~3-25ns for typical strings), returns original string unchanged
// - Slow path: Only allocates and copies when invalid bytes are found (rare case)
//
// This is used during ingestion to prevent DuckDB query failures caused by non-UTF-8 data
// in Parquet files. Common sources of invalid UTF-8 include rsyslog messages, binary protocols,
// and data from non-UTF-8 encodings (Latin-1, Windows-1252).
func SanitizeUTF8(s string) (string, bool) {
	// Fast path: ~3-25ns for typical strings, no allocation
	if utf8.ValidString(s) {
		return s, false
	}

	// Slow path: only executed for invalid UTF-8 (rare)
	// Pre-allocate with some growth room for replacement characters
	result := make([]byte, 0, len(s)+len(s)/8)
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			// Invalid UTF-8 byte - replace with U+FFFD (replacement character)
			result = append(result, '\xef', '\xbf', '\xbd')
			i++
		} else {
			result = append(result, s[i:i+size]...)
			i += size
		}
	}
	return string(result), true
}
