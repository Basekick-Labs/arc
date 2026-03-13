package ingest

import "unicode/utf8"

// sanitizeUTF8SlowPath replaces invalid UTF-8 sequences with the Unicode replacement character (U+FFFD).
// This is the shared slow path used by both the standard and simdutf builds.
// Only called when ValidateUTF8 has already determined the string contains invalid bytes.
func sanitizeUTF8SlowPath(s string) string {
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
	return string(result)
}
