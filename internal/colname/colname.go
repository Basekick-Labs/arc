// Package colname bounds a client-sent column name for a message or a log
// line. It is a leaf package so any package can quote a name the way ingest
// does without importing ingest (#1744).
package colname

import (
	"strconv"
	"unicode/utf8"
)

// MaxShownBytes bounds a client-sent name echoed in an answer or a log.
const MaxShownBytes = 256

// Quote is name for a message or log line: as is when it is valid UTF-8,
// otherwise Go-quoted, so an invalid byte is shown escaped and never written
// raw. Cut to MaxShownBytes first.
func Quote(name string) string {
	name = Truncate(name)
	if utf8.ValidString(name) {
		return name
	}
	return strconv.Quote(name)
}

// Truncate is name cut to MaxShownBytes (whole runes when it is valid
// UTF-8), with "…" when cut: no length limit applies to names, and an answer
// or log line must not echo an arbitrarily long one.
func Truncate(name string) string {
	if len(name) <= MaxShownBytes {
		return name
	}
	cut := MaxShownBytes
	if utf8.ValidString(name) {
		for cut > 0 && !utf8.RuneStart(name[cut]) {
			cut--
		}
	}
	return name[:cut] + "…"
}
