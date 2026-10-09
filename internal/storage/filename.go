package storage

import "strings"

// StripRewriteSuffix returns the logical parquet path represented by an
// immutable DELETE rewrite. A rewrite is named <logical>_rewrite_<unixnano>.parquet.
// Non-rewrite names are returned unchanged. Keeping this parser in storage
// gives compaction, tiering, pruning and DELETE one definition of the filename
// convention.
func StripRewriteSuffix(filename string) string {
	const marker = "_rewrite_"
	if !strings.HasSuffix(filename, ".parquet") {
		return filename
	}
	base := strings.TrimSuffix(filename, ".parquet")
	idx := strings.LastIndex(base, marker)
	if idx < 0 || idx+len(marker) == len(base) {
		return filename
	}
	for _, r := range base[idx+len(marker):] {
		if r < '0' || r > '9' {
			return filename
		}
	}
	return base[:idx] + ".parquet"
}
