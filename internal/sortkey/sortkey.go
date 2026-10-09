// Package sortkey parses the direction suffix used by ingest and compaction
// sort keys.
package sortkey

import "strings"

// Key describes one column in a sort specification. A missing direction is
// ascending for compatibility with existing configurations.
type Key struct {
	Column string
	Desc   bool
}

// Parse turns a configured key such as "time:desc" into its column and
// direction. Only the final :asc or :desc suffix is special; other colons
// remain part of the column name.
func Parse(spec string) Key {
	spec = strings.TrimSpace(spec)
	column, direction := spec, ""
	// Sort columns may themselves contain colons. Recognize a direction only
	// at the final separator.
	if last := strings.LastIndex(spec, ":"); last >= 0 {
		suffix := spec[last+1:]
		if strings.EqualFold(suffix, "asc") || strings.EqualFold(suffix, "desc") {
			column = strings.TrimSpace(spec[:last])
		direction = suffix
		}
	}
	return Key{Column: column, Desc: strings.EqualFold(direction, "desc")}
}

// TimeKey returns an explicitly configured time key when present, otherwise
// the new default for time-series data.
func TimeKey(keys []string) string {
	for _, raw := range keys {
		if Parse(raw).Column == "time" {
			return raw
		}
	}
	return "time:desc"
}
