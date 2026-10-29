package api

// Whole-column helpers for tests: production converts column chunks.

// inferAndConvertColumn converts a whole string column to the narrowest
// native type that fits all of it (see csvColumnKind and convertCSVColumn).
func inferAndConvertColumn(raw []string) (interface{}, []bool) {
	k := newCSVColumnKind()
	for _, s := range raw {
		k.observe(s)
	}
	return convertCSVColumn(raw, k)
}

// stringsToTimeMicros converts a whole string time column.
func stringsToTimeMicros(raw []string, timeFormat string) ([]int64, error) {
	return stringsToTimeMicrosFrom(raw, timeFormat, 0)
}

// buildImportResult is the answer for a whole time column.
func buildImportResult(database, measurement string, header []string, timeColumn string, timeMicros []int64) *ImportResult {
	var st importStats
	st.add(timeMicros)
	return st.result(database, measurement, header, timeColumn)
}
