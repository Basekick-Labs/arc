package api

import "testing"

// A user cannot turn GeoParquet conversion back on (#440): with it on, a
// GeoParquet file next to Arc's BLOB files makes every query of the column
// fail. The setting is not protected by the engine's lockdown, only by the
// validators refusing SET.
func TestGeoParquetConversionCannotBeTurnedOn(t *testing.T) {
	for _, sql := range []string{
		"SET GLOBAL enable_geoparquet_conversion = true",
		"SET enable_geoparquet_conversion = true",
		"RESET enable_geoparquet_conversion",
		"SELECT 1; SET enable_geoparquet_conversion = true",
	} {
		if err := ValidateSQLRequest(sql); err == nil {
			t.Errorf("query: %q was accepted", sql)
		}
		if err := validateCQQuery(sql); err == nil {
			t.Errorf("continuous query: %q was accepted", sql)
		}
	}
	h := &DeleteHandler{}
	if _, err := h.validateWhereClause("1=1; SET GLOBAL enable_geoparquet_conversion = true"); err == nil {
		t.Error("DELETE WHERE: a SET was accepted")
	}
}
