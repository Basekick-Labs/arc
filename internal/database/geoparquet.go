package database

import (
	"context"
	"database/sql"
	"fmt"
)

// DisableGeoParquetConversion makes the engine read a GeoParquet column (one
// a file's "geo" metadata names) as the BLOB it is stored as, not as
// GEOMETRY, and reads the setting back (#440).
//
// Arc stores geometries as WKB in binary columns. With conversion on (the
// engine's default), one GeoParquet file in a measurement turns the column
// into GEOMETRY for every file read with it, and the engine cannot cast the
// other files' BLOB to GEOMETRY: every query that reads the column fails, and
// so does compaction of the partition. Queries written for the BLOB column
// (ST_GeomFromWKB(geom), octet_length) would fail on GEOMETRY too.
//
// GLOBAL: a plain SET is per connection (the setting is LOCAL scope), and
// every other connection of the pool would read with conversion on. A file
// that uses Parquet's own GEOMETRY type (not the "geo"
// metadata) still reads as GEOMETRY; such files must come in through the
// import API, which stores them as BLOB.
func DisableGeoParquetConversion(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "SET GLOBAL enable_geoparquet_conversion = false"); err != nil {
		return fmt.Errorf("disable GeoParquet conversion: %w", err)
	}
	var on bool
	if err := db.QueryRowContext(ctx, "SELECT current_setting('enable_geoparquet_conversion')::BOOLEAN").Scan(&on); err != nil {
		return fmt.Errorf("read back enable_geoparquet_conversion: %w", err)
	}
	if on {
		return fmt.Errorf("enable_geoparquet_conversion is still on after SET GLOBAL")
	}
	return nil
}
