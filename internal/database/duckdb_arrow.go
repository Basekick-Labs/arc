//go:build duckdb_arrow

package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

// arrowQueryOnConn executes a query via the Arrow API on a raw driver connection.
func arrowQueryOnConn(ctx context.Context, driverConn any, query string) (array.RecordReader, error) {
	dc, ok := driverConn.(driver.Conn)
	if !ok {
		return nil, fmt.Errorf("connection does not implement driver.Conn")
	}
	arrowAPI, err := duckdb.NewArrowFromConn(dc)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow interface: %w", err)
	}
	return arrowAPI.QueryContext(ctx, query)
}

// ArrowQueryContext executes a query using DuckDB's native Arrow API,
// returning an array.RecordReader that yields Arrow record batches directly
// from DuckDB's internal columnar chunks — no row-by-row scanning.
//
// The caller MUST close both resources when done:
//  1. reader.Release() — releases Arrow batches and closes underlying rows
//  2. conn.Close() — returns the connection to the pool
func (d *DuckDB) ArrowQueryContext(ctx context.Context, query string) (array.RecordReader, *sql.Conn, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	var reader array.RecordReader
	err = conn.Raw(func(driverConn any) error {
		var rawErr error
		reader, rawErr = arrowQueryOnConn(ctx, driverConn, query)
		return rawErr
	})

	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("arrow query failed: %w", err)
	}

	d.logger.Debug().
		Str("query", query).
		Msg("Arrow query executed")

	return reader, conn, nil
}

// ArrowQueryWithProfileContext executes a query with DuckDB profiling enabled,
// returning Arrow record batches and a QueryProfile with timing breakdown.
//
// The caller MUST close both resources when done:
//  1. reader.Release() — releases Arrow batches and closes underlying rows
//  2. conn.Close() — returns the connection to the pool
func (d *DuckDB) ArrowQueryWithProfileContext(ctx context.Context, query string) (array.RecordReader, *sql.Conn, *QueryProfile, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	// Create temp file for profiling output
	tmpFile, err := os.CreateTemp("", "duckdb_profile_*.json")
	if err != nil {
		// Fall back to non-profile Arrow query
		var reader array.RecordReader
		rawErr := conn.Raw(func(driverConn any) error {
			var err error
			reader, err = arrowQueryOnConn(ctx, driverConn, query)
			return err
		})
		if rawErr != nil {
			conn.Close()
			return nil, nil, nil, rawErr
		}
		return reader, conn, nil, nil
	}
	profilePath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(profilePath)

	// Enable profiling on this specific connection
	if _, err := conn.ExecContext(ctx, "PRAGMA enable_profiling='json'"); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to enable profiling")
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA profiling_output='%s'", profilePath)); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to set profiling output")
	}
	if _, err := conn.ExecContext(ctx, "SET custom_profiling_settings='{\"PLANNER\": \"true\", \"PLANNER_BINDING\": \"true\", \"PHYSICAL_PLANNER\": \"true\", \"OPERATOR_TIMING\": \"true\", \"OPERATOR_CARDINALITY\": \"true\"}'"); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to set custom profiling settings")
	}

	// Execute query via Arrow API
	start := time.Now()
	var reader array.RecordReader
	rawErr := conn.Raw(func(driverConn any) error {
		var err error
		reader, err = arrowQueryOnConn(ctx, driverConn, query)
		return err
	})
	totalTime := time.Since(start)

	// Disable profiling
	conn.ExecContext(ctx, "PRAGMA disable_profiling")

	if rawErr != nil {
		conn.Close()
		return nil, nil, nil, fmt.Errorf("arrow query failed: %w", rawErr)
	}

	// Read profile data
	profile := &QueryProfile{
		TotalMs: float64(totalTime.Milliseconds()),
	}
	if profileData, err := os.ReadFile(profilePath); err == nil && len(profileData) > 0 {
		var profileJSON map[string]interface{}
		if err := json.Unmarshal(profileData, &profileJSON); err == nil {
			if timing, ok := profileJSON["timing"].(float64); ok {
				profile.Latency = timing * 1000
			}
			if children, ok := profileJSON["children"].([]interface{}); ok {
				for _, child := range children {
					if childMap, ok := child.(map[string]interface{}); ok {
						if name, ok := childMap["name"].(string); ok && name == "PLANNER" {
							if timing, ok := childMap["timing"].(float64); ok {
								profile.PlannerMs = timing * 1000
							}
						}
					}
				}
			}
			profile.ExecutionMs = profile.TotalMs - profile.PlannerMs
		}
	}

	return reader, conn, profile, nil
}
