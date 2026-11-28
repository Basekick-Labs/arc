//go:build duckdb_arrow

package api

import (
	"github.com/basekick-labs/arc/internal/database"
	"github.com/gofiber/fiber/v2"
)

func queryFailureStatusCode(err error) int {
	switch database.ClassifyDuckDBQueryError(err) {
	case database.DuckDBQueryErrorStatement:
		return fiber.StatusBadRequest
	case database.DuckDBQueryErrorStorage, database.DuckDBQueryErrorMemory:
		return fiber.StatusServiceUnavailable
	default:
		return fiber.StatusInternalServerError
	}
}
