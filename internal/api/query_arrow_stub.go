//go:build !duckdb_arrow

package api

import "github.com/gofiber/fiber/v2"

// registerArrowRoutes is a no-op when Arrow support is not compiled in.
func (h *QueryHandler) registerArrowRoutes(_ *fiber.App, _ fiber.Handler) {}

// registerFlightSQLSchemaRoute is a no-op when Arrow support is not compiled
// in; NewFlightSQLServer refuses such a build.
func (h *QueryHandler) registerFlightSQLSchemaRoute(_ *fiber.App) {}
