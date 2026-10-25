package api

import (
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
)

// Every function a supported DuckDB extension adds that reads a file is
// refused by the query, continuous-query and DELETE validators, in every
// spelling DuckDB executes (#440).
func TestExtensionFileFunctionsRefused(t *testing.T) {
	denied := database.DeniedExtensionFunctions()
	for _, want := range []string{"st_read", "st_read_meta", "st_readosm", "st_readshp", "shapefile_meta", "st_drivers", "st_transform"} {
		if !containsString(denied, want) {
			t.Fatalf("%s is not denied", want)
		}
	}
	h := &DeleteHandler{}
	for _, fn := range denied {
		upper := strings.ToUpper(fn)
		for _, sql := range []string{
			"SELECT * FROM " + fn + "('x')",
			"SELECT * FROM cpu, " + upper + "  ('x')",
			`SELECT * FROM "` + upper + `"('x')`,
			"SELECT * FROM main." + fn + "('x')",
			"SELECT * FROM system.main." + fn + "('x')",
			`SELECT * FROM "system"."main"."` + fn + `"('x')`,
			"SELECT * FROM " + fn + "/* c */('x')",
			"SELECT * FROM cpu WHERE EXISTS (FROM " + fn + "('x'))",
			"SELECT (SELECT count(*) FROM " + fn + "('x'))",
		} {
			if err := ValidateSQLRequest(sql); err == nil {
				t.Errorf("query: %q was accepted", sql)
			}
			if err := validateCQQuery(sql); err == nil {
				t.Errorf("continuous query: %q was accepted", sql)
			}
		}
		for _, where := range []string{
			"EXISTS (FROM " + fn + "('x'))",
			"1=1 OR " + upper + "('x') IS NULL",
			`"` + fn + `"('x') IS NOT NULL`,
		} {
			if _, err := h.validateWhereClause(where); err == nil {
				t.Errorf("DELETE WHERE: %q was accepted", where)
			}
		}
		// The name as an ordinary identifier, not a call, is not refused.
		if err := ValidateSQLRequest("SELECT " + fn + "_x FROM cpu"); err != nil {
			t.Errorf("false positive on identifier %s_x: %v", fn, err)
		}
	}
}

// A file reader is refused when its name is a prefix of a longer denied one
// and the other way round: each name is its own alternative.
func TestExtensionFileFunctionsPrefixes(t *testing.T) {
	for _, sql := range []string{"SELECT * FROM st_read_meta('x')", "SELECT * FROM st_read('x')", "SELECT * FROM st_readshp('x')"} {
		if err := ValidateSQLRequest(sql); err == nil {
			t.Errorf("%q was accepted", sql)
		}
	}
}

// A unicode-escaped identifier can spell a denied name the denylist cannot
// read (U&"\0073t_read" is st_read); it is refused outright, in queries and
// DELETE predicates. The pinned DuckDB does not execute it, so this pins the
// refusal against a parser upgrade.
func TestUnicodeEscapedIdentifiersRefused(t *testing.T) {
	h := &DeleteHandler{}
	for _, sql := range []string{`SELECT * FROM U&"\0073t_read"('x')`, `SELECT * FROM u&"st\005Fread"('x')`, `SELECT U&'\0061' AS a`} {
		if err := ValidateSQLRequest(sql); err == nil {
			t.Errorf("query: %q was accepted", sql)
		}
	}
	if _, err := h.validateWhereClause(`u&"st\005Fread"('x') IS NOT NULL`); err == nil {
		t.Error("DELETE WHERE: a unicode-escaped call was accepted")
	}
	// Not an escape: U& inside a string, and a bitwise AND.
	for _, sql := range []string{`SELECT 'U&"x"' AS s`, "SELECT a & b FROM cpu"} {
		if err := ValidateSQLRequest(sql); err != nil {
			t.Errorf("%q refused: %v", sql, err)
		}
	}
}

// Spatial's geometry functions stay usable in queries and DELETE predicates.
func TestExtensionGeometryFunctionsAccepted(t *testing.T) {
	for _, sql := range []string{
		"SELECT ST_X(ST_Point(lon, lat)) FROM cpu",
		"SELECT ST_AsText(ST_GeomFromWKB(geom)) FROM places WHERE ST_Within(ST_GeomFromWKB(geom), ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))'))",
		"SELECT ST_Distance(ST_Point(0, 0), ST_Point(lon, lat)) AS d FROM cpu ORDER BY d LIMIT 5",
	} {
		if err := ValidateSQLRequest(sql); err != nil {
			t.Errorf("%q refused: %v", sql, err)
		}
	}
	h := &DeleteHandler{}
	if _, err := h.validateWhereClause("ST_X(ST_Point(lon, lat)) > 0"); err != nil {
		t.Errorf("DELETE WHERE on a geometry predicate refused: %v", err)
	}
}

// The spatial extension's other ways to reach a file: a GDAL COPY, and a
// file path in table position. Both are refused by rules that predate it;
// pinned here because no function list can show them.
func TestExtensionFileAccessWithoutAFunctionName(t *testing.T) {
	for _, sql := range []string{
		"COPY (SELECT 1) TO 'x.gpkg' (FORMAT GDAL, DRIVER 'GPKG')",
		"SELECT * FROM 'x.shp'",
		"SELECT * FROM 'x.gpkg'",
		"SELECT * FROM 'x.fgb'",
		"SELECT * FROM 'x.geojson'",
		"SELECT * FROM 'x.vrt'",
	} {
		if err := ValidateSQLRequest(sql); err == nil {
			t.Errorf("%q was accepted", sql)
		}
	}
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
