package database

import (
	"fmt"
	"sort"
	"strings"
)

// supportedExtension is a DuckDB extension database.extensions may load.
//
// An extension adds SQL functions the query and DELETE validators were never
// checked against, and the validators are denylists. So an extension is
// supported only once its function surface has been reviewed: every table
// function, pragma and macro it adds, and every function taking text (which
// can name a file or URL), is either denied — refused by both validators —
// or recorded as reviewed safe. TestSupportedExtensionFunctionsReviewed loads
// each one and fails on any such signature it adds, including a new overload
// of an existing name, that is in neither list, and on any macro whose body
// names a denied function; a newer build of the extension cannot widen the
// surface unreviewed.
//
// A denied function is refused by name only. Arc's DuckDB sandbox does not
// bound what an extension's own I/O opens (spatial's GDAL and PROJ open files
// themselves), so for these the validators are the only control: never move
// one to reviewed without proving the library cannot open a path.
type supportedExtension struct {
	// denied are refused by the query, continuous query and DELETE
	// validators whether or not the extension is loaded.
	denied []string
	// reviewed are table functions and text-taking functions that touch no
	// file, URL or other database.
	reviewed []string
}

var supportedExtensions = map[string]supportedExtension{
	"spatial": {
		denied: []string{
			// Read a file through GDAL, which opens what it reads (and what
			// the file points at) with its own I/O rather than DuckDB's, so
			// the sandbox's allowed_directories does not bound it. Also a
			// read of another database's files inside the storage root.
			"st_read", "st_read_meta", "st_readosm", "st_readshp", "shapefile_meta",
			// Lists the GDAL drivers of the build.
			"st_drivers",
			// A CRS argument such as '+init=<path>:<id>' makes PROJ open a
			// file at any host path, outside the sandbox (#440).
			"st_transform",
		},
		reviewed: []string{
			"st_generatepoints", "rtree_index_scan", "rtree_index_dump", "pragma_rtree_index_info",
			// Parse their text argument inline.
			"st_makevalid", "st_assvg", "st_quadkey", "st_buffer",
			"st_ashexwkb", "st_geomfromgeojson", "st_geomfromhexewkb", "st_geomfromhexwkb",
			"st_geomfromtext", "st_geometrytype", "st_asmvt",
			"duckdb_proj_version", "duckdb_proj_compiled_version",
			// Macros over ST_Affine.
			"st_rotate", "st_rotatex", "st_rotatey", "st_rotatez", "st_scale", "st_translate", "st_transscale",
		},
	},
}

// SupportedExtensionNames is the sorted set database.extensions accepts.
func SupportedExtensionNames() []string {
	names := make([]string, 0, len(supportedExtensions))
	for name := range supportedExtensions {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// DeniedExtensionFunctions is every function a supported extension adds that
// the validators refuse, lower-case and sorted. It does not depend on which
// extensions this node loads: a denied name nothing defines costs nothing,
// and a validator whose rules change with configuration is one a reviewer
// cannot read.
func DeniedExtensionFunctions() []string {
	var out []string
	for _, ext := range supportedExtensions {
		out = append(out, ext.denied...)
	}
	sort.Strings(out)
	return out
}

// Extensions is the DuckDB extensions this engine loaded from
// database.extensions (#440). Read-only after New.
func (d *DuckDB) Extensions() []string {
	if d == nil || d.config == nil {
		return nil
	}
	return d.config.Extensions
}

// validateExtensions refuses any entry that is not a supported extension's
// bare name. INSTALL "<entry>" also accepts a local path or a URL, and with
// arcx configured the engine allows unsigned extensions, so an unchecked
// entry would load arbitrary native code.
func validateExtensions(names []string) error {
	if name, ok := unsupportedExtension(names); ok {
		return fmt.Errorf("database.extensions: %q is not a supported DuckDB extension (supported: %s)",
			name, strings.Join(SupportedExtensionNames(), ", "))
	}
	return nil
}

// unsupportedExtension is the first entry of names that is not supported.
func unsupportedExtension(names []string) (string, bool) {
	for _, name := range names {
		if _, ok := supportedExtensions[name]; !ok {
			return name, true
		}
	}
	return "", false
}
