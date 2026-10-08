package replicaview

import (
	"fmt"
	"strconv"
	"strings"
)

func quoteString(value string) string     { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
func quoteIdentifier(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }

// SQL renders the immutable snapshot as a relation. WITH ORDINALITY numbers a
// single replica file before the entry-range filter. Explicit column aliases
// keep every user column intact, even one named ordinality or file_row_number.
func (s *Snapshot) SQL(resolve func(string) string, anchor string, options string) (string, error) {
	if s.Err != nil {
		return "", s.Err
	}
	var queries []string
	if anchor != "" {
		queries = append(queries, "SELECT * FROM read_parquet("+quoteString(anchor)+", hive_partitioning=false)")
	}
	for _, source := range s.Sources {
		readPath := source.File.ReadPath
		if readPath == "" {
			readPath = source.File.Path
		}
		path := quoteString(resolve(readPath))
		if !source.File.Metadata.IsReplica() {
			suffix := ""
			if options != "" {
				suffix = ", " + options
			}
			queries = append(queries, "SELECT * FROM read_parquet("+path+suffix+")")
			continue
		}
		columns := source.File.Metadata.Columns
		if len(columns) == 0 {
			return "", fmt.Errorf("replica file lacks column names: %s", source.File.Path)
		}
		// Alias all physical fields positionally; the last alias belongs to the
		// table function's generated ordinal, and cannot collide with user names.
		aliases := make([]string, len(columns)+1)
		projection := make([]string, len(columns))
		for i, column := range columns {
			aliases[i] = quoteIdentifier("c" + strconv.Itoa(i))
			projection[i] = "r." + aliases[i] + " AS " + quoteIdentifier(column)
		}
		aliases[len(columns)] = `"entry_row"`
		var ranges []string
		for _, segment := range source.Segments {
			ranges = append(ranges, fmt.Sprintf("(r.entry_row > %d AND r.entry_row <= %d)", segment.Start, segment.End))
		}
		if len(ranges) == 0 {
			continue
		}
		queries = append(queries, "SELECT "+strings.Join(projection, ",")+" FROM read_parquet("+path+", hive_partitioning=false) WITH ORDINALITY AS r("+strings.Join(aliases, ",")+") WHERE "+strings.Join(ranges, " OR "))
	}
	if len(queries) == 0 {
		return "", fmt.Errorf("replica query has no available schema")
	}
	return "(" + strings.Join(queries, " UNION ALL BY NAME ") + ")", nil
}
