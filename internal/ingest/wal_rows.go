package ingest

import "fmt"

// WALRowTarget returns the database and measurement a row-form WAL record
// names (database "default" when it names none). Arc writes its rows with
// "_measurement" and "_database"; only rows without "_measurement" (entries
// from before Arc's Go WAL, or written by hand) are read by "measurement",
// "m" and "database", so a user column with one of those names stays a
// column.
func WALRowTarget(rec map[string]interface{}) (database, measurement string) {
	if m, ok := rec["_measurement"].(string); ok && m != "" {
		database, _ = rec["_database"].(string)
		if database == "" {
			database = "default"
		}
		return database, m
	}
	for _, key := range []string{"measurement", "m"} {
		measurement, _ = rec[key].(string)
		if measurement != "" {
			break
		}
	}
	for _, key := range []string{"_database", "database"} {
		database, _ = rec[key].(string)
		if database != "" {
			break
		}
	}
	if database == "" {
		database = "default"
	}
	return
}

// IsWALRowTargetKey reports whether key of rec names the row's target rather
// than a column (see WALRowTarget).
func IsWALRowTargetKey(rec map[string]interface{}, key string) bool {
	switch key {
	case "_measurement", "_database":
		return true
	case "measurement", "m", "database":
		m, _ := rec["_measurement"].(string)
		return m == "" // an Arc row names its target with "_measurement"
	}
	return false
}

// ValidateWALRows returns the one target the records of a row-form WAL
// entry name, refusing rows with no measurement or differing targets.
func ValidateWALRows(records []map[string]interface{}) (database, measurement string, err error) {
	for i, rec := range records {
		rowDatabase, rowMeasurement := WALRowTarget(rec)
		if rowMeasurement == "" {
			return "", "", fmt.Errorf("WAL row %d has no measurement", i)
		}
		if i == 0 {
			database, measurement = rowDatabase, rowMeasurement
		} else if database != rowDatabase || measurement != rowMeasurement {
			return "", "", fmt.Errorf("WAL row entry spans multiple databases or measurements")
		}
	}
	return database, measurement, nil
}

// WALRowsToColumns turns the records of one row-form WAL entry into columns,
// refusing an entry whose rows name no measurement or more than one target.
// Used by WAL recovery and by a stream takeover replay, so both read an
// entry the same way.
func WALRowsToColumns(records []map[string]interface{}) (database, measurement string, columns map[string][]interface{}, err error) {
	if database, measurement, err = ValidateWALRows(records); err != nil {
		return "", "", nil, err
	}
	columns = make(map[string][]interface{})
	for rowIndex, rec := range records {
		for key, value := range rec {
			// An explicit nil is a column NULL in every row of the entry
			// (see ingest.allNil): it stays, as the live write kept it.
			if IsWALRowTargetKey(rec, key) {
				continue
			}
			column, ok := columns[key]
			if !ok {
				column = make([]interface{}, len(records))
				columns[key] = column
			}
			column[rowIndex] = value
		}
	}
	return database, measurement, columns, nil
}

// IsWALTargetColumnName reports a column name the WAL's row form keeps for
// the row's target ("_measurement", "_database"): a column with that name
// would be lost or re-target its rows on replay (see WALRowTarget).
func IsWALTargetColumnName(name string) bool {
	return name == "_measurement" || name == "_database"
}
