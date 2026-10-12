package ingest

import "fmt"

// WALRowTarget returns the database and measurement a row-form WAL record
// names (database "default" when it names none).
func WALRowTarget(rec map[string]interface{}) (database, measurement string) {
	for _, key := range []string{"_measurement", "measurement", "m"} {
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
			switch key {
			case "_measurement", "measurement", "m", "_database", "database":
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
