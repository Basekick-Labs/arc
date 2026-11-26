package api

// validateSQLErr is ValidateSQLRequest for tests that only check whether a
// statement is accepted.
func validateSQLErr(sql string) error {
	_, err := ValidateSQLRequest(sql)
	return err
}
