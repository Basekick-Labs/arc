package api

import (
	"errors"
	"strconv"
)

const defaultExecutionLimit = 50
const maxExecutionLimit = 1000

// parseExecutionLimit keeps execution-history pagination bounded before it
// reaches SQLite, where a negative LIMIT can mean an unbounded result set.
func parseExecutionLimit(raw string) (int, error) {
	if raw == "" {
		return defaultExecutionLimit, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 0 || limit > maxExecutionLimit {
		return 0, errors.New("invalid limit")
	}
	return limit, nil
}
