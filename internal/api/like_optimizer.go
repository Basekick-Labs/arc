package api

import (
	"regexp"
	"strings"
)

// LIKE pattern optimizer for DuckDB queries.
//
// This optimizer reorders WHERE clause predicates to improve query performance
// by putting cheaper operations (like empty string checks) before expensive
// operations (like LIKE '%pattern%' scans).
//
// Rationale:
// - Empty string check (col <> '') is O(1) per row
// - LIKE '%pattern%' is O(n*m) where n=string length, m=pattern length
// - DuckDB evaluates predicates left-to-right and can short-circuit
// - By filtering out empty strings first, we reduce the number of expensive LIKE operations

var (
	// Pattern for: WHERE col LIKE/NOT LIKE 'pattern' AND col2 <> ''
	// We want to move the empty check first (cheaper operation)
	patternEmptyCheckAfterLike = regexp.MustCompile(
		`(?i)(WHERE\s+)(\w+\s+(?:NOT\s+)?LIKE\s+'[^']+')(\s+AND\s+)(\w+\s*<>\s*'')`)

	// Pattern for: WHERE <predicates> AND col <> '' (at end of WHERE clause)
	// Matches empty check before GROUP BY, ORDER BY, LIMIT, or end of query
	patternEndEmptyCheck = regexp.MustCompile(
		`(?i)(WHERE\s+)(.*?)(\s+AND\s+)(\w+\s*<>\s*'')(\s*(?:GROUP|ORDER|LIMIT|$))`)
)

// OptimizeLikePatterns rewrites SQL to reorder WHERE clause predicates
// for better query performance. Returns the optimized SQL and whether
// any changes were made.
func OptimizeLikePatterns(sql string) (string, bool) {
	// Fast path: no LIKE or WHERE keyword
	sqlUpper := strings.ToUpper(sql)
	if !strings.Contains(sqlUpper, "LIKE") || !strings.Contains(sqlUpper, "WHERE") {
		return sql, false
	}

	original := sql

	// Optimization 1: Move empty string checks before LIKE predicates
	// FROM: WHERE URL LIKE '%google%' AND SearchPhrase <> ''
	// TO:   WHERE SearchPhrase <> '' AND URL LIKE '%google%'
	sql = reorderEmptyCheckBeforeLike(sql)

	// Optimization 2: For complex predicates with multiple conditions,
	// ensure cheap checks come first
	sql = optimizeMultiplePredicates(sql)

	return sql, sql != original
}

// reorderEmptyCheckBeforeLike moves empty string checks before LIKE predicates
// when they are on different columns.
func reorderEmptyCheckBeforeLike(sql string) string {
	// Match pattern: WHERE col1 LIKE '%x%' AND col2 <> ''
	// Rewrite to:    WHERE col2 <> '' AND col1 LIKE '%x%'
	return patternEmptyCheckAfterLike.ReplaceAllString(sql, "${1}${4}${3}${2}")
}

// optimizeMultiplePredicates handles more complex cases with multiple predicates
func optimizeMultiplePredicates(sql string) string {
	// For queries like Q23:
	// WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''
	// We want: WHERE SearchPhrase <> '' AND Title LIKE '%Google%' AND URL NOT LIKE '%.google.%'

	// Find the pattern: ... AND col <> '' at the end of WHERE clause
	// and move it to the beginning

	return patternEndEmptyCheck.ReplaceAllStringFunc(sql, func(match string) string {
		parts := patternEndEmptyCheck.FindStringSubmatch(match)
		if len(parts) < 6 {
			return match
		}
		// parts[1] = "WHERE "
		// parts[2] = middle predicates
		// parts[3] = " AND "
		// parts[4] = "col <> ''"
		// parts[5] = rest (GROUP BY, etc.)

		// Don't reorder if the empty check is already first
		if strings.TrimSpace(parts[2]) == "" {
			return match
		}

		// Check if the middle predicates contain LIKE - only then is reordering beneficial
		if !strings.Contains(strings.ToUpper(parts[2]), "LIKE") {
			return match
		}

		// Reorder: WHERE col <> '' AND <other predicates> <rest>
		return parts[1] + parts[4] + parts[3] + parts[2] + parts[5]
	})
}
