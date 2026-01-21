// Package sql provides SQL parsing and manipulation utilities.
package sql

import (
	"fmt"
	"strings"
)

// StringMask holds a placeholder and the original string content it replaced.
type StringMask struct {
	Placeholder string
	Original    string
}

// MaskStringLiterals replaces string literals with placeholders to prevent regex from matching inside them.
// Handles both single-quoted ('...') and double-quoted ("...") strings, including escaped quotes.
// Returns the masked SQL and a slice of masks for later restoration.
//
// The hasQuotes parameter is an optimization - if false, the function returns the original
// string without any processing (avoids allocation).
func MaskStringLiterals(sql string, hasQuotes bool) (string, []StringMask) {
	// Fast path: if no quotes, return original string (avoids allocation)
	if !hasQuotes {
		return sql, nil
	}

	var masks []StringMask
	var result strings.Builder
	result.Grow(len(sql))

	i := 0
	maskIndex := 0

	for i < len(sql) {
		ch := sql[i]

		// Check for string literal start (single or double quote)
		if ch == '\'' || ch == '"' {
			quote := ch
			start := i
			i++ // Move past opening quote

			// Find the closing quote, handling escaped quotes
			for i < len(sql) {
				if sql[i] == quote {
					// Check if it's an escaped quote ('' or "")
					if i+1 < len(sql) && sql[i+1] == quote {
						i += 2 // Skip escaped quote
						continue
					}
					// Also handle backslash escaping (\' or \")
					if i > 0 && sql[i-1] == '\\' {
						i++
						continue
					}
					break // Found closing quote
				}
				i++
			}

			// Include the closing quote if found
			if i < len(sql) {
				i++
			}

			// Extract the full string literal and create a placeholder
			original := sql[start:i]
			placeholder := fmt.Sprintf("__STR_%d__", maskIndex)
			masks = append(masks, StringMask{Placeholder: placeholder, Original: original})
			result.WriteString(placeholder)
			maskIndex++
		} else {
			result.WriteByte(ch)
			i++
		}
	}

	return result.String(), masks
}

// UnmaskStringLiterals restores the original string literals from their placeholders.
func UnmaskStringLiterals(sql string, masks []StringMask) string {
	result := sql
	for _, mask := range masks {
		result = strings.Replace(result, mask.Placeholder, mask.Original, 1)
	}
	return result
}

// HasQuotes returns true if the SQL string contains any quote characters.
// This is a fast check to avoid unnecessary processing in MaskStringLiterals.
func HasQuotes(sql string) bool {
	return strings.ContainsAny(sql, "'\"")
}
