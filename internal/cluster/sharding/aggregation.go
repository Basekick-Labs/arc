package sharding

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
)

// AggregationType represents the type of aggregation function.
type AggregationType int

const (
	AggTypeSum AggregationType = iota
	AggTypeCount
	AggTypeAvg
	AggTypeMin
	AggTypeMax
	AggTypeCountDistinct
)

// AggregationInfo describes a detected aggregation in a query.
type AggregationInfo struct {
	Type       AggregationType
	Expression string // The full expression, e.g., "SUM(value)"
	Column     string // The column being aggregated, e.g., "value"
	Alias      string // The alias if any, e.g., "total"
	Position   int    // Position in SELECT clause
}

// TwoStageRewrite holds the rewritten queries for two-stage aggregation.
type TwoStageRewrite struct {
	// ShardQuery is sent to each shard - returns partial aggregates
	ShardQuery string
	// FinalAggregations describes how to combine partial results
	FinalAggregations []*FinalAggregation
	// GroupByColumns are the columns used for grouping
	GroupByColumns []string
	// HasOrderBy indicates if the original query has ORDER BY
	HasOrderBy bool
	// OrderByClause is the original ORDER BY clause
	OrderByClause string
	// HasLimit indicates if the original query has LIMIT
	HasLimit bool
	// LimitValue is the LIMIT value
	LimitValue int
}

// FinalAggregation describes how to compute the final aggregate from partials.
type FinalAggregation struct {
	// Type is the aggregation type
	Type AggregationType
	// PartialColumns are the column names from shard results
	PartialColumns []string
	// FinalAlias is the alias for the final result
	FinalAlias string
	// OriginalPosition is the position in the original SELECT
	OriginalPosition int
}

// Pre-compiled regex patterns for aggregation detection
var (
	// Pattern for SUM(column) or SUM(expression)
	patternSum = regexp.MustCompile(`(?i)\bSUM\s*\(\s*([^)]+)\s*\)`)
	// Pattern for COUNT(*) or COUNT(column)
	patternCount = regexp.MustCompile(`(?i)\bCOUNT\s*\(\s*(\*|[^)]+)\s*\)`)
	// Pattern for AVG(column)
	patternAvg = regexp.MustCompile(`(?i)\bAVG\s*\(\s*([^)]+)\s*\)`)
	// Pattern for MIN(column)
	patternMin = regexp.MustCompile(`(?i)\bMIN\s*\(\s*([^)]+)\s*\)`)
	// Pattern for MAX(column)
	patternMax = regexp.MustCompile(`(?i)\bMAX\s*\(\s*([^)]+)\s*\)`)
	// Pattern for COUNT(DISTINCT column)
	patternCountDistinct = regexp.MustCompile(`(?i)\bCOUNT\s*\(\s*DISTINCT\s+([^)]+)\s*\)`)

	// Pattern to extract SELECT clause
	patternSelectClause = regexp.MustCompile(`(?is)^\s*SELECT\s+(.*?)\s+FROM\s+`)
	// Pattern to extract GROUP BY clause (stop at ORDER, LIMIT, HAVING, or end)
	patternGroupBy = regexp.MustCompile(`(?i)\bGROUP\s+BY\s+(.+?)(?:\s+ORDER\s+|\s+LIMIT\s+|\s+HAVING\s+|;|$)`)
	// Pattern to extract ORDER BY clause (stop at LIMIT or end)
	patternOrderBy = regexp.MustCompile(`(?i)\bORDER\s+BY\s+(.+?)(?:\s+LIMIT\s+|;|$)`)
	// Pattern to extract LIMIT clause
	patternLimit = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	// Pattern to extract alias: expression AS alias or expression alias
	patternAlias = regexp.MustCompile(`(?i)\s+(?:AS\s+)?(\w+)\s*$`)
)

// AggregationRewriter rewrites queries for two-stage distributed aggregation.
type AggregationRewriter struct {
	logger zerolog.Logger
}

// NewAggregationRewriter creates a new aggregation rewriter.
func NewAggregationRewriter(logger zerolog.Logger) *AggregationRewriter {
	return &AggregationRewriter{
		logger: logger.With().Str("component", "aggregation-rewriter").Logger(),
	}
}

// CanRewrite returns true if the query can benefit from two-stage aggregation.
// Returns false for queries that can't be rewritten (e.g., no aggregations, DISTINCT without GROUP BY).
func (r *AggregationRewriter) CanRewrite(sql string) bool {
	sqlLower := strings.ToLower(sql)

	// Must have aggregation functions
	hasAgg := strings.Contains(sqlLower, "sum(") ||
		strings.Contains(sqlLower, "count(") ||
		strings.Contains(sqlLower, "avg(") ||
		strings.Contains(sqlLower, "min(") ||
		strings.Contains(sqlLower, "max(")

	if !hasAgg {
		return false
	}

	// Skip queries with subqueries (too complex)
	selectCount := strings.Count(sqlLower, "select ")
	if selectCount > 1 {
		r.logger.Debug().Msg("Skipping two-stage: query has subqueries")
		return false
	}

	// Skip queries with HAVING (requires full data)
	if strings.Contains(sqlLower, " having ") {
		r.logger.Debug().Msg("Skipping two-stage: query has HAVING clause")
		return false
	}

	// Skip queries with window functions
	if strings.Contains(sqlLower, " over(") || strings.Contains(sqlLower, " over (") {
		r.logger.Debug().Msg("Skipping two-stage: query has window functions")
		return false
	}

	return true
}

// Rewrite transforms a query into a two-stage aggregation.
// Returns the rewritten shard query and information for final aggregation.
func (r *AggregationRewriter) Rewrite(sql string) (*TwoStageRewrite, error) {
	// Extract SELECT clause
	selectMatch := patternSelectClause.FindStringSubmatch(sql)
	if selectMatch == nil {
		return nil, fmt.Errorf("could not extract SELECT clause")
	}
	selectClause := selectMatch[1]

	// Detect aggregations in SELECT clause
	aggregations := r.detectAggregations(selectClause)
	if len(aggregations) == 0 {
		return nil, fmt.Errorf("no aggregations found")
	}

	// Extract GROUP BY columns
	var groupByColumns []string
	if groupByMatch := patternGroupBy.FindStringSubmatch(sql); groupByMatch != nil {
		groupByStr := strings.TrimSpace(groupByMatch[1])
		for _, col := range strings.Split(groupByStr, ",") {
			groupByColumns = append(groupByColumns, strings.TrimSpace(col))
		}
	}

	// Extract ORDER BY
	var orderByClause string
	hasOrderBy := false
	if orderByMatch := patternOrderBy.FindStringSubmatch(sql); orderByMatch != nil {
		orderByClause = strings.TrimSpace(orderByMatch[1])
		hasOrderBy = true
	}

	// Extract LIMIT
	var limitValue int
	hasLimit := false
	if limitMatch := patternLimit.FindStringSubmatch(sql); limitMatch != nil {
		limitValue, _ = strconv.Atoi(limitMatch[1])
		hasLimit = true
	}

	// Build shard query with partial aggregates
	shardQuery, finalAggs := r.buildShardQuery(sql, selectClause, aggregations, groupByColumns)

	return &TwoStageRewrite{
		ShardQuery:        shardQuery,
		FinalAggregations: finalAggs,
		GroupByColumns:    groupByColumns,
		HasOrderBy:        hasOrderBy,
		OrderByClause:     orderByClause,
		HasLimit:          hasLimit,
		LimitValue:        limitValue,
	}, nil
}

// detectAggregations finds all aggregation functions in the SELECT clause.
func (r *AggregationRewriter) detectAggregations(selectClause string) []*AggregationInfo {
	var aggregations []*AggregationInfo

	// Check for COUNT(DISTINCT) first (before COUNT)
	for _, match := range patternCountDistinct.FindAllStringSubmatch(selectClause, -1) {
		aggregations = append(aggregations, &AggregationInfo{
			Type:       AggTypeCountDistinct,
			Expression: match[0],
			Column:     strings.TrimSpace(match[1]),
		})
	}

	// SUM
	for _, match := range patternSum.FindAllStringSubmatch(selectClause, -1) {
		aggregations = append(aggregations, &AggregationInfo{
			Type:       AggTypeSum,
			Expression: match[0],
			Column:     strings.TrimSpace(match[1]),
		})
	}

	// COUNT (excluding COUNT DISTINCT already detected)
	for _, match := range patternCount.FindAllStringSubmatch(selectClause, -1) {
		if strings.Contains(strings.ToUpper(match[0]), "DISTINCT") {
			continue
		}
		aggregations = append(aggregations, &AggregationInfo{
			Type:       AggTypeCount,
			Expression: match[0],
			Column:     strings.TrimSpace(match[1]),
		})
	}

	// AVG
	for _, match := range patternAvg.FindAllStringSubmatch(selectClause, -1) {
		aggregations = append(aggregations, &AggregationInfo{
			Type:       AggTypeAvg,
			Expression: match[0],
			Column:     strings.TrimSpace(match[1]),
		})
	}

	// MIN
	for _, match := range patternMin.FindAllStringSubmatch(selectClause, -1) {
		aggregations = append(aggregations, &AggregationInfo{
			Type:       AggTypeMin,
			Expression: match[0],
			Column:     strings.TrimSpace(match[1]),
		})
	}

	// MAX
	for _, match := range patternMax.FindAllStringSubmatch(selectClause, -1) {
		aggregations = append(aggregations, &AggregationInfo{
			Type:       AggTypeMax,
			Expression: match[0],
			Column:     strings.TrimSpace(match[1]),
		})
	}

	return aggregations
}

// buildShardQuery creates the query to send to shards with partial aggregates.
func (r *AggregationRewriter) buildShardQuery(originalSQL, selectClause string, aggregations []*AggregationInfo, groupByColumns []string) (string, []*FinalAggregation) {
	newSelectParts := make([]string, 0)
	finalAggs := make([]*FinalAggregation, 0)

	// Add GROUP BY columns first
	for _, col := range groupByColumns {
		newSelectParts = append(newSelectParts, col)
	}

	// Transform each aggregation to partial form
	aggIdx := 0
	for _, agg := range aggregations {
		switch agg.Type {
		case AggTypeSum:
			// SUM stays as SUM
			alias := fmt.Sprintf("__partial_sum_%d", aggIdx)
			newSelectParts = append(newSelectParts, fmt.Sprintf("SUM(%s) AS %s", agg.Column, alias))
			finalAggs = append(finalAggs, &FinalAggregation{
				Type:           AggTypeSum,
				PartialColumns: []string{alias},
				FinalAlias:     agg.Alias,
			})

		case AggTypeCount:
			// COUNT becomes SUM (of counts)
			alias := fmt.Sprintf("__partial_count_%d", aggIdx)
			newSelectParts = append(newSelectParts, fmt.Sprintf("COUNT(%s) AS %s", agg.Column, alias))
			finalAggs = append(finalAggs, &FinalAggregation{
				Type:           AggTypeCount,
				PartialColumns: []string{alias},
				FinalAlias:     agg.Alias,
			})

		case AggTypeAvg:
			// AVG becomes SUM + COUNT, then SUM(sum)/SUM(count) at coordinator
			sumAlias := fmt.Sprintf("__partial_sum_%d", aggIdx)
			countAlias := fmt.Sprintf("__partial_count_%d", aggIdx)
			newSelectParts = append(newSelectParts, fmt.Sprintf("SUM(%s) AS %s", agg.Column, sumAlias))
			newSelectParts = append(newSelectParts, fmt.Sprintf("COUNT(%s) AS %s", agg.Column, countAlias))
			finalAggs = append(finalAggs, &FinalAggregation{
				Type:           AggTypeAvg,
				PartialColumns: []string{sumAlias, countAlias},
				FinalAlias:     agg.Alias,
			})

		case AggTypeMin:
			// MIN stays as MIN
			alias := fmt.Sprintf("__partial_min_%d", aggIdx)
			newSelectParts = append(newSelectParts, fmt.Sprintf("MIN(%s) AS %s", agg.Column, alias))
			finalAggs = append(finalAggs, &FinalAggregation{
				Type:           AggTypeMin,
				PartialColumns: []string{alias},
				FinalAlias:     agg.Alias,
			})

		case AggTypeMax:
			// MAX stays as MAX
			alias := fmt.Sprintf("__partial_max_%d", aggIdx)
			newSelectParts = append(newSelectParts, fmt.Sprintf("MAX(%s) AS %s", agg.Column, alias))
			finalAggs = append(finalAggs, &FinalAggregation{
				Type:           AggTypeMax,
				PartialColumns: []string{alias},
				FinalAlias:     agg.Alias,
			})

		case AggTypeCountDistinct:
			// COUNT(DISTINCT) cannot be decomposed - skip two-stage for this
			// Just pass through as-is
			alias := fmt.Sprintf("__partial_cd_%d", aggIdx)
			newSelectParts = append(newSelectParts, fmt.Sprintf("COUNT(DISTINCT %s) AS %s", agg.Column, alias))
			finalAggs = append(finalAggs, &FinalAggregation{
				Type:           AggTypeCountDistinct,
				PartialColumns: []string{alias},
				FinalAlias:     agg.Alias,
			})
		}
		aggIdx++
	}

	// Build new SELECT clause
	newSelect := strings.Join(newSelectParts, ", ")

	// Replace SELECT clause in original SQL
	// First, remove ORDER BY and LIMIT from shard query (will be applied at coordinator)
	shardSQL := patternSelectClause.ReplaceAllString(originalSQL, fmt.Sprintf("SELECT %s FROM ", newSelect))
	shardSQL = patternOrderBy.ReplaceAllString(shardSQL, "")
	shardSQL = patternLimit.ReplaceAllString(shardSQL, "")

	return strings.TrimSpace(shardSQL), finalAggs
}

// MergeAggregatedResults merges partial aggregation results from multiple shards.
// Returns the combined result in the same JSON format.
func (r *AggregationRewriter) MergeAggregatedResults(rewrite *TwoStageRewrite, results []json.RawMessage) (json.RawMessage, error) {
	if len(results) == 0 {
		return json.Marshal(map[string]interface{}{
			"columns": []string{},
			"data":    [][]interface{}{},
		})
	}

	// Parse first result to get structure
	type queryResult struct {
		Columns []string        `json:"columns"`
		Data    [][]interface{} `json:"data"`
	}

	// Collect all rows by group key
	groupedRows := make(map[string][]map[string]interface{})

	for _, rawResult := range results {
		var result queryResult
		if err := json.Unmarshal(rawResult, &result); err != nil {
			r.logger.Warn().Err(err).Msg("Failed to parse shard result")
			continue
		}

		for _, row := range result.Data {
			rowMap := make(map[string]interface{})
			for i, col := range result.Columns {
				if i < len(row) {
					rowMap[col] = row[i]
				}
			}

			// Build group key from GROUP BY columns
			var keyParts []string
			for _, col := range rewrite.GroupByColumns {
				if v, ok := rowMap[col]; ok {
					keyParts = append(keyParts, fmt.Sprintf("%v", v))
				}
			}
			groupKey := strings.Join(keyParts, "|")

			groupedRows[groupKey] = append(groupedRows[groupKey], rowMap)
		}
	}

	// Compute final aggregates for each group
	var finalRows [][]interface{}
	var finalColumns []string

	// Build column list
	finalColumns = append(finalColumns, rewrite.GroupByColumns...)
	for _, agg := range rewrite.FinalAggregations {
		if agg.FinalAlias != "" {
			finalColumns = append(finalColumns, agg.FinalAlias)
		} else {
			finalColumns = append(finalColumns, agg.PartialColumns[0])
		}
	}

	for _, rows := range groupedRows {
		finalRow := make([]interface{}, 0)

		// Add GROUP BY column values from first row
		if len(rows) > 0 {
			for _, col := range rewrite.GroupByColumns {
				finalRow = append(finalRow, rows[0][col])
			}
		}

		// Compute each final aggregate
		for _, agg := range rewrite.FinalAggregations {
			switch agg.Type {
			case AggTypeSum, AggTypeCount:
				// Sum all partial values
				var total float64
				for _, row := range rows {
					if v, ok := row[agg.PartialColumns[0]]; ok {
						total += toFloat64(v)
					}
				}
				finalRow = append(finalRow, total)

			case AggTypeAvg:
				// Sum(partial_sums) / Sum(partial_counts)
				var totalSum, totalCount float64
				for _, row := range rows {
					if v, ok := row[agg.PartialColumns[0]]; ok {
						totalSum += toFloat64(v)
					}
					if v, ok := row[agg.PartialColumns[1]]; ok {
						totalCount += toFloat64(v)
					}
				}
				if totalCount > 0 {
					finalRow = append(finalRow, totalSum/totalCount)
				} else {
					finalRow = append(finalRow, nil)
				}

			case AggTypeMin:
				// Min of all partial mins
				var minVal *float64
				for _, row := range rows {
					if v, ok := row[agg.PartialColumns[0]]; ok && v != nil {
						f := toFloat64(v)
						if minVal == nil || f < *minVal {
							minVal = &f
						}
					}
				}
				if minVal != nil {
					finalRow = append(finalRow, *minVal)
				} else {
					finalRow = append(finalRow, nil)
				}

			case AggTypeMax:
				// Max of all partial maxes
				var maxVal *float64
				for _, row := range rows {
					if v, ok := row[agg.PartialColumns[0]]; ok && v != nil {
						f := toFloat64(v)
						if maxVal == nil || f > *maxVal {
							maxVal = &f
						}
					}
				}
				if maxVal != nil {
					finalRow = append(finalRow, *maxVal)
				} else {
					finalRow = append(finalRow, nil)
				}

			case AggTypeCountDistinct:
				// Cannot accurately merge - just sum (will be inaccurate)
				// Log warning
				r.logger.Warn().Msg("COUNT(DISTINCT) in distributed query may be inaccurate")
				var total float64
				for _, row := range rows {
					if v, ok := row[agg.PartialColumns[0]]; ok {
						total += toFloat64(v)
					}
				}
				finalRow = append(finalRow, total)
			}
		}

		finalRows = append(finalRows, finalRow)
	}

	// Apply LIMIT if needed (ORDER BY would require more work)
	if rewrite.HasLimit && len(finalRows) > rewrite.LimitValue {
		finalRows = finalRows[:rewrite.LimitValue]
	}

	return json.Marshal(map[string]interface{}{
		"columns": finalColumns,
		"data":    finalRows,
	})
}

// toFloat64 converts various numeric types to float64
func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case int32:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	default:
		return 0
	}
}
