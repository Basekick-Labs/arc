package api

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/rs/zerolog"
)

func TestExtractCTENames(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name:     "single CTE",
			sql:      "WITH campaign AS (SELECT * FROM events) SELECT * FROM campaign",
			expected: []string{"campaign"},
		},
		{
			name:     "multiple CTEs",
			sql:      "WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1 JOIN cte2",
			expected: []string{"cte1", "cte2"},
		},
		{
			name:     "recursive CTE",
			sql:      "WITH RECURSIVE tree AS (SELECT * FROM nodes) SELECT * FROM tree",
			expected: []string{"tree"},
		},
		{
			name:     "CTE with underscore",
			sql:      "WITH user_data AS (SELECT * FROM users) SELECT * FROM user_data",
			expected: []string{"user_data"},
		},
		{
			name:     "no CTE",
			sql:      "SELECT * FROM mydb.cpu",
			expected: []string{},
		},
		{
			name:     "case insensitive WITH",
			sql:      "with MyData AS (SELECT 1) SELECT * FROM mydata",
			expected: []string{"mydata"},
		},
		{
			name:     "case insensitive RECURSIVE",
			sql:      "WITH recursive myTree AS (SELECT 1) SELECT * FROM mytree",
			expected: []string{"mytree"},
		},
		{
			name:     "three CTEs",
			sql:      "WITH a AS (SELECT 1), b AS (SELECT 2), c AS (SELECT 3) SELECT * FROM a, b, c",
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "CTE with complex inner query",
			sql:      "WITH hourly_avg AS (SELECT host, AVG(value) as avg_val FROM mydb.cpu WHERE time > '2024-01-01' GROUP BY host) SELECT * FROM hourly_avg",
			expected: []string{"hourly_avg"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractCTENames(tt.sql)
			if len(result) != len(tt.expected) {
				t.Errorf("extractCTENames(%q) returned %d names, expected %d",
					tt.sql, len(result), len(tt.expected))
				return
			}
			for _, name := range tt.expected {
				if !result[name] {
					t.Errorf("extractCTENames(%q) missing expected name %q", tt.sql, name)
				}
			}
		})
	}
}

func TestMaskStringLiterals(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedMasked string
		expectedMasks  int
	}{
		{
			name:           "single quoted string",
			input:          "SELECT * FROM t WHERE x = 'hello'",
			expectedMasked: "SELECT * FROM t WHERE x = __STR_0__",
			expectedMasks:  1,
		},
		{
			name:           "double quoted string",
			input:          `SELECT * FROM t WHERE x = "hello"`,
			expectedMasked: "SELECT * FROM t WHERE x = __STR_0__",
			expectedMasks:  1,
		},
		{
			name:           "multiple strings",
			input:          "SELECT * FROM t WHERE x = 'hello' AND y = 'world'",
			expectedMasked: "SELECT * FROM t WHERE x = __STR_0__ AND y = __STR_1__",
			expectedMasks:  2,
		},
		{
			name:           "string with FROM keyword",
			input:          "SELECT * FROM logs WHERE msg = 'SELECT * FROM mydb.cpu'",
			expectedMasked: "SELECT * FROM logs WHERE msg = __STR_0__",
			expectedMasks:  1,
		},
		{
			name:           "escaped single quote",
			input:          "SELECT * FROM t WHERE x = 'it''s ok'",
			expectedMasked: "SELECT * FROM t WHERE x = __STR_0__",
			expectedMasks:  1,
		},
		{
			name:           "no strings",
			input:          "SELECT * FROM mydb.cpu WHERE time > 1234",
			expectedMasked: "SELECT * FROM mydb.cpu WHERE time > 1234",
			expectedMasks:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			features := scanSQLFeatures(tt.input)
			masked, masks := maskStringLiterals(tt.input, features.hasQuotes)
			if masked != tt.expectedMasked {
				t.Errorf("maskStringLiterals() masked = %q, want %q", masked, tt.expectedMasked)
			}
			if len(masks) != tt.expectedMasks {
				t.Errorf("maskStringLiterals() masks count = %d, want %d", len(masks), tt.expectedMasks)
			}
		})
	}
}

func TestUnmaskStringLiterals(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		masks    []stringMask
		expected string
	}{
		{
			name:     "restore single string",
			input:    "SELECT * FROM t WHERE x = __STR_0__",
			masks:    []stringMask{{placeholder: "__STR_0__", original: "'hello'"}},
			expected: "SELECT * FROM t WHERE x = 'hello'",
		},
		{
			name:  "restore multiple strings",
			input: "SELECT * FROM t WHERE x = __STR_0__ AND y = __STR_1__",
			masks: []stringMask{
				{placeholder: "__STR_0__", original: "'hello'"},
				{placeholder: "__STR_1__", original: "'world'"},
			},
			expected: "SELECT * FROM t WHERE x = 'hello' AND y = 'world'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := unmaskStringLiterals(tt.input, tt.masks)
			if result != tt.expected {
				t.Errorf("unmaskStringLiterals() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestStripSQLComments(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single line comment at end",
			input:    "SELECT * FROM mydb.cpu -- this is a comment",
			expected: "SELECT * FROM mydb.cpu ",
		},
		{
			name:     "single line comment with newline",
			input:    "SELECT * FROM mydb.cpu -- comment\nWHERE time > 1234",
			expected: "SELECT * FROM mydb.cpu \nWHERE time > 1234",
		},
		{
			name:     "multi-line comment",
			input:    "SELECT * FROM mydb.cpu /* comment */ WHERE time > 1234",
			expected: "SELECT * FROM mydb.cpu   WHERE time > 1234",
		},
		{
			name:     "multi-line comment spanning lines",
			input:    "SELECT * /* start\nend */ FROM mydb.cpu",
			expected: "SELECT *   FROM mydb.cpu",
		},
		{
			name:     "no comments",
			input:    "SELECT * FROM mydb.cpu WHERE time > 1234",
			expected: "SELECT * FROM mydb.cpu WHERE time > 1234",
		},
		{
			name:     "comment with table reference",
			input:    "SELECT * FROM mydb.cpu -- also query mydb.memory\nWHERE time > 1234",
			expected: "SELECT * FROM mydb.cpu \nWHERE time > 1234",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			features := scanSQLFeatures(tt.input)
			result := stripSQLComments(tt.input, features.hasDashComment || features.hasBlockComment)
			if result != tt.expected {
				t.Errorf("stripSQLComments() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestConvertSQLWithCTE(t *testing.T) {
	// Create a minimal QueryHandler for testing
	h := &QueryHandler{
		storage: &mockLocalBackend{basePath: "./data"},
		pruner:  pruning.NewPartitionPruner(zerolog.Nop()),
		logger:  zerolog.Nop(),
	}

	tests := []struct {
		name             string
		inputSQL         string
		shouldContain    []string // Substrings that SHOULD be in the result
		shouldNotContain []string // Substrings that should NOT be in the result
	}{
		{
			name:     "CTE name not converted to path",
			inputSQL: "WITH campaign AS (SELECT * FROM mydb.events WHERE type = 'campaign') SELECT * FROM campaign WHERE time > '2024-01-01'",
			shouldContain: []string{
				"read_parquet('./data/mydb/events/**/*.parquet'", // physical table converted
				"FROM campaign WHERE",                            // CTE reference preserved
			},
			shouldNotContain: []string{
				"read_parquet('./data/default/campaign/", // CTE should NOT be converted to path
			},
		},
		{
			name:     "multiple CTEs not converted",
			inputSQL: "WITH a AS (SELECT * FROM mydb.t1), b AS (SELECT * FROM mydb.t2) SELECT * FROM a JOIN b ON a.id = b.id",
			shouldContain: []string{
				"read_parquet('./data/mydb/t1/**/*.parquet'", // t1 converted
				"read_parquet('./data/mydb/t2/**/*.parquet'", // t2 converted
				"FROM a JOIN b",                               // CTE references preserved
			},
			shouldNotContain: []string{
				"read_parquet('./data/default/a/",
				"read_parquet('./data/default/b/",
			},
		},
		{
			name:     "simple query without CTE still works",
			inputSQL: "SELECT * FROM mydb.cpu WHERE time > '2024-01-01'",
			shouldContain: []string{
				"read_parquet('./data/mydb/cpu/**/*.parquet'",
			},
			shouldNotContain: []string{},
		},
		{
			name:     "JOIN with database.table converted",
			inputSQL: "SELECT * FROM mydb.cpu JOIN mydb.memory ON cpu.host = memory.host",
			shouldContain: []string{
				"read_parquet('./data/mydb/cpu/**/*.parquet'",
				"read_parquet('./data/mydb/memory/**/*.parquet'",
			},
			shouldNotContain: []string{},
		},
		{
			name:     "recursive CTE not converted",
			inputSQL: "WITH RECURSIVE tree AS (SELECT * FROM mydb.nodes WHERE parent IS NULL UNION ALL SELECT * FROM mydb.nodes n JOIN tree t ON n.parent = t.id) SELECT * FROM tree",
			shouldContain: []string{
				"read_parquet('./data/mydb/nodes/**/*.parquet'",
				"FROM tree", // Final SELECT FROM tree should be preserved
			},
			shouldNotContain: []string{
				"read_parquet('./data/default/tree/",
			},
		},
		// String literal protection tests
		{
			name:     "string literal containing FROM not converted",
			inputSQL: "SELECT * FROM mydb.logs WHERE message = 'SELECT * FROM mydb.cpu'",
			shouldContain: []string{
				"read_parquet('./data/mydb/logs/**/*.parquet'", // logs table converted
				"'SELECT * FROM mydb.cpu'",                      // string literal preserved
			},
			shouldNotContain: []string{
				"read_parquet('./data/mydb/cpu/", // cpu inside string should NOT be converted
			},
		},
		{
			name:     "string literal with LIKE pattern",
			inputSQL: "SELECT * FROM mydb.logs WHERE error LIKE '%FROM mydb.%'",
			shouldContain: []string{
				"read_parquet('./data/mydb/logs/**/*.parquet'",
				"'%FROM mydb.%'", // LIKE pattern preserved
			},
			shouldNotContain: []string{},
		},
		{
			name:     "multiple string literals",
			inputSQL: "SELECT * FROM mydb.data WHERE x = 'FROM mydb.a' AND y = 'FROM mydb.b'",
			shouldContain: []string{
				"read_parquet('./data/mydb/data/**/*.parquet'",
				"'FROM mydb.a'",
				"'FROM mydb.b'",
			},
			shouldNotContain: []string{
				"read_parquet('./data/mydb/a/",
				"read_parquet('./data/mydb/b/",
			},
		},
		// Comment protection tests
		{
			name:     "single line comment not converted",
			inputSQL: "SELECT * FROM mydb.cpu -- this references mydb.memory\nWHERE time > 1234",
			shouldContain: []string{
				"read_parquet('./data/mydb/cpu/**/*.parquet'",
			},
			shouldNotContain: []string{
				"read_parquet('./data/mydb/memory/", // comment content should not be converted
			},
		},
		{
			name:     "multi-line comment not converted",
			inputSQL: "SELECT * FROM mydb.cpu /* also query mydb.memory */ WHERE time > 1234",
			shouldContain: []string{
				"read_parquet('./data/mydb/cpu/**/*.parquet'",
			},
			shouldNotContain: []string{
				"read_parquet('./data/mydb/memory/", // comment content should not be converted
			},
		},
		// Combined: string with comment-like content
		{
			name:     "string literal with comment-like content",
			inputSQL: "SELECT * FROM mydb.logs WHERE msg = 'use -- for comments'",
			shouldContain: []string{
				"read_parquet('./data/mydb/logs/**/*.parquet'",
				"'use -- for comments'", // string preserved even with -- inside
			},
			shouldNotContain: []string{},
		},
		// LATERAL JOIN support
		{
			name:     "LATERAL JOIN with database.table",
			inputSQL: "SELECT * FROM mydb.events e LATERAL JOIN mydb.stats s ON s.event_id = e.id",
			shouldContain: []string{
				"read_parquet('./data/mydb/events/**/*.parquet'",
				"read_parquet('./data/mydb/stats/**/*.parquet'",
			},
			shouldNotContain: []string{},
		},
		{
			name:     "CROSS JOIN LATERAL",
			inputSQL: "SELECT * FROM mydb.events CROSS JOIN LATERAL mydb.related",
			shouldContain: []string{
				"read_parquet('./data/mydb/events/**/*.parquet'",
				"read_parquet('./data/mydb/related/**/*.parquet'",
			},
			shouldNotContain: []string{},
		},
		// Deep subquery
		{
			name:     "nested subqueries",
			inputSQL: "SELECT * FROM (SELECT * FROM (SELECT * FROM mydb.cpu) t1) t2",
			shouldContain: []string{
				"read_parquet('./data/mydb/cpu/**/*.parquet'",
			},
			shouldNotContain: []string{},
		},
		// UNION queries
		{
			name:     "UNION of two tables",
			inputSQL: "SELECT * FROM mydb.cpu UNION ALL SELECT * FROM mydb.memory",
			shouldContain: []string{
				"read_parquet('./data/mydb/cpu/**/*.parquet'",
				"read_parquet('./data/mydb/memory/**/*.parquet'",
			},
			shouldNotContain: []string{},
		},
		// Table alias with AS
		{
			name:     "table with AS alias",
			inputSQL: "SELECT c.host FROM mydb.cpu AS c WHERE c.value > 50",
			shouldContain: []string{
				"read_parquet('./data/mydb/cpu/**/*.parquet'",
			},
			shouldNotContain: []string{},
		},
		// JSON string containing table reference
		{
			name:     "JSON string with table reference",
			inputSQL: `SELECT * FROM mydb.data WHERE json_col = '{"table": "mydb.cpu"}'`,
			shouldContain: []string{
				"read_parquet('./data/mydb/data/**/*.parquet'",
				`'{"table": "mydb.cpu"}'`, // JSON string preserved
			},
			shouldNotContain: []string{
				"read_parquet('./data/mydb/cpu/", // should not convert inside JSON
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := h.convertSQLToStoragePaths(tt.inputSQL)

			for _, substr := range tt.shouldContain {
				if !strings.Contains(result, substr) {
					t.Errorf("Result should contain %q but doesn't.\nInput: %s\nResult: %s",
						substr, tt.inputSQL, result)
				}
			}

			for _, substr := range tt.shouldNotContain {
				if strings.Contains(result, substr) {
					t.Errorf("Result should NOT contain %q but does.\nInput: %s\nResult: %s",
						substr, tt.inputSQL, result)
				}
			}
		})
	}
}

func TestJoinClausePatterns(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		shouldMatch bool
		database    string
		table       string
	}{
		{name: "basic JOIN", sql: "SELECT * FROM a JOIN mydb.cpu ON 1=1", shouldMatch: true, database: "mydb", table: "cpu"},
		{name: "LEFT JOIN", sql: "SELECT * FROM a LEFT JOIN mydb.cpu ON 1=1", shouldMatch: true, database: "mydb", table: "cpu"},
		{name: "RIGHT JOIN", sql: "SELECT * FROM a RIGHT JOIN mydb.cpu ON 1=1", shouldMatch: true, database: "mydb", table: "cpu"},
		{name: "INNER JOIN", sql: "SELECT * FROM a INNER JOIN mydb.cpu ON 1=1", shouldMatch: true, database: "mydb", table: "cpu"},
		{name: "LATERAL JOIN", sql: "SELECT * FROM a LATERAL JOIN mydb.cpu ON 1=1", shouldMatch: true, database: "mydb", table: "cpu"},
		{name: "no JOIN", sql: "SELECT * FROM mydb.cpu", shouldMatch: false, database: "", table: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Using patternJoinDBTable which captures: database, table
			matches := patternJoinDBTable.FindStringSubmatch(tt.sql)
			matched := matches != nil
			if matched != tt.shouldMatch {
				t.Errorf("patternJoinDBTable for %q: matched=%v, want %v", tt.sql, matched, tt.shouldMatch)
			}
			if matched && tt.shouldMatch {
				if len(matches) < 3 {
					t.Errorf("expected at least 3 groups, got %d", len(matches))
				} else {
					if matches[1] != tt.database {
						t.Errorf("database = %q, want %q", matches[1], tt.database)
					}
					if matches[2] != tt.table {
						t.Errorf("table = %q, want %q", matches[2], tt.table)
					}
				}
			}
		})
	}
}

func TestValidateIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// Valid identifiers
		{name: "simple name", input: "mydb", wantErr: false},
		{name: "with underscore", input: "my_database", wantErr: false},
		{name: "with hyphen", input: "my-database", wantErr: false},
		{name: "with numbers", input: "db123", wantErr: false},
		{name: "underscore prefix", input: "_private", wantErr: false},
		{name: "mixed", input: "my_db-123", wantErr: false},

		// Invalid identifiers
		{name: "empty", input: "", wantErr: true},
		{name: "starts with number", input: "123db", wantErr: true},
		{name: "contains space", input: "my db", wantErr: true},
		{name: "contains semicolon", input: "db;DROP", wantErr: true},
		{name: "contains quote", input: "db'name", wantErr: true},
		{name: "contains double quote", input: `db"name`, wantErr: true},
		{name: "contains dot", input: "db.name", wantErr: true},
		{name: "contains slash", input: "db/name", wantErr: true},
		{name: "contains backslash", input: `db\name`, wantErr: true},
		{name: "sql injection attempt", input: "db; DROP TABLE users--", wantErr: true},
		{name: "too long", input: string(make([]byte, 200)), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateIdentifier(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateIdentifier(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestValidateOrderByClause(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// Valid ORDER BY clauses
		{name: "simple column", input: "time", wantErr: false},
		{name: "with ASC", input: "time ASC", wantErr: false},
		{name: "with DESC", input: "time DESC", wantErr: false},
		{name: "lowercase desc", input: "time desc", wantErr: false},
		{name: "multiple columns", input: "time DESC, host ASC", wantErr: false},
		{name: "multiple columns no space", input: "time DESC,host ASC", wantErr: false},
		{name: "underscore column", input: "created_at DESC", wantErr: false},

		// Invalid ORDER BY clauses
		{name: "empty", input: "", wantErr: true},
		{name: "contains semicolon", input: "time; DROP TABLE", wantErr: true},
		{name: "contains comment", input: "time--comment", wantErr: true},
		{name: "contains quote", input: "time'", wantErr: true},
		{name: "starts with number", input: "123time", wantErr: true},
		{name: "contains function call", input: "UPPER(time)", wantErr: true},
		{name: "sql injection", input: "time DESC; DROP TABLE users--", wantErr: true},
		{name: "too long", input: string(make([]byte, 300)), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOrderByClause(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateOrderByClause(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestValidateWhereClauseQuery(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// Valid WHERE clauses
		{name: "simple equality", input: "host = 'server01'", wantErr: false},
		{name: "numeric comparison", input: "value > 100", wantErr: false},
		{name: "time range", input: "time >= 1609459200000000 AND time < 1609545600000000", wantErr: false},
		{name: "IN clause", input: "host IN ('server01', 'server02')", wantErr: false},
		{name: "LIKE clause", input: "host LIKE 'server%'", wantErr: false},
		{name: "BETWEEN", input: "value BETWEEN 10 AND 100", wantErr: false},
		{name: "IS NULL", input: "field IS NULL", wantErr: false},
		{name: "complex condition", input: "(host = 'server01' OR host = 'server02') AND value > 50", wantErr: false},

		// Invalid WHERE clauses - SQL injection attempts
		{name: "semicolon", input: "host = 'server01'; DROP TABLE users", wantErr: true},
		{name: "comment dash", input: "host = 'server01'--comment", wantErr: true},
		{name: "comment block start", input: "host = 'server01'/*", wantErr: true},
		{name: "comment block end", input: "*/host = 'server01'", wantErr: true},
		{name: "DROP keyword", input: "host = 'DROP'", wantErr: true},
		{name: "DELETE keyword", input: "DELETE FROM users", wantErr: true},
		{name: "INSERT keyword", input: "INSERT INTO users", wantErr: true},
		{name: "UPDATE keyword", input: "UPDATE users SET", wantErr: true},
		{name: "TRUNCATE keyword", input: "TRUNCATE TABLE", wantErr: true},
		{name: "UNION keyword", input: "1=1 UNION SELECT * FROM users", wantErr: true},
		{name: "EXEC keyword", input: "EXEC xp_cmdshell", wantErr: true},

		// Invalid - unmatched quotes
		{name: "unmatched single quote", input: "host = 'server01", wantErr: true},
		{name: "unmatched double quote", input: `host = "server01`, wantErr: true},
		{name: "unmatched parentheses", input: "(host = 'server01'", wantErr: true},

		// Too long
		{name: "too long", input: string(make([]byte, 5000)), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWhereClauseQuery(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateWhereClauseQuery(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestDangerousSQLPatterns(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		shouldFail bool
	}{
		// Safe queries
		{name: "simple select", sql: "SELECT * FROM cpu", shouldFail: false},
		{name: "select with where", sql: "SELECT * FROM cpu WHERE host = 'server01'", shouldFail: false},
		{name: "select with aggregation", sql: "SELECT AVG(value) FROM cpu GROUP BY host", shouldFail: false},
		{name: "select with join", sql: "SELECT * FROM cpu JOIN memory ON cpu.host = memory.host", shouldFail: false},
		{name: "subquery", sql: "SELECT * FROM (SELECT * FROM cpu) t", shouldFail: false},

		// Dangerous queries
		{name: "DROP TABLE", sql: "DROP TABLE users", shouldFail: true},
		{name: "DROP DATABASE", sql: "DROP DATABASE mydb", shouldFail: true},
		{name: "DELETE FROM", sql: "DELETE FROM users", shouldFail: true},
		{name: "TRUNCATE TABLE", sql: "TRUNCATE TABLE users", shouldFail: true},
		{name: "ALTER TABLE", sql: "ALTER TABLE users ADD column", shouldFail: true},
		{name: "CREATE TABLE", sql: "CREATE TABLE new_table (id INT)", shouldFail: true},
		{name: "INSERT INTO", sql: "INSERT INTO users VALUES (1)", shouldFail: true},
		{name: "UPDATE SET", sql: "UPDATE users SET name = 'hacked'", shouldFail: true},

		// Case variations
		{name: "drop table lowercase", sql: "drop table users", shouldFail: true},
		{name: "DROP TABLE mixed case", sql: "DrOp TaBlE users", shouldFail: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched := false
			for _, pattern := range dangerousSQLPatterns {
				if pattern.MatchString(tt.sql) {
					matched = true
					break
				}
			}
			if matched != tt.shouldFail {
				t.Errorf("dangerousSQLPatterns for %q: matched=%v, shouldFail=%v", tt.sql, matched, tt.shouldFail)
			}
		})
	}
}

func TestShowDatabasesPattern(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		shouldMatch bool
	}{
		{name: "basic", sql: "SHOW DATABASES", shouldMatch: true},
		{name: "lowercase", sql: "show databases", shouldMatch: true},
		{name: "mixed case", sql: "Show Databases", shouldMatch: true},
		{name: "with semicolon", sql: "SHOW DATABASES;", shouldMatch: true},
		{name: "with trailing space", sql: "SHOW DATABASES ", shouldMatch: true},
		{name: "with leading space", sql: " SHOW DATABASES", shouldMatch: true},

		{name: "not show databases", sql: "SELECT * FROM databases", shouldMatch: false},
		{name: "show tables", sql: "SHOW TABLES", shouldMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched := showDatabasesPattern.MatchString(tt.sql)
			if matched != tt.shouldMatch {
				t.Errorf("showDatabasesPattern.MatchString(%q) = %v, want %v", tt.sql, matched, tt.shouldMatch)
			}
		})
	}
}

func TestShowTablesPattern(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		shouldMatch bool
		database  string
	}{
		{name: "basic tables", sql: "SHOW TABLES", shouldMatch: true, database: ""},
		{name: "basic measurements", sql: "SHOW MEASUREMENTS", shouldMatch: true, database: ""},
		{name: "tables from db", sql: "SHOW TABLES FROM mydb", shouldMatch: true, database: "mydb"},
		{name: "measurements from db", sql: "SHOW MEASUREMENTS FROM mydb", shouldMatch: true, database: "mydb"},
		{name: "lowercase", sql: "show tables from mydb", shouldMatch: true, database: "mydb"},
		{name: "with semicolon", sql: "SHOW TABLES;", shouldMatch: true, database: ""},

		{name: "select query", sql: "SELECT * FROM tables", shouldMatch: false, database: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := showTablesPattern.FindStringSubmatch(tt.sql)
			matched := matches != nil
			if matched != tt.shouldMatch {
				t.Errorf("showTablesPattern.MatchString(%q) = %v, want %v", tt.sql, matched, tt.shouldMatch)
			}
			if matched && tt.database != "" {
				if len(matches) < 2 || matches[1] != tt.database {
					extractedDB := ""
					if len(matches) > 1 {
						extractedDB = matches[1]
					}
					t.Errorf("extracted database = %q, want %q", extractedDB, tt.database)
				}
			}
		})
	}
}

func TestDBTablePattern(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		shouldMatch bool
		database    string
		table       string
	}{
		{name: "basic", sql: "SELECT * FROM mydb.cpu", shouldMatch: true, database: "mydb", table: "cpu"},
		{name: "with where", sql: "SELECT * FROM mydb.cpu WHERE host='a'", shouldMatch: true, database: "mydb", table: "cpu"},
		{name: "underscore names", sql: "SELECT * FROM my_db.cpu_usage", shouldMatch: true, database: "my_db", table: "cpu_usage"},
		{name: "numbers in names", sql: "SELECT * FROM db1.table2", shouldMatch: true, database: "db1", table: "table2"},

		{name: "no database", sql: "SELECT * FROM cpu", shouldMatch: false, database: "", table: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// patternDBTable captures: database (group 1), table (group 2)
			matches := patternDBTable.FindStringSubmatch(tt.sql)
			matched := matches != nil
			if matched != tt.shouldMatch {
				t.Errorf("patternDBTable for %q: matched=%v, want %v", tt.sql, matched, tt.shouldMatch)
			}
			if matched && tt.shouldMatch {
				if len(matches) < 3 {
					t.Errorf("expected at least 3 groups (full match + 2 captures), got %d", len(matches))
				} else {
					if matches[1] != tt.database {
						t.Errorf("database = %q, want %q", matches[1], tt.database)
					}
					if matches[2] != tt.table {
						t.Errorf("table = %q, want %q", matches[2], tt.table)
					}
				}
			}
		})
	}
}

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{12, "12"},
		{123, "123"},
		{1234, "1,234"},
		{12345, "12,345"},
		{123456, "123,456"},
		{1234567, "1,234,567"},
		{1000000, "1,000,000"},
		{-1234, "-1,234"},
		{-1000000, "-1,000,000"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatNumber(tt.input)
			if result != tt.expected {
				t.Errorf("formatNumber(%d) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestValidIdentifierPattern(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		shouldMatch bool
	}{
		// Valid
		{name: "simple", input: "mydb", shouldMatch: true},
		{name: "underscore start", input: "_private", shouldMatch: true},
		{name: "with numbers", input: "db123", shouldMatch: true},
		{name: "with underscore", input: "my_db", shouldMatch: true},
		{name: "with hyphen", input: "my-db", shouldMatch: true},
		{name: "mixed", input: "my_db-123", shouldMatch: true},

		// Invalid
		{name: "starts with number", input: "123db", shouldMatch: false},
		{name: "starts with hyphen", input: "-db", shouldMatch: false},
		{name: "contains space", input: "my db", shouldMatch: false},
		{name: "contains dot", input: "my.db", shouldMatch: false},
		{name: "contains quote", input: "my'db", shouldMatch: false},
		{name: "contains semicolon", input: "my;db", shouldMatch: false},
		{name: "empty", input: "", shouldMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched := validIdentifierPattern.MatchString(tt.input)
			if matched != tt.shouldMatch {
				t.Errorf("validIdentifierPattern.MatchString(%q) = %v, want %v", tt.input, matched, tt.shouldMatch)
			}
		})
	}
}

// Benchmark tests
func BenchmarkValidateIdentifier(b *testing.B) {
	for i := 0; i < b.N; i++ {
		validateIdentifier("my_database_name")
	}
}

func BenchmarkValidateWhereClause(b *testing.B) {
	clause := "host = 'server01' AND time >= 1609459200000000 AND value > 50"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		validateWhereClauseQuery(clause)
	}
}

func BenchmarkDangerousSQLCheck(b *testing.B) {
	sql := "SELECT AVG(value), host FROM mydb.cpu WHERE time > 1609459200000000 GROUP BY host ORDER BY time DESC LIMIT 100"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, pattern := range dangerousSQLPatterns {
			pattern.MatchString(sql)
		}
	}
}

// BenchmarkConvertSQLToStoragePaths benchmarks the SQL-to-storage-path conversion
// This is used to measure the baseline before adding CTE support
func BenchmarkConvertSQLToStoragePaths(b *testing.B) {
	// Create a minimal QueryHandler for benchmarking
	h := &QueryHandler{
		storage: &mockLocalBackend{basePath: "./data"},
		pruner:  pruning.NewPartitionPruner(zerolog.Nop()),
		logger:  zerolog.Nop(),
	}

	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "simple_select",
			sql:  "SELECT * FROM mydb.cpu WHERE time > 1609459200000000",
		},
		{
			name: "with_join",
			sql:  "SELECT * FROM mydb.cpu JOIN mydb.memory ON cpu.host = memory.host WHERE time > 1609459200000000",
		},
		{
			name: "with_single_cte",
			sql:  "WITH campaign AS (SELECT * FROM mydb.events WHERE type = 'campaign') SELECT * FROM campaign WHERE time > 1609459200000000",
		},
		{
			name: "with_multiple_ctes",
			sql:  "WITH cte1 AS (SELECT * FROM mydb.events), cte2 AS (SELECT * FROM mydb.stats) SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.event_id",
		},
		{
			name: "complex_query",
			sql:  "WITH hourly_avg AS (SELECT host, AVG(value) as avg_val FROM mydb.cpu WHERE time BETWEEN '2024-01-01' AND '2024-01-02' GROUP BY host) SELECT * FROM hourly_avg WHERE avg_val > 50 ORDER BY avg_val DESC",
		},
		// Benchmark without string literals (fast path)
		{
			name: "no_string_literals",
			sql:  "SELECT host, AVG(value) as avg_val FROM mydb.cpu WHERE time > 1704067200000000 AND time < 1704153600000000 GROUP BY host ORDER BY avg_val DESC LIMIT 100",
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.convertSQLToStoragePaths(tc.sql)
			}
		})
	}
}

// BenchmarkGetTransformedSQL benchmarks the cached SQL transformation
func BenchmarkGetTransformedSQL(b *testing.B) {
	// Create a minimal QueryHandler for benchmarking
	h := &QueryHandler{
		storage:    &mockLocalBackend{basePath: "./data"},
		pruner:     pruning.NewPartitionPruner(zerolog.Nop()),
		queryCache: database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize),
		logger:     zerolog.Nop(),
	}

	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "simple_select",
			sql:  "SELECT * FROM mydb.cpu WHERE time > 1609459200000000",
		},
		{
			name: "with_join",
			sql:  "SELECT * FROM mydb.cpu JOIN mydb.memory ON cpu.host = memory.host WHERE time > 1609459200000000",
		},
		{
			name: "complex_query",
			sql:  "WITH hourly_avg AS (SELECT host, AVG(value) as avg_val FROM mydb.cpu WHERE time BETWEEN '2024-01-01' AND '2024-01-02' GROUP BY host) SELECT * FROM hourly_avg WHERE avg_val > 50 ORDER BY avg_val DESC",
		},
	}

	for _, tc := range testCases {
		// Benchmark cache miss (first call)
		b.Run(tc.name+"_cache_miss", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.queryCache.Invalidate() // Force cache miss
				h.getTransformedSQL(tc.sql, "")
			}
		})

		// Benchmark cache hit (pre-populated)
		b.Run(tc.name+"_cache_hit", func(b *testing.B) {
			h.getTransformedSQL(tc.sql, "") // Pre-populate cache
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.getTransformedSQL(tc.sql, "")
			}
		})
	}
}

// mockLocalBackend is a minimal mock for benchmarking that implements storage.Backend
type mockLocalBackend struct {
	basePath string
}

func (m *mockLocalBackend) GetBasePath() string                                            { return m.basePath }
func (m *mockLocalBackend) Write(ctx context.Context, path string, data []byte) error      { return nil }
func (m *mockLocalBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	return nil
}
func (m *mockLocalBackend) Read(ctx context.Context, path string) ([]byte, error)          { return nil, nil }
func (m *mockLocalBackend) ReadTo(ctx context.Context, path string, w io.Writer) error     { return nil }
func (m *mockLocalBackend) List(ctx context.Context, prefix string) ([]string, error)      { return nil, nil }
func (m *mockLocalBackend) Delete(ctx context.Context, path string) error                  { return nil }
func (m *mockLocalBackend) Exists(ctx context.Context, path string) (bool, error)          { return false, nil }
func (m *mockLocalBackend) Close() error                                                   { return nil }
func (m *mockLocalBackend) Type() string                                                   { return "mock" }
func (m *mockLocalBackend) ConfigJSON() string                                             { return "{}" }

func TestRewriteTimeBucket(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// All intervals now use epoch-based arithmetic for 2.5x faster performance
		{
			name:     "1 hour with INTERVAL keyword",
			input:    "SELECT time_bucket(INTERVAL '1 hour', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 3600) * 3600) AS bucket FROM cpu",
		},
		{
			name:     "1 hour without INTERVAL keyword",
			input:    "SELECT time_bucket('1 hour', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 3600) * 3600) AS bucket FROM cpu",
		},
		{
			name:     "1 day",
			input:    "SELECT time_bucket('1 day', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 86400) * 86400) AS bucket FROM cpu",
		},
		{
			name:     "1 minute",
			input:    "SELECT time_bucket('1 minute', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 60) * 60) AS bucket FROM cpu",
		},
		{
			name:     "1 week",
			input:    "SELECT time_bucket('1 week', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 604800) * 604800) AS bucket FROM cpu",
		},
		{
			name:     "1 second",
			input:    "SELECT time_bucket('1 second', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 1) * 1) AS bucket FROM cpu",
		},
		// Custom intervals -> epoch arithmetic
		{
			name:     "30 minutes",
			input:    "SELECT time_bucket('30 minutes', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 1800) * 1800) AS bucket FROM cpu",
		},
		{
			name:     "15 minutes",
			input:    "SELECT time_bucket('15 minutes', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 900) * 900) AS bucket FROM cpu",
		},
		{
			name:     "4 hours",
			input:    "SELECT time_bucket('4 hours', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 14400) * 14400) AS bucket FROM cpu",
		},
		{
			name:     "2 days",
			input:    "SELECT time_bucket('2 days', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 172800) * 172800) AS bucket FROM cpu",
		},
		// Months should NOT be rewritten (variable length)
		{
			name:     "1 month stays unchanged",
			input:    "SELECT time_bucket('1 month', time) AS bucket FROM cpu",
			expected: "SELECT time_bucket('1 month', time) AS bucket FROM cpu",
		},
		{
			name:     "2 months stays unchanged",
			input:    "SELECT time_bucket('2 months', time) AS bucket FROM cpu",
			expected: "SELECT time_bucket('2 months', time) AS bucket FROM cpu",
		},
		// Case insensitivity
		{
			name:     "case insensitive TIME_BUCKET",
			input:    "SELECT TIME_BUCKET('1 hour', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 3600) * 3600) AS bucket FROM cpu",
		},
		{
			name:     "case insensitive Time_Bucket",
			input:    "SELECT Time_Bucket('1 HOUR', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 3600) * 3600) AS bucket FROM cpu",
		},
		// Multiple time_bucket calls
		{
			name:     "multiple time_bucket calls",
			input:    "SELECT time_bucket('1 hour', time) AS h, time_bucket('30 minutes', time) AS m FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 3600) * 3600) AS h, to_timestamp((epoch(time)::BIGINT / 1800) * 1800) AS m FROM cpu",
		},
		// Column with expression
		{
			name:     "column expression",
			input:    "SELECT time_bucket('1 hour', created_at) AS bucket FROM events",
			expected: "SELECT to_timestamp((epoch(created_at)::BIGINT / 3600) * 3600) AS bucket FROM events",
		},
		// No time_bucket - unchanged
		{
			name:     "no time_bucket unchanged",
			input:    "SELECT * FROM cpu WHERE time > NOW() - INTERVAL '1 hour'",
			expected: "SELECT * FROM cpu WHERE time > NOW() - INTERVAL '1 hour'",
		},
		// Plural forms
		{
			name:     "singular hour",
			input:    "SELECT time_bucket('1 hours', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 3600) * 3600) AS bucket FROM cpu",
		},
		{
			name:     "singular day",
			input:    "SELECT time_bucket('1 days', time) AS bucket FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 86400) * 86400) AS bucket FROM cpu",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rewriteTimeBucket(tt.input)
			if result != tt.expected {
				t.Errorf("rewriteTimeBucket(%q)\n  got:      %q\n  expected: %q",
					tt.input, result, tt.expected)
			}
		})
	}
}

func TestRewriteTimeBucketWithOrigin(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string // Check if output contains this (since epoch values vary)
	}{
		{
			name:     "3-arg with timestamp origin",
			input:    "SELECT time_bucket('1 hour', time, '2024-01-01 00:30:00') AS bucket FROM cpu",
			contains: "to_timestamp(",
		},
		{
			name:     "3-arg with TIMESTAMP keyword",
			input:    "SELECT time_bucket('1 hour', time, TIMESTAMP '2024-01-01 00:30:00') AS bucket FROM cpu",
			contains: "to_timestamp(",
		},
		{
			name:     "3-arg with date only origin",
			input:    "SELECT time_bucket('30 minutes', time, '2024-01-01') AS bucket FROM cpu",
			contains: "to_timestamp(",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rewriteTimeBucket(tt.input)
			if !strings.Contains(result, tt.contains) {
				t.Errorf("rewriteTimeBucket(%q)\n  result: %q\n  should contain: %q",
					tt.input, result, tt.contains)
			}
			// Should NOT contain time_bucket anymore
			if strings.Contains(strings.ToLower(result), "time_bucket") {
				t.Errorf("rewriteTimeBucket(%q)\n  result still contains time_bucket: %q",
					tt.input, result)
			}
		})
	}
}

func TestIntervalToSeconds(t *testing.T) {
	tests := []struct {
		amount   string
		unit     string
		expected int
	}{
		{"1", "second", 1},
		{"30", "second", 30},
		{"1", "minute", 60},
		{"30", "minute", 1800},
		{"1", "hour", 3600},
		{"4", "hour", 14400},
		{"1", "day", 86400},
		{"2", "day", 172800},
		{"1", "week", 604800},
		{"1", "month", 0}, // months return 0 (variable length)
		{"invalid", "hour", 0},
	}

	for _, tt := range tests {
		name := tt.amount + " " + tt.unit
		t.Run(name, func(t *testing.T) {
			result := intervalToSeconds(tt.amount, tt.unit)
			if result != tt.expected {
				t.Errorf("intervalToSeconds(%q, %q) = %d, expected %d",
					tt.amount, tt.unit, result, tt.expected)
			}
		})
	}
}

func TestParseTimeBucketOrigin(t *testing.T) {
	tests := []struct {
		input   string
		wantErr bool
	}{
		{"2024-01-01 00:30:00", false},
		{"2024-01-01T00:30:00", false},
		{"2024-01-01 00:30:00Z", false},
		{"2024-01-01T00:30:00Z", false},
		{"2024-01-01", false},
		{"invalid", true},
		{"2024/01/01", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := parseTimeBucketOrigin(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTimeBucketOrigin(%q) error = %v, wantErr %v",
					tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestRewriteDateTrunc(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Standard intervals converted to epoch arithmetic
		{
			name:     "date_trunc hour",
			input:    "SELECT date_trunc('hour', time) AS h FROM cpu GROUP BY 1",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 3600) * 3600) AS h FROM cpu GROUP BY 1",
		},
		{
			name:     "date_trunc day",
			input:    "SELECT date_trunc('day', time) AS d FROM cpu GROUP BY 1",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 86400) * 86400) AS d FROM cpu GROUP BY 1",
		},
		{
			name:     "date_trunc minute",
			input:    "SELECT date_trunc('minute', time) AS m FROM cpu GROUP BY 1",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 60) * 60) AS m FROM cpu GROUP BY 1",
		},
		{
			name:     "date_trunc second",
			input:    "SELECT date_trunc('second', time) AS s FROM cpu GROUP BY 1",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 1) * 1) AS s FROM cpu GROUP BY 1",
		},
		{
			name:     "date_trunc week",
			input:    "SELECT date_trunc('week', time) AS w FROM cpu GROUP BY 1",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 604800) * 604800) AS w FROM cpu GROUP BY 1",
		},
		// Month should NOT be rewritten (variable length)
		{
			name:     "date_trunc month unchanged",
			input:    "SELECT date_trunc('month', time) AS m FROM cpu GROUP BY 1",
			expected: "SELECT date_trunc('month', time) AS m FROM cpu GROUP BY 1",
		},
		// Case insensitivity
		{
			name:     "case insensitive DATE_TRUNC",
			input:    "SELECT DATE_TRUNC('hour', time) AS h FROM cpu GROUP BY 1",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 3600) * 3600) AS h FROM cpu GROUP BY 1",
		},
		{
			name:     "case insensitive Date_Trunc",
			input:    "SELECT Date_Trunc('DAY', time) AS d FROM cpu GROUP BY 1",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 86400) * 86400) AS d FROM cpu GROUP BY 1",
		},
		// Multiple date_trunc calls
		{
			name:     "multiple date_trunc calls",
			input:    "SELECT date_trunc('hour', time) AS h, date_trunc('day', time) AS d FROM cpu",
			expected: "SELECT to_timestamp((epoch(time)::BIGINT / 3600) * 3600) AS h, to_timestamp((epoch(time)::BIGINT / 86400) * 86400) AS d FROM cpu",
		},
		// Different column names
		{
			name:     "different column name",
			input:    "SELECT date_trunc('hour', created_at) AS h FROM events GROUP BY 1",
			expected: "SELECT to_timestamp((epoch(created_at)::BIGINT / 3600) * 3600) AS h FROM events GROUP BY 1",
		},
		// No date_trunc - unchanged
		{
			name:     "no date_trunc unchanged",
			input:    "SELECT * FROM cpu WHERE time > NOW() - INTERVAL '1 hour'",
			expected: "SELECT * FROM cpu WHERE time > NOW() - INTERVAL '1 hour'",
		},
		// date_trunc in WHERE clause
		{
			name:     "date_trunc in WHERE clause",
			input:    "SELECT * FROM cpu WHERE date_trunc('day', time) = '2024-01-01'",
			expected: "SELECT * FROM cpu WHERE to_timestamp((epoch(time)::BIGINT / 86400) * 86400) = '2024-01-01'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rewriteDateTrunc(tt.input)
			if result != tt.expected {
				t.Errorf("rewriteDateTrunc(%q)\n  got:      %q\n  expected: %q",
					tt.input, result, tt.expected)
			}
		})
	}
}
