package api

import (
	"context"
	"io"
	"strings"
	"testing"

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
		{name: "no JOIN", sql: "SELECT * FROM mydb.cpu", shouldMatch: false, database: "", table: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			matches := patternDBTable.FindStringSubmatch(tt.sql)
			matched := matches != nil
			if matched != tt.shouldMatch {
				t.Errorf("patternDBTable for %q: matched=%v, want %v", tt.sql, matched, tt.shouldMatch)
			}
			if matched {
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
