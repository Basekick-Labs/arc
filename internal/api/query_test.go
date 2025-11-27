package api

import (
	"testing"
)

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
