package api

import "testing"

// The query API runs read statements only: anything that changes the
// catalog, the session or the database is refused before it reaches DuckDB,
// including statement types the keyword denylist never named.
func TestValidateSQLRequest_ReadStatementsOnly(t *testing.T) {
	refused := []string{
		"CREATE MACRO m(x) AS 1",
		"CREATE OR REPLACE MACRO m(x) AS 1",
		"CREATE TEMP MACRO m(x) AS 1",
		"create temporary macro m(x) as table select 1",
		"CREATE FUNCTION f(x) AS x",
		"CREATE VIEW v AS SELECT 1",
		"CREATE OR REPLACE VIEW v AS SELECT 1",
		"CREATE OR REPLACE TABLE t AS SELECT 1",
		"CREATE TEMP TABLE t AS SELECT 1",
		"CREATE SCHEMA s",
		"CREATE SEQUENCE q",
		"CREATE TYPE e AS ENUM ('a')",
		"DROP MACRO m",
		"DROP FUNCTION f",
		"DROP SCHEMA main",
		"DROP SEQUENCE q",
		"ALTER VIEW v RENAME TO w",
		"INSERT OR REPLACE INTO t VALUES (1)",
		"USE memory",
		"CHECKPOINT",
		"FORCE CHECKPOINT",
		"VACUUM",
		"ANALYZE",
		"BEGIN TRANSACTION",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"ABORT",
		"DEALLOCATE p",
		"COMMENT ON TABLE t IS 'x'",
		"UPDATE t SET a = 1",
		"MERGE INTO t USING u ON t.a = u.a WHEN MATCHED THEN DELETE",
		"/* c */ CREATE MACRO m(x) AS 1",
		"-- c\nCREATE MACRO m(x) AS 1",
		"  \n\tCREATE MACRO m(x) AS 1",
		"(CREATE MACRO m(x) AS 1)",
		"EXPLAIN CREATE MACRO m(x) AS 1",
		"EXPLAIN ANALYZE CREATE TABLE t AS SELECT 1",
		"EXPLAIN ANALYZE CREATE MACRO m(x) AS 1",
		"CREATE MACRO m(x) AS 1;",
	}
	for _, q := range refused {
		if err := ValidateSQLRequest(q); err == nil {
			t.Errorf("accepted: %q", q)
		}
	}
	accepted := []string{
		"SELECT 1",
		"select * from db.cpu where time > now() - interval '1 hour'",
		"  SELECT 1;",
		"/* c */ SELECT 1",
		"-- c\nSELECT 1",
		"(SELECT 1) UNION ALL (SELECT 2)",
		"((SELECT 1))",
		"WITH a AS (SELECT 1 AS x) SELECT x FROM a",
		"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r",
		"FROM db.cpu LIMIT 1",
		"VALUES (1), (2)",
		"SHOW DATABASES",
		"SHOW TABLES FROM db",
		"DESCRIBE db.cpu",
		"DESCRIBE SELECT 1",
		"SUMMARIZE db.cpu",
		"EXPLAIN SELECT 1",
		"EXPLAIN ANALYZE SELECT 1",
		"explain analyze with a as (select 1) select * from a",
		"PIVOT db.cpu ON host USING avg(value)",
		"UNPIVOT db.cpu ON a, b INTO NAME k VALUE v",
		"SELECT created_at, updated FROM db.cpu",
	}
	for _, q := range accepted {
		if err := ValidateSQLRequest(q); err != nil {
			t.Errorf("refused %q: %v", q, err)
		}
	}
}

// Shapes that hide a statement from a first-keyword check, or a write behind
// a read keyword.
func TestValidateSQLRequest_ReadStatementsBypasses(t *testing.T) {
	refused := []string{
		"/* /* */ SELECT */ CREATE MACRO m(x) AS 20",
		"/* a /* b */ c */ SELECT 1",
		"SELECT 1 /* x /* y */",
		"WITH x AS (SELECT 1) INSERT\u00a0INTO t SELECT 5",
		"WITH x AS (SELECT 1) DELETE\u00a0FROM t",
		"CREATE\u00a0MACRO m(x) AS 1",
		"\ufeffCREATE MACRO m(x) AS 1",
		"\u2003CREATE MACRO m(x) AS 1",
		"WITH x AS (SELECT 1) INSERT OR REPLACE INTO t SELECT 1",
		"WITH x AS (SELECT 1) INSERT OR IGNORE INTO t SELECT 1",
		"WITH x AS (SELECT 1) MERGE INTO t USING x ON true WHEN NOT MATCHED THEN INSERT VALUES (1)",
		"WITH x AS (SELECT 1) UPDATE t SET a = 1",
		"WITH x AS (SELECT 1)",
		"WITH x AS (SELECT 1) CREATE TABLE t AS SELECT 1",
		"WITH x AS (SELECT 1) , y AS (SELECT 2) USE memory",
		"EXPLAIN (ANALYZE) CREATE MACRO m(x) AS 1",
		"EXPLAIN (ANALYZE, FORMAT json) CREATE MACRO m(x) AS 1",
		"EXPLAIN ANALYZE (FORMAT json) CREATE MACRO m(x) AS 1",
		"EXPLAIN EXPLAIN SELECT 1",
		"EXPLAIN (ANALYZE SELECT 1",
		"SELECT * FROM enable_logging(['QueryLog'])",
		"SELECT x FROM (SELECT 1 x) WHERE x IN (FROM enable_logging(['QueryLog']))",
		"SELECT * FROM duckdb_logs",
		"SELECT * FROM \"duckdb_logs\"",
		"SELECT * FROM duckdb_log_contexts()",
		"SELECT * FROM disable_logging()",
		"SELECT * FROM truncate_duckdb_logs()",
		"SELECT write_log('x')",
		"SELECT * FROM checkpoint()",
		"SELECT * FROM force_checkpoint()",
		"SELECT nextval('s')",
		"SELECT currval('s')",
		"SELECT setseed(0.5)",
		"SELECT * FROM duckdb_secrets()",
		"SELECT * FROM which_secret('s3://b/x', 's3')",
		"SELECT * FROM duckdb_settings()",
		"SELECT current_setting('allowed_directories')",
		"SELECT * FROM pg_settings",
		"SELECT * FROM duckdb_variables()",
		"SELECT getvariable('x')",
		"SELECT \"current_setting\"('threads')",
		"SELECT CURRENT_SETTING ('threads')",
	}
	for _, q := range refused {
		if err := ValidateSQLRequest(q); err == nil {
			t.Errorf("accepted: %q", q)
		}
	}
	accepted := []string{
		"EXPLAIN (FORMAT json) SELECT 1",
		"EXPLAIN ANALYZE (FORMAT json) SELECT 1",
		"WITH a AS MATERIALIZED (SELECT 1) SELECT * FROM a",
		"WITH a AS NOT MATERIALIZED (SELECT 1) SELECT * FROM a",
		"WITH a(x) AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b",
		"WITH a AS (SELECT 1) (SELECT * FROM a)",
		"WITH a AS (SELECT 1) FROM a",
		"WITH a AS (SELECT 1) VALUES (1)",
		"WITH RECURSIVE r(n) USING KEY (n) AS (SELECT 1 UNION SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r",
		"SELECT '/* not a comment /* */' AS a",
		"SELECT 1 /* one */ /* two */",
		"SELECT 'enable_logging(x)' AS s, 'duckdb_logs' AS t",
		"SELECT checkpoint_id, settings_name FROM db.cpu",
		"SELECT 'a\u00a0b' AS s",
	}
	for _, q := range accepted {
		if err := ValidateSQLRequest(q); err != nil {
			t.Errorf("refused %q: %v", q, err)
		}
	}
}
