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
