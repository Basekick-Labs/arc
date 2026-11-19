//go:build duckdb_arrow

package database

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func newDescribeTestDB(t *testing.T, maxConnections int) *DuckDB {
	t.Helper()
	db, err := New(&Config{
		MaxConnections:   maxConnections,
		MemoryLimit:      "256MB",
		ThreadCount:      2,
		LocalStorageRoot: t.TempDir(),
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// arrowQuerySchema is the schema ArrowQueryContext streams for query.
func arrowQuerySchema(t *testing.T, db *DuckDB, query string) string {
	t.Helper()
	reader, conn, err := db.ArrowQueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("ArrowQueryContext(%q): %v", query, err)
	}
	defer conn.Close()
	defer reader.Release()
	return reader.Schema().String()
}

// The described schema is the one the query's result has, across the types a
// result can hold. The string form compares names, order, types, nullability
// and field metadata (JSON's extension name among them).
func TestDescribeArrowSchemaMatchesTheQueryResult(t *testing.T) {
	db := newDescribeTestDB(t, 2)
	for _, tc := range []struct{ name, query string }{
		{"integers", `SELECT 1::TINYINT AS a, 1::SMALLINT AS b, 1::INTEGER AS c, 1::BIGINT AS d, 1::HUGEINT AS e,
			1::UTINYINT AS f, 1::USMALLINT AS g, 1::UINTEGER AS h, 1::UBIGINT AS i, 1::UHUGEINT AS j`},
		{"floats and decimals", `SELECT 1.5::FLOAT AS a, 1.5::DOUBLE AS b, 1.5::DECIMAL(4,1) AS c,
			1::DECIMAL(38,0) AS d, 1::DECIMAL(18,3) AS e, sum(1) AS f`},
		{"text and binary", `SELECT 'x' AS a, 'x'::BLOB AS b, '{"a":1}'::JSON AS c, uuid() AS d, '101'::BIT AS e, true AS f`},
		{"times", `SELECT TIMESTAMP '2024-01-01' AS a, TIMESTAMPTZ '2024-01-01 00:00:00+00' AS b, DATE '2024-01-01' AS c,
			TIME '12:00:00' AS d, INTERVAL 1 DAY AS e, '2024-01-01'::TIMESTAMP_S AS f, '2024-01-01'::TIMESTAMP_MS AS g,
			'2024-01-01'::TIMESTAMP_NS AS h, '12:00:00+01'::TIMETZ AS i`},
		{"nested", `SELECT [1, 2] AS a, ['x'] AS b, {'k': 1, 'v': [1.5]} AS c, MAP {'a': 1} AS d,
			[1, 2, 3]::INTEGER[3] AS e, [{'x': 1::DECIMAL(10,2)}] AS f, {'j': '{}'::JSON} AS g`},
		{"enum", `SELECT 'b'::ENUM('a', 'b', 'it''s') AS e`},
		{"null", `SELECT NULL AS n, NULL::INTEGER AS m, [NULL] AS l`},
		{"duplicate names", `SELECT 1 AS a, 'x' AS a`},
		{"names that need quoting", `SELECT 1 AS "we""ird", 2 AS "sp ace", 3 AS "Upper"`},
		{"trailing semicolon", `SELECT 1 AS a;`},
		{"trailing comment", "SELECT 1 AS a -- a note"},
		{"union, through DESCRIBE", `SELECT union_value(k := 1) AS u`},
		{"table function", `SELECT * FROM range(3) t(r), generate_series(TIMESTAMP '2024-01-01', TIMESTAMP '2024-01-02', INTERVAL 1 HOUR) g(ts)`},
		{"explain", `EXPLAIN SELECT 1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := db.DescribeArrowSchema(context.Background(), tc.query)
			if err != nil {
				t.Fatalf("DescribeArrowSchema: %v", err)
			}
			if got, want := schema.String(), arrowQuerySchema(t, db, tc.query); got != want {
				t.Fatalf("described schema\n%s\nquery result schema\n%s", got, want)
			}
		})
	}
}

// The statement is bound, not run: a statement that fails only when it runs
// is described.
func TestDescribeArrowSchemaDoesNotRunTheStatement(t *testing.T) {
	db := newDescribeTestDB(t, 2)
	query := `SELECT error('the statement ran') AS x FROM range(1)`
	if _, _, err := db.ArrowQueryContext(context.Background(), query); err == nil {
		t.Fatal("the probe statement does not fail when it runs")
	}
	schema, err := db.DescribeArrowSchema(context.Background(), query)
	if err != nil {
		t.Fatalf("DescribeArrowSchema ran the statement: %v", err)
	}
	if schema.NumFields() != 1 || schema.Field(0).Name != "x" {
		t.Fatalf("schema %s", schema)
	}
}

func TestDescribeArrowSchemaRefusals(t *testing.T) {
	db := newDescribeTestDB(t, 2)

	t.Run("a statement that does not bind gets DuckDB's error", func(t *testing.T) {
		_, err := db.DescribeArrowSchema(context.Background(), `SELECT no_such_column`)
		if !errors.Is(err, ErrSchemaBind) || !strings.Contains(err.Error(), "no_such_column") {
			t.Fatalf("err = %v, want ErrSchemaBind with DuckDB's message", err)
		}
	})
	t.Run("parameters", func(t *testing.T) {
		_, err := db.DescribeArrowSchema(context.Background(), `SELECT ?::INTEGER AS p`)
		if !errors.Is(err, ErrSchemaParameters) {
			t.Fatalf("err = %v, want ErrSchemaParameters", err)
		}
	})
	for _, query := range []string{`SELECT (1, 2) AS s`, `SELECT row(1, 'a') AS s`} {
		t.Run("unnamed struct "+query, func(t *testing.T) {
			_, err := db.DescribeArrowSchema(context.Background(), query)
			if !errors.Is(err, ErrSchemaUnsupported) {
				t.Fatalf("err = %v, want ErrSchemaUnsupported", err)
			}
		})
	}
	t.Run("a statement with no query result", func(t *testing.T) {
		_, err := db.DescribeArrowSchema(context.Background(), `SET threads = 2`)
		if !errors.Is(err, ErrSchemaUnsupported) {
			t.Fatalf("err = %v, want ErrSchemaUnsupported", err)
		}
	})
	t.Run("an ended context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := db.DescribeArrowSchema(ctx, `SELECT 1 AS a`)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})
}

// The bind and the schema query share one pool connection, so a pool of one
// serves it, and the connection is back in the pool afterwards.
func TestDescribeArrowSchemaOnASingleConnectionPool(t *testing.T) {
	db := newDescribeTestDB(t, 1)
	for i := 0; i < 3; i++ {
		if _, err := db.DescribeArrowSchema(context.Background(), `SELECT union_value(k := 1) AS u, 1 AS a`); err != nil {
			t.Fatalf("describe %d: %v", i+1, err)
		}
	}
	if _, err := db.DescribeArrowSchema(context.Background(), `SELECT (1, 2) AS s`); !errors.Is(err, ErrSchemaUnsupported) {
		t.Fatalf("err = %v, want ErrSchemaUnsupported", err)
	}
	if got := arrowQuerySchema(t, db, `SELECT 1 AS a`); got == "" {
		t.Fatal("no schema")
	}
	if inUse := db.Stats().InUse; inUse != 0 {
		t.Fatalf("%d connections still in use", inUse)
	}
}
