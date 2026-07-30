package api

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

func TestWrapSourceMeasurement(t *testing.T) {
	const rp = "read_parquet('s3://b/db/m/**/*.parquet', union_by_name=true)"

	tests := []struct {
		name        string
		query       string
		database    string
		measurement string
		rpExpr      string
		want        string
	}{
		{
			name:        "FROM db.measurement",
			query:       "SELECT * FROM db.m WHERE x > 1",
			database:    "db",
			measurement: "m",
			rpExpr:      rp,
			want:        "SELECT * FROM " + rp + " WHERE x > 1",
		},
		{
			name:        "case-insensitive keyword (replacement normalizes to uppercase FROM)",
			query:       "select * from db.m",
			database:    "db",
			measurement: "m",
			rpExpr:      rp,
			// The matched lowercase "from" is replaced with literal "FROM ";
			// DuckDB is case-insensitive on the keyword so this is cosmetic.
			want: "select * FROM " + rp,
		},
		{
			name:        "dollar sign in path is literal (not submatch expansion)",
			query:       "SELECT * FROM db.m",
			database:    "db",
			measurement: "m",
			rpExpr:      "read_parquet('s3://b/$1/m', union_by_name=true)",
			want:        "SELECT * FROM read_parquet('s3://b/$1/m', union_by_name=true)",
		},
		// --- negative cases: must be left UNCHANGED ---
		// These document the deliberately narrow scope. A regex cannot safely
		// rewrite these without corrupting valid SQL, so they pass through. If a
		// future change widens the pattern, these guard against silent corruption
		// (e.g. rewriting a table-valued function into a scalar position).
		{
			name:        "measurement as prefix is not matched",
			query:       "SELECT * FROM db.measurement_5m",
			database:    "db",
			measurement: "measurement",
			rpExpr:      rp,
			want:        "SELECT * FROM db.measurement_5m",
		},
		{
			name:        "different database is not matched",
			query:       "SELECT * FROM otherdb.m",
			database:    "db",
			measurement: "m",
			rpExpr:      rp,
			want:        "SELECT * FROM otherdb.m",
		},
		{
			name:        "comma column reference is not rewritten",
			query:       "SELECT a, db.m AS x FROM t",
			database:    "db",
			measurement: "m",
			rpExpr:      rp,
			want:        "SELECT a, db.m AS x FROM t",
		},
		{
			name:        "GROUP BY column reference is not rewritten",
			query:       "SELECT a FROM t GROUP BY a, db.m",
			database:    "db",
			measurement: "m",
			rpExpr:      rp,
			want:        "SELECT a FROM t GROUP BY a, db.m",
		},
		{
			// KNOWN PRE-EXISTING LIMITATION (documented, not endorsed): a
			// three-part name in a table position (FROM db.m.field) has its db.m
			// prefix rewritten because the trailing \b matches the '.'. Arc's
			// query model uses two-part db.measurement names, so this shape does
			// not arise in practice today. Captured here so a future parser-based
			// rewrite is expected to FIX it (change this assertion when it does).
			name:        "three-part name: known partial-rewrite limitation",
			query:       "SELECT x FROM db.m.field",
			database:    "db",
			measurement: "m",
			rpExpr:      rp,
			want:        "SELECT x FROM read_parquet('s3://b/db/m/**/*.parquet', union_by_name=true).field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := wrapSourceMeasurement(tt.query, tt.database, tt.measurement, tt.rpExpr)
			if got != tt.want {
				t.Errorf("wrapSourceMeasurement(%q, %q, %q)\n  got:  %q\n  want: %q",
					tt.query, tt.database, tt.measurement, got, tt.want)
			}
		})
	}
}

func TestValidateTagColumns(t *testing.T) {
	tests := []struct {
		name    string
		tags    []string
		wantErr bool
	}{
		{"nil is valid", nil, false},
		{"empty slice is valid", []string{}, false},
		{"simple names", []string{"host", "region"}, false},
		{"underscore and hyphen and digits", []string{"data_center", "az-1", "shard0"}, false},
		{"empty name rejected", []string{"host", ""}, true},
		{"space rejected", []string{"host name"}, true},
		{"quote rejected (injection guard)", []string{"host\""}, true},
		{"paren rejected", []string{"count(*)"}, true},
		{"dot rejected", []string{"db.host"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTagColumns(tt.tags)
			if tt.wantErr && err == nil {
				t.Errorf("validateTagColumns(%v): expected error, got nil", tt.tags)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("validateTagColumns(%v): unexpected error: %v", tt.tags, err)
			}
		})
	}
}

func TestTagColumnsRoundTrip(t *testing.T) {
	// nil/empty encodes to NULL and decodes back to nil.
	for _, empty := range [][]string{nil, {}} {
		enc, err := encodeTagColumns(empty)
		if err != nil {
			t.Fatalf("encodeTagColumns(%v): %v", empty, err)
		}
		if enc != nil {
			t.Errorf("encodeTagColumns(%v): expected nil (NULL), got %q", empty, *enc)
		}
		got, decErr := decodeTagColumns(enc)
		if decErr != nil {
			t.Errorf("decodeTagColumns(nil): unexpected error: %v", decErr)
		}
		if got != nil {
			t.Errorf("decodeTagColumns(nil): expected nil, got %v", got)
		}
	}

	// Non-empty round-trips exactly.
	in := []string{"host", "region", "az-1"}
	enc, err := encodeTagColumns(in)
	if err != nil {
		t.Fatalf("encodeTagColumns: %v", err)
	}
	if enc == nil {
		t.Fatal("encodeTagColumns: expected non-nil for a non-empty list")
	}
	got, err := decodeTagColumns(enc)
	if err != nil {
		t.Fatalf("decodeTagColumns: %v", err)
	}
	if len(got) != len(in) {
		t.Fatalf("round-trip length mismatch: got %v, want %v", got, in)
	}
	for i := range in {
		if got[i] != in[i] {
			t.Errorf("round-trip[%d]: got %q, want %q", i, got[i], in[i])
		}
	}
}

func TestDecodeTagColumnsMalformed(t *testing.T) {
	// A corrupt value returns an error (caller warns) rather than panicking.
	bad := "not-json"
	if got, err := decodeTagColumns(&bad); got != nil || err == nil {
		t.Errorf("decodeTagColumns(malformed): expected (nil, error), got (%v, %v)", got, err)
	}
	// Empty string is a legitimate no-tags value: nil, no error.
	empty := ""
	if got, err := decodeTagColumns(&empty); got != nil || err != nil {
		t.Errorf("decodeTagColumns(empty string): expected (nil, nil), got (%v, %v)", got, err)
	}
}

// newSQLiteOnlyCQHandler builds a handler wired to a temp SQLite DB only —
// enough to exercise the create/get/list/migration paths that touch just
// sqliteDB (no DuckDB/ArrowBuffer needed).
func newSQLiteOnlyCQHandler(t *testing.T, dbPath string) *ContinuousQueryHandler {
	t.Helper()
	sqliteDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	h := &ContinuousQueryHandler{
		sqliteDB: sqliteDB,
		logger:   zerolog.Nop(),
	}
	if err := h.initTables(); err != nil {
		t.Fatalf("initTables: %v", err)
	}
	return h
}

func TestCQTagColumnsPersistAndMigrate(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "cq.db")

	// First handler: create the schema fresh (includes tag_columns).
	h := newSQLiteOnlyCQHandler(t, dbPath)

	// Insert a CQ with tag_columns directly (exercise the storage round-trip).
	tagsJSON, err := encodeTagColumns([]string{"host", "region"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	_, err = h.sqliteDB.Exec(`
		INSERT INTO continuous_queries
		(name, description, database, source_measurement, destination_measurement, query, interval, tag_columns, retention_days, delete_source_after_days, is_active)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "cq1", nil, "db", "src", "dst", "SELECT 1 {start_time} {end_time}", "1h", tagsJSON, nil, nil, true)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := h.getQuery(1)
	if err != nil {
		t.Fatalf("getQuery: %v", err)
	}
	if len(got.TagColumns) != 2 || got.TagColumns[0] != "host" || got.TagColumns[1] != "region" {
		t.Fatalf("tag_columns round-trip: got %v, want [host region]", got.TagColumns)
	}
	h.sqliteDB.Close()

	// Second handler over the SAME file: initTables runs the ALTER migration
	// guard against an already-migrated DB — must be idempotent (no error), and
	// the existing row must still read back its tags.
	h2 := newSQLiteOnlyCQHandler(t, dbPath)
	defer h2.sqliteDB.Close()
	got2, err := h2.getQuery(1)
	if err != nil {
		t.Fatalf("getQuery after re-init: %v", err)
	}
	if len(got2.TagColumns) != 2 {
		t.Fatalf("tag_columns lost across re-init: got %v", got2.TagColumns)
	}

	// A CQ created without tags reads back nil (back-compat / no-dedup path).
	_, err = h2.sqliteDB.Exec(`
		INSERT INTO continuous_queries
		(name, database, source_measurement, destination_measurement, query, interval, is_active)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, "cq2", "db", "src", "dst2", "SELECT 1 {start_time} {end_time}", "1h", true)
	if err != nil {
		t.Fatalf("insert cq2: %v", err)
	}
	got3, err := h2.getQuery(2)
	if err != nil {
		t.Fatalf("getQuery cq2: %v", err)
	}
	if got3.TagColumns != nil {
		t.Errorf("expected nil tag_columns for CQ created without tags, got %v", got3.TagColumns)
	}
}

// TestCQMigrationFromLegacySchema verifies the ALTER guard adds tag_columns to a
// database created BEFORE the column existed (a real upgrade), and legacy rows
// read back with nil tags.
func TestCQMigrationFromLegacySchema(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "legacy.db")

	// Build a legacy continuous_queries table WITHOUT tag_columns and seed a row.
	legacy, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err = legacy.Exec(`
		CREATE TABLE continuous_queries (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			description TEXT,
			database TEXT NOT NULL,
			source_measurement TEXT NOT NULL,
			destination_measurement TEXT NOT NULL,
			query TEXT NOT NULL,
			interval TEXT NOT NULL,
			retention_days INTEGER,
			delete_source_after_days INTEGER,
			is_active BOOLEAN DEFAULT TRUE,
			last_processed_time TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`)
	if err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	_, err = legacy.Exec(`INSERT INTO continuous_queries
		(name, database, source_measurement, destination_measurement, query, interval, is_active)
		VALUES ('legacy', 'db', 'src', 'dst', 'SELECT 1 {start_time} {end_time}', '1h', true)`)
	if err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}
	legacy.Close()

	// Opening via the handler runs initTables → the ALTER migration.
	h := newSQLiteOnlyCQHandler(t, dbPath)
	defer h.sqliteDB.Close()

	got, err := h.getQuery(1)
	if err != nil {
		t.Fatalf("getQuery legacy row after migration: %v", err)
	}
	if got.TagColumns != nil {
		t.Errorf("legacy row should have nil tag_columns, got %v", got.TagColumns)
	}
	if got.Name != "legacy" {
		t.Errorf("legacy row corrupted: name=%q", got.Name)
	}
}

func TestTimeColumnIsUnique(t *testing.T) {
	tests := []struct {
		name  string
		times []interface{}
		want  bool
	}{
		{"empty is unique", nil, true},
		{"single is unique", []interface{}{int64(100)}, true},
		{"distinct int64 times", []interface{}{int64(100), int64(200), int64(300)}, true},
		{"duplicate int64 times (grouped, no tags)", []interface{}{int64(100), int64(100)}, false},
		{"duplicate among distinct", []interface{}{int64(100), int64(200), int64(100)}, false},
		{"distinct strings", []interface{}{"2026-01-01", "2026-01-02"}, true},
		{"duplicate strings", []interface{}{"2026-01-01", "2026-01-01"}, false},
	}
	// Fails closed above the scan cap: an all-distinct output larger than the cap
	// returns false (not dedup-safe) rather than building an unbounded map.
	huge := make([]interface{}, maxUniquenessCheckRows+1)
	for i := range huge {
		huge[i] = int64(i)
	}
	if timeColumnIsUnique(huge) {
		t.Errorf("expected false for output larger than the uniqueness-check cap (fail closed)")
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := timeColumnIsUnique(tt.times); got != tt.want {
				t.Errorf("timeColumnIsUnique(%v) = %v, want %v", tt.times, got, tt.want)
			}
		})
	}
}

func TestValidateTagColumnsRejectsTime(t *testing.T) {
	for _, name := range []string{"time", "TIME", "Time"} {
		if err := validateTagColumns([]string{name}); err == nil {
			t.Errorf("validateTagColumns([%q]): expected rejection of reserved 'time'", name)
		}
	}
	// A non-time column alongside is fine.
	if err := validateTagColumns([]string{"host", "region"}); err != nil {
		t.Errorf("validateTagColumns([host region]): unexpected error: %v", err)
	}
}
