package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// The startup scrub reaches every distinct SQLite file that may hold stored
// error text (retention and continuous-query history may live outside the
// auth database), opens a file only if it exists, and visits a path once.
func TestScrubStoredCredentialTextEveryDatabase(t *testing.T) {
	dir := t.TempDir()
	authPath := filepath.Join(dir, "arc.db")
	retPath := filepath.Join(dir, "retention.db")
	missing := filepath.Join(dir, "never.db")
	leak := `Get "https://acct.blob.core.windows.net/c?comp=list&sig=SECRET12345": EOF`
	seed := func(path, ddl, insert string) {
		db, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(insert, leak); err != nil {
			t.Fatal(err)
		}
	}
	seed(authPath, `CREATE TABLE tier_migrations (id INTEGER PRIMARY KEY, error TEXT)`, `INSERT INTO tier_migrations (error) VALUES (?)`)
	seed(retPath, `CREATE TABLE retention_executions (id INTEGER PRIMARY KEY, error_message TEXT)`, `INSERT INTO retention_executions (error_message) VALUES (?)`)

	scrubStoredCredentialText(nil, []string{authPath, retPath, authPath, missing}, zerolog.Nop())

	for path, q := range map[string]string{
		authPath: `SELECT error FROM tier_migrations`,
		retPath:  `SELECT error_message FROM retention_executions`,
	} {
		db, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatal(err)
		}
		var text string
		if err := db.QueryRow(q).Scan(&text); err != nil {
			t.Fatal(err)
		}
		db.Close()
		if strings.Contains(text, "SECRET12345") {
			t.Fatalf("%s not scrubbed: %s", filepath.Base(path), text)
		}
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("a database file that did not exist was created: %v", err)
	}
}
