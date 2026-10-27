package credscrub

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

// Stored error text from earlier versions loses its credential material; other
// rows, other columns and absent tables are left alone; a second run changes
// nothing; batches past the batch size are all visited.
func TestRunScrubsStoredErrorText(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	ctx := context.Background()
	for _, q := range []string{
		`CREATE TABLE tier_migrations (id INTEGER PRIMARY KEY, file_path TEXT, error TEXT)`,
		`CREATE TABLE retention_executions (id INTEGER PRIMARY KEY, error_message TEXT)`,
		`CREATE TABLE tier_files (path TEXT PRIMARY KEY, tier TEXT)`, // no quarantine_reason column
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	leak := `failed to list: Get "https://acct.blob.core.windows.net/c?comp=list&sig=SECRETSIG123": EOF`
	tx, _ := db.Begin()
	for i := 0; i < batchSize+50; i++ {
		e := "disk full"
		if i%2 == 0 {
			e = leak
		}
		if _, err := tx.Exec(`INSERT INTO tier_migrations (file_path, error) VALUES (?, ?)`, fmt.Sprintf("f%d", i), e); err != nil {
			t.Fatal(err)
		}
	}
	_ = tx.Commit()
	if _, err := db.Exec(`INSERT INTO retention_executions (error_message) VALUES (?), (NULL), ('BlobEndpoint=https://h;SharedAccessSignature=sv=1&sig=X12345678;')`, leak); err != nil {
		t.Fatal(err)
	}

	n, err := Run(ctx, db, zerolog.Nop())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := (batchSize+50+1)/2 + 2; n != want {
		t.Fatalf("rewrote %d rows, want %d", n, want)
	}
	var leaks int
	for _, q := range []string{
		`SELECT COUNT(*) FROM tier_migrations WHERE error LIKE '%sig=%' AND error NOT LIKE '%sig=REDACTED%'`,
		`SELECT COUNT(*) FROM retention_executions WHERE error_message LIKE '%SECRETSIG%' OR error_message LIKE '%X12345678%'`,
	} {
		if err := db.QueryRow(q).Scan(&leaks); err != nil || leaks != 0 {
			t.Fatalf("%s: %d left, %v", q, leaks, err)
		}
	}
	var kept string
	if err := db.QueryRow(`SELECT error FROM tier_migrations WHERE file_path = 'f0'`).Scan(&kept); err != nil || !strings.Contains(kept, "https://acct.blob.core.windows.net/c") {
		t.Fatalf("host and path must survive: %q, %v", kept, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM tier_migrations WHERE error = 'disk full'`).Scan(&leaks); err != nil || leaks != (batchSize+50)/2 {
		t.Fatalf("unrelated rows changed: %d", leaks)
	}
	if n, err := Run(ctx, db, zerolog.Nop()); err != nil || n != 0 {
		t.Fatalf("second run rewrote %d rows, %v; want none", n, err)
	}
}
