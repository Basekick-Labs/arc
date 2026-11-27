package credscrub

import (
	"context"
	"database/sql"
	"encoding/json"
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
	// A full batch of rows the filter selects but the redactor leaves as
	// they are, ahead of the real candidates: the scan must move past them.
	for i := 0; i < batchSize; i++ {
		if _, err := tx.Exec(`INSERT INTO tier_migrations (file_path, error) VALUES (?, 'bad key s3://b?k')`, fmt.Sprintf("fp%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < batchSize+50; i++ {
		e := "disk full"
		if i%20 != 0 {
			e = leak // more than one batch of candidates
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
	leaky := 0
	for i := 0; i < batchSize+50; i++ {
		if i%20 != 0 {
			leaky++
		}
	}
	if want := leaky + 2; n != want {
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
	if err := db.QueryRow(`SELECT error FROM tier_migrations WHERE file_path = 'f1'`).Scan(&kept); err != nil || !strings.Contains(kept, "https://acct.blob.core.windows.net/c") {
		t.Fatalf("host and path must survive: %q, %v", kept, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM tier_migrations WHERE error = 'disk full' OR error = 'bad key s3://b?k'`).Scan(&leaks); err != nil || leaks != 2*batchSize+50-leaky {
		t.Fatalf("unrelated rows changed: %d", leaks)
	}
	// Once per column: a row written later with old-shaped text is not
	// revisited (this version writes clean text at the source).
	if _, err := db.Exec(`INSERT INTO tier_migrations (file_path, error) VALUES ('late', ?)`, leak); err != nil {
		t.Fatal(err)
	}
	if n, err := Run(ctx, db, zerolog.Nop()); err != nil || n != 0 {
		t.Fatalf("second run rewrote %d rows, %v; want none", n, err)
	}
	var marks int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + markerTable).Scan(&marks); err != nil || marks != len(columns) {
		t.Fatalf("markers %d, %v; want one per known column (%d)", marks, err, len(columns))
	}
}

// An audit record's detail is JSON: after the scrub it holds no credential and
// still decodes.
func TestRunScrubsAuditDetailJSON(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE audit_logs (id INTEGER PRIMARY KEY, event_type TEXT, detail TEXT)`); err != nil {
		t.Fatal(err)
	}
	detail, _ := json.Marshal(map[string]string{
		"run_id":        "r1",
		"abort_message": `walk failed: Get "https://acct.blob.core.windows.net/c?comp=list&sig=AUDITSECRET1": EOF`,
	})
	// Only aborted reconciliation runs record error text; other events are
	// not read (a large audit table is scanned through the event-type index).
	if _, err := db.Exec(`INSERT INTO audit_logs (event_type, detail) VALUES ('reconcile.run_aborted', ?), ('http.request', ?)`, string(detail), string(detail)); err != nil {
		t.Fatal(err)
	}
	if n, err := Run(context.Background(), db, zerolog.Nop()); err != nil || n != 1 {
		t.Fatalf("rewrote %d, %v; want 1", n, err)
	}
	var got string
	if err := db.QueryRow(`SELECT detail FROM audit_logs WHERE event_type = 'reconcile.run_aborted'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	var back map[string]string
	if strings.Contains(got, "AUDITSECRET1") || json.Unmarshal([]byte(got), &back) != nil || back["run_id"] != "r1" ||
		!strings.Contains(back["abort_message"], "https://acct.blob.core.windows.net/c") {
		t.Fatalf("audit detail after scrub: %s", got)
	}
}
