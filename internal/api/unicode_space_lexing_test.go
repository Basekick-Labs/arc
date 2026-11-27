package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/basekick-labs/arc/internal/database"
	_ "github.com/duckdb/duckdb-go/v2"
)

const (
	nbsp  = "\u00a0"
	nnbsp = "\u202f"
)

// ValidateSQLRequest returns the statement with the Unicode spaces DuckDB's
// parser replaces already replaced; that is the text callers must use.
func TestValidateSQLRequestReturnsCanonicalSQL(t *testing.T) {
	tests := []struct{ in, want string }{
		{"SELECT" + nbsp + "1", "SELECT 1"},
		{"SELECT 'a" + nbsp + "b'", "SELECT 'a" + nbsp + "b'"}, // inside a literal: part of the value
		{"SELECT 1", "SELECT 1"},
	}
	for _, tt := range tests {
		got, err := ValidateSQLRequest(tt.in)
		if err != nil {
			t.Fatalf("ValidateSQLRequest(%q): %v", tt.in, err)
		}
		if got != tt.want {
			t.Errorf("ValidateSQLRequest(%q) = %q, want %q", tt.in, got, tt.want)
		}
		if again, err := ValidateSQLRequest(got); err != nil || again != got {
			t.Errorf("not stable on its own output: %q -> %q, %v", got, again, err)
		}
	}
}

// Arc must read a literal after a Unicode space as DuckDB does: DuckDB sees
// a separator there and opens the literal.
func TestValidateSQLRequestLiteralAfterUnicodeSpace(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string // "" = accepted
	}{
		{"dollar string after a no-break space holds a semicolon", "SELECT" + nbsp + "$$a;b$$ AS s", ""},
		{"dollar string after a no-break space, then a second statement", "SELECT" + nbsp + "$$a$$ AS s; SELECT 2", "Multiple SQL statements"},
		{"quote inside a dollar string after a no-break space", "SELECT" + nbsp + "$$ ' $$ AS s; SELECT 2 --'", "Multiple SQL statements"},
		{"escape string after a narrow no-break space", "SELECT" + nnbsp + "E'a\\'b' AS s", ""},
		{"escape string after a narrow no-break space, then a second statement", "SELECT" + nnbsp + "E'\\'' AS s; SELECT 2 --'", "Multiple SQL statements"},
		// DuckDB's pre-pass leaves this space alone (it thinks the quote in
		// the comment opened a string), its lexer then reads it in code.
		{"space the pre-pass and the lexer disagree on", "SELECT 1 /* ' */," + nbsp + "2, 'x'", "Unicode space characters"},
		{"no-break space in the last two bytes", "SELECT 1" + nbsp, "Unicode space characters"},
		{"no-break space inside a literal", "SELECT 'a" + nbsp + "b' AS s", ""},
		{"no-break space inside a comment", "SELECT 1 -- a" + nbsp + "b\n", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateSQLRequest(tt.sql)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Errorf("refused: %v", err)
			case tt.wantErr != "" && err == nil:
				t.Errorf("accepted; want refusal containing %q", tt.wantErr)
			case tt.wantErr != "" && !strings.Contains(err.Error(), tt.wantErr):
				t.Errorf("refused with %q; want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// A second /* that shares the first one's '*' still nests.
func TestValidateSQLRequestOverlappingCommentOpeners(t *testing.T) {
	for _, q := range []string{
		"SELECT 1 /*/*/ */ */",
		"SELECT 1 /*/*/ ' */ */ , 2 --'",
	} {
		if _, err := ValidateSQLRequest(q); err == nil || !strings.Contains(err.Error(), "Nested block comments") {
			t.Errorf("ValidateSQLRequest(%q) = %v, want the nested-comment refusal", q, err)
		}
	}
}

func TestValidateWhereClauseUnicodeSpaces(t *testing.T) {
	h := &DeleteHandler{}
	got, _, err := h.validateWhereClause("host" + nbsp + "= 'a" + nbsp + "b'")
	if err != nil {
		t.Fatalf("refused: %v", err)
	}
	if got != "host = 'a"+nbsp+"b'" {
		t.Errorf("canonical clause = %q", got)
	}
	if _, _, err := h.validateWhereClause("host = " + nbsp + "$$a$$ AND 1 = 1"); err != nil {
		t.Errorf("dollar string after a no-break space refused: %v", err)
	}
	if _, _, err := h.validateWhereClause("host = " + nbsp + "$$ ' $$ ; SELECT '"); err == nil {
		t.Error("a semicolon outside the literal was accepted")
	}
}

// Every statement ValidateSQLRequest accepts must still be one SELECT, or
// unparseable, after Arc's rewrites, as DuckDB's own parser (its Unicode
// space pre-pass included) reads the SQL Arc would run. The inputs mix
// Unicode spaces, every literal form and comments, the shapes where Arc's
// reading and DuckDB's have diverged before.
func TestTransformedSQLStaysOneStatement(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	h := newCommaJoinTestHandler()
	h.queryCache = database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize)

	frags := []string{
		" ", nbsp, nnbsp, "\u2060", "\ufeff", "\u3000", "\u1680",
		"'", "''", "\"", "$$", "$t$", "$", "E'", "e'", "\\",
		"--c\n", "--c\r", "/*", "*/", "/* ' */", "(", ")", ",", ";",
		"1", "a", "x$AS", "AS", "FROM cpu", "SELECT 2",
	}
	// Seeds: one statement each, then a second one, in the shapes where the
	// two readings have diverged before; each run mutates them.
	seeds := []string{
		"SELECT" + nbsp + "$$ ' $$ AS s; SELECT 2 --'",
		"SELECT" + nnbsp + "E'\\'' AS s; SELECT 2 --'",
		"SELECT 1 /*/*/ ' */ */ ; SELECT 2 --'",
		"SELECT 1 /* ' */, x" + nbsp + "E'a\\'' ; SELECT 2 --'",
		"SELECT x$AS\u2060$AS$ ; SELECT 2 ; SELECT $AS$",
	}
	rng := rand.New(rand.NewSource(1480))
	accepted := 0
	for n := 0; n < 6000; n++ {
		var b strings.Builder
		if n%2 == 0 {
			b.WriteString("SELECT ")
			for k := rng.Intn(10) + 2; k > 0; k-- {
				b.WriteString(frags[rng.Intn(len(frags))])
			}
		} else {
			q := seeds[rng.Intn(len(seeds))]
			for k := rng.Intn(3); k > 0; k-- {
				at := rng.Intn(len(q) + 1)
				for at < len(q) && !utf8.RuneStart(q[at]) {
					at++ // insert between characters, never inside one
				}
				q = q[:at] + frags[rng.Intn(len(frags))] + q[at:]
			}
			b.WriteString(q)
		}
		canonical, err := ValidateSQLRequest(b.String())
		if err != nil {
			continue
		}
		accepted++
		conv, _, _, err := h.getTransformedSQLForQueryWithRefs(context.Background(), canonical, "")
		if err != nil {
			continue
		}
		var out string
		if err := db.QueryRow("SELECT json_serialize_sql(?::VARCHAR)::VARCHAR", conv).Scan(&out); err != nil {
			t.Fatalf("json_serialize_sql(%q): %v", conv, err)
		}
		var parsed struct {
			Error      bool              `json:"error"`
			ErrorType  string            `json:"error_type"`
			Statements []json.RawMessage `json:"statements"`
		}
		if err := json.Unmarshal([]byte(out), &parsed); err != nil {
			t.Fatalf("decode %q: %v", out, err)
		}
		switch {
		case parsed.Error && parsed.ErrorType == "parser":
			// DuckDB will not run it.
		case parsed.Error || len(parsed.Statements) != 1:
			t.Errorf("accepted %q, rewritten to %q, which DuckDB reads as: %.200s", b.String(), conv, out)
		}
	}
	if accepted < 100 {
		t.Fatalf("only %d inputs accepted; the generator no longer exercises the validator", accepted)
	}
}
