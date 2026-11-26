package sql

import "testing"

// StripUnicodeSpaces must produce the text DuckDB's Parser::StripUnicodeSpaces
// produces, quirks included. Each case below pins one rule of that pass.
func TestStripUnicodeSpaces(t *testing.T) {
	const (
		nbsp = "\u00a0" // 2-byte
		emsp = "\u2003" // 3-byte
		zwsp = "\u200b"
		wj   = "\u2060"
		bom  = "\ufeff"
	)
	tests := []struct {
		name, in, want string
	}{
		{"no space", "SELECT 1", "SELECT 1"},
		{"no-break space between tokens", "SELECT" + nbsp + "1", "SELECT 1"},
		{"each listed code point", "SELECT" + "\u2000\u200a" + zwsp + "\u202f\u205f" + wj + "\u3000" + bom + "1", "SELECT        1"},
		{"BOM at the start", bom + "SELECT 1", " SELECT 1"},
		{"kept inside a single-quoted string", "SELECT 'a" + nbsp + "b'", "SELECT 'a" + nbsp + "b'"},
		{"kept inside a quoted identifier", `SELECT "a` + emsp + `b" FROM t`, `SELECT "a` + emsp + `b" FROM t`},
		{"doubled quote does not end the string", "SELECT 'it''s" + nbsp + "x'", "SELECT 'it''s" + nbsp + "x'"},
		{"kept inside a dollar string", "SELECT $$a" + nbsp + "b$$", "SELECT $$a" + nbsp + "b$$"},
		{"kept inside a tagged dollar string", "SELECT $t$a" + nbsp + "b$t$, 1", "SELECT $t$a" + nbsp + "b$t$, 1"},
		{"replaced after a dollar string closes", "SELECT $$a$$," + nbsp + "1", "SELECT $$a$$, 1"},
		{"kept inside a line comment", "SELECT 1 --a" + nbsp + "b\n", "SELECT 1 --a" + nbsp + "b\n"},
		{"replaced after the comment ends at LF", "SELECT 1 --c\n" + nbsp + "2", "SELECT 1 --c\n 2"},
		{"replaced after the comment ends at CR", "SELECT 1 --c\r" + nbsp + "2", "SELECT 1 --c\r 2"},
		// The pass has no block-comment state: a quote inside /* */ opens a span.
		{"quote inside a block comment opens a span", "SELECT 1 /* ' */," + nbsp + "2 'x'", "SELECT 1 /* ' */," + nbsp + "2 'x'"},
		// A $ followed by a tag byte opens a span wherever it stands, even
		// inside a word; the tag here is "AS" + the word joiner's bytes.
		{"dollar inside a word opens a tag", "SELECT x$AS" + wj + "$ , 2", "SELECT x$AS" + wj + "$ , 2"},
		// The quoted span is searched from the opener's last $, so $t$t$ closes at once.
		{"tag closes on the opener's own dollar", "SELECT $t$t$," + nbsp + "1", "SELECT $t$t$, 1"},
		// Scanning resumes on the closer's last $, which can open a new tag:
		// here the space's own bytes are tag bytes, so it is kept.
		{"closer's last dollar opens a new tag", "SELECT $t$t$" + nbsp + "1", "SELECT $t$t$" + nbsp + "1"},
		// An invalid tag byte is read again as ordinary text.
		{"invalid tag byte reread", "SELECT $a-" + nbsp + "1", "SELECT $a- 1"},
		// The scan stops two bytes short of the end: a 2-byte space there is
		// left, a 3-byte one is replaced.
		{"2-byte space in the last two bytes is left", "SELECT 1" + nbsp, "SELECT 1" + nbsp},
		{"3-byte space at the very end is replaced", "SELECT 1" + emsp, "SELECT 1 "},
		{"other non-ASCII untouched", "SELECT 'é', 1 AS a\u1680b, 2 AS c\u2028d", "SELECT 'é', 1 AS a\u1680b, 2 AS c\u2028d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StripUnicodeSpaces(tt.in)
			if got != tt.want {
				t.Errorf("StripUnicodeSpaces(%q)\n got %q\nwant %q", tt.in, got, tt.want)
			}
			if again := StripUnicodeSpaces(got); again != got {
				t.Errorf("not idempotent: %q -> %q", got, again)
			}
		})
	}
}

// The fast path must not hide a space: a lone lead byte check, not a rune set.
func TestStripUnicodeSpacesFastPathSeesEveryLeadByte(t *testing.T) {
	for _, sp := range []string{"\u00a0", "\u2000", "\u3000", "\ufeff"} {
		if got := StripUnicodeSpaces("SELECT" + sp + "1"); got != "SELECT 1" {
			t.Errorf("space %U not replaced: %q", []rune(sp)[0], got)
		}
	}
}

func TestHasUnicodeSpace(t *testing.T) {
	for in, want := range map[string]bool{
		"SELECT 1":            false,
		"SELECT\u00a01":       true,
		"SELECT\u2060x":       true,
		"SELECT 'é'":          false,
		"SELECT a\u1680b":     false, // DuckDB does not replace U+1680
		"SELECT a\u2028b":     false,
		"SELECT \ufeff":       true,
		"SELECT \u200b\u3000": true,
	} {
		if got := HasUnicodeSpace(in); got != want {
			t.Errorf("HasUnicodeSpace(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestBlockComment(t *testing.T) {
	tests := []struct {
		in             string
		end            int
		closed, nested bool
	}{
		{"/* a */ x", 7, true, false},
		{"/**/", 4, true, false},
		{"/*/ x", 5, false, false},
		// The second opener shares the first one's '*'.
		{"/*/*/ */ */", 11, false, true},
		{"/* a /* b */ */", 15, false, true},
		{"/* open", 7, false, false},
	}
	for _, tt := range tests {
		end, closed, nested := BlockComment(tt.in, 0)
		if end != tt.end || closed != tt.closed || nested != tt.nested {
			t.Errorf("BlockComment(%q) = %d,%v,%v, want %d,%v,%v", tt.in, end, closed, nested, tt.end, tt.closed, tt.nested)
		}
	}
}
