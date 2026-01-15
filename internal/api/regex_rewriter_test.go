package api

import (
	"strings"
	"testing"
)

func TestRewriteRegexToStringFuncs(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantSame bool // true if output should be same as input (no rewrite)
		contains []string
	}{
		{
			name:     "no regex function",
			input:    "SELECT * FROM hits WHERE url LIKE '%google%'",
			wantSame: true,
		},
		{
			name:     "URL domain extraction with REGEXP_REPLACE",
			input:    `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k FROM hits`,
			wantSame: false,
			contains: []string{"CASE", "WHEN", "LIKE", "split_part", "substr"},
		},
		{
			name:     "non-URL REGEXP_REPLACE should not be rewritten",
			input:    `SELECT REGEXP_REPLACE(name, '[0-9]+', '') FROM users`,
			wantSame: true,
		},
		{
			name:     "URL domain extraction with REGEXP_EXTRACT",
			input:    `SELECT REGEXP_EXTRACT(Referer, '^https?://(?:www\.)?([^/]+)', 1) AS domain FROM hits`,
			wantSame: false,
			contains: []string{"CASE", "WHEN", "LIKE", "split_part", "substr"},
		},
		{
			name:     "non-URL REGEXP_EXTRACT should not be rewritten",
			input:    `SELECT REGEXP_EXTRACT(name, '([0-9]+)', 1) FROM users`,
			wantSame: true,
		},
		{
			name:     "REGEXP_EXTRACT with group 2 should not be rewritten",
			input:    `SELECT REGEXP_EXTRACT(Referer, '^https?://(?:www\.)?([^/]+)', 2) FROM hits`,
			wantSame: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, changed := RewriteRegexToStringFuncs(tt.input)

			if tt.wantSame {
				if changed {
					t.Errorf("expected no change, but got:\n%s", result)
				}
				if result != tt.input {
					t.Errorf("expected same output, got:\n%s", result)
				}
			} else {
				if !changed {
					t.Errorf("expected change, but got same output")
				}
				for _, substr := range tt.contains {
					if !strings.Contains(result, substr) {
						t.Errorf("expected output to contain %q, got:\n%s", substr, result)
					}
				}
			}
		})
	}
}

func TestBuildURLDomainCASE(t *testing.T) {
	result := buildURLDomainCASE("Referer")

	// Should contain all protocol/www combinations
	if !strings.Contains(result, "https://www.") {
		t.Error("missing https://www. case")
	}
	if !strings.Contains(result, "http://www.") {
		t.Error("missing http://www. case")
	}
	if !strings.Contains(result, "https://") {
		t.Error("missing https:// case")
	}
	if !strings.Contains(result, "http://") {
		t.Error("missing http:// case")
	}

	// Should use split_part and substr
	if !strings.Contains(result, "split_part") {
		t.Error("missing split_part function")
	}
	if !strings.Contains(result, "substr") {
		t.Error("missing substr function")
	}
}

func BenchmarkRewriteRegexToStringFuncs_NoRegex(b *testing.B) {
	sql := "SELECT * FROM hits WHERE url LIKE '%google%'"
	for i := 0; i < b.N; i++ {
		RewriteRegexToStringFuncs(sql)
	}
}

func BenchmarkRewriteRegexToStringFuncs_WithRegex(b *testing.B) {
	sql := `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k FROM hits`
	for i := 0; i < b.N; i++ {
		RewriteRegexToStringFuncs(sql)
	}
}

func BenchmarkRewriteRegexToStringFuncs_WithRegexExtract(b *testing.B) {
	sql := `SELECT REGEXP_EXTRACT(Referer, '^https?://(?:www\.)?([^/]+)', 1) AS domain FROM hits`
	for i := 0; i < b.N; i++ {
		RewriteRegexToStringFuncs(sql)
	}
}
