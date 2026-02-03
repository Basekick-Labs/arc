package api

import (
	"testing"
)

func TestOptimizeLikePatterns(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		changed  bool
	}{
		{
			name:     "no LIKE clause - unchanged",
			input:    "SELECT * FROM hits WHERE id = 1",
			expected: "SELECT * FROM hits WHERE id = 1",
			changed:  false,
		},
		{
			name:     "no WHERE clause - unchanged",
			input:    "SELECT * FROM hits",
			expected: "SELECT * FROM hits",
			changed:  false,
		},
		{
			name:     "Q22: URL LIKE then SearchPhrase empty check",
			input:    "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
			expected: "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' AND URL LIKE '%google%' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
			changed:  true,
		},
		{
			name:     "Q23: Title LIKE + URL NOT LIKE + SearchPhrase empty",
			input:    "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
			expected: "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' AND Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
			changed:  true,
		},
		{
			name:     "empty check already first - unchanged",
			input:    "SELECT * FROM hits WHERE SearchPhrase <> '' AND URL LIKE '%google%'",
			expected: "SELECT * FROM hits WHERE SearchPhrase <> '' AND URL LIKE '%google%'",
			changed:  false,
		},
		{
			name:     "single LIKE without empty check - unchanged",
			input:    "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
			expected: "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
			changed:  false,
		},
		{
			name:     "prefix LIKE (not infix) - unchanged",
			input:    "SELECT * FROM hits WHERE URL LIKE 'http%' AND SearchPhrase <> ''",
			expected: "SELECT * FROM hits WHERE SearchPhrase <> '' AND URL LIKE 'http%'",
			changed:  true,
		},
		{
			name:     "case insensitive - handles lowercase",
			input:    "select * from hits where url like '%google%' and searchphrase <> ''",
			expected: "select * from hits where searchphrase <> '' and url like '%google%'",
			changed:  true,
		},
		{
			name:     "multiple empty checks - moves last one",
			input:    "SELECT * FROM hits WHERE Title <> '' AND URL LIKE '%google%' AND SearchPhrase <> '' LIMIT 10",
			expected: "SELECT * FROM hits WHERE SearchPhrase <> '' AND Title <> '' AND URL LIKE '%google%' LIMIT 10",
			changed:  true,
		},
		{
			name:     "empty check without LIKE - unchanged (no benefit)",
			input:    "SELECT * FROM hits WHERE id = 1 AND status = 'active' AND name <> '' GROUP BY id",
			expected: "SELECT * FROM hits WHERE id = 1 AND status = 'active' AND name <> '' GROUP BY id",
			changed:  false,
		},
		{
			name:     "Q42-style query - no LIKE, no change",
			input:    "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= 15888 GROUP BY WindowClientWidth, WindowClientHeight",
			expected: "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= 15888 GROUP BY WindowClientWidth, WindowClientHeight",
			changed:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, changed := OptimizeLikePatterns(tt.input)
			if result != tt.expected {
				t.Errorf("OptimizeLikePatterns() result = %q, expected %q", result, tt.expected)
			}
			if changed != tt.changed {
				t.Errorf("OptimizeLikePatterns() changed = %v, expected %v", changed, tt.changed)
			}
		})
	}
}

func BenchmarkOptimizeLikePatterns(b *testing.B) {
	queries := []string{
		"SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
		"SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
		"SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
		"SELECT * FROM hits WHERE id = 1",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, q := range queries {
			OptimizeLikePatterns(q)
		}
	}
}
