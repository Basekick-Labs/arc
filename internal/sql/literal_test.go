package sql

import "testing"

func TestQuoteStringLiteral(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "single quote",
			in:   "single'quote",
			want: "'single''quote'",
		},
		{
			name: "double quote",
			in:   `double"quote`,
			want: `'double"quote'`,
		},
		{
			name: "backslash",
			in:   `back\slash`,
			want: `'back\slash'`,
		},
		{
			name: "percent",
			in:   "percent%",
			want: "'percent%'",
		},
		{
			name: "non-ASCII",
			in:   "non-ASCII-日本語",
			want: "'non-ASCII-日本語'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := QuoteStringLiteral(tt.in); got != tt.want {
				t.Errorf("QuoteStringLiteral(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
